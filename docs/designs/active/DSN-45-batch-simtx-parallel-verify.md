---
id: DSN-45
title: SimTx 批量并行验证（leader 先行）
type: DSN
status: planned
priority: P0
author: Designer
created: 2026-08-27
updated: 2026-08-27
scope: [ssc/verify.go, ssc/api/types.go, node/worker/worker.go, cmd/build_keys/build_keys.go, core/state_processor.go]
refs: [DSN-34, DSN-35, DSN-37, DSN-40, DSN-41, BUG-09, BUG-10, BUG-11, BUG-13]
---

> **状态**：设计阶段，待评审后实施
> **背景**：BUG-13 定位时延主因是 SimTx 在交易池排队（submitToCommit 跨 ~20 块）；32 核机器只用了 ~4 核，瓶颈是区块构建（worker）串行处理 SimTx，而非算力不足。

## 1. 概述

在**保持单块 1s 时间预算不变**的前提下，把 leader 区块构建里 SimTx 的验证从「串行一笔一笔」改为「**冲突分组 + 跨 SimTx 并行 execVerify + 串行确定性写入**」，从而用满 32 核、在同样的 1s 墙钟内处理更多 SimTx。validator 侧保持原样（第一版），后续在同一开关下再做「跳过仲裁 → 并行 → 统一写入」。

## 2. 现状与问题

### 2.1 现状（rate=200 实验实测）
- `CommitSSCTransactions` 是**单线程 for 循环**，一笔一笔 `commitTransaction` → `VerifySimulation`。
- `VerifySimulation` 里虽然有并行（DSN-34：`stateDB.Copy()` + 每 callState 一个 goroutine），但那是**单笔交易内部**的 callState 并行，每笔 callState 少 → 同一时刻只占 ~4 核。
- 结果：32 核机器 CPU 占用低（~4 核），但 SimTx 仍排队 ~20 块（submitToCommit P50=36.8s / shard1）。

### 2.2 每笔耗时（rate=200 实测）
| 类型 | avg | p50 | p90 | p99 |
|---|---:|---:|---:|---:|
| SimTx (VerifySimulation total) | 2.09ms | 1.42 | 4.08 | 12.35 |
| └ execVerify | 1.80ms | 1.14 | 3.61 | 11.71 |
| └ lockState | 1.99ms | 1.33 | 3.94 | 12.16 |
| └ lockCheck | 0.14ms | 0.09 | 0.26 | 0.79 |
| CRTx (CommitOrRollbackWithProof total) | 0.32ms | 0.24 | 0.53 | 1.35 |

→ 单笔不贵（~2ms），瓶颈在**没有跨 SimTx 并行**，而非算力。

### 2.3 历史教训（为什么上次没采用）
- **BUG-09**：批量方案让「precompile 跳过单笔验证 + `PendingBatchSimulations()` 收集」，非 origin leader 上收集为空 → SimTx 完全没验证 → 提交率 98%→18.85%、产块停摆。
- **BUG-10**：批量前串行仲裁（CheckLock 遍历）在堆积 1082 笔时耗时 45s，耗尽 deadline，全部超时跳过。
- **BUG-11**：ChainPatch 在验证成功前就被写入 onChainPatches，脏 Patch 污染下游；Phase 1b Merge 破坏独立性。

## 3. 设计原则

1. **只改 leader 的区块构建路径**，validator 第一版不动 → 不存在「验证被跳过」，根治 BUG-09。
2. **并行只用于确定性只读部分**（lockCheck、execVerify 在 stateDB.Copy 上），所有状态变更按同一顺序串行提交 → leader/validator 一致由构造保证。
3. **仲裁可并行**：lockCheck 是 `globalLockedStates.Load(key)` 只读操作（sync.Map 并发安全），不是「访问需要上锁的资源」。
4. **单参数配置**（实验用）：`EnableParallelBatch bool`，默认关，不做链上 fork（实验无兼容负担，全量统一部署同一份配置即可）。

## 4. 设计方案

### 4.1 配置

```go
// ssc/api/types.go — Config 结构
type Config struct {
    // ...现有字段
    EnableParallelBatch bool // 实验开关：SimTx 批量并行验证，默认 false
}
```

暴露接口（供 worker 读取）：
```go
// ssc/api/sscs.go — Service 接口
IsParallelBatchEnabled() bool
```

设置（cmd/build_keys/build_keys.go，实验用）：
```go
EnableParallelBatch: false,  // 跑实验时改 true 做 A/B
```

消费（node/worker/worker.go）：
```go
if w.sscService.IsParallelBatchEnabled() {
    w.processSSCBatchParallel(sscTxns, coinbase, remaining, remainingTime)
} else {
    w.CommitSSCTransactions(sscTxns, coinbase, remaining, remainingTime)
}
```

### 4.2 交易处理顺序（所有节点一致）

```
每块统一顺序（leader 产块 & validator 复算一致）:
  ① CRTx 桶   → 串行（先，完成 SimTx 生命周期）
  ② SimTx 桶  → 批量并行路径（本次核心）
  ③ Normal    → 串行
```

### 4.3 拆分 VerifySimulation 为可复用三段（ssc/verify.go）

```go
// 1) 只读仲裁：检查与链上锁的冲突（可并行）
verifyLockCheck(sim, stateDB) bool
// 2) 只读执行：在 stateDB.Copy() 上跑 EVM（可并行）
verifyExec(sim, stateDBCopy) (result, error)
// 3) 写路径：写锁 + 发 vote（必须按序串行）
verifyLockState(sim, result, stateDB)
```

现有 `VerifySimulation` 改为顺序调用这三段（行为不变，保证可回退）。

### 4.4 批量并行流程（node/worker/worker.go — processSSCBatchParallel）

```
SimTx 池（按 sender/nonce 有序）
│
├─ Phase 0  收集
│    · 1s 预算内按 nonce 取每 sender 连续前缀
│    · 上限 MaxSimTxPerBlock=500（防大块正反馈，规避 BUG-10）
│    · deadline = 开始 + 预算
│
├─ Phase 1  仲裁（只读，并行）
│    1a 并行 lockCheck：每笔查全局锁（sync.Map 只读）→ 候选集
│    1b 冲突分组：反向索引 key→SimTx 建冲突图
│        → 独立组（组内无公共 key），组间可并行
│    · 候选上限 500 + deadline 800ms
│
├─ Phase 2  并行 execVerify
│    · 每个独立组一个 worker；组间并行、组内按序
│    · 每笔 SimTx 一份 stateDB.Copy()（确定性、隔离）
│    · 收集 通过/失败
│
├─ Phase 3  串行确定性写入（按 Phase 0 原始块序）
│    · lockStateWithRWSet（写真实 stateDB）
│    · 写 receipt / 更新 gas / nonce / 追加区块
│    · 发 commit vote
│
└─ Phase 4  retry/回滚
     · 冲突/失败 → callForRetry / rollback（同现状）
```

### 4.5 validator 侧（后续，同一开关下）

```
收到已定稿区块（SimTx 集合与顺序已固化）
  → 跳过仲裁（结果由 leader 决定并固化在区块里）
  → 并行 execVerify（stateDB.Copy 每笔一份）
  → 按区块顺序串行统一写入（lockState/receipt/gas/state root）
  → 与 leader 产出完全一致
```

### 4.6 一致性保证

| 环节 | 性质 | 并行方式 | 一致性 |
|---|---|---|---|
| lockCheck | 只读（sync.Map） | ✅ 可并行 | 无副作用 |
| execVerify | 纯函数（stateDB.Copy 隔离） | ✅ 可并行 | 确定 |
| lockState/写状态 | 确定性、需按序 | ❌ 串行 | 按块序 |
| leader vs validator | 同一顺序写 | — | 构造保证一致 |

### 4.7 关键风险控制

| 风险 | 措施 |
|---|---|
| BUG-09（验证被跳过） | validator 不动；leader 只并行 execVerify，不跳验证 |
| BUG-10（仲裁爆炸） | 候选上限 500 + deadline 800ms；lockCheck 并行 |
| BUG-11（ChainPatch 脏写） | AddOnChainPatch 只在验证成功后；Phase 1b 不 merge |
| stateDB.Copy GC 压力 | 每笔一个 Copy（而非每 callState），监控 GC |

## 5. 变更文件清单

| 文件 | 改动 |
|:-----|:-----|
| `ssc/api/types.go` | `Config.EnableParallelBatch bool` |
| `ssc/api/sscs.go` | `IsParallelBatchEnabled()` 接口 |
| `ssc/verify.go` | 拆 `verifyLockCheck` / `verifyExec` / `verifyLockState`；`BatchVerifySimulations` 或复用三段 |
| `node/worker/worker.go` | `processSSCBatchParallel` + 开关分支 |
| `cmd/build_keys/build_keys.go` | `EnableParallelBatch: false`（实验改 true） |
| `core/state_processor.go` | （后续）validator 并行路径 |

## 6. 验证计划

1. 默认 `EnableParallelBatch=false` → 行为不变（无回归）。
2. 打开后 A/B：
   - 每块 SimTx 处理数（`[BlockBudget]` simTxn）是否提升
   - CPU 占用是否从 ~4 核升到 20+ 核
   - submitToCommit / totalGap 是否下降
   - 提交率/超时无劣化
3. validator 侧启用后：state root 与 leader 一致、无分叉。

## 7. 开放问题

| 问题 | 说明 |
|---|---|
| 冲突分组是否严格 O(N²) | 用反向索引 key→txs，实际是 O(总 key 数)，非 O(N²) |
| gas/nonce 在批量写入时如何与串行路径保持完全一致 | Phase 3 按同一顺序逐笔调用原 commit 写逻辑 |
| stateDB.Copy 在 500 笔时的内存/GC 开销 | 需要实验监控 |
