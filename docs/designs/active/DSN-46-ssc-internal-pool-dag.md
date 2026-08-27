---
id: DSN-46
title: 专用跨分片内部交易池（去 Nonce + 进池仲裁分组 + ChainPatch/DAG 接入）
type: DSN
status: planned
priority: P0
author: Designer
created: 2026-08-27
updated: 2026-08-27
scope: [ssc, core/types, node/worker, core/state_processor, node/node]
refs: [DSN-23, DSN-26, DSN-45, BUG-09, BUG-11, BUG-13]
---

> **状态**：设计阶段
> **背景**：SimTx/CRTx 是内部共识交易，却被强制走 `(sender, nonce)` 排序；所有 SimTx 来自单个 submitter 账户 → 严格串行，是“32 核只跑 ~4 核”的主因之一。

## 1. 概述

为 SimTx/CRTx 实现**专用跨分片内部交易池**：
- **去除 nonce 强制串行**：内部交易不再走 `(sender, nonce)` 排序；**以区块数组顺序作为唯一序号，不设 seq 字段**；
- **进池即仲裁**：对链上锁做只读冲突检查，把不冲突的 SimTx **增量划分到互不冲突的 Batch**；
- **接入 ChainPatch/DAG**：SimTx 就绪由「上游已提交」决定，替代「nonce 排序保证顺序」；
- 出块时**直接拿到互不冲突批次** → 并行 execVerify（配合 DSN-45）。

## 2. 现状与问题

- `cmd/harmony/main.go:910`：所有 SimTx 用同一 submitter 私钥（单账户）。
- `ssc/tx_submitter.go`：`simNonce`/`crNonce` 是内部计数器，非真实用户 nonce。
- `worker.go` 用 `NewTransactionsByPriceAndNonce` 按 `(sender, nonce)` 严格排序 → 单账户全串行。
- 进池无仲裁：冲突检测全堆到出块时做（BUG-10 的仲裁爆炸根源之一）。
- ChainPatch/DAG 依赖目前靠 nonce 顺序隐式保证，未显式建模为就绪依赖。

### 每笔耗时（rate=200，DSN-45 §2.2）
| 类型 | avg | 说明 |
|---|---:|---|
| SimTx 处理 | 2.09ms | execVerify 1.80 + lockState 1.99 |
| CRTx 处理 | 0.32ms | 很便宜 |

→ 单笔不贵，瓶颈是**单账户 nonce 串行**导致无法跨 SimTx 并行。

## 3. 设计目标

1. 去除 nonce 对 SimTx/CRTx 的强制串行。
2. 进池即完成「链上锁冲突检查 + 冲突分组」，出块直接拿互不冲突批次。
3. 显式接入 ChainPatch/DAG 就绪依赖，替代 nonce 顺序保证。
4. 保持 leader/validator 确定性一致。
5. 配置可开关（实验用，`EnableInternalPool bool`，默认关）。

## 4. 方案设计

### 4.1 池结构

```go
type SSCInternalPool struct {
    batches    []*Batch                  // 分层不冲突组：P1, P2, ...
    waiting    map[common.Hash]*Entry    // 被链上锁阻塞（等锁释放重评）
    dagWaiting map[common.Hash]*Entry    // 被 DAG 上游阻塞（等上游提交）
    keyIndex   map[api.LockKey]map[common.Hash]struct{} // key → txs
    rwSetCache map[common.Hash]*api.RWSet               // 进池提取的 RWSet 缓存（不落盘）
    onChain    api.StateDB               // 只读视图（globalLockedStates）
    dag        map[common.Hash][]common.Hash            // tx → 上游列表
}

type Entry struct {
    txHash   common.Hash
    rwSet    *api.RWSet    // 从 SimTx payload 提取（池内缓存，不落盘）
    upstream []common.Hash // ChainPatch DAG 上游（来自 payload.UpstreamTxList）
    state    EntryState    // pending / ready / committed / waitingOnChain / dagWaiting
}
```

**Batch 不变量**：同一 Batch 内任意两笔 key 互不相交（成员可并行）；Batch 之间按优先级/进池先后有序。
**顺序权威**：**区块数组顺序 = 唯一序号**，不设 seq 字段；池内的先后只是内存实现细节（进池先后/优先级），不序列化、不落盘。

### 4.2 进池即仲裁 + 分组（onAddSimTx）

```
onAddSimTx(sim):
  1. 提取 rwset（从 payload；缓存到 rwSetCache）
  2. DAG 检查: 若上游未提交 → dagWaiting（等上游 Commit 通知）
  3. 链上锁检查: 对 rwset 每个 key 查 globalLockedStates（只读 sync.Map）
       冲突 → waiting（等 OnBlockCommitted/CR 释放锁后重评）
  4. 不冲突 → 分组:
       找第一个 key 与 rwset 不相交的 Batch 加入；都冲突则新建 Batch
     · 更新 keyIndex
  5. 顺序：以进池先后/优先级决定 Batch 内部与之间顺序（内存态，不落盘）
```

- 链上锁检查是只读（`globalLockedStates.Load`），进池时做很便宜。
- `batches` 增量维护，出块直接取，避免出块时重复仲裁。

### 4.3 ChainPatch/DAG 接入

- 每个 SimTx 的 `upstream` 来自 ChainPatch 的 `UpstreamTxList`（DAG 边）。
- **就绪条件 = 所有上游已提交/finalized**，否则进 `dagWaiting`。
- **DAG 就绪通知（R7）**：复用 `OnBlockCommitted` 回调——每块提交后扫描 `dagWaiting`，凡上游已提交/finalize 的移入 ready 并重新分组。这是每块一次的有界扫描，不是每笔通知。
- **索引统一（R7）**：内部池的 `keyIndex` 与现有 retryScheduler 的 `subscriber`/`keyIndex` **统一为一份**（由内部池持有，retryScheduler 引用），避免两套仲裁打架。
- 语义替换：~~「跳过锁检查是因为 nonce 排序保证顺序」~~ → **「上游已提交 + 冲突分组保证顺序」**。
- 规避 BUG-11：只在验证成功后才 `AddOnChainPatch` / finalize。

### 4.4 出块流程（leader，1s 预算）

```
取 batches[0]（单批，内部全局互不冲突；上限 500 + deadline 800ms）
  → 出块前对 batches[0] 做一次只读最终复核（链上锁可能已变化，R4）
  → 并行 execVerify（每笔 stateDB.Copy()，组内天然并行）
  → 按「区块数组顺序」（leader 定的确定性块序）串行装配 receipt/gas/root + 发 vote
  → 未通过/冲突 → retry / rollback
```

> **单批提案（R6）**：每块只提案 `batches[0]`——它内部全局互不冲突，所以全部可并行；
> 若预算有余，再串行提案 `batches[1]`（与 batches[0] 有 key 冲突，不能同块并行）。

### 4.5 CRTx 处理

- CRTx 进专用池（或独立队列），**优先于 SimTx**（完成生命周期）。
- CRTx 释放锁，不需冲突分组，按区块顺序处理。

### 4.6 一致性保证

| 项 | 保证 |
|---|---|
| 排序 | 以区块数组顺序为唯一序号（leader 定、validator 跟）；池内先后是内存态 |
| 仲裁 | 进池=初筛；出块对 `batches[0]` 做只读最终复核（R4） |
| 并行 | 只用于只读 execVerify（stateDB.Copy 隔离） |
| 提案 | 每块单批 `batches[0]`（全局互不冲突），预算余再串行下批（R6） |
| 写路径 | 按区块数组顺序串行装配、块序一致 |
| DAG 就绪 | 上游提交后才就绪（OnBlockCommitted 扫描通知），替代 nonce（R7） |

### 4.7 配置（实验用，单参数）

```go
// ssc/api/types.go
Config.EnableInternalPool bool // 默认 false

// 消费：worker.go / state_processor.go
if ssc.IsInternalPoolEnabled() { 使用内部池 + 进池分组 } else { 老 nonce 池 }
```

## 5. 与 DSN-45 的关系

| DSN | 职责 |
|---|---|
| DSN-45（A） | 块内并行 execVerify（拿到批次后怎么并行跑） |
| DSN-46（B） | 去 nonce + 进池仲裁分组 + DAG 接入（顺序/分组怎么来） |

两者配合：B 提供“现成的互不冲突批次”，A 消费它做并行；B 是 A 的并行度上限的根治。

## 6. 变更文件清单

| 文件 | 改动 |
|:-----|:-----|
| `ssc/api/types.go` | `Config.EnableInternalPool bool`；`Entry`/`Batch`/`SSCInternalPool` 类型 |
| `ssc/`（新文件） | `internal_pool.go`：池实现（进池仲裁、分组、DAG 就绪、RWSet 缓存） |
| `ssc/tx_submitter.go` | 提交到内部池而非 nonce 池 |
| `node/worker/worker.go` | 出块从内部池取 `batches[0]` |
| `node/node.go` | 内部池替换 addPendingTransactions 里的 SSC 路径 |
| `core/state_processor.go` | validator 复算走同一内部池顺序 |
| `cmd/build_keys/build_keys.go` | `EnableInternalPool: false` |

## 7. 验证计划

1. 默认关 → 无回归。
2. 开 A/B：
   - CPU 占用是否从 ~4 核提升到 20+ 核（去 nonce 串行）
   - submitToCommit / totalGap 是否下降
   - 同账户 SimTx 是否不再串行
   - DAG 就绪/重试是否正确（无 BUG-11 脏 Patch）
3. validator 与 leader 结果一致、无分叉。

## 8. 风险与开放问题

| 问题 | 说明 | 处理 |
|---|---|---|
| gas/nonce 语义 | SimTx/CRTx 是内部交易，非用户 nonce | 复用标准 ApplyTransaction 机制（DSN-45 §4.4.1），收据/gas/root 统一 |
| 池内先后如何定 | 进池先后/优先级（内存态，不落盘） | 已定：区块顺序由 leader 定、validator 跟 |
| 跨分片一致性 | 各分片内部池独立 | 需保证跨分片 SimTx 提交顺序一致（沿用现有跨分片协调） |
| DAG 通知竞态 | 上游 finalize 与下游就绪 | 已定：OnBlockCommitted 每块有界扫描（R7） |
| 索引统一 | 内部池与 retryScheduler 两套索引 | 已定：统一为一份，池持有、retry 引用（R7） |
