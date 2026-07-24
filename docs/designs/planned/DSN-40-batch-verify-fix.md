---
id: DSN-40
title: StateProcessor batch verify 修复 + 区块生产退化根治
type: DSN
status: planned
priority: P0
author: Designer
created: 2026-07-21
updated: 2026-07-21
scope: [core/state_processor.go, ssc/verify.go]
refs: [BUG-09, DSN-35, DSN-37]
---

> **状态**：⏳ 设计讨论中 — 本文件为草案框架，具体方案待设计 session 敲定

## 1. 概述

修复 DSN-35/37（StateProcessor 交易分类 + batch verify）引入的区块生产退化 bug，使 batch verify 在非 origin shard 的 leader 上正常工作，恢复被移除的 `statedb.Prepare`，堵住 SimTx 验证被跳过的路径。

## 2. 已知问题（来自 BUG-09）

| # | 问题 | 严重度 | 所属模块 |
|---|------|--------|---------|
| P1 | `statedb.Prepare(txHash, blockHash, idx)` 在 StateProcessor.Process 中被移除 | 中 | `core/state_processor.go` |
| P2 | 非 origin shard 的 leader 上 SimTx 验证被跳过（`pendingBatchSims` 为空 → `PendingBatchSimulations()` 返回空 → `BatchVerifySimulations` 不执行） | **致命** | `ssc/verify.go`, `core/state_processor.go` |
| P3 | `batchVerifyPassed` Phase 3 并行 execVerify 中 `dependentResults` index out of range | 高 | `ssc/verify.go` |
| P4 | retry 系统未激活（`CHAIN_RETRY_STATS` 全零）→ 锁越积越多 → 区块生产退化 | **致命** | `ssc/retry_scheduler.go` |

## 3. 待决策的修复方向

### 方向 A：补回 `statedb.Prepare`

**在 `processBucket` 每笔交易前**补回 `statedb.Prepare(txHash, blockHash, txIndex)`。

```go
func (p *StateProcessor) processBucket(...) {
    for _, tx := range bucket.txs {
        statedb.Prepare(tx.Hash(), block.Hash(), globalTxIndex)
        globalTxIndex++
        // ... existing logic
    }
}
```

需要引入 bucket 维度的全局 tx index 计数器。

**影响**：低风险，只是恢复原有行为。

### 方向 B：修复非 origin shard 的 batch verify 路径

**方案 B1：`doBatchVerify` 确保调用 `BatchVerifySimulations`**

当前 `doBatchVerify` 已存在，但需要确认在 Validator 侧正确执行。增加日志确认每次调用。

**方案 B2：Leader 侧 fallback 路径**

当 `PendingBatchSimulations()` 返回空时，**不从 pendingBatchSims 取**，而是从 **`simBucket` 反序列化**（类似 `doBatchVerify` 的方式），确保 Leader 侧的 SimTx 不会因为 `pendingBatchSims` 为空而被跳过验证。

```go
if len(simBucket.txs) > 0 {
    w.CommitSSCTransactions(simNetTxns, coinbase, remaining, remainingTime)
    
    // 当前路径：pendingBatchSims
    if simPtrs := collector.PendingBatchSimulations(); len(simPtrs) > 0 {
        BatchVerifySimulations(sims, ...)
    } else {
        // ⚡ fallback：直接从区块 SimTx 反序列化验证
        doBatchVerify(simBucket, w.current.state, w.current.header)
    }
}
```

**方案 B3：简化——移除 `pendingBatchSims` 机制**

不再依赖 `CommitSimulation` 收集 SimTxs，直接在 `CommitTransactions` 中从 `simBucket` 反序列化 SimTxs 并调用 `BatchVerifySimulations`。消除两条路径的不一致。

### 方向 C：retry 系统激活

确认 batch verify 失败后 `callForRetry` 和 `sendRollbackVoteForRetry` 是否正确触发了 retry 流程。需要检查 `callForRetry` 中的条件：

```go
func (v *Verifier) callForRetry(...) {
    if !v.committee.IsLeader(...) { return }  // 非 Leader 不调度
    v.retrySchd.CallForRetry(...)
}
```

`batchVerifyPassed` 中只有 `conflictSet` 的 SimTxs 能进入 `callForRetry`，但 Phase 0.5 中失败的不走 `batchVerifyPassed`。需要确认 Phase 0.5 中失败的 SimTxs 是否进入了 retry 流程。

### 方向 D：`dependentResults` index out of range

排查 Phase 3 并行验证中 `dependentResults` slice 的竞态。重点：
- `GetResult` 中的 `subCtx.callFrame.PC++` 是否是 atomic-safe
- 同一个 `dependentResults` slice 是否被多个 goroutine 共享
- 模拟阶段的 `dependentResults` 长度计算是否正确

## 4. 关键决策点

| # | 决策 | 选项 | 建议 |
|---|------|------|------|
| 1 | `statedb.Prepare` 补回 | 逐笔/分桶/不补 | **逐笔补回** |
| 2 | batch verify 触发路径 | pendingBatchSims / simBucket 反序列化 / 合并 | **移除 pendingBatchSims，统一用 simBucket 反序列化** |
| 3 | retry 激活 | 保持现有 / 修复 callForRetry 条件 | **待定** |
| 4 | Phase 3 并行安全 | atomic PC / per-goroutine subCtx / 串行化 | **待定** |

## 5. 验证计划

1. RATE=100 实验确认：
   - 提交率恢复到 90%+
   - Shard 1/3 正常产块到实验结束
   - `retryPool` <100 且 `CHAIN_RETRY_STATS` 非零
2. RATE=150 同步验证
3. 对比 RATE=100 和 RATE=200 的 OnBlockCommitted stats

## 6. 边界与风险

- **风险 1**：simBucket 反序列化比 pendingBatchSims 多一个 JSON unmarshal 开销。但这是白阻塞的（precompile 已经 unmarshal 了一次），不是额外开销。
- **风险 2**：`statedb.Prepare` 补回可能暴露其他依赖 `thash` 的竞态。但旧路径一贯如此，回归风险低。
- **边界**：不做 batch verify 的并发模型重构——仅修复 SimTx 验证被跳过的 bug。
