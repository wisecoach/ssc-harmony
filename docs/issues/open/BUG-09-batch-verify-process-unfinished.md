---
id: BUG-09
title: StateProcessor batch verify 导致 Shard 1/3 区块生产停止，未完成交易暴增
type: BUG
status: active
priority: P0
reporter: BOSS
module: core/state_processor.go, ssc/verify.go
severity: critical
created: 2026-07-21
refs: [DSN-35, DSN-37]
---

## 问题描述

DSN-35/37（StateProcessor 交易分类 + doBatchVerify）实施后，RATE=100 实验的提交率从 98% 骤降到 18.85%。Shard 1 和 Shard 3 的区块生产逐渐退化并最终完全停止，导致大量交易永远保持在 unfinished 状态。

## 实验结果

### RATE=100 总览

| 指标 | 值 |
|------|----|
| 总交易 | 10,000 |
| 成功提交 | 1,885 (18.85%) |
| 超时 | 3,175 |
| 回滚 | 6 |
| 未完成 | 4,925 |
| TPS | 18.33 tx/s |
| 总耗时 | 102.83s |

### 分片状态

| Shard | Commit | Timeout | Unfinished | Rollback |
|-------|--------|---------|------------|----------|
| 0 | 801 | 1,129 | 134 | 0 |
| 1 | 291 | 490 | **2,083** | 1 |
| 2 | 640 | 992 | 57 | 1 |
| 3 | 153 | 564 | **2,651** | 4 |

### Baseline 对比

| Rate | Before (batch=off) | After (DSN-39) |
|------|-------------------|----------------|
| 100 | 98% 提交 | 18.85% 提交 |
| 150 | 82% 提交 | 待确认 |

## 复现条件

1. `EnableParallelBatch: true`（`build_keys.go:291`）
2. RATE=100, shard=4, validator=4
3. 实验运行 ~3.5 分钟

## 实际行为

### Shard 1/3 区块生产退化

Shard 1 区块间隔从 2s → 18s，block 21 后完全停止：
```
block 1→2:   2.0s (正常)
block 15→16: 4.7s ⚠️
block 16→17: 5.8s
block 17→18: 8.8s
block 18→19: 11.4s
block 19→20: 14.8s
block 20→21: 17.9s 💀
之后：永不
```

Shard 1 最终 block=21（正常 shard: Shard 2=96, Shard 0=71）
Shard 3 最终 block=14（batch verify 仅 5 次）

### Close 率极低

| Shard | Pool Timer Start | Leader Close Tx | Close 率 |
|-------|----------------|----------------|----------|
| 0 | 4,677 | 3,080 | 65.9% |
| 1 | 6,055 | 871 | 14.4% |
| 2 | 3,687 | 2,583 | 70.1% |
| 3 | 5,145 | 157 | 3.1% |

已 close 的 Shard 1 tx 中：524 CXT_COMMITTED + 331 CXT_ROLLBACKED (SP1 超时)

### Retry 系统未激活

- `CHAIN_RETRY_STATS` 全部为 0（retrySignalReceived=0, retryCommitCall=0）
- `globalLocked >> globalFinished`（Shard 1: 1671 locked vs 680 finished）
- `retryPool` 堆积（Shard 1: 1416）但从未被 retry

### OnBlockCommitted stats 尾行

```
Shard 1 (block 21): retryPool=1416, globalLocked=1671, globalFinished=680
Shard 3 (block 14): retryPool=222,  globalLocked=641,  globalFinished=112
Shard 0 (block 71): retryPool=1008, globalLocked=1761, globalFinished=1870
Shard 2 (block 96): retryPool=625,  globalLocked=1798, globalFinished=1731
```

### 非预期错误

`verify.go:215` — `get result failed, index out of range`：
```
"callIndex":"[5:0:1]","pc":1,"dependentResultsLength":1
```
只在 Shard 3 (port 9120) 出现。`dependentResults[pc]` 越界——`dependentResults` 只有 1 个元素但 `pc=1`。

## 根因分析

### 根因 1：`statedb.Prepare` 在 StateProcessor 分类改造中被移除

**代码位置**：`core/state_processor.go`

旧路径逐笔遍历 `block.Transactions()` 时，每笔交易前都调了 `statedb.Prepare(tx.Hash(), block.Hash(), i)`。改造为分类/bucket 模式后，这个调用被**完全移除**。

虽然 `Prepare` 只设置 `thash/bhash/txIndex`（不直接影响 EVM 执行语义），但：
- `statedb.thash` 用于日志地址关联
- 多个 bucket 共享同一个 `statedb`，未正确设置当前 tx 信息可能导致日志/事件错位

**影响度**：低。不影响 state 一致性，但可能导致日志异常。

### 根因 2：`batchVerifyPassed` 并行 Phase 3 中 `dependentResults` 的竞态访问

**代码位置**：`ssc/verify.go:1226-1242, 1268-1284`

```go
// Phase 1.5: 子上下文预分配 — 每个 CallState 的 dependentResults 共享底层 slice
for _, ref := range passedCS {
    subCtx := &callVerifyContext{
        dependentResults: cs.DependentResults,  // 引用原 slice
    }
    v.storeSubCtx(...)
}

// Phase 3: 全并行 execVerify — 多个 goroutine 可能并发读同一个 CallState
for i, ref := range passedCS {
    go func(...) {
        v.verifyExecuteForCallState(sim, txHash, r.callState, copies[csIdx])
        // 内部调 GetResult → loadSubCtx → subCtx.dependentResults[pc]
        // 如果同一个 CallState 被多个 Phase 3 goroutine 引用（同块同 SimTx 的多个 CallState），
        // 且 GetResult 中 atomic 非安全的 callFrame.PC++ 可能导致 pc > len(dependentResults)
    }(i, ref)
}
```

`GetResult`（verify.go:205-237）中 `subCtx.callFrame.Next()` 修改 `PC`。虽然 `callFrame` 是 per-CallState 的，但如果同一个 CallState 的 `dependentResults` 在多个 goroutine 中同时访问...实际上每个 CallState 只有一个 goroutine 执行，所以这应该安全。

但日志中的 `index out of range`（pc=1, depLen=1）表明 `dependentResults` 在模拟时就不完整——只记录了 1 个跨分片 CALL 结果，但实际执行触发了 2 次 CALL。这是**模拟阶段的问题**，不是 verify 的问题。串行路径可能因为不同的 stateDB 快照看不到这个问题。

### 根因 3（核心）：Batch verify 未能在非活跃 shard 上正确执行

Shard 3 只有 5 次 batch verify 调用，而 Shard 2 有 68 次。batch verify 只在当前区块的 `simBucket` 非空时才被触发。Shard 3 作为目标 shard（非 origin），收到的 SimTx 极少，导致 batch verify 被跳过的次数多。

在 batch 模式下，SimTx 的 precompile 返回 `nil, nil`（跳过单笔验证），因此没有 `VerifySimulation` → `verifySimulationParsed` 路径。**如果 `doBatchVerify`/`PendingBatchSimulations()` 没有正确调用 `BatchVerifySimulations`，这些 SimTx 的验证被完全跳过。**

```
Leader 侧: CommitTransactions → SimTx precompile (skip verify) → PendingBatchSimulations() → BatchVerifySimulations
Validator 侧: StateProcessor.Process → SimTx precompile (skip verify) → doBatchVerify() → BatchVerifySimulations
```

问题：**如果 `PendingBatchSimulations()` 返回空（`pendingBatchSims` 为空），Leader 侧不会触发 batch verify**。而 `pendingBatchSims` 只在 `CommitSimulation`（origin shard leader 的模拟阶段）中被填充。非 origin shard 的 leader 从不调 `CommitSimulation`，所以其 `pendingBatchSims` 始终为空。

**结论**：在非 origin shard 的 leader 上，SimTx 的验证被完全跳过（precompile skip + PendingBatchSimulations 返回空）。Validator 侧 `doBatchVerify` 是唯一验证路径，但如果 `doBatchVerify` 中的反序列化或 `BatchVerifySimulations` 调用有任何非预期行为，也会导致验证失败。

### 根因 4（连锁反应）：区块生产退化

1. SimTx 验证缺失 → 锁状态不正确 → `globalLocked` 只增不减
2. `OnBlockCommitted` 处理 retryPool 越来越慢（因为各种 `Range` 遍历大 map）
3. 共识轮次时间从 2s 膨胀到 18s
4. Shard 1/3 最终完全停止产块
5. 剩余 5000+ tx 永远 unfinished（timer 没有触发超时回调）

## 验证方法

- [ ] `statedb.Prepare` 补回后，RATE=100 提交率恢复到 90%+
- [ ] `doBatchVerify` 中增加日志确认 `BatchVerifySimulations` 执行
- [ ] 非 origin shard 的 `PendingBatchSimulations()` 返回空时，有 fallback 验证
- [ ] Shard 1/3 能正常跑完整个实验（不提前停止）
- [ ] `retry` 系统正常激活，`CHAIN_RETRY_STATS` 非零
