---
id: DSN-41
title: Batch Worker Pool — 灵活并行验证架构
type: DSN
status: active
priority: P0
author: Designer
created: 2026-07-21
updated: 2026-07-22
scope: [ssc/verify.go, ssc/api/types.go, core/state_processor.go, node/worker/worker.go]
refs: [BUG-09, BUG-10, DSN-35, DSN-40]
---

## 1. 概述

将 batch verify 从当前固定的 Phase 0-5 管线模式，改为基于 worker pool 的灵活并行验证架构。支持四种并行模式（全串行/CallState 内并行/SimTx 间并行/全并行），通过两个独立开关控制。引入超量上限（500）和整体超时（800ms）防止大块卡死区块生产。

**关键设计变更 v2**：Batch verify 在 CommitSSCTransactions **之前**执行。只有验证通过的 SimTx 才提交上链，不通过的（冲突/超时）留在 pending pool 走 retry。保证 nonce 连续性。

## 2. 现状与问题

### 2.1 当前 batch verify 问题

当前 `batchVerifyPassed` 的 Phase 0-5 管线在 Shard 3 block 15 的 544 SimTx 上耗时 **21s**，其中：

- `execVerify`: ~48% 耗时（stateDB.Copy + EVM 执行）
- `lockCheck`: ~46% 耗时（跨 SimTx 状态锁检查，串行）
- 其余: ~6% （lockState, post-lock 等）

**核心问题**：固定管线模式无法灵活控制并行度，大块时 SimTx 全部涌入处理，导致区块生产循环卡死。

### 2.2 BUG-09 根因

Shard 3 的产块退化正反馈循环：
1. pending pool 中 SimTx 堆积（前序块处理慢）
2. 后续块一次性捞入大量 SimTx（block 15: 544 个）
3. 仲裁通过后 544 个全部进入 batch verify → 21s 处理时间
4. 区块生产间隔从 2s 膨胀到 24s → SimTx 进一步堆积 → goto 1

### 2.3 BUG-10 根因（v1 实现问题）

v1 实现中 `PendingBatchSimulations()` 跨块累积 SimTx，导致 batch 膨胀到 1082 个。Phase 0.5 仲裁 45s 耗尽 deadline，worker pool 投递 0 个任务。且 batch verify 在 `CommitSSCTransactions` 之后执行，超时未验证的 SimTx 已上链但永远卡死。

**修复**：
1. 不再用 `PendingBatchSimulations()`（跨块累积），改为从当块 `pendingSimTxs`（或 `simBucket`）直接反序列化
2. batch verify 在 `CommitSSCTransactions` 之前执行，只提交通过的 SimTx

## 3. 设计方案

### 3.1 架构概览

```
pendingSimTxs（按 sender nonce 排序）
    ↓
Phase 0.5 仲裁（串行）
    ↓ passed（最多 500 个）
Worker pool（32 workers, 整体超时 800ms, 按 nonce 顺序投递）
    ├─ worker 1: stateDB.Copy() → execVerify → SendCommitVote
    ├─ worker 2: ...
    └─ (按序等待前面的完成)
    ↓ 所有通过 = 可提交
CommitSSCTransactions → 只提交通过的 SimTx（nonce 连续前缀）
    ↓
未通过的（冲突/超时/execFail）→ callForRetry / 留在 pending pool
```

### 3.2 核心理念变化

| 方面 | v1（旧） | v2（新） |
|:-----|:---------|:---------|
| batch verify 时机 | `CommitSSCTransactions` **后** | `CommitSSCTransactions` **前** |
| 提交决策 | 全部上链，再调 batch verify | batch verify 决定哪些提交 |
| 未验证的交易 | 已上链但卡死（超时无 vote） | 留在 pending pool，下个块重试 |
| nonce 连续性 | 依赖 ssc 内部 nonce 管理 | batch worker 按序投递保证 |
| SimTx 来源 | `PendingBatchSimulations()`（跨块累积） | `pendingSimTxs` map（当块） |

### 3.3 两个独立开关

```go
type BatchVerifyConfig struct {
    EnableParallelBatch  bool // 总开关：启用 batch 模式
    SimTxParallelism     int  // SimTx 间并行 worker 数（0=串行, 32=32 workers）
    CallStateParallelism int  // CallState 内并行 worker 数（0=串行, 4=4 workers）
    MaxSimTxPerBlock     int  // 单块最大 SimTx 验证量（默认 500）
    MaxBatchTimeMs       int  // 整体超时（默认 800ms）
}
```

四种模式组合：

| 模式 | SimTxParallelism | CallStateParallelism | 场景 |
|:-----|:----------------:|:--------------------:|:-----|
| **全串行** | 0 | 0 | 低负载，最小开销 |
| **CallState 内并行** | 0 | >0 | 单 SimTx 多 CallState |
| **SimTx 间并行** | >0 | 0 | 块内多 SimTx，默认 |
| **全并行** | >0 | >0 | 大块，同时多 SimTx + 多 CallState |

### 3.4 Worker 职责

每个 worker 处理一个 SimTx 的完整验证流程：

1. **stateDB.Copy()** — 共享只读 trie，开销 ~0.02ms
2. **execVerify** — EVM 重新执行合约（主瓶颈）
3. **SendCommitVote** — 异步发 commit vote（不等待 lockState）

Worker **不做**：
- `lockCheck` — 在 Phase 0.5 仲裁中串行完成
- `lockState` — 由主 goroutine 在所有 worker 完成后统一并行写回原 stateDB

因为 `stateDB.Copy()` 创建的是独立副本，副本上的 `SetAndLockState` 不会同步回原 stateDB。所以 lockState 必须在原 db 上操作。而原 db 的 `sync.Map` 操作并发安全，多个主 goroutine 可以并行写无冲突的 key。

### 3.5 按 nonce 顺序投递

`pendingSimTxs` 是按 sender 地址分组的 map。投递到 worker pool 前需要按 nonce 排序：

```go
// 将 map 展开为按 nonce 排序的 slice
type simTxEntry struct {
    tx    *types.Transaction
    sim   api.CXTSimulation
}
sortedSims := flattenAndSortByNonce(pendingSimTxs)
// sortedSims 中的 SimTx 按 (sender, nonce) 字典序排列

// 按序投递到 worker pool
for _, entry := range sortedSims[:maxSim] {
    if time.Now().After(deadline) {
        break
    }
    // 等待前一个 SimTx（相同 sender）完成
    if prevNonce+1 != entry.tx.Nonce() {
        waitForPrevCompletion(entry.tx.From())
    }
    simCh <- entry
}
// 完成后的 passed 列表自动保证 nonce 连续
```

通过这种按序投递 + 按序等待，保证了**通过验证的 SimTx 有连续的 nonce**，可以直接提交 `CommitSSCTransactions`。

### 3.6 Worker Pool 调度

```go
func batchVerifyThenCommit(sortedSims []simTxEntry, stateDB api.StateDB, header *block.Header, remainingTime time.Duration) ([]*types.Transaction, []api.CXTSimulation) {
    deadline := time.Now().Add(remainingTime)
    if remainingTime > maxBatchMs {
        deadline = time.Now().Add(maxBatchMs)
    }

    simCh := make(chan simTxEntry, len(sortedSims))
    resultCh := make(chan simResult, len(sortedSims))
    var wg sync.WaitGroup

    // 启动 SimTxParallelism 个 worker
    for i := 0; i < SimTxParallelism; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            db := stateDB.(*corestate.DB)
            for entry := range simCh {
                err := v.verifyOneSimTx(&entry.sim, db, header)
                resultCh <- simResult{tx: entry.tx, sim: &entry.sim, err: err}
            }
        }()
    }

    // 按序投递（受 deadline 控制）
    for _, entry := range sortedSims {
        if time.Now().After(deadline) {
            break
        }
        simCh <- entry
    }
    close(simCh)
    wg.Wait()
    close(resultCh)

    // 收集结果：通过的 SimTx 可以提交
    var passedTxs []*types.Transaction
    var passedSims []api.CXTSimulation
    for r := range resultCh {
        if r.err == nil {
            passedTxs = append(passedTxs, r.tx)
            passedSims = append(passedSims, *r.sim)
        }
    }

    // lockState（只对通过的 SimTx）
    for _, sim := range passedSims {
        v.lockStates(&sim, stateDB)
    }

    return passedTxs, passedSims
}
```

### 3.7 Worker 和 CommitSSCTransactions 的集成（leader 侧）

```go
// worker.go — CommitTransactions 中 SimTx 处理段
if len(pendingSimTxs) > 0 {
    // 1. 从 pendingSimTxs 反序列化 SimTx
    sims := deserializeSimTxs(pendingSimTxs)
    
    // 2. Batch verify（在 CommitSSCTransactions 之前）
    passedTxs, passedSims := batchVerifyThenCommit(sims, w.current.state, w.current.header, remainingTime)
    
    // 3. 只提交通过的 SimTx
    simNetTxns := types.NewTransactionsByPriceAndNonce(w.current.signer, w.current.ethSigner, passedTxs)
    w.CommitSSCTransactions(simNetTxns, coinbase, remaining, remainingTime)
    
    // 4. 未通过的 SimTx 不做处理（留在 pending pool，下个块重试）
}
```

### 3.8 Validator 侧（state_processor.go）

Validator 侧不需要上链决策——它只负责重新执行区块交易。`doBatchVerify` 保持现有行为：执行 SimTx 后调 `BatchVerifySimulations`，不修改 `CommitSSCTransactions` 行为。

### 3.9 串行/并行的分解

| 阶段 | 串行/并行 | 谁做 | 说明 |
|:-----|:---------:|:-----|:------|
| Phase 0.5 仲裁 | **串行** | 主 goroutine | 必须按顺序（写写冲突依赖顺序） |
| lockCheck | **串行**（随仲裁） | 主 goroutine | 跟仲裁同一遍遍历，`CheckLock` 是 sync.Map.Load（只读） |
| stateDB.Copy | **并行** | worker | 每个 SimTx 独立副本 |
| execVerify | **并行** | worker | 主瓶颈，EVM 执行 |
| SendCommitVote | **并行** | worker | 不依赖 lockState，提前发 |
| lockState | **串行** | 主 goroutine | 只在通过的 SimTx 上执行 |
| 提交上链 | **串行** | leader | `CommitSSCTransactions` 只传通过的 SimTx |

### 3.10 Phase 0.5 仲裁 + lockCheck

单个串行遍历，每个 SimTx 做三件事：

```go
for i := range simulations[:maxSim] {
    // 0.5a: 写集 vs 已提交写集（同块冲突）
    for key := range writeKeys[i] {
        if _, exists := committedWrites[key]; exists {
            failed = append(failed, i)
            continue SimLoop
        }
    }

    // 0.5b: 写集 vs 全局锁（跨块冲突）
    for key := range writeKeys[i] {
        if err := stateDB.CheckLock(key, sim.TxHash); err != nil {
            failed = append(failed, i)
            continue SimLoop
        }
    }

    // 0.5c: 读集 vs 已提交写集
    for key := range readKeys[i] {
        if _, exists := committedWrites[key]; exists {
            failed = append(failed, i)
            continue SimLoop
        }
    }

    // 无冲突
    for key := range writeKeys[i] {
        committedWrites[key] = struct{}{}
    }
    passed = append(passed, i)
}
```

## 4. 关键决策

| # | 决策项 | 选择 | 理由 |
|---|--------|------|------|
| D1 | batch verify 时机 | **CommitSSCTransactions 之前** | 通过的才提交，不通过的下个块重试，保证 nonce 连续 |
| D2 | SimTx 来源 | **当块 pendingSimTxs（map 反序列化）** | 替代 `PendingBatchSimulations()`，避免跨块累积 |
| D3 | 调度模型 | **Worker Pool（chan + goroutine）** | 比 semaphore 更灵活；超时控制自然 |
| D4 | Worker 职责 | **Copy + execVerify + SendCommitVote** | execVerify 是主瓶颈；vote 不依赖 lockState |
| D5 | lockState 时序 | **所有 worker 完成后统一做** | 副本上的 SetAndLockState 不回同步到原 db |
| D6 | 并行粒度控制 | **两个独立开关**（SimTxParallelism + CallStateParallelism） | 四种模式灵活组合 |
| D7 | 超量上限 | **500 SimTx/块** | 超过的不处理，留在 pending pool 等下个块 |
| D8 | 整体超时 | **800ms（从 CommitTransactions 起计）** | 超时后停止投递，已投递的让跑完 |
| D9 | 超限/超时处理 | **不 callForRetry** | 交易未 commit，留在 pending pool 自动重试 |
| D10 | lockCheck 并行 | **不并行，随仲裁串行做** | 仲裁必须串行，lockCheck 同一次遍历做完 |

## 5. 配置

### 5.1 配置结构体

```go
type BatchVerifyConfig struct {
    EnableParallelBatch  bool   `json:"enable_parallel_batch"`
    SimTxParallelism     int    `json:"sim_tx_parallelism"`       // 默认 32
    CallStateParallelism int    `json:"call_state_parallelism"`   // 默认 0（CallState 内串行）
    MaxSimTxPerBlock     int    `json:"max_sim_tx_per_block"`     // 默认 500
    MaxBatchTimeMs       uint64 `json:"max_batch_time_ms"`        // 默认 800（从 CommitTransactions 起计）
}
```

### 5.2 配置文件改动

`cmd/build_keys/build_keys.go` 中增加配置字段：

```go
BatchVerifyConfig: api.TimeoutConfig{
    EnableParallelBatch:  true,
    SimTxParallelism:     32,
    CallStateParallelism: 0,
    MaxSimTxPerBlock:     500,
    MaxBatchTimeMs:       800,
},
```

## 6. 边界与风险

| 风险 | 缓解 |
|:-----|:------|
| worker 超时关闭后，已投递但未执行的任务丢失 | `close(simCh)` 后 `wg.Wait()` 保证已投递的跑完 |
| SendCommitVote 在 lockState 前发出，其他节点可能认为 SimTx 已确认即使 lockState 失败 | lockState 不会失败（Phase 0.5 已确认无冲突） |
| execFail 的 SimTx 已经发了 SendCommitVote | 只在 execVerify 成功后发 vote |
| 超量 500 个，下个块又捞起 → 重复仲裁 | 前 500 个正常通过则 pending pool 逐渐消耗 |
| `CommitSSCTransactions` 只传通过的 SimTx，但 `remaining` 计数不对 | `remaining` 减去 `len(passedTxs)` 而不是 `len(pendingSimTxs)` |
| 按 nonce 投递导致高 nonce 的 SimTx 等待低 nonce → 串行瓶颈 | 不同 sender 的 SimTx 独立 nonce，可并行；只有相同 sender 才需等待 |

## 7. 验证计划

1. **RATE=100**: 提交率恢复到 90%+，Shard 1/3 正常产块到实验结束
2. `Leader batchVerify: calling BatchVerifySimulations` 日志在每个 shard leader 出现
3. 日志中**没有**跨块累积的 batch（最大 batchSize 应 ≤ 当块 SimTx 数 + 少量）
4. CHAIN_RETRY_STATS 非零
5. RATE=150 同步验证
