# [A04] 链上重试次数限制

> **阅读顺序**（`[前缀]` 表示阅读顺序）：
> 1. 📄 `docs/A01-sscc-refactor-plan.md` [A01] — SSCC 模块化重构总览
> 2. 📄 `docs/A02-lock-retry-mechanism.md` [A02] — 锁与重试机制
> 3. 📄 `docs/A03-CR-priority-optimization.md` [A03] — CR 提交优先级优化
> 4. 📄 `docs/A04-onchain-retry-limit.md` [A04] **（本文）** — 链上重试次数限制
> 5. 📄 `docs/A05-hotkey-retry-design.md` [A05] — HotKey 链式重试（数据层）
> 6. 📄 `docs/A06-lock-priority-coordination.md` [A06] — 锁优先级协调（协调层）
>
> **运维辅助**（与主线并行的独立文档）：
> - 📄 `docs/B01-log-query-guide.md` [B01] — 日志查询指南
> - 📄 `docs/B02-log-lifecycle.md` [B02] — 交易生命周期日志大全

## 背景

当前 CXT 交易的链上验证（`VerifySimulation`）遇锁冲突后会通过 `CallForRetry` 无限重试，`simulationNum` 不断递增但无上限。这导致：

- 锁竞争激烈的场景下，大量交易卡在重试循环中，消耗系统资源
- 无法区分"即将成功"和"永远冲突"的交易

## 三个优化思路

| # | 方案 | 说明 |
|---|------|------|
| 1 | 交易执行顺序优化 | `tx_submitter.go` 已设 Commit/Rollback 优先，但重试机制可能有 bug |
| 2 | **放弃链上重试（本轮实施）** | 限制链上验证的重试次数，超限后直接回滚 |
| 3 | 逐个修现有 bug | 通过 `filter_error.sh` 定位并修复，已在并行执行 |

## 设计决策

### Q1: 限制的范围是？

**决策：** 只限制链上验证重试（`Condition=Verify`），链下模拟（`Condition=Simulate`）不受限制。

三个 `CallForRetry` 调用点：

| 调用点 | 位置 | Condition | 是否受限 |
|--------|------|-----------|:--------:|
| `VerifySimulation` 遇锁冲突 | impl.go:1106 | `Verify` | ✅ |
| `StartSimulateCXTransaction` 遇锁冲突 | impl.go:1695 | `Simulate` | ❌ |
| `startReSimulation` 遇锁冲突 | impl.go:3755 | `Simulate` | ❌ |

### Q2: 计数机制

引入 `lockedSimulationNum` 记录交易**首次被链上锁定时的 simulationNum**，限制条件为：

```
simulationNum <= lockedSimulationNum + MaxOnChainRetries
```

**记录时机：** `VerifySimulation` 函数入口处，当 `CXTSimulationState.OnChainLockedSimulationNum == 0` 时，写入 `simulation.SimulationNum`。

### Q3: 检查点

**位置：** `VerifySimulation` 内 `len(conflictLockKeys) > 0` 的分支中，`CallForRetry` 之前。

```go
// 在构建 retryTx 之后、CallForRetry 之前
if simulation.SimulationNum > state.OnChainLockedSimulationNum + config.MaxOnChainRetries {
    // 超出限制，构建 Rollback vote
    ...
    return
}
```

### Q4: 超限后的行为

**决策：** 构建 `CXTCommitVote` (Type=Rollback) 发送给 origin shard，走共识回滚路径，不直接调用 `closeTransaction`。

### Q5: 新增类型

**新 Reason：** `ReasonMaxOnChainRetriesExceeded`

```go
// types.go
const (
    ReasonSuccess CXTCommitReason = iota
    ReasonExecutionFailed
    ReasonInvalidSimulation
    ReasonConflictRWSetFailedLock
    ReasonConflictRWSetRecall
    ReasonCxtTimeoutForSp1
    ReasonMaxOnChainRetriesExceeded  // 新增
)
```

**新 InvalidSimulationType：** `MaxRetryExceeded`

```go
// types.go
const (
    InvalidSerialization InvalidSimulationType = iota
    InvalidSignature
    InvalidExecution
    CXTTimeout
    MaxRetryExceeded  // 新增
)
```

### Q6: 配置参数

**位置：** `TimeoutConfig` 中新增字段

```go
type TimeoutConfig struct {
    Sp1              uint64 `json:"sp1" yaml:"sp1"`
    PoolTimeout      uint64 `json:"pool_timeout" yaml:"pool_timeout"`
    MaxOnChainRetries uint64 `json:"max_on_chain_retries" yaml:"max_on_chain_retries"`
}
```

## 数据流

```
SimulationTx 被打包进区块
  ↓
VerifySimulation() 入口
  → 记录 OnChainLockedSimulationNum (仅首次)
  ↓
检查 write/read set 锁状态
  ↓
如果 len(conflictLockKeys) > 0:
  ├─ simulationNum > lockedSimulationNum + MaxOnChainRetries?
  │   └─ Yes → 构建 Rollback vote → sendCXTCommitVote → return
  └─ No  → CallForRetry(retryTx) → 等待下次上锁后重试
```

## 涉及修改的文件

| 文件 | 修改内容 |
|------|----------|
| `ssc/api/types.go` | `CXTSimulationState` 加 `OnChainLockedSimulationNum`、`TimeoutConfig` 加 `MaxOnChainRetries`、`CXTCommitReason` 加 `ReasonMaxOnChainRetriesExceeded`、`InvalidSimulationType` 加 `MaxRetryExceeded` |
| `ssc/impl.go` | `VerifySimulation` 入口记录 lockedSimulationNum + 冲突分支加超限检查 |
