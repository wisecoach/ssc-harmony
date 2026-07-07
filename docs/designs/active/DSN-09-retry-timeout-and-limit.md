# [DSN-09] RetryScheduler 链下超时与重试超限

> **版本**：v2（2026-07-04）
> **解决的问题**：链下重试池（`retryScheduler`）缺少超时和重试次数上限，导致失效交易长期驻留 pool
>
> **关联文档**：
> - [DSN-08](DSN-08-retry-limit.md) — 链上重试次数限制（已有实现）
> - [DSN-04](DSN-04-onchain-retry-limit.md) — 链上重试限制（设计思路）
> - [DSN-02](DSN-02-lock-retry-mechanism.md) — 锁与重试机制总览

---

## 1. 问题背景

当前 `retryScheduler` 链下管理有三个缺陷：

**① 无重试次数上限（`AddToRetry` 入口）**

`AddToRetry`（`retry_scheduler.go:368`）不做任何 `SimulationNum` 检查，无论 tx 重试了多少次都直接入 pool。DSN-08 曾计划在此处拦截，但当前代码：
- `AddToRetry` 无检查
- `checkRetryLimitExceeded` 只在链上 verify 时生效
- 链下信号聚合 → `tryToReSimulation` 路径完全没有次数检查

结果：交易可以无限重试（实际观察到 16+ 次）。

**② 被动池超时用了错的值**

`OnBlockCommitted`（`retry_scheduler.go:530-534`）中被动池超时判断：

```go
if rs.maxRetriesTotal > 0 && currentBlock >= enterBlock+uint64(rs.maxRetriesTotal) {
```

`maxRetriesTotal` 是重试次数上限，被错误地当作 block timeout 使用。注释也写着 `TODO 目前超时逻辑是错的`。

**③ 主动池无超时保护**

已上链的交易在 `retryPool` 中无超时机制。如果所有 related shard 都打不开临时锁，交易永远卡在 pool 里，最终靠 `PoolTimeout=100k` blocks（`build_keys.go:286` 硬编码）的超长 timeout 等死。

---

## 2. 总体架构

### 2.1 两条独立的重试保护线

| 保护线 | 位置 | 检查时机 | 超限后果 | 控制参数 |
|--------|------|----------|----------|----------|
| 链下重试次数 | `AddToRetry`（`retry_scheduler.go`） | 入 pool 前 | 直接 `CloseTransaction`，不经过链上 | `MaxRetriesTotal` |
| 链下区块超时 | `OnBlockCommitted`（`retry_scheduler.go`） | 每区块一次 | 直接 `CloseTransaction`，不经过链上 | `PoolTimeout` / `Sp1` |
| 链上重试次数 | `VerifySimulation`（`verify.go`） | 链上验证时 | 发 `sendRollbackVoteForRetry` → CR 回滚 | `MaxOnChainRetries` |

链下拦截效率更高（省去一轮模拟→上链→验证），链上是兜底（防止链下拦截失效）。

### 2.2 全链路重试流程

```
首次 Simulation（StartSimulateCXTransaction）
  ↓ 锁冲突
CallForRetry → AddToRetry
  ├── MaxRetriesTotal 超限 → CloseTransaction(reason=retry_limit_exceeded)
  └── 正常 → 入 retryPool, 记录 enterBlock
                ↓
        每区块 OnBlockCommitted:
          ├── 信号聚合 → 全部 shard ready → tryToReSimulation
          │     └── RetryCommit(TempLockView.TryLock)
          │           ├── 全部成功 → TriggerReSimulation → StartReSimulation
          │           │     ↓ 锁冲突
          │           │   CallForRetry(simNum+1) → 回到 AddToRetry（继续循环）
          │           │     ↓ VerifySimulation 通过
          │           │   CommitSimulation → SimTx 上链
          │           │     ↓
          │           │   VerifySimulation（所有 related shard）
          │           │     ├── 成功 → lockStateWithRWSet → Commit vote
          │           │     ├── 锁冲突 + MaxOnChainRetries 未超限 → CallForRetry
          │           │     └── 锁冲突 + MaxOnChainRetries 超限 → sendRollbackVoteForRetry
          │           └── 任一失败 → 进入被动池（等待下一轮 RetryCommit 唤醒）
          │
          ├── 主动池超时扫描（新增）
          │     ├── 已上链 + Sp1 超时 → 不做任何事（继续尝试，依赖 MaxRetriesTotal + verify 兜底）
          │     └── 未上链 + PoolTimeout 超时 → CloseTransaction(reason=PoolTimeout)
          │
          └── 被动池超时扫描（修复）
                ├── 已上链 → 不做任何操作（继续等待 RetryCommit 唤醒）
                └── 未上链 + PoolTimeout 超时 → CloseTransaction(reason=PoolTimeout)
```

### 2.3 两条重试次数上限的作用范围

| 参数 | 控制范围 | 检查位置 | 公式 |
|------|----------|----------|------|
| `MaxRetriesTotal` | 全链路 | `AddToRetry` 入口 + `verify.go:checkRetryLimitExceeded` | `SimulationNum > MaxRetriesTotal` |
| `MaxOnChainRetries` | 链上 verify 阶段 | `verify.go:checkRetryLimitExceeded` | 存在链上锁时 `SimulationNum > lockedSimNum + MaxOnChainRetries` |

`checkRetryLimitExceeded` 同时检查两个条件，**满足任意一条即超限**，发送 `sendRollbackVoteForRetry`：

```go
return chainExceeded || totalExceeded, lockedSimNum
```

`MaxRetriesTotal` 管的是"链下一共让重试几次"，`MaxOnChainRetries` 管的是"上过链后还允许重试几次"。两者结合形成完整保护：
- 链下无限重试但是超限前没通过 VerifySimulation → 到 `MaxRetriesTotal` 上限关闭
- 链下进了 pool 但一直抢不到锁 → 区块超时关闭

---

## 3. 链上 VerifySimulation 恢复重试

### 3.1 现状

ablation B 将所有冲突路径改为**直接发 Rollback vote**，`VerifySimulation` 中不再有 `CallForRetry`。两个冲突分支：

| 冲突路径 | ablation B 行为 |
|----------|----------------|
| `conflictCallStateIndex >= 0`（`verify.go:451-476`） | `checkRetryLimitExceeded` 超限→`sendRollbackVoteForRetry`；未超限→发 `ReasonConflictRWSetFailedLock` vote（**均不调 CallForRetry**） |
| `conflictLockKeys > 0`（`verify.go:479-510`） | 直接发 Rollback vote（**不调 CallForRetry**） |

### 3.2 改动要点

`verify.go` 的 `conflictCallStateIndex >= 0` 分支有三个改动：

1. **`checkRetryLimitExceeded` 提前到 `lockStateWithExecution` 之前** — 如果已经超限，直接发 Rollback vote，避免不必要的重执行开销
2. **新增配置 `EnableLockOnConflict`**（`TimeoutConfig` 中）— 控制是否在冲突时通过 `lockStateWithExecution` 上局部锁。关闭时直接 `CallForRetry`，不上锁
3. **恢复 `CallForRetry`** — 未超限且允许重试时，调 `CallForRetry` 调度下一轮链下重试

### 3.3 配置项

```go
type TimeoutConfig struct {
    Sp1                uint64 // ...
    PoolTimeout        uint64 // ...
    MaxOnChainRetries  uint64 // ...
    MaxRetriesTotal    uint64 // ...
    ForceSimulation    bool   // ...
    EnableLockOnConflict bool `json:"enable_lock_on_conflict" yaml:"enable_lock_on_conflict"` // 新增
}
```

| `EnableLockOnConflict` | 冲突时行为（未超限时） |
|:----------------------:|----------------------|
| `true` | `lockStateWithExecution`（上局部锁 + 重执行）→ `RollbackTx` → `CallForRetry` |
| `false` | 直接 `RollbackTx` → `CallForRetry`（不上锁） |

`lockStateWithExecution` 的作用是在当前冲突的 callState 写集上上局部锁，防止下一轮 SimTx 又撞同一把锁。但它的代价是完整重执行合约，且如果 `MaxOnChainRetries` 已经接近上限，这轮上锁很可能白费。通过配置让实验者自己权衡。

### 3.4 改后伪代码

```
conflictCallStateIndex >= 0:

  // 第一步：先检查超限，避免不必要的 lockStateWithExecution
  if exceeded, lockedSimNum := checkRetryLimitExceeded(txHash, simulation); exceeded {
      发 sendRollbackVoteForRetry → return
  }

  // 第二步：按配置决定是否上局部锁
  if config.EnableLockOnConflict {
      // 上锁 + 重执行冲突 callState
      lockStateWithExecution(...)
      if failed {
          发 Rollback vote → return
      }
  }

  // 第三步：解锁 + 调度重试
  stateDB.RollbackTx(txHash)
  if config.EnableLockOnConflict && 未超限 {
      callForRetry(simulationNum+1)
  } else if 允许重试 {
      callForRetry(simulationNum+1)
  }
  return
```

`conflictLockKeys > 0` 分支（纯写锁冲突，不涉及 callState 重执行）不受此配置影响，始终走：

```
stateDB.RollbackTx
→ checkRetryLimitExceeded
  → 超限 → sendRollbackVoteForRetry
  → 未超限 → callForRetry(simNum+1)（不上锁）
```

### 3.5 代码变更（`verify.go`）

**`conflictCallStateIndex >= 0` 分支（`:451-476`）：**

```go
// [冲突检测到，conflictCallStateIndex >= 0]

// 第一步：先检查超限
if exceeded, lockedSimNum := v.checkRetryLimitExceeded(txHash, simulation); exceeded {
    // 超限：直接发回滚投票，不执行 lockStateWithExecution 了
    stateDB.RollbackTx(txHash)
    v.sendRollbackVoteForRetry(...)
    return
}

// 第二步：按配置决定是否上局部锁
if v.config.EnableLockOnConflict {
    snapshot := stateDB.Snapshot()
    err := v.lockStateWithExecution(simulation.Epochs, simulation.CallStates[conflictCallStateIndex], stateDB.(*corestate.DB))
    if err != nil {
        stateDB.RevertToSnapshot(snapshot)
        stateDB.RollbackTx(txHash)
        // 上锁失败，发 Rollback vote
        v.communicator.SendCommitVote(vote)
        return
    }
}

// 第三步：解锁 + 调度重试
stateDB.RollbackTx(txHash)
v.callForRetry(txHash, simulation)
return
```

**`conflictLockKeys > 0` 分支（`:479-510`）：**

```go
// [纯写锁冲突]
stateDB.RollbackTx(txHash)
if exceeded, lockedSimNum := v.checkRetryLimitExceeded(txHash, simulation); exceeded {
    v.sendRollbackVoteForRetry(...)
} else {
    v.callForRetry(txHash, simulation)
}
return
```

### 3.6 `Verifier` 新增 `callForRetry` 方法

`verify.go` 新增方法，通过 `communicator` 回调触发重试：

```go
func (v *Verifier) callForRetry(txHash common.Hash, simulation *api.CXTSimulation) {
    if !v.committee.IsLeader(simulation.Epochs[v.committee.SelfShard]) {
        return
    }
    v.stats.setCxtStage(txHash, 4)
    v.communicator.CallForRetry(&api.RetryTx{
        TxHash:        txHash,
        Epochs:        simulation.Epochs,
        RelatedShards: simulation.RelatedShards,
        SimulationNum: simulation.SimulationNum + 1,
        Condition:     api.Verify,
        OriginShardID: simulation.OriginShardId,
    })
}
```

### 3.7 `Verifier` 需要回调接口

在 `ssc/api/types.go` 的 `VerifierCommunicator` 中新增：

```go
type VerifierCommunicator struct {
    // ... 现有字段 ...
    CallForRetry func(tx *api.RetryTx)
}
```

---

## 4. 链下超时模型

### 4.1 两条链下限

| 维度 | 指标 | 适用场景 | 触发后行为 |
|------|------|----------|-----------|
| 重试次数上限 | `SimulationNum >= MaxRetriesTotal` | 所有进入 retry pool 的交易 | `closeTransaction` (commit:false, reason="retry_limit_exceeded") |
| 区块超时（未上链） | `enterBlock + PoolTimeout >= currentBlock` | 未上过链的 tx | `closeTransaction` (commit:false, reason="PoolTimeout") |
| 区块超时（已上链） | `enterBlock + Sp1 >= currentBlock` | 已上过链的 tx | **不做任何操作**（继续尝试形成 SimTx，由 verify 的 `checkRetryLimitExceeded` 处理）|

**已上链的 tx 不能通过链下机制结束**。已上链意味着：
- `VerifySimulation` 已经调过 `lockStateWithRWSet`，链上锁（`stateLockManager.lockedStates`）正在持有
- 可能有 pending 的回滚投票流程尚未结束

唯一正确路径：继续尝试 `tryToReSimulation` → 形成 SimTx 上链 → 下一轮 `VerifySimulation` → `checkRetryLimitExceeded` 超限 → `sendRollbackVoteForRetry`（发 CR 回滚投票）→ 走完投票流程释放锁。

如果移出 pool，该 tx 永远没有机会再上链，锁会一直持有。所以已上链的 tx 超时后**不做任何事**——超时检测仅用于打 warn 日志，不改变 tx 状态。

`AddToRetry` 的 `MaxRetriesTotal` 拦截是链下的兜底：如果已上链的 tx 链下重试次数也超了 `MaxRetriesTotal`，则在 `AddToRetry` 入口就被拦截，根本进不了 pool。

### 4.2 区块超时的两种 timeout 值

| 交易状态 | 使用哪个 timeout | 理由 |
|----------|:----------------:|------|
| 未上过链（`IsOnChain == false`） | `PoolTimeout` | 还在准入阶段，用通用超时兜底 |
| 已上过链（`IsOnChain == true`） | `Sp1` | 已上链意味着至少完成一次模拟→验证→投票流程，用更短的 Sp1 超时快速停止无效重试 |

`IsOnChain` 判断依据：`Verifier.txLockedSimNum` 中存在该交易 → 说明至少上过一次链（`lockStateWithRWSet` 设过链上锁）。

### 4.3 超时扫描时机

每区块提交时（`OnBlockCommitted`），同时扫描主动池（`retryPool`）和被动池（`passivePool`）：

```
OnBlockCommitted(block)
  ├── TempLockView.OnBlockCommitted
  ├── 清理 staleTxs
  ├── 主动池：信号聚合（已有逻辑，不改）
  ├── 主动池：超时扫描（新增）
  │     ├── 已上链 → Sp1 超时
  │     └── 未上链 → PoolTimeout 超时
  ├── 被动池：超时扫描（修复现有逻辑）
  │     └── PoolTimeout 超时（不再用 maxRetriesTotal）
  └── 发送 promoted signals
```

---

## 5. 详细设计：retry_scheduler.go

### 5.1 新增字段

```go
type retryScheduler struct {
    // ... 现有字段 ...
    maxRetriesTotal int    // 已存在：重试次数上限

    // 新增：超时配置
    poolTimeout     uint64 // PoolTimeout 区块数（未上链 tx 超时）
    sp1Timeout      uint64 // Sp1 区块数（已上链 tx 超时）
    retryPoolEnter  sync.Map // key: common.Hash, value: uint64(enterBlockNum)
}
```

### 5.2 `NewRetryScheduler` 签名变更

```go
func NewRetryScheduler(ctx, bc, state, comm, selfShard, tempLockView, config *api.TimeoutConfig)
```

`impl.go:182` 调用处传入 `sscConfig.Timeout`。

构造时提取：

```go
rs.maxRetriesTotal = int(config.MaxRetriesTotal)
rs.poolTimeout = config.PoolTimeout
rs.sp1Timeout = config.Sp1
```

### 5.3 `AddToRetry` — 重试次数上限拦截

**位置**：`retry_scheduler.go:AddToRetry()`，在 `staleTxs` 检查之后、`retryPool.LoadOrStore` 之前。

```go
// 链下重试次数上限
if rs.maxRetriesTotal > 0 && tx.SimulationNum >= rs.maxRetriesTotal {
    rs.state.CloseTransaction(tx.TxHash, false, "retry_limit_exceeded")
    utils.SSCLogger().Info().Str("txHash", tx.TxHash.Hex()).
        Int("simulationNum", tx.SimulationNum).
        Int("maxRetries", rs.maxRetriesTotal).
        Msg("AddToRetry: retry limit exceeded, closing transaction")
    return
}
```

**说明**：
- `>=` 而非 `>`：`SimulationNum` 从 0 开始。`MaxRetriesTotal=5` 时，允许 simulationNum=0..4（共 5 次），第 6 次 simulationNum=5 时拦截
- 拦截后直接 `closeTransaction`，不经过链上 verify 路径，避免额外一轮开销

### 5.4 `AddToRetry` 记录 enter block

在 `retryPool.LoadOrStore` 成功之后：

```go
if _, loaded := rs.retryPool.LoadOrStore(tx.TxHash, tx); loaded {
    return
}
// 新增：记录进入 pool 的区块号
if rs.bc != nil {
    rs.retryPoolEnter.Store(tx.TxHash, rs.bc.CurrentHeader().NumberU64())
}
```

### 5.5 `OnBlockCommitted` — 主动池超时扫描

在信号聚合（`retryPool.Range` for signals）之后、被动池扫描之前插入：

```
for each tx in retryPool:
  enterBlock, ok := rs.retryPoolEnter.Load(txHash)
  if !ok: continue
  if rs.state.IsOnChain(txHash):
    // 已上链 → Sp1 超时：不打日志，不做任何操作
    // 该 tx 需要继续尝试形成 SimTx，由 verify.checkRetryLimitExceeded 处理
    // 同时也受 AddToRetry.MaxRetriesTotal 兜底
    if rs.sp1Timeout > 0 && currentBlock >= enterBlock + rs.sp1Timeout:
      utils.SSCLogger().Warn().Str("txHash", txHash.Hex()).
        Uint64("heldBlocks", currentBlock - enterBlock).
        Msg("on-chain retry timeout, continue retrying via VerifySimulation")
  else:
    // 未上链 → PoolTimeout 超时：直接 CloseTransaction
    if rs.poolTimeout > 0 && currentBlock >= enterBlock + rs.poolTimeout:
      expired = append(expired, txHash)
for each expired tx:
  retryPool.Delete, retryPoolEnter.Delete → CloseTransaction
```

### 5.6 被动池超时修复

被动池的 tx 也是分两种状态：

- **已上链**的被动池 tx：不做任何操作，继续等待 `RetryCommit` 唤醒 → `tryToReSimulation` → SimTx → verify 兜底
- **未上链**的被动池 tx：用 `PoolTimeout` 超时关闭

现有代码（`retry_scheduler.go:530-534`，被动池扫描）未区分上链状态，统一用 `maxRetriesTotal` 做超时：

```go
if rs.maxRetriesTotal > 0 && currentBlock >= enterBlock+uint64(rs.maxRetriesTotal) {
```

改为：

```go
// 已上链的被动池 tx：不做任何操作
if rs.state.IsOnChain(txHash) {
    continue
}
// 未上链：PoolTimeout 超时
if rs.poolTimeout > 0 && currentBlock >= enterBlock+rs.poolTimeout {
    expiredPassive = append(expiredPassive, txHash)
}
```

---

## 6. 配置项

| 参数 | 位置 | 当前值 | 建议值 | 说明 |
|------|------|:------:|:------:|------|
| `MaxOnChainRetries` | `build_keys.go:287` | 1 | 5 | 链上验证阶段最多重试次数。0=首次冲突即回滚 |
| `MaxRetriesTotal` | `build_keys.go:288` | 3 | 10 | 链下重试次数上限（含链上失败）。0=不限制 |
| `PoolTimeout` | `build_keys.go:286` | 5 | 10 | 未上链交易在 pool 中的最大区块超时。0=不限制 |
| `Sp1` | `build_keys.go:285` | 5 | 5 | 已上链交易在 pool 中的最大区块超时。0=不限制 |
| `EnableLockOnConflict` | `build_keys.go`（新增） | false | false | 冲突时是否通过 lockStateWithExecution 上局部锁 |

### 6.1 实验推荐配置

| 场景 | `MaxOnChainRetries` | `MaxRetriesTotal` | `EnableLockOnConflict` | 理由 |
|------|:-------------------:|:-----------------:|:----------------------:|------|
| Baseline（关重试） | 0 | 1 | false | 首次冲突即回滚，最小代价 |
| 允许有限重试 | 5 | 10 | false | 允许重试但不锁局部锁，靠链下重试调度 |
| 完整保护 | 5 | 10 | true | 上局部锁防抢占，链下+链上双重超限兜底 |

---

## 7. 文件变更清单

| 文件 | 变更 |
|------|------|
| `ssc/api/types.go` | `TimeoutConfig` 加 `EnableLockOnConflict` 字段；`VerifierCommunicator` 加 `CallForRetry` 字段 |
| `ssc/verify.go` | `conflictCallStateIndex` 分支：`checkRetryLimitExceeded` 提前到 `lockStateWithExecution` 之前；`lockStateWithExecution` 由 `EnableLockOnConflict` 控制；恢复 `CallForRetry`；新增 `callForRetry` 方法 |
| `ssc/retry_scheduler.go` | `retryScheduler` 结构体新增 `poolTimeout`、`sp1Timeout`、`retryPoolEnter`；`NewRetryScheduler` 签名加 `config *api.TimeoutConfig`；`AddToRetry` 加超限拦截 + enter block 记录；`OnBlockCommitted` 加主动池超时扫描（已上链仅打 warn 日志）；被动池超时修复（已上链跳过，未上链用 poolTimeout） |
| `ssc/impl.go` | `NewRetryScheduler` 调用处传入 `sscConfig.Timeout`；`VerifierCommunicator` 绑定 `CallForRetry` 回调 |
| `cmd/build_keys/build_keys.go` | 新增 `EnableLockOnConflict` 配置；调整各项建议值 |

---

## 8. 边界情况

| 场景 | 预期行为 |
|------|----------|
| `MaxRetriesTotal=0` | `AddToRetry` 入口跳过（`>0` 为 false），无限重试 |
| `MaxOnChainRetries=0` | 首次链上冲突即走 `sendRollbackVoteForRetry`，回滚 |
| `PoolTimeout=0` | 未上链 tx 和被动池均不超时 |
| `Sp1=0` | 已上链 tx 在主动池中不超时 |
| `stateDB.RollbackTx` + Rollback vote 并发 | 安全。一票否决 + 分片独立状态 |
| 超时 + 同时 `retry commit success` | 并发无问题：`CloseTransaction` 清理 pool entry；`tryToReSimulation` 已成功后不影响结果 |
| `retryPoolEnter` 无 entry（旧 pool 内的 tx） | `ok == false` → 跳过超时检查，向后兼容 |
| 主动池超时（未上链）→ `CloseTransaction` | `CloseTransaction` 内调用 `StaleTx` → `GarbageCollect(TempLockView)` → 从 pool/passivePool/passivePoolEnter 清理 |
| 主动池超时（已上链） | 不做任何操作，继续尝试形成 SimTx，由 `AddToRetry.MaxRetriesTotal` 或 verify 的 `checkRetryLimitExceeded` 处理 |
| 被动池超时（未上链）→ `CloseTransaction` | 同主动池未上链 |
| 被动池超时（已上链） | 不做任何操作，继续等待 `RetryCommit` 唤醒 |
| tx 多次进出 pool | 每次 `AddToRetry` 重新记录 `retryPoolEnter`，**不是**累加 |

---

## 9. 实现顺序

1. `verify.go` 恢复 `CallForRetry` + 新增 `callForRetry` + `VerifierCommunicator` 回调
2. `retryScheduler` 结构体加字段（`poolTimeout`, `sp1Timeout`, `retryPoolEnter`）
3. `NewRetryScheduler` 签名变更 + `impl.go` 调用处传入 config + 绑定回调
4. `AddToRetry` 入口加重试次数拦截 + enter block 记录
5. `OnBlockCommitted` 加主动池超时扫描
6. 被动池超时修复（`maxRetriesTotal` → `poolTimeout`）
