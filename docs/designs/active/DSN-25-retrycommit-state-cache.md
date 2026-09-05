# [DSN-25] OnBlockCommitted StateDB 缓存 — 消除 RetryCommit ↔ ReSimulation 锁检查 Race

> **版本**：v1（2026-07-09）
> **状态**：设计阶段
> **关联文档**：`DSN-24-unified-lock-check.md`（统一锁检查）、`../archived/DSN-23-patchpool-dag-design.md`（DAG 多 Patch）

---

## 1. Problem Statement

### 1.1 96.4% 的 ReSimulation 失败

2026-07-09 实验数据（RATE=100, delay=10, 4 shards）：

| 阶段 | 事件数 | 占比 |
|:----|:------:|:----:|
| RetryCommit success（TLV + stateDB CheckLock 双通过） | 11,655 | — |
| ReSimulation 尝试 | 11,557 | — |
| ✅ ReSimulation success | 417 | **3.6%** |
| ❌ Resimulation failed: state is locked | 11,140 | **96.4%** |

RetryCommit 的 stateDB CheckLock 通过了，但 StartReSimulation 的 VM 执行时 stateDB locker 说"被锁着"。

### 1.2 根因：两处查的不是同一个 state

```
RetryCommit (retry_scheduler.go:1288):
  stateAt, err := rs.bc.State()
  → 调用时刻的 live state 指针（版本 A）
  → CheckLock 全部通过 ✅ 返回 Locked: true

           ↓ 时间差 ↓
    （其他 goroutine 在 state A 上加锁；或新区块 N+1 提交）

StartReSimulation (simulator_leader.go:684):
  header := sim.bc.CurrentHeader()
  → 执行时刻的 state（版本 B，可能 ≠ A）
  → VM 执行 → Lockable → "被锁！" ❌
```

**两个问题：**

1. `rs.bc.State()` 返回的是 **live 指针**——调完就变了，不保证 CheckLock 时和实际执行时是同一份 state
2. `StartReSimulation` 每次重新取 `CurrentHeader()`，可能已是更新后的 block——和 RetryCommit 查的 state 不同版本

### 1.3 首次模拟不上 TLV 锁的副作用

首次模拟路径（`StartSimulateCXTransaction`）全程不调 `TempLockView.TryLock`——注释标注了但从未实现。这意味着多笔交易的首轮模拟可以同时成功→同时提交 SimTx→VerifySimulation 互相撞锁→部分回滚→进入 retry pool→然后 RetryCommit 开始和 stateDB 的 race。

之前试过让首次模拟上 TLV 锁，效果反而更差（增加了 block space 浪费）。

---

## 2. 设计方案

### 2.1 核心思路

**在 OnBlockCommitted 时缓存当前 stateDB 的 live 指针，供 RetryCommit 和 StartReSimulation 共用。**

```
OnBlockCommitted(blockNum):
  cachedState ← rs.bc.State()        // 存 live 指针（CheckLock 只读，安全）
  cachedBlockNum ← blockNum

RetryCommit(tx):
  Phase 1: TLV TryLockWithPriority
  Phase 2: stateDB = cachedState     // 用缓存的 state 做 CheckLock
           CheckLock(key) → 只读查询同一份 state
           if pass → return Locked: true

StartReSimulation(tx):
  req.BlockHash = cachedState.Header().Hash()  // 用缓存的同一版本
  → VM 执行 → Lockable → 和 RetryCommit 查的是同一份 state → 0 race
```

**为什么存 live 指针安全：** `CheckLock` 只读不写 `lockedStates`。即使缓存后别人在 stateDB 上加锁，CheckLock 只是查询当前值。关键是从 RetryCommit 到 StartReSimulation 的整个流程使用**同一个 state 对象**，确保 CheckLock 和 Lockable 之间没有"别的代码改了锁状态"的 gap。

### 2.2 缓存结构

```go
// retry_scheduler.go — retryScheduler struct 新增字段
type retryScheduler struct {
    // ... 现有字段 ...
    cachedState    atomic.Value   // 缓存的 stateDB 指针（api.StateDB）
    cachedBlockNum uint64         // 对应的 block number
}
```

用 `atomic.Value` 而非 `sync.RWMutex`：OnBlockCommitted 和 RetryCommit 是不同 goroutine，`atomic.Value` 的 Store/Load 是 lock-free 的，适合这种"一写多读"模式。

### 2.3 修改点

**① OnBlockCommitted：缓存 state（retry_scheduler.go:482）**

当前 `OnBlockCommitted` 里已有 `currentStateDB` 本地变量，只用于函数内的信号预检。改为写入 `rs.cachedState`：

```go
func (rs *retryScheduler) OnBlockCommitted(block *types.Block) {
    rs.tempLockView.OnBlockCommitted(block)
    
    // 缓存当前 stateDB 供 RetryCommit / StartReSimulation 共用
    if stateAt, err := rs.bc.State(); err == nil {
        rs.cachedState.Store(stateAt)
        rs.cachedBlockNum = block.NumberU64()
    }
    
    // ... 原有逻辑：staleTxs 清理、retry signals ...
}
```

**② 新增 getCachedStateDB 方法：**

```go
// getCachedStateDB 返回 OnBlockCommitted 时缓存的 stateDB。
// 和 RetryCommit / StartReSimulation 同一版本 → 消除 CheckLock 与 Lockable 的 race。
func (rs *retryScheduler) getCachedStateDB() (api.StateDB, bool) {
    v := rs.cachedState.Load()
    if v == nil {
        return nil, false
    }
    return v.(api.StateDB), true
}
```

**③ RetryCommit Phase 2 改用缓存（retry_scheduler.go:1200）：**

```go
// 原代码：
stateDB, err := rs.getStateDB(txHash)
if err != nil { ... }

// 改为：
stateDB, ok := rs.getCachedStateDB()
if !ok {
    // 缓存不可用（启动初期）→ fallback 到 live bc.State()
    stateDB, err = rs.getStateDB(txHash)
    if err != nil { ... }
}
```

**④ StartReSimulation 使用缓存的 block hash（simulator_leader.go:684-696）：**

```go
// 原代码：
header := sim.bc.CurrentHeader()

// 改为：
stateDB, ok := rs.getCachedStateDB()
if ok {
    header = stateDB.Header()   // 用缓存的 block header
} else {
    header = sim.bc.CurrentHeader()  // fallback
}
req.BlockHash = header.Hash()
```

---

## 3. 决策记录

| 序号 | 问题 | 选择 | 理由 |
|:----:|:-----|:----|:-----|
| 1 | 缓存 stateDB 的内容 | **存 live 指针** | CheckLock 只读，无需深拷贝 |
| 2 | StartReSimulation 用哪个 state | **也用缓存** | 完全消除同一 block 内的 race |
| 3 | 缓存到 ReSim 之间出了新区块 | **直接跑** | 新区块不一定碰相同 key，检测开销不值 |

---

## 4. 验证标准

| # | 验证项 | 指标 |
|:-:|:-------|:-----|
| 1 | ReSimulation 失败率 | 从 96.4% **降至 <10%**（预期：同一 block 内应为 0%） |
| 2 | RetryCommit → ReSim flow | 缓存命中的情况下，ReSim 不报 stateDB locked |
| 3 | 缓存 fallback | 启动初期/缓存未就绪时，自动降级到 `rs.bc.State()` |
| 4 | 跨 block 场景 | 缓存过期后 ReSim 仍可用旧 state 执行（决策3） |

---

## 5. 回退策略

如果缓存策略导致 StartReSimulation 用了过旧的 state 导致模拟结果上链失败，可在 `OnBlockCommitted` 中加一个 `maxStaleBlocks` 阈值：

```go
if block.NumberU64()-rs.cachedBlockNum > maxStaleBlocks {
    // 缓存太旧 → 放弃本次 StartReSimulation，等下次 OnBlockCommitted 更新缓存后重新调度
    utils.SSCLogger().Warn().Msg("cachedState too stale, skip re-simulation")
    return
}
```

当前先不加——等实验数据确认是否需要。
