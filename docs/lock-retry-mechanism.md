# Harmony SSCC 锁与重试机制

> 基于 `ssc/` 包代码分析，2026-06-12 更新

---

## 一、三层锁架构

```
┌───────────────────────────────────────────────────┐
│  1. stateLockManager  — 全局链上锁管理器（单例）    │
│     保存所有已上链交易的锁状态，stateRoot 快照化     │
│     位于 state_lock_impl.go                        │
├───────────────────────────────────────────────────┤
│  2. stateLocker  — 模拟实例锁控制器（每模拟一份）    │
│     GetLockerAt(stateRoot) 获取"时间旅行"锁视图     │
│     Lock/Unlock 只在 pendingStates 中，隔离的       │
│     位于 state_locker.go                           │
├───────────────────────────────────────────────────┤
│  3. TempLockView  — 链下临时锁（Leader 维护）        │
│     管理本批次正在模拟的交易 + 从 stateLockManager    │
│     同步的已提交锁状态                               │
│     位于 temp_lock_view.go                         │
└───────────────────────────────────────────────────┘
```

### 1.1 stateLockManager — 全局锁管理器

```
stateLockManager
├── lockedStates *lockedStates       # 全局锁池
│   ├── lockedStates: LockKey → lockedState      # 写锁正向映射
│   ├── rlockedStates: LockKey → rlockedState    # 读锁正向映射
│   ├── callIndex2lockedState: txHash→callIndex→LockKey→value  # 写锁反向索引
│   └── callIndex2rlockedState: txHash→callIndex→LockKey→struct{} # 读锁反向索引
├── finishedTxs  map[txHash]bool     # 已完成（commit/rollback）的交易
├── snapshots    map[stateRoot]snapshot  # 状态根 → 锁快照（LRU）
├── snapshotMeta map[stateRoot]meta     # 访问元数据
├── maxSnapshots=5 / snapshotTTL=1h   # 配置
└── commitCnt   uint64               # 区块计数
```

**快照机制解决的问题**：stateDB 是版本化的（每区块一个 stateRoot），但锁状态是实时的全局状态。模拟交易在旧的 stateRoot 上执行时，应看到那个时间点的锁状态，不受后续区块影响。

### 1.2 stateLocker — 模拟实例的锁

```
stateLocker
├── baseSnapshot      # 只读基准快照（来自 stateLockManager 的深拷贝）
├── pendingStates     # 隔离的待提交锁变更（Lock/Unlock 只影响这里）
├── pendingUnlocks    # CommitTx/RollbackTx 记录的待释放 txHash
├── tmpFinishedTxs    # 本模拟中完成的 tx
├── journal           # 锁操作日志（支持 Snapshot/RevertToSnapshot 回滚）
└── validRevisions    # 快照点列表
```

每个 `simulate` 调用 `GetLockerAt(stateRoot)` 获得一个全新的 `stateLocker`。模拟期间的所有 Lock/Unlock 只影响该实例的 `pendingStates`。

### 1.3 TempLockView — 链下临时锁

```
TempLockView
├── committedWriteLocks  # 从 stateLockManager 同步的已提交写锁
├── committedReadLocks   # 同上
├── tempWriteLocks       # 本批次临时写锁: LockKey → txHash
├── tempReadLocks        # 本批次临时读锁: LockKey → [txHash...]
└── txReadWriteSets      # txHash → RWKeySet（用于清理）
```

**临时锁只在普通区块提交时通过 `OnBlockCommitted` 清理。**

---

## 二、锁的生命周期

### 阶段 A：新交易准入 — TempLockView.TryLock

新交易请求 `simulate` 时，leader 通过 `TempLockView.TryLock()` 做准入检查：

1. **写-写冲突**：检查 `committedWriteLocks` + `tempWriteLocks`，key 被其他交易持有 → 拒绝
2. **读-临时写冲突**：检查 `tempWriteLocks`，key 被本批次前面的交易写了 → 拒绝（读到未提交值）
3. **读-已提交写冲突**：**允许**（读的是最新已提交值）
4. 通过 → 注册到 `tempWriteLocks/tempReadLocks/txReadWriteSets`

**注意**：调用方需保证 `writes` 按全局顺序排序（死锁预防）。

### 阶段 B：模拟执行 — stateLocker.Lock/RLock

1. `Lockable(key)` — 检查 `baseSnapshot` + `pendingStates` 是否有冲突
2. `Lock(key)` — 写入 `pendingStates`，追加 journal 条目
3. `Snapshot()` / `RevertToSnapshot()` — 交易内部分回滚

### 阶段 C：CommitTx / RollbackTx

- **CommitTx**: 释放 txHash 在 `pendingStates` 中的所有锁，注册到 `pendingUnlocks`，标记 `tmpFinishedTxs[txHash] = true`
- **RollbackTx**: 恢复 stateDB 中的值，释放锁，`tmpFinishedTxs[txHash] = false`

**CommitTx/RollbackTx 只在 pendingStates 层面操作，不直接改全局锁。**

### 阶段 D：区块提交 — stateLocker.Commit(newRoot)

1. 将 `pendingStates` 的锁合并到 `stateLockManager.lockedStates`
2. `pendingUnlocks.applyTo(stateLockManager)` — 从全局锁池中删除已释放的锁
3. 合并 `tmpFinishedTxs` 到 `stateLockManager.finishedTxs`
4. 清空 pendingStates、pendingUnlocks
5. `handleLockCommit(newRoot)` — 深拷贝当前锁状态为快照，绑定到 newRoot
6. `TempLockView.OnBlockCommitted` — 清理区块中交易的临时锁，同步最新 committedWriteLocks

### 阶段 E：链上验证上锁 — lockStateWithRWSet

`VerifySimulation` 成功路径（无冲突）时调用 `lockStateWithRWSet`：

```
stateDB.SetAndLockState(key) → locker.Lock(key)
```

这把锁上到**当前区块执行**的 `stateLocker.pendingStates` 中，在区块结束时 `Commit()` 合并到全局。

**解锁时机**：`CommitOrRollbackWithProof` 调用 `stateDB.CommitTx/RollbackTx`，记录到 `pendingUnlocks`，等待下一个区块提交时 `applyTo` 释放。

### 阶段 F：锁状态同步（TempLockView → OnBlockCommitted）

`TempLockView.OnBlockCommitted` 在每个普通区块提交后执行：

1. 遍历区块中的交易，从 `tempWriteLocks/tempReadLocks` 中删除
2. 调用 `stateLockManager.GetRWLockStates()` 同步最新的 committedWriteLocks

---

## 三、交易重试机制

### 3.1 retryScheduler 数据结构

```
retryScheduler
├── retryPool: map[txHash → RetryTx]            # 待重试交易池
├── signals: map[txHash → simNum → shard → signal]  # 各分片就绪信号
├── inFlight: map[txHash → struct{}]            # 正在重试中的交易（防重入）
├── staleTxs: map[txHash → struct{}]            # 已过期交易
├── hotKeySet: map[LockKey → struct{}]          # 热点 key 集合
└── tempLockView *TempLockView                  # 引用临时锁视图
```

### 3.2 重试触发路径

```
simulate 完成
  ↓
TempLockView.TryLock (预检查)        ← impl.go:1864
  ├── 成功 → 进链上 Verification
  └── 失败 → CallForRetry → 进 retryPool
                                ↓
OnBlockCommitted (每普通区块)         ← retry_scheduler.go:310
  ├── 清理 staleTxs
  ├── 遍历 retryPool
  │   ├── inFlight true → skip（正在重试中）
  │   ├── CanLock true → Ready = true → promote
  │   └── CanLock false → Ready = false → log "retry tx blocked"
  └── 汇总 ready signals → sendReSimulationSignals
                                ↓
HandleReSimulationSignal            ← retry_scheduler.go:407
  ├── 收集所有 shard 的 ready 信号
  └── 所有 shard ready → tryToReSimulation
                                ↓
tryToReSimulation                   ← retry_scheduler.go:477
  ├── 并发调所有 related shard 的 RetryCommit RPC
  │   └── RetryCommit → TempLockView.TryLock（重新上临时锁）
  ├── 全部 locked → retry commit success
  │   → 设置 inFlight[txHash]
  │   → go startReSimulation
  └── 任一 locked false → retry commit failed
      → 对已 lock 的 shard 发 RetryCancel
                                ↓
startReSimulation                   ← impl.go:3731
  ├── getState → SimulationRequest
  │   └── nil → ClearInFlight + RetryCancel → return    ← 本次新修
  ├── 并发调所有 committee member 的 HandleSimulateRequest
  ├── 聚合结果 → 构建 SimulationCommit
  ├── 成功 → thresholdSign → Multicast(CommitSimulation)
  ├── LockConflict → CallForRetry → ClearInFlight + RetryCancel → return  ← 本次新修
  └── 其他错误 → thresholdSign → Multicast(CommitSimulation)
```

### 3.3 热点 key 链式加速

CR 交易释放 hot key 后，`chainHotKeyCR` 立即查找 `retryPool` 中需要这些 key 的交易：

1. 过滤 CR WriteSet 中的 hot key
2. 匹配需要 hot key 且 `CanLock == true` 的交易
3. 发 `HandleHotKeyRetrySignal` RPC，附着 `CRHotWritePatch`
4. 对应交易的 `RetryCommit` 中从读写集排除 hot patch key（认为即将释放）

### 3.4 retryCount=0 时的行为

不同冲突分支走向：

| 分支 | 条件 | MaxOnChainRetries=0 时 |
|------|------|----------------------|
| `conflictCallStateIndex` | 读集不一致 | `checkRetryLimitExceeded` 超限 → 直接发 Rollback vote |
| `conflictLockKeys` | 写锁冲突 | `checkRetryLimitExceeded` 超限 → 直接发 Rollback vote |
| 成功 | 无冲突 | `lockStateWithRWSet` 上链上锁 → 发 Commit vote |

---

## 四、锁释放链

```
链上锁（stateLockManager）释放链：

  CommitOrRollbackWithProof
    → stateDB.CommitTx/RollbackTx
    → pendingUnlocks.addTx(txHash)
    → closeTransaction → StaleTx
      → GarbageCollect(TempLockView)
    → (下个区块) stateLocker.Commit()
      → pendingUnlocks.applyTo(stateLockManager)
        → 从全局 lockedStates 中删除
      → handleLockCommit → 快照更新
    → (下个区块) TempLockView.OnBlockCommitted
      → 同步最新的 committedWriteLocks


TempLockView 临时锁释放链：

  正常路径：block committed → TempLockView.OnBlockCommitted
    → 遍历区块 tx → 从 tempWriteLocks 中删除
    → 同步 committedWriteLocks

  RetryCancel/StaleTx → GarbageCollect(txHash)
    → 从 tempWriteLocks/tempReadLocks/txReadWriteSets 中删除
```

---

## 五、当前已知的死锁路径（已修复）

### 路径 1：startReSimulation LockConflict → inFlight 残留

```
retry commit success
  → inFlight[txHash] = true
  → startReSimulation → HandleSimulateRequest → LockConflict
  → CallForRetry → return
  ❌ 没有 ClearInFlight → inFlight 永久 true
  ❌ 没有 RetryCancel → tempWriteLocks 残留
  → OnBlockCommitted 永远跳过该 tx
  → 其他交易被 tempWriteLocks 中的孤儿锁永久挡住
```

**修复**：LockConflict 分支在 CallForRetry 后追加 `ClearInFlight` + `RetryCancel`（`impl.go:3924-3925`）

### 路径 2：startReSimulation SimulationRequest == nil → 直接 return

```
retry commit success（在 non-origin shard 上触发）
  → startReSimulation
  → state.SimulationRequest == nil（该 shard 没有模拟请求记录）
  → 直接 return
  ❌ 没有 ClearInFlight → inFlight 永久 true
  ❌ 没有 RetryCancel → tempWriteLocks 残留
```

**修复**：`lastReq == nil` 分支追加 `ClearInFlight` + `RetryCancel`（`impl.go:3756-3757`）

### 已排除的路径

- **浅引用导致的锁冲突误判**：`GetLockerAt` Case 1 和 Case 3 的 `baseSnapshot` 从浅引用改为深拷贝 ✅

---

## 六、剩余风险

| 风险 | 影响 | 状态 |
|------|------|------|
| TempLockView 的锁只在普通区块 `OnBlockCommitted` 时清理，CR 区块提交后锁释放但 TempLockView 可能未同步 | 临时锁短暂残留，下个普通区块时修复 | 低（临时的） |
| `GetLockerAt` 快照 miss 时降级为当前全局状态 | 模拟用错 stateRoot 的锁视图 | 低（有 warn 日志）|
| `pendingUnlocks.applyTo` 依赖反向索引 `callIndex2lockedState[txHash]`，如果 journal revert 清了它，applyTo 空跑 | 锁在 `lockedStates` 残留 | 低（需 CommitTx 后再 revert journal 才会触发） |
| `inFlight` 超时保护（当前只有 ClearInFlight 修复，没有超时自动清理） | 如果 `startReSimulation` 走其他 early return 路径，inFlight 仍可能残留 | 低（所有已知 early return 已补 ClearInFlight） |

---

## 七、文件索引

| 文件 | 职责 |
|------|------|
| `state_lock_impl.go` | `lockedStates` 锁存储、`stateLockManager` 管理器、`handleLockCommit` 快照、`GetLockerAt` |
| `state_lock_entry.go` | Journal 系统：5 种 entry + snapshot/revert |
| `state_locker.go` | `stateLocker` 实例：Lock/Unlock/CommitTx/RollbackTx/Commit、`pendingUnlockCache` |
| `temp_lock_view.go` | `TempLockView`：TryLock/CanLock/OnBlockCommitted/GarbageCollect |
| `retry_scheduler.go` | `retryScheduler`：重试池、信号收集、promotion、hot key 链 |
| `impl.go` | `VerifySimulation`、`CommitOrRollbackWithProof`、`closeTransaction`、`startReSimulation`、`tryToReSimulation`、`lockStateWithRWSet` |

---

*编写于 2026-06-12*
