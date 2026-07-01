# Harmony SSC 锁与重试方案

## 概述

本系统的核心目标是**避免跨分片交易在链上才暴露出状态冲突**，通过多层次的锁机制在链下就判断交易是否可以执行，减少链上重试成本。

系统包含**三个层次**的锁管理：

```
┌─────────────────────────────────────────────────────────┐
│ Layer 3: RetryScheduler                                 │
│  跨分片协调重试信号                                      │
│  (retry_scheduler.go)                                   │
├─────────────────────────────────────────────────────────┤
│ Layer 2: TempLockView                                   │
│  链下临时锁（委员会 leader 维护）                          │
│  (temp_lock_view.go)                                    │
├─────────────────────────────────────────────────────────┤
│ Layer 1: stateLocker + stateLockManager                 │
│  链上已提交锁 + 版本化快照                                │
│  (state_lock_entry.go, state_lock_impl.go,              │
│   state_locker.go)                                      │
└─────────────────────────────────────────────────────────┘
```

---

## Layer 1: 链上锁管理（stateLocker + stateLockManager）

### 职责

保存所有**已上链**跨分片交易的状态锁信息，防止不同跨分片交易在链上执行时相互冲突。

### 文件结构

| 文件 | 职责 |
|------|------|
| `state_lock_entry.go` | Journal 条目类型（5种）、revision 快照、lockJournal 回滚机制 |
| `state_lock_impl.go`  | lockedStates 数据结构、stateLockManager 全局管理器、版本化快照 |
| `state_locker.go`     | stateLocker 实例（每个模拟交易独享）、锁操作、事务提交/回滚、pendingUnlockCache |

### 核心数据结构

#### lockedStates — 锁状态存储

```
lockedStates
├── lockedStates:           LockKey → *lockedState         (写锁正向映射)
├── rlockedStates:          LockKey → *rlockedState        (读锁正向映射)
├── callIndex2lockedState:  txHash → callIndex → key→value (写锁反向索引)
└── callIndex2rlockedState: txHash → callIndex → key→{}   (读锁反向索引)
```

**正向映射**：给定一个 LockKey，判断是否被其他交易锁定 → 冲突检测用。
**反向映射**：给定一个 txHash，找出其所有锁 → 事务清理用。

#### lockedState / rlockedState — 锁状态描述符

- **lockedState**: `lockedBy` 写锁持有者（`common.Hash{}` 表示空闲），支持重入
- **rlockedState**: `lockedBy []common.Hash` 读锁可被多个交易同时持有

#### stateLockManager — 全局锁管理器

```
stateLockManager
├── lockedStates: *lockedStates          ← 全局锁状态
├── finishedTxs:  map[txHash]bool        ← 已完成的交易
├── snapshots:    map[stateRoot]*snapshot ← 版本化快照
├── currentRoot:  common.Hash            ← 当前区块的 stateRoot
├── maxSnapshots: int                    ← LRU 上限（默认 5）
├── snapshotTTL:  time.Duration          ← 1 小时
└── tempLockView: *TempLockView          ← 关联的临时锁视图
```

#### stateLocker — 每个模拟交易独享

```
stateLocker (per simulation instance)
├── baseSnapshot: *lockedStatesSnapshot  ← 该 stateRoot 的只读锁快照（不可变）
├── pendingStates: *lockedStates         ← 本实例隔离的 pending 状态
├── pendingUnlocks: *pendingUnlockCache  ← 待解锁交易跟踪
├── tmpFinishedTxs: map[txHash]bool      ← 本次模拟中完成的交易
├── journal: *lockJournal                ← 支持快照回滚
└── stateLockManager (共享引用)
```

### 数据流

```
GetLockerAt(stateRoot) ──────────────► stateLocker
                                         │
模拟交易执行:                               │
  Lockable() / RLockable() ── 冲突检测 ────┤
  Lock() / RLock() ──────── 写入 pending ──┤
  Snapshot() / RevertToSnapshot() ─── 回滚 ─┤
                                         │
CommitTx(txHash) ────────── 释放 pending ──┤
  (记录到 pendingUnlocks)                   │
                                         ▼
Commit(newRoot) ───────────────────────► stateLockManager
  1. 合并 pendingStates → lockedStates      │
  2. pendingUnlocks.applyTo() → 删除已释放锁 │
  3. handleLockCommit(newRoot) → 创建快照 ──┘
```

### Journal + Revision 回滚机制

每步 Lock/Unlock 操作都追加一个 `journalEntry` 到 `lockJournal`：
- **lockEntry**: Lock 操作，revert 时从 pendingStates 删除
- **rlockEntry**: RLock 操作，revert 时从 pendingStates 删除
- **unlockEntry**: unlock 操作，revert 时加回 pendingStates
- **unlockRLockEntry**: unlockRLock 操作，revert 时加回 pendingStates
- **finishTxEntry**: CommitTx/RollbackTx 完成标记

`Snapshot()` 记录当前 journal 位置，`RevertToSnapshot()` 反向遍历 journal 逐条 revert。

### 二阶段解锁设计

解锁分两步进行，因为锁操作被隔离在 stateLocker 的 pendingStates 中：

```
阶段 1（CommitTx/RollbackTx 时）：
  pendingStates 中释放锁 + 记录到 pendingUnlocks

阶段 2（Commit(newRoot) 合并全局时）：
  pendingUnlocks.applyTo() 从 stateLockManager 全局状态删除已释放锁
```

好处：全局锁状态只在 `Commit()` 时原子变更，中间状态不会泄漏。

---

## Layer 2: 链下临时锁（TempLockView）

### 职责

由 committee leader 维护，管理**还未正式上链**的模拟交易对哪些状态上了临时锁，
避免上了链才判断出交易冲突需要重试。

### 核心思想

TempLockView 维护了两个层级的锁视图：

```
当前锁状态视图（从下到上优先）：
  1. committedWriteLocks / committedReadLocks ← 从 stateLockManager 同步的已提交锁
  2. tempWriteLocks / tempReadLocks             ← 本批次临时锁（TryLock 产生）
  3. txReadWriteSets                            ← 每个交易已登记的读写集
```

**冲突检测规则**：

| 当前操作 | 已提交写锁 | 已提交读锁 | 临时写锁 | 临时读锁 |
|---------|-----------|-----------|---------|---------|
| 写 | ❌ 冲突 | ✅ 允许 | ❌ 冲突 | ✅ 允许 |
| 读 | ✅ 允许 | ✅ 允许 | ❌ 冲突(1) | ✅ 允许 |

(1) 读与临时写冲突：因为本批次前面的交易写了此 key，当前交易读它会看到「未提交」的不一致状态。

### 生命周期

```
新交易到达 → TryLock(txHash, reads, writes) ── 检查冲突
              ├── 无冲突 → 注册临时锁 → 可以开始模拟
              └── 有冲突 → 返回 false → 交易等待重试

区块提交 → OnBlockCommitted(block)
            ├── 清理区块中所有交易的临时锁
            ├── 从 stateLockManager 同步最新已提交锁
            └── 通知 retryScheduler 判断哪些交易可以重试

Stale 交易 → GarbageCollect(txHash) ── 清理过期交易
```

### 死锁预防

调用方必须保证 `writes` 数组按全局顺序排序（如按 LockKey 的地址+key 排序）。
TryLock 按 writes 迭代检查，若所有交易都按相同顺序获取锁，就不会产生循环等待。

---

## Layer 3: 跨分片重试调度（RetryScheduler）

### 职责

在 committee leader 之间协调，根据跨分片交易涉及的相关状态，
判断其链上锁和链下临时锁是否**全部完成解锁**。
解锁全部完成后再触发跨分片交易的再次模拟。

### 核心数据结构

```
retryScheduler
├── retryPool: map[txHash]*RetryTx           ← 待重试交易池
├── staleTxs:  map[txHash]struct{}           ← 已过期的交易（下次清理）
├── signals:   map[txHash]map[simulationNum] ← 各分片就绪信号
                 └── map[shardID]*ReSimulationSignal
└── tempLockView: *TempLockView              ← 引用临时锁视图
```

### 重试流程

```
                  ┌──────────────────────────────┐
                  │  交易在链上执行锁失败            │
                  │  → 调用 CallForRetry(tx)       │
                  │    向所有 relatedShards 广播    │
                  └──────────────┬───────────────┘
                                 ▼
                  ┌──────────────────────────────┐
                  │ AddToRetry(tx) → retryPool    │
                  │ 各分片 leader 收到并加入重试池   │
                  └──────────────┬───────────────┘
                                 ▼
              区块提交触发 OnBlockCommitted(block)
              1. tempLockView.OnBlockCommitted → 清理临时锁
              2. 对 retryPool 中每个 tx：
                 CanLock(txHash, readSet, writeSet) ← 检查临时锁
                 就绪? → 生成 Ready=true 信号
                 未就绪? → 生成 Ready=false 信号
              3. sendReSimulationSignals(signals)
                 发送到 origin shard 的 leader
                                 │
                                 ▼
               ┌──────────────────────────────┐
               │ HandleReSimulationSignal      │
               │ 只在 origin shard 的 leader 处理    │
               │                                 │
               │ for each signal:                 │
               │   setSignal(fromShard, signal)   │
               │   if all shards Ready:           │
               │     → tryToReSimulation(tx)    │
               └────────────────┬───────────────┘
                                ▼
               ┌──────────────────────────────┐
               │ tryToReSimulation             │
               │ 1. 向所有 relatedShards       │
               │    调用 RetryCommit(txHash)    │
               │    → tempLockView.TryLock()   │
               │                                │
               │ 2. 全部成功?                   │
               │    → startReSimulation()       │
               │    清理 signals + retryPool    │
               │                                │
               │ 3. 部分失败?                   │
               │    → RetryCancel() 回滚        │
               │     已上锁的分片                │
               │    重置发送失败分片的信号为 false│
               └────────────────────────────────┘
```

### 关键设计点

**全分片就绪才能重试**：
跨分片交易涉及多个分片的状态。重试时，所有 relatedShards 都必须确认该交易的锁已经全部释放，且 TempLockView 中没有冲突，才能开始重新模拟。

**两阶段提交式重试**：
1. 第一阶段：收集全部分片的 ReSimulationSignal
2. 第二阶段：向所有分片执行 RetryCommit（实质是 TryLock 写入临时锁）
3. 只有全部成功才真正 startReSimulation；部分失败则回滚已成功部分

---

## 全局数据流（完整流程）

```
  ┌─────────────────────────────────────────────────────────────────────┐
  │ 0. 新区块提交                                                       │
  │    stateDB.Commit() → stateLocker.Commit(newRoot)                    │
  │      → pendingStates 合并到 stateLockManager                         │
  │      → handleLockCommit(newRoot)：创建该 stateRoot 的快照             │
  └────────────────────────────────┬────────────────────────────────────┘
                                   ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │ 1. 新模拟交易到达                                                    │
  │    GetLockerAt(stateRoot) → 拿到匹配 stateRoot 的 stateLocker        │
  │      → baseSnapshot = 该 stateRoot 的锁快照（只读）                   │
  │      → pendingStates = 空隔离状态                                     │
  └────────────────────────────────┬────────────────────────────────────┘
                                   ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │ 2. TempLockView.TryLock(txHash, reads, writes)                      │
  │    (委员会 leader 的链下判断)                                        │
  │    → 检查 committed 写锁 + temp 写锁                                 │
  │    → 通过则注册临时锁 → 进入模拟队列                                   │
  └────────────────────────────────┬────────────────────────────────────┘
                                   ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │ 3. 模拟执行（worker 从优先级队列拉取）                                │
  │    processSimulationTask → startSimulateCXTransaction                │
  │      → 在 stateLocker 中 Lockable() 检查                             │
  │      → Lock() / RLock() 写入 pendingStates                           │
  │      → Snapshot() / RevertToSnapshot() 处理子调用                     │
  │      → CommitTx() / RollbackTx()                                     │
  └────────────────────────────────┬────────────────────────────────────┘
                                   ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │ 4. 模拟结果上链                                                     │
  │    stateDB.Commit() → stateLocker.Commit(newRoot)                    │
  │      → 合并 pendingStates 到 stateLockManager                        │
  │      → 应用 pendingUnlocks（释放已提交事务的锁）                      │
  │      → 创建 newRoot 的快照                                           │
  │      → tempLockView.OnBlockCommitted()                             │
  │         清理临时锁，从 stateLockManager 同步锁状态                     │
  └────────────────────────────────┬────────────────────────────────────┘
                                   ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │ 5. 失败交易重试                                                      │
  │    CallForRetry(tx) → 各分片 AddToRetry                              │
  │    OnBlockCommitted → CanLock 检查 → sendReSimulationSignals         │
  │    tryToReSimulation → 两阶段完成 → startReSimulation                │
  └─────────────────────────────────────────────────────────────────────┘
```

---

## 关键设计决策

### 1. 为什么锁要版本化（snapshot by stateRoot）？

**问题**：模拟交易在 stateRoot A 上执行时，检查了锁状态并锁定了某些 key。
但如果此时另一个区块 B 提交了，全局锁状态就变了。
回到 stateRoot A 的模拟交易再用新的锁状态做冲突判断——**就错了**。

**解法**：每个 `stateDB.Commit()` 时，同步对锁状态做快照（`handleLockCommit`）。
模拟交易用 `GetLockerAt(stateRoot)` 拿到与 stateRoot 匹配的锁上下文。
快照用 LRU + TTL 自动清理，不造成无限增长。

### 2. 为什么 Lock/Unlock 要延迟到 Commit？

**问题**：多个模拟交易共享同一个 `stateLockManager` 的引用。
如果 Lock 直接写入全局 `lockedStates`，其他并行模拟会立即看到未提交的锁——造成误判。

**解法**：`stateLocker` 的 Lock/Unlock 只操作 `pendingStates`（隔离）。
`CommitTx` 时释放但记录到 `pendingUnlocks`。
`Commit(newRoot)` 时一次性合并到全局，再通过 `applyTo` 释放已完成的锁。

### 3. 为什么 TempLockView 不从 stateLocker 直接获取锁状态？

**职责分离**：
- `stateLocker` / `stateLockManager` — 链上已提交锁 + 模拟实例的隔离锁
- `TempLockView` — 链下临时锁，由 committee leader 维护，仅用于批前判断不直接操作 stateLocker

TempLockView 从 `stateLockManager.GetRWLockStates()` 同步已提交锁，
然后叠加本批次的临时锁做冲突检测——**读为主，不写回链上状态**。

### 4. 为什么重试需要全分片 Ready 信号？

跨分片交易涉及多个分片的状态，任一 shard 未就绪（锁未释放）都可能导致重试再次失败。
全分片信号 + 两阶段提交的 TryLock 确保重试时**所有相关锁可获取**。

---

## 潜在问题 / TODO

### 已知问题

1. **addRLockedState 的 bug**：`state_lock_impl.go` 第 121 行 `addRLockedState` 中，
   读锁的反向索引被写入了 `callIndex2lockedState`（写锁映射），而非 `callIndex2rlockedState`。
   虽然写的是 value，存的是 struct{}{}，不会产生运行时错误，但不一致反映了历史迁移问题。

2. **锁超时处理**：`checkLockTime` 目前只打 WARN 日志 + 标记 `hasTimeout=true`，
   没有自动解锁机制。如果某个模拟交易崩溃了，它的锁会一直留在全局状态中，
   直到 snapshot 被 TTL 清除或被其他交易手动恢复。

3. **快照降级**：`GetLockerAt` 找不到 stateRoot 的快照时，静默降级到当前全局状态。
   这在快照被 TTL 清除后可能导致不一致。

4. **tempReadLocks 不用于 GC**：`tempReadLocks` 在 `OnBlockCommitted` 中只做清理，
   不参与冲突检测（冲突检测只检查 writeLocks）。这意味着读锁的临时信息仅作调试用。

5. **无日志清理**：`checkLockTime` 每 10 秒打印一次所有锁状态，
   高负载下日志可能过多。

### 待优化

- [ ] 锁超时自动解锁
- [ ] 快照降级时是否应该暂停模拟（而不是静默降级）
- [ ] 更精细的 tempReadLock 冲突检测
- [ ] addRLockedState 的反向索引清理
- [ ] P2P 网络消息的幂等性保证（防止重试信号重复）
