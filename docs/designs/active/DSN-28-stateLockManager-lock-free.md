# [DSN-28] stateLockManager 无锁重构 — 版本化 MVCC + per-tx 锁

> **版本**：v1（2026-07-13）
> **状态**：设计稿
> **关联文档**：`DSN-27-tlv-mutex-contention.md`（TLV sync.Map 化）、`DSN-25-retrycommit-state-cache.md`（stateDB 缓存）

---

## 1. Problem Statement

### 1.1 时序数据：CRTx 49% 慢，3.2% 超过 50ms

2026-07-13 实验（RATE=100, delay=10, 4 shards, 1 SSC contract, 9040 Leader）：

| 类型 | 数量 | apply avg | P50 | P90 | P99 | max | >50ms |
|:----|:---:|:---------:|:---:|:---:|:---:|:---:|:-----:|
| **SimTx** | 4,466 | 7.07ms | 5.04ms | 10.30ms | 38.77ms | 579ms | 0.8% |
| **CRTx** | 3,965 | **10.39ms** | **0.94ms** | **25.47ms** | **133.38ms** | **815ms** | **3.2%** 🔴 |

CRTx 的 P50 只有 0.94ms（正常，`CommitOrRollbackWithProof` 约 0.4ms），但 P99 高达 133ms，最高 815ms。而 SimTx 的 P50=5.04ms（`VerifySimulation` 主导），P99=38.77ms，减速幅度只有 CRTx 的 1/3。

### 1.2 根因：stateLocker.txLock 全局互斥锁

CRTx 内部调用链：

```
commitTransaction → SimulateCXTransaction → SSCVM(Precompiled)
  → CxtCommitOrRollbackAddr.RunWriteCapable
    → Committer.CommitOrRollbackWithProof
      → stateDB.CommitTx(txHash)  ← 每笔 CRTx 触发
        → s.txLock.Lock()         ← 全局锁！
        → 操作 s.pendingStates     ← 实际只操作自己的 txHash
        → s.txLock.Unlock()
```

`stateLocker.txLock` 是每个 locker 实例的**全局互斥锁**。Leader 的 `CommitSSCTransactions` 串行循环中，每笔 tx 的 `commitTransaction` 都会竞争这把锁。在没有并发写的情况下，这把锁的实际作用只是保护 `pendingStates` 这个 map——而 map 的操作是按 txHash 分区的。

### 1.3 stateLockManager.mu 的问题

当前 `stateLockManager.mu` 保护：

| 保护对象 | 写入频率 | 写入者 |
|:---------|:--------|:------|
| `lockedStates` map | ~500 次/块 | `locker.Commit()` 合并 pending |
| `finishedTxs` map | ~500 次/块 | `locker.Commit()` 标记完成 |
| `snapshots` map | 1 次/块 | `handleLockCommit` 创建快照 |
| `currentRoot` | 1 次/块 | `handleLockCommit` 更新 |
| `lockStartBlock` | per-lock | 分布式写入 |
| `snapshotMeta` | per-get | `GetLockerAt` 更新访问计数 |

**问题**：所有写入都需要 `mu.Lock()`，而 `mu` 也保护读操作（`GetLockerAt`、`GetRWLockStates`、`CheckLock` 等）。读多写少的场景下，这把锁造成了不必要的串行化。

### 1.4 当前架构

```
stateLockManager（全局单例）
├── mu sync.RWMutex           ← Hot! 全部操作抢这把锁
├── lockedStates map          ← 全局锁状态
├── finishedTxs map           ← 已完成的 tx
├── snapshots map             ← 历史快照（N 个版本）
└── currentRoot               ← 当前 stateRoot

stateLocker（短期实例，每次 GetLockerAt 创建）
├── txLock sync.RWMutex       ← Hot! 保护 pendingStates
├── pendingStates             ← 本 locker 待提交的锁变更
├── baseSnapshot              ← 创建时的 lock state（浅拷贝，指向 manager）
└── *stateLockManager         ← 嵌入引用
```

**双锁热点**：`stateLockManager.mu`（写多读多）+ `stateLocker.txLock`（串行阻塞）。

---

## 2. 设计方案

### 2.1 核心策略

1. **`stateLocker.txLock` 替换为 per-txHash 锁**：`CommitTx(txHash)` 只等相同 txHash 的操作，不同 tx 互不阻塞。
2. **`stateLockManager.mu` 替换为 `sync.Map` + `atomic.Value`**：所有字段改为 Go 无锁并发原语，不保留全局锁。
3. **版本化 MVCC**：`handleLockCommit` 不再深拷贝 + `clear()`，只做 `version++`。旧数据靠版本号过滤，不再删除。
4. **只保留 2 个版本**：当前版本（给新模拟用）+ 上一个版本（给 Verify 用）。不再维护 N 个历史快照。

### 2.2 关键洞察：所有 GetLockerAt 请求的都是当前版本

通过代码追踪，所有 `GetLockerAt` 调用点：

| 调用点 | 传入 root | 是否当前版本？ |
|:-------|:----------|:--------------:|
| `HandleSimulateRequest` → `bc.StateAt(header.Root())` | `CurrentHeader().Root()` | ✅ 当前 |
| `HandleCXTCall` → `bc.StateAt(header.Root())` | 同上 | ✅ 当前 |
| `processSimulationTask` → `bc.StateAt(header.Root())` | 同上 | ✅ 当前 |
| `getStateDB` → `bc.State()` | `CurrentHeader().Root()` | ✅ 当前 |
| `getCachedStateDB` | OnBlockCommitted 时缓存的 stateDB | ✅ 刚提交 |
| `GetLocker()` → `GetLockerAt(currentRoot)` | `currentRoot` | ✅ 当前 |

**结论：2 个版本（当前 + 上一个）足够支撑所有场景。** 历史版本快照机制（`snapshots map`）可以去除。

---

## 3. 改造详情

### 3.1 新的 stateLockManager

```go
type stateLockManager struct {
    // 全部无锁化
    lockedStates   sync.Map        // LockKey → *lockedState{lockedBy, version}
    finishedTxs    sync.Map        // common.Hash → bool
    lockStartBlock sync.Map        // LockKey → uint64（用于锁时长统计）

    // 版本号（atomic 递增）
    version        atomic.Uint64   // 当前全局版本
    
    // 版本边界（替代快照 map）
    prevVersion    uint64          // 上一个版本（Verify 用）
    prevRoot       common.Hash     // 上一个版本对应的 stateRoot

    // 当前状态（atomic 读写）
    currentVersion atomic.Uint64   // 与 version 同步
    currentRoot    atomic.Value    // common.Hash

    // 引用（不变）
    sscService     *sscService
    tempLockView   *TempLockView
}
```

**移除**的字段：

| 原字段 | 替代方案 |
|:-------|:--------|
| `mu sync.RWMutex` | **移除** |
| `snapshots map[Hash]*lockedStatesSnapshot` | **移除**，2 版本够用 |
| `snapshotMeta map[Hash]*snapshotMetadata` | **移除** |
| `maxSnapshots int` | **移除** |
| `snapshotTTL time.Duration` | **移除** |
| `currentBlockNum uint64` | 保留但用 atomic |
| `commitCnt uint64` | 保留但用 atomic |

### 3.2 新的 lockedState

```go
type lockedState struct {
    lockedBy common.Hash   // 锁持有者的 txHash
    version  uint64        // 创建时的版本号 ← 新增！
}

type rlockedState struct {
    locked   bool
    lockedBy []common.Hash
    version  uint64         // 创建时的版本号 ← 新增！
}
```

### 3.3 handleLockCommit — 版本更新

**改前**（深拷贝 + clear，15 行代码 + 全局锁）：

```go
func (s *stateLockManager) handleLockCommit(newRoot common.Hash) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    snapshot := deepCopy(s.lockedStates)   // O(n) 深拷贝
    s.snapshots[newRoot] = snapshot
    s.snapshotMeta[newRoot] = &snapshotMetadata{...}
    s.currentRoot = newRoot
    s.lockedStates.clear()                 // O(n) 清空
    s.commitCnt++
}
```

**改后**（3 行代码，无锁）：

```go
func (s *stateLockManager) handleLockCommit(newRoot common.Hash) error {
    newVersion := s.version.Add(1)         // 版本递增
    s.prevVersion = newVersion - 1         // 上一个版本号存档（Verify 用）
    s.prevRoot = s.currentRoot.Swap(newRoot).(common.Hash)
    // lockedStates 不清理，不复制
}
```

### 3.4 GetLockerAt — 创建 stateLocker

**改前**（3 个 Case + 深拷贝引用 + `mu.RLock`）：

```go
func (s *stateLockManager) GetLockerAt(root common.Hash) *stateLocker {
    s.mu.RLock()
    defer s.mu.RUnlock()
    
    if root == currentRoot {
        // 浅拷贝引用 s.lockedStates.lockedStates → 需要 RLock 保护
        return &stateLocker{baseSnapshot: s.lockedStates, ...}
    }
    if snapshot, exists := s.snapshots[root]; exists {
        return &stateLocker{baseSnapshot: snapshot, ...}
    }
    // fallback 到当前
}
```

**改后**（无锁，无深拷贝，无快照 map）：

```go
func (s *stateLockManager) GetLockerAt(root common.Hash) *stateLocker {
    curRoot := s.currentRoot.Load().(common.Hash)
    curVer := s.currentVersion.Load()
    
    baseVersion := curVer
    if root != curRoot && root != (common.Hash{}) && root == s.prevRoot {
        baseVersion = s.prevVersion          // 历史版本请求 → 用上一个版本过滤
    }
    // 其他非当前非上版本 root → 降级为当前版本（write warn 日志）
    
    return &stateLocker{
        baseVersion: baseVersion,            // 只保存版本号，不保存数据快照
        root:        root,
        stateLockManager: s,
        pendingStates: newLockedStates(),
        ...
    }
}
```

### 3.5 CheckLock — 带版本过滤

```go
func (s *stateLocker) CheckLock(key api.LockKey, txHash common.Hash) error {
    // 1. 查全局 lockedStates（sync.Map，无锁并发安全）
    if v, ok := s.lockedStates.Load(key); ok {
        st := v.(*lockedState)
        if st.lockedBy != empty && st.lockedBy != txHash && st.version >= s.baseVersion {
            return ErrLockConflict_OnChain
        }
    }

    // 2. 查本 locker 的 pendingStates
    if v, ok := s.pendingStates.lockedStates.Load(key); ok {
        st := v.(*lockedState)
        if st.lockedBy != empty && st.lockedBy != txHash {
            return ErrLockConflict_OffChain
        }
    }
    return nil
}
```

### 3.6 stateLocker 的 per-tx 锁

```go
type stateLocker struct {
    txLocksMu   sync.Mutex                          // 仅保护 txLocks map 的分配
    txLocks     map[common.Hash]*sync.Mutex          // txHash → per-tx 锁

    baseVersion   uint64                             // 创建时的版本号
    root          common.Hash
    stateDB       api.StateDB
    *stateLockManager                                // 嵌入

    // pendingStates 改用 sync.Map（本 locker 私有，同一 txHash 的锁不冲突）
    pendingStates *lockedStates                      // LockKey → *lockedState
    
    // 以下字段不变
    pendingUnlocks *pendingUnlockCache
    tmpFinishedTxs map[common.Hash]bool
    journal        *lockJournal
    validRevisions []revision
    nextRevisionId int
}

// getTxLock 获取或创建一笔 tx 的私有锁
func (s *stateLocker) getTxLock(txHash common.Hash) *sync.Mutex {
    s.txLocksMu.Lock()
    lk, ok := s.txLocks[txHash]
    if !ok {
        lk = &sync.Mutex{}
        s.txLocks[txHash] = lk
    }
    s.txLocksMu.Unlock()
    return lk
}

func (s *stateLocker) CommitTx(txHash common.Hash) error {
    lk := s.getTxLock(txHash)
    lk.Lock()
    defer lk.Unlock()
    // ... 和原来一样操作 callIndex2lockedState[txHash]
}
```

Lock 和 Unlock 操作同理只锁自己的 txHash 锁。

### 3.7 Commit（locker.Commit：pending → 全局）

**改前**（`txLock + manager.mu` 双重锁）：

```go
func (s *stateLocker) Commit(newRoot common.Hash) error {
    s.txLock.Lock()                       // 自己的 pending 锁
    s.stateLockManager.mu.Lock()          // 全局锁
    
    // 合并 pendingStates → manager.lockedStates
    for key, ps := range s.pendingStates.lockedStates {
        s.stateLockManager.lockedStates.lockedStates[key] = ps
    }
    // 合并 pendingUnlocks → applyTo(manager)
    s.pendingUnlocks.applyTo(s.stateLockManager)
    
    s.stateLockManager.mu.Unlock()
    s.txLock.Unlock()
    return s.handleLockCommit(newRoot)
}
```

**改后**（无锁，`sync.Map.Store` 是 per-key 原子）：

```go
func (s *stateLocker) Commit(newRoot common.Hash) error {
    // 只锁自己的 pending（不同 txHash 不冲突）
    s.txLocksMu.Lock()
    // ... (获取所有需合并的 key)
    s.txLocksMu.Unlock()
    
    // 合并 pendingStates → manager.lockedStates（sync.Map，无锁）
    s.pendingStates.lockedStates.Range(func(key, value interface{}) bool {
        s.lockedStates.Store(key, value)
        return true
    })
    // 合并 pendingUnlocks：用 sync.Map.Delete 逐 key 释放
    s.pendingUnlocks.applyTo(s.stateLockManager)
    
    return s.handleLockCommit(newRoot)
}
```

### 3.8 GetRWLockStates（被 TempLockView 调用）

**改前**（`mu.RLock` + 整表复制）：

```go
func (s *stateLockManager) GetRWLockStates() (...) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    // 全表复制...
}
```

**改后**（`sync.Map.Range`，无锁）：

```go
func (s *stateLockManager) GetRWLockStates() (...) {
    writeLockedStates := make(map[api.LockKey]*lockedState)
    s.lockedStates.Range(func(k, v interface{}) bool {
        writeLockedStates[k.(api.LockKey)] = v.(*lockedState)
        return true
    })
    return writeLockedStates, nil
}
```

---

## 4. 安全分析

### 4.1 并发安全

| 操作 | 保护机制 | 冲突可能性 |
|:-----|:--------|:----------|
| `lockedStates.Store(key, val)` | `sync.Map` atomic | 不同 key 不冲突 |
| `lockedStates.Load(key)` | `sync.Map` atomic | 读与写不冲突 |
| `version.Add(1)` | CPU 原子指令 | 无冲突 |
| `currentRoot.Store/.Load` | `atomic.Value` | 无冲突 |
| `txLocks[txHash].Lock()` | `sync.Mutex` per-tx | 不同 txHash 不冲突 |
| `pendingStates.lockedStates.Range` | 本 locker 独占 | 仅 Commit 时调用，独占 |

**无死锁风险**：不存在锁顺序依赖——每个操作只拿自己 txHash 的锁，不按顺序拿多个锁。

### 4.2 正确性边界

**场景：lockerA.Commit() 与 lockerB.CheckLock() 并发**

```
lockerA:
  pendingStates 中有 key=C (lockedBy=txA, version=V+1)
  Commit(): lockedStates.Store(C, lockedState{txA, V+1})

lockerB:
  baseVersion = V
  CheckLock(C): 从 lockedStates.Load(C)
    → 有可能读到 lockerA 刚写入的 C (version=V+1)
    → version=V+1 >= baseVersion=V → 冲突！
```

这是正确的：key=C 确实被 txA 锁了，lockerB 理应检测到冲突。**新版本不掩盖冲突，只会暴露旧的已释放状态。**

**场景：lockerA 释放锁，lockerB 看到已释放**

```
lockerA.CommitTx(txA): pendingStates 中删除 C
lockerA.Commit(): lockedStates.Store(C, lockedState{empty, V+1})

lockerB.CheckLock(C): 
  → lockedBy=empty → 无冲突 ✓
```

### 4.3 锁释放后的条目清理

不再 `clear()`。释放的锁条目：

```go
type lockedState struct {
    lockedBy common.Hash    // = empty 表示已释放
    version  uint64         // 释放时的版本
}
```

这些条目永久存在于 `sync.Map` 中。**条目数上限 ≈ 历史上出现过的所有不同 LockKey 总数。** 每笔 tx 的 write set 大小 ≈ 1-10 个 key，500 tx/块试验中总 key 数 ≤ 万级，内存开销可忽略。

`lockStartBlock` 同理，只追加不清理。

---

## 5. 影响范围

| 文件 | 改动 |
|:-----|:-----|
| `ssc/state_lock_impl.go` | 重写 `stateLockManager`，移除 `mu`，`sync.Map` 化，版本化 |
| `ssc/state_locker.go` | `txLock` → per-tx 锁，`baseSnapshot` → `baseVersion` |
| `ssc/lockedStates.go` | 内部 `map` 改为 `sync.Map` |
| `ssc/pendingUnlockCache.go` | `applyTo` 改用 `sync.Map.Delete` |
| `ssc/statistics.go` | 移除快照统计，改为版本统计 |
| `ssc/temp_lock_view.go` | `GetRWLockStates` 调用方适配 |

---

## 6. 验证标准

| # | 条件 | 验证方式 |
|:-|:-----|:--------|
| 1 | `GetLockerAt` 历史请求 = 0 次 | `grep "historical state locker"` 为空 |
| 2 | `snapshot not found` 日志 = 0 条 | 改造后已无快照概念 |
| 3 | CRTx P90 下降 > 50% | 从 ~25ms 降到 ~1ms |
| 4 | CRTx >50ms 比例降到 < 0.1% | 从 3.2% 下降 |
| 5 | 提交率不降 | 对比改前 baseline |
| 6 | 无 data race | `go test -race` 通过 |

---

## 7. 未解决项

| # | 问题 | 状态 |
|:-|:-----|:-----|
| 1 | `commitCnt` 是否还需使用？ | 保留 atomic 版本 |
| 2 | `lockStartBlock` 是否还需使用？ | 保留作为超时锁检测 |
| 3 | cleanSnapshot goroutine 是否去除？ | 无快照后去除 |
| 4 | `TempLockView.OnBlockCommitted` 读取锁状态适配 | 改为 `sync.Map.Range` |
