# [DSN-27] TempLockView 全局 Mutex 争抢优化 — 全部 sync.Map 化

> **版本**：v3（2026-07-11）
> **状态**：设计完成
> **关联文档**：`DSN-24-unified-lock-check.md`（TLV 三层仲裁）、`../archived/DSN-26-patchpool-dag-tlv-extension.md`（DAG→Phase 1b）

---

## 1. Problem Statement

### 1.1 Timing 数据：14,714 次 >100ms 的 TryLock

2026-07-11 实验（RATE=100, delay=10, 4 shards, 10,000 txs）：

| 阶段 | 调用次数 | P50 | P90 | P99 | Max |
|:----|:-------:|:---:|:---:|:---:|:---:|
| RetryCommit 总耗时 | 71,314 | 13ms | 474ms | 1.5s | 3.0s |
| **TLV TryLock (仅慢调用)** | **14,714** | **215ms** | **504ms** | **1.0s** | — |

### 1.2 根因：99.9% 的时间在等全局 Mutex

TLV 慢调用的内部耗时拆解：

```
TLV TryLockWithPriority 慢调用（>100ms = 14,714 次）：
┌──────────────────────────────────────────────┐
│  v.mu.Lock() 等待时间    P50=215ms  P90=504ms │ ← 99.9% 的耗时
│  实际锁检查逻辑          avg=0.16ms            │ ← 0.1%
│  其中 67.7% 的慢调用     写 key 数量 = 0       │ ← 纯排队，没工作
└──────────────────────────────────────────────┘
```

**根因：** 单把 `v.mu` 串行化所有操作，而不同的 tx 操作不同的 key 时本可以完全并行。

### 1.3 `v.mu` 保护的数据结构

```go
type TempLockView struct {
    mu               sync.RWMutex                   // ← 全局瓶颈
    tempWriteLocks   map[api.LockKey]tempLockEntry  // 每个 key 一个条目
    tempReadLocks    map[api.LockKey][]common.Hash   // 每个 key 一个持有者列表
    txReadWriteSets  map[common.Hash]*RWKeySet       // 每笔 tx 一个条目
    woundedTxs       map[common.Hash]struct{}        // 每笔 tx 一个标记
    stateLockManager *stateLockManager
}
```

`v.mu` 保护了四种不同粒度的数据。其中 per-tx 的数据（`txReadWriteSets`, `woundedTxs`）天然无冲突——不同 tx 操作不同的 key（txHash）。per-key 的数据（`tempWriteLocks`, `tempReadLocks`）才是真正的冲突点。

---

## 2. 设计方案

### 2.1 核心策略

**全部用 `sync.Map` 替代 `map + sync.Mutex`，移除全局 `v.mu`。**

| 原字段 | 保护方式 | 问题 | 新方案 |
|:-------|:--------|:-----|:-------|
| `txReadWriteSets` | `v.mu` | per-tx 数据无冲突 | `sync.Map` |
| `woundedTxs` | `v.mu` | per-tx 数据无冲突 | `sync.Map` |
| `tempWriteLocks` | `v.mu` | per-key 数据，需原子获取 | `sync.Map` + `LoadOrStore` |
| `tempReadLocks` | `v.mu` | per-key 数据，数组遍历慢 | **两层 `sync.Map`** |
| `v.mu` 自身 | — | 全局串行化瓶颈 | **移除** |

**不保留任何全局锁。** 不存在跨 key 原子性需求——即使 key_A 拿到但 key_B 被抢了，只需回滚已拿的 key，后续 stateDB CheckLock / ForceSimulation 会兜底。

### 2.2 新 TempLockView 结构

```go
type TempLockView struct {
    // Per-Key: sync.Map（lock-free 并发，不同 key 不冲突）
    tempWriteLocks sync.Map // key: api.LockKey, value: tempLockEntry
    // 两层 sync.Map: LockKey → readHolderMap
    tempReadLocks  sync.Map // key: api.LockKey, value: *readHolderMap

    // Per-Tx: sync.Map（lock-free 并发，不同 tx 不冲突）
    txReadWriteSets sync.Map // key: common.Hash, value: *RWKeySet
    woundedTxs      sync.Map // key: common.Hash, value: struct{}{}

    stateLockManager *stateLockManager
}

// readHolderMap 是内层 sync.Map，存该 key 的所有读锁持有者
type readHolderMap struct {
    sync.Map // key: common.Hash, value: struct{}{}
}

func NewTempLockView(manager *stateLockManager) *TempLockView {
    return &TempLockView{
        stateLockManager: manager,
    }
}
```

### 2.3 TryLockWithPriority 新逻辑

```go
func (v *TempLockView) TryLockWithPriority(txHash common.Hash, priority api.Priority,
    reads []api.LockKey, writes []api.LockKey) (locked bool, wounded bool) {

    // ── Phase 1: 逐个原子获取写锁 ──
    var acquiredWrites []api.LockKey
    for _, key := range writes {
        entry := tempLockEntry{Holder: txHash, Priority: priority}
        existing, loaded := v.tempWriteLocks.LoadOrStore(key, entry)
        if !loaded {
            // ✅ 成功获取（key 原来为空）
            acquiredWrites = append(acquiredWrites, key)
            continue
        }
        // key 已被占
        existingEntry := existing.(tempLockEntry)
        if bytes.Equal(existingEntry.Holder.Bytes(), txHash.Bytes()) {
            // 自己占着，不算冲突
            continue
        }
        if v.canWound(existingEntry, priority, txHash) {
            // ⚔️ 可以 Wound → 直接替换
            v.woundedTxs.Store(existingEntry.Holder, struct{}{})
            v.tempWriteLocks.Store(key, entry)
            acquiredWrites = append(acquiredWrites, key)
            continue
        }
        // ❌ 抢不过 → 回滚已拿的写锁
        for _, k := range acquiredWrites {
            v.tempWriteLocks.CompareAndDelete(k, tempLockEntry{Holder: txHash, Priority: priority})
        }
        return false, false
    }

    // ── Phase 2: 检查+注册读锁 ──
    for _, key := range reads {
        // 检查写锁（key 是否被别的 tx 写锁着）
        if existing, loaded := v.tempWriteLocks.Load(key); loaded {
            existingEntry := existing.(tempLockEntry)
            if !bytes.Equal(existingEntry.Holder.Bytes(), txHash.Bytes()) {
                if !v.canWound(existingEntry, priority, txHash) {
                    // ❌ 读锁被高优写锁占着 → 回滚
                    for _, k := range acquiredWrites {
                        v.tempWriteLocks.CompareAndDelete(k, tempLockEntry{Holder: txHash, Priority: priority})
                    }
                    return false, false
                }
                // 可以 wound 写锁持有者
                v.woundedTxs.Store(existingEntry.Holder, struct{}{})
                v.tempWriteLocks.Store(key, tempLockEntry{Holder: txHash, Priority: priority})
                acquiredWrites = append(acquiredWrites, key)
                continue
            }
        }
        // 注册读锁
        v.addReadLock(key, txHash)
    }

    // ── Phase 3: 注册 tx 的读写集（sync.Map，无锁）──
    v.txReadWriteSets.Store(txHash, &RWKeySet{Reads: reads, Writes: writes})
    return true, false
}

// addReadLock 为 tx 注册对 key 的读锁。
// 内层 readHolderMap 的 Store 是 key=txHash，不同 tx 的 Store 不冲突。
func (v *TempLockView) addReadLock(key api.LockKey, txHash common.Hash) {
    holdersVal, _ := v.tempReadLocks.LoadOrStore(key, &readHolderMap{})
    holders := holdersVal.(*readHolderMap)
    holders.Store(txHash, struct{}{})
}
```

### 2.4 其他方法调整

#### IsTempLockedBySelf

```go
func (v *TempLockView) IsTempLockedBySelf(txHash common.Hash, lockKey api.LockKey) bool {
    if val, loaded := v.tempWriteLocks.Load(lockKey); loaded {
        entry := val.(tempLockEntry)
        return bytes.Equal(entry.Holder.Bytes(), txHash.Bytes())
    }
    // 检查读锁持有者列表
    if holdersVal, loaded := v.tempReadLocks.Load(lockKey); loaded {
        holders := holdersVal.(*readHolderMap)
        _, exists := holders.Load(txHash)
        return exists
    }
    return false
}
```

#### HasConflict

```go
func (v *TempLockView) HasConflict(txHash common.Hash, lockKey api.LockKey) bool {
    if val, loaded := v.tempWriteLocks.Load(lockKey); loaded {
        entry := val.(tempLockEntry)
        if !bytes.Equal(entry.Holder.Bytes(), txHash.Bytes()) {
            return true // 其他 tx 持有写锁
        }
        return false
    }
    if holdersVal, loaded := v.tempReadLocks.Load(lockKey); loaded {
        holders := holdersVal.(*readHolderMap)
        hasOther := false
        holders.Range(func(k, _ interface{}) bool {
            if !bytes.Equal(k.(common.Hash).Bytes(), txHash.Bytes()) {
                hasOther = true
                return false
            }
            return true
        })
        return hasOther
    }
    return false
}
```

#### IsWounded / ClearWounded

```go
func (v *TempLockView) IsWounded(txHash common.Hash) bool {
    _, exists := v.woundedTxs.Load(txHash)
    return exists
}

func (v *TempLockView) ClearWounded(txHash common.Hash) {
    v.woundedTxs.Delete(txHash)
}
```

#### GarbageCollect

```go
func (v *TempLockView) GarbageCollect(txHash common.Hash) {
    // 1. 取出 tx 的读写集（sync.Map Load/Delete，无锁）
    rwSetVal, exists := v.txReadWriteSets.Load(txHash)
    if exists {
        v.txReadWriteSets.Delete(txHash)
    }
    v.woundedTxs.Delete(txHash)
    if !exists {
        return
    }
    rwSet := rwSetVal.(*RWKeySet)

    // 2. 释放写锁（CompareAndDelete 确保不误删别人的）
    selfEntry := tempLockEntry{Holder: txHash}
    for _, key := range rwSet.Writes {
        v.tempWriteLocks.CompareAndDelete(key, selfEntry)
    }

    // 3. 释放读锁
    for _, key := range rwSet.Reads {
        if holdersVal, loaded := v.tempReadLocks.Load(key); loaded {
            holders := holdersVal.(*readHolderMap)
            holders.Delete(txHash)
        }
    }
}
```

#### OnBlockCommitted

```go
func (v *TempLockView) OnBlockCommitted(block *types.Block) {
    // 全量清空 tempWriteLocks
    // sync.Map 没有 Clear 方法，用 Range + Delete
    v.tempWriteLocks.Range(func(key, _ interface{}) bool {
        v.tempWriteLocks.Delete(key)
        return true
    })
    v.tempReadLocks.Range(func(key, _ interface{}) bool {
        v.tempReadLocks.Delete(key)
        return true
    })
    v.txReadWriteSets.Range(func(key, _ interface{}) bool {
        v.txReadWriteSets.Delete(key)
        return true
    })
    v.woundedTxs.Range(func(key, _ interface{}) bool {
        v.woundedTxs.Delete(key)
        return true
    })
}
```

> 注意：`sync.Map.Range` 在遍历中加全局锁，但 `OnBlockCommitted` 每 ~2s 才调用一次，不是瓶颈。

---

## 3. 修改清单

### 3.1 文件 `ssc/temp_lock_view.go`

| 修改 | 说明 |
|:-----|:------|
| `TempLockView` struct | 移除 `mu`，`tempReadLocks` 改为 `sync.Map`，其余 3 个 map 改为 `sync.Map` |
| 新增 `readHolderMap` | 内层 sync.Map |
| 新增 `addReadLock` | 两层 sync.Map 的读锁注册 |
| `TryLockWithPriority` | 重写：key 独立 `LoadOrStore` + 失败回滚 |
| `OnBlockCommitted` | Range + Delete 全量清空所有 sync.Map |
| `GarbageCollect` | CompareAndDelete 删除写锁 |
| `ClearWounded` | `sync.Map.Delete` |
| `IsWounded` | `sync.Map.Load` |
| `IsTempLockedBySelf` | `sync.Map.Load` 写锁 + 内层 `Load` 读锁 |
| `HasConflict` | `sync.Map.Load` 写锁 + 内层 `Range` 读锁 |
| `CanLock` | 同 `IsTempLockedBySelf` |
| `TryLock` | 调用 `TryLockWithPriority`，不变 |
| `NewTempLockView` | 移除 map 的 make |

### 3.2 其他文件

无影响。所有外部代码通过 `rs.tempLockView.*` 方法调用，接口不变。

---

## 4. 决策记录

| # | 问题 | 决策 | 理由 |
|:-:|:-----|:----|:------|
| 1 | 全局锁策略 | **完全移除 v.mu** | 全部 sync.Map 化，不同 tx / 不同 key 完全并行 |
| 2 | `tempWriteLocks` 实现 | `sync.Map` + `LoadOrStore` | 单 key 原子获取，无需跨 key 原子性 |
| 3 | `tempReadLocks` 实现 | **两层 `sync.Map`** | `[]common.Hash` 数组随机查找 O(n)，两層 map 是 O(1) |
| 4 | 跨 key 原子性 | **不保证** | 被抢了回滚已拿 key，stateDB 兜底 |
| 5 | Wound 时的 Store 竞争 | **接受** | 两人都 wound 对方，最坏双双重试 |
| 6 | `GarbageCollect` 回滚 | `CompareAndDelete` | 确保只删自己的，不误伤 |
| 7 | `OnBlockCommitted` 清空 | `Range + Delete` | 低频（~2s/次），遍历性能可接受 |

---

## 5. 预期收益

| 指标 | 当前 | 预期 | 依据 |
|:-----|:----:|:----:|:-----|
| TLV >100ms 慢调用数 | 14,714 | <500 | 不同 key / 不同 tx 完全并行 |
| RetryCommit P50 | 13ms | <5ms | TLV 无锁化 |
| RetryCommit P90 | 474ms | <30ms | 不再有 500+ tx 排队抢一把锁 |
| 提交率 | ~90% | 不退步 | 锁语义不变，仅粒度更细 |

---

## 6. 验证标准

| # | 验证项 | 指标 |
|:-:|:-------|:-----|
| 1 | TLV TryLock >100ms 的慢调用 | 从 20.6% 降至 <1% |
| 2 | 编译通过 | `go build` 无错误 |
| 3 | 并发正确性 | 10,000 tx 跑 3 次，提交率方差 <3%，无 panic |
| 4 | GarbageCollect 无泄漏 | 验证每块末尾 tempWriteLocks 为空（一致状态） |
