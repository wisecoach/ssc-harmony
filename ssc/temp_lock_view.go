package ssc

import (
	"bytes"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
)

// tempLockEntry records the lock holder and its priority for Wound-Wait.
type tempLockEntry struct {
	Holder   common.Hash  // retryTx holding this lock
	Priority api.Priority // holder's priority (used for wounding decisions)
}

// RWKeySet — 交易的读写集
type RWKeySet struct {
	Reads  []api.LockKey
	Writes []api.LockKey
}

// readHolderMap is the inner sync.Map for read-lock holders of a single key.
// key: txHash common.Hash, value: struct{}{}
type readHolderMap struct {
	sync.Map
}

// TempLockView — 临时锁视图（委员会 leader 维护）
//
// 全部使用 sync.Map 替代传统 map + Mutex，不同 tx / 不同 key 之间完全并行。
// - tempWriteLocks: LoadOrStore 实现单 key 原子获取，失败回滚已拿 key
// - tempReadLocks:  两层 sync.Map，O(1) 查找持有者
// - txReadWriteSets: per-tx 数据，不同 tx 互不冲突
// - woundedTxs:      per-tx 标记，不同 tx 互不冲突
type TempLockView struct {
	tempWriteLocks   sync.Map // key: api.LockKey, value: tempLockEntry
	tempReadLocks    sync.Map // key: api.LockKey, value: *readHolderMap
	txReadWriteSets  sync.Map // key: common.Hash, value: *RWKeySet
	woundedTxs       sync.Map // key: common.Hash, value: struct{}{}
	stateLockManager *stateLockManager
}

func NewTempLockView(manager *stateLockManager) *TempLockView {
	return &TempLockView{
		stateLockManager: manager,
	}
}

// TryLock 尝试为交易获取临时读/写锁。
// 无优先级判断，等价于 TryLockWithPriority 但总是返回 wounded=false。
func (v *TempLockView) TryLock(txHash common.Hash, reads []api.LockKey, writes []api.LockKey) bool {
	locked, _ := v.TryLockWithPriority(txHash, api.Priority{}, reads, writes)
	return locked
}

// TryLockWithPriority 尝试为交易获取临时读/写锁，支持 Wound-Wait 优先级抢占。
//
// 返回：
//
//	locked:  是否成功获得所有锁
//	wounded: 是否被更高优先级的交易踢掉（仅当 locked=false 时有意义）
//
// 每把写锁通过 LoadOrStore 原子获取，不存在跨 key 全局锁。
// 部分 key 获取失败后回滚已拿的 key，后续 stateDB CheckLock / ForceSimulation 兜底。
func (v *TempLockView) TryLockWithPriority(txHash common.Hash, priority api.Priority, reads []api.LockKey, writes []api.LockKey) (locked bool, wounded bool) {
	tEntry := time.Now()
	defer func() {
		tTotal := time.Since(tEntry)
		if tTotal > 100*time.Millisecond {
			utils.SSCLogger().Warn().Str("txHash", txHash.Hex()).
				Dur("total", tTotal).
				Int("writes", len(writes)).
				Int("reads", len(reads)).
				Msg("TryLockWithPriority: slow")
		}
	}()

	v.stateLockManager.sscService.stats.TempLockTryTotal.Add(1)

	rwSet := &RWKeySet{
		Reads:  append([]api.LockKey(nil), reads...),
		Writes: append([]api.LockKey(nil), writes...),
	}

	// Step 1: 逐个原子获取写锁（LoadOrStore）— 失败则回滚
	var acquiredWrites []api.LockKey
	for _, key := range rwSet.Writes {
		entry := tempLockEntry{Holder: txHash, Priority: priority}
		existing, loaded := v.tempWriteLocks.LoadOrStore(key, entry)
		if !loaded {
			// key 原来为空，成功获取
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
			// 可以 Wound → 替换
			v.woundedTxs.Store(existingEntry.Holder, struct{}{})
			v.tempWriteLocks.Store(key, entry)
			acquiredWrites = append(acquiredWrites, key)
			utils.SSCLogger().Debug().Str("wounded", existingEntry.Holder.Hex()).
				Str("wounder", txHash.Hex()).
				Str("key", string(key)).
				Msg("Wound: high priority tx took lock from low priority")
			continue
		}
		// 被高优先级占着（或 Finalized）→ 失败，回滚已拿的写锁
		v.stateLockManager.sscService.stats.TempLockTryFail.Add(1)
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Str("holder", existingEntry.Holder.Hex()).
			Str("key", string(key)).
			Msg("TryLockWithPriority: tempWriteLock conflict, cannot wound")
		// 回滚
		for _, k := range acquiredWrites {
			if val, ok := v.tempWriteLocks.Load(k); ok {
				entry := val.(tempLockEntry)
				if bytes.Equal(entry.Holder.Bytes(), txHash.Bytes()) {
					v.tempWriteLocks.Delete(k)
				}
			}
		}
		return false, false
	}

	// Step 2: 检查+注册读锁
	for _, key := range rwSet.Reads {
		// 检查该 key 是否被别的 tx 写锁着
		if existing, loaded := v.tempWriteLocks.Load(key); loaded {
			existingEntry := existing.(tempLockEntry)
			if !bytes.Equal(existingEntry.Holder.Bytes(), txHash.Bytes()) {
				if !v.canWound(existingEntry, priority, txHash) {
					// 读锁被高优写锁占着 → 失败，回滚已拿的写锁
					v.stateLockManager.sscService.stats.TempLockTryFail.Add(1)
					utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
						Str("holder", existingEntry.Holder.Hex()).
						Str("key", string(key)).
						Msg("TryLockWithPriority: read-tempWriteLock conflict, cannot wound")
					for _, k := range acquiredWrites {
						if val, ok := v.tempWriteLocks.Load(k); ok {
							entry := val.(tempLockEntry)
							if bytes.Equal(entry.Holder.Bytes(), txHash.Bytes()) {
								v.tempWriteLocks.Delete(k)
							}
						}
					}
					return false, false
				}
				// 可以 wound 写锁持有者 → 升级为写锁
				v.woundedTxs.Store(existingEntry.Holder, struct{}{})
				v.tempWriteLocks.Store(key, tempLockEntry{Holder: txHash, Priority: priority})
				acquiredWrites = append(acquiredWrites, key)
				utils.SSCLogger().Debug().Str("wounded", existingEntry.Holder.Hex()).
					Str("wounder", txHash.Hex()).
					Str("key", string(key)).
					Msg("Wound (readset): high priority tx took read lock from low priority")
				continue
			}
		}
		// 注册读锁
		v.addReadLock(key, txHash)
	}

	// Step 3: 注册交易读写集（sync.Map，无锁）
	v.txReadWriteSets.Store(txHash, rwSet)
	return true, false
}

// addReadLock 为 tx 注册对 key 的读锁。
// 内层 readHolderMap 以 txHash 为 key，不同 tx 的 Store 不冲突。
func (v *TempLockView) addReadLock(key api.LockKey, txHash common.Hash) {
	holdersVal, _ := v.tempReadLocks.LoadOrStore(key, &readHolderMap{})
	holders := holdersVal.(*readHolderMap)
	holders.Store(txHash, struct{}{})
}

// canWound 检查是否可以踢掉当前锁持有者。
// 如果持有者优先级更高，或者持有者的 Patch 已 Finalized → false
func (v *TempLockView) canWound(entry tempLockEntry, requesterPri api.Priority, requester common.Hash) bool {
	// 检查 PatchPool 中该持有者的 Patch 是否已 Finalized
	if v.stateLockManager.sscService.retryScheduler.isPatchFinalized(entry.Holder) {
		return false
	}
	// 如果持有者优先级更低（数值更大），可以踢
	return requesterPri.Less(entry.Priority)
}

// IsWounded 检查当前交易是否已被 Wound（被更高优先级的交易踢掉）。
func (v *TempLockView) IsWounded(txHash common.Hash) bool {
	_, exists := v.woundedTxs.Load(txHash)
	return exists
}

// ClearWounded 清理指定交易的 wounded 标记（在交易重试开始时调用）。
func (v *TempLockView) ClearWounded(txHash common.Hash) {
	v.woundedTxs.Delete(txHash)
}

// IsTempLockedBySelf 检查当前交易是否持有该 key 的 TempLock（写锁或读锁）。
// 用于 GetState/SetState 的锁仲裁：如果自己持有 TempLock，可以跳过 SLM 检查。
func (v *TempLockView) IsTempLockedBySelf(txHash common.Hash, lockKey api.LockKey) bool {
	// 检查写锁
	if val, loaded := v.tempWriteLocks.Load(lockKey); loaded {
		entry := val.(tempLockEntry)
		return bytes.Equal(entry.Holder.Bytes(), txHash.Bytes())
	}

	// 检查读锁 holder 列表
	if holdersVal, loaded := v.tempReadLocks.Load(lockKey); loaded {
		holders := holdersVal.(*readHolderMap)
		_, exists := holders.Load(txHash)
		return exists
	}

	return false
}

// HasConflict 检查指定 key 是否被**其他**交易在 TempLockView 中预约。
// 用于 GetState/SetState 的锁仲裁：若当前 tx 自己持有，不算冲突。
// lockKey 应为 api.FormKey(address, key) 的结果。
func (v *TempLockView) HasConflict(txHash common.Hash, lockKey api.LockKey) bool {
	// 检查写锁
	if val, loaded := v.tempWriteLocks.Load(lockKey); loaded {
		entry := val.(tempLockEntry)
		if !bytes.Equal(entry.Holder.Bytes(), txHash.Bytes()) {
			return true // 其他交易持有写锁
		}
		return false // 自己持有，不算冲突
	}

	// 检查读锁
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

// CanLock 是 TryLock 的只读版本，不产生实际锁操作。
func (v *TempLockView) CanLock(txHash common.Hash, reads []api.LockKey, writes []api.LockKey) bool {
	for _, key := range writes {
		if val, loaded := v.tempWriteLocks.Load(key); loaded {
			entry := val.(tempLockEntry)
			if !bytes.Equal(entry.Holder.Bytes(), txHash.Bytes()) {
				return false
			}
		}
	}

	for _, key := range reads {
		if val, loaded := v.tempWriteLocks.Load(key); loaded {
			entry := val.(tempLockEntry)
			if !bytes.Equal(entry.Holder.Bytes(), txHash.Bytes()) {
				return false
			}
		}
	}
	return true
}

// OnBlockCommitted 清理区块中所有交易的临时锁，并返回被释放的 key 集合。
func (v *TempLockView) OnBlockCommitted(block *types.Block) []api.LockKey {
	t0 := time.Now()
	blockTxHashes := make([]common.Hash, 0, len(block.Transactions()))
	for _, tx := range block.Transactions() {
		blockTxHashes = append(blockTxHashes, tx.Hash())
	}
	tBuildList := time.Since(t0)

	// 统计日志（大致计数）
	writeLockCount := 0
	v.tempWriteLocks.Range(func(_, _ interface{}) bool {
		writeLockCount++
		return true
	})
	readLockCount := 0
	v.tempReadLocks.Range(func(_, _ interface{}) bool {
		readLockCount++
		return true
	})
	woundedCount := 0
	v.woundedTxs.Range(func(_, _ interface{}) bool {
		woundedCount++
		return true
	})

	var releasedKeys []api.LockKey
	for _, txHash := range blockTxHashes {
		// 取出 tx 的读写集
		rwSetVal, exists := v.txReadWriteSets.Load(txHash)
		if !exists {
			v.woundedTxs.Delete(txHash)
			continue
		}
		rwSet := rwSetVal.(*RWKeySet)

		// 清理写锁
		for _, key := range rwSet.Writes {
			if val, ok := v.tempWriteLocks.Load(key); ok {
				entry := val.(tempLockEntry)
				if bytes.Equal(entry.Holder.Bytes(), txHash.Bytes()) {
					v.tempWriteLocks.Delete(key)
					releasedKeys = append(releasedKeys, key)
				}
			}
		}
		// 清理读锁
		for _, key := range rwSet.Reads {
			if holdersVal, ok := v.tempReadLocks.Load(key); ok {
				holders := holdersVal.(*readHolderMap)
				holders.Delete(txHash)
				releasedKeys = append(releasedKeys, key)
			}
		}
		v.txReadWriteSets.Delete(txHash)
		v.woundedTxs.Delete(txHash)
	}
	tCleanupLoop := time.Since(t0)
	utils.SSCLogger().Info().
		Uint64("blockNum", block.NumberU64()).
		Int("txCount", len(blockTxHashes)).
		Int("releasedKeys", len(releasedKeys)).
		Int("keys", writeLockCount+readLockCount).
		Dur("buildList", tBuildList).
		Dur("cleanupLoop", tCleanupLoop).
		Dur("total", time.Since(t0)).
		Msg("[TempLockView] OnBlockCommitted timing breakdown")

	// Deduplicate released keys
	seen := make(map[api.LockKey]struct{})
	unique := releasedKeys[:0]
	for _, key := range releasedKeys {
		if _, exists := seen[key]; !exists {
			seen[key] = struct{}{}
			unique = append(unique, key)
		}
	}
	return unique
}

// GarbageCollect 清理 stale 交易（如 nonce 过期）。
func (v *TempLockView) GarbageCollect(staleTxHash common.Hash) {
	// 取出 tx 的读写集
	rwSetVal, exists := v.txReadWriteSets.Load(staleTxHash)
	if !exists {
		v.woundedTxs.Delete(staleTxHash)
		return
	}
	rwSet := rwSetVal.(*RWKeySet)

	// 清理写锁
	for _, key := range rwSet.Writes {
		if val, ok := v.tempWriteLocks.Load(key); ok {
			entry := val.(tempLockEntry)
			if bytes.Equal(entry.Holder.Bytes(), staleTxHash.Bytes()) {
				v.tempWriteLocks.Delete(key)
			}
		}
	}
	// 清理读锁
	for _, key := range rwSet.Reads {
		if holdersVal, ok := v.tempReadLocks.Load(key); ok {
			holders := holdersVal.(*readHolderMap)
			holders.Delete(staleTxHash)
		}
	}
	v.txReadWriteSets.Delete(staleTxHash)
	v.woundedTxs.Delete(staleTxHash)
}

// Stats 返回 TLV 各 sync.Map 的条目数
func (v *TempLockView) Stats() (writeLocks, readLocks, txSets, wounded int) {
	v.tempWriteLocks.Range(func(_, _ any) bool { writeLocks++; return true })
	v.tempReadLocks.Range(func(_, _ any) bool { readLocks++; return true })
	v.txReadWriteSets.Range(func(_, _ any) bool { txSets++; return true })
	v.woundedTxs.Range(func(_, _ any) bool { wounded++; return true })
	return
}
