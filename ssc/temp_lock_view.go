package ssc

import (
	"bytes"
	"math"
	"sync"

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

// TempLockView — 临时锁视图（委员会 leader 维护）
// ... 省略重复的注释，结构不变 ...
type TempLockView struct {
	mu                  sync.RWMutex
	committedWriteLocks map[api.LockKey]*lockedState
	committedReadLocks  map[api.LockKey]*rlockedState
	tempWriteLocks      map[api.LockKey]tempLockEntry // LockKey → holder+priority
	tempReadLocks       map[api.LockKey][]common.Hash
	txReadWriteSets     map[common.Hash]*RWKeySet
	woundedTxs          map[common.Hash]struct{} // txs that have been Wounded
	stateLockManager    *stateLockManager
}

func NewTempLockView(manager *stateLockManager) *TempLockView {
	return &TempLockView{
		mu:                  sync.RWMutex{},
		committedWriteLocks: make(map[api.LockKey]*lockedState),
		committedReadLocks:  make(map[api.LockKey]*rlockedState),
		tempReadLocks:       make(map[api.LockKey][]common.Hash),
		tempWriteLocks:      make(map[api.LockKey]tempLockEntry),
		txReadWriteSets:     make(map[common.Hash]*RWKeySet),
		woundedTxs:          make(map[common.Hash]struct{}),
		stateLockManager:    manager,
	}
}

// TryLock 尝试为交易获取临时读/写锁。
// 无优先级判断，等价于 TryLockWithPriority 但总是返回 wounded=false。
func (v *TempLockView) TryLock(txHash common.Hash, reads []api.LockKey, writes []api.LockKey) bool {
	// 使用最低优先级（FirstSimBlock=MaxUint64），确保后续 retry tx 的 TryLockWithPriority 可将其 Wound
	locked, _ := v.TryLockWithPriority(txHash, api.Priority{FirstSimBlock: math.MaxUint64, TxHash: txHash}, reads, writes)
	return locked
}

// TryLockWithPriority 尝试为交易获取临时读/写锁，支持 Wound-Wait 优先级抢占。
//
// 返回：
//
//	locked:  是否成功获得所有锁
//	wounded: 是否被更高优先级的交易踢掉（仅当 locked=false 时有意义）
//
// Wound-Wait 规则：
//
//	如果锁空闲 → 拿锁 ✅
//	如果锁被低优先级占着 → Wound（踢掉低优先级，自己拿锁）✅
//	如果锁被高优先级或 Finalized 的 Patch 占着 → 失败 ❌
func (v *TempLockView) TryLockWithPriority(txHash common.Hash, priority api.Priority, reads []api.LockKey, writes []api.LockKey) (locked bool, wounded bool) {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.stateLockManager.sscService.stats.TempLockTryTotal.Add(1)

	rwSet := &RWKeySet{
		Reads:  append([]api.LockKey(nil), reads...),
		Writes: append([]api.LockKey(nil), writes...),
	}

	// Step 1: 检查写集 — 支持 Wound-Wait
	for _, key := range rwSet.Writes {
		// 写-写冲突：已提交锁
		if holder, held := v.committedWriteLocks[key]; held {
			if bytes.Compare(holder.lockedBy.Bytes(), txHash.Bytes()) != 0 {
				v.stateLockManager.sscService.stats.TempLockTryFail.Add(1)
				return false, false
			}
		}
		// 写-写冲突：临时锁 — 支持 Wound
		if entry, held := v.tempWriteLocks[key]; held {
			if bytes.Compare(entry.Holder.Bytes(), txHash.Bytes()) != 0 {
				// 检查是否可以 Wound
				if v.canWound(entry, priority, txHash) {
					// Wound: 踢掉低优先级占锁者
					v.woundedTxs[entry.Holder] = struct{}{}
					v.tempWriteLocks[key] = tempLockEntry{
						Holder:   txHash,
						Priority: priority,
					}
					utils.SSCLogger().Info().Str("wounded", entry.Holder.Hex()).
						Str("wounder", txHash.Hex()).
						Str("key", string(key)).
						Msg("Wound: high priority tx took lock from low priority")
				} else {
					// 被高优先级占着（或 Finalized）→ 失败
					v.stateLockManager.sscService.stats.TempLockTryFail.Add(1)
					return false, false
				}
			}
		}
	}

	// Step 2: 检查读集 — 支持 Wound-Wait
	for _, key := range rwSet.Reads {
		if entry, writtenInTemp := v.tempWriteLocks[key]; writtenInTemp {
			if bytes.Compare(entry.Holder.Bytes(), txHash.Bytes()) != 0 {
				// 检查是否可以 Wound
				if v.canWound(entry, priority, txHash) {
					v.woundedTxs[entry.Holder] = struct{}{}
					v.tempWriteLocks[key] = tempLockEntry{
						Holder:   txHash,
						Priority: priority,
					}
					utils.SSCLogger().Info().Str("wounded", entry.Holder.Hex()).
						Str("wounder", txHash.Hex()).
						Str("key", string(key)).
						Msg("Wound (readset): high priority tx took read lock from low priority")
				} else {
					v.stateLockManager.sscService.stats.TempLockTryFail.Add(1)
					return false, false
				}
			}
		}
	}

	// Step 3: 注册临时锁（只注册尚未锁定的 key）
	v.txReadWriteSets[txHash] = rwSet
	for _, key := range rwSet.Writes {
		if _, already := v.tempWriteLocks[key]; !already {
			v.tempWriteLocks[key] = tempLockEntry{
				Holder:   txHash,
				Priority: priority,
			}
		}
	}
	for _, key := range rwSet.Reads {
		v.tempReadLocks[key] = append(v.tempReadLocks[key], txHash)
	}

	return true, false
}

// canWound 检查是否可以踢掉当前锁持有者。
// 如果持有者优先级更高，或者持有者的 Patch 已 Finalized → false
func (v *TempLockView) canWound(entry tempLockEntry, requesterPri api.Priority, requester common.Hash) bool {
	// 检查 PatchPool 中该持有者的 Patch 是否已 Finalized
	if v.stateLockManager.sscService.retryScheduler.patchPool.IsFinalized(entry.Holder) {
		return false
	}
	// 如果持有者优先级更低（数值更大），可以踢
	return requesterPri.Less(entry.Priority)
}

// IsWounded 检查当前交易是否已被 Wound（被更高优先级的交易踢掉）。
func (v *TempLockView) IsWounded(txHash common.Hash) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	_, exists := v.woundedTxs[txHash]
	return exists
}

// ClearWounded 清理指定交易的 wounded 标记（在交易重试开始时调用）。
func (v *TempLockView) ClearWounded(txHash common.Hash) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.woundedTxs, txHash)
}

// CanLock 是 TryLock 的只读版本，不产生实际锁操作。
func (v *TempLockView) CanLock(txHash common.Hash, reads []api.LockKey, writes []api.LockKey) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()

	for _, key := range writes {
		if holder, held := v.committedWriteLocks[key]; held {
			if bytes.Compare(holder.lockedBy.Bytes(), txHash.Bytes()) != 0 {
				return false
			}
		}
		if entry, held := v.tempWriteLocks[key]; held {
			if bytes.Compare(entry.Holder.Bytes(), txHash.Bytes()) != 0 {
				return false
			}
		}
	}

	for _, key := range reads {
		if entry, writtenInTemp := v.tempWriteLocks[key]; writtenInTemp {
			if bytes.Compare(entry.Holder.Bytes(), txHash.Bytes()) != 0 {
				return false
			}
		}
	}
	return true
}

// OnBlockCommitted 清理区块中所有交易的临时锁。
func (v *TempLockView) OnBlockCommitted(block *types.Block) {
	blockTxHashes := make([]common.Hash, 0, len(block.Transactions()))
	for _, tx := range block.Transactions() {
		blockTxHashes = append(blockTxHashes, tx.Hash())
	}

	utils.SSCLogger().Info().Uint64("blockNum", block.NumberU64()).
		Int("tempWriteLocks", len(v.tempWriteLocks)).
		Int("tempReadLocks", len(v.tempReadLocks)).
		Int("committedWriteLocks", len(v.committedWriteLocks)).
		Int("woundedTxs", len(v.woundedTxs)).
		Msg("TempLockView on block committed")

	v.mu.Lock()
	defer v.mu.Unlock()

	for _, txHash := range blockTxHashes {
		if rwSet, exists := v.txReadWriteSets[txHash]; exists {
			for _, key := range rwSet.Writes {
				if entry, ok := v.tempWriteLocks[key]; ok && bytes.Compare(entry.Holder.Bytes(), txHash.Bytes()) == 0 {
					delete(v.tempWriteLocks, key)
				}
			}
			for _, key := range rwSet.Reads {
				if holders, ok := v.tempReadLocks[key]; ok {
					newHolders := make([]common.Hash, 0, len(holders))
					for _, h := range holders {
						if bytes.Compare(h.Bytes(), txHash.Bytes()) != 0 {
							newHolders = append(newHolders, h)
						}
					}
					if len(newHolders) == 0 {
						delete(v.tempReadLocks, key)
					} else {
						v.tempReadLocks[key] = newHolders
					}
				}
			}
			delete(v.txReadWriteSets, txHash)
		}
		// 清理 wounded 标记
		delete(v.woundedTxs, txHash)
	}

	v.committedWriteLocks, v.committedReadLocks = v.stateLockManager.GetRWLockStates()
}

// GarbageCollect 清理 stale 交易（如 nonce 过期）。
func (v *TempLockView) GarbageCollect(staleTxHash common.Hash) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if rwSet, exists := v.txReadWriteSets[staleTxHash]; exists {
		for _, key := range rwSet.Writes {
			if entry, ok := v.tempWriteLocks[key]; ok && bytes.Compare(entry.Holder.Bytes(), staleTxHash.Bytes()) == 0 {
				delete(v.tempWriteLocks, key)
			}
		}
		for _, key := range rwSet.Reads {
			if holders, ok := v.tempReadLocks[key]; ok {
				newHolders := make([]common.Hash, 0, len(holders))
				for _, h := range holders {
					if bytes.Compare(h.Bytes(), staleTxHash.Bytes()) != 0 {
						newHolders = append(newHolders, h)
					}
				}
				if len(newHolders) == 0 {
					delete(v.tempReadLocks, key)
				} else {
					v.tempReadLocks[key] = newHolders
				}
			}
		}
		delete(v.txReadWriteSets, staleTxHash)
	}
	delete(v.woundedTxs, staleTxHash)
}
