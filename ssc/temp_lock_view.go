package ssc

import (
	"bytes"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/ssc/api"
)

type RWKeySet struct {
	Reads  []api.LockKey
	Writes []api.LockKey
}

func NewTempLockView(manager *stateLockManager) *TempLockView {
	return &TempLockView{
		mu:                  sync.RWMutex{},
		committedWriteLocks: make(map[api.LockKey]*lockedState),
		committedReadLocks:  make(map[api.LockKey]*rlockedState),
		tempReadLocks:       make(map[api.LockKey][]common.Hash),
		tempWriteLocks:      make(map[api.LockKey]common.Hash),
		txReadWriteSets:     make(map[common.Hash]*RWKeySet),
		stateLockManager:    manager,
	}
}

type TempLockView struct {
	mu                  sync.RWMutex
	committedWriteLocks map[api.LockKey]*lockedState
	committedReadLocks  map[api.LockKey]*rlockedState
	tempWriteLocks      map[api.LockKey]common.Hash
	tempReadLocks       map[api.LockKey][]common.Hash
	txReadWriteSets     map[common.Hash]*RWKeySet
	stateLockManager    *stateLockManager
}

// TryLock 尝试为交易获取临时读/写锁
// 要求：writes 必须按全局顺序排序（防死锁）
func (v *TempLockView) TryLock(txHash common.Hash, reads []api.LockKey, writes []api.LockKey) bool {
	v.mu.Lock()
	defer v.mu.Unlock()

	rwSet := &RWKeySet{
		Reads:  append([]api.LockKey(nil), reads...),
		Writes: append([]api.LockKey(nil), writes...),
	}

	// Step 1: 检查写集是否与 committed 或 temp 写冲突
	for _, key := range rwSet.Writes {
		// 写-写冲突：已提交或其他模拟交易已写
		if holder, held := v.committedWriteLocks[key]; held {
			// 需要判断锁是否是该交易的锁
			if bytes.Compare(holder.lockedBy.Bytes(), txHash.Bytes()) != 0 {
				return false
			}
		}
		if holder, held := v.tempWriteLocks[key]; held {
			if bytes.Compare(holder.Bytes(), txHash.Bytes()) != 0 {
				return false
			}
		}
		// 写-读冲突：若本批次前面的交易读了此 key，当前写会导致其读到旧值
		// → 但由顺序执行保证，此处只需确保写不重叠（读集在 Step 2 检查）
	}

	// Step 2: 检查读集是否被 committed 或 temp 写覆盖（Read-after-Write）
	for _, key := range rwSet.Reads {
		// 如果该 key 被已提交的写覆盖 → OK（读的是最新值）
		// 但如果被**本批次临时写**覆盖 → 当前读会看到“未提交”状态，不一致！
		if holder, writtenInTemp := v.tempWriteLocks[key]; writtenInTemp {
			if bytes.Compare(holder.Bytes(), txHash.Bytes()) != 0 {
				return false // 本批次前面的交易写了此 key，当前交易读它会不一致
			}
		}
		// 注意：读-读、读-已提交写 是允许的
	}

	// Step 3: 无冲突，注册临时锁
	v.txReadWriteSets[txHash] = rwSet

	// 注册写锁
	for _, key := range rwSet.Writes {
		v.tempWriteLocks[key] = txHash
	}

	// 注册读锁（可选：仅用于调试或高级 GC）
	for _, key := range rwSet.Reads {
		v.tempReadLocks[key] = append(v.tempReadLocks[key], txHash)
	}

	return true
}

func (v *TempLockView) CanLock(txHash common.Hash, reads []api.LockKey, writes []api.LockKey) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()

	// Step 1: 检查写集是否与 committed 或 temp 写冲突
	for _, key := range writes {
		// 写-写冲突：已提交或其他模拟交易已写
		if holder, held := v.committedWriteLocks[key]; held {
			// 需要判断锁是否是该交易的锁
			if bytes.Compare(holder.lockedBy.Bytes(), txHash.Bytes()) != 0 {
				return false
			}
		}
		if holder, held := v.tempWriteLocks[key]; held {
			if bytes.Compare(holder.Bytes(), txHash.Bytes()) != 0 {
				return false
			}
		}
		// 写-读冲突：若本批次前面的交易读了此 key，当前写会导致其读到旧值
		// → 但由顺序执行保证，此处只需确保写不重叠（读集在 Step 2 检查）
	}

	// Step 2: 检查读集是否被 committed 或 temp 写覆盖（Read-after-Write）
	for _, key := range reads {
		// 如果该 key 被已提交的写覆盖 → OK（读的是最新值）
		// 但如果被**本批次临时写**覆盖 → 当前读会看到“未提交”状态，不一致！
		if holder, writtenInTemp := v.tempWriteLocks[key]; writtenInTemp {
			if bytes.Compare(holder.Bytes(), txHash.Bytes()) != 0 {
				return false // 本批次前面的交易写了此 key，当前交易读它会不一致
			}
		}
		// 注意：读-读、读-已提交写 是允许的
	}

	return true
}

// OnBlockCommitted 清理区块中所有交易的临时锁
// blockTxHashes: 区块中包含的所有交易哈希（按执行顺序）
func (v *TempLockView) OnBlockCommitted(block *types.Block) {
	blockTxHashes := make([]common.Hash, 0, len(block.Transactions()))
	for _, tx := range block.Transactions() {
		blockTxHashes = append(blockTxHashes, tx.Hash())
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	for _, txHash := range blockTxHashes {
		if rwSet, exists := v.txReadWriteSets[txHash]; exists {
			// 清理写锁
			for _, key := range rwSet.Writes {
				if holder, ok := v.tempWriteLocks[key]; ok && holder == txHash {
					delete(v.tempWriteLocks, key)
				}
			}
			// 清理读锁（从列表中移除）
			for _, key := range rwSet.Reads {
				if holders, ok := v.tempReadLocks[key]; ok {
					newHolders := make([]common.Hash, 0, len(holders))
					for _, h := range holders {
						if h != txHash {
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
			// 删除缓存
			delete(v.txReadWriteSets, txHash)
		}
	}

	// 同步最新的已提交写锁（从 stateLockManager 获取快照）
	v.committedWriteLocks, v.committedReadLocks = v.stateLockManager.GetRWLockStates()
}

// GarbageCollect 清理 stale 交易（如 nonce 过期）
func (v *TempLockView) GarbageCollect(staleTxHash common.Hash) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if rwSet, exists := v.txReadWriteSets[staleTxHash]; exists {
		// 清理写锁
		for _, key := range rwSet.Writes {
			if holder, ok := v.tempWriteLocks[key]; ok && holder == staleTxHash {
				delete(v.tempWriteLocks, key)
			}
		}
		// 清理读锁
		for _, key := range rwSet.Reads {
			if holders, ok := v.tempReadLocks[key]; ok {
				newHolders := make([]common.Hash, 0, len(holders))
				for _, h := range holders {
					if h != staleTxHash {
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
}
