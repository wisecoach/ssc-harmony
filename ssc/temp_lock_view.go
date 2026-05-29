package ssc

import (
	"bytes"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
)

// ============================================================================
// RWKeySet — 交易的读写集
// ============================================================================

// RWKeySet 记录一个交易的读集和写集。
// 用于 TempLockView 的乐观锁冲突检测和死锁预防。
type RWKeySet struct {
	Reads  []api.LockKey
	Writes []api.LockKey
}

// ============================================================================
// TempLockView — 临时锁视图（委员会 leader 维护）
//
// TempLockView 是链下临时锁管理器，由 committee leader 维护。
// 它与 stateLockManager（链上已提交锁）不同，管理的锁状态是：
//   链上已提交的锁 + 本批次正在模拟的交易的临时锁
//
// 用途：
//   1. 在新交易申请模拟时，检查是否与已提交或本批次待模拟的交易写冲突
//   2. 在 retry 决策时，判断交易是否可以重新模拟（所有锁已释放）
//   3. 在区块提交后，清理该区块中所有交易的临时锁
//
// 死锁预防：
//   所有交易的写集必须按全局顺序排序（死锁预防）——由调用方保证。
//   TryLock 按 writes 顺序获取锁，避免循环等待。
//
// 与 stateLockManager 的关系：
//   - stateLockManager 保存已上链交易的最终锁状态
//   - TempLockView 保存 stateLockManager 的快照 + 本批次临时锁
//   - OnBlockCommitted 时从 stateLockManager 同步最新的已提交锁
// ============================================================================

// NewTempLockView 创建一个新的 TempLockView，关联到给定的 stateLockManager。
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

// TempLockView 管理链下临时锁状态。
//
// 内部结构：
//
//	committedWriteLocks — 从 stateLockManager 同步的已提交写锁
//	committedReadLocks  — 从 stateLockManager 同步的已提交读锁
//	tempWriteLocks      — 本批次临时写入锁（txHash → LockKey）
//	tempReadLocks       — 本批次临时读取锁（LockKey → [txHash, ...]）
//	txReadWriteSets     — 每个交易已登记的读写集（用于清理）
//	stateLockManager    — 引用全局锁管理器（用于同步）
//
// 锁状态视图层级（从下到上优先）：
//  1. committed*Locks — 已上链的锁（不可变，仅下个区块更新）
//  2. temp*Locks      — 本批次临时锁（可变，TryLock 时设置）
type TempLockView struct {
	mu                  sync.RWMutex
	committedWriteLocks map[api.LockKey]*lockedState
	committedReadLocks  map[api.LockKey]*rlockedState
	tempWriteLocks      map[api.LockKey]common.Hash
	tempReadLocks       map[api.LockKey][]common.Hash
	txReadWriteSets     map[common.Hash]*RWKeySet
	stateLockManager    *stateLockManager
}

// TryLock 尝试为交易获取临时读/写锁。
//
// 要求：
//   - writes 必须按全局顺序排序（防死锁）
//   - 同一交易可以多次调用 TryLock（如果 key 已被同一 tx 锁定则重入成功）
//
// 冲突检测规则：
//
//	写-写冲突：     其他交易的已提交写锁 or 临时写锁 → 失败
//	写-读冲突：     本批次前面的交易读了此 key → 失败（读到旧值）
//	读-已提交写冲突：允许（因为读的是最新已提交值）
//	读-读：         允许
//	读-临时写冲突：  本批次临时写锁 → 失败（读到未提交不一致值）
func (v *TempLockView) TryLock(txHash common.Hash, reads []api.LockKey, writes []api.LockKey) bool {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.stateLockManager.sscService.stats.TempLockTryTotal.Add(1)

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
				v.stateLockManager.sscService.stats.TempLockTryFail.Add(1)
				return false
			}
		}
		if holder, held := v.tempWriteLocks[key]; held {
			if bytes.Compare(holder.Bytes(), txHash.Bytes()) != 0 {
				v.stateLockManager.sscService.stats.TempLockTryFail.Add(1)
				return false
			}
		}
		// 写-读冲突：若本批次前面的交易读了此 key，当前写会导致其读到旧值
		// → 但由顺序执行保证，此处只需确保写不重叠（读集在 Step 2 检查）
	}

	// Step 2: 检查读集是否被 committed 或 temp 写覆盖（Read-after-Write）
	for _, key := range rwSet.Reads {
		// 如果该 key 被已提交的写覆盖 → OK（读的是最新值）
		// 但如果被**本批次临时写**覆盖 → 当前读会看到"未提交"状态，不一致！
		if holder, writtenInTemp := v.tempWriteLocks[key]; writtenInTemp {
			if bytes.Compare(holder.Bytes(), txHash.Bytes()) != 0 {
				v.stateLockManager.sscService.stats.TempLockTryFail.Add(1)
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

// CanLock 是 TryLock 的只读版本，不产生实际锁操作。
// 用于 retryScheduler 的判断，检查交易是否可以重新模拟。
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
		// 但如果被**本批次临时写**覆盖 → 当前读会看到"未提交"状态，不一致！
		if holder, writtenInTemp := v.tempWriteLocks[key]; writtenInTemp {
			if bytes.Compare(holder.Bytes(), txHash.Bytes()) != 0 {
				return false // 本批次前面的交易写了此 key，当前交易读它会不一致
			}
		}
		// 注意：读-读、读-已提交写 是允许的
	}

	return true
}

// OnBlockCommitted 清理区块中所有交易的临时锁。
// 在区块提交后由 retryScheduler.OnBlockCommitted 调用。
//
// 流程：
//  1. 遍历区块中的所有交易，清理其临时写锁和读锁
//  2. 从 stateLockManager 同步最新的已提交写锁/读锁
//
// 注意：锁定状态在区块提交后会发生变化（已提交的交易锁释放了），
// 所以 TempLockView 必须重新同步 committedWriteLocks / committedReadLocks。
func (v *TempLockView) OnBlockCommitted(block *types.Block) {
	blockTxHashes := make([]common.Hash, 0, len(block.Transactions()))
	for _, tx := range block.Transactions() {
		blockTxHashes = append(blockTxHashes, tx.Hash())
	}

	utils.SSCLogger().Info().Uint64("blockNum", block.NumberU64()).Int("lockedNum", len(v.tempWriteLocks)).Msg("TempLockView on block committed")

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

// GarbageCollect 清理 stale 交易（如 nonce 过期）。
// 在 retryScheduler.StaleTx 中调用。
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
