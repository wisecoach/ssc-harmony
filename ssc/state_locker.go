// Copyright 2026 Harmony One
// This file implements versioned state locker with snapshot support.
// It allows retrieving locker state at a specific stateRoot, solving the
// consistency issue between stateDB (versioned) and locker (real-time global).
//
// Key features:
// - Snapshot-based versioning: saves locker state at each stateDB.Commit()
// - LRU + time-based eviction: automatically cleans up old snapshots
// - Thread-safe: all operations are protected by RWMutex
// - Backward compatible: original journal/revert mechanism unchanged

package ssc

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/pkg/errors"
)

// ============================================================================
// Configuration Constants
// ============================================================================

const (
	// DefaultMaxSnapshots keeps the last N stateRoot snapshots in memory
	DefaultMaxSnapshots = 5

	// DefaultSnapshotTTL is the maximum time a snapshot is kept (1 hour)
	DefaultSnapshotTTL = time.Hour

	// SnapshotCleanupInterval is how often to run cleanup
	SnapshotCleanupInterval = 10 * time.Minute
)

// ============================================================================
// Snapshot Structures
// ============================================================================

// lockedStatesSnapshot 是某个 stateRoot 下锁状态的不可变快照。
// 不包含 journal —— 只包含已提交的锁状态。
//
// 每次 stateDB.Commit() 后自动创建，为后续模拟交易提供版本化的锁视图。
type lockedStatesSnapshot struct {
	lockedStates  map[api.LockKey]*lockedState
	rlockedStates map[api.LockKey]*rlockedState
	finishedTxs   map[common.Hash]bool
	timestamp     time.Time // 用于 TTL 过期清除
}

// snapshotMetadata 跟踪快照访问信息，用于 LRU 淘汰决策。
type snapshotMetadata struct {
	lastAccess  time.Time
	accessCount uint64
}

// ============================================================================
// stateLocker with Versioning Support
//
// stateLocker 是每个模拟交易实例独享的锁控制器。
// 每个模拟交易在创建时通过 GetLockerAt(stateRoot) 获得一个 stateLocker，
// 该 locker 包含：
//   - baseSnapshot: 该 stateRoot 的只读锁快照（不可变）
//   - pendingStates: 本实例的隔离 pending 状态（仅本实例可见）
//
// 关键设计：
// - Lock/Unlock 只操作 pendingStates，不直接影响全局锁
// - 锁冲突检测同时检查 baseSnapshot + pendingStates
// - CommitTx/RollbackTx 将事务锁释放记录到 pendingUnlocks
// - Commit(newRoot) 将 pendingStates 合并到 stateLockManager 全局状态
// ============================================================================

// stateLocker is a version-aware locker instance
type stateLocker struct {
	txLock  sync.RWMutex
	root    common.Hash
	stateDB api.StateDB
	*stateLockManager

	// baseSnapshot is the read-only base state from snapshot (or current state)
	// This is immutable for the lifetime of this locker instance
	baseSnapshot *lockedStatesSnapshot

	// pendingStates holds uncommitted changes for this locker instance
	// These changes are isolated until Commit() is called
	pendingStates *lockedStates
	// pendingUnlocks tracks txHashes that need to be unlocked from stateLockManager on Commit
	// Unlock operations are transaction-level
	pendingUnlocks *pendingUnlockCache
	tmpFinishedTxs map[common.Hash]bool
	journal        *lockJournal
	validRevisions []revision
	nextRevisionId int
}

// BindStateDB binds a stateDB instance to this locker
func (s *stateLocker) BindStateDB(root common.Hash, stateDB api.StateDB) {
	s.stateDB = stateDB
	s.root = root
}

// ============================================================================
// Lock/Unlock Operations
//
// 锁操作的核心逻辑：
//
// 1. 冲突检测（Lockable/RLockable）：
//    同时检查 baseSnapshot（已提交的只读基准）和 pendingStates（本实例 pending 状态）
//    确保不会与已提交的锁或其他并行模拟的 pending 状态冲突
//
// 2. 上锁（Lock/RLock）：
//    写入 pendingStates（隔离），追加 journal 条目（支持快照回滚）
//
// 3. 释放锁（unlock/unlockRLock）：
//    从 pendingStates 删除，追加 unlock 类型的 journal 条目
//
// 所有操作产生 journal 条目，支持 RevertToSnapshot 回滚。
// ============================================================================

// RLockable 检查读锁是否可以获取（不产生实际上锁）。
// 读锁与写锁互斥：
//   - 如果 key 被任何写锁（base 或 pending）持有 → 不可读锁
//   - 如果 key 仅被读锁持有 → 可读锁（读读不互斥）
func (s *stateLocker) RLockable(key api.LockKey, txHash common.Hash) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 检查 base snapshot（只读，不可变）
	if ls, exists := s.baseSnapshot.lockedStates[key]; exists {
		s.sscService.stats.LockableFailRlock.Add(1)
		return errors.Wrap(api.ErrLockedByOtherTx, fmt.Sprintf("locked by tx %s", ls.lockedBy.Hex()))
	}

	// 检查 pending states（本 instance 未提交的变更）
	if ls, exists := s.pendingStates.lockedStates[key]; exists {
		s.sscService.stats.LockableFailRlock.Add(1)
		return errors.Wrap(api.ErrLockedByOtherTx, fmt.Sprintf("locked by tx %s in pending states", ls.lockedBy.Hex()))
	}

	return nil
}

// Lockable 检查写锁是否可以获取（不产生实际上锁）。
// 写锁与写锁互斥，也与读锁互斥：
//   - key 被其他交易的写锁持有 → 不可锁
//   - key 被其他交易的读锁持有 → 不可锁（读锁升级为写锁需等待）
//   - key 被同一交易持有 → 可重入
func (s *stateLocker) Lockable(key api.LockKey, txHash common.Hash) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Str("key", string(key)).
		Str("root", s.root.Hex()).
		Int("baseSnapshot", len(s.baseSnapshot.lockedStates)).
		Int("pendingStates", s.pendingStates.length()).
		Msgf("check lockable")

	// 检查 base snapshot（只读，不可变）
	if ls, exists := s.baseSnapshot.lockedStates[key]; exists {
		if !ls.lockable(txHash) {
			s.sscService.stats.LockableFailBase.Add(1)
			s.sscService.stats.RecordLockConflict(string(key))
			utils.SSCLogger().Warn().Str("txHash", txHash.Hex()).
				Str("key", string(key)).
				Str("root", s.root.Hex()).
				Msgf("key is locked by other tx %s in base snapshot", ls.lockedBy.Hex())
			return errors.Wrap(api.ErrLockedByOtherTx, fmt.Sprintf("locked by tx %s", ls.lockedBy.Hex()))
		}
	}

	// 检查 pending states（本 instance 未提交的变更）
	if ls, exists := s.pendingStates.lockedStates[key]; exists {
		if !ls.lockable(txHash) {
			s.sscService.stats.LockableFailPending.Add(1)
			s.sscService.stats.RecordLockConflict(string(key))
			utils.SSCLogger().Warn().Str("txHash", txHash.Hex()).
				Str("key", string(key)).
				Str("root", s.root.Hex()).
				Msgf("key is locked by other tx %s in pending states", ls.lockedBy.Hex())
			return errors.Wrap(api.ErrLockedByOtherTx, fmt.Sprintf("locked by tx %s", ls.lockedBy.Hex()))
		}
	}

	// 检查 base snapshot 的读锁
	if ls, exists := s.baseSnapshot.rlockedStates[key]; exists {
		s.sscService.stats.LockableFailRlock.Add(1)
		return errors.Wrap(api.ErrLockedByOtherTx, fmt.Sprintf("rlocked by tx %s in base snapshot", ls.lockedBy))
	}

	// 检查 pending states 的读锁
	if ls, exists := s.pendingStates.rlockedStates[key]; exists {
		s.sscService.stats.LockableFailRlock.Add(1)
		return errors.Wrap(api.ErrLockedByOtherTx, fmt.Sprintf("rlocked by tx %s in pending states", ls.lockedBy))
	}

	return nil
}

// Lock 获取一个写锁。写入 pendingStates（隔离），追加 journal 条目。
func (s *stateLocker) Lock(txHash common.Hash, callIndex api.CallIndex, key api.LockKey, value common.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 写入 pendingStates（隔离，对其他交易不可见，直到 Commit）
	ls, exists := s.pendingStates.lockedStates[key]
	if !exists {
		ls = &lockedState{}
	}
	ls.lockedBy = txHash
	ls.lockTime = time.Now()
	s.pendingStates.addLockedState(txHash, callIndex.ToString(), key, value, ls)
	s.journal.append(&lockEntry{
		txHash:    txHash,
		callIndex: callIndex,
		key:       key,
		value:     value,
	})
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
		Str("callIndex", callIndex.ToString()).
		Str("key", string(key)).
		Msgf("mu state, pending locked %d state, journal %d", s.pendingStates.length(), s.journal.length())
	return nil
}

// RLock 获取一个读锁。写入 pendingStates（隔离），追加 journal 条目。
func (s *stateLocker) RLock(txHash common.Hash, callIndex api.CallIndex, key api.LockKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 写入 pendingStates（隔离，对其他交易不可见，直到 Commit）
	ls, exists := s.pendingStates.rlockedStates[key]
	if !exists {
		ls = &rlockedState{}
	}
	ls.lockedBy = append(ls.lockedBy, txHash)
	ls.lockTime = time.Now()
	s.pendingStates.addRLockedState(txHash, callIndex.ToString(), key, ls)
	s.journal.append(&rlockEntry{
		txHash:    txHash,
		callIndex: callIndex,
		key:       key,
	})
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Str("callIndex", callIndex.ToString()).
		Str("key", string(key)).
		Msgf("mu state, pending rlocked %d state, journal %d", s.pendingStates.length(), s.journal.length())
	return nil
}

// unlock 释放一个写锁。从 pendingStates 中删除，追加 unlockEntry。
// rollback 标志用于区分 CommitTx 和 RollbackTx。
func (s *stateLocker) unlock(txHash common.Hash, callIndexStr string, key api.LockKey, rollback bool, newValue common.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 从 pendingStates 解锁
	if state, exists := s.pendingStates.lockedStates[key]; exists {
		value := s.pendingStates.deleteLockedState(txHash, callIndexStr, key)
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
			Msgf("unlock state from pending, txHash=%s, callIndex=%s, key=%s", txHash.Hex(), callIndexStr, key)
		s.journal.append(&unlockEntry{
			state:        state,
			txHash:       txHash,
			callIndexStr: callIndexStr,
			key:          key,
			oldValue:     value,
			newValue:     newValue,
			rollback:     rollback,
		})
	}
	return nil
}

// unlockRLock 释放一个读锁。从 pendingStates 中删除，追加 unlockRLockEntry。
func (s *stateLocker) unlockRLock(txHash common.Hash, callIndexStr string, key api.LockKey, rollback bool, newValue common.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 从 pendingStates 解锁
	if state, exists := s.pendingStates.rlockedStates[key]; exists {
		s.pendingStates.deleteRLockedState(txHash, callIndexStr, key)
		s.journal.append(&unlockRLockEntry{
			state:        state,
			txHash:       txHash,
			callIndexStr: callIndexStr,
			key:          key,
		})
	}
	return nil
}

// ============================================================================
// Snapshot and Revert (transaction-level)
//
// 提供事务级别的快照/回滚机制，类似以太坊 StateDB 的 Snapshot() / RevertToSnapshot()。
// 用于模拟过程中发现冲突时，回滚当前交易的锁变更。
// ============================================================================

// Snapshot 创建一个新的快照点，返回 revision ID。
// 后续可以通过 RevertToSnapshot 回滚到此点。
func (s *stateLocker) Snapshot() int {
	id := s.nextRevisionId
	s.nextRevisionId++
	s.validRevisions = append(s.validRevisions, revision{id, s.journal.length()})
	return id
}

// RevertToSnapshot 回滚到指定的快照点。
// 从 journal 中移除该快照之后的所有条目，并执行 revert 操作。
func (s *stateLocker) RevertToSnapshot(targetId int) {
	idx := sort.Search(len(s.validRevisions), func(i int) bool {
		return s.validRevisions[i].id >= targetId
	})
	if idx >= len(s.validRevisions) || s.validRevisions[idx].id != targetId {
		panic("invalid snapshot id")
	}
	snapshot := s.validRevisions[idx].journalIndex

	s.journal.revert(s, snapshot)
	s.validRevisions = s.validRevisions[:idx]
}

// ============================================================================
// Transaction Commit/Rollback
//
// 事务级别的提交/回滚：
//
// CommitTx:
//   1. 解锁该 txHash 在 pendingStates 中的所有写锁和读锁
//   2. 将 txHash 注册到 pendingUnlocks ← 提交到 stateLockManager 时清除
//   3. 标记 tmpFinishedTxs[txHash] = true
//
// RollbackTx:
//   1. 读取 stateDB 的当前值用于 revert 备份
//   2. 将 stateDB 中的值恢复为锁定前的旧值
//   3. 解锁该 txHash 在 pendingStates 中的所有锁
//   4. 将 txHash 注册到 pendingUnlocks
//   5. 标记 tmpFinishedTxs[txHash] = false
//
// 两者的区别：
//   - CommitTx 正常释放锁，stateDB 的值保持不变
//   - RollbackTx 恢复 stateDB 旧值，锁释放标记 rollback=true
//
// 注意：这些操作只在 pendingStates 层面进行。
// 真正的全局锁释放延迟到 Commit(newRoot) 时由 pendingUnlocks.applyTo() 执行。
// ============================================================================

// CommitTx 提交一个交易，释放其在 pendingStates 中的所有锁。
func (s *stateLocker) CommitTx(txHash common.Hash) error {
	s.txLock.Lock()
	defer s.txLock.Unlock()

	// 从 pendingStates 中解锁该交易的所有写锁
	c2ls := s.pendingStates.getCallIndex2LockedStates(txHash)
	if c2ls != nil {
		for callIndex, lockKeyMap := range c2ls {
			for lockKey := range lockKeyMap {
				err := s.unlock(txHash, callIndex, lockKey, false, common.Hash{})
				if err != nil {
					return err
				}
			}
		}
	}
	// 从 pendingStates 中解锁该交易的所有读锁
	c2rls := s.pendingStates.getCallIndex2RLockedStates(txHash)
	if c2rls != nil {
		for callIndex, lockKeyMap := range c2rls {
			for lockKey := range lockKeyMap {
				err := s.unlockRLock(txHash, callIndex, lockKey, false, common.Hash{})
				if err != nil {
					return err
				}
			}
		}
	}

	// 记录此 tx 需要在 Commit 时从 stateLockManager 解锁
	s.pendingUnlocks.addTx(txHash)

	s.tmpFinishedTxs[txHash] = true
	s.journal.append(&finishTxEntry{
		txHash:           txHash,
		commitOrRollback: true,
	})
	return nil
}

// RollbackTx 回滚一个交易，恢复 stateDB 中的值并释放锁。
func (s *stateLocker) RollbackTx(txHash common.Hash) error {
	s.txLock.Lock()
	defer s.txLock.Unlock()

	// 从 pendingStates 中回滚该交易的所有写锁
	c2ls := s.pendingStates.getCallIndex2LockedStates(txHash)
	if c2ls != nil {
		for callIndex, lockKeyMap := range c2ls {
			for lockKey, oldValue := range lockKeyMap {
				addr, key := lockKey.Value()
				newValue, err := s.stateDB.GetStateWithoutLock(addr, key)
				if err != nil {
					return err
				}
				err = s.stateDB.SetStateWithoutLock(addr, key, oldValue)
				if err != nil {
					return err
				}
				err = s.unlock(txHash, callIndex, lockKey, true, newValue)
				if err != nil {
					return err
				}
			}
		}
	}
	// 从 pendingStates 中回滚该交易的所有读锁
	c2rls := s.pendingStates.getCallIndex2RLockedStates(txHash)
	if c2rls != nil {
		for callIndex, lockKeyMap := range c2rls {
			for lockKey := range lockKeyMap {
				err := s.unlockRLock(txHash, callIndex, lockKey, true, common.Hash{})
				if err != nil {
					return err
				}
			}
		}
	}

	// 记录此 tx 需要在 Commit 时从 stateLockManager 解锁
	s.pendingUnlocks.addTx(txHash)

	s.tmpFinishedTxs[txHash] = false
	s.journal.append(&finishTxEntry{
		txHash:           txHash,
		commitOrRollback: false,
	})
	return nil
}

// Commit commits the current locker state and creates a snapshot for the given stateRoot.
// This should be called in sync with stateDB.Commit().
// It merges pendingStates (uncommitted changes) into the global stateLockManager.
func (s *stateLocker) Commit(newRoot common.Hash) error {
	s.txLock.Lock()

	// === 将 pendingStates 合并到全局 stateLockManager ===
	// 这是关键的隔离解除点：变更只在 Commit 之后才对全局可见
	s.stateLockManager.mu.Lock()

	utils.SSCLogger().Info().
		Int("lockedStates", len(s.stateLockManager.lockedStates.lockedStates)).
		Int("pendingLockedStates", len(s.pendingStates.lockedStates)).
		Int("lockedTxs", len(s.stateLockManager.lockedStates.callIndex2lockedState)).
		Int("unlockedTxs", len(s.pendingUnlocks.unlockedTxs)).
		Msgf("Commit state mu, pendingStates: %d, tmpFinishedTxs: %d, root: %s",
			s.pendingStates.length(), len(s.tmpFinishedTxs), s.root.Hex())

	// 合并写锁
	for key, pendingState := range s.pendingStates.lockedStates {
		s.stateLockManager.lockedStates.lockedStates[key] = &lockedState{
			lockedBy: pendingState.lockedBy,
			lockTime: pendingState.lockTime,
		}
	}
	// 合并读锁
	for key, pendingState := range s.pendingStates.rlockedStates {
		s.stateLockManager.lockedStates.rlockedStates[key] = &rlockedState{
			lockedBy: append([]common.Hash{}, pendingState.lockedBy...),
			lockTime: pendingState.lockTime,
		}
	}

	// 合并 callIndex 反向映射（用于正确的 unlock 跟踪）
	for txHash, callIndexMap := range s.pendingStates.callIndex2lockedState {
		for callIndex, lockKeyMap := range callIndexMap {
			for lockKey, value := range lockKeyMap {
				s.stateLockManager.lockedStates.setLockedValue(txHash, callIndex, lockKey, value)
			}
		}
	}
	for txHash, callIndexMap := range s.pendingStates.callIndex2rlockedState {
		for callIndex, lockKeyMap := range callIndexMap {
			for lockKey := range lockKeyMap {
				s.stateLockManager.lockedStates.addRLockedState(txHash, callIndex, lockKey,
					s.stateLockManager.lockedStates.rlockedStates[lockKey])
			}
		}
	}
	s.stateLockManager.mu.Unlock()

	// === 将 pendingUnlocks 应用到 stateLockManager ===
	// 这步将已释放交易的锁从全局状态中移除
	s.pendingUnlocks.applyTo(s.stateLockManager)

	// 将所有临时完成的交易提交到已完成的交易
	for hash, b := range s.tmpFinishedTxs {
		s.finishedTxs[hash] = b
	}
	// 清空临时状态
	s.journal = &lockJournal{}
	s.validRevisions = make([]revision, 0)
	s.nextRevisionId = 0
	// 合并后清空 pendingStates
	s.pendingStates = &lockedStates{
		lockedStates:           make(map[api.LockKey]*lockedState),
		rlockedStates:          make(map[api.LockKey]*rlockedState),
		callIndex2lockedState:  make(map[common.Hash]map[string]map[api.LockKey]common.Hash),
		callIndex2rlockedState: make(map[common.Hash]map[string]map[api.LockKey]struct{}),
	}
	// 清空 pendingUnlocks
	s.pendingUnlocks = &pendingUnlockCache{
		unlockedTxs: make(map[common.Hash]bool),
	}
	// 更新 baseSnapshot 到最新全局状态（可选，locker 通常在 Commit 后不再使用）
	s.baseSnapshot = &lockedStatesSnapshot{
		lockedStates:  s.stateLockManager.lockedStates.lockedStates,
		rlockedStates: s.stateLockManager.lockedStates.rlockedStates,
		finishedTxs:   s.stateLockManager.finishedTxs,
		timestamp:     time.Now(),
	}
	s.txLock.Unlock()

	// === 为版本化创建快照 ===
	return s.handleLockCommit(newRoot)
}

// ============================================================================
// Pending Unlock Cache
//
// pendingUnlockCache 跟踪哪些 txHash 需要在 Commit 时从 stateLockManager 中解锁。
//
// 为什么需要延迟解锁（而不是立即解锁）：
//   锁操作（Lock/Unlock）发生在 stateLocker.pendingStates 中，是隔离的。
//   当 CommitTx/RollbackTx 被调用时，只在 pendingStates 中释放了锁，
//   但全局 stateLockManager.lockedStates 并不知道细粒度的锁释放过程。
//
//   只有 Commit(newRoot) 合并 pendingStates 到全局状态时，
//   才需要将 pendingUnlocks 中记录的已释放交易的锁从全局状态中删除。
//
// ============================================================================

// pendingUnlockCache 记录需要在 Commit 时从 stateLockManager 解锁的 txHash。
type pendingUnlockCache struct {
	unlockedTxs map[common.Hash]bool // 需要从 stateLockManager 解锁的 txHash
}

// addTx 标记一个 txHash 为待解锁。
func (p *pendingUnlockCache) addTx(txHash common.Hash) {
	p.unlockedTxs[txHash] = true
}

// applyTo 将 pending 解锁应用到 stateLockManager。
// 对于每个已记录的 txHash，将其在 callIndex2lockedState 中的所有锁从全局状态删除。
func (p *pendingUnlockCache) applyTo(mgr *stateLockManager) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	for txHash := range p.unlockedTxs {
		mgr.sscService.stats.PendingUnlockTotal.Add(1)
		mgr.sscService.stats.PendingUnlockBatch.Add(1)
		// 获取此 tx 的所有写锁并从全局状态删除
		c2ls := mgr.lockedStates.getCallIndex2LockedStates(txHash)
		if c2ls != nil {
			for callIndex, lockKeyMap := range c2ls {
				for lockKey := range lockKeyMap {
					// 从 lockedStates 解锁
					if ls, exists := mgr.lockedStates.lockedStates[lockKey]; exists {
						heldNs := time.Since(ls.lockTime).Nanoseconds()
						mgr.sscService.stats.RecordLockHeld(string(lockKey), heldNs)
						delete(mgr.lockedStates.lockedStates, lockKey)
						utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
							Str("callIndex", callIndex).
							Str("key", string(lockKey)).
							Msg("applied write unlock to stateLockManager")
					}
				}
				// 清理 callIndex 映射
				delete(c2ls, callIndex)
			}
		}

		// 获取此 tx 的所有读锁并从全局状态删除
		c2rls := mgr.lockedStates.getCallIndex2RLockedStates(txHash)
		if c2rls != nil {
			for callIndex, lockKeyMap := range c2rls {
				for lockKey := range lockKeyMap {
					// 从 rlockedStates 解锁
					if rls, exists := mgr.lockedStates.rlockedStates[lockKey]; exists {
						heldNs := time.Since(rls.lockTime).Nanoseconds()
						mgr.sscService.stats.RecordLockHeld(string(lockKey), heldNs)
						delete(mgr.lockedStates.rlockedStates, lockKey)
						utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
							Str("callIndex", callIndex).
							Str("key", string(lockKey)).
							Msg("applied read unlock to stateLockManager")
					}
				}
				// 清理 callIndex 映射
				delete(c2rls, callIndex)
			}
		}

		// 清理 txHash 映射
		delete(mgr.lockedStates.callIndex2lockedState, txHash)
		delete(mgr.lockedStates.callIndex2rlockedState, txHash)
	}
}
