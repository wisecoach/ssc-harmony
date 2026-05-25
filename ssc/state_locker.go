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

// lockedStatesSnapshot is an immutable snapshot of locked states at a specific stateRoot.
// It does not include journal - only the committed state.
type lockedStatesSnapshot struct {
	lockedStates  map[api.LockKey]*lockedState
	rlockedStates map[api.LockKey]*rlockedState
	finishedTxs   map[common.Hash]bool
	timestamp     time.Time // for TTL-based eviction
}

// snapshotMetadata tracks additional info for eviction decisions
type snapshotMetadata struct {
	lastAccess  time.Time
	accessCount uint64
}

// ============================================================================
// stateLocker with Versioning Support
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
// Lock/Unlock Operations (unchanged from original)
// ============================================================================
func (s *stateLocker) RLockable(key api.LockKey, txHash common.Hash) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check base snapshot (read-only, immutable)
	if ls, exists := s.baseSnapshot.lockedStates[key]; exists {
		return errors.Wrap(api.ErrLockedByOtherTx, fmt.Sprintf("locked by tx %s", ls.lockedBy.Hex()))
	}

	// Check pending states (uncommitted changes from this locker)
	if ls, exists := s.pendingStates.lockedStates[key]; exists {
		return errors.Wrap(api.ErrLockedByOtherTx, fmt.Sprintf("locked by tx %s in pending states", ls.lockedBy.Hex()))
	}

	return nil
}

func (s *stateLocker) Lockable(key api.LockKey, txHash common.Hash) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Str("key", string(key)).
		Str("root", s.root.Hex()).
		Int("baseSnapshot", len(s.baseSnapshot.lockedStates)).
		Int("pendingStates", s.pendingStates.length()).
		Msgf("check lockable")

	// Check base snapshot (read-only, immutable)
	if ls, exists := s.baseSnapshot.lockedStates[key]; exists {
		if !ls.lockable(txHash) {
			utils.SSCLogger().Warn().Str("txHash", txHash.Hex()).
				Str("key", string(key)).
				Str("root", s.root.Hex()).
				Msgf("key is locked by other tx %s in base snapshot", ls.lockedBy.Hex())
			return errors.Wrap(api.ErrLockedByOtherTx, fmt.Sprintf("locked by tx %s", ls.lockedBy.Hex()))
		}
	}

	// Check pending states (uncommitted changes from this locker)
	if ls, exists := s.pendingStates.lockedStates[key]; exists {
		if !ls.lockable(txHash) {
			utils.SSCLogger().Warn().Str("txHash", txHash.Hex()).
				Str("key", string(key)).
				Str("root", s.root.Hex()).
				Msgf("key is locked by other tx %s in pending states", ls.lockedBy.Hex())
			return errors.Wrap(api.ErrLockedByOtherTx, fmt.Sprintf("locked by tx %s", ls.lockedBy.Hex()))
		}
	}

	// Check read locks in base snapshot
	if ls, exists := s.baseSnapshot.rlockedStates[key]; exists {
		return errors.Wrap(api.ErrLockedByOtherTx, fmt.Sprintf("rlocked by tx %s in base snapshot", ls.lockedBy))
	}

	// Check read locks in pending states
	if ls, exists := s.pendingStates.rlockedStates[key]; exists {
		return errors.Wrap(api.ErrLockedByOtherTx, fmt.Sprintf("rlocked by tx %s in pending states", ls.lockedBy))
	}

	return nil
}

func (s *stateLocker) Lock(txHash common.Hash, callIndex api.CallIndex, key api.LockKey, value common.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Write to pendingStates (isolated, not visible to other transactions until Commit)
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

func (s *stateLocker) RLock(txHash common.Hash, callIndex api.CallIndex, key api.LockKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Write to pendingStates (isolated, not visible to other transactions until Commit)
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

func (s *stateLocker) unlock(txHash common.Hash, callIndexStr string, key api.LockKey, rollback bool, newValue common.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Unlock from pendingStates
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

func (s *stateLocker) unlockRLock(txHash common.Hash, callIndexStr string, key api.LockKey, rollback bool, newValue common.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Unlock from pendingStates
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
// Snapshot and Revert (transaction-level, unchanged)
// ============================================================================

func (s *stateLocker) Snapshot() int {
	id := s.nextRevisionId
	s.nextRevisionId++
	s.validRevisions = append(s.validRevisions, revision{id, s.journal.length()})
	return id
}

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
// ============================================================================

func (s *stateLocker) CommitTx(txHash common.Hash) error {
	s.txLock.Lock()
	defer s.txLock.Unlock()

	// Unlock from pendingStates (transaction-level commit)
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

	// Track this tx for unlocking from stateLockManager on Commit
	s.pendingUnlocks.addTx(txHash)

	s.tmpFinishedTxs[txHash] = true
	s.journal.append(&finishTxEntry{
		txHash:           txHash,
		commitOrRollback: true,
	})
	return nil
}

func (s *stateLocker) RollbackTx(txHash common.Hash) error {
	s.txLock.Lock()
	defer s.txLock.Unlock()

	// Rollback from pendingStates (transaction-level rollback)
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

	// Track this tx for unlocking from stateLockManager on Commit
	s.pendingUnlocks.addTx(txHash)

	s.tmpFinishedTxs[txHash] = false
	s.journal.append(&finishTxEntry{
		txHash:           txHash,
		commitOrRollback: false,
	})
	return nil
}

// ============================================================================
// Block-level Commit with Versioning
// ============================================================================

// Commit commits the current locker state and creates a snapshot for the given stateRoot.
// This should be called in sync with stateDB.Commit().
// It merges pendingStates (uncommitted changes) into the global stateLockManager.
func (s *stateLocker) Commit(newRoot common.Hash) error {
	s.txLock.Lock()

	// === Merge pendingStates into global stateLockManager ===
	// This is the key isolation point: changes only become visible globally after Commit
	s.stateLockManager.mu.Lock()

	utils.SSCLogger().Info().
		Int("lockedStates", len(s.stateLockManager.lockedStates.lockedStates)).
		Int("pendingLockedStates", len(s.pendingStates.lockedStates)).
		Int("lockedTxs", len(s.stateLockManager.lockedStates.callIndex2lockedState)).
		Int("unlockedTxs", len(s.pendingUnlocks.unlockedTxs)).
		Msgf("Commit state mu, pendingStates: %d, tmpFinishedTxs: %d, root: %s",
			s.pendingStates.length(), len(s.tmpFinishedTxs), s.root.Hex())

	for key, pendingState := range s.pendingStates.lockedStates {
		// Copy pending write-lock to global state
		s.stateLockManager.lockedStates.lockedStates[key] = &lockedState{
			lockedBy:   pendingState.lockedBy,
			lockTime:   pendingState.lockTime,
			hasTimeout: pendingState.hasTimeout,
		}
	}
	for key, pendingState := range s.pendingStates.rlockedStates {
		// Copy pending read-lock to global state
		s.stateLockManager.lockedStates.rlockedStates[key] = &rlockedState{
			lockedBy:   append([]common.Hash{}, pendingState.lockedBy...),
			lockTime:   pendingState.lockTime,
			hasTimeout: pendingState.hasTimeout,
		}
	}

	// Merge callIndex mappings for proper unlock tracking
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

	// === Apply pending unlocks to stateLockManager ===
	// This unlocks txHash-level locks from global state after merge
	s.pendingUnlocks.applyTo(s.stateLockManager)

	// Commit all temporary locked States to the committed States
	for hash, b := range s.tmpFinishedTxs {
		s.finishedTxs[hash] = b
	}
	// Clear temporary States
	s.journal = &lockJournal{}
	s.validRevisions = make([]revision, 0)
	s.nextRevisionId = 0
	// Clear pendingStates after successful merge
	s.pendingStates = &lockedStates{
		lockedStates:           make(map[api.LockKey]*lockedState),
		rlockedStates:          make(map[api.LockKey]*rlockedState),
		callIndex2lockedState:  make(map[common.Hash]map[string]map[api.LockKey]common.Hash),
		callIndex2rlockedState: make(map[common.Hash]map[string]map[api.LockKey]struct{}),
	}
	// Clear pendingUnlocks after successful application
	s.pendingUnlocks = &pendingUnlockCache{
		unlockedTxs: make(map[common.Hash]bool),
	}
	// Update baseSnapshot to point to the new committed state (optional, for consistency)
	// The locker is typically not used after Commit, but we update for safety
	s.baseSnapshot = &lockedStatesSnapshot{
		lockedStates:  s.stateLockManager.lockedStates.lockedStates,
		rlockedStates: s.stateLockManager.lockedStates.rlockedStates,
		finishedTxs:   s.stateLockManager.finishedTxs,
		timestamp:     time.Now(),
	}
	s.txLock.Unlock()

	// === Create snapshot for versioning ===
	return s.handleLockCommit(newRoot)
}

// ============================================================================
// Pending Unlock Cache
// ============================================================================

// pendingUnlockCache tracks txHashes that need to be unlocked from stateLockManager on Commit
type pendingUnlockCache struct {
	unlockedTxs map[common.Hash]bool // txHashes to unlock from stateLockManager
}

// addTx marks a txHash to be unlocked on Commit
func (p *pendingUnlockCache) addTx(txHash common.Hash) {
	p.unlockedTxs[txHash] = true
}

// applyTo applies pending unlocks to the stateLockManager
// For each tracked txHash, it unlocks all keys in callIndex2lockedState[txHash]
func (p *pendingUnlockCache) applyTo(mgr *stateLockManager) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	for txHash := range p.unlockedTxs {
		// Get all lockKeys for this tx from callIndex2lockedState
		c2ls := mgr.lockedStates.getCallIndex2LockedStates(txHash)
		if c2ls != nil {
			for callIndex, lockKeyMap := range c2ls {
				for lockKey := range lockKeyMap {
					// Unlock from lockedStates
					if _, exists := mgr.lockedStates.lockedStates[lockKey]; exists {
						delete(mgr.lockedStates.lockedStates, lockKey)
						utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
							Str("callIndex", callIndex).
							Str("key", string(lockKey)).
							Msg("applied write unlock to stateLockManager")
					}
				}
				// Clean up callIndex mapping
				delete(c2ls, callIndex)
			}
		}

		// Also unlock from rlockedStates
		c2rls := mgr.lockedStates.getCallIndex2RLockedStates(txHash)
		if c2rls != nil {
			for callIndex, lockKeyMap := range c2rls {
				for lockKey := range lockKeyMap {
					// Unlock from rlockedStates
					if _, exists := mgr.lockedStates.rlockedStates[lockKey]; exists {
						delete(mgr.lockedStates.rlockedStates, lockKey)
						utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
							Str("callIndex", callIndex).
							Str("key", string(lockKey)).
							Msg("applied read unlock to stateLockManager")
					}
				}
				// Clean up callIndex mapping
				delete(c2rls, callIndex)
			}
		}

		// Clean up txHash mapping
		delete(mgr.lockedStates.callIndex2lockedState, txHash)
		delete(mgr.lockedStates.callIndex2rlockedState, txHash)
	}
}
