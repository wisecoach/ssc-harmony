package ssc

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/pkg/errors"
)

type stateLocker struct {
	txLock  sync.RWMutex
	root    common.Hash
	stateDB api.StateDB
	*stateLockManager

	tmpFinishedTxs map[common.Hash]bool
	journal        *lockJournal
	validRevisions []revision
	nextRevisionId int
}

func (s *stateLocker) BindStateDB(root common.Hash, stateDB api.StateDB) {
	s.stateDB = stateDB
	s.root = root
}

func (s *stateLocker) RLockable(key api.LockKey, txHash common.Hash) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if ls, exists := s.lockedStates.lockedStates[key]; exists {
		return errors.Wrap(api.ErrLockedByOtherTx, fmt.Sprintf("locked by tx %s", ls.lockedBy.Hex()))
	}
	return nil
}

func (s *stateLocker) Lockable(key api.LockKey, txHash common.Hash) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Str("key", string(key)).
		Str("root", s.root.Hex()).
		Int("lockedStates", s.lockedStates.length()).
		Msgf("check loackable")

	if ls, exists := s.lockedStates.lockedStates[key]; exists {
		if bytes.Compare(ls.lockedBy.Bytes(), txHash.Bytes()) != 0 {
			utils.SSCLogger().Warn().Str("txHash", txHash.Hex()).
				Str("key", string(key)).
				Str("root", s.root.Hex()).
				Msgf("key is locked by other tx %s", ls.lockedBy.Hex())
			return errors.Wrap(api.ErrLockedByOtherTx, fmt.Sprintf("locked by tx %s", ls.lockedBy.Hex()))
		}
	}

	if ls, exists := s.lockedStates.rlockedStates[key]; exists {
		return errors.Wrap(api.ErrLockedByOtherTx, fmt.Sprintf("rlocked by tx %s", ls.lockedBy))
	}
	return nil
}

func (s *stateLocker) Lock(txHash common.Hash, callIndex api.CallIndex, key api.LockKey, value common.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ls, exists := s.lockedStates.lockedStates[key]
	if !exists {
		ls = &lockedState{}
	}
	ls.lockedBy = txHash
	ls.lockTime = time.Now()
	s.lockedStates.addLockedState(txHash, callIndex.ToString(), key, value, ls)
	s.journal.append(&lockEntry{
		txHash:    txHash,
		callIndex: callIndex,
		key:       key,
		value:     value,
	})
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Str("callIndex", callIndex.ToString()).
		Str("key", string(key)).
		Msgf("mu state, locked %d state, journal %d", s.lockedStates.length(), s.journal.length())
	return nil
}

func (s *stateLocker) RLock(txHash common.Hash, callIndex api.CallIndex, key api.LockKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ls, exists := s.lockedStates.rlockedStates[key]
	if !exists {
		ls = &rlockedState{}
	}
	ls.lockedBy = append(ls.lockedBy, txHash)
	ls.lockTime = time.Now()
	s.lockedStates.addRLockedState(txHash, callIndex.ToString(), key, ls)
	s.journal.append(&rlockEntry{
		txHash:    txHash,
		callIndex: callIndex,
		key:       key,
	})
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Str("callIndex", callIndex.ToString()).
		Str("key", string(key)).
		Msgf("mu state, rlocked %d state, journal %d", s.lockedStates.length(), s.journal.length())
	return nil
}

func (s *stateLocker) unlock(txHash common.Hash, callIndexStr string, key api.LockKey, rollback bool, newValue common.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if state, exists := s.lockedStates.lockedStates[key]; exists {
		value := s.lockedStates.deleteLockedState(txHash, callIndexStr, key)
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Msg("unlock state, waiting txs Num2wait")
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

	if state, exists := s.lockedStates.rlockedStates[key]; exists {
		s.lockedStates.deleteRLockedState(txHash, callIndexStr, key)
		s.journal.append(&unlockRLockEntry{
			state:        state,
			txHash:       txHash,
			callIndexStr: callIndexStr,
			key:          key,
		})
	}
	return nil
}

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

func (s *stateLocker) CommitTx(txHash common.Hash) error {
	s.txLock.Lock()
	defer s.txLock.Unlock()

	c2ls := s.lockedStates.getCallIndex2LockedStates(txHash)
	if c2ls != nil {
		for callIndex, lockKeyMap := range c2ls {
			for lockKey, _ := range lockKeyMap {
				err := s.unlock(txHash, callIndex, lockKey, false, common.Hash{})
				if err != nil {
					return err
				}
			}
		}
	}
	c2rls := s.lockedStates.getCallIndex2RLockedStates(txHash)
	if c2rls != nil {
		for callIndex, lockKeyMap := range c2rls {
			for lockKey, _ := range lockKeyMap {
				err := s.unlockRLock(txHash, callIndex, lockKey, false, common.Hash{})
				if err != nil {
					return err
				}
			}
		}
	}
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

	c2ls := s.lockedStates.getCallIndex2LockedStates(txHash)
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
	c2rls := s.lockedStates.getCallIndex2RLockedStates(txHash)
	if c2rls != nil {
		for callIndex, lockKeyMap := range c2rls {
			for lockKey, _ := range lockKeyMap {
				err := s.unlockRLock(txHash, callIndex, lockKey, true, common.Hash{})
				if err != nil {
					return err
				}
			}
		}
	}
	s.tmpFinishedTxs[txHash] = false
	s.journal.append(&finishTxEntry{
		txHash:           txHash,
		commitOrRollback: false,
	})
	return nil
}

func (s *stateLocker) Commit() error {
	utils.SSCLogger().Debug().Msgf("Commit state mu, tmpFinishedTxs: %d",
		len(s.tmpFinishedTxs))
	s.txLock.Lock()
	// commit all temporary locked States to the committed States
	for hash, b := range s.tmpFinishedTxs {
		s.finishedTxs[hash] = b
	}
	// clear temporary States
	s.journal = &lockJournal{}
	s.validRevisions = make([]revision, 0)
	s.nextRevisionId = 0
	s.txLock.Unlock()

	return s.handleLockCommit()
}
