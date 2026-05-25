package ssc

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
)

type revision struct {
	id           int
	journalIndex int
}

type journalEntry interface {
	// revert undoes the changes introduced by this journal entry.
	revert(locker *stateLocker)
}

type lockEntry struct {
	txHash    common.Hash
	callIndex api.CallIndex
	key       api.LockKey
	value     common.Hash
}

func (l *lockEntry) revert(locker *stateLocker) {
	locker.mu.Lock()
	defer locker.mu.Unlock()

	// Revert from pendingStates (isolated transaction state)
	locker.pendingStates.deleteLockedState(l.txHash, l.callIndex.ToString(), l.key)
}

type rlockEntry struct {
	txHash    common.Hash
	callIndex api.CallIndex
	key       api.LockKey
}

func (l *rlockEntry) revert(locker *stateLocker) {
	locker.mu.Lock()
	defer locker.mu.Unlock()

	// Revert from pendingStates (isolated transaction state)
	locker.pendingStates.deleteRLockedState(l.txHash, l.callIndex.ToString(), l.key)
}

type unlockEntry struct {
	txHash       common.Hash
	state        *lockedState
	callIndexStr string
	key          api.LockKey
	newValue     common.Hash
	oldValue     common.Hash
	rollback     bool
}

func (l *unlockEntry) revert(locker *stateLocker) {
	func() {
		locker.mu.Lock()
		defer locker.mu.Unlock()
		// Revert to pendingStates (isolated transaction state)
		locker.pendingStates.addLockedState(l.txHash, l.callIndexStr, l.key, l.oldValue, l.state)
	}()

	if l.rollback {
		locker.mu.Lock()
		locker.pendingStates.setLockedValue(l.txHash, l.callIndexStr, l.key, l.oldValue)
		locker.mu.Unlock()

		addr, key := l.key.Value()
		locker.stateDB.SetState(l.txHash, addr, key, l.newValue)
	}
}

type unlockRLockEntry struct {
	txHash       common.Hash
	state        *rlockedState
	callIndexStr string
	key          api.LockKey
}

func (l *unlockRLockEntry) revert(locker *stateLocker) {
	locker.mu.Lock()
	defer locker.mu.Unlock()
	// Revert to pendingStates (isolated transaction state)
	locker.pendingStates.addRLockedState(l.txHash, l.callIndexStr, l.key, l.state)
}

type finishTxEntry struct {
	txHash           common.Hash
	commitOrRollback bool
}

func (l *finishTxEntry) revert(locker *stateLocker) {
	delete(locker.tmpFinishedTxs, l.txHash)
}

type lockJournal struct {
	entries []journalEntry
}

func (j *lockJournal) length() int {
	return len(j.entries)
}

func (j *lockJournal) append(entry journalEntry) {
	j.entries = append(j.entries, entry)
}

func (j *lockJournal) revert(locker *stateLocker, snapshot int) {
	utils.SSCLogger().Debug().Msgf("revert mu journal to snapshot %d, current %d", snapshot, len(j.entries))
	for i := len(j.entries) - 1; i >= snapshot; i-- {
		j.entries[i].revert(locker)
	}
	j.entries = j.entries[:snapshot]
}
