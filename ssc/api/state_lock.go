package api

import (
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/pkg/errors"
)

var (
	ErrLockConflict_OnChain  = errors.New("state is locked by other tx on chain")
	ErrLockConflict_OffChain = errors.New("state is locked by other tx off chain")
	ErrTxHasBeenClosed       = errors.New("tx has been closed")
	ErrTxNotExist            = errors.New("transaction is not exist")
	ErrDeadLockDetected      = errors.New("dead lock detected")
)

type LockKey string

func (k LockKey) Value() (common.Address, common.Hash) {
	values := strings.Split(string(k), ":")
	return common.HexToAddress(values[0]), common.HexToHash(values[1])
}

func FormKey(address common.Address, key common.Hash) LockKey {
	return LockKey(address.Hex() + ":" + key.Hex())
}

// StateLockManager
// @Description: save the locked state and build StateLocker to update locked state
type StateLockManager interface {
	InitLockManager(genesisRoot common.Hash) error
	GetLocker() StateLocker
	GetLockerAt(root common.Hash) (StateLocker, error)
}

// StateLocker
//
//	 @Description: used to manage lock of state
//		Note: it'S not thread safe
type StateLocker interface {

	// BindStateDB
	//  @Description: Bind the state locker to a stateDB
	//
	BindStateDB(root common.Hash, stateDB StateDB)

	Lockable(key LockKey, txHash common.Hash) error

	RLockable(key LockKey, txHash common.Hash) error

	Lock(txHash common.Hash, callIndex CallIndex, key LockKey, value common.Hash) error

	RLock(txHash common.Hash, callIndex CallIndex, key LockKey) error

	// Snapshot
	// @Description: create a snapshot of the current state lock, it should be called by stateDB
	Snapshot() int

	// RevertToSnapshot
	// @Description: revert the state lock to the snapshot with the given id, it should be called by stateDB
	RevertToSnapshot(id int)

	// CommitTx
	//  @Description:	commit tx, apply the locked state and unlock.
	CommitTx(txHash common.Hash) error

	// RollbackTx
	//  @Description:	rollback tx and unfreeze balance.
	RollbackTx(txHash common.Hash) error

	// Commit
	//  @Description: Commit the state lock, it should be called by stateDB
	Commit(newRoot common.Hash) error
}
