package api

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/pkg/errors"
	"strings"
)

var (
	ErrLockedByOtherTx  = errors.New("state is locked by other tx")
	ErrTxHasBeenClosed  = errors.New("tx has been closed")
	ErrTxNotExist       = errors.New("transaction is not exist")
	ErrDeadLockDetected = errors.New("dead lock detected")
)

type LockKey string

func (k LockKey) Value() (common.Address, common.Hash) {
	values := strings.Split(string(k), ":")
	return common.HexToAddress(values[0]), common.HexToHash(values[1])
}

type LockType int

const (
	SharedLock LockType = iota
	ExclusiveLock
)

func FormKey(address common.Address, key common.Hash) LockKey {
	return LockKey(address.Hex() + ":" + key.Hex())
}

// StateLockManager
// @Description: save the locked state and build StateLocker to update locked state
type StateLockManager interface {
	// GetLocker
	//  @Description: build and return a state locker
	//  @return StateLocker
	//
	GetLocker() StateLocker

	// Subscribe
	//  @Description: subscribe the tx for the states
	//  @param simulation
	//  @return error
	//
	Subscribe(txHash common.Hash, nonce uint64, sender common.Address, states map[LockKey]interface{}, originShardId uint32, nextSimulationNum int, simulateOrVerify bool) error

	// UnSubscribe
	//  @Description: unsubscribe the tx
	//  @param txHash
	//  @return error
	//
	UnSubscribe(txHash common.Hash) error

	NotifyReSimulationStart(txHash common.Hash)
}

// StateLocker
//
//	 @Description: used to manage lock of state
//		Note: it's not thread safe
type StateLocker interface {

	// BindStateDB
	//  @Description: Bind the state locker to a stateDB
	//
	BindStateDB(stateDB StateDB)

	// Locked
	//  @Description: Check if the key is available for locking.
	//  @return error ErrLockedByOtherTx if the key is locked by other transaction.
	Locked(key LockKey) error

	// CheckLockable
	//  @Description: Check if the key is available for reentrant locking.
	//  @param txHash	the reentrant transaction hash.
	//  @return bool	if the key is available for reentrant locking.
	CheckLockable(key LockKey, txHash common.Hash, lockType LockType) error

	Lock(txHash common.Hash, callIndex CallIndex, key LockKey, value common.Hash, lockType LockType) error

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
	Commit() error
}
