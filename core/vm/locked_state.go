package vm

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/core/state"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/pkg/errors"
	"math/big"
	"sort"
)

var (
	ErrLockedByOtherTx = errors.New("state is locked by other tx")
)

type lockKey string

func formKey(address common.Address, key common.Hash) lockKey {
	return lockKey(address.Hex() + key.Hex())
}

func NewLockableStateWrapper() *LockableState {
	return &LockableState{
		lockedStates:          make(map[lockKey]common.Hash),
		callIndex2lockedState: make(map[common.Hash]map[string]map[common.Address]map[common.Hash]common.Hash),
		lockedAddBalance:      make(map[common.Hash]map[string]map[common.Address]*big.Int),
		lockedSubBalance:      make(map[common.Hash]map[string]map[common.Address]*big.Int),
	}
}

type LockableState struct {
	*state.DB

	lockedStates          map[lockKey]common.Hash                                                   // address + key -> txHash, used for lock
	callIndex2lockedState map[common.Hash]map[string]map[common.Address]map[common.Hash]common.Hash // txHash -> callIndex -> address -> key -> oldValue, used for unlock

	lockedAddBalance map[common.Hash]map[string]map[common.Address]*big.Int // txHash -> callIndex -> address -> freezeAddBalance
	lockedSubBalance map[common.Hash]map[string]map[common.Address]*big.Int // txHash -> callIndex -> address -> freezeSubBalance
}

func (s *LockableState) WithDB(db *state.DB) *LockableState {
	ls := &LockableState{
		DB:                    db,
		lockedStates:          s.lockedStates,
		callIndex2lockedState: s.callIndex2lockedState,
		lockedAddBalance:      s.lockedAddBalance,
		lockedSubBalance:      s.lockedSubBalance,
	}
	return ls
}

func (s *LockableState) GetState(address common.Address, key common.Hash) (common.Hash, error) {
	_, exists := s.lockedStates[formKey(address, key)]
	if exists {
		return common.Hash{}, ErrLockedByOtherTx
	}
	return s.DB.GetState(address, key)
}

func (s *LockableState) SetState(address common.Address, key common.Hash, value common.Hash) error {
	_, exists := s.lockedStates[formKey(address, key)]
	if exists {
		return ErrLockedByOtherTx
	}
	return s.DB.SetState(address, key, value)
}

func (s *LockableState) GetStateWithLock(txHash common.Hash, callIndex api.CallIndex, address common.Address, key common.Hash) (common.Hash, error) {
	lockedTx, exists := s.lockedStates[formKey(address, key)]
	if exists && lockedTx != txHash {
		return common.Hash{}, ErrLockedByOtherTx
	}
	if s.callIndex2lockedState[txHash] == nil {
		s.callIndex2lockedState[txHash] = make(map[string]map[common.Address]map[common.Hash]common.Hash)
	}
	if s.callIndex2lockedState[txHash][callIndex.ToString()] == nil {
		s.callIndex2lockedState[txHash][callIndex.ToString()] = make(map[common.Address]map[common.Hash]common.Hash)
	}
	if s.callIndex2lockedState[txHash][callIndex.ToString()][address] == nil {
		s.callIndex2lockedState[txHash][callIndex.ToString()][address] = make(map[common.Hash]common.Hash)
	}
	s.callIndex2lockedState[txHash][callIndex.ToString()][address][key], _ = s.DB.GetState(address, key)
	s.lockedStates[formKey(address, key)] = txHash
	return s.DB.GetState(address, key)
}

func (s *LockableState) SetStateWithLock(txHash common.Hash, callIndex api.CallIndex, address common.Address, key common.Hash, value common.Hash) error {
	lockedTx, exists := s.lockedStates[formKey(address, key)]
	if exists && lockedTx != txHash {
		return ErrLockedByOtherTx
	}
	if s.callIndex2lockedState[txHash] == nil {
		s.callIndex2lockedState[txHash] = make(map[string]map[common.Address]map[common.Hash]common.Hash)
	}
	if s.callIndex2lockedState[txHash][callIndex.ToString()] == nil {
		s.callIndex2lockedState[txHash][callIndex.ToString()] = make(map[common.Address]map[common.Hash]common.Hash)
	}
	if s.callIndex2lockedState[txHash][callIndex.ToString()][address] == nil {
		s.callIndex2lockedState[txHash][callIndex.ToString()][address] = make(map[common.Hash]common.Hash)
	}
	s.callIndex2lockedState[txHash][callIndex.ToString()][address][key], _ = s.DB.GetState(address, key)
	s.lockedStates[formKey(address, key)] = txHash
	return s.DB.SetState(address, key, value)
}

func (s *LockableState) AddBalanceWithLock(txHash common.Hash, callIndex api.CallIndex, address common.Address, amount *big.Int) error {
	if s.lockedAddBalance[txHash] == nil {
		s.lockedAddBalance[txHash] = make(map[string]map[common.Address]*big.Int)
	}
	if s.lockedAddBalance[txHash][callIndex.ToString()] == nil {
		s.lockedAddBalance[txHash][callIndex.ToString()] = make(map[common.Address]*big.Int)
	}
	if s.lockedAddBalance[txHash][callIndex.ToString()][address] == nil {
		s.lockedAddBalance[txHash][callIndex.ToString()][address] = big.NewInt(0)
	}
	s.lockedAddBalance[txHash][callIndex.ToString()][address].Add(s.lockedAddBalance[txHash][callIndex.ToString()][address], amount)
	return nil
}

func (s *LockableState) SubBalanceWithLock(txHash common.Hash, callIndex api.CallIndex, address common.Address, amount *big.Int) error {
	if s.lockedSubBalance[txHash] == nil {
		s.lockedSubBalance[txHash] = make(map[string]map[common.Address]*big.Int)
	}
	if s.lockedSubBalance[txHash][callIndex.ToString()] == nil {
		s.lockedSubBalance[txHash][callIndex.ToString()] = make(map[common.Address]*big.Int)
	}
	if s.lockedSubBalance[txHash][callIndex.ToString()][address] == nil {
		s.lockedSubBalance[txHash][callIndex.ToString()][address] = big.NewInt(0)
	}
	s.lockedSubBalance[txHash][callIndex.ToString()][address].Add(s.lockedSubBalance[txHash][callIndex.ToString()][address], amount)
	return nil
}

func (s *LockableState) Commit(unlock bool, txHash common.Hash) {
	// release freeze balance, add balance
	if s.lockedAddBalance[txHash] != nil {
		for _, addBalance := range s.lockedAddBalance[txHash] {
			for address, amount := range addBalance {
				s.DB.AddBalance(address, amount)
			}
		}
	}
	if unlock {
		delete(s.lockedAddBalance, txHash)
		delete(s.lockedSubBalance, txHash)
	}

	if s.callIndex2lockedState[txHash] != nil {
		stateMap := make(map[common.Address]map[common.Hash]common.Hash)
		for _, addr2state := range s.callIndex2lockedState[txHash] {
			for address, keyValues := range addr2state {
				for key, value := range keyValues {
					if stateMap[address] == nil {
						stateMap[address] = make(map[common.Hash]common.Hash)
					}
					stateMap[address][key] = value
				}
			}
		}
		for address, keyValues := range stateMap {
			for key, value := range keyValues {
				if unlock {
					delete(s.lockedStates, formKey(address, key))
				}
				s.DB.SetState(address, key, value)
			}
		}
	}
	if unlock {
		delete(s.callIndex2lockedState, txHash)
	}
}

func (s *LockableState) RollbackCall(txHash common.Hash, index api.CallIndex) {
	if s.callIndex2lockedState[txHash] != nil {
		lockState := s.callIndex2lockedState[txHash]
		if lockState[index.ToString()] != nil {
			for address, keyValues := range lockState[index.ToString()] {
				for key, oldValue := range keyValues {
					delete(s.lockedStates, formKey(address, key))
					s.DB.SetState(address, key, oldValue)
				}
			}
		}
	}
	if s.lockedAddBalance[txHash] != nil {
		if s.lockedAddBalance[txHash][index.ToString()] != nil {
			for address, amount := range s.lockedAddBalance[txHash][index.ToString()] {
				s.DB.SubBalance(address, amount)
			}
		}
	}
	if s.lockedSubBalance[txHash] != nil {
		if s.lockedSubBalance[txHash][index.ToString()] != nil {
			for address, amount := range s.lockedSubBalance[txHash][index.ToString()] {
				s.DB.AddBalance(address, amount)
			}
		}
	}
	delete(s.callIndex2lockedState[txHash], index.ToString())
	delete(s.lockedAddBalance[txHash], index.ToString())
	delete(s.lockedSubBalance[txHash], index.ToString())
}

func (s *LockableState) Rollback(unlock bool, txHash common.Hash) {
	// release freeze balance, sub balance
	if s.lockedAddBalance[txHash] != nil {
		for _, addBalance := range s.lockedSubBalance[txHash] {
			for address, amount := range addBalance {
				s.DB.AddBalance(address, amount)
			}
		}
	}
	if unlock {
		delete(s.lockedAddBalance, txHash)
		delete(s.lockedSubBalance, txHash)
	}

	if s.callIndex2lockedState[txHash] != nil {
		lockState := s.callIndex2lockedState[txHash]
		sortedCallIndex := make([]api.CallIndex, 0, len(lockState))
		for callIndex := range lockState {
			sortedCallIndex = append(sortedCallIndex, api.FromString(callIndex))
		}
		sort.Slice(sortedCallIndex, func(i, j int) bool {
			return sortedCallIndex[i].Compare(sortedCallIndex[j]) < 0
		})
		oldState := make(map[common.Address]map[common.Hash]common.Hash)
		for i := len(sortedCallIndex) - 1; i >= 0; i-- {
			callIndex := sortedCallIndex[i]
			for address, keyValues := range lockState[callIndex.ToString()] {
				if oldState[address] == nil {
					oldState[address] = make(map[common.Hash]common.Hash)
				}
				for key, oldValue := range keyValues {
					oldState[address][key] = oldValue
				}
			}
		}
		for address, keyValues := range oldState {
			for key, oldValue := range keyValues {
				delete(s.lockedStates, formKey(address, key))
				s.DB.SetState(address, key, oldValue)
			}
		}
	}
	delete(s.callIndex2lockedState, txHash)
}
