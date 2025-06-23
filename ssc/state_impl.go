package ssc

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/ssc/api"
	"math/big"
)

func (s *sscService) GetRWSet(txHash common.Hash) *api.RWSet {
	s.stateLock.RLock()
	defer s.stateLock.RUnlock()

	state := s.simulationState[txHash]
	if state == nil {
		return nil
	}

	callState := state.SimulationCallStates[state.SimulationNum].Get(state.CallIndex)
	if callState == nil {
		return nil
	}

	return callState.RWSet
}

func (s *sscService) EndCTX(txHash common.Hash) {
	s.stateLock.Lock()
	delete(s.simulationState, txHash)
	s.stateLock.Unlock()
}

func (s *sscService) CreateAccount(txHash common.Hash, address common.Address) {
	rwset := s.GetRWSet(txHash)
	rwset.CurrentState.Balance[address] = new(big.Int)
}

func (s *sscService) SubBalance(db api.StateDB, txHash common.Hash, address common.Address, balance *big.Int) {
	rwset := s.GetRWSet(txHash)
	bal := rwset.CurrentState.Balance[address]
	if bal == nil {
		bal = db.GetBalance(address)
		rwset.ReadState.Balance[address] = bal
	}
	newBal := bal.Sub(bal, balance)
	rwset.CurrentState.Balance[address] = newBal
	rwset.WriteState.Balance[address] = newBal
}

func (s *sscService) AddBalance(db api.StateDB, txHash common.Hash, address common.Address, balance *big.Int) {
	rwset := s.GetRWSet(txHash)
	bal := rwset.CurrentState.Balance[address]
	if bal == nil {
		bal = db.GetBalance(address)
		rwset.ReadState.Balance[address] = bal
	}
	newBal := bal.Add(bal, balance)
	rwset.CurrentState.Balance[address] = newBal
	rwset.WriteState.Balance[address] = newBal
}

func (s *sscService) GetBalance(db api.StateDB, txHash common.Hash, address common.Address) *big.Int {
	rwset := s.GetRWSet(txHash)
	bal := rwset.CurrentState.Balance[address]
	if bal == nil {
		bal = db.GetBalance(address)
		rwset.ReadState.Balance[address] = bal
		rwset.CurrentState.Balance[address] = bal
	}
	return bal
}

func (s *sscService) GetState(db api.StateDB, txHash common.Hash, address common.Address, key common.Hash) (common.Hash, error) {
	rwset := s.GetRWSet(txHash)
	if rwset.ReadState.State[address] == nil {
		rwset.ReadState.State[address] = make(map[common.Hash]common.Hash)
	}
	if rwset.CurrentState.State[address] == nil {
		rwset.CurrentState.State[address] = make(map[common.Hash]common.Hash)
	}
	if value, exists := rwset.CurrentState.State[address][key]; !exists {
		val, _ := db.GetState(address, key)
		rwset.CurrentState.State[address][key] = val
		rwset.ReadState.State[address][key] = val
		return val, nil
	} else {
		return value, nil
	}
}

func (s *sscService) SetState(db api.StateDB, txHash common.Hash, address common.Address, key common.Hash, value common.Hash) error {
	rwset := s.GetRWSet(txHash)
	if rwset.CurrentState.State[address] == nil {
		rwset.CurrentState.State[address] = make(map[common.Hash]common.Hash)
	}
	if rwset.WriteState.State[address] == nil {
		rwset.WriteState.State[address] = make(map[common.Hash]common.Hash)
	}
	rwset.CurrentState.State[address][key] = value
	rwset.WriteState.State[address][key] = value
	return nil
}

func (s *sscService) SubSimuBalance(txHash common.Hash, address common.Address, amount *big.Int) error {
	verifyContext := s.executionVerifyContexts[txHash]
	if verifyContext.CurrentState.Balance[address] == nil {
		balance, exists := verifyContext.CallStateMap[verifyContext.CallIndex.ToString()].RWSet.ReadState.Balance[address]
		if !exists {
			return api.ErrInvalidExecution
		}
		verifyContext.CurrentState.Balance[address] = balance
	}
	verifyContext.CurrentState.Balance[address].Sub(verifyContext.CurrentState.Balance[address], amount)
	return nil
}

func (s *sscService) AddSimuBalance(txHash common.Hash, address common.Address, balance *big.Int) error {
	verifyContext := s.executionVerifyContexts[txHash]
	if verifyContext.CurrentState.Balance[address] == nil {
		bal, exists := verifyContext.CallStateMap[verifyContext.CallIndex.ToString()].RWSet.ReadState.Balance[address]
		if !exists {
			return api.ErrInvalidExecution
		}
		verifyContext.CurrentState.Balance[address] = bal
	}
	verifyContext.CurrentState.Balance[address].Add(verifyContext.CurrentState.Balance[address], balance)
	return nil
}

func (s *sscService) GetSimuBalance(txHash common.Hash, address common.Address) (*big.Int, error) {
	verifyContext := s.executionVerifyContexts[txHash]
	if verifyContext.CurrentState.Balance[address] == nil {
		bal, exists := verifyContext.CallStateMap[verifyContext.CallIndex.ToString()].RWSet.ReadState.Balance[address]
		if !exists {
			return nil, api.ErrInvalidExecution
		}
		verifyContext.CurrentState.Balance[address] = bal
	}
	return verifyContext.CurrentState.Balance[address], nil
}

func (s *sscService) GetSimuState(txHash common.Hash, address common.Address, key common.Hash) (common.Hash, error) {
	verifyContext := s.executionVerifyContexts[txHash]
	if verifyContext.CurrentState.State[address] != nil {
		return verifyContext.CurrentState.State[address][key], nil
	}
	state, exists := verifyContext.CallStateMap[verifyContext.CallIndex.ToString()].RWSet.ReadState.State[address]
	if !exists {
		return common.Hash{}, api.ErrInvalidExecution
	}
	return state[key], nil
}

func (s *sscService) SetSimuState(txHash common.Hash, address common.Address, key common.Hash, value common.Hash) error {
	verifyContext := s.executionVerifyContexts[txHash]
	if verifyContext.CurrentState.State[address] == nil {
		verifyContext.CurrentState.State[address] = make(map[common.Hash]common.Hash)
	}
	verifyContext.CurrentState.State[address][key] = value
	return nil
}

func (s *sscService) GetResult(txHash common.Hash) (result []byte, leftOverGas uint64, err error) {
	verifyContext := s.executionVerifyContexts[txHash]
	if verifyContext == nil {
		return nil, 0, api.ErrInvalidExecution
	}
	ret := verifyContext.DependentResults[verifyContext.CurrentIndex]
	verifyContext.CurrentIndex++
	return ret.Result, ret.LeftOverGas, nil
}
