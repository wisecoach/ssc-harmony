package vm

import (
	"github.com/ethereum/go-ethereum/common"
)

var (
	SimulationCommitAddr    = common.Address([20]byte{249})
	CxtCommitOrRollbackAddr = common.Address([20]byte{250})
	EmptyAddr               = common.Address([20]byte{251})
)

var WriteCapablePrecompiledSSCContracts = map[common.Address]WriteCapablePrecompiledSSCContract{
	SimulationCommitAddr:    &simulationCommit{},
	CxtCommitOrRollbackAddr: &cxtCommitOrRollback{},
}

var SSCAddrsApplyOnChain = map[common.Address]interface{}{
	SimulationCommitAddr:    struct{}{},
	CxtCommitOrRollbackAddr: struct{}{},
}

type WriteCapablePrecompiledSSCContract interface {
	// RequiredGas calculates the contract gas use
	RequiredGas(vm *SSCVM, contract *Contract, input []byte) (uint64, error)
	// RunWriteCapable use a different name from read-only contracts to be safe
	RunWriteCapable(vm *SSCVM, contract *Contract, input []byte) ([]byte, error)
}

type simulationCommit struct {
}

func (s *simulationCommit) RequiredGas(vm *SSCVM, contract *Contract, input []byte) (uint64, error) {
	return 0, nil
}

func (s *simulationCommit) RunWriteCapable(vm *SSCVM, contract *Contract, input []byte) ([]byte, error) {
	vm.SSCService.VerifySimulation(input, vm.PreExec)
	return nil, nil
}

type cxtCommitOrRollback struct {
}

func (c *cxtCommitOrRollback) RequiredGas(vm *SSCVM, contract *Contract, input []byte) (uint64, error) {
	return 0, nil
}

func (c *cxtCommitOrRollback) RunWriteCapable(vm *SSCVM, contract *Contract, input []byte) ([]byte, error) {
	// unlock when it's executed on chain
	err := vm.SSCService.CommitOrRollbackWithProof(input, vm.PreExec)
	if err != nil {
		return nil, err
	}
	return nil, nil
}
