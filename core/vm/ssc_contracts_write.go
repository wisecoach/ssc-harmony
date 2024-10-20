package vm

import (
	"github.com/ethereum/go-ethereum/common"
)

var (
	SimulationCommitAddr   = common.Address([20]byte{249})
	ReSimulationCommitAddr = common.Address([20]byte{250})
)

var WriteCapablePrecompiledSSCContracts = map[common.Address]WriteCapablePrecompiledSSCContract{
	SimulationCommitAddr:   &simulationCommit{},
	ReSimulationCommitAddr: &reSimulationCommit{},
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
	// TODO implement me
	panic("implement me")
}

func (s *simulationCommit) RunWriteCapable(vm *SSCVM, contract *Contract, input []byte) ([]byte, error) {
	vm.SSCService.VerifySimulation(input)
	return nil, nil
}

type reSimulationCommit struct {
}

func (s *reSimulationCommit) RequiredGas(vm *SSCVM, contract *Contract, input []byte) (uint64, error) {
	// TODO implement me
	panic("implement me")
}

func (s *reSimulationCommit) RunWriteCapable(vm *SSCVM, contract *Contract, input []byte) ([]byte, error) {
	vm.SSCService.VerifyReSimulation(input)
	return nil, nil
}
