package vm

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/internal/utils"
)

var (
	SimulationCommitAddr    = common.Address([20]byte{255, 0})
	CxtCommitOrRollbackAddr = common.Address([20]byte{255, 1})
	EmptyAddr               = common.Address([20]byte{255, 2})
	NewEpochAddr            = common.Address([20]byte{255, 3})
	SLOpinionAddr           = common.Address([20]byte{255, 4})
)

// WriteCapablePrecompiledSSCContracts for every ssc precompiled contract, we need to register it here
var WriteCapablePrecompiledSSCContracts = map[common.Address]WriteCapablePrecompiledSSCContract{
	SimulationCommitAddr:    &simulationCommit{},
	CxtCommitOrRollbackAddr: &cxtCommitOrRollback{},
	EmptyAddr:               &empty{},
	NewEpochAddr:            &newEpoch{},
	SLOpinionAddr:           &slOpinion{},
}

// SSCAddrsApplyOnChain for simulate and verify cross shard tx, or other precompiled contracts no need to be simulate off-chain and verify on-chain
var SSCAddrsApplyOnChain = map[common.Address]interface{}{
	SimulationCommitAddr:    struct{}{},
	CxtCommitOrRollbackAddr: struct{}{},
	EmptyAddr:               struct{}{},
	SLOpinionAddr:           struct{}{},
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
	startTime := time.Now()
	defer utils.SSCLogger().Debug().
		Str("txHash", vm.Context.TxHash.Hex()).
		Dur("cost", time.Since(startTime)).
		Msgf("verify simulation")
	vm.SSCService.VerifySimulation(input, vm.StateDB, vm.Context.Header)
	return nil, nil
}

type cxtCommitOrRollback struct {
}

func (c *cxtCommitOrRollback) RequiredGas(vm *SSCVM, contract *Contract, input []byte) (uint64, error) {
	return 0, nil
}

func (c *cxtCommitOrRollback) RunWriteCapable(vm *SSCVM, contract *Contract, input []byte) ([]byte, error) {
	// unlock when it's executed on chain
	err := vm.SSCService.CommitOrRollbackWithProof(input, vm.StateDB, vm.Context.Header.Number().Uint64())
	if err != nil {
		return nil, err
	}
	return nil, nil
}

type empty struct {
}

func (e *empty) RequiredGas(vm *SSCVM, contract *Contract, input []byte) (uint64, error) {
	return 0, nil
}

func (e *empty) RunWriteCapable(vm *SSCVM, contract *Contract, input []byte) ([]byte, error) {
	return nil, nil
}

type newEpoch struct {
}

func (n *newEpoch) RequiredGas(vm *SSCVM, contract *Contract, input []byte) (uint64, error) {
	return 0, nil
}

func (n *newEpoch) RunWriteCapable(vm *SSCVM, contract *Contract, input []byte) ([]byte, error) {
	blockNum := vm.Context.BlockNumber.Uint64()
	err := vm.SSCService.NewEpoch(input, vm, vm.StateDB, blockNum)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

type slOpinion struct {
}

func (s *slOpinion) RequiredGas(vm *SSCVM, contract *Contract, input []byte) (uint64, error) {
	return 0, nil
}
func (s *slOpinion) RunWriteCapable(vm *SSCVM, contract *Contract, input []byte) ([]byte, error) {
	startTime := time.Now()
	defer utils.SSCLogger().Debug().
		Str("txHash", vm.Context.TxHash.Hex()).
		Dur("cost", time.Since(startTime)).
		Msgf("upload SLOpinion")
	err := vm.SSCService.UploadSLOpinion(input, vm.StateDB)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func IsSSCAddrApplyOnChain(addr common.Address) bool {
	_, ok := SSCAddrsApplyOnChain[addr]
	return ok
}

func IsWriteCapablePrecompiledSSCContract(addr common.Address) bool {
	_, ok := WriteCapablePrecompiledSSCContracts[addr]
	return ok
}
