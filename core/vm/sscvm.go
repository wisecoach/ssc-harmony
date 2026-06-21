package vm

import (
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/harmony-one/harmony/core/state"
	"github.com/harmony-one/harmony/internal/params"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/pkg/errors"
)

type ExecutionType int
type TransferType int

const (
	SimulationCall ExecutionType = iota
	SimulationReCall
	ExecutionVerify
	LockExecution
	Precompiled
)

func (e ExecutionType) String() string {
	switch e {
	case SimulationCall:
		return "SimulationCall"
	case SimulationReCall:
		return "SimulationReCall"
	case ExecutionVerify:
		return "ExecutionVerify"
	case LockExecution:
		return "LockExecution"
	case Precompiled:
		return "Precompiled"
	default:
		return "UnknownExecutionType"
	}
}

const (
	Internal TransferType = iota
	CrossShard
)

func (t TransferType) String() string {
	switch t {
	case Internal:
		return "Internal"
	case CrossShard:
		return "CrossShard"
	default:
		return "UnknownTransferType"
	}
}

var (
	ErrUnsupportedExecutionTYpe = errors.New("unsupported execution type")
	ErrFromSameShard            = errors.New("cross-call from same shard is not allowed")
	ErrNotTargetShard           = errors.New("not target shard for cross-call")
)

type SSCVM struct {
	// Context provides auxiliary blockchain related information
	Context Context
	// DB gives access to the underlying state
	StateDB *state.DB
	// Depth is the current call Stack
	depth int
	// SSC Service used to implement the Cross-Shard Transaction
	SSCService api.Service

	// ExecutionType of SSCVM, used to determine the instruction set of the VM
	ExecutionType ExecutionType

	// chainConfig contains information about the current chain
	chainConfig *params.ChainConfig
	// chain rules contains the chain rules for the current epoch
	chainRules params.Rules
	// virtual machine configuration options used to initialise the
	// evm.
	vmConfig Config
	// global (to this context) ethereum virtual machine
	// used throughout the execution of the tx.
	interpreters []Interpreter
	interpreter  Interpreter
	// abort is used to abort the EVM calling operations
	// NOTE: must be set atomically
	abort int32
	// callGasTemp holds the gas available for the current call. This is needed because the
	// available gas is calculated in gasCall* according to the 63/64 rule and later
	// applied in opCall*.
	callGasTemp uint64

	// forceVMErr records the lock conflict error in ForceSimulation mode.
	// Set by opSload/opSstore when a lock conflict is encountered but execution continues.
	// Read by TransitionDb to propagate the conflict to the caller.
	forceVMErr error
}

func NewSSCVM(ctx Context, statedb *state.DB, chainConfig *params.ChainConfig, vmConfig Config, sscService api.Service, executionType ExecutionType) *SSCVM {
	vm := &SSCVM{
		Context:       ctx,
		StateDB:       statedb,
		vmConfig:      vmConfig,
		chainConfig:   chainConfig,
		chainRules:    chainConfig.Rules(ctx.EpochNumber),
		interpreters:  make([]Interpreter, 0, 1),
		SSCService:    sscService,
		ExecutionType: executionType,
	}

	// vmConfig.EVMInterpreter will be used by EVM-C, it won't be checked here
	// as we always want to have the built-in EVM as the failover option.
	vm.interpreters = append(vm.interpreters, NewSSCVMInterpreter(vm, vmConfig))
	vm.interpreter = vm.interpreters[0]

	return vm
}

// Cancel cancels any running EVM operation. This may be called concurrently and
// it's safe to be called multiple times.
func (vm *SSCVM) Cancel() {
	atomic.StoreInt32(&vm.abort, 1)
}

// Cancelled returns true if Cancel has been called
func (vm *SSCVM) Cancelled() bool {
	return atomic.LoadInt32(&vm.abort) == 1
}

// Interpreter returns the current interpreter
func (vm *SSCVM) Interpreter() Interpreter {
	return vm.interpreter
}

// Call executes the contract associated with the addr with the given input as
// parameters. It also handles any necessary value transfer required and takes
// the necessary steps to create accounts and reverses the state in case of an
// execution error or failed value transfer.
func (vm *SSCVM) Call(caller ContractRef, addr common.Address, input []byte, gas uint64, value *big.Int) (ret []byte, leftOverGas uint64, err error) {
	startTime := time.Now()
	if vm.vmConfig.NoRecursion && vm.depth > 0 {
		return nil, gas, nil
	}

	// Fail if we're trying to execute above the call depth limit
	if vm.depth > int(params.CallCreateDepth) {
		return nil, gas, ErrDepth
	}

	if IsWriteCapablePrecompiledSSCContract(addr) {
		defer utils.SSCLogger().Info().
			Str("ExecutionType", vm.ExecutionType.String()).
			Str("txHash", vm.Context.TxHash.Hex()).
			Str("callIndex", vm.Context.CrossCallIndex.ToString()).
			Dur("cost", time.Since(startTime)).
			Msgf("callSSCPrecompiledContract: caller=%s, addr=%s",
				caller.Address().Hex(), addr.Hex())
		return vm.callSSCPrecompiledContract(caller, addr, input, gas, value)
	}

	// if cross-call
	targetShardId := vm.SSCService.GetShardID(addr)
	isCrossCall := targetShardId != vm.Context.ShardID

	utils.SSCLogger().Debug().
		Str("ExecutionType", vm.ExecutionType.String()).
		Str("txHash", vm.Context.TxHash.Hex()).
		Str("callIndex", vm.Context.CrossCallIndex.ToString()).
		Msgf("Call: caller=%s, addr=%s, targetShardId=%d, isCrossCall=%t",
			caller.Address().Hex(), addr.Hex(), targetShardId, isCrossCall)

	if isCrossCall {
		return vm.CrossCall(targetShardId, caller.Address(), addr, input, gas, value)
	}

	// Fail if we're trying to transfer more than the available balance
	if !vm.CanTransfer(caller.Address(), value, Internal) {
		return nil, gas, ErrInsufficientBalance
	}

	var (
		to       = AccountRef(addr)
		snapshot = vm.StateDB.Snapshot()
	)
	if !vm.StateDB.Exist(addr) {
		precompiles := PrecompiledContractsHomestead
		var writeCapablePrecompiles map[common.Address]WriteCapablePrecompiledSSCContract
		if vm.ChainConfig().IsS3(vm.Context.EpochNumber) {
			precompiles = PrecompiledContractsByzantium
		}
		if vm.chainRules.IsIstanbul {
			precompiles = PrecompiledContractsIstanbul
		}
		if vm.chainRules.IsVRF {
			precompiles = PrecompiledContractsVRF
		}
		if vm.chainRules.IsSHA3 {
			precompiles = PrecompiledContractsSHA3FIPS
		}
		if vm.chainRules.IsStakingPrecompile {
			precompiles = PrecompiledContractsStaking
		}
		if vm.chainRules.IsCrossShardXferPrecompile {
			writeCapablePrecompiles = WriteCapablePrecompiledSSCContracts
		}
		if (len(writeCapablePrecompiles) == 0 || writeCapablePrecompiles[addr] == nil) && precompiles[addr] == nil && vm.ChainConfig().IsS3(vm.Context.EpochNumber) && value.Sign() == 0 {
			utils.SSCLogger().Error().Str("txHash", vm.Context.TxHash.Hex()).
				Str("callIndex", vm.Context.CrossCallIndex.ToString()).
				Msgf("Call: caller=%s, addr=%s, targetShardId=%d, isCrossCall=%t",
					caller.Address().Hex(), addr.Hex(), targetShardId, isCrossCall)
			return nil, gas, nil
		}
		vm.StateDB.CreateAccount(addr)
	}
	vm.Transfer(caller.Address(), to.Address(), value, Internal)

	codeHash := vm.StateDB.GetCodeHash(addr)
	code := vm.StateDB.GetCode(addr)
	// If address is a validator address, then it's not a smart contract address
	// we don't use its code and codeHash fields
	if vm.Context.IsValidator(vm.StateDB, addr) {
		codeHash = emptyCodeHash
		code = nil
	}
	// Initialise a new contract and set the code that is to be used by the EVM.
	// The contract is a scoped environment for this execution context only.
	contract := NewContract(vm.Context.TxHash, caller, to, value, gas)
	contract.SetCallCode(&addr, codeHash, code)

	ret, err = vm.run(contract, input, false)

	// When an error was returned by the EVM or when setting the creation code
	// above we revert to the snapshot and consume any gas remaining. Additionally
	// when we're in homestead this also counts for code storage gas errors.
	if err != nil {
		vm.StateDB.RevertToSnapshot(snapshot)
		if err != ErrExecutionReverted {
			contract.UseGas(contract.Gas)
		}
	}
	return ret, contract.Gas, err
}

func (vm *SSCVM) CrossCall(targetShardId uint32, callerAddr common.Address, addr common.Address, input []byte, gas uint64, value *big.Int) (ret []byte, leftOverGas uint64, err error) {
	// get result from simulation if ExecutionType is ExecutionVerify or LockExecution
	if vm.ExecutionType == ExecutionVerify || vm.ExecutionType == LockExecution {
		ret, leftOverGas, err = vm.SSCService.GetResult(vm.Context.TxHash)
		return
	}

	// Get shard epochs snapshot from the service
	req := &api.CXTCallRequest{
		TargetShardId:  targetShardId,
		TxHash:         vm.Context.TxHash,
		Caller:         callerAddr,
		Addr:           addr,
		Input:          input,
		Gas:            gas,
		GasPrice:       vm.Context.GasPrice,
		Value:          value,
		BaseSSCMessage: api.BaseSSCMessage{},
	}
	result := vm.SSCService.CallCXTContract(req)
	if len(result.Err) > 0 {
		err = errors.New(result.Err)
	}
	return result.Result, result.LeftOverGas, err
}

func (vm *SSCVM) callSSCPrecompiledContract(caller ContractRef, addr common.Address, input []byte, gas uint64, value *big.Int) (ret []byte, leftOverGas uint64, err error) {
	to := AccountRef(addr)
	contract := NewContract(vm.Context.TxHash, caller, to, value, gas)
	contract.SetCallCode(&addr, emptyCodeHash, nil)
	snapshot := vm.StateDB.Snapshot()

	vm.Transfer(caller.Address(), to.Address(), value, Internal)
	ret, err = vm.run(contract, input, false)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Str("txHash", vm.Context.TxHash.Hex()).Msgf("callSSCPrecompiledContract: caller=%s, addr=%s", caller.Address().Hex(), addr.Hex())
		vm.StateDB.RevertToSnapshot(snapshot)
		if err != ErrExecutionReverted {
			contract.UseGas(contract.Gas)
		}
	}
	return ret, contract.Gas, err
}

func (vm *SSCVM) CallFromOtherShard(fromShard uint32, caller ContractRef, addr common.Address, input []byte, gas uint64, value *big.Int) (ret []byte, leftOverGas uint64, err error) {
	if vm.vmConfig.NoRecursion && vm.depth > 0 {
		utils.SSCLogger().Error().Str("txHash", vm.Context.TxHash.Hex()).Msgf("callFromOtherShard: caller=%s, addr=%s", caller.Address().Hex(), addr.Hex())
		return nil, gas, nil
	}

	// Fail if we're trying to execute above the call depth limit
	if vm.depth > int(params.CallCreateDepth) {
		return nil, gas, ErrDepth
	}

	if vm.Context.ShardID == fromShard {
		return nil, 0, ErrFromSameShard
	}

	targetShardId := vm.SSCService.GetShardID(addr)
	sscContract := IsWriteCapablePrecompiledSSCContract(addr)
	if targetShardId != vm.Context.ShardID && !sscContract {
		utils.SSCLogger().Error().Err(ErrNotTargetShard).Str("txHash", vm.Context.TxHash.Hex()).
			Str("callIndex", vm.Context.CrossCallIndex.ToString()).
			Msgf("not the target shard for cross-call, targetShardId=%d, currentShardId=%d, addr=%s, shardNum=%d", targetShardId, vm.Context.ShardID, addr.Hex(), vm.SSCService.ShardNum())
		return nil, gas, ErrNotTargetShard
	}

	if !vm.Context.CanTransfer(vm.StateDB, caller.Address(), value) {
		return nil, gas, ErrInsufficientBalance
	}

	var (
		to       = AccountRef(addr)
		snapshot = vm.StateDB.Snapshot()
	)
	if !vm.StateDB.Exist(addr) {
		precompiles := PrecompiledContractsHomestead
		var writeCapablePrecompiles map[common.Address]WriteCapablePrecompiledSSCContract
		if vm.ChainConfig().IsS3(vm.Context.EpochNumber) {
			precompiles = PrecompiledContractsByzantium
		}
		if vm.chainRules.IsIstanbul {
			precompiles = PrecompiledContractsIstanbul
		}
		if vm.chainRules.IsVRF {
			precompiles = PrecompiledContractsVRF
		}
		if vm.chainRules.IsSHA3 {
			precompiles = PrecompiledContractsSHA3FIPS
		}
		if vm.chainRules.IsStakingPrecompile {
			precompiles = PrecompiledContractsStaking
		}
		if vm.chainRules.IsCrossShardXferPrecompile {
			writeCapablePrecompiles = WriteCapablePrecompiledSSCContracts
		}
		if (len(writeCapablePrecompiles) == 0 || writeCapablePrecompiles[addr] == nil) && precompiles[addr] == nil && vm.ChainConfig().IsS3(vm.Context.EpochNumber) && value.Sign() == 0 {
			utils.SSCLogger().Error().Str("txHash", vm.Context.TxHash.Hex()).Msgf("create a new account without any value, caller=%s, addr=%s", caller.Address().Hex(), addr.Hex())
			return nil, gas, nil
		}
		vm.StateDB.CreateAccount(addr)
	}
	vm.Transfer(caller.Address(), to.Address(), value, CrossShard)

	codeHash := vm.StateDB.GetCodeHash(addr)
	code := vm.StateDB.GetCode(addr)
	// If address is a validator address, then it's not a smart contract address
	// we don't use its code and codeHash fields
	if vm.Context.IsValidator(vm.StateDB, addr) {
		codeHash = emptyCodeHash
		code = nil
	}
	// Initialise a new contract and set the code that is to be used by the EVM.
	// The contract is a scoped environment for this execution context only.
	contract := NewContract(vm.Context.TxHash, caller, to, value, gas)
	contract.SetCallCode(&addr, codeHash, code)

	utils.SSCLogger().Debug().Str("txHash", vm.Context.TxHash.Hex()).Str("callIndex", vm.Context.CrossCallIndex.ToString()).
		Str("fromShard", strconv.Itoa(int(fromShard))).Str("toShard", strconv.Itoa(int(targetShardId))).
		Str("from", caller.Address().Hex()).Str("to", addr.Hex()).
		Uint64("gas", gas).Str("value", value.String()).
		Msgf("callFromOtherShard, code_size: %d", len(code))

	ret, err = vm.run(contract, input, false)

	// When an error was returned by the EVM or when setting the creation code
	// above we revert to the snapshot and consume any gas remaining. Additionally
	// when we're in homestead this also counts for code storage gas errors.
	if err != nil {
		vm.StateDB.RevertToSnapshot(snapshot)
		if err != ErrExecutionReverted {
			contract.UseGas(contract.Gas)
		}
	}
	return ret, contract.Gas, err
}

// CallCode executes the contract associated with the addr with the given input
// as parameters. It also handles any necessary value transfer required and takes
// the necessary steps to create accounts and reverses the state in case of an
// execution error or failed value transfer.
//
// CallCode differs from Call in the sense that it executes the given address'
// code with the caller as context.
func (vm *SSCVM) CallCode(caller ContractRef, addr common.Address, input []byte, gas uint64, value *big.Int) (ret []byte, leftOverGas uint64, err error) {
	if vm.vmConfig.NoRecursion && vm.depth > 0 {
		return nil, gas, nil
	}

	// Fail if we're trying to execute above the call depth limit
	if vm.depth > int(params.CallCreateDepth) {
		return nil, gas, ErrDepth
	}

	if IsWriteCapablePrecompiledSSCContract(addr) {
		utils.SSCLogger().Debug().
			Str("ExecutionType", vm.ExecutionType.String()).
			Str("txHash", vm.Context.TxHash.Hex()).
			Str("callIndex", vm.Context.CrossCallIndex.ToString()).
			Msgf("callSSCPrecompiledContract: caller=%s, addr=%s",
				caller.Address().Hex(), addr.Hex())
		return vm.callSSCPrecompiledContract(caller, addr, input, gas, value)
	}

	// if cross-call
	targetShardId := vm.SSCService.GetShardID(addr)
	isCrossCall := targetShardId != vm.Context.ShardID
	if isCrossCall {
		return vm.CrossCall(targetShardId, caller.Address(), addr, input, gas, value)
	}

	// Fail if we're trying to transfer more than the available balance
	if !vm.Context.CanTransfer(vm.StateDB, caller.Address(), value) {
		return nil, gas, ErrInsufficientBalance
	}

	var (
		snapshot = vm.StateDB.Snapshot()
		to       = AccountRef(caller.Address())
	)
	// initialise a new contract and set the code that is to be used by the
	// vm. The contract is a scoped environment for this execution context only.
	contract := NewContract(vm.Context.TxHash, caller, to, value, gas)
	contract.SetCallCode(&addr, vm.StateDB.GetCodeHash(addr), vm.StateDB.GetCode(addr))

	ret, err = vm.run(contract, input, false)
	if err != nil {
		vm.StateDB.RevertToSnapshot(snapshot)
		if err != ErrExecutionReverted {
			contract.UseGas(contract.Gas)
		}
	}
	return ret, contract.Gas, err
}

// DelegateCall executes the contract associated with the addr with the given input
// as parameters. It reverses the state in case of an execution error.
//
// DelegateCall differs from CallCode in the sense that it executes the given address'
// code with the caller as context and the caller is set to the caller of the caller.
func (vm *SSCVM) DelegateCall(caller ContractRef, addr common.Address, input []byte, gas uint64) (ret []byte, leftOverGas uint64, err error) {
	if vm.vmConfig.NoRecursion && vm.depth > 0 {
		return nil, gas, nil
	}
	// Fail if we're trying to execute above the call depth limit
	if vm.depth > int(params.CallCreateDepth) {
		return nil, gas, ErrDepth
	}

	if IsWriteCapablePrecompiledSSCContract(addr) {
		utils.SSCLogger().Info().
			Str("ExecutionType", vm.ExecutionType.String()).
			Str("txHash", vm.Context.TxHash.Hex()).
			Str("callIndex", vm.Context.CrossCallIndex.ToString()).
			Msgf("callSSCPrecompiledContract: caller=%s, addr=%s",
				caller.Address().Hex(), addr.Hex())
		return vm.callSSCPrecompiledContract(caller, addr, input, gas, big.NewInt(0))
	}

	// if cross-call
	targetShardId := vm.SSCService.GetShardID(addr)
	isCrossCall := targetShardId != vm.Context.ShardID
	if isCrossCall {
		return vm.CrossCall(targetShardId, caller.Address(), addr, input, gas, big.NewInt(0))
	}

	var (
		snapshot = vm.StateDB.Snapshot()
		to       = AccountRef(caller.Address())
	)

	// Initialise a new contract and make initialise the delegate values
	contract := NewContract(vm.Context.TxHash, caller, to, nil, gas).AsDelegate()
	contract.SetCallCode(&addr, vm.StateDB.GetCodeHash(addr), vm.StateDB.GetCode(addr))

	ret, err = vm.run(contract, input, false)
	if err != nil {
		vm.StateDB.RevertToSnapshot(snapshot)
		if err != ErrExecutionReverted {
			contract.UseGas(contract.Gas)
		}
	}
	return ret, contract.Gas, err
}

func (vm *SSCVM) StaticCall(caller ContractRef, addr common.Address, input []byte, gas uint64) (ret []byte, leftOverGas uint64, err error) {
	if vm.vmConfig.NoRecursion && vm.depth > 0 {
		return nil, gas, nil
	}
	// Fail if we're trying to execute above the call depth limit
	if vm.depth > int(params.CallCreateDepth) {
		return nil, gas, ErrDepth
	}

	if IsWriteCapablePrecompiledSSCContract(addr) {
		utils.SSCLogger().Info().
			Str("ExecutionType", vm.ExecutionType.String()).
			Str("txHash", vm.Context.TxHash.Hex()).
			Str("callIndex", vm.Context.CrossCallIndex.ToString()).
			Msgf("callSSCPrecompiledContract: caller=%s, addr=%s",
				caller.Address().Hex(), addr.Hex())
		return vm.callSSCPrecompiledContract(caller, addr, input, gas, big.NewInt(0))
	}

	// if cross-call
	targetShardId := vm.SSCService.GetShardID(addr)
	isCrossCall := targetShardId != vm.Context.ShardID
	if isCrossCall {
		return vm.CrossCall(targetShardId, caller.Address(), addr, input, gas, big.NewInt(0))
	}

	var (
		to       = AccountRef(addr)
		snapshot = vm.StateDB.Snapshot()
	)
	// Initialise a new contract and set the code that is to be used by the
	// EVM. The contract is a scoped environment for this execution context
	// only.
	contract := NewContract(vm.Context.TxHash, caller, to, new(big.Int), gas)
	contract.SetCallCode(&addr, vm.StateDB.GetCodeHash(addr), vm.StateDB.GetCode(addr))

	// We do an AddBalance of zero here, just in order to trigger a touch.
	// This doesn't matter on Mainnet, where all empties are gone at the time of Byzantium,
	// but is the correct thing to do and matters on other networks, in tests, and potential
	// future scenarios
	vm.StateDB.AddBalance(addr, bigZero)

	// When an error was returned by the EVM or when setting the creation code
	// above we revert to the snapshot and consume any gas remaining. Additionally
	// when we're in Homestead this also counts for code storage gas errors.
	ret, err = vm.run(contract, input, true)
	if err != nil {
		vm.StateDB.RevertToSnapshot(snapshot)
		if err != ErrExecutionReverted {
			contract.UseGas(contract.Gas)
		}
	}
	return ret, contract.Gas, err
}

func (vm *SSCVM) run(contract *Contract, input []byte, readOnly bool) ([]byte, error) {
	startTime := time.Now()
	if contract.CodeAddr != nil {
		precompiles := PrecompiledContractsHomestead
		// assign empty write capable precompiles till they are available in the fork
		if vm.ChainConfig().IsS3(vm.Context.EpochNumber) {
			precompiles = PrecompiledContractsByzantium
		}
		if vm.chainRules.IsIstanbul {
			precompiles = PrecompiledContractsIstanbul
		}
		if vm.chainRules.IsVRF {
			precompiles = PrecompiledContractsVRF
		}
		if vm.chainRules.IsSHA3 {
			precompiles = PrecompiledContractsSHA3FIPS
		}
		if vm.chainRules.IsStakingPrecompile {
			precompiles = PrecompiledContractsStaking
		}
		if p := precompiles[*contract.CodeAddr]; p != nil {
			if _, ok := p.(*vrf); ok {
				if vm.chainRules.IsPrevVRF {
					requestedBlockNum := big.NewInt(0).SetBytes(input)
					minBlockNum := big.NewInt(0).Sub(vm.Context.BlockNumber, common.Big257)

					if requestedBlockNum.Cmp(vm.Context.BlockNumber) == 0 {
						input = vm.Context.VRF.Bytes()
					} else if requestedBlockNum.Cmp(minBlockNum) > 0 && requestedBlockNum.Cmp(vm.Context.BlockNumber) < 0 {
						// requested block number is in range
						input = vm.Context.GetVRF(requestedBlockNum.Uint64()).Bytes()
					} else {
						// else default to the current block's VRF
						input = vm.Context.VRF.Bytes()
					}
				} else {
					// Override the input with vrf data of the requested block so it can be returned to the contract program.
					input = vm.Context.VRF.Bytes()
				}
			} else if _, ok := p.(*epoch); ok {
				input = vm.Context.EpochNumber.Bytes()
			}
			return RunPrecompiledContract(p, input, contract)
		}
		writeCapablePrecompiles := WriteCapablePrecompiledSSCContracts
		// it's used to cross-call for harmony, we don't need to RunWriteCapablePrecompiledContract
		if len(writeCapablePrecompiles) > 0 {
			if p := writeCapablePrecompiles[*contract.CodeAddr]; p != nil {
				defer utils.SSCLogger().Info().Str("txHash", vm.Context.TxHash.Hex()).
					Str("executionType", vm.ExecutionType.String()).
					Dur("duration", time.Since(startTime)).
					Msgf("RunWriteCapablePrecompiledContract: %s, contractType: %s", contract.CodeAddr.Hex(), reflect.TypeOf(p).Elem().Name())
				if readOnly {
					return nil, errWriteProtection
				}
				gas, err := p.RequiredGas(vm, contract, input)
				if err != nil {
					return nil, err
				}
				if !contract.UseGas(gas) {
					return nil, ErrOutOfGas
				}
				return p.RunWriteCapable(vm, contract, input)
			}
		}
	}
	for _, interpreter := range vm.interpreters {
		if interpreter.CanRun(contract.Code) {
			if vm.interpreter != interpreter {
				// Ensure that the interpreter pointer is set back
				// to its current value upon return.
				defer func(i Interpreter) {
					vm.interpreter = i
				}(vm.interpreter)
				vm.interpreter = interpreter
			}

			if vm.ChainConfig().IsDataCopyFixEpoch(vm.Context.EpochNumber) {
				contract.WithDataCopyFix = true
			}
			return interpreter.Run(contract, input, readOnly)
		}
	}
	return nil, ErrNoCompatibleInterpreter
}

// ChainConfig returns the environment's chain configuration
func (vm *SSCVM) ChainConfig() *params.ChainConfig { return vm.chainConfig }

// create creates a new contract using code as deployment code.
func (vm *SSCVM) create(caller ContractRef, codeAndHash *codeAndHash, gas uint64, value *big.Int, address common.Address) ([]byte, common.Address, uint64, error) {
	// Depth check execution. Fail if we're trying to execute above the
	// limit.
	if vm.depth > int(params.CallCreateDepth) {
		return nil, common.Address{}, gas, ErrDepth
	}
	if !vm.CanTransfer(caller.Address(), value, Internal) {
		return nil, common.Address{}, gas, ErrInsufficientBalance
	}
	nonce := vm.StateDB.GetNonce(caller.Address())
	vm.StateDB.SetNonce(caller.Address(), nonce+1)

	// Ensure there's no existing contract already at the designated address
	contractHash := vm.StateDB.GetCodeHash(address)
	if vm.StateDB.GetNonce(address) != 0 || (contractHash != (common.Hash{}) && contractHash != emptyCodeHash) {
		return nil, common.Address{}, 0, ErrContractAddressCollision
	}
	// Create a new account on the state
	snapshot := vm.StateDB.Snapshot()
	vm.StateDB.CreateAccount(address)
	if vm.ChainConfig().IsEIP155(vm.Context.EpochNumber) {
		vm.StateDB.SetNonce(address, 1)
	}
	vm.Transfer(caller.Address(), address, value, Internal)

	// initialise a new contract and set the code that is to be used by the
	// vm. The contract is a scoped environment for this execution context
	// only.
	contract := NewContract(vm.Context.TxHash, caller, AccountRef(address), value, gas)
	contract.SetCodeOptionalHash(&address, codeAndHash)

	if vm.vmConfig.NoRecursion && vm.depth > 0 {
		return nil, address, gas, nil
	}

	ret, err := vm.run(contract, nil, false)

	// check whether the max code size has been exceeded
	maxCodeSizeExceeded := vm.ChainConfig().IsEIP155(vm.Context.EpochNumber) && len(ret) > params.MaxCodeSize
	// if the contract creation ran successfully and no errors were returned
	// calculate the gas required to store the code. If the code could not
	// be stored due to not enough gas set an error and let it be handled
	// by the error checking condition below.
	if err == nil && !maxCodeSizeExceeded {
		createDataGas := uint64(len(ret)) * params.CreateDataGas
		if contract.UseGas(createDataGas) {
			vm.StateDB.SetCode(address, ret, false)
		} else {
			err = ErrCodeStoreOutOfGas
		}
	}

	// When an error was returned by the vm or when setting the creation code
	// above we revert to the snapshot and consume any gas remaining. Additionally
	// when we're in homestead this also counts for code storage gas errors.
	if maxCodeSizeExceeded || (err != nil && (vm.ChainConfig().IsS3(vm.Context.EpochNumber) || err != ErrCodeStoreOutOfGas)) {
		vm.StateDB.RevertToSnapshot(snapshot)
		if err != ErrExecutionReverted {
			contract.UseGas(contract.Gas)
		}
	}
	// Assign err if contract code size exceeds the max while the err is still empty.
	if maxCodeSizeExceeded && err == nil {
		err = errMaxCodeSizeExceeded
	}
	return ret, address, contract.Gas, err

}

// Create creates a new contract using code as deployment code.
func (vm *SSCVM) Create(caller ContractRef, code []byte, gas uint64, value *big.Int) (ret []byte, contractAddr common.Address, leftOverGas uint64, err error) {
	contractAddr = crypto.CreateAddress(caller.Address(), vm.StateDB.GetNonce(caller.Address()))
	return vm.create(caller, &codeAndHash{code: code}, gas, value, contractAddr)
}

// Create2 creates a new contract using code as deployment code.
//
// The different between Create2 with Create is Create2 uses sha3(0xff ++ msg.sender ++ salt ++ sha3(init_code))[12:]
// instead of the usual sender-and-nonce-hash as the address where the contract is initialized at.
func (vm *SSCVM) Create2(caller ContractRef, code []byte, gas uint64, endowment *big.Int, salt *big.Int) (ret []byte, contractAddr common.Address, leftOverGas uint64, err error) {
	codeAndHash := &codeAndHash{code: code}
	contractAddr = crypto.CreateAddress2(caller.Address(), common.BigToHash(salt), codeAndHash.Hash().Bytes())
	return vm.create(caller, codeAndHash, gas, endowment, contractAddr)
}

func (vm *SSCVM) Transfer(from common.Address, to common.Address, amount *big.Int, transferType TransferType) {
	if amount == nil || amount.Sign() == 0 {
		return
	}
	switch vm.ExecutionType {
	case SimulationCall:
		vm.transfer_Call(from, to, amount, transferType)
	case SimulationReCall:
		vm.transfer_Recall(from, to, amount, transferType)
	case LockExecution:
		vm.transfer_RV(from, to, amount, transferType)
	case ExecutionVerify:
		vm.transfer_EV(from, to, amount, transferType)
	}
}

func (vm *SSCVM) transfer_Call(from common.Address, to common.Address, amount *big.Int, transferType TransferType) {
	txHash := vm.Context.TxHash
	if transferType == Internal {
		vm.SSCService.SubBalance(vm.StateDB, txHash, from, amount)
	}
	vm.SSCService.AddBalance(vm.StateDB, txHash, to, amount)
}

func (vm *SSCVM) transfer_Recall(from common.Address, to common.Address, amount *big.Int, transferType TransferType) {

}

func (vm *SSCVM) transfer_RV(from common.Address, to common.Address, amount *big.Int, transferType TransferType) {

}

func (vm *SSCVM) transfer_EV(from common.Address, to common.Address, amount *big.Int, transferType TransferType) {
	txHash := vm.Context.TxHash
	if transferType == Internal {
		vm.SSCService.SubSimuBalance(txHash, from, amount)
	}
	vm.SSCService.AddSimuBalance(txHash, to, amount)
}

func (vm *SSCVM) CanTransfer(from common.Address, amount *big.Int, transferType TransferType) bool {
	switch vm.ExecutionType {
	case SimulationCall:
		return vm.canTransfer_Call(from, amount, transferType)
	case SimulationReCall:
		return vm.canTransfer_Recall(from, amount, transferType)
	case LockExecution:
		return vm.canTransfer_RV(from, amount, transferType)
	case ExecutionVerify:
		return vm.canTransfer_EV(from, amount, transferType)
	}
	return false
}

func (vm *SSCVM) canTransfer_Call(from common.Address, amount *big.Int, transferType TransferType) bool {
	return true
}

func (vm *SSCVM) canTransfer_Recall(from common.Address, amount *big.Int, transferType TransferType) bool {
	return true
}

func (vm *SSCVM) canTransfer_RV(from common.Address, amount *big.Int, transferType TransferType) bool {
	return true
}

func (vm *SSCVM) canTransfer_EV(from common.Address, amount *big.Int, transferType TransferType) bool {
	return true
}

func (vm *SSCVM) SetForceVMErr(err error) {
	vm.forceVMErr = err
}

func (vm *SSCVM) GetForceVMErr() error {
	return vm.forceVMErr
}

func IsLockConflictErr(err string) bool {
	return strings.Contains(err, api.ErrLockConflict_OnChain.Error()) ||
		strings.Contains(err, api.ErrLockConflict_OffChain.Error())
}
