package vm

import (
	"bytes"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/harmony-one/harmony/internal/params"
)

var (
	ExecutionVerifyInstructions = newExecutionVerifyInstructions()
)

func newExecutionVerifyInstructions() JumpTable {
	is := newBaseInstructions()
	is[CALLDATALOAD].execute = opCallDataLoad_SSC_EV
	is[SLOAD].execute = opSload_SSC_EV
	is[SSTORE].execute = opSstore_SSC_EV
	is[CALL].execute = opCall_SSC_EV
	is[CALLCODE].execute = opCallCode_SSC_EV
	is[DELEGATECALL].execute = opDelegateCall_SSC_EV
	is[STATICCALL].execute = opStaticCall_SSC_EV
	is[RETURN].execute = opReturn_SSC_EV
	return is
}

func opCallDataLoad_SSC_EV(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	stack.push(interpreter.intPool.get().SetBytes(getDataBig(contract.Input, stack.pop(), big32)))
	return nil, nil
}

func opSload_SSC_EV(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	loc := stack.peek()
	txHash := interpreter.vm.Context.TxHash
	val, _ := interpreter.vm.SSCService.GetSimuState(txHash, contract.Address(), common.BigToHash(loc))
	loc.SetBytes(val.Bytes())
	return nil, nil
}

func opSstore_SSC_EV(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	loc := common.BigToHash(stack.pop())
	val := stack.pop()
	txHash := interpreter.vm.Context.TxHash
	interpreter.vm.SSCService.SetSimuState(txHash, contract.Address(), loc, common.BigToHash(val))

	interpreter.intPool.put(val)
	return nil, nil
}

func opCall_SSC_EV(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	// Pop gas. The actual gas in interpreter.vm.callGasTemp.
	interpreter.intPool.put(stack.pop())
	gas := interpreter.vm.callGasTemp
	// Pop other call parameters.
	addr, value, inOffset, inSize, retOffset, retSize := stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop()
	toAddr := common.BigToAddress(addr)
	value = math.U256(value)
	// Get the arguments from the memory.
	args := memory.GetPtr(inOffset.Int64(), inSize.Int64())

	if value.Sign() != 0 {
		gas += params.CallStipend
	}

	var (
		ret       []byte
		returnGas uint64
		err       error
	)

	if len(args) > 8+16 && bytes.Compare(args[9:21], CTX_PREFIX) == 0 {
		ret, returnGas, err = interpreter.vm.SSCService.GetResult(interpreter.vm.Context.TxHash)
		if err != nil {
			return nil, err
		}
	} else {
		ret, returnGas, err = interpreter.vm.Call(contract, toAddr, args, gas, value)
	}

	if err != nil {
		stack.push(interpreter.intPool.getZero())
	} else {
		stack.push(interpreter.intPool.get().SetUint64(1))
	}
	if err == nil || err == ErrExecutionReverted {
		if contract.WithDataCopyFix {
			ret = common.CopyBytes(ret)
		}
		memory.Set(retOffset.Uint64(), retSize.Uint64(), ret)
	}
	contract.Gas += returnGas

	interpreter.intPool.put(addr, value, inOffset, inSize, retOffset, retSize)
	return ret, nil
}

func opCallCode_SSC_EV(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	// Pop gas. The actual gas is in interpreter.vm.callGasTemp.
	interpreter.intPool.put(stack.pop())
	gas := interpreter.vm.callGasTemp
	// Pop other call parameters.
	addr, value, inOffset, inSize, retOffset, retSize := stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop()
	toAddr := common.BigToAddress(addr)
	value = math.U256(value)
	// Get arguments from the memory.
	args := memory.GetPtr(inOffset.Int64(), inSize.Int64())

	if value.Sign() != 0 {
		gas += params.CallStipend
	}

	var (
		ret       []byte
		returnGas uint64
		err       error
	)

	if len(args) > 8+16 && bytes.Compare(args[9:21], CTX_PREFIX) == 0 {
		ret, returnGas, err = interpreter.vm.SSCService.GetResult(interpreter.vm.Context.TxHash)
		if err != nil {
			return nil, err
		}
	} else {
		ret, returnGas, err = interpreter.vm.CallCode(contract, toAddr, args, gas, value)
	}

	if err != nil {
		stack.push(interpreter.intPool.getZero())
	} else {
		stack.push(interpreter.intPool.get().SetUint64(1))
	}
	if err == nil || err == ErrExecutionReverted {
		if contract.WithDataCopyFix {
			ret = common.CopyBytes(ret)
		}
		memory.Set(retOffset.Uint64(), retSize.Uint64(), ret)
	}
	contract.Gas += returnGas

	interpreter.intPool.put(addr, value, inOffset, inSize, retOffset, retSize)
	return ret, nil
}

func opDelegateCall_SSC_EV(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	// Pop gas. The actual gas is in interpreter.vm.callGasTemp.
	interpreter.intPool.put(stack.pop())
	gas := interpreter.vm.callGasTemp
	// Pop other call parameters.
	addr, inOffset, inSize, retOffset, retSize := stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop()
	toAddr := common.BigToAddress(addr)
	// Get arguments from the memory.
	args := memory.GetPtr(inOffset.Int64(), inSize.Int64())
	var (
		ret       []byte
		returnGas uint64
		err       error
	)

	if len(args) > 8+16 && bytes.Compare(args[9:21], CTX_PREFIX) == 0 {
		ret, returnGas, err = interpreter.vm.SSCService.GetResult(interpreter.vm.Context.TxHash)
		if err != nil {
			return nil, err
		}
	} else {
		ret, returnGas, err = interpreter.vm.DelegateCall(contract, toAddr, args, gas)
	}
	if err != nil {
		stack.push(interpreter.intPool.getZero())
	} else {
		stack.push(interpreter.intPool.get().SetUint64(1))
	}
	if err == nil || err == ErrExecutionReverted {
		if contract.WithDataCopyFix {
			ret = common.CopyBytes(ret)
		}
		memory.Set(retOffset.Uint64(), retSize.Uint64(), ret)
	}
	contract.Gas += returnGas

	interpreter.intPool.put(addr, inOffset, inSize, retOffset, retSize)
	return ret, nil
}

func opStaticCall_SSC_EV(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	// Pop gas. The actual gas is in interpreter.evm.callGasTemp.
	interpreter.intPool.put(stack.pop())
	gas := interpreter.vm.callGasTemp
	// Pop other call parameters.
	addr, inOffset, inSize, retOffset, retSize := stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop()
	toAddr := common.BigToAddress(addr)
	// Get arguments from the memory.
	args := memory.GetPtr(inOffset.Int64(), inSize.Int64())

	var (
		ret       []byte
		returnGas uint64
		err       error
	)

	if len(args) > 8+16 && bytes.Compare(args[9:21], CTX_PREFIX) == 0 {
		ret, returnGas, err = interpreter.vm.SSCService.GetResult(interpreter.vm.Context.TxHash)
		if err != nil {
			return nil, err
		}
	} else {
		ret, returnGas, err = interpreter.vm.StaticCall(contract, toAddr, args, gas)
	}
	if err != nil {
		stack.push(interpreter.intPool.getZero())
	} else {
		stack.push(interpreter.intPool.get().SetUint64(1))
	}
	if err == nil || err == ErrExecutionReverted {
		if contract.WithDataCopyFix {
			ret = common.CopyBytes(ret)
		}
		memory.Set(retOffset.Uint64(), retSize.Uint64(), ret)
	}
	contract.Gas += returnGas

	interpreter.intPool.put(addr, inOffset, inSize, retOffset, retSize)
	return ret, nil
}

func opReturn_SSC_EV(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	offset, size := stack.pop(), stack.pop()
	ret := memory.GetPtr(offset.Int64(), size.Int64())

	interpreter.intPool.put(offset, size)
	return ret, nil
}
