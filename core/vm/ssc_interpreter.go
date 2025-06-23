package vm

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/common/math"
	"sync/atomic"
)

// SSCVMInterpreter represents an SSCVM interpreter
type SSCVMInterpreter struct {
	vm  *SSCVM
	cfg Config

	intPool *intPool

	hasher    keccakState // Keccak256 hasher instance shared across opcodes
	hasherBuf common.Hash // Keccak256 hasher result array shared aross opcodes

	readOnly   bool   // Whether to throw on stateful modifications
	returnData []byte // Last CALL's return data for subsequent reuse
}

// NewEVMInterpreter returns a new instance of the Interpreter.
func NewSSCVMInterpreter(vm *SSCVM, cfg Config) *SSCVMInterpreter {

	switch vm.executionType {
	case SimulationCall:
		cfg.JumpTable = SimulationCallInstructions
	case SimulationReCall:
		cfg.JumpTable = SimulationRecallInstructions
	case ExecutionVerify:
		cfg.JumpTable = ExecutionVerifyInstructions
	case LockExecution:
		cfg.JumpTable = LockExecutionInstructions
	}

	return &SSCVMInterpreter{
		vm:  vm,
		cfg: cfg,
	}
}

func (in *SSCVMInterpreter) Run(contract *Contract, input []byte, readOnly bool) (ret []byte, err error) {

	if in.intPool == nil {
		in.intPool = poolOfIntPools.get()
		defer func() {
			poolOfIntPools.put(in.intPool)
			in.intPool = nil
		}()
	}

	// hexCode := common.Bytes2Hex(contract.Code)
	// print("SSCVMInterpreter.Run: contract.Code: ", hexCode, "\n")

	// Increment the call depth which is restricted to 1024
	in.vm.depth++
	defer func() { in.vm.depth-- }()

	// Make sure the readOnly is only set if we aren't in readOnly yet.
	// This makes also sure that the readOnly flag isn't removed for child calls.
	if readOnly && !in.readOnly {
		in.readOnly = true
		defer func() { in.readOnly = false }()
	}

	// Reset the previous call's return data. It's unimportant to preserve the old buffer
	// as every returning call will return new data anyway.
	in.returnData = nil

	// Don't bother with the execution if there's no code.
	if len(contract.Code) == 0 {
		return nil, nil
	}

	var (
		op    OpCode        // current opcode
		mem   = NewMemory() // bound memory
		stack = newstack()  // local Stack
		// For optimisation reason we're using uint64 as the program counter.
		// It's theoretically possible to go above 2^64. The YP defines the PC
		// to be uint256. Practically much less so feasible.
		pc   = uint64(0) // program counter
		cost uint64
		// copies used by tracer
		res []byte // result of the opcode execution function
	)
	contract.Input = input

	// Reclaim the Stack as an int pool when the execution stops
	defer func() { in.intPool.put(stack.data...) }()

	// The Interpreter main run loop (contextual). This loop runs until either an
	// explicit STOP, RETURN or SELFDESTRUCT is executed, an error occurred during
	// the execution of one of the operations or until the done flag is set by the
	// parent context.
	for atomic.LoadInt32(&in.vm.abort) == 0 {

		// Get the operation from the jump table and validate the Stack to ensure there are
		// enough Stack items available to perform the operation.
		op = contract.GetOp(pc)
		operation := in.cfg.JumpTable[op]
		if !operation.valid {
			return nil, fmt.Errorf("invalid opcode 0x%x", int(op))
		}
		// Validate Stack
		if sLen := stack.len(); sLen < operation.minStack {
			return nil, fmt.Errorf("Stack underflow (%d <=> %d)", sLen, operation.minStack)
		} else if sLen > operation.maxStack {
			return nil, fmt.Errorf("Stack limit reached %d (%d)", sLen, operation.maxStack)
		}
		// If the operation is valid, enforce and write restrictions
		if in.readOnly && in.vm.chainRules.IsS3 {
			// If the interpreter is operating in readonly mode, make sure no
			// state-modifying operation is performed. The 3rd Stack item
			// for a call operation is the value. Transferring value from one
			// account to the others means the state is modified and should also
			// return with an error.
			if operation.writes || (op == CALL && stack.Back(2).Sign() != 0) {
				return nil, errWriteProtection
			}
		}
		// Static portion of gas
		// cost = operation.constantGas // For tracing
		if !contract.UseGas(operation.constantGas) {
			return nil, ErrOutOfGas
		}

		var memorySize uint64
		// calculate the new memory size and expand the memory to fit
		// the operation
		// Memory check needs to be done prior to evaluating the dynamic gas portion,
		// to detect calculation overflows
		if operation.memorySize != nil {
			memSize, overflow := operation.memorySize(stack)
			if overflow {
				return nil, errGasUintOverflow
			}
			// memory is expanded in words of 32 bytes. Gas
			// is also calculated in words.
			if memorySize, overflow = math.SafeMul(toWordSize(memSize), 32); overflow {
				return nil, errGasUintOverflow
			}
		}
		// Dynamic portion of gas
		// consume the gas and return an error if not enough gas is available.
		// cost is explicitly set so that the capture state defer method can get the proper cost
		if operation.dynamicGas != nil {
			var dynamicCost uint64
			dynamicCost, err = operation.dynamicGas(in.vm, contract, stack, mem, memorySize)
			cost += dynamicCost // total cost, for debug tracing
			if err != nil || !contract.UseGas(dynamicCost) {
				return nil, ErrOutOfGas
			}
		}
		if memorySize > 0 {
			mem.Resize(memorySize)
		}

		// execute the operation
		res, err = operation.execute(&pc, in, contract, mem, stack)

		// verifyPool is a build flag. Pool verification makes sure the integrity
		// of the integer pool by comparing values to a default value.
		if verifyPool {
			verifyIntegerPool(in.intPool)
		}
		// if the operation clears the return data (e.g. it has returning data)
		// set the last return to the result of the operation.
		if operation.returns {
			in.returnData = res
		}

		switch {
		case err != nil:
			return nil, err
		case operation.reverts:
			return res, ErrExecutionReverted
		case operation.halts:
			return res, nil
		case !operation.jumps:
			pc++
		}
	}
	return nil, nil
}

func (in *SSCVMInterpreter) CanRun(bytes []byte) bool {
	return true
}
