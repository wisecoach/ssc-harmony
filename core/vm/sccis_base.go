package vm

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/harmony-one/harmony/internal/params"
	"github.com/harmony-one/harmony/shard"
	"golang.org/x/crypto/sha3"
	"math/big"
)

var (
	BaseInstructions = newBaseInstructions()
)

func newBaseInstructions() JumpTable {
	return JumpTable{
		STOP: {
			execute:     opStop_SSC_Base,
			constantGas: 0,
			minStack:    minStack(0, 0),
			maxStack:    maxStack(0, 0),
			halts:       true,
			valid:       true,
		},
		ADD: {
			execute:     opAdd_SSC_Base,
			constantGas: GasFastestStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			valid:       true,
		},
		MUL: {
			execute:     opMul_SSC_Base,
			constantGas: GasFastStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			valid:       true,
		},
		SUB: {
			execute:     opSub_SSC_Base,
			constantGas: GasFastestStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			valid:       true,
		},
		DIV: {
			execute:     opDiv_SSC_Base,
			constantGas: GasFastStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			valid:       true,
		},
		SDIV: {
			execute:     opSdiv_SSC_Base,
			constantGas: GasFastStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			valid:       true,
		},
		MOD: {
			execute:     opMod_SSC_Base,
			constantGas: GasFastStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			valid:       true,
		},
		SMOD: {
			execute:     opSmod_SSC_Base,
			constantGas: GasFastStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			valid:       true,
		},
		ADDMOD: {
			execute:     opAddmod_SSC_Base,
			constantGas: GasMidStep,
			minStack:    minStack(3, 1),
			maxStack:    maxStack(3, 1),
			valid:       true,
		},
		MULMOD: {
			execute:     opMulmod_SSC_Base,
			constantGas: GasMidStep,
			minStack:    minStack(3, 1),
			maxStack:    maxStack(3, 1),
			valid:       true,
		},
		EXP: {
			execute:    opExp_SSC_Base,
			dynamicGas: gasExpFrontier,
			minStack:   minStack(2, 1),
			maxStack:   maxStack(2, 1),
			valid:      true,
		},
		SIGNEXTEND: {
			execute:     opSignExtend_SSC_Base,
			constantGas: GasFastStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			valid:       true,
		},
		LT: {
			execute:     opLt_SSC_Base,
			constantGas: GasFastestStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			valid:       true,
		},
		GT: {
			execute:     opGt_SSC_Base,
			constantGas: GasFastestStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			valid:       true,
		},
		SLT: {
			execute:     opSlt_SSC_Base,
			constantGas: GasFastestStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			valid:       true,
		},
		SGT: {
			execute:     opSgt_SSC_Base,
			constantGas: GasFastestStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			valid:       true,
		},
		EQ: {
			execute:     opEq_SSC_Base,
			constantGas: GasFastestStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			valid:       true,
		},
		ISZERO: {
			execute:     opIszero_SSC_Base,
			constantGas: GasFastestStep,
			minStack:    minStack(1, 1),
			maxStack:    maxStack(1, 1),
			valid:       true,
		},
		AND: {
			execute:     opAnd_SSC_Base,
			constantGas: GasFastestStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			valid:       true,
		},
		XOR: {
			execute:     opXor_SSC_Base,
			constantGas: GasFastestStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			valid:       true,
		},
		OR: {
			execute:     opOr_SSC_Base,
			constantGas: GasFastestStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			valid:       true,
		},
		NOT: {
			execute:     opNot_SSC_Base,
			constantGas: GasFastestStep,
			minStack:    minStack(1, 1),
			maxStack:    maxStack(1, 1),
			valid:       true,
		},
		BYTE: {
			execute:     opByte_SSC_Base,
			constantGas: GasFastestStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			valid:       true,
		},
		SHA3: {
			execute:     opSha3_SSC_Base,
			constantGas: params.Sha3Gas, // Once per SHA3 operation.
			dynamicGas:  gasSha3,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			memorySize:  memorySha3,
			valid:       true,
		},
		ADDRESS: {
			execute:     opAddress_SSC_Base,
			constantGas: GasQuickStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		BALANCE: {
			execute:     opBalance_SSC_Base,
			constantGas: params.BalanceGasFrontier,
			minStack:    minStack(1, 1),
			maxStack:    maxStack(1, 1),
			valid:       true,
		},
		ORIGIN: {
			execute:     opOrigin_SSC_Base,
			constantGas: GasQuickStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		CALLER: {
			execute:     opCaller_SSC_Base,
			constantGas: GasQuickStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		CALLVALUE: {
			execute:     opCallValue_SSC_Base,
			constantGas: GasQuickStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		CALLDATALOAD: {
			execute:     opCallDataLoad_SSC_Base,
			constantGas: GasFastestStep,
			minStack:    minStack(1, 1),
			maxStack:    maxStack(1, 1),
			valid:       true,
		},
		CALLDATASIZE: {
			execute:     opCallDataSize_SSC_Base,
			constantGas: GasQuickStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		CALLDATACOPY: {
			execute:     opCallDataCopy_SSC_Base,
			constantGas: GasFastestStep,
			dynamicGas:  gasCallDataCopy,
			minStack:    minStack(3, 0),
			maxStack:    maxStack(3, 0),
			memorySize:  memoryCallDataCopy,
			valid:       true,
		},
		CODESIZE: {
			execute:     opCodeSize_SSC_Base,
			constantGas: GasQuickStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		CODECOPY: {
			execute:     opCodeCopy_SSC_Base,
			constantGas: GasFastestStep,
			dynamicGas:  gasCodeCopy,
			minStack:    minStack(3, 0),
			maxStack:    maxStack(3, 0),
			memorySize:  memoryCodeCopy,
			valid:       true,
		},
		GASPRICE: {
			execute:     opGasprice_SSC_Base,
			constantGas: GasQuickStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		EXTCODESIZE: {
			execute:     opExtCodeSize_SSC_Base,
			constantGas: params.ExtcodeSizeGasFrontier,
			minStack:    minStack(1, 1),
			maxStack:    maxStack(1, 1),
			valid:       true,
		},
		EXTCODECOPY: {
			execute:     opExtCodeCopy_SSC_Base,
			constantGas: params.ExtcodeCopyBaseFrontier,
			dynamicGas:  gasExtCodeCopy,
			minStack:    minStack(4, 0),
			maxStack:    maxStack(4, 0),
			memorySize:  memoryExtCodeCopy,
			valid:       true,
		},
		BLOCKHASH: {
			execute:     opBlockhash_SSC_Base,
			constantGas: GasExtStep,
			minStack:    minStack(1, 1),
			maxStack:    maxStack(1, 1),
			valid:       true,
		},
		COINBASE: {
			execute:     opCoinbase_SSC_Base,
			constantGas: GasQuickStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		TIMESTAMP: {
			execute:     opTimestamp_SSC_Base,
			constantGas: GasQuickStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		NUMBER: {
			execute:     opNumber_SSC_Base,
			constantGas: GasQuickStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		DIFFICULTY: {
			execute:     opDifficulty_SSC_Base,
			constantGas: GasQuickStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		GASLIMIT: {
			execute:     opGasLimit_SSC_Base,
			constantGas: GasQuickStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		POP: {
			execute:     opPop_SSC_Base,
			constantGas: GasQuickStep,
			minStack:    minStack(1, 0),
			maxStack:    maxStack(1, 0),
			valid:       true,
		},
		MLOAD: {
			execute:     opMload_SSC_Base,
			constantGas: GasFastestStep,
			dynamicGas:  gasMLoad,
			minStack:    minStack(1, 1),
			maxStack:    maxStack(1, 1),
			memorySize:  memoryMLoad,
			valid:       true,
		},
		MSTORE: {
			execute:     opMstore_SSC_Base,
			constantGas: GasFastestStep,
			dynamicGas:  gasMStore,
			minStack:    minStack(2, 0),
			maxStack:    maxStack(2, 0),
			memorySize:  memoryMStore,
			valid:       true,
		},
		MSTORE8: {
			execute:     opMstore8_SSC_Base,
			constantGas: GasFastestStep,
			dynamicGas:  gasMStore8,
			memorySize:  memoryMStore8,
			minStack:    minStack(2, 0),
			maxStack:    maxStack(2, 0),

			valid: true,
		},
		SLOAD: {
			execute:     opSload_SSC_Base,
			constantGas: params.SloadGasFrontier,
			minStack:    minStack(1, 1),
			maxStack:    maxStack(1, 1),
			valid:       true,
		},
		SSTORE: {
			execute:    opSstore_SSC_Base,
			dynamicGas: gasSStore,
			minStack:   minStack(2, 0),
			maxStack:   maxStack(2, 0),
			valid:      true,
			writes:     true,
		},
		JUMP: {
			execute:     opJump_SSC_Base,
			constantGas: GasMidStep,
			minStack:    minStack(1, 0),
			maxStack:    maxStack(1, 0),
			jumps:       true,
			valid:       true,
		},
		JUMPI: {
			execute:     opJumpi_SSC_Base,
			constantGas: GasSlowStep,
			minStack:    minStack(2, 0),
			maxStack:    maxStack(2, 0),
			jumps:       true,
			valid:       true,
		},
		PC: {
			execute:     opPc_SSC_Base,
			constantGas: GasQuickStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		MSIZE: {
			execute:     opMsize_SSC_Base,
			constantGas: GasQuickStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		GAS: {
			execute:     opGas_SSC_Base,
			constantGas: GasQuickStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		JUMPDEST: {
			execute:     opJumpdest_SSC_Base,
			constantGas: params.JumpdestGas,
			minStack:    minStack(0, 0),
			maxStack:    maxStack(0, 0),
			valid:       true,
		},
		PUSH1: {
			execute:     opPush1_SSC_Base,
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH2: {
			execute:     makePush(2, 2),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH3: {
			execute:     makePush(3, 3),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH4: {
			execute:     makePush(4, 4),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH5: {
			execute:     makePush(5, 5),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH6: {
			execute:     makePush(6, 6),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH7: {
			execute:     makePush(7, 7),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH8: {
			execute:     makePush(8, 8),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH9: {
			execute:     makePush(9, 9),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH10: {
			execute:     makePush(10, 10),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH11: {
			execute:     makePush(11, 11),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH12: {
			execute:     makePush(12, 12),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH13: {
			execute:     makePush(13, 13),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH14: {
			execute:     makePush(14, 14),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH15: {
			execute:     makePush(15, 15),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH16: {
			execute:     makePush(16, 16),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH17: {
			execute:     makePush(17, 17),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH18: {
			execute:     makePush(18, 18),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH19: {
			execute:     makePush(19, 19),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH20: {
			execute:     makePush(20, 20),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH21: {
			execute:     makePush(21, 21),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH22: {
			execute:     makePush(22, 22),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH23: {
			execute:     makePush(23, 23),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH24: {
			execute:     makePush(24, 24),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH25: {
			execute:     makePush(25, 25),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH26: {
			execute:     makePush(26, 26),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH27: {
			execute:     makePush(27, 27),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH28: {
			execute:     makePush(28, 28),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH29: {
			execute:     makePush(29, 29),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH30: {
			execute:     makePush(30, 30),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH31: {
			execute:     makePush(31, 31),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		PUSH32: {
			execute:     makePush(32, 32),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			valid:       true,
		},
		DUP1: {
			execute:     makeDup(1),
			constantGas: GasFastestStep,
			minStack:    minDupStack(1),
			maxStack:    maxDupStack(1),
			valid:       true,
		},
		DUP2: {
			execute:     makeDup(2),
			constantGas: GasFastestStep,
			minStack:    minDupStack(2),
			maxStack:    maxDupStack(2),
			valid:       true,
		},
		DUP3: {
			execute:     makeDup(3),
			constantGas: GasFastestStep,
			minStack:    minDupStack(3),
			maxStack:    maxDupStack(3),
			valid:       true,
		},
		DUP4: {
			execute:     makeDup(4),
			constantGas: GasFastestStep,
			minStack:    minDupStack(4),
			maxStack:    maxDupStack(4),
			valid:       true,
		},
		DUP5: {
			execute:     makeDup(5),
			constantGas: GasFastestStep,
			minStack:    minDupStack(5),
			maxStack:    maxDupStack(5),
			valid:       true,
		},
		DUP6: {
			execute:     makeDup(6),
			constantGas: GasFastestStep,
			minStack:    minDupStack(6),
			maxStack:    maxDupStack(6),
			valid:       true,
		},
		DUP7: {
			execute:     makeDup(7),
			constantGas: GasFastestStep,
			minStack:    minDupStack(7),
			maxStack:    maxDupStack(7),
			valid:       true,
		},
		DUP8: {
			execute:     makeDup(8),
			constantGas: GasFastestStep,
			minStack:    minDupStack(8),
			maxStack:    maxDupStack(8),
			valid:       true,
		},
		DUP9: {
			execute:     makeDup(9),
			constantGas: GasFastestStep,
			minStack:    minDupStack(9),
			maxStack:    maxDupStack(9),
			valid:       true,
		},
		DUP10: {
			execute:     makeDup(10),
			constantGas: GasFastestStep,
			minStack:    minDupStack(10),
			maxStack:    maxDupStack(10),
			valid:       true,
		},
		DUP11: {
			execute:     makeDup(11),
			constantGas: GasFastestStep,
			minStack:    minDupStack(11),
			maxStack:    maxDupStack(11),
			valid:       true,
		},
		DUP12: {
			execute:     makeDup(12),
			constantGas: GasFastestStep,
			minStack:    minDupStack(12),
			maxStack:    maxDupStack(12),
			valid:       true,
		},
		DUP13: {
			execute:     makeDup(13),
			constantGas: GasFastestStep,
			minStack:    minDupStack(13),
			maxStack:    maxDupStack(13),
			valid:       true,
		},
		DUP14: {
			execute:     makeDup(14),
			constantGas: GasFastestStep,
			minStack:    minDupStack(14),
			maxStack:    maxDupStack(14),
			valid:       true,
		},
		DUP15: {
			execute:     makeDup(15),
			constantGas: GasFastestStep,
			minStack:    minDupStack(15),
			maxStack:    maxDupStack(15),
			valid:       true,
		},
		DUP16: {
			execute:     makeDup(16),
			constantGas: GasFastestStep,
			minStack:    minDupStack(16),
			maxStack:    maxDupStack(16),
			valid:       true,
		},
		SWAP1: {
			execute:     makeSwap(1),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(2),
			maxStack:    maxSwapStack(2),
			valid:       true,
		},
		SWAP2: {
			execute:     makeSwap(2),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(3),
			maxStack:    maxSwapStack(3),
			valid:       true,
		},
		SWAP3: {
			execute:     makeSwap(3),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(4),
			maxStack:    maxSwapStack(4),
			valid:       true,
		},
		SWAP4: {
			execute:     makeSwap(4),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(5),
			maxStack:    maxSwapStack(5),
			valid:       true,
		},
		SWAP5: {
			execute:     makeSwap(5),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(6),
			maxStack:    maxSwapStack(6),
			valid:       true,
		},
		SWAP6: {
			execute:     makeSwap(6),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(7),
			maxStack:    maxSwapStack(7),
			valid:       true,
		},
		SWAP7: {
			execute:     makeSwap(7),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(8),
			maxStack:    maxSwapStack(8),
			valid:       true,
		},
		SWAP8: {
			execute:     makeSwap(8),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(9),
			maxStack:    maxSwapStack(9),
			valid:       true,
		},
		SWAP9: {
			execute:     makeSwap(9),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(10),
			maxStack:    maxSwapStack(10),
			valid:       true,
		},
		SWAP10: {
			execute:     makeSwap(10),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(11),
			maxStack:    maxSwapStack(11),
			valid:       true,
		},
		SWAP11: {
			execute:     makeSwap(11),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(12),
			maxStack:    maxSwapStack(12),
			valid:       true,
		},
		SWAP12: {
			execute:     makeSwap(12),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(13),
			maxStack:    maxSwapStack(13),
			valid:       true,
		},
		SWAP13: {
			execute:     makeSwap(13),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(14),
			maxStack:    maxSwapStack(14),
			valid:       true,
		},
		SWAP14: {
			execute:     makeSwap(14),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(15),
			maxStack:    maxSwapStack(15),
			valid:       true,
		},
		SWAP15: {
			execute:     makeSwap(15),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(16),
			maxStack:    maxSwapStack(16),
			valid:       true,
		},
		SWAP16: {
			execute:     makeSwap(16),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(17),
			maxStack:    maxSwapStack(17),
			valid:       true,
		},
		LOG0: {
			execute:    makeLog(0),
			dynamicGas: makeGasLog(0),
			minStack:   minStack(2, 0),
			maxStack:   maxStack(2, 0),
			memorySize: memoryLog,
			valid:      true,
			writes:     true,
		},
		LOG1: {
			execute:    makeLog(1),
			dynamicGas: makeGasLog(1),
			minStack:   minStack(3, 0),
			maxStack:   maxStack(3, 0),
			memorySize: memoryLog,
			valid:      true,
			writes:     true,
		},
		LOG2: {
			execute:    makeLog(2),
			dynamicGas: makeGasLog(2),
			minStack:   minStack(4, 0),
			maxStack:   maxStack(4, 0),
			memorySize: memoryLog,
			valid:      true,
			writes:     true,
		},
		LOG3: {
			execute:    makeLog(3),
			dynamicGas: makeGasLog(3),
			minStack:   minStack(5, 0),
			maxStack:   maxStack(5, 0),
			memorySize: memoryLog,
			valid:      true,
			writes:     true,
		},
		LOG4: {
			execute:    makeLog(4),
			dynamicGas: makeGasLog(4),
			minStack:   minStack(6, 0),
			maxStack:   maxStack(6, 0),
			memorySize: memoryLog,
			valid:      true,
			writes:     true,
		},
		CREATE: {
			execute:     opCreate_SSC_Base,
			constantGas: params.CreateGas,
			dynamicGas:  gasCreate,
			minStack:    minStack(3, 1),
			maxStack:    maxStack(3, 1),
			memorySize:  memoryCreate,
			valid:       true,
			writes:      true,
			returns:     true,
		},
		CALL: {
			execute:     opCall_SSC_Base,
			constantGas: params.CallGasFrontier,
			dynamicGas:  gasCall,
			minStack:    minStack(7, 1),
			maxStack:    maxStack(7, 1),
			memorySize:  memoryCall,
			valid:       true,
			returns:     true,
		},
		CALLCODE: {
			execute:     opCallCode_SSC_Base,
			constantGas: params.CallGasFrontier,
			dynamicGas:  gasCallCode,
			minStack:    minStack(7, 1),
			maxStack:    maxStack(7, 1),
			memorySize:  memoryCall,
			valid:       true,
			returns:     true,
		},
		RETURN: {
			execute:    opReturn_SSC_Base,
			dynamicGas: gasReturn,
			minStack:   minStack(2, 0),
			maxStack:   maxStack(2, 0),
			memorySize: memoryReturn,
			halts:      true,
			valid:      true,
		},
		SELFDESTRUCT: {
			execute:    opSuicide_SSC_Base,
			dynamicGas: gasSelfdestruct,
			minStack:   minStack(1, 0),
			maxStack:   maxStack(1, 0),
			halts:      true,
			valid:      true,
			writes:     true,
		},
	}
}

func opAdd_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	x, y := stack.pop(), stack.peek()
	math.U256(y.Add(x, y))

	interpreter.intPool.put(x)
	return nil, nil
}

func opSub_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	x, y := stack.pop(), stack.peek()
	math.U256(y.Sub(x, y))

	interpreter.intPool.put(x)
	return nil, nil
}

func opMul_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	x, y := stack.pop(), stack.pop()
	stack.push(math.U256(x.Mul(x, y)))

	interpreter.intPool.put(y)

	return nil, nil
}

func opDiv_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	x, y := stack.pop(), stack.peek()
	if y.Sign() != 0 {
		math.U256(y.Div(x, y))
	} else {
		y.SetUint64(0)
	}
	interpreter.intPool.put(x)
	return nil, nil
}

func opSdiv_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	x, y := math.S256(stack.pop()), math.S256(stack.pop())
	res := interpreter.intPool.getZero()

	if y.Sign() == 0 || x.Sign() == 0 {
		stack.push(res)
	} else {
		if x.Sign() != y.Sign() {
			res.Div(x.Abs(x), y.Abs(y))
			res.Neg(res)
		} else {
			res.Div(x.Abs(x), y.Abs(y))
		}
		stack.push(math.U256(res))
	}
	interpreter.intPool.put(x, y)
	return nil, nil
}

func opMod_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	x, y := stack.pop(), stack.pop()
	if y.Sign() == 0 {
		stack.push(x.SetUint64(0))
	} else {
		stack.push(math.U256(x.Mod(x, y)))
	}
	interpreter.intPool.put(y)
	return nil, nil
}

func opSmod_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	x, y := math.S256(stack.pop()), math.S256(stack.pop())
	res := interpreter.intPool.getZero()

	if y.Sign() == 0 {
		stack.push(res)
	} else {
		if x.Sign() < 0 {
			res.Mod(x.Abs(x), y.Abs(y))
			res.Neg(res)
		} else {
			res.Mod(x.Abs(x), y.Abs(y))
		}
		stack.push(math.U256(res))
	}
	interpreter.intPool.put(x, y)
	return nil, nil
}

func opExp_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	base, exponent := stack.pop(), stack.pop()
	// some shortcuts
	cmpToOne := exponent.Cmp(big1)
	if cmpToOne < 0 { // Exponent is zero
		// x ^ 0 == 1
		stack.push(base.SetUint64(1))
	} else if base.Sign() == 0 {
		// 0 ^ y, if y != 0, == 0
		stack.push(base.SetUint64(0))
	} else if cmpToOne == 0 { // Exponent is one
		// x ^ 1 == x
		stack.push(base)
	} else {
		stack.push(math.Exp(base, exponent))
		interpreter.intPool.put(base)
	}
	interpreter.intPool.put(exponent)
	return nil, nil
}

func opSignExtend_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	back := stack.pop()
	if back.Cmp(big.NewInt(31)) < 0 {
		bit := uint(back.Uint64()*8 + 7)
		num := stack.pop()
		mask := back.Lsh(common.Big1, bit)
		mask.Sub(mask, common.Big1)
		if num.Bit(int(bit)) > 0 {
			num.Or(num, mask.Not(mask))
		} else {
			num.And(num, mask)
		}

		stack.push(math.U256(num))
	}

	interpreter.intPool.put(back)
	return nil, nil
}

func opNot_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	x := stack.peek()
	math.U256(x.Not(x))
	return nil, nil
}

func opLt_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	x, y := stack.pop(), stack.peek()
	if x.Cmp(y) < 0 {
		y.SetUint64(1)
	} else {
		y.SetUint64(0)
	}
	interpreter.intPool.put(x)
	return nil, nil
}

func opGt_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	x, y := stack.pop(), stack.peek()
	if x.Cmp(y) > 0 {
		y.SetUint64(1)
	} else {
		y.SetUint64(0)
	}
	interpreter.intPool.put(x)
	return nil, nil
}

func opSlt_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	x, y := stack.pop(), stack.peek()

	xSign := x.Cmp(tt255)
	ySign := y.Cmp(tt255)

	switch {
	case xSign >= 0 && ySign < 0:
		y.SetUint64(1)

	case xSign < 0 && ySign >= 0:
		y.SetUint64(0)

	default:
		if x.Cmp(y) < 0 {
			y.SetUint64(1)
		} else {
			y.SetUint64(0)
		}
	}
	interpreter.intPool.put(x)
	return nil, nil
}

func opSgt_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	x, y := stack.pop(), stack.peek()

	xSign := x.Cmp(tt255)
	ySign := y.Cmp(tt255)

	switch {
	case xSign >= 0 && ySign < 0:
		y.SetUint64(0)

	case xSign < 0 && ySign >= 0:
		y.SetUint64(1)

	default:
		if x.Cmp(y) > 0 {
			y.SetUint64(1)
		} else {
			y.SetUint64(0)
		}
	}
	interpreter.intPool.put(x)
	return nil, nil
}

func opEq_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	x, y := stack.pop(), stack.peek()
	if x.Cmp(y) == 0 {
		y.SetUint64(1)
	} else {
		y.SetUint64(0)
	}
	interpreter.intPool.put(x)
	return nil, nil
}

func opIszero_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	x := stack.peek()
	if x.Sign() > 0 {
		x.SetUint64(0)
	} else {
		x.SetUint64(1)
	}
	return nil, nil
}

func opAnd_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	x, y := stack.pop(), stack.pop()
	stack.push(x.And(x, y))

	interpreter.intPool.put(y)
	return nil, nil
}

func opOr_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	x, y := stack.pop(), stack.peek()
	y.Or(x, y)

	interpreter.intPool.put(x)
	return nil, nil
}

func opXor_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	x, y := stack.pop(), stack.peek()
	y.Xor(x, y)

	interpreter.intPool.put(x)
	return nil, nil
}

func opByte_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	th, val := stack.pop(), stack.peek()
	if th.Cmp(common.Big32) < 0 {
		b := math.Byte(val, 32, int(th.Int64()))
		val.SetUint64(uint64(b))
	} else {
		val.SetUint64(0)
	}
	interpreter.intPool.put(th)
	return nil, nil
}

func opAddmod_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	x, y, z := stack.pop(), stack.pop(), stack.pop()
	if z.Cmp(bigZero) > 0 {
		x.Add(x, y)
		x.Mod(x, z)
		stack.push(math.U256(x))
	} else {
		stack.push(x.SetUint64(0))
	}
	interpreter.intPool.put(y, z)
	return nil, nil
}

func opMulmod_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	x, y, z := stack.pop(), stack.pop(), stack.pop()
	if z.Cmp(bigZero) > 0 {
		x.Mul(x, y)
		x.Mod(x, z)
		stack.push(math.U256(x))
	} else {
		stack.push(x.SetUint64(0))
	}
	interpreter.intPool.put(y, z)
	return nil, nil
}

// opSHL implements Shift Left
// The SHL instruction (shift left) pops 2 values from the Stack, first arg1 and then arg2,
// and pushes on the Stack arg2 shifted to the left by arg1 number of bits.
func opSHL_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	// Note, second operand is left in the Stack; accumulate result into it, and no need to push it afterwards
	shift, value := math.U256(stack.pop()), math.U256(stack.peek())
	defer interpreter.intPool.put(shift) // First operand back into the pool

	if shift.Cmp(common.Big256) >= 0 {
		value.SetUint64(0)
		return nil, nil
	}
	n := uint(shift.Uint64())
	math.U256(value.Lsh(value, n))

	return nil, nil
}

// opSHR implements Logical Shift Right
// The SHR instruction (logical shift right) pops 2 values from the Stack, first arg1 and then arg2,
// and pushes on the Stack arg2 shifted to the right by arg1 number of bits with zero fill.
func opSHR_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	// Note, second operand is left in the Stack; accumulate result into it, and no need to push it afterwards
	shift, value := math.U256(stack.pop()), math.U256(stack.peek())
	defer interpreter.intPool.put(shift) // First operand back into the pool

	if shift.Cmp(common.Big256) >= 0 {
		value.SetUint64(0)
		return nil, nil
	}
	n := uint(shift.Uint64())
	math.U256(value.Rsh(value, n))

	return nil, nil
}

// opSAR implements Arithmetic Shift Right
// The SAR instruction (arithmetic shift right) pops 2 values from the Stack, first arg1 and then arg2,
// and pushes on the Stack arg2 shifted to the right by arg1 number of bits with sign extension.
func opSAR_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	// Note, S256 returns (potentially) a new bigint, so we're popping, not peeking this one
	shift, value := math.U256(stack.pop()), math.S256(stack.pop())
	defer interpreter.intPool.put(shift) // First operand back into the pool

	if shift.Cmp(common.Big256) >= 0 {
		if value.Sign() >= 0 {
			value.SetUint64(0)
		} else {
			value.SetInt64(-1)
		}
		stack.push(math.U256(value))
		return nil, nil
	}
	n := uint(shift.Uint64())
	value.Rsh(value, n)
	stack.push(math.U256(value))

	return nil, nil
}

func opSha3_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	offset, size := stack.pop(), stack.pop()
	data := memory.GetPtr(offset.Int64(), size.Int64())

	if interpreter.hasher == nil {
		interpreter.hasher = sha3.NewLegacyKeccak256().(keccakState)
	} else {
		interpreter.hasher.Reset()
	}
	interpreter.hasher.Write(data)
	interpreter.hasher.Read(interpreter.hasherBuf[:])

	evm := interpreter.vm
	if evm.vmConfig.EnablePreimageRecording {
		evm.StateDB.AddPreimage(interpreter.hasherBuf, data)
	}
	stack.push(interpreter.intPool.get().SetBytes(interpreter.hasherBuf[:]))

	interpreter.intPool.put(offset, size)
	return nil, nil
}

func opAddress_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	stack.push(interpreter.intPool.get().SetBytes(contract.Address().Bytes()))
	return nil, nil
}

func opOrigin_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	stack.push(interpreter.intPool.get().SetBytes(interpreter.vm.Origin.Bytes()))
	return nil, nil
}

func opCaller_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	stack.push(interpreter.intPool.get().SetBytes(contract.Caller().Bytes()))
	return nil, nil
}

func opCallValue_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	stack.push(interpreter.intPool.get().Set(contract.value))
	return nil, nil
}

func opCallDataSize_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	stack.push(interpreter.intPool.get().SetInt64(int64(len(contract.Input))))
	return nil, nil
}

func opCallDataCopy_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	var (
		memOffset  = stack.pop()
		dataOffset = stack.pop()
		length     = stack.pop()
	)
	memory.Set(memOffset.Uint64(), length.Uint64(), getDataBig(contract.Input, dataOffset, length))

	interpreter.intPool.put(memOffset, dataOffset, length)
	return nil, nil
}

func opReturnDataSize_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	stack.push(interpreter.intPool.get().SetUint64(uint64(len(interpreter.returnData))))
	return nil, nil
}

func opReturnDataCopy_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	var (
		memOffset  = stack.pop()
		dataOffset = stack.pop()
		length     = stack.pop()

		end = interpreter.intPool.get().Add(dataOffset, length)
	)
	defer interpreter.intPool.put(memOffset, dataOffset, length, end)

	if !end.IsUint64() || uint64(len(interpreter.returnData)) < end.Uint64() {
		return nil, errReturnDataOutOfBounds
	}
	memory.Set(memOffset.Uint64(), length.Uint64(), interpreter.returnData[dataOffset.Uint64():end.Uint64()])

	return nil, nil
}

func opExtCodeSize_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	slot := stack.peek()
	address := common.BigToAddress(slot)
	fixValidatorCode := interpreter.vm.chainRules.IsValidatorCodeFix &&
		interpreter.vm.ShardID == shard.BeaconChainShardID &&
		interpreter.vm.StateDB.IsValidator(address)
	if fixValidatorCode {
		// https://github.com/ethereum/solidity/blob/develop/Changelog.md#081-2021-01-27
		// per this link, <address>.code.length calls extcodesize on the address so this fix will work
		slot.SetUint64(0)
		return nil, nil
	}
	slot.SetUint64(uint64(interpreter.vm.StateDB.GetCodeSize(common.BigToAddress(slot))))

	return nil, nil
}

func opCodeSize_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	l := interpreter.intPool.get().SetInt64(int64(len(contract.Code)))
	stack.push(l)

	return nil, nil
}

func opCodeCopy_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	var (
		memOffset  = stack.pop()
		codeOffset = stack.pop()
		length     = stack.pop()
	)
	codeCopy := getDataBig(contract.Code, codeOffset, length)
	memory.Set(memOffset.Uint64(), length.Uint64(), codeCopy)

	interpreter.intPool.put(memOffset, codeOffset, length)
	return nil, nil
}

func opExtCodeCopy_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	var (
		addr       = common.BigToAddress(stack.pop())
		memOffset  = stack.pop()
		codeOffset = stack.pop()
		length     = stack.pop()
	)
	var code []byte
	fixValidatorCode := interpreter.vm.chainRules.IsValidatorCodeFix &&
		interpreter.vm.ShardID == shard.BeaconChainShardID &&
		interpreter.vm.StateDB.IsValidator(addr)
	if fixValidatorCode {
		// for EOAs that are not validators, statedb returns nil
		code = nil
	} else {
		code = interpreter.vm.StateDB.GetCode(addr)
	}
	codeCopy := getDataBig(code, codeOffset, length)
	memory.Set(memOffset.Uint64(), length.Uint64(), codeCopy)

	interpreter.intPool.put(memOffset, codeOffset, length)
	return nil, nil
}

// opExtCodeHash returns the code hash of a specified account.
// There are several cases when the function is called, while we can relay everything
// to `state.GetCodeHash` function to ensure the correctness.
//
//	(1) Caller tries to get the code hash of a normal contract account, state
//
// should return the relative code hash and set it as the result.
//
//	(2) Caller tries to get the code hash of a non-existent account, state should
//
// return common.Hash{} and zero will be set as the result.
//
//	(3) Caller tries to get the code hash for an account without contract code,
//
// state should return emptyCodeHash(0xc5d246...) as the result.
//
//	(4) Caller tries to get the code hash of a precompiled account, the result
//
// should be zero or emptyCodeHash.
//
// It is worth noting that in order to avoid unnecessary create and clean,
// all precompile accounts on mainnet have been transferred 1 wei, so the return
// here should be emptyCodeHash.
// If the precompile account is not transferred any amount on a private or
// customized chain, the return value will be zero.
//
//	(5) Caller tries to get the code hash for an account which is marked as suicided
//
// in the current transaction, the code hash of this account should be returned.
//
//	(6) Caller tries to get the code hash for an account which is marked as deleted,
//
// this account should be regarded as a non-existent account and zero should be returned.
func opExtCodeHash_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	slot := stack.peek()
	address := common.BigToAddress(slot)
	if interpreter.vm.StateDB.Empty(address) {
		slot.SetUint64(0)
	} else {
		fixValidatorCode := interpreter.vm.chainRules.IsValidatorCodeFix &&
			interpreter.vm.ShardID == shard.BeaconChainShardID &&
			interpreter.vm.StateDB.IsValidator(address)
		if fixValidatorCode {
			slot.SetBytes(emptyCodeHash.Bytes())
		} else {
			slot.SetBytes(interpreter.vm.StateDB.GetCodeHash(address).Bytes())
		}
	}
	return nil, nil
}

func opGasprice_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	stack.push(interpreter.intPool.get().Set(interpreter.vm.GasPrice))
	return nil, nil
}

func opBlockhash_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	num := stack.pop()

	n := interpreter.intPool.get().Sub(interpreter.vm.BlockNumber, common.Big257)
	if num.Cmp(n) > 0 && num.Cmp(interpreter.vm.BlockNumber) < 0 {
		stack.push(interpreter.vm.GetHash(num.Uint64()).Big())
	} else {
		stack.push(interpreter.intPool.getZero())
	}
	interpreter.intPool.put(num, n)
	return nil, nil
}

func opCoinbase_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	stack.push(interpreter.intPool.get().SetBytes(interpreter.vm.Coinbase.Bytes()))
	return nil, nil
}

func opTimestamp_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	stack.push(math.U256(interpreter.intPool.get().Set(interpreter.vm.Time)))
	return nil, nil
}

func opNumber_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	stack.push(math.U256(interpreter.intPool.get().Set(interpreter.vm.BlockNumber)))
	return nil, nil
}

func opDifficulty_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	stack.push(math.U256(interpreter.intPool.get().Set(big.NewInt(0))))
	return nil, nil
}

func opGasLimit_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	stack.push(math.U256(interpreter.intPool.get().SetUint64(interpreter.vm.GasLimit)))
	return nil, nil
}

func opPop_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	interpreter.intPool.put(stack.pop())
	return nil, nil
}

func opMload_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	v := stack.peek()
	offset := v.Int64()
	v.SetBytes(memory.GetPtr(offset, 32))
	return nil, nil
}

func opMstore_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	// pop value of the Stack
	mStart, val := stack.pop(), stack.pop()
	memory.Set32(mStart.Uint64(), val)

	interpreter.intPool.put(mStart, val)
	return nil, nil
}

func opMstore8_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	off, val := stack.pop().Int64(), stack.pop().Int64()
	memory.store[off] = byte(val & 0xff)

	return nil, nil
}

func opJump_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	pos := stack.pop()
	if !contract.validJumpdest(pos) {
		return nil, errInvalidJump
	}
	*pc = pos.Uint64()

	interpreter.intPool.put(pos)
	return nil, nil
}

func opJumpi_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	pos, cond := stack.pop(), stack.pop()
	if cond.Sign() != 0 {
		if !contract.validJumpdest(pos) {
			return nil, errInvalidJump
		}
		*pc = pos.Uint64()
	} else {
		*pc++
	}

	interpreter.intPool.put(pos, cond)
	return nil, nil
}

func opJumpdest_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	return nil, nil
}

func opPc_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	stack.push(interpreter.intPool.get().SetUint64(*pc))
	return nil, nil
}

func opMsize_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	stack.push(interpreter.intPool.get().SetInt64(int64(memory.Len())))
	return nil, nil
}

func opGas_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	stack.push(interpreter.intPool.get().SetUint64(contract.Gas))
	return nil, nil
}

func opCreate_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	var (
		value        = stack.pop()
		offset, size = stack.pop(), stack.pop()
		input        = memory.GetCopy(offset.Int64(), size.Int64())
		gas          = contract.Gas
	)
	if interpreter.vm.ChainConfig().IsS3(interpreter.vm.EpochNumber) {
		gas -= gas / 64
	}

	contract.UseGas(gas)
	res, addr, returnGas, suberr := interpreter.vm.Create(contract, input, gas, value)
	// Push item on the Stack based on the returned error. If the ruleset is
	// homestead we must check for CodeStoreOutOfGasError (homestead only
	// rule) and treat as an error, if the ruleset is frontier we must
	// ignore this error and pretend the operation was successful.
	if interpreter.vm.ChainConfig().IsS3(interpreter.vm.EpochNumber) && suberr == ErrCodeStoreOutOfGas {
		stack.push(interpreter.intPool.getZero())
	} else if suberr != nil && suberr != ErrCodeStoreOutOfGas {
		stack.push(interpreter.intPool.getZero())
	} else {
		stack.push(interpreter.intPool.get().SetBytes(addr.Bytes()))
	}
	contract.Gas += returnGas
	interpreter.intPool.put(value, offset, size)

	if suberr == ErrExecutionReverted {
		return res, nil
	}
	return nil, nil
}

func opCreate2_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	var (
		endowment    = stack.pop()
		offset, size = stack.pop(), stack.pop()
		salt         = stack.pop()
		input        = memory.GetCopy(offset.Int64(), size.Int64())
		gas          = contract.Gas
	)

	// Apply EIP150
	gas -= gas / 64
	contract.UseGas(gas)
	res, addr, returnGas, suberr := interpreter.vm.Create2(contract, input, gas, endowment, salt)
	// Push item on the Stack based on the returned error.
	if suberr != nil {
		stack.push(interpreter.intPool.getZero())
	} else {
		stack.push(interpreter.intPool.get().SetBytes(addr.Bytes()))
	}
	contract.Gas += returnGas
	interpreter.intPool.put(endowment, offset, size, salt)

	if suberr == ErrExecutionReverted {
		return res, nil
	}
	return nil, nil
}

func opRevert_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	offset, size := stack.pop(), stack.pop()
	ret := memory.GetPtr(offset.Int64(), size.Int64())

	interpreter.intPool.put(offset, size)
	return ret, nil
}

func opStop_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	return nil, nil
}

func opSuicide_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	balance := interpreter.vm.StateDB.GetBalance(contract.Address())
	interpreter.vm.StateDB.AddBalance(common.BigToAddress(stack.pop()), balance)

	interpreter.vm.StateDB.Suicide(contract.Address())
	return nil, nil
}

// opPush1 is a specialized version of pushN
func opPush1_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	var (
		codeLen = uint64(len(contract.Code))
		integer = interpreter.intPool.get()
	)
	*pc++
	if *pc < codeLen {
		stack.push(integer.SetUint64(uint64(contract.Code[*pc])))
	} else {
		stack.push(integer.SetUint64(0))
	}
	return nil, nil
}

func opCallDataLoad_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	stack.push(interpreter.intPool.get().SetBytes(getDataBig(contract.Input, stack.pop(), big32)))
	return nil, nil
}

// ---------------------------------- options need to override for different execution type ----------------------------

func opBalance_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	slot := stack.peek()
	slot.Set(interpreter.vm.StateDB.GetBalance(common.BigToAddress(slot)))
	return nil, nil
}

func opSload_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	loc := stack.peek()
	val, _ := interpreter.vm.StateDB.GetState(contract.Address(), common.BigToHash(loc))
	loc.SetBytes(val.Bytes())
	return nil, nil
}

func opSstore_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	loc := common.BigToHash(stack.pop())
	val := stack.pop()
	interpreter.vm.StateDB.SetState(contract.Address(), loc, common.BigToHash(val))

	interpreter.intPool.put(val)
	return nil, nil
}

func opCall_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
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
	ret, returnGas, err := interpreter.vm.Call(contract, toAddr, args, gas, value)
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

func opCallCode_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
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
	ret, returnGas, err := interpreter.vm.CallCode(contract, toAddr, args, gas, value)
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

func opDelegateCall_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	// Pop gas. The actual gas is in interpreter.vm.callGasTemp.
	interpreter.intPool.put(stack.pop())
	gas := interpreter.vm.callGasTemp
	// Pop other call parameters.
	addr, inOffset, inSize, retOffset, retSize := stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop()
	toAddr := common.BigToAddress(addr)
	// Get arguments from the memory.
	args := memory.GetPtr(inOffset.Int64(), inSize.Int64())

	ret, returnGas, err := interpreter.vm.DelegateCall(contract, toAddr, args, gas)
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

func opReturn_SSC_Base(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	interpreter := inp.(*SSCVMInterpreter)
	offset, size := stack.pop(), stack.pop()
	ret := memory.GetPtr(offset.Int64(), size.Int64())

	interpreter.intPool.put(offset, size)
	return ret, nil
}
