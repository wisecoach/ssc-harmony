// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/core/vm"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/numeric"
	"github.com/harmony-one/harmony/shard"
	"github.com/pkg/errors"
)

type SSCStateTransition struct {
	gp         *GasPool
	msg        Message
	gas        uint64
	gasPrice   *big.Int
	initialGas uint64
	value      *big.Int
	data       []byte
	state      vm.StateDB
	vm         *vm.SSCVM
}

func NewSSCStateTransition(vm *vm.SSCVM, msg Message, gp *GasPool) *SSCStateTransition {
	return &SSCStateTransition{
		gp:       gp,
		vm:       vm,
		msg:      msg,
		gasPrice: msg.GasPrice(),
		value:    msg.Value(),
		data:     msg.Data(),
		state:    vm.StateDB,
	}
}

// to returns the recipient of the message.
func (st *SSCStateTransition) to() common.Address {
	if st.msg == nil || st.msg.To() == nil /* contract creation */ {
		return common.Address{}
	}
	return *st.msg.To()
}

func (st *SSCStateTransition) useGas(amount uint64) error {
	if st.gas < amount {
		return vm.ErrOutOfGas
	}
	st.gas -= amount

	return nil
}

func (st *SSCStateTransition) buyGas() error {
	mgval := new(big.Int).Mul(new(big.Int).SetUint64(st.msg.Gas()), st.gasPrice)
	if have := st.state.GetBalance(st.msg.From()); have.Cmp(mgval) < 0 {
		return errors.Wrapf(
			errInsufficientBalanceForGas,
			"had: %s but need: %s", have.String(), mgval.String(),
		)
	}
	utils.SSCLogger().Debug().Msgf("buyGas, buy=%d, left=%d", st.msg.Gas(), st.gp.Gas())
	if err := st.gp.SubGas(st.msg.Gas()); err != nil {
		return err
	}
	st.gas += st.msg.Gas()

	st.initialGas = st.msg.Gas()
	st.state.SubBalance(st.msg.From(), mgval)
	return nil
}

func (st *SSCStateTransition) preCheck() error {
	// Make sure this transaction's nonce is correct.
	if st.msg.CheckNonce() && vm.IsSSCAddrApplyOnChain(st.to()) {
		// just check the nonce if is lower
		nonce := st.state.GetNonce(st.msg.From())
		if nonce > st.msg.Nonce() {
			utils.SSCLogger().Error().Err(ErrNonceTooLow).Str("from", st.msg.From().Hex()).Msgf("nonce too low, tx nonce: %d, state nonce: %d", st.msg.Nonce(), nonce)
			return ErrNonceTooLow
		}

		if nonce < st.msg.Nonce() {
			utils.SSCLogger().Error().Err(ErrNonceTooHigh).Str("from", st.msg.From().Hex()).Msgf("nonce too high, tx nonce: %d, state nonce: %d", st.msg.Nonce(), nonce)
			return ErrNonceTooHigh
		} else if nonce > st.msg.Nonce() {
			utils.SSCLogger().Error().Err(ErrNonceTooHigh).Str("from", st.msg.From().Hex()).Msgf("nonce too high, tx nonce: %d, state nonce: %d", st.msg.Nonce(), nonce)
			return ErrNonceTooLow
		}
	}
	utils.SSCLogger().Debug().Str("from", st.msg.From().Hex()).Msgf("nonce check passed, tx nonce: %d, state nonce: %d", st.msg.Nonce(), st.state.GetNonce(st.msg.From()))
	return st.buyGas()
}

// TransitionDb will transition the state by applying the current message and
// returning the result including the used gas. It returns an error if failed.
// An error indicates a consensus issue.
func (st *SSCStateTransition) TransitionDb() (ExecutionResult, error) {
	if err := st.preCheck(); err != nil {
		return ExecutionResult{}, err
	}
	msg := st.msg
	sender := vm.AccountRef(msg.From())
	homestead := st.vm.ChainConfig().IsS3(st.vm.Context.EpochNumber) // s3 includes homestead
	istanbul := st.vm.ChainConfig().IsIstanbul(st.vm.Context.EpochNumber)
	contractCreation := msg.To() == nil

	// Pay intrinsic gas
	gas, err := vm.IntrinsicGas(st.data, contractCreation, homestead, istanbul, false)
	if err != nil {
		return ExecutionResult{}, err
	}
	if err = st.useGas(gas); err != nil {
		return ExecutionResult{}, fmt.Errorf("%w: have %d, want %d", ErrIntrinsicGas, st.gas, gas)
	}

	sscvm := st.vm

	var ret []byte
	// All VM errors are valid except for insufficient balance, therefore returned separately
	var vmErr error

	st.state.SetNonce(msg.From(), st.state.GetNonce(sender.Address())+1)
	ret, st.gas, vmErr = sscvm.Call(sender, st.to(), st.data, st.gas, st.value)
	utils.SSCLogger().Debug().Msgf("TransitionDb, gas used: %d, gas left: %d, caller: %s", st.gasUsed(), st.gas, st.to())
	if vmErr != nil {
		// The only possible consensus-error would be if there wasn't
		// sufficient balance to make the transfer happen. The first
		// balance transfer may never fail.
		if vmErr == vm.ErrInsufficientBalance {
			utils.Logger().Debug().Err(vmErr).Msg("VM returned with error")
			return ExecutionResult{}, vmErr
		}
	}
	st.refundGas()
	st.collectGas()

	return ExecutionResult{
		ReturnData: ret,
		UsedGas:    st.gasUsed(),
		VMErr:      vmErr,
	}, err
}

func (st *SSCStateTransition) refundGas() {
	// Apply refund counter, capped to half of the used gas.
	refund := st.gasUsed() / 2
	if refund > st.state.GetRefund() {
		refund = st.state.GetRefund()
	}
	st.gas += refund

	// Return ETH for remaining gas, exchanged at the original rate.
	remaining := new(big.Int).Mul(new(big.Int).SetUint64(st.gas), st.gasPrice)
	st.state.AddBalance(st.msg.From(), remaining)

	// Also return remaining gas to the block gas counter so it is
	// available for the next transaction.
	st.gp.AddGas(st.gas)
}

func (st *SSCStateTransition) collectGas() {
	if config := st.vm.ChainConfig(); !config.IsStaking(st.vm.Context.EpochNumber) {
		// Before staking epoch, add the fees to the block producer
		txFee := new(big.Int).Mul(
			new(big.Int).SetUint64(st.gasUsed()),
			st.gasPrice,
		)
		st.state.AddBalance(st.vm.Context.Coinbase, txFee)
	} else if feeCollectors := shard.Schedule.InstanceForEpoch(
		st.vm.Context.EpochNumber,
	).FeeCollectors(); len(feeCollectors) > 0 {
		// The caller must ensure that the feeCollectors are accurately set
		// at the appropriate epochs
		txFee := numeric.NewDecFromBigInt(
			new(big.Int).Mul(
				new(big.Int).SetUint64(st.gasUsed()),
				st.gasPrice,
			),
		)
		for address, percent := range feeCollectors {
			collectedFee := percent.Mul(txFee)
			st.state.AddBalance(address, collectedFee.TruncateInt())
		}
	}
}

// gasUsed returns the amount of gas used up by the state transition.
func (st *SSCStateTransition) gasUsed() uint64 {
	return st.initialGas - st.gas
}
