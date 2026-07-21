package core

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/block"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/core/vm"
	"github.com/harmony-one/harmony/shard"
	"github.com/harmony-one/harmony/ssc/api"
	"math/big"
)

// NewSSCVMContext creates a new context for use in the SSCVM.
func NewSSCVMContext(origin common.Address, txHash common.Hash, simNum int, callIndex api.CallIndex, gasPrice *big.Int, header *block.Header, chain ChainContext, author *common.Address) vm.Context {
	// If we don't have an explicit author (i.e. not mining), extract from the header
	var beneficiary common.Address
	if author == nil {
		beneficiary = common.Address{} // Ignore error, we're past header validation
	} else {
		beneficiary = *author
	}
	vrf := common.Hash{}
	if len(header.Vrf()) >= 32 {
		vrfAndProof := header.Vrf()
		copy(vrf[:], vrfAndProof[:32])
	}
	return vm.Context{
		CanTransfer:           SSC_CanTransfer,
		Transfer:              SSC_Transfer,
		GetHash:               GetHashFn(header, chain),
		GetVRF:                GetVRFFn(header, chain),
		IsValidator:           IsValidator,
		Origin:                origin,
		GasPrice:              new(big.Int).Set(gasPrice),
		Coinbase:              beneficiary,
		GasLimit:              header.GasLimit(),
		BlockNumber:           header.Number(),
		EpochNumber:           header.Epoch(),
		Time:                  header.Time(),
		VRF:                   vrf,
		TxType:                0,
		CrossCallIndex:        callIndex,
		SimulationNum:         simNum,
		TxHash:                txHash,
		CreateValidator:       CreateValidatorFn(header, chain),
		EditValidator:         EditValidatorFn(header, chain),
		Delegate:              DelegateFn(header, chain),
		Undelegate:            UndelegateFn(header, chain),
		CollectRewards:        CollectRewardsFn(header, chain),
		CalculateMigrationGas: CalculateMigrationGasFn(chain),
		ShardID:               chain.ShardID(),
		NumShards:             shard.Schedule.InstanceForEpoch(header.Epoch()).NumShards(),
		Header:                header,
	}
}

// SSC_CanTransfer checks whether there are enough funds in the address' account to make a transfer.
// This does not take the necessary gas in to account to make the transfer valid.
func SSC_CanTransfer(db vm.StateDB, addr common.Address, amount *big.Int) bool {
	return true
}

// SSC_Transfer subtracts amount from sender and adds amount to recipient using the given Db
func SSC_Transfer(db vm.StateDB, sender, recipient common.Address, amount *big.Int, txType types.TransactionType) {
	if txType == types.SameShardTx {
		db.SubBalance(sender, amount)
	}
	db.AddBalance(recipient, amount)
}
