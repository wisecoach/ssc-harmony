package shardingconfig

import (
	"fmt"
	"math/big"

	ethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/internal/params"
	"github.com/harmony-one/harmony/numeric"

	"github.com/harmony-one/harmony/internal/genesis"
)

func NewExprnetSchedule(shardNum int, shardSize int) Schedule {
	exprnet := MustNewInstance(false,
		uint32(shardNum), shardSize,
		shardSize, 0,
		numeric.MustNewDecFromStr("0.68"), genesis.ExprHarmonyAccounts,
		genesis.ExprHarmonyAccounts, emptyAllowlist, nil,
		numeric.ZeroDec(), ethCommon.Address{},
		exprnetReshardingEpoch, exprnetBlocksPerEpochV2,
	)
	return exprnetSchedule{instance: exprnet}
}

var feeCollectorsExprnet = FeeCollectors{
	// pk: 0x1111111111111111111111111111111111111111111111111111111111111111
	mustAddress("0x19E7E376E7C213B7E7e7e46cc70A5dD086DAff2A"): numeric.MustNewDecFromStr("0.5"),
	// pk: 0x2222222222222222222222222222222222222222222222222222222222222222
	mustAddress("0x1563915e194D8CfBA1943570603F7606A3115508"): numeric.MustNewDecFromStr("0.5"),
}

// pk: 0x3333333333333333333333333333333333333333333333333333333333333333
var hip30CollectionAddressExprnet = mustAddress("0x5CbDd86a2FA8Dc4bDdd8a8f69dBa48572EeC07FB")

type exprnetSchedule struct {
	instance Instance
}

const (
	exprnetV1Epoch = 1

	exprnetEpochBlock1      = 5
	exprnetBlocksPerEpoch   = 5000
	exprnetBlocksPerEpochV2 = 5000

	exprnetVdfDifficulty = 5000 // This takes about 10s to finish the vdf
)

func (ls exprnetSchedule) InstanceForEpoch(epoch *big.Int) Instance {
	return ls.instance
}

func (ls exprnetSchedule) BlocksPerEpochOld() uint64 {
	return exprnetBlocksPerEpoch
}

func (ls exprnetSchedule) BlocksPerEpoch() uint64 {
	return exprnetBlocksPerEpochV2
}

func (ls exprnetSchedule) twoSecondsFirstBlock() uint64 {
	if params.ExprnetChainConfig.TwoSecondsEpoch.Uint64() == 0 {
		return 0
	}
	return (params.ExprnetChainConfig.TwoSecondsEpoch.Uint64()-1)*ls.BlocksPerEpochOld() + exprnetEpochBlock1
}

func (ls exprnetSchedule) CalcEpochNumber(blockNum uint64) *big.Int {
	firstBlock2s := ls.twoSecondsFirstBlock()
	switch {
	case blockNum < exprnetEpochBlock1:
		return big.NewInt(0)
	case blockNum < firstBlock2s:
		return big.NewInt(int64((blockNum-exprnetEpochBlock1)/ls.BlocksPerEpochOld() + 1))
	default:
		extra := uint64(0)
		if firstBlock2s == 0 {
			blockNum -= exprnetEpochBlock1
			extra = 1
		}
		return big.NewInt(int64(extra + (blockNum-firstBlock2s)/ls.BlocksPerEpoch() + params.ExprnetChainConfig.TwoSecondsEpoch.Uint64()))
	}
}

func (ls exprnetSchedule) IsLastBlock(blockNum uint64) bool {
	switch {
	case blockNum < exprnetEpochBlock1-1:
		return false
	case blockNum == exprnetEpochBlock1-1:
		return true
	default:
		firstBlock2s := ls.twoSecondsFirstBlock()
		switch {
		case blockNum >= firstBlock2s:
			if firstBlock2s == 0 {
				blockNum -= exprnetEpochBlock1
			}
			return ((blockNum-firstBlock2s)%ls.BlocksPerEpoch() == ls.BlocksPerEpoch()-1)
		default: // genesis
			blocks := ls.BlocksPerEpochOld()
			return ((blockNum-exprnetEpochBlock1)%blocks == blocks-1)
		}
	}
}

func (ls exprnetSchedule) EpochLastBlock(epochNum uint64) uint64 {
	switch {
	case epochNum == 0:
		return exprnetEpochBlock1 - 1
	default:
		switch {
		case params.ExprnetChainConfig.IsTwoSeconds(big.NewInt(int64(epochNum))):
			blocks := ls.BlocksPerEpoch()
			firstBlock2s := ls.twoSecondsFirstBlock()
			block2s := (1 + epochNum - params.ExprnetChainConfig.TwoSecondsEpoch.Uint64()) * blocks
			if firstBlock2s == 0 {
				return block2s - blocks + exprnetEpochBlock1 - 1
			}
			return firstBlock2s + block2s - 1
		default: // genesis
			blocks := ls.BlocksPerEpochOld()
			return exprnetEpochBlock1 + blocks*epochNum - 1
		}
	}
}

func (ls exprnetSchedule) VdfDifficulty() int {
	return exprnetVdfDifficulty
}

func (ls exprnetSchedule) GetNetworkID() NetworkID {
	return ExprNet
}

// GetShardingStructure is the sharding structure for exprnet.
func (ls exprnetSchedule) GetShardingStructure(numShard, shardID int) []map[string]interface{} {
	res := []map[string]interface{}{}
	for i := 0; i < numShard; i++ {
		res = append(res, map[string]interface{}{
			"current": int(shardID) == i,
			"shardID": i,
			"http":    fmt.Sprintf("http://127.0.0.1:%d", 9500+2*i),
			"ws":      fmt.Sprintf("ws://127.0.0.1:%d", 9800+2*i),
		})
	}
	return res
}

// IsSkippedEpoch returns if an epoch was skipped on shard due to staking epoch
func (ls exprnetSchedule) IsSkippedEpoch(shardID uint32, epoch *big.Int) bool {
	return false
}

// RewardFrequency returns the frequency of block reward
func (ls exprnetSchedule) RewardFrequency() uint64 {
	return 16
}

var (
	exprnetReshardingEpoch = []*big.Int{
		big.NewInt(0), big.NewInt(exprnetV1Epoch), params.ExprnetChainConfig.StakingEpoch, params.ExprnetChainConfig.TwoSecondsEpoch,
	}
)
