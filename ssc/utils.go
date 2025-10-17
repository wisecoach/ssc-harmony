package ssc

import (
	"github.com/harmony-one/harmony/core"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/pkg/errors"
	"math/big"
)

func BitsToInt(data [20]byte, k int) int {
	if k <= 0 || k > 160 {
		return 0 // 或者返回错误
	}

	// 将整个字节数组转换为大整数
	bigInt := new(big.Int).SetBytes(data[:])

	// 右移保留前k位
	bigInt.Rsh(bigInt, uint(160-k))

	// 转换为int（注意可能丢失精度）
	return int(bigInt.Int64())
}

func BitsToUint32(data [20]byte, k int) uint32 {
	if k <= 0 || k > 160 {
		return 0 // 或者返回错误
	}

	// 将整个字节数组转换为大整数
	bigInt := new(big.Int).SetBytes(data[:])

	// 右移保留前k位
	bigInt.Rsh(bigInt, uint(160-k))

	// 转换为int（注意可能丢失精度）
	return uint32(bigInt.Int64())
}

func LoadSSCConfigFromBlockChain(bc core.BlockChain) (*api.ShardSimulateCommitteeConfig, error) {
	state, err := bc.State()
	if err != nil {
		return nil, err
	}
	config := state.GetSSCConfig()
	if config == nil {
		return nil, errors.New("")
	}
	return config, nil
}
