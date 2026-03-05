package ssc

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"math/big"
	"math/rand"

	"github.com/harmony-one/harmony/core"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/pkg/errors"
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

// SelectWeightedIndices 从 weights 中按权重无放回地选出 k 个不同下标
func SelectWeightedIndices(weights []float64, k int, seed int64) []int {
	n := len(weights)
	if k <= 0 {
		return []int{}
	}

	// 创建带种子的随机数生成器
	r := rand.New(rand.NewSource(seed))

	// 可用下标池（初始为 [0, 1, ..., n-1]）
	available := make([]int, n)
	for i := range available {
		available[i] = i
	}

	var result []int

	for i := 0; i < k; i++ {
		// 计算当前总权重
		var totalWeight float64
		for _, idx := range available {
			totalWeight += weights[idx]
		}

		// 随机选择一个点
		pick := r.Float64() * totalWeight

		// 累加权重直到命中
		var cumWeight float64
		selectedPos := -1 // 在 available 中的位置
		for j, idx := range available {
			cumWeight += weights[idx]
			if pick < cumWeight {
				selectedPos = j
				break
			}
		}

		if selectedPos == -1 {
			selectedPos = len(available) - 1 // 安全兜底
		}

		// 将选中的原始下标加入结果
		selectedIdx := available[selectedPos]
		result = append(result, selectedIdx)

		// 从 available 中移除已选下标（无放回）
		available = append(available[:selectedPos], available[selectedPos+1:]...)
	}

	return result
}

func FindNonce(ctx context.Context, data []byte, difficulty int) (nonce uint64, hashOut []byte, err error) {
	if difficulty > 63 {
		return 0, nil, errors.New("difficulty must be <= 63")
	}

	target := uint64(math.MaxUint64 >> difficulty)
	var buf [8]byte

	for nonce = 0; ; nonce++ {
		// 检查 context 是否已取消
		select {
		case <-ctx.Done():
			return 0, nil, errors.New("find nonce cancelled")
		default:
			// 继续挖矿
		}

		binary.BigEndian.PutUint64(buf[:], nonce)
		input := append(data, buf[:]...)
		hash := sha256.Sum256(input)
		hashUint64 := binary.BigEndian.Uint64(hash[:8])

		if hashUint64 <= target {
			return nonce, hash[:], nil
		}
	}
}

func VerifyPoW(data []byte, nonce uint64, difficulty int) bool {
	if difficulty > 63 {
		return false
	}
	target := uint64(math.MaxUint64 >> difficulty)

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, nonce)
	input := append(data, buf...)
	hash := sha256.Sum256(input)
	hashUint64 := binary.BigEndian.Uint64(hash[:8])
	return hashUint64 <= target
}
