package ssc

import (
	"encoding/csv"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/harmony-one/harmony/ssc/api"
)

// paramConfig defines a set of parameters to test
type paramConfig struct {
	name            string
	A               float64 // Omega base rate
	C               float64 // uncertainty constant
	W1, W2, W3      float64 // weights for A, S, P
	RB, RS          float64 // reward coefficients
	BlockPerEpoch   uint64
	Theta           float64 // cost coefficient
	T               uint64  // redemption delay epochs
	ValidatorCount  int
	MembersPerShard int
	TotalBlocks     uint64
}

// runSingleConfig runs simulation and returns summary metrics
func runSingleConfig(t *testing.T, cfg paramConfig, outputDir string) map[string]float64 {
	t.Logf("=== Running config: %s ===", cfg.name)

	rewardPrice := big.NewInt(1000)
	config := &api.ReputationConfig{
		SLFileSize:    1024,
		SLDifficulty:  10,
		SLTimeout:     time.Second * 30,
		SLPeriod:      time.Second * 10,
		A:             cfg.A,
		C:             cfg.C,
		W1:            cfg.W1,
		W2:            cfg.W2,
		W3:            cfg.W3,
		RewardPrice:   rewardPrice,
		RB:            cfg.RB,
		RS:            cfg.RS,
		BlockPerEpoch: cfg.BlockPerEpoch,
		Theta:         cfg.Theta,
		T:             cfg.T,
	}

	dir := filepath.Join(outputDir, cfg.name)
	os.MkdirAll(dir, 0755)

	sim := NewSimulator(config, cfg.ValidatorCount, 1, cfg.MembersPerShard, dir)
	defer sim.Close()

	sim.Run(cfg.TotalBlocks, 0)

	// Collect final metrics
	metrics := make(map[string]float64)
	metrics["total_epochs"] = float64(sim.currentEpoch)
	metrics["total_blocks"] = float64(sim.blockNum)
	metrics["total_reward"], _ = sim.state.Reward.Float64()
	metrics["total_tx"] = float64(sim.state.TxNumSum)

	// Per-validator metrics
	var bScores, aScores, sScores, pScores, rewards []float64
	for _, v := range sim.validators {
		ur := sim.state.Accounts[v.Address]
		if ur == nil {
			continue
		}
		bScores = append(bScores, ur.B)
		aScores = append(aScores, ur.A)
		sScores = append(sScores, ur.S)
		pScores = append(pScores, ur.P)
		totalReward := new(big.Int).SetInt64(0)
		for _, r := range ur.Rewards {
			totalReward = new(big.Int).Add(totalReward, r)
		}
		for _, r := range ur.StakedReward {
			totalReward = new(big.Int).Add(totalReward, r)
		}
		r, _ := totalReward.Float64()
		rewards = append(rewards, r)
	}

	metrics["avg_B"] = mean(bScores)
	metrics["std_B"] = stdDev(bScores)
	metrics["min_B"] = minVal(bScores)
	metrics["max_B"] = maxVal(bScores)
	metrics["avg_A"] = mean(aScores)
	metrics["std_A"] = stdDev(aScores)
	metrics["avg_S"] = mean(sScores)
	metrics["avg_P"] = mean(pScores)
	metrics["avg_reward"] = mean(rewards)
	metrics["std_reward"] = stdDev(rewards)
	metrics["reward_range"] = maxVal(rewards) - minVal(rewards)
	metrics["cv_B"] = metrics["std_B"] / (metrics["avg_B"] + 1e-10) // coefficient of variation

	return metrics
}

func mean(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

func stdDev(vals []float64) float64 {
	if len(vals) < 2 {
		return 0
	}
	m := mean(vals)
	sum := 0.0
	for _, v := range vals {
		diff := v - m
		sum += diff * diff
	}
	return sqrt(sum / float64(len(vals)))
}

func minVal(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	m := vals[0]
	for _, v := range vals[1:] {
		if v < m {
			m = v
		}
	}
	return m
}

func maxVal(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	m := vals[0]
	for _, v := range vals[1:] {
		if v > m {
			m = v
		}
	}
	return m
}

func sqrt(x float64) float64 {
	if x <= 0 {
		return 0
	}
	z := x
	for i := 0; i < 10; i++ {
		z = (z + x/z) / 2
	}
	return z
}

// TestParamSweep 多组参数对比测试
func TestParamSweep(t *testing.T) {
	outputDir := "/mnt/E/gowork/src/github.com/wisecoach/ssc-cli/data_process/output/ssc_reputation_sweep"
	os.MkdirAll(outputDir, 0755)

	configs := []paramConfig{
		// Config 1: 当前默认配置
		{
			name: "cfg01_default",
			A:    0.1, C: 1.0,
			W1: 0.4, W2: 0.4, W3: 0.2,
			RB: 0.1, RS: 0.9,
			BlockPerEpoch: 20,
			Theta:         0.01, T: 2,
			ValidatorCount: 10, MembersPerShard: 4, TotalBlocks: 1000,
		},
		// Config 2: 更大 epoch（更少 epoch 切换）
		{
			name: "cfg02_large_epoch",
			A:    0.1, C: 1.0,
			W1: 0.4, W2: 0.4, W3: 0.2,
			RB: 0.1, RS: 0.9,
			BlockPerEpoch: 100,
			Theta:         0.01, T: 2,
			ValidatorCount: 10, MembersPerShard: 4, TotalBlocks: 1000,
		},
		// Config 3: 高 A（更重视不确定性 Omega）
		{
			name: "cfg03_high_A",
			A:    0.5, C: 1.0,
			W1: 0.4, W2: 0.4, W3: 0.2,
			RB: 0.1, RS: 0.9,
			BlockPerEpoch: 20,
			Theta:         0.01, T: 2,
			ValidatorCount: 10, MembersPerShard: 4, TotalBlocks: 1000,
		},
		// Config 4: 高 C（更高不确定性基线）
		{
			name: "cfg04_high_C",
			A:    0.1, C: 5.0,
			W1: 0.4, W2: 0.4, W3: 0.2,
			RB: 0.1, RS: 0.9,
			BlockPerEpoch: 20,
			Theta:         0.01, T: 2,
			ValidatorCount: 10, MembersPerShard: 4, TotalBlocks: 1000,
		},
		// Config 5: 更重视信誉 S（W2 高）
		{
			name: "cfg05_high_S_weight",
			A:    0.1, C: 1.0,
			W1: 0.2, W2: 0.6, W3: 0.2,
			RB: 0.1, RS: 0.9,
			BlockPerEpoch: 20,
			Theta:         0.01, T: 2,
			ValidatorCount: 10, MembersPerShard: 4, TotalBlocks: 1000,
		},
		// Config 6: 更重视收益 A（W1 高）
		{
			name: "cfg06_high_A_weight",
			A:    0.1, C: 1.0,
			W1: 0.6, W2: 0.2, W3: 0.2,
			RB: 0.1, RS: 0.9,
			BlockPerEpoch: 20,
			Theta:         0.01, T: 2,
			ValidatorCount: 10, MembersPerShard: 4, TotalBlocks: 1000,
		},
		// Config 7: 更重视性能 P（W3 高）
		{
			name: "cfg07_high_P_weight",
			A:    0.1, C: 1.0,
			W1: 0.2, W2: 0.2, W3: 0.6,
			RB: 0.1, RS: 0.9,
			BlockPerEpoch: 20,
			Theta:         0.01, T: 2,
			ValidatorCount: 10, MembersPerShard: 4, TotalBlocks: 1000,
		},
		// Config 8: 高基础奖励 RB（更平均的奖励分配）
		{
			name: "cfg08_high_RB",
			A:    0.1, C: 1.0,
			W1: 0.4, W2: 0.4, W3: 0.2,
			RB: 0.5, RS: 0.5,
			BlockPerEpoch: 20,
			Theta:         0.01, T: 2,
			ValidatorCount: 10, MembersPerShard: 4, TotalBlocks: 1000,
		},
		// Config 9: 高成本 Theta
		{
			name: "cfg09_high_theta",
			A:    0.1, C: 1.0,
			W1: 0.4, W2: 0.4, W3: 0.2,
			RB: 0.1, RS: 0.9,
			BlockPerEpoch: 20,
			Theta:         0.05, T: 2,
			ValidatorCount: 10, MembersPerShard: 4, TotalBlocks: 1000,
		},
		// Config 10: 更长赎回延迟 T
		{
			name: "cfg10_long_redemption",
			A:    0.1, C: 1.0,
			W1: 0.4, W2: 0.4, W3: 0.2,
			RB: 0.1, RS: 0.9,
			BlockPerEpoch: 20,
			Theta:         0.01, T: 5,
			ValidatorCount: 10, MembersPerShard: 4, TotalBlocks: 1000,
		},
		// Config 11: 更多 validator + 更大 committee
		{
			name: "cfg11_large_scale",
			A:    0.1, C: 1.0,
			W1: 0.4, W2: 0.4, W3: 0.2,
			RB: 0.1, RS: 0.9,
			BlockPerEpoch: 50,
			Theta:         0.01, T: 2,
			ValidatorCount: 20, MembersPerShard: 8, TotalBlocks: 2000,
		},
		// Config 12: 均衡权重 + 适中 epoch
		{
			name: "cfg12_balanced",
			A:    0.2, C: 2.0,
			W1: 0.33, W2: 0.34, W3: 0.33,
			RB: 0.2, RS: 0.8,
			BlockPerEpoch: 50,
			Theta:         0.02, T: 3,
			ValidatorCount: 10, MembersPerShard: 5, TotalBlocks: 1000,
		},
	}

	// Run all configs and collect metrics
	allMetrics := make([]map[string]float64, len(configs))
	for i, cfg := range configs {
		allMetrics[i] = runSingleConfig(t, cfg, outputDir)
	}

	// Write comparison CSV
	csvPath := filepath.Join(outputDir, "comparison.csv")
	csvFile, err := os.Create(csvPath)
	if err != nil {
		t.Fatalf("Failed to create comparison CSV: %v", err)
	}
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	// Header
	headers := []string{
		"config", "epochs", "total_reward", "avg_B", "std_B", "cv_B",
		"min_B", "max_B", "B_range", "avg_A", "std_A",
		"avg_S", "avg_P", "avg_reward", "std_reward", "reward_range",
	}
	writer.Write(headers)

	// Data rows
	for i, cfg := range configs {
		m := allMetrics[i]
		row := []string{
			cfg.name,
			fmt.Sprintf("%.0f", m["total_epochs"]),
			fmt.Sprintf("%.0f", m["total_reward"]),
			fmt.Sprintf("%.4f", m["avg_B"]),
			fmt.Sprintf("%.4f", m["std_B"]),
			fmt.Sprintf("%.4f", m["cv_B"]),
			fmt.Sprintf("%.4f", m["min_B"]),
			fmt.Sprintf("%.4f", m["max_B"]),
			fmt.Sprintf("%.4f", m["max_B"]-m["min_B"]),
			fmt.Sprintf("%.4f", m["avg_A"]),
			fmt.Sprintf("%.4f", m["std_A"]),
			fmt.Sprintf("%.4f", m["avg_S"]),
			fmt.Sprintf("%.4f", m["avg_P"]),
			fmt.Sprintf("%.0f", m["avg_reward"]),
			fmt.Sprintf("%.0f", m["std_reward"]),
			fmt.Sprintf("%.0f", m["reward_range"]),
		}
		writer.Write(row)
	}

	// Print summary table
	fmt.Println("\n\n========== PARAMETER SWEEP SUMMARY ==========")
	fmt.Printf("%-20s %6s %10s %8s %8s %8s %8s %8s %10s\n",
		"Config", "Epochs", "TotalReward", "Avg_B", "Std_B", "CV_B", "Avg_A", "Std_A", "B_Range")
	fmt.Println("---------------------------------------------------------------------------------------")
	for i, cfg := range configs {
		m := allMetrics[i]
		fmt.Printf("%-20s %6.0f %10.0f %8.4f %8.4f %8.4f %8.4f %8.4f %10.4f\n",
			cfg.name, m["total_epochs"], m["total_reward"],
			m["avg_B"], m["std_B"], m["cv_B"],
			m["avg_A"], m["std_A"], m["max_B"]-m["min_B"])
	}
	fmt.Println()

	// Recommendation logic: pick config with good differentiation (high CV_B) + reasonable spread
	bestIdx := 0
	bestScore := -1.0
	for i, m := range allMetrics {
		// Score: higher CV_B = better differentiation, higher B_range = more spread
		// Penalize extremely high std (instability) and extremely low range (no differentiation)
		score := m["cv_B"]*10 + (m["max_B"]-m["min_B"])*5 - m["std_B"]*2
		if score > bestScore {
			bestScore = score
			bestIdx = i
		}
	}

	fmt.Printf("🏆 Recommended: %s (score=%.4f)\n", configs[bestIdx].name, bestScore)
	fmt.Printf("   A=%.2f C=%.1f W1=%.2f W2=%.2f W3=%.2f RB=%.2f RS=%.2f BlockPerEpoch=%d Theta=%.3f T=%d\n",
		configs[bestIdx].A, configs[bestIdx].C,
		configs[bestIdx].W1, configs[bestIdx].W2, configs[bestIdx].W3,
		configs[bestIdx].RB, configs[bestIdx].RS,
		configs[bestIdx].BlockPerEpoch,
		configs[bestIdx].Theta, configs[bestIdx].T)
}
