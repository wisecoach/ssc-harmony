package ssc

import (
	"crypto/rand"
	"encoding/csv"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/ssc/api"
)

// Simulator 信誉系统模拟器
type Simulator struct {
	config       *api.ReputationConfig
	validators   []*api.Member
	shards       map[uint32]*api.ShardSimulateCommittee
	state        *ReputationState
	parser       *StateDataParser
	currentEpoch api.Epoch
	blockNum     uint64
	outputDir    string
	csvFiles     map[string]*csv.Writer // key: "shardID_validatorIndex" -> CSV writer
	csvHandles   map[string]*os.File    // key: "shardID_validatorIndex" -> CSV file handle
}

// NewSimulator 创建模拟器
func NewSimulator(config *api.ReputationConfig, validatorCount int, shardCount uint32, membersPerShard int, outputDir string) *Simulator {
	// 生成 validator 列表
	validators := make([]*api.Member, validatorCount)
	for i := 0; i < validatorCount; i++ {
		addr := common.HexToAddress(fmt.Sprintf("0x%040d", i+1))
		blsKey := fmt.Sprintf("bls_key_%d", i)
		validators[i] = &api.Member{
			Address:   addr,
			PubKey:    fmt.Sprintf("pubkey_%d", i),
			Endpoint:  fmt.Sprintf("http://node%d.example.com", i+1),
			BLSPubKey: blsKey,
		}
	}

	// 创建分片委员会
	shards := make(map[uint32]*api.ShardSimulateCommittee)
	for shardID := uint32(0); shardID < shardCount; shardID++ {
		// 为每个分片选择 members
		members := make([]*api.Member, membersPerShard)
		for i := 0; i < membersPerShard; i++ {
			idx := (int(shardID)*membersPerShard + i) % validatorCount
			members[i] = validators[idx]
		}

		shards[shardID] = &api.ShardSimulateCommittee{
			ShardID:            shardID,
			Epoch:              0,
			Members:            members,
			Number:             membersPerShard,
			Threshold:          membersPerShard * 2 / 3,
			Validators:         validators,
			ValidatorThreshold: validatorCount * 2 / 3,
			MemberIndex:        make(map[common.Address]int),
			ValidatorIndex:     make(map[common.Address]int),
		}

		// 建立索引
		for i, v := range validators {
			shards[shardID].ValidatorIndex[v.Address] = i
		}
		for _, m := range members {
			shards[shardID].MemberIndex[m.Address] = shards[shardID].ValidatorIndex[m.Address]
		}
	}

	// 初始化 state
	state := &ReputationState{
		Accounts:     make(map[common.Address]*AccountReputation),
		GasUsed:      0,
		Reward:       big.NewInt(0),
		SelfOpinions: make(map[common.Address]*api.SLOpinion),
		TxNum:        0,
		TxNumSum:     0,
	}

	// 为每个 validator 初始化账户
	for _, v := range validators {
		state.Accounts[v.Address] = &AccountReputation{
			Address:      v.Address,
			SLOpinions:   make(map[common.Address]*api.SLOpinion),
			StakedReward: make(map[api.Epoch]*big.Int),
			Rewards:      make([]*big.Int, 0),
			Mu:           0,
			Rev:          big.NewFloat(0),
			A:            0.5,
			B:            0,
			S:            0,
			P:            0,
			W:            0,
			STxNum:       0,
			STxNumSum:    0,
		}
		// 初始化 opinions
		for _, v2 := range validators {
			state.Accounts[v.Address].SLOpinions[v2.Address] = &api.SLOpinion{
				From: v.Address,
				To:   v2.Address,
				S:    0,
				F:    0,
			}
		}
	}

	// 创建输出目录
	if outputDir != "" {
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			fmt.Printf("Warning: failed to create output dir: %v\n", err)
			outputDir = ""
		}
	}

	// 为每个 validator 初始化 CSV 文件（每个 shard 中的每个 validator）
	csvFiles := make(map[string]*csv.Writer)
	csvHandles := make(map[string]*os.File)

	if outputDir != "" {
		for shardID := uint32(0); shardID < shardCount; shardID++ {
			// 为每个 validator 创建 CSV 文件
			for validatorIndex, _ := range validators {
				key := fmt.Sprintf("%d_%d", shardID, validatorIndex)
				filename := filepath.Join(outputDir, fmt.Sprintf("validator_%d_%d.csv", shardID, validatorIndex))
				file, err := os.Create(filename)
				if err != nil {
					fmt.Printf("Warning: failed to create CSV file for %s: %v\n", key, err)
					continue
				}

				writer := csv.NewWriter(file)
				// 写入表头（与 output_reputation 一致）
				headers := []string{"epoch", "timestamp", "addr", "A", "B", "S", "P", "W", "Rev", "Mu", "STxNum", "STxNumSum", "TotalReward"}
				if err := writer.Write(headers); err != nil {
					fmt.Printf("Warning: failed to write CSV headers for %s: %v\n", key, err)
				}
				csvFiles[key] = writer
				csvHandles[key] = file
			}
		}
		fmt.Printf("Created %d CSV files in %s\n", len(csvFiles), outputDir)
	}

	return &Simulator{
		config:       config,
		validators:   validators,
		shards:       shards,
		state:        state,
		parser:       NewStateDataParser(),
		currentEpoch: 0,
		blockNum:     0,
		outputDir:    outputDir,
		csvFiles:     csvFiles,
		csvHandles:   csvHandles,
	}
}

// generateRandomBlockDelta 生成随机的 BlockDelta
// 规则：每个交易从 committee 的 member 中随机选出 3 个参与签名
func (s *Simulator) generateRandomBlockDelta(shardID uint32) *BlockStateDelta {
	committee := s.shards[shardID]
	memberCount := len(committee.Members)
	signCount := 3 // 每个交易选 3 个 member 签名

	// 随机生成 crossGasUsed (10000 - 100000)
	crossGasUsed := uint64(10000 + randInt(90000))

	// 随机生成交易数量 (5 - 20)
	txCount := 50 + randInt(150)

	// 为每个交易随机选出 signCount 个 member 参与并签名
	memberTxCounts := make(map[int]int)
	memberSignedCounts := make(map[int]int)
	for t := 0; t < txCount; t++ {
		// 从 member 中随机选出 signCount 个（不放回）
		indices := make([]int, memberCount)
		for i := 0; i < memberCount; i++ {
			indices[i] = i
			memberTxCounts[i]++
		}
		// Fisher-Yates 洗牌，只取前 signCount 个
		for i := memberCount - 1; i > memberCount-signCount-1; i-- {
			j := randInt(i + 1)
			indices[i], indices[j] = indices[j], indices[i]
		}
		for k := 0; k < signCount; k++ {
			idx := indices[memberCount-signCount+k]
			memberSignedCounts[idx]++
		}
	}

	return &BlockStateDelta{
		RewardIncrement:    new(big.Int).Mul(new(big.Int).SetUint64(crossGasUsed), s.config.RewardPrice),
		TxCount:            txCount,
		MemberTxCounts:     memberTxCounts,
		MemberSignedCounts: memberSignedCounts,
	}
}

// generateOpinionDelta 生成随机的 OpinionDelta
func (s *Simulator) generateOpinionDelta(fromValidator int) *OpinionDelta {
	fromAddr := s.validators[fromValidator].Address

	opinions := make(map[common.Address]*api.SLOpinion)
	for i, to := range s.validators {
		if i == fromValidator {
			continue
		}
		// 随机生成 S/F (成功/失败次数)
		s_count := 10 + randInt(10)
		f_count := randInt(3) // 失败概率较低

		opinions[to.Address] = &api.SLOpinion{
			From: fromAddr,
			To:   to.Address,
			S:    s_count,
			F:    f_count,
		}
	}

	return &OpinionDelta{
		From:     fromAddr,
		Opinions: opinions,
	}
}

// applyBlockDelta 应用 BlockDelta 到 state
func (s *Simulator) applyBlockDelta(shardID uint32, delta *BlockStateDelta) {
	state := s.state
	committee := s.shards[shardID]

	// 更新奖励
	state.Reward = new(big.Int).Add(state.Reward, delta.RewardIncrement)

	// 更新交易计数
	state.TxNum += delta.TxCount
	state.TxNumSum += delta.TxCount

	// 更新成员参与计数
	for memberIndex, count := range delta.MemberTxCounts {
		if memberIndex >= len(committee.Members) {
			continue
		}
		memberAddr := committee.Members[memberIndex].Address
		if account, exists := state.Accounts[memberAddr]; exists {
			account.TxNum += count
			account.TxNumSum += count
		}
	}
	for memberIndex, count := range delta.MemberSignedCounts {
		if memberIndex >= len(committee.Members) {
			continue
		}
		memberAddr := committee.Members[memberIndex].Address
		if account, exists := state.Accounts[memberAddr]; exists {
			account.STxNum += count
			account.STxNumSum += count
		}
	}
}

// applyOpinionDelta 应用 OpinionDelta 到 state
func (s *Simulator) applyOpinionDelta(delta *OpinionDelta) {
	ar := s.state.Accounts[delta.From]
	if ar == nil {
		return
	}

	for toAddr, opinion := range delta.Opinions {
		existing := ar.SLOpinions[toAddr]
		if existing != nil {
			existing.S = opinion.S
			existing.F = opinion.F
			// 重新计算 Beta, Delta, Omega
			total := float64(opinion.S+opinion.F) + s.config.C
			existing.Beta = float64(opinion.S) / total
			existing.Delta = float64(opinion.F) / total
			existing.Omega = s.config.C / total
		}
	}
}

// startNewEpoch 开始新 epoch（简化版，只计算信誉和激励）
func (s *Simulator) startNewEpoch(shardID uint32) {
	state := s.state
	config := s.config
	committee := s.shards[shardID]
	validators := committee.Validators
	accounts := state.Accounts
	oldEpoch := s.currentEpoch
	newEpoch := oldEpoch + 1

	fmt.Printf("\n========== EPOCH %d -> %d ==========\n", oldEpoch, newEpoch)

	// 更新 S_u
	for _, u := range validators {
		s_u := float64(0)
		for _, v := range validators {
			opinion := accounts[v.Address].SLOpinions[u.Address]
			s_u += (1-config.A)*opinion.Beta + config.A*opinion.Omega
		}
		accounts[u.Address].S = s_u / float64(len(validators))
	}

	sumW := float64(1)
	// 更新 P_u, w_u
	for _, u := range validators {
		ur := accounts[u.Address]
		ur.P = float64(ur.STxNumSum) / float64(ur.TxNumSum+1)
		ur.W = config.RB + config.RS*float64(ur.STxNum+1)/float64(state.TxNum+1)
		sumW += ur.W
	}

	// 更新 R_u，添加 staked reward
	for _, u := range committee.Members {
		ur := accounts[u.Address]
		if sumW <= 0 {
			sumW = 1
		}
		reward := new(big.Float).Mul(new(big.Float).SetInt(state.Reward), new(big.Float).SetFloat64(ur.W/sumW))
		ur.StakedReward[oldEpoch], _ = reward.Int(new(big.Int))

		redeemableEpoch := oldEpoch - api.Epoch(config.T)
		if ur.StakedReward[redeemableEpoch] != nil {
			ur.Rewards = append(ur.Rewards, ur.StakedReward[redeemableEpoch])
			delete(ur.StakedReward, redeemableEpoch)
		}
	}

	revList := make([]*big.Float, 0, len(validators))
	// 更新 A_u（归一化收益）
	for _, u := range validators {
		ur := accounts[u.Address]
		base := new(big.Float).Mul(new(big.Float).SetInt(state.Reward), new(big.Float).SetFloat64(ur.S))
		sumReward := new(big.Int).SetInt64(1)
		for _, reward := range ur.StakedReward {
			sumReward = new(big.Int).Add(sumReward, reward)
		}

		var avgReward *big.Float
		if len(ur.StakedReward) == 0 {
			avgReward = new(big.Float).SetInt64(0)
		} else {
			avgReward = new(big.Float).Quo(new(big.Float).SetInt(sumReward), new(big.Float).SetInt64(int64(len(ur.StakedReward))))
		}

		cost := new(big.Float).Mul(new(big.Float).SetFloat64(config.Theta), new(big.Float).SetUint64(state.GasUsed))
		risk := new(big.Float).Mul(new(big.Float).SetInt(sumReward), new(big.Float).SetFloat64(ur.Mu))
		rev := new(big.Float)

		if base.Cmp(avgReward) > 0 {
			rev = rev.Add(rev, base)
		} else {
			rev = rev.Add(rev, avgReward)
		}
		rev = rev.Sub(rev, cost)
		rev = rev.Sub(rev, risk)
		ur.Rev = rev
		revList = append(revList, rev)
	}

	// 归一化 A 到 [0,1]
	sortFloats(revList)
	minRev := revList[0]
	maxRev := revList[len(revList)-1]
	distance := new(big.Float).Sub(maxRev, minRev)

	for _, u := range validators {
		ur := accounts[u.Address]
		if distance.Cmp(big.NewFloat(0)) == 0 {
			ur.A = 0.5
		} else {
			normalized := new(big.Float).Quo(new(big.Float).Sub(ur.Rev, minRev), distance)
			ur.A, _ = normalized.Float64()
			if ur.A < 0 {
				ur.A = 0
			} else if ur.A > 1 {
				ur.A = 1
			}
		}
	}

	// 更新 B_u
	weightedList := make([]float64, 0, len(validators))
	for _, u := range validators {
		ur := accounts[u.Address]
		ur.B = ur.A*config.W1 + config.W2*ur.S + config.W3*ur.P
		weightedList = append(weightedList, ur.B)
	}

	// 打印 epoch 结果
	fmt.Printf("\n--- Epoch %d Results ---\n", newEpoch)
	fmt.Printf("Total Reward: %s\n", state.Reward.String())
	fmt.Printf("Total TxNum: %d\n", state.TxNumSum)
	fmt.Println("\nValidator Metrics:")
	fmt.Println("Addr\t\t\tA\tB\t\tS\t\tP\t\tW\t\tRev\t\tSTxNum\tReward")
	for _, u := range validators {
		ur := accounts[u.Address]
		totalReward := new(big.Int).SetInt64(0)
		for _, r := range ur.Rewards {
			totalReward = new(big.Int).Add(totalReward, r)
		}
		fmt.Printf("%s\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.2f\t%d\t%s\n",
			u.Address.Hex()[0:10], ur.A, ur.B, ur.S, ur.P, ur.W, ur.Rev, ur.STxNumSum, totalReward.String())
	}

	// 选择新 committee（从 validators 中按权重选出 committee.Number 个）
	selectedIndexes := SelectWeightedIndices(weightedList, committee.Number, int64(oldEpoch))
	fmt.Printf("\nNew Committee Members (validator indices): %v\n", selectedIndexes)

	// 更新 committee 的 Members 和 MemberIndex
	newMembers := make([]*api.Member, len(selectedIndexes))
	for i, idx := range selectedIndexes {
		newMembers[i] = validators[idx]
	}
	committee.Members = newMembers
	newMemberIndex := make(map[common.Address]int)
	for _, m := range newMembers {
		newMemberIndex[m.Address] = committee.ValidatorIndex[m.Address]
	}
	committee.MemberIndex = newMemberIndex

	fmt.Printf("New Committee Addresses:\n")
	for _, m := range newMembers {
		fmt.Printf("  %s (validator index=%d, B=%.4f)\n", m.Address.Hex()[0:10], committee.ValidatorIndex[m.Address], accounts[m.Address].B)
	}

	// 清除 state
	state.GasUsed = 0
	state.TxNum = 0
	for _, u := range validators {
		ur := accounts[u.Address]
		ur.STxNum = 0
		ur.TxNum = 0
	}

	s.currentEpoch = newEpoch

	// 导出到 CSV
	s.exportToCSV(shardID, newEpoch, validators, accounts)
}

// Run 运行模拟
func (s *Simulator) Run(totalBlocks uint64, shardID uint32) {
	config := s.config
	blocksPerEpoch := config.BlockPerEpoch
	blocksPerSLTest := blocksPerEpoch / 10 // SLTestPerEpoch = 10

	fmt.Printf("Starting Simulation:\n")
	fmt.Printf("  Total Blocks: %d\n", totalBlocks)
	fmt.Printf("  Blocks per Epoch: %d\n", blocksPerEpoch)
	fmt.Printf("  Blocks per SL Test: %d\n", blocksPerSLTest)
	fmt.Printf("  Shard ID: %d\n", shardID)
	fmt.Printf("  Validators: %d\n", len(s.validators))
	fmt.Printf("  Committee Size: %d\n\n", len(s.shards[shardID].Members))

	opinionRound := 0

	for block := uint64(1); block <= totalBlocks; block++ {
		s.blockNum = block

		// 1. 生成并应用 BlockDelta
		delta := s.generateRandomBlockDelta(shardID)
		s.applyBlockDelta(shardID, delta)

		// 2. 每 blocksPerSLTest 个区块生成 OpinionDelta
		// 所有 validator 互相评估：每个 validator 对其他所有 validator 进行 SL 测试
		if block%blocksPerSLTest == 0 {
			opinionRound++
			for fromIdx := 0; fromIdx < len(s.validators); fromIdx++ {
				opinionDelta := s.generateOpinionDelta(fromIdx)
				s.applyOpinionDelta(opinionDelta)
			}

			if block%blocksPerEpoch == 0 {
				fmt.Printf("\n[Block %d] SL Test Round %d completed (%d validators evaluated each other)\n", block, opinionRound, len(s.validators))
			}
		}

		// 3. 每 blocksPerEpoch 个区块开始新 epoch
		if block%blocksPerEpoch == 0 {
			s.startNewEpoch(shardID)
		}

		// 进度报告
		if block%10 == 0 || block == totalBlocks {
			fmt.Printf("Progress: Block %d/%d, Epoch %d\n", block, totalBlocks, s.currentEpoch)
		}
	}

	// 最终报告
	s.printFinalReport()
}

// exportToCSV 导出 epoch 数据到 CSV
func (s *Simulator) exportToCSV(shardID uint32, epoch api.Epoch, validators []*api.Member, accounts map[common.Address]*AccountReputation) {
	if s.outputDir == "" {
		return
	}

	timestamp := time.Now().Format(time.RFC3339)

	// 为每个 validator 写入对应的 CSV 文件
	for validatorIndex, u := range validators {
		ur := accounts[u.Address]
		if ur == nil {
			continue
		}

		key := fmt.Sprintf("%d_%d", shardID, validatorIndex)
		writer, exists := s.csvFiles[key]
		if !exists || writer == nil {
			continue
		}

		// 计算总奖励
		totalReward := new(big.Int).SetInt64(0)
		for _, r := range ur.Rewards {
			totalReward = new(big.Int).Add(totalReward, r)
		}
		for _, r := range ur.StakedReward {
			totalReward = new(big.Int).Add(totalReward, r)
		}

		// 写入 CSV 行（与 output_reputation 一致）
		row := []string{
			fmt.Sprintf("%d", epoch),
			timestamp,
			u.Address.Hex(),
			fmt.Sprintf("%.6f", ur.A),
			fmt.Sprintf("%.6f", ur.B),
			fmt.Sprintf("%.6f", ur.S),
			fmt.Sprintf("%.6f", ur.P),
			fmt.Sprintf("%.6f", ur.W),
			fmt.Sprintf("%.6f", ur.Rev),
			fmt.Sprintf("%.6f", ur.Mu),
			fmt.Sprintf("%d", ur.STxNumSum),
			fmt.Sprintf("%d", ur.STxNumSum),
			totalReward.String(),
		}

		if err := writer.Write(row); err != nil {
			fmt.Printf("Warning: failed to write CSV row for %s: %v\n", key, err)
		}
		writer.Flush()
	}
}

// Close 关闭 CSV 文件
func (s *Simulator) Close() {
	for key, writer := range s.csvFiles {
		if writer != nil {
			writer.Flush()
		}
		if file, exists := s.csvHandles[key]; exists && file != nil {
			file.Close()
		}
	}
	fmt.Printf("Closed %d CSV files\n", len(s.csvFiles))
}

// printFinalReport 打印最终报告
func (s *Simulator) printFinalReport() {
	fmt.Println("\n========== FINAL REPORT ==========")
	fmt.Printf("Total Epochs: %d\n", s.currentEpoch)
	fmt.Printf("Total Blocks: %d\n", s.blockNum)
	fmt.Printf("Total Rewards Distributed: %s\n", s.state.Reward.String())
	fmt.Printf("Total Transactions: %d\n\n", s.state.TxNumSum)

	fmt.Println("Final Validator Rankings (by B score):")

	// 计算最终 B 分数并排序
	type validatorScore struct {
		addr  common.Address
		score float64
	}

	scores := make([]validatorScore, len(s.validators))
	for i, v := range s.validators {
		scores[i] = validatorScore{
			addr:  v.Address,
			score: s.state.Accounts[v.Address].B,
		}
	}

	// 按 B 分数降序排序
	for i := 0; i < len(scores)-1; i++ {
		for j := i + 1; j < len(scores); j++ {
			if scores[j].score > scores[i].score {
				scores[i], scores[j] = scores[j], scores[i]
			}
		}
	}

	fmt.Println("Rank\tAddr\t\t\tB Score\tTotal Reward")
	for i, vs := range scores {
		ur := s.state.Accounts[vs.addr]
		totalReward := new(big.Int).SetInt64(0)
		for _, r := range ur.Rewards {
			totalReward = new(big.Int).Add(totalReward, r)
		}
		fmt.Printf("%d\t%s\t%.4f\t%s\n", i+1, vs.addr.Hex()[0:18], vs.score, totalReward.String())
	}
}

// 辅助函数
func randInt(max int) int {
	buf := make([]byte, 4)
	rand.Read(buf)
	return int(new(big.Int).SetBytes(buf).Uint64()) % max
}

func sortFloats(floats []*big.Float) {
	for i := 0; i < len(floats)-1; i++ {
		for j := i + 1; j < len(floats); j++ {
			if floats[i].Cmp(floats[j]) > 0 {
				floats[i], floats[j] = floats[j], floats[i]
			}
		}
	}
}

// 测试函数
func TestReputationSimulation(t *testing.T) {
	// 配置参数
	config := &api.ReputationConfig{
		SLFileSize:    1024,
		SLDifficulty:  10,
		SLTimeout:     time.Second * 30,
		SLPeriod:      time.Second * 10,
		A:             0.1, // Omega 基础比率
		C:             1.0, // 不确定性常数
		W1:            0.4, // A 的权重
		W2:            0.4, // S 的权重
		W3:            0.2, // P 的权重
		RewardPrice:   big.NewInt(1000),
		RB:            0.1,  // 基础奖励系数
		RS:            0.9,  // 模拟奖励系数
		BlockPerEpoch: 20,   // 每 100 个区块一个 epoch
		Theta:         0.01, // 成本系数
		T:             2,    // 2 个 epoch 后可赎回
	}

	// 设置输出目录
	outputDir := "/mnt/E/gowork/src/github.com/wisecoach/ssc-cli/data_process/output/ssc_reputation_example_reputation"

	// 创建模拟器：2 个分片，10 个 validator，每个分片 4 个 member
	sim := NewSimulator(config, 10, 2, 4, outputDir)

	// 确保测试结束时关闭 CSV 文件
	defer sim.Close()

	// 运行模拟：500 个区块（5 个 epoch）
	sim.Run(1000, 0)

	fmt.Printf("\nCSV files exported to: %s\n", outputDir)
}

// BenchmarkReputationCalculation 性能测试
func BenchmarkReputationCalculation(b *testing.B) {
	config := &api.ReputationConfig{
		SLFileSize:    1024,
		SLDifficulty:  10,
		SLTimeout:     time.Second * 30,
		SLPeriod:      time.Second * 10,
		A:             0.1,
		C:             1.0,
		W1:            0.4,
		W2:            0.4,
		W3:            0.2,
		RewardPrice:   big.NewInt(1000),
		RB:            0.1,
		RS:            0.9,
		BlockPerEpoch: 100,
		Theta:         0.01,
		T:             2,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sim := NewSimulator(config, 10, 2, 4, "")
		sim.Run(10000, 0)
		sim.Close()
	}
}

// TestParameterSensitivity 参数敏感性测试
func TestParameterSensitivity(t *testing.T) {
	baseConfig := &api.ReputationConfig{
		SLFileSize:    1024,
		SLDifficulty:  10,
		SLTimeout:     time.Second * 30,
		SLPeriod:      time.Second * 10,
		A:             0.1,
		C:             1.0,
		W1:            0.4,
		W2:            0.4,
		W3:            0.2,
		RewardPrice:   big.NewInt(1000),
		RB:            0.1,
		RS:            0.9,
		BlockPerEpoch: 100,
		Theta:         0.01,
		T:             2,
	}

	testCases := []struct {
		name   string
		modify func(*api.ReputationConfig)
	}{
		{"High_W1", func(c *api.ReputationConfig) { c.W1 = 0.7; c.W2 = 0.2; c.W3 = 0.1 }},
		{"High_W2", func(c *api.ReputationConfig) { c.W1 = 0.2; c.W2 = 0.7; c.W3 = 0.1 }},
		{"High_RS", func(c *api.ReputationConfig) { c.RB = 0.3; c.RS = 0.7 }},
		{"High_Theta", func(c *api.ReputationConfig) { c.Theta = 0.05 }},
		{"Low_C", func(c *api.ReputationConfig) { c.C = 0.1 }},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := *baseConfig
			tc.modify(&config)

			sim := NewSimulator(&config, 10, 2, 4, "")
			sim.Run(300, 0)
			sim.Close()
		})
	}
}
