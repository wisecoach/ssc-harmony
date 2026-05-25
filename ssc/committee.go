package ssc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/core/vm"
	"github.com/harmony-one/harmony/crypto/bls"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/harmony-one/harmony/ssc/lm"
	"github.com/pkg/errors"
)

type CommitteeMechanism struct {
	Config            *api.ShardSimulateCommitteeConfig
	SelfAddr          common.Address
	SelfShard         uint32
	shardNum          uint32
	lastEpochBlockNum uint64
	CurrentEpoch      api.Epoch
	cmLock            lm.RWMutex
	epochWaitLock     lm.RWMutex

	Committees         map[uint32]map[api.Epoch]*api.ShardSimulateCommittee
	LatestCommittees   map[uint32]*api.ShardSimulateCommittee
	currentShardEpochs []api.Epoch // snapshot of all shards' current epochs, indexed by shardID
	isMember           map[api.Epoch]bool
	state              *ReputationState
	epochReadyCh       map[uint32]map[api.Epoch]chan struct{}

	signerMgr   api.BLSSignerMgr
	txSubmitter api.TxSubmitter
	comm        *Comm
	ctx         context.Context
	parser      *StateDataParser // 独立的数据解析器
}

func NewCommitteeMechanism(ctx context.Context, selfAddr common.Address, selfShard uint32, config *api.ShardSimulateCommitteeConfig, signerMgr api.BLSSignerMgr, submitter api.TxSubmitter, comm *Comm) *CommitteeMechanism {
	cm := &CommitteeMechanism{
		Config:           config,
		SelfAddr:         selfAddr,
		SelfShard:        selfShard,
		shardNum:         0,
		CurrentEpoch:     0,
		cmLock:           lm.NewRWMutex(),
		Committees:       make(map[uint32]map[api.Epoch]*api.ShardSimulateCommittee),
		LatestCommittees: make(map[uint32]*api.ShardSimulateCommittee),
		isMember:         make(map[api.Epoch]bool),
		signerMgr:        signerMgr,
		txSubmitter:      submitter,
		comm:             comm,
		ctx:              ctx,
	}
	cm.state = &ReputationState{
		Accounts:     make(map[common.Address]*AccountReputation),
		GasUsed:      0,
		Reward:       big.NewInt(0),
		SelfOpinions: make(map[common.Address]*api.SLOpinion),
		TxNum:        0,
		TxNumSum:     0,
	}
	cm.parser = NewStateDataParser()
	cm.epochReadyCh = make(map[uint32]map[api.Epoch]chan struct{})
	utils.SSCLogger().Info().Msgf("COMMITTEE_INIT,SelfAddr=%s,SelfShard=%d,CurrentEpoch=%d,ShardNum=%d",
		cm.SelfAddr.Hex(), cm.SelfShard, cm.CurrentEpoch, cm.ShardNum())
	err := cm.loadFromConfig(config)
	if err != nil {
		panic(err)
		return nil
	}
	go cm.workForSLTest()
	return cm
}

func (cm *CommitteeMechanism) GetLatestCommittees() map[uint32]*api.ShardSimulateCommittee {
	cm.cmLock.RLock()
	defer cm.cmLock.RUnlock()

	return cm.LatestCommittees
}

func (cm *CommitteeMechanism) CopyCurrentEpochs() []api.Epoch {
	cm.cmLock.RLock()
	defer cm.cmLock.RUnlock()

	epochs := make([]api.Epoch, len(cm.currentShardEpochs))
	copy(epochs, cm.currentShardEpochs)
	return epochs
}

func (cm *CommitteeMechanism) GetShardID(address common.Address) uint32 {
	cm.cmLock.RLock()
	defer cm.cmLock.RUnlock()

	shardNum := cm.shardNum
	shardBits := int(math.Ceil(math.Log2(float64(shardNum))))
	shardId := BitsToUint32(address, shardBits)

	return shardId
}

func (cm *CommitteeMechanism) ShardNum() uint32 {
	cm.cmLock.RLock()
	defer cm.cmLock.RUnlock()

	if cm.shardNum == 0 {
		return uint32(len(cm.Committees))
	}
	return cm.shardNum
}

func (cm *CommitteeMechanism) loadFromConfig(config *api.ShardSimulateCommitteeConfig) error {
	cm.cmLock.Lock()
	defer cm.cmLock.Unlock()
	cm.currentShardEpochs = make([]api.Epoch, len(config.Committees))
	for _, committee := range config.Committees {
		err := cm.updateCommittee(committee.ShardID, committee)
		if err != nil {
			return err
		}
	}
	return nil
}

// Note: it will get cmLock, don't call it after get cmLock
func (cm *CommitteeMechanism) preCheckAndWait(epoch api.Epoch, shardId uint32) {
	if !cm.epochExists(epoch, shardId) {
		cm.waitForEpoch(epoch, shardId)
	}
}

func (cm *CommitteeMechanism) epochExists(epoch api.Epoch, shardId uint32) bool {
	cm.cmLock.RLock()
	defer cm.cmLock.RUnlock()

	if cm.Committees[shardId] == nil {
		return false
	}
	return cm.Committees[shardId][epoch] != nil
}

func (cm *CommitteeMechanism) waitForEpoch(epoch api.Epoch, shardId uint32) {
	startTime := time.Now()
	var waitCh chan struct{}

	cm.epochWaitLock.Lock()
	if cm.epochReadyCh[shardId] == nil {
		cm.epochReadyCh[shardId] = make(map[api.Epoch]chan struct{})
	}
	waitCh = cm.epochReadyCh[shardId][epoch]
	if waitCh == nil {
		waitCh = make(chan struct{})
		cm.epochReadyCh[shardId][epoch] = waitCh
	}
	cm.epochWaitLock.Unlock()

	if waitCh != nil {
		utils.SSCLogger().Info().Msgf("waiting for epoch %d to be ready, shardId=%d", epoch, shardId)
		select {
		case <-waitCh:
			utils.SSCLogger().Info().Msgf("epoch %d is ready, cost=%v", epoch, time.Since(startTime))
		case <-time.After(time.Hour * 10):
			utils.SSCLogger().Error().Msgf("timeout waiting for epoch %d, cost=%v", epoch, time.Since(startTime))
		}
	}
}

func (cm *CommitteeMechanism) getCommittee(epoch api.Epoch, shardId uint32) *api.ShardSimulateCommittee {
	committee := cm.Committees[shardId][epoch]
	if committee == nil {
		currentCommittee := cm.LatestCommittees[shardId]
		utils.SSCLogger().Error().Msgf("cannot find committee for (%d, %d), but current committee is (%d, %d)",
			shardId, epoch, currentCommittee.ShardID, currentCommittee.Epoch)
		return currentCommittee
	}
	return committee
}

func (cm *CommitteeMechanism) GetLeader(epoch api.Epoch, shardId uint32) *api.Member {
	cm.preCheckAndWait(epoch, shardId)

	cm.cmLock.RLock()
	defer cm.cmLock.RUnlock()

	if committee := cm.getCommittee(epoch, shardId); committee != nil {
		index := 0
		return committee.Members[index]
	}

	return nil
}

func (cm *CommitteeMechanism) IsLeader(epoch api.Epoch) bool {
	cm.preCheckAndWait(epoch, cm.SelfShard)

	cm.cmLock.RLock()
	defer cm.cmLock.RUnlock()

	if committee := cm.getCommittee(epoch, cm.SelfShard); committee != nil {
		index := 0
		isLeader := bytes.Compare(committee.Members[index].Address.Bytes(), cm.SelfAddr.Bytes()) == 0
		utils.SSCLogger().Debug().Msgf("LEADER_CHECK,Epoch=%d,ShardID=%d,LeaderAddr=%s,SelfAddr=%s,IsLeader=%v",
			cm.CurrentEpoch, cm.SelfShard, committee.Members[index].Address.Hex(), cm.SelfAddr.Hex(), isLeader)
		return isLeader
	}

	utils.SSCLogger().Error().Msgf("committee for shard %d not found", cm.SelfShard)

	return false
}

func (cm *CommitteeMechanism) IsMember(epoch api.Epoch) bool {
	cm.preCheckAndWait(epoch, cm.SelfShard)

	cm.cmLock.RLock()
	defer cm.cmLock.RUnlock()

	return cm.isMember[epoch]
}

func (cm *CommitteeMechanism) GetCommittee(epoch api.Epoch, shardID uint32) *api.ShardSimulateCommittee {
	cm.preCheckAndWait(epoch, shardID)

	cm.cmLock.RLock()
	defer cm.cmLock.RUnlock()

	return cm.getCommittee(epoch, shardID)
}

func (cm *CommitteeMechanism) updateCommittee(shardID uint32, committee *api.ShardSimulateCommittee) error {
	if _, exists := cm.Committees[shardID][committee.Epoch]; exists {
		utils.SSCLogger().Info().Msgf("COMMITTEE_EXISTS,ShardID=%d,Epoch=%d", shardID, committee.Epoch)
		return nil
	}

	if cm.Committees[shardID] == nil {
		cm.Committees[shardID] = make(map[api.Epoch]*api.ShardSimulateCommittee)
	}
	if committee.Validators == nil {
		committee.Validators = cm.LatestCommittees[shardID].Validators
	}
	cm.Committees[shardID][committee.Epoch] = committee
	cm.LatestCommittees[shardID] = committee
	cm.currentShardEpochs[int(shardID)] = committee.Epoch
	cm.shardNum = uint32(len(cm.Committees))

	// Update currentShardEpochs: ensure slice is large enough and update this shard's epoch
	cm.updateShardEpochs(shardID, committee.Epoch)

	if cm.SelfShard == shardID {
		cm.CurrentEpoch = committee.Epoch
		for _, member := range committee.Members {
			if bytes.Compare(member.Address.Bytes(), cm.SelfAddr.Bytes()) == 0 {
				cm.isMember[committee.Epoch] = true
				break
			}
		}
		for _, validator := range committee.Validators {
			if _, exists := cm.state.SelfOpinions[validator.Address]; !exists {
				cm.state.SelfOpinions[validator.Address] = &api.SLOpinion{
					From: cm.SelfAddr,
					To:   validator.Address,
				}
			}
		}
	}

	pubKeys := make([]bls.PublicKeyWrapper, 0, len(committee.Validators))
	validatorAddr2Index := make(map[common.Address]int)
	for i, validator := range committee.Validators {
		validatorAddr2Index[validator.Address] = i
		pubKey, err := bls.WrapperPublicKeyFromString(validator.BLSPubKey)
		if err != nil {
			utils.SSCLogger().Error().Msgf("BLS_KEY_PARSE_FAILED,Member=%s,Error=%v", validator.Address.Hex(), err)
			return err
		}
		pubKeys = append(pubKeys, *pubKey)
	}
	committee.ValidatorIndex = validatorAddr2Index

	validatorPubKeys := make([]bls.PublicKeyWrapper, 0, len(committee.Validators))
	for _, v := range committee.Validators {
		validatorPubKey, err := bls.WrapperPublicKeyFromString(v.BLSPubKey)
		if err != nil {
			utils.SSCLogger().Error().Msgf("BLS_KEY_PARSE_FAILED,Validator=%s,Error=%v", v.Address.Hex(), err)
			return err
		}
		validatorPubKeys = append(validatorPubKeys, *validatorPubKey)
	}
	cm.signerMgr.UpdateValidatorPubKeys(shardID, committee.Epoch, true, validatorAddr2Index, validatorPubKeys, committee.ValidatorThreshold)

	sscAddr2Index := make(map[common.Address]int)
	for _, member := range committee.Members {
		sscAddr2Index[member.Address] = validatorAddr2Index[member.Address]
	}
	committee.MemberIndex = sscAddr2Index
	cm.signerMgr.UpdateSSCPubKeys(shardID, committee.Epoch, true, sscAddr2Index, pubKeys, committee.Threshold)

	// notify waiters for this epoch
	cm.epochWaitLock.Lock()
	if cm.epochReadyCh[shardID] == nil {
		cm.epochReadyCh[shardID] = make(map[api.Epoch]chan struct{})
	}
	if readyCh, exists := cm.epochReadyCh[shardID][committee.Epoch]; exists {
		close(readyCh)
	} else {
		readyCh = make(chan struct{})
		cm.epochReadyCh[shardID][committee.Epoch] = readyCh
		close(readyCh)
	}
	cm.epochWaitLock.Unlock()

	utils.SSCLogger().Info().Interface("committee", committee).Interface("sscAddr2Index", sscAddr2Index).Msgf("COMMITTEE_UPDATE,ShardID=%d,Epoch=%d,MemberCount=%d,ValidatorCount=%d",
		shardID, committee.Epoch, len(committee.Members), len(committee.Validators))

	for _, u := range committee.Validators {
		if _, exists := cm.state.Accounts[u.Address]; !exists {
			cm.state.Accounts[u.Address] = &AccountReputation{
				Address:      u.Address,
				SLOpinions:   make(map[common.Address]*api.SLOpinion),
				StakedReward: make(map[api.Epoch]*big.Int),
				Rewards:      make([]*big.Int, 0),
				Mu:           0,
				Rev:          new(big.Float),
			}
			for _, v := range committee.Validators {
				cm.state.Accounts[u.Address].SLOpinions[v.Address] = &api.SLOpinion{
					From: u.Address,
					To:   v.Address,
				}
			}
		}
	}
	// Log committee member indices in validator list
	memberIndices := make([]string, len(committee.Members))
	for i, m := range committee.Members {
		memberIndices[i] = fmt.Sprintf("%d", validatorAddr2Index[m.Address])
	}
	utils.SSCLogger().Info().Msgf("COMMITTEE_UPDATED,ShardID=%d,Epoch=%d,MemberIndices=[%s]",
		shardID, committee.Epoch, strings.Join(memberIndices, "|"))
	return nil
}

// updateShardEpochs updates the currentShardEpochs slice with the given shard's epoch
// Must be called with cmLock held
func (cm *CommitteeMechanism) updateShardEpochs(shardID uint32, epoch api.Epoch) {
	// Ensure slice is large enough
	if int(shardID) >= len(cm.currentShardEpochs) {
		newEpochs := make([]api.Epoch, shardID+1)
		copy(newEpochs, cm.currentShardEpochs)
		cm.currentShardEpochs = newEpochs
	}
	cm.currentShardEpochs[shardID] = epoch
}

// GetShardEpochs returns a copy of the current shard epochs snapshot
// This is safe for concurrent use and prevents external modification
func (cm *CommitteeMechanism) GetShardEpochs() []api.Epoch {
	cm.cmLock.RLock()
	defer cm.cmLock.RUnlock()

	if cm.currentShardEpochs == nil {
		return nil
	}
	// Return a copy to prevent external modification
	shardEpochs := make([]api.Epoch, len(cm.currentShardEpochs))
	copy(shardEpochs, cm.currentShardEpochs)
	return shardEpochs
}

// GetCurrentEpoch returns the current epoch for the given shard
func (cm *CommitteeMechanism) GetCurrentEpoch(shardID uint32) api.Epoch {
	cm.cmLock.RLock()
	defer cm.cmLock.RUnlock()

	if int(shardID) >= len(cm.currentShardEpochs) {
		return 0
	}
	return cm.currentShardEpochs[shardID]
}

func (cm *CommitteeMechanism) GetValidators(epoch api.Epoch, shardId uint32) []*api.Member {
	cm.preCheckAndWait(epoch, shardId)

	cm.cmLock.RLock()
	defer cm.cmLock.RUnlock()

	if _, ok := cm.Committees[shardId][epoch]; ok {
		return cm.Committees[shardId][epoch].Validators
	}
	return make([]*api.Member, 0)
}

func (cm *CommitteeMechanism) CurrentValidatorAddrs() []common.Address {
	cm.cmLock.RLock()
	defer cm.cmLock.RUnlock()

	addrs := make([]common.Address, 0)
	validators := cm.LatestCommittees[cm.SelfShard].Validators
	for _, v := range validators {
		addrs = append(addrs, v.Address)
	}
	return addrs
}

// BlockStateDelta 表示从区块解析出的状态变更数据
// 解耦数据解析与状态写入
type BlockStateDelta struct {
	RewardIncrement    *big.Int    // 奖励增量
	TxCount            int         // 跨分片交易数量
	MemberTxCounts     map[int]int // 成员参与的交易计数 (memberIndex -> count)
	MemberSignedCounts map[int]int // 签名计数 (memberIndex -> count)
}

// HandleBlockCommitted
// 1. 从区块解析数据 (解耦到 parser)
// 2. 应用状态变更
// 3. 检查是否需要启动新 epoch
func (cm *CommitteeMechanism) HandleBlockCommitted(block *types.Block) error {
	committee := cm.getCommittee(cm.CurrentEpoch, cm.SelfShard)

	// Step 1: 解析区块数据，提取状态变更 (解耦：解析逻辑独立)
	delta := cm.parser.ParseBlock(block, committee, cm.Config.Reputation.RewardPrice)

	// Step 2: 应用状态变更 (解耦：写入逻辑独立)
	err := func() error {
		cm.cmLock.Lock()
		defer cm.cmLock.Unlock()
		return cm.applyBlockDelta(delta)
	}()

	if err != nil {
		utils.SSCLogger().Error().Msgf("BLOCK_COMMIT_FAILED,Error=%v", err)
		return err
	}

	// Step 3: 检查是否需要启动新 epoch
	if block.NumberU64() > 0 && block.NumberU64()%cm.Config.Reputation.BlockPerEpoch == 0 {
		err := cm.startNewEpoch(block.NumberU64())
		if err != nil {
			utils.SSCLogger().Error().Msgf("START_EPOCH_FAILED,BlockNum=%d,Error=%v", block.NumberU64(), err)
			return err
		}
	}

	return nil
}

// applyBlockDelta 应用区块状态变更到 state
// 必须持有 cmLock 调用
func (cm *CommitteeMechanism) applyBlockDelta(delta *BlockStateDelta) error {
	state := cm.state

	// 更新奖励
	state.Reward = new(big.Int).Add(state.Reward, delta.RewardIncrement)

	// 更新交易计数
	state.TxNum += delta.TxCount
	state.TxNumSum += delta.TxCount

	// 更新成员参与计数
	committee := cm.getCommittee(cm.CurrentEpoch, cm.SelfShard)
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

	return nil
}

// startNewEpoch
// 1. update epoch and blockNum
// 2. update $B,A,S,P$
// 3. clear state
func (cm *CommitteeMechanism) startNewEpoch(blockNum uint64) error {
	newCommittee, oldCommittee, err := func() (*api.ShardSimulateCommittee, *api.ShardSimulateCommittee, error) {
		cm.cmLock.RLock()
		defer cm.cmLock.RUnlock()

		state := cm.state
		config := cm.Config.Reputation
		accounts := state.Accounts
		oldCommittee := cm.getCommittee(cm.CurrentEpoch, cm.SelfShard)
		if oldCommittee == nil {
			return nil, nil, errors.New("oldCommittee not found")
		}
		validators := oldCommittee.Validators
		newEpoch := api.Epoch(blockNum / cm.Config.Reputation.BlockPerEpoch)

		// update S_u
		for _, u := range validators {
			s_u := float64(0)
			for _, v := range validators {
				opinion := accounts[v.Address].SLOpinions[u.Address]
				s_u += (1-config.A)*opinion.Beta + config.A*opinion.Omega
			}
			accounts[u.Address].S = s_u / float64(len(validators))
		}

		sumW := float64(1)
		// update P_u, w_u
		for _, u := range validators {
			ur := accounts[u.Address]
			ur.P = float64(ur.STxNumSum) / float64(ur.TxNumSum+1)
			ur.W = config.RB + config.RS*float64(ur.STxNum+1)/float64(state.TxNum+1)
			sumW += ur.W
		}

		// update R_u, add staked reward
		for _, u := range oldCommittee.Members {
			ur := accounts[u.Address]
			// Protect against division by zero or negative sumW
			if sumW <= 0 {
				sumW = 1
			}
			reward := new(big.Float).Mul(new(big.Float).SetInt(state.Reward), new(big.Float).SetFloat64(ur.W/sumW))
			ur.StakedReward[cm.CurrentEpoch], _ = reward.Int(new(big.Int))
			redeemableEpoch := cm.CurrentEpoch - api.Epoch(config.T)
			if ur.StakedReward[redeemableEpoch] != nil {
				ur.Rewards = append(ur.Rewards, ur.StakedReward[redeemableEpoch])
				delete(ur.StakedReward, redeemableEpoch)
			}
		}

		revList := make([]*big.Float, 0, len(validators))
		// update A_u
		for _, u := range validators {
			ur := accounts[u.Address]
			base := new(big.Float).Mul(new(big.Float).SetInt(state.Reward), new(big.Float).SetFloat64(ur.S))
			sumReward := new(big.Int).SetInt64(1)
			for _, reward := range ur.StakedReward {
				sumReward = new(big.Int).Add(sumReward, reward)
			}
			// Protect against division by zero when no staked rewards exist yet
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
		// Log reputation data for each validator in CSV format
		for i, u := range validators {
			ur := accounts[u.Address]
			totalReward := new(big.Int).SetInt64(0)
			for _, r := range ur.Rewards {
				totalReward = new(big.Int).Add(totalReward, r)
			}
			utils.SSCLogger().Info().Msgf("REPUTATION_EPOCH, BlockNum=%d,CurrentEpoch=%d,NewEpoch=%d,ShardID=%d,ValidatorIndex=%d,Addr=%s,A=%.6f,B=%.6f,S=%.6f,P=%.6f,W=%.6f,Rev=%.6f,Mu=%.6f,STxNum=%d,STxNumSum=%d,TotalReward=%s",
				blockNum, cm.CurrentEpoch, newEpoch, cm.SelfShard, i, u.Address.Hex(), ur.A, ur.B, ur.S, ur.P, ur.W, ur.Rev, ur.Mu, ur.STxNumSum, ur.STxNumSum, totalReward.String())
		}
		sort.Slice(revList, func(i, j int) bool { return revList[i].Cmp(revList[j]) < 0 })
		minRev := revList[0]
		maxRev := revList[len(revList)-1]
		distance := maxRev.Sub(maxRev, minRev)
		for _, u := range validators {
			ur := accounts[u.Address]
			if distance.Cmp(big.NewFloat(0)) == 0 {
				// All validators have same revenue, assign neutral value
				ur.A = 0.5
			} else {
				// Properly normalize revenue to [0, 1] range
				normalized := new(big.Float).Quo(new(big.Float).Sub(ur.Rev, minRev), distance)
				ur.A, _ = normalized.Float64()
				// Clamp to [0, 1] range to prevent Inf/NaN
				if ur.A < 0 {
					ur.A = 0
				} else if ur.A > 1 {
					ur.A = 1
				}
			}
		}

		weightedList := make([]float64, 0, len(validators))
		// update B_u
		for _, u := range validators {
			ur := accounts[u.Address]
			ur.B = ur.A*config.W1 + config.W2*ur.S + config.W3*ur.P
			weightedList = append(weightedList, ur.B)
		}

		// select new oldCommittee
		selectedIndexes := SelectWeightedIndices(weightedList, oldCommittee.Number, int64(cm.CurrentEpoch))
		// order by weight desc
		sort.Slice(selectedIndexes, func(i, j int) bool { return weightedList[i] > weightedList[j] })
		newCommittee := &api.ShardSimulateCommittee{
			ShardID:            cm.SelfShard,
			Epoch:              newEpoch,
			Members:            make([]*api.Member, len(selectedIndexes)),
			Number:             oldCommittee.Number,
			Threshold:          oldCommittee.Threshold,
			ValidatorThreshold: oldCommittee.ValidatorThreshold,
		}
		newMemberIndices := make([]string, len(newCommittee.Members))
		for i, index := range selectedIndexes {
			newCommittee.Members[i] = validators[index]
			newMemberIndices[i] = fmt.Sprintf("%d", index)
		}
		utils.SSCLogger().Info().Msgf("COMMITTEE_CHANGE,OldEpoch=%d,NewEpoch=%d,OldCount=%d,NewCount=%d,NewMemberIndices=[%s]",
			oldCommittee.Epoch, newCommittee.Epoch, len(oldCommittee.Members), len(newCommittee.Members), newMemberIndices)

		// clear state
		state.GasUsed = 0
		state.TxNum = 0
		for _, u := range validators {
			ur := accounts[u.Address]
			ur.STxNum = 0
			ur.TxNum = 0
		}
		return newCommittee, oldCommittee, nil
	}()

	if err != nil {
		utils.SSCLogger().Error().Msgf("COMMITTEE_UPDATE_FAILED,Epoch=%d,Error=%v", cm.CurrentEpoch, err)
		return err
	}

	// the first member submit new epoch tx
	if cm.SelfAddr == oldCommittee.Members[0].Address {
		utils.SSCLogger().Info().Msgf("NEW_EPOCH_SUBMIT,Epoch=%d,ShardID=%d,Submitter=%s",
			newCommittee.Epoch, newCommittee.ShardID, cm.SelfAddr.Hex())
		newEpoch := &api.NewEpoch{
			Committee: newCommittee,
		}
		err := cm.txSubmitter.SubmitNewEpoch(newEpoch)
		if err != nil {
			utils.SSCLogger().Error().Msgf("NEW_EPOCH_SUBMIT_FAILED,Epoch=%d,ShardID=%d,Error=%v",
				newCommittee.Epoch, newCommittee.ShardID, err)
			return err
		}
	}

	return nil
}

func (cm *CommitteeMechanism) HandleNewEpoch(newEpoch *api.NewEpoch, blockNum uint64) error {
	cm.cmLock.Lock()
	defer cm.cmLock.Unlock()

	utils.SSCLogger().Info().Msgf("NEW_EPOCH_HANDLED,Epoch=%d,ShardID=%d,TotalReward=%s,AccountCount=%d",
		newEpoch.Committee.Epoch, newEpoch.Committee.ShardID, cm.state.Reward.String(), len(cm.state.Accounts))
	err := cm.updateCommittee(newEpoch.Committee.ShardID, newEpoch.Committee)
	if err != nil {
		utils.SSCLogger().Error().Msgf("COMMITTEE_UPDATE_FAILED,Epoch=%d,ShardID=%d,Error=%v",
			newEpoch.Committee.Epoch, newEpoch.Committee.ShardID, err)
		return err
	}
	cm.lastEpochBlockNum = blockNum
	return nil
}

func (cm *CommitteeMechanism) SLTest(req *api.SLTestRequest) *api.SLTestResult {
	timeout := cm.Config.Reputation.SLTimeout
	ctx, cancel := context.WithTimeout(cm.ctx, timeout)
	defer cancel()
	nonce, _, err := FindNonce(ctx, req.TestFile, req.Difficulty)
	if err != nil {
		return &api.SLTestResult{
			Error: err.Error(),
		}
	}
	return &api.SLTestResult{
		Result: nonce,
	}
}

func (cm *CommitteeMechanism) workForSLTest() {
	utils.SSCLogger().Info().Msg("start work for sl test")
	config := cm.Config.Reputation
	opinions := cm.state.SelfOpinions
	testCnt := 0
	// BootStrap
	<-time.After(time.Second * 30)
	for {
		<-time.After(config.SLPeriod)

		validators := cm.getCommittee(cm.CurrentEpoch, cm.SelfShard).Validators
		lock := sync.Mutex{}
		wg := sync.WaitGroup{}
		wg.Add(len(validators))

		randFile := make([]byte, config.SLFileSize)
		req := &api.SLTestRequest{
			TestFile:   randFile,
			Difficulty: config.SLDifficulty,
		}
		results := make(map[int]*api.SLTestResult)

		utils.SSCLogger().Info().Msgf("SL_TEST_START,Epoch=%d,ValidatorCount=%d,TestCnt=%d",
			cm.CurrentEpoch, len(validators), testCnt)

		ctx, cancel := context.WithTimeout(cm.ctx, config.SLTimeout)
		for i, validator := range validators {
			endpoint := validator.Endpoint
			go func() {
				defer wg.Done()
				result := &api.SLTestResult{}
				err := cm.comm.CallToEndpoint(ctx, result, endpoint, api.Method_SLTest, req)
				if err != nil {
					utils.SSCLogger().Warn().Msgf("SL_TEST_CALL_FAILED,Validator=%s,Endpoint=%s,Error=%v",
						validator.Address.Hex(), endpoint, err)
					return
				}
				lock.Lock()
				defer lock.Unlock()
				results[i] = result
			}()
		}
		wg.Wait()
		cancel()

		cachedResult := uint64(0)

		cm.cmLock.Lock()
		for i, validator := range validators {
			result, exists := results[i]
			if !exists || result.Error != "" {
				opinions[validator.Address].F += 1
			} else {
				valid := false
				if cachedResult == 0 {
					valid = VerifyPoW(req.TestFile, result.Result, config.SLDifficulty)
					utils.SSCLogger().Debug().Uint64("result", result.Result).Msgf("verify pow for %s, valid=%v", validator.Address.Hex(), valid)
					if valid {
						cachedResult = result.Result
					}
				} else {
					valid = result.Result == cachedResult
				}
				if valid {
					opinions[validator.Address].S += 1
				} else {
					opinions[validator.Address].F += 1
				}
			}
		}

		selfOpinions := &api.SelfOpinions{
			From:     cm.SelfAddr,
			Opinions: make([]*api.SLOpinion, 0),
		}
		for _, opinion := range opinions {
			opinion.Beta = float64(opinion.S) / (float64(opinion.S+opinion.F) + config.C)
			opinion.Delta = float64(opinion.F) / (float64(opinion.S+opinion.F) + config.C)
			opinion.Omega = config.C / (float64(opinion.S+opinion.F) + config.C)
			selfOpinions.Opinions = append(selfOpinions.Opinions, opinion)
		}
		// Log SL test results and opinion metrics in CSV format
		for addr, opinion := range opinions {
			utils.SSCLogger().Info().Msgf("SL_TEST_RESULT,Epoch=%d,TestCnt=%d,Addr=%s,S=%d,F=%d,Beta=%.6f,Delta=%.6f,Omega=%.6f",
				cm.CurrentEpoch, testCnt, addr.Hex(), opinion.S, opinion.F, opinion.Beta, opinion.Delta, opinion.Omega)
		}
		cm.cmLock.Unlock()

		err := cm.txSubmitter.SubmitUploadOpinions(selfOpinions)
		if err != nil {
			utils.SSCLogger().Error().Msgf("OPINIONS_SUBMIT_FAILED,Epoch=%d,Error=%v", cm.CurrentEpoch, err)
		}

		testCnt++
	}
}

// OpinionDelta 表示从上传的 opinion 数据中解析出的状态变更
// 解耦数据解析与状态写入
type OpinionDelta struct {
	From     common.Address
	Opinions map[common.Address]*api.SLOpinion
}

func (cm *CommitteeMechanism) UploadSLOpinion(opinionsBytes []byte, stateDB api.StateDB) error {
	startTime := time.Now()
	defer func() {
		err := recover()
		if err != nil {
			utils.SSCLogger().Error().Dur("cost", time.Since(startTime)).Msgf("UploadSLOpinion,Epoch=%d,Error=%v", cm.CurrentEpoch, err)
		} else {
			utils.SSCLogger().Info().Dur("cost", time.Since(startTime)).Msgf("UploadSLOpinion,Epoch=%d,Success", cm.CurrentEpoch)
		}
	}()

	// Step 1: 解析 opinion 数据 (解耦：解析逻辑独立)
	delta, err := cm.parser.ParseOpinions(opinionsBytes)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msgf("ParseOpinionsBytes Failed")
		return err
	}

	utils.SSCLogger().Info().Msgf("OPINIONS_UPLOADED,From=%s,OpinionCount=%d",
		delta.From.Hex(), len(delta.Opinions))

	// Step 2: 应用状态变更 (解耦：写入逻辑独立)
	cm.cmLock.Lock()
	defer cm.cmLock.Unlock()

	ar := cm.state.Accounts[delta.From]
	if ar == nil {
		utils.SSCLogger().Warn().Msgf("ACCOUNT_NOT_FOUND,Addr=%s", delta.From.Hex())
		return nil
	}

	for toAddr, opinion := range delta.Opinions {
		ar.SLOpinions[toAddr] = opinion
	}

	return nil
}

type AccountReputation struct {
	Address      common.Address
	B            float64 // B of TPB
	A            float64 // A of TPB
	S            float64 // S of TPB
	P            float64 // P of TPB
	TxNum        int     // 该epoch参与的交易数量
	TxNumSum     int     // 总参与交易数量
	STxNum       int     // 该epoch参与并签名交易数量
	STxNumSum    int     // 总参与并签名交易数量
	SLOpinions   map[common.Address]*api.SLOpinion
	StakedReward map[api.Epoch]*big.Int
	Rewards      []*big.Int
	W            float64    // weight of Reward
	Mu           float64    // coefficient of malicious behavior
	Rev          *big.Float // revenue
}

type ReputationState struct {
	Accounts     map[common.Address]*AccountReputation
	TxNum        int
	TxNumSum     int
	SelfOpinions map[common.Address]*api.SLOpinion // self opinions
	Reward       *big.Int                          // sum reward of current epoch
	GasUsed      uint64
}

// ============================================================================
// StateDataParser - 独立的数据解析层
// 负责从区块、交易、opinion 数据等源解析出状态变更数据
// 与状态写入逻辑完全解耦
// ============================================================================

type StateDataParser struct{}

// NewStateDataParser 创建状态数据解析器
func NewStateDataParser() *StateDataParser {
	return &StateDataParser{}
}

// ParseBlock 从区块中解析状态变更数据
// 返回 BlockStateDelta，调用方负责将 delta 应用到 state
func (p *StateDataParser) ParseBlock(block *types.Block, committee *api.ShardSimulateCommittee, rewardPrice *big.Int) *BlockStateDelta {
	delta := &BlockStateDelta{
		RewardIncrement:    big.NewInt(0),
		TxCount:            0,
		MemberTxCounts:     make(map[int]int),
		MemberSignedCounts: make(map[int]int),
	}

	// 计算奖励增量
	crossGasUsed := block.Header().CrossGasUsed()
	delta.RewardIncrement = new(big.Int).Mul(new(big.Int).SetUint64(crossGasUsed), rewardPrice)

	transactions := block.Transactions()
	for _, tx := range transactions {
		if !tx.CrossShard() {
			continue
		}

		to := *tx.To()
		if bytes.Compare(to.Bytes(), vm.SimulationCommitAddr.Bytes()) == 0 {
			utils.SSCLogger().Info().Msgf("SIMULATION_COMMIT_TX,Epoch=%d,TxHash=%s", block.Epoch(), tx.Hash().Hex())
			// SimulationCommit 交易，解析参与成员
			simulation := &api.CXTSimulation{}
			if err := json.Unmarshal(tx.Data(), simulation); err != nil {
				utils.SSCLogger().Error().Err(err).Msg("failed to unmarshal simulation commit")
				continue // 解析失败不影响其他交易
			}

			bitmap := simulation.GetBLSBitMap()
			if bitmap == nil {
				utils.SSCLogger().Error().Msg("failed to parse simulation commit bitmap")
				continue
			}

			// 解析参与签名的成员
			for _, member := range committee.Members {
				i := committee.MemberIndex[member.Address]
				byt := i >> 3
				msk := byte(1) << uint(i&7)
				if bitmap[byt]&msk != 0 {
					delta.MemberSignedCounts[i]++
				}
				delta.MemberTxCounts[i]++
			}
			delta.TxCount++
		}
	}

	return delta
}

// ParseOpinions 从 opinion bytes 中解析状态变更数据
// 返回 OpinionDelta，调用方负责将 delta 应用到 state
func (p *StateDataParser) ParseOpinions(opinionsBytes []byte) (*OpinionDelta, error) {
	selfOpinions := &api.SelfOpinions{}
	if err := json.Unmarshal(opinionsBytes, selfOpinions); err != nil {
		utils.SSCLogger().Error().Msgf("OPINIONS_UNMARSHAL_FAILED,Error=%v", err)
		return nil, err
	}

	delta := &OpinionDelta{
		From:     selfOpinions.From,
		Opinions: make(map[common.Address]*api.SLOpinion),
	}

	for _, opinion := range selfOpinions.Opinions {
		delta.Opinions[opinion.To] = opinion
	}

	return delta, nil
}
