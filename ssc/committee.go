package ssc

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"math/big"
	"sort"
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

	Committees       map[uint32]map[api.Epoch]*api.ShardSimulateCommittee
	LatestCommittees map[uint32]*api.ShardSimulateCommittee
	isMember         bool
	state            *ReputationState

	signerMgr   api.BLSSignerMgr
	txSubmitter api.TxSubmitter
	comm        *Comm
	ctx         context.Context
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
		isMember:         false,
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
	utils.SSCLogger().Info().Interface("config", config).Msgf("CommitteeMechanism initialized: SelfAddr: %s, SelfShard: %d, CurrentEpoch: %d, ShardNum: %d",
		cm.SelfAddr.Hex(), cm.SelfShard, cm.CurrentEpoch, cm.ShardNum())
	err := cm.loadFromConfig(config)
	if err != nil {
		panic(err)
		return nil
	}
	go cm.workForSLTest()
	return cm
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
	for _, committee := range config.Committees {
		err := cm.updateCommittee(committee.ShardID, committee)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cm *CommitteeMechanism) getCommittee(epoch api.Epoch, shardId uint32) *api.ShardSimulateCommittee {
	committee := cm.Committees[shardId][epoch]
	return committee
}

func (cm *CommitteeMechanism) GetLeader(epoch api.Epoch, shardId uint32) *api.Member {
	cm.cmLock.RLock()
	defer cm.cmLock.RUnlock()

	if committee := cm.getCommittee(epoch, shardId); committee != nil {
		index := 0
		return committee.Members[index]
	}

	return nil
}

func (cm *CommitteeMechanism) CurrentLeader(shardId uint32) *api.Member {
	cm.cmLock.RLock()
	defer cm.cmLock.RUnlock()

	if committee := cm.LatestCommittees[shardId]; committee != nil {
		index := 0
		return committee.Members[index]
	}

	return nil
}

func (cm *CommitteeMechanism) IsLeader() bool {
	cm.cmLock.RLock()
	defer cm.cmLock.RUnlock()

	if committee, ok := cm.Committees[cm.SelfShard][cm.CurrentEpoch]; ok {
		index := 0
		isLeader := bytes.Compare(committee.Members[index].Address.Bytes(), cm.SelfAddr.Bytes()) == 0
		utils.SSCLogger().Debug().Msgf("leader index: %d, self addr: %s, leader addr: %s, is leader: %v", index, cm.SelfAddr.Hex(), committee.Members[index].Address.Hex(), isLeader)
		return isLeader
	}

	utils.SSCLogger().Error().Msgf("committee for shard %d not found", cm.SelfShard)

	return false
}

func (cm *CommitteeMechanism) IsMember() bool {
	cm.cmLock.RLock()
	defer cm.cmLock.RUnlock()

	return cm.isMember
}

func (cm *CommitteeMechanism) GetCommittee(epoch api.Epoch, shardID uint32) *api.ShardSimulateCommittee {
	cm.cmLock.RLock()
	defer cm.cmLock.RUnlock()

	return cm.getCommittee(epoch, shardID)
}

func (cm *CommitteeMechanism) updateCommittee(shardID uint32, committee *api.ShardSimulateCommittee) error {
	utils.SSCLogger().Info().Interface("committee", committee).Msgf("updating committee for shard %d, epoch=%d", shardID, committee.Epoch)
	if _, exists := cm.Committees[shardID][committee.Epoch]; exists {
		utils.SSCLogger().Debug().Interface("committee", committee).Msgf("committee for shard %d already exists", shardID)
		return nil
	}

	validatorChanged := true
	if cm.Committees[shardID] == nil {
		cm.Committees[shardID] = make(map[api.Epoch]*api.ShardSimulateCommittee)
	}
	if committee.Validators == nil {
		committee.Validators = cm.LatestCommittees[shardID].Validators
		validatorChanged = false
	}
	cm.Committees[shardID][committee.Epoch] = committee
	cm.LatestCommittees[shardID] = committee
	cm.shardNum = uint32(len(cm.Committees))
	if cm.SelfShard == shardID {
		for _, member := range committee.Members {
			if member.Address == cm.SelfAddr {
				cm.isMember = true
				cm.CurrentEpoch = committee.Epoch
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

	addr2Index := make(map[common.Address]int)
	pubKeys := make([]bls.PublicKeyWrapper, 0, len(committee.Members))
	for i, member := range committee.Members {
		addr2Index[member.Address] = i
		pubKey, err := bls.WrapperPublicKeyFromString(member.BLSPubKey)
		if err != nil {
			utils.SSCLogger().Error().Err(err).Msg("failed to parse BLS public key")
			return err
		}
		pubKeys = append(pubKeys, *pubKey)
	}
	cm.signerMgr.UpdateSSCPubKeys(shardID, committee.Epoch, addr2Index, pubKeys)

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
	if validatorChanged {
		validatorPubKeys := make([]bls.PublicKeyWrapper, 0, len(committee.Validators))
		validatorAddr2Index := make(map[common.Address]int)
		for _, v := range committee.Validators {
			validatorAddr2Index[v.Address] = len(validatorPubKeys)
			validatorPubKey, err := bls.WrapperPublicKeyFromString(v.BLSPubKey)
			if err != nil {
				utils.SSCLogger().Error().Err(err).Msg("failed to parse BLS public key")
				return err
			}
			validatorPubKeys = append(validatorPubKeys, *validatorPubKey)
		}
		cm.signerMgr.UpdateValidatorPubKeys(shardID, committee.Epoch, validatorAddr2Index, validatorPubKeys)
	}
	utils.SSCLogger().Info().Interface("committee", committee).Msgf("updated committee for shard %d", shardID)
	return nil
}

func (cm *CommitteeMechanism) GetValidators(epoch api.Epoch, shardId uint32) []*api.Member {
	cm.cmLock.RLock()
	defer cm.cmLock.RUnlock()

	if _, ok := cm.Committees[shardId][epoch]; ok {
		return cm.Committees[shardId][epoch].Validators
	}
	return make([]*api.Member, 0)
}

// HandleBlockCommitted
// 1. update $R$, |Tx|, |STx_u|
// 2. check if need to start new epoch
//
//	@Description:
//	@param block
//	@return error
func (cm *CommitteeMechanism) HandleBlockCommitted(block *types.Block) error {
	err := func() error {
		cm.cmLock.Lock()
		defer cm.cmLock.Unlock()
		transactions := block.Transactions()
		crossGasUsed := block.Header().CrossGasUsed()
		reward := new(big.Int).Mul(new(big.Int).SetUint64(crossGasUsed), cm.Config.Reputation.RewardPrice)
		cm.state.Reward = new(big.Int).Add(cm.state.Reward, reward)
		committee := cm.getCommittee(cm.CurrentEpoch, cm.SelfShard)
		for _, tx := range transactions {
			if tx.CrossShard() {
				to := *tx.To()
				if !vm.IsSSCAddrApplyOnChain(to) {
					// update |Tx|
					cm.state.TxNum++
					cm.state.TxNumSum++
				} else {
					if to == vm.SimulationCommitAddr {
						simulation := &api.SimulationCommit{}
						err := json.Unmarshal(tx.Data(), simulation)
						if err != nil {
							utils.SSCLogger().Error().Err(err).Msg("failed to unmarshal simulation commit")
							return err
						}
						bitmap := simulation.GetBLSBitMap()
						for i, member := range committee.Members {
							byt := i >> 3
							msk := byte(1) << uint(i&7)
							if bitmap[byt]&msk != 0 {
								// update |STx_u|
								cm.state.Accounts[member.Address].STxNum += 1
								cm.state.Accounts[member.Address].STxNumSum += 1
							}
						}
					}
				}
			}
		}
		return nil
	}()

	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to handle block committed")
		return err
	}

	// check if need to start new epoch
	if block.NumberU64() == cm.lastEpochBlockNum+cm.Config.Reputation.BlockPerEpoch {
		err := cm.startNewEpoch(block)
		if err != nil {
			utils.SSCLogger().Error().Err(err).Msg("failed to start new epoch")
			return err
		}
	}

	return nil
}

// startNewEpoch
// 1. update epoch and blockNum
// 2. update $B,A,S,P$
// 3. clear state
func (cm *CommitteeMechanism) startNewEpoch(block *types.Block) error {
	newCommittee, oldCommittee, err := func() (*api.ShardSimulateCommittee, *api.ShardSimulateCommittee, error) {
		cm.cmLock.RLock()
		defer cm.cmLock.RUnlock()

		state := cm.state
		config := cm.Config.Reputation
		accounts := state.Accounts
		committee := cm.getCommittee(cm.CurrentEpoch, cm.SelfShard)
		if committee == nil {
			return nil, nil, errors.New("committee not found")
		}
		validators := committee.Validators

		// update S_u
		for _, u := range validators {
			s_u := float64(0)
			for _, v := range validators {
				opinion := accounts[v.Address].SLOpinions[u.Address]
				s_u += opinion.Beta + config.A*opinion.Omega
			}
			accounts[u.Address].S = s_u
		}

		sumW := float64(1)
		// update P_u, w_u
		for _, u := range validators {
			ur := accounts[u.Address]
			ur.P = float64(ur.STxNumSum+1) / float64(state.TxNumSum+1)
			ur.W = config.RB + config.RS*float64(ur.STxNum+1)/float64(state.TxNum+1)
			sumW += ur.W
		}

		// update R_u, add staked reward
		for _, u := range committee.Members {
			ur := accounts[u.Address]
			utils.SSCLogger().Info().Msgf("ur.W=%f, sum=%f", ur.W, sumW)
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
			avgReward := new(big.Float).Quo(new(big.Float).SetInt(sumReward), new(big.Float).SetInt64(int64(len(ur.StakedReward))))
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
		utils.SSCLogger().Info().Interface("revList", revList).Msgf("update R_u")
		sort.Slice(revList, func(i, j int) bool { return revList[i].Cmp(revList[j]) < 0 })
		minRev := revList[0]
		maxRev := revList[len(revList)-1]
		distance := maxRev.Sub(minRev, new(big.Float))
		for _, u := range validators {
			ur := accounts[u.Address]
			if distance.Cmp(new(big.Float)) == 0 {
				ur.A = 1
			} else {
				ur.A, _ = ur.Rev.Quo(ur.Rev.Sub(ur.Rev, minRev), distance).Float64()
			}
		}

		weightedList := make([]float64, 0, len(validators))
		// update B_u
		for _, u := range validators {
			ur := accounts[u.Address]
			ur.B = ur.A*config.W1 + config.W2*ur.S + config.W3*ur.P
			weightedList = append(weightedList, ur.B)
		}

		// select new committee
		selectedIndexes := SelectWeightedIndices(weightedList, int(committee.Number), int64(cm.CurrentEpoch))
		newCommittee := &api.ShardSimulateCommittee{
			ShardID:   cm.SelfShard,
			Epoch:     cm.CurrentEpoch + 1,
			Members:   make([]*api.Member, 0, len(selectedIndexes)),
			Number:    committee.Number,
			Threshold: committee.Threshold,
		}
		for _, index := range selectedIndexes {
			newCommittee.Members = append(newCommittee.Members, validators[index])
		}

		// clear state
		state.GasUsed = 0
		state.TxNum = 0
		for _, u := range validators {
			ur := accounts[u.Address]
			ur.STxNum = 0
		}
		return newCommittee, committee, nil
	}()

	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to update committee")
		return err
	}

	// the first member submit new epoch tx
	if cm.SelfAddr == oldCommittee.Members[0].Address {
		utils.SSCLogger().Info().Msgf("submit new epoch tx, epoch=%d, for shard=%d, selfAddr=%s, firstMemberAddr=%s", newCommittee.Epoch, newCommittee.ShardID, cm.SelfAddr.Hex(), newCommittee.Members[0].Address.Hex())
		newEpoch := &api.NewEpoch{
			Committee: newCommittee,
		}
		err := cm.txSubmitter.SubmitNewEpoch(newEpoch)
		if err != nil {
			utils.SSCLogger().Error().Err(err).Msg("submit new epoch tx failed")
			return err
		}
	}

	return nil
}

func (cm *CommitteeMechanism) HandleNewEpoch(newEpoch *api.NewEpoch, stateDB api.StateDB, blockNum uint64) error {
	cm.cmLock.Lock()
	defer cm.cmLock.Unlock()

	utils.SSCLogger().Info().Interface("state", cm.state.Accounts).Uint64("reward", cm.state.Reward.Uint64()).Msgf("handle new epoch, epoch=%d, for shard=%d", newEpoch.Committee.Epoch, newEpoch.Committee.ShardID)
	err := cm.updateCommittee(newEpoch.Committee.ShardID, newEpoch.Committee)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("update committee failed")
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

		utils.SSCLogger().Debug().Msgf("start sl test for %d validators, cnt=%d", len(validators), testCnt)

		ctx, cancel := context.WithTimeout(cm.ctx, config.SLTimeout)
		for i, validator := range validators {
			endpoint := validator.Endpoint
			go func() {
				defer wg.Done()
				result := &api.SLTestResult{}
				err := cm.comm.CallToEndpoint(ctx, result, endpoint, api.Method_SLTest, req)
				if err != nil {
					utils.SSCLogger().Warn().Err(err).Msg("call to endpoint failed")
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
		utils.SSCLogger().Debug().Interface("opinions", opinions).Msgf("end sl test for %d validators, cnt=%d", len(validators), testCnt)
		cm.cmLock.Unlock()

		err := cm.txSubmitter.SubmitUploadOpinions(selfOpinions)
		if err != nil {
			utils.SSCLogger().Error().Err(err).Msg("submit upload opinions failed")
		}

		testCnt++
	}
}

func (cm *CommitteeMechanism) UploadSLOpinion(opinionsBytes []byte, stateDB api.StateDB) error {
	selfOpinions := &api.SelfOpinions{}
	err := json.Unmarshal(opinionsBytes, selfOpinions)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msgf("unmarshal self opinions failed")
		return err
	}
	utils.SSCLogger().Info().Msgf("upload self opinions from %s", selfOpinions.From.Hex())
	ar := cm.state.Accounts[selfOpinions.From]
	for _, opinion := range selfOpinions.Opinions {
		ar.SLOpinions[opinion.To] = opinion
	}
	return nil
}

type AccountReputation struct {
	Address      common.Address
	B            float64 // B of TPB
	A            float64 // A of TPB
	S            float64 // S of TPB
	P            float64 // P of TPB
	STxNum       int
	STxNumSum    int
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
