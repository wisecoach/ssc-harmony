package ssc

import (
	"encoding/binary"
	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/core"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"math"
	"math/big"
	"sync"
)

type CommitteeMechanism struct {
	SelfAddr     common.Address
	SelfShard    uint32
	shardNum     uint32
	CurrentEpoch api.Epoch
	Committees   map[uint32]*api.ShardSimulateCommittee
	Candidates   map[common.Address]*api.Candidate
	Validators   map[uint32]map[common.Address]*api.Validator

	isMember bool
	bc       core.BlockChain

	lock sync.RWMutex
}

func NewCommitteeMechanism(selfAddr common.Address, selfShard uint32, bc core.BlockChain) *CommitteeMechanism {
	cm := &CommitteeMechanism{
		SelfAddr:     selfAddr,
		SelfShard:    selfShard,
		CurrentEpoch: 0,
		Committees:   make(map[uint32]*api.ShardSimulateCommittee),
		Candidates:   make(map[common.Address]*api.Candidate),
		Validators:   make(map[uint32]map[common.Address]*api.Validator),
		bc:           bc,
		lock:         sync.RWMutex{},
	}
	cm.loadFromState()
	utils.SSCLogger().Info().Msgf("CommitteeMechanism initialized: SelfAddr: %s, SelfShard: %d, CurrentEpoch: %d, ShardNum: %d",
		cm.SelfAddr.Hex(), cm.SelfShard, cm.CurrentEpoch, cm.ShardNum())
	return cm
}

func (cm *CommitteeMechanism) GetShardID(address common.Address) uint32 {
	cm.lock.RLock()
	defer cm.lock.RUnlock()

	shardNum := cm.shardNum
	shardBits := int(math.Ceil(math.Log2(float64(shardNum))))
	shardId := BitsToUint32(address, shardBits)

	return shardId
}

func (cm *CommitteeMechanism) ShardNum() uint32 {
	cm.lock.RLock()
	defer cm.lock.RUnlock()

	if cm.shardNum == 0 {
		return uint32(len(cm.Committees))
	}
	return cm.shardNum
}

func (cm *CommitteeMechanism) loadFromState() {
	db, err := cm.bc.State()
	if err != nil {
		utils.SSCLogger().Err(err).Msg("load committees from state db failed")
		return
	}
	config := db.GetSSCConfig()
	if config == nil {
		utils.SSCLogger().Error().Msg("SSCConfig is nil")
		return
	}
	for _, committee := range config.Committees {
		cm.Committees[committee.ShardID] = committee
	}
	if cm.Committees[cm.SelfShard] == nil {
		utils.SSCLogger().Error().Msgf("self shard %d committee is nil", cm.SelfShard)
		return
	}

	for _, member := range cm.Committees[cm.SelfShard].Members {
		if member.Address == cm.SelfAddr {
			cm.isMember = true
			break
		}
	}

	cm.CurrentEpoch = cm.Committees[cm.SelfShard].Epoch
	cm.shardNum = uint32(len(cm.Committees))
}

func (cm *CommitteeMechanism) GetLeader(shardId uint32, txhash common.Hash) *api.Member {
	cm.lock.RLock()
	defer cm.lock.RUnlock()

	if committee, ok := cm.Committees[shardId]; ok {
		index := int(binary.BigEndian.Uint32(txhash[:4])) % committee.Number
		return committee.Members[index]
	}

	utils.SSCLogger().Error().Msgf("committee for shard %d not found", shardId)

	return nil
}

func (cm *CommitteeMechanism) IsMember() bool {
	cm.lock.RLock()
	defer cm.lock.RUnlock()

	return cm.isMember
}

func (cm *CommitteeMechanism) GetCommittee(shardID uint32) *api.ShardSimulateCommittee {
	cm.lock.RLock()
	defer cm.lock.RUnlock()

	return cm.Committees[shardID]
}

func (cm *CommitteeMechanism) UpdateCommittee(shardID uint32, committee *api.ShardSimulateCommittee) {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	cm.Committees[shardID] = committee
	cm.shardNum = uint32(len(cm.Committees))
	if cm.SelfShard == shardID {
		cm.CurrentEpoch = committee.Epoch
		for _, member := range committee.Members {
			if member.Address == cm.SelfAddr {
				cm.isMember = true
				break
			}
		}
	}
}

func (cm *CommitteeMechanism) GetValidators(shardId uint32) map[common.Address]*api.Validator {
	cm.lock.RLock()
	defer cm.lock.RUnlock()

	return cm.Validators[shardId]
}

func (cm *CommitteeMechanism) UpdateValidators(shardId uint32, validators map[common.Address]*api.Validator) {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	cm.Validators[shardId] = validators
	cm.shardNum = uint32(len(cm.Committees))
}

func (cm *CommitteeMechanism) Stake(address common.Address, stake *big.Int) {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	if candidate, ok := cm.Candidates[address]; ok {
		candidate.Stake.Add(candidate.Stake, stake)
	} else {
		cm.Candidates[address] = &api.Candidate{
			Address: address,
			Stake:   stake,
		}
	}
}
