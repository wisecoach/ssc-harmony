package ssc

import (
	"encoding/binary"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/core"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/rs/zerolog"
	"math/big"
	"sync"
)

type CommitteeMechanism struct {
	SelfAddr     common.Address
	SelfShard    uint32
	CurrentEpoch api.Epoch
	Committees   map[uint32]*api.ShardSimulateCommittee
	Candidates   map[common.Address]*api.Candidate
	Validators   map[uint32]map[common.Address]*api.Validator

	bc core.BlockChain

	logger *zerolog.Logger
	lock   sync.RWMutex
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
		logger:       utils.Logger(),
		lock:         sync.RWMutex{},
	}
	cm.LoadFromState()
	return cm
}

func (cm *CommitteeMechanism) LoadFromState() {
	db, err := cm.bc.State()
	if err != nil {
		cm.logger.Err(err).Msg("load committees from state db failed")
		return
	}
	config := db.GetSSCConfig()
	if config == nil {
		cm.logger.Error().Msg("SSCConfig is nil")
		return
	}
	for _, committee := range config.Committees {
		// tempDelete cm.logger.Info().Msgf("load committee %d", committee.ShardID)
		cm.Committees[committee.ShardID] = committee
	}
	if cm.Committees[cm.SelfShard] == nil {
		fmt.Printf("self shard %d, Committees len = %d, committee is nil\n", cm.SelfShard, len(cm.Committees))
		// cm.logger.Error().Msgf("self shard %d committee is nil", cm.SelfShard)
		return
	}
	cm.CurrentEpoch = cm.Committees[cm.SelfShard].Epoch
}

func (cm *CommitteeMechanism) GetLeader(shardId uint32, txhash common.Hash) *api.Member {
	cm.lock.RLock()
	defer cm.lock.RUnlock()

	if committee, ok := cm.Committees[shardId]; ok {
		index := int(binary.BigEndian.Uint32(txhash[:4])) % committee.Number
		return committee.Members[index]
	}

	return nil
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
