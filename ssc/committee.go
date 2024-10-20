package ssc

import (
	"encoding/binary"
	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/ssc/api"
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

	lock sync.RWMutex
}

func NewCommitteeMechanism(selfShard uint32, currentEpoch api.Epoch) *CommitteeMechanism {
	return &CommitteeMechanism{
		SelfShard:    selfShard,
		CurrentEpoch: currentEpoch,
		Committees:   make(map[uint32]*api.ShardSimulateCommittee),
	}
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
