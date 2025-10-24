package ssc

import (
	"bytes"
	"encoding/binary"
	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/crypto/bls"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/harmony-one/harmony/ssc/lm"
	"math"
	"math/big"
)

type CommitteeMechanism struct {
	Config       *api.ShardSimulateCommitteeConfig
	SelfAddr     common.Address
	SelfShard    uint32
	shardNum     uint32
	CurrentEpoch api.Epoch
	cmLock       lm.RWMutex
	Committees   map[uint32]*api.ShardSimulateCommittee
	Candidates   map[common.Address]*api.Candidate
	Validators   map[uint32][]*api.Validator
	isMember     bool
	signerMgr    api.BLSSignerMgr
}

func NewCommitteeMechanism(selfAddr common.Address, selfShard uint32, config *api.ShardSimulateCommitteeConfig, signerMgr api.BLSSignerMgr) *CommitteeMechanism {
	cm := &CommitteeMechanism{
		Config:       config,
		SelfAddr:     selfAddr,
		SelfShard:    selfShard,
		shardNum:     0,
		CurrentEpoch: 0,
		cmLock:       lm.NewRWMutex(),
		Committees:   make(map[uint32]*api.ShardSimulateCommittee),
		Candidates:   make(map[common.Address]*api.Candidate),
		Validators:   make(map[uint32][]*api.Validator),
		isMember:     false,
		signerMgr:    signerMgr,
	}
	cm.loadFromConfig(config)
	utils.SSCLogger().Info().Msgf("CommitteeMechanism initialized: SelfAddr: %s, SelfShard: %d, CurrentEpoch: %d, ShardNum: %d",
		cm.SelfAddr.Hex(), cm.SelfShard, cm.CurrentEpoch, cm.ShardNum())
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

func (cm *CommitteeMechanism) loadFromConfig(config *api.ShardSimulateCommitteeConfig) {
	for _, committee := range config.Committees {
		cm.UpdateCommittee(committee.ShardID, committee)
		cm.Committees[committee.ShardID] = committee
	}
}

func (cm *CommitteeMechanism) GetLeader(shardId uint32, txhash common.Hash) *api.Member {
	cm.cmLock.RLock()
	defer cm.cmLock.RUnlock()

	if committee, ok := cm.Committees[shardId]; ok {
		index := int(binary.BigEndian.Uint32(txhash[:4])) % committee.Number
		return committee.Members[index]
	}

	utils.SSCLogger().Error().Msgf("committee for shard %d not found", shardId)

	return nil
}

func (cm *CommitteeMechanism) IsLeader(txhash common.Hash) bool {
	cm.cmLock.RLock()
	defer cm.cmLock.RUnlock()

	if committee, ok := cm.Committees[cm.SelfShard]; ok {
		index := int(binary.BigEndian.Uint32(txhash[:4])) % committee.Number
		isLeader := bytes.Compare(committee.Members[index].Address.Bytes(), cm.SelfAddr.Bytes()) == 0
		utils.SSCLogger().Info().Msgf("leader index: %d, self addr: %s, leader addr: %s, is leader: %v", index, cm.SelfAddr.Hex(), committee.Members[index].Address.Hex(), isLeader)
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

func (cm *CommitteeMechanism) GetCommittee(shardID uint32) *api.ShardSimulateCommittee {
	cm.cmLock.RLock()
	defer cm.cmLock.RUnlock()

	return cm.Committees[shardID]
}

func (cm *CommitteeMechanism) UpdateCommittee(shardID uint32, committee *api.ShardSimulateCommittee) {
	cm.cmLock.Lock()
	defer cm.cmLock.Unlock()

	utils.SSCLogger().Info().Interface("committee", committee).Msgf("updating committee for shard %d", shardID)

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

	addr2Index := make(map[common.Address]int)
	pubKeys := make([]bls.PublicKeyWrapper, 0, len(committee.Members))
	for i, member := range committee.Members {
		addr2Index[member.Address] = i
		pubKey, err := bls.WrapperPublicKeyFromString(member.BLSPubKey)
		if err != nil {
			utils.SSCLogger().Error().Err(err).Msg("failed to parse BLS public key")
			return
		}
		pubKeys = append(pubKeys, *pubKey)
	}
	cm.signerMgr.UpdateSSCPubKeys(shardID, addr2Index, pubKeys)
}

func (cm *CommitteeMechanism) GetValidators(shardId uint32) []*api.Validator {
	cm.cmLock.RLock()
	defer cm.cmLock.RUnlock()

	return cm.Validators[shardId]
}

func (cm *CommitteeMechanism) UpdateValidators(shardId uint32, validators []*api.Validator) {
	cm.cmLock.Lock()
	defer cm.cmLock.Unlock()

	cm.Validators[shardId] = validators
	cm.shardNum = uint32(len(cm.Committees))

	addr2Index := make(map[common.Address]int)
	pubKeys := make([]bls.PublicKeyWrapper, 0)
	for i, member := range validators {
		addr2Index[member.Address] = i
		pubKey, err := bls.WrapperPublicKeyFromString(member.PubKey)
		if err != nil {
			utils.SSCLogger().Error().Err(err).Msg("failed to parse BLS public key")
			return
		}
		pubKeys = append(pubKeys, *pubKey)
	}
	cm.signerMgr.UpdateValidatorPubKeys(shardId, addr2Index, pubKeys)
}

func (cm *CommitteeMechanism) Stake(address common.Address, stake *big.Int) {
	cm.cmLock.Lock()
	defer cm.cmLock.Unlock()

	if candidate, ok := cm.Candidates[address]; ok {
		candidate.Stake.Add(candidate.Stake, stake)
	} else {
		cm.Candidates[address] = &api.Candidate{
			Address: address,
			Stake:   stake,
		}
	}
}
