package ssc

import (
	"bytes"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	blslib "github.com/harmony-one/bls/ffi/go/bls"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/crypto/bls"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/pkg/errors"
)

type blsSignerMgr struct {
	sscSigner       api.BLSSigner
	validatorSigner api.BLSSigner
}

func NewBLSSignerMgr(shardId uint32, selfAddr common.Address, key *bls.PrivateKeyWrapper) api.BLSSignerMgr {
	mgr := &blsSignerMgr{}
	mgr.sscSigner = &fakeBLSSigner{}
	mgr.validatorSigner = &fakeBLSSigner{}
	// mgr.sscSigner = newBLSSigner(shardId, selfAddr, key)
	// mgr.validatorSigner = newBLSSigner(shardId, selfAddr, key)
	return mgr
}

func (b *blsSignerMgr) GetSSCSigner() api.BLSSigner {
	return b.sscSigner
}

func (b *blsSignerMgr) GetValidatorSigner() api.BLSSigner {
	return b.validatorSigner
}

func (b *blsSignerMgr) UpdateSSCPubKeys(shardID uint32, epoch api.Epoch, changed bool, addr2Index map[common.Address]int, pubKeys []bls.PublicKeyWrapper, threshold int) {
	b.sscSigner.UpdatePubKeys(shardID, epoch, changed, addr2Index, pubKeys, threshold)
}

func (b *blsSignerMgr) UpdateValidatorPubKeys(shardID uint32, epoch api.Epoch, changed bool, addr2Index map[common.Address]int, pubKeys []bls.PublicKeyWrapper, threshold int) {
	b.validatorSigner.UpdatePubKeys(shardID, epoch, changed, addr2Index, pubKeys, threshold)
}

func newBLSSigner(shardId uint32, selfAddr common.Address, key *bls.PrivateKeyWrapper) api.BLSSigner {
	// return &fakeBLSSigner{} for testing
	return &blsSigner{
		mu:               sync.RWMutex{},
		currentEpoch:     0,
		addr2Index:       make(map[uint32]map[api.Epoch]map[common.Address]int),
		shard2PublicKeys: make(map[uint32]map[api.Epoch][]bls.PublicKeyWrapper),
		shard2Threshold:  make(map[uint32]map[api.Epoch]int),
		epochReadyCh:     make(map[uint32]map[api.Epoch]chan struct{}),
		privateKey:       key,
		selfShardId:      shardId,
		selfAddr:         selfAddr,
	}
}

type fakeBLSSigner struct {
}

func (f *fakeBLSSigner) Address() common.Address {
	return common.Address{}
}

func (f *fakeBLSSigner) Sign(msg api.MessageToSign) ([]byte, error) {
	return nil, nil
}

func (f *fakeBLSSigner) Aggregate(msgs []api.SSCMessage) (signatures []byte, bitmap []byte, err error) {
	return
}

func (f *fakeBLSSigner) Verify(msg api.BLSSignedMessage) error {
	return nil
}

func (f *fakeBLSSigner) UpdatePubKeys(shardID uint32, epoch api.Epoch, changed bool, addr2Index map[common.Address]int, pubKeys []bls.PublicKeyWrapper, threshold int) {
}

type blsSigner struct {
	mu               sync.RWMutex
	currentEpoch     api.Epoch
	addr2Index       map[uint32]map[api.Epoch]map[common.Address]int
	shard2PublicKeys map[uint32]map[api.Epoch][]bls.PublicKeyWrapper
	shard2Threshold  map[uint32]map[api.Epoch]int
	epochReadyCh     map[uint32]map[api.Epoch]chan struct{} // closed when epoch is ready
	privateKey       *bls.PrivateKeyWrapper
	selfShardId      uint32
	selfAddr         common.Address
}

func (s *blsSigner) Address() common.Address {
	return s.selfAddr
}

func (s *blsSigner) Sign(msg api.MessageToSign) ([]byte, error) {
	startTime := time.Now()
	defer utils.SSCLogger().Debug().Dur("cost", time.Since(startTime)).Msgf("signing BLS message, with addr=%s", s.selfAddr.Hex())
	m := common.Bytes2Hex(msg.Bytes())
	sig := s.privateKey.Pri.Sign(m)
	if sig == nil {
		return nil, errors.New("signature is nil")
	}
	return sig.Serialize(), nil
}

func (s *blsSigner) Aggregate(msgs []api.SSCMessage) (signatures []byte, bitmap []byte, err error) {
	if len(msgs) == 0 {
		return nil, nil, errors.New("empty msg")
	}
	startTime := time.Now()
	defer utils.SSCLogger().Debug().Dur("cost", time.Since(startTime)).Msgf("aggregating BLS signatures, num=%d", len(msgs))

	msg0 := msgs[0]
	shardID := s.selfShardId
	msgEpoch := msg0.GetEpochs()[shardID]
	var waitCh chan struct{}

	s.mu.RLock()
	currentEpoch := s.currentEpoch
	if currentEpoch < msgEpoch {
		utils.SSCLogger().Debug().Msgf("current epoch=%d, msg epoch=%d", currentEpoch, msgEpoch)
		waitCh = s.epochReadyCh[shardID][msgEpoch]
		if waitCh == nil {
			waitCh = make(chan struct{})
			s.epochReadyCh[shardID][msgEpoch] = waitCh
		}
	}
	s.mu.RUnlock()

	if waitCh != nil {
		utils.SSCLogger().Debug().Msgf("waiting for epoch %d to be ready, current epoch=%d", msgEpoch, currentEpoch)
		select {
		case <-waitCh:
			utils.SSCLogger().Debug().Msgf("epoch %d is ready", msgEpoch)
		case <-time.After(time.Hour * 10):
			return nil, nil, errors.New("timeout waiting for epoch to be ready")
		}
	}

	s.mu.RLock()
	pubKeys, exists := s.shard2PublicKeys[shardID][msgEpoch]
	addr2Index := s.addr2Index[shardID][msgEpoch]
	if !exists {
		utils.SSCLogger().Error().Interface("Addr2Index", s.addr2Index).Msgf("haven't config pubKeys, epoch: %d, shardId: %d", msgEpoch, shardID)
	}
	s.mu.RUnlock()

	// check if message is identical
	for i := 1; i < len(msgs); i++ {
		selfIndex := addr2Index[s.selfAddr]
		leadeerIndex := addr2Index[msgs[0].GetSenderAddr()]
		msgIndex := addr2Index[msgs[i].GetSenderAddr()]
		if !bytes.Equal(msg0.Bytes(), msgs[i].Bytes()) {
			utils.SSCLogger().Error().Interface("msg0", msg0).Interface(fmt.Sprintf("msg_%d", i), msgs[i]).
				Msgf("messages are not identical, selfIndex=%d, index=[%d, %d]", selfIndex, leadeerIndex, msgIndex)
			return nil, nil, errors.New("messages are not identical")
		}
	}
	mask := bls.NewMask(pubKeys)
	signs := make([]*blslib.Sign, 0)
	usedIndexes := make([]int, 0)
	for _, msg := range msgs {
		sign := blslib.Sign{}
		signature := msg.GetSignature()
		if signature == nil {
			return nil, nil, errors.New("signature is nil")
		}
		err := sign.Deserialize(signature)
		if err != nil {
			stack := debug.Stack()
			utils.SSCLogger().Error().Err(err).Interface("msg", msg).Msgf("failed to deserialize BLS signature, stack=%s", string(stack))
			return nil, nil, err
		}
		signs = append(signs, &sign)
		index, exists := addr2Index[msg.GetSenderAddr()]
		if !exists {
			stack := debug.Stack()
			utils.SSCLogger().Error().Err(err).Interface("msg", msg).Uint64("currentEpoch", uint64(currentEpoch)).Interface("addr2Index", addr2Index).Interface("allAddr2Index", s.addr2Index).Msgf("failed to find address %s in shard %d, %s", msg.GetSenderAddr(), s.selfShardId, string(stack))
			return nil, nil, errors.New("address not found")
		}
		usedIndexes = append(usedIndexes, index)
		if enabled, _ := mask.IndexEnabled(index); enabled {
			msgAddrs := make([]string, 0)
			for _, message := range msgs {
				msgAddrs = append(msgAddrs, message.GetSenderAddr().Hex())
			}
			utils.SSCLogger().Error().Interface("addr2Index", addr2Index).Interface("usedIndex", usedIndexes).Interface("msgs", msgAddrs).Msgf("address %s already signed, index=%d", msg.GetSenderAddr(), index)
			return nil, nil, errors.New("address already signed")
		}
		err = mask.SetBit(index, true)
		if err != nil {
			utils.SSCLogger().Error().Err(err).Interface("msg", msg).Msgf("failed to set bit %d in BLS mask, %d", index, mask.Len())
			return nil, nil, err
		}
	}

	if len(msgs) < s.shard2Threshold[s.selfShardId][msgEpoch] {
		utils.SSCLogger().Error().Interface("addr2Index", addr2Index).Msgf("not enough signatures, want: %d, get: %d, indexes: [%v]", s.shard2Threshold[s.selfShardId][msgEpoch], len(msgs), usedIndexes)
		return nil, nil, errors.New("not enough signatures")
	}

	utils.SSCLogger().Debug().Interface("addr2Index", addr2Index).Msgf("aggregate sigs, epoch: %d, want: %d, get: %d, total: %d, indexes: [%v]",
		msgEpoch, s.shard2Threshold[s.selfShardId][msgEpoch], mask.CountEnabled(), mask.CountTotal(), usedIndexes)

	keys := mask.GetPubKeyFromMask(true)
	keyHexes := make([]string, 0, len(keys))
	for _, key := range keys {
		keyHexes = append(keyHexes, key.SerializeToHexStr())
	}
	aggregateSig := bls.AggregateSig(signs)
	return aggregateSig.Serialize(), mask.Bitmap, nil
}

func (s *blsSigner) Verify(msg api.BLSSignedMessage) error {
	startTime := time.Now()

	if len(msg.GetEpochs()) == 0 {
		stack := debug.Stack()
		utils.SSCLogger().Error().Interface("msg", msg).Msgf("failed to verify signature, msgEpochs are nil, stack=%s", string(stack))
		return errors.New("failed to verify signature")
	}
	shardID := msg.GetShardId()
	msgEpoch := msg.GetEpochs()[shardID]
	defer utils.SSCLogger().Debug().Dur("cost", time.Since(startTime)).Msgf("verifying BLS signature, epoch=%d, shard=%d", msgEpoch, msg.GetShardId())

	var waitCh chan struct{}
	s.mu.RLock()
	currentEpoch := s.currentEpoch
	if currentEpoch < msgEpoch {
		waitCh = s.epochReadyCh[shardID][msgEpoch]
		if waitCh == nil {
			waitCh = make(chan struct{})
			s.epochReadyCh[shardID][msgEpoch] = waitCh
		}
	}
	s.mu.RUnlock()

	if waitCh != nil {
		utils.SSCLogger().Debug().Msgf("waiting for epoch %d to be ready", msgEpoch)
		select {
		case <-waitCh:
			utils.SSCLogger().Debug().Msgf("epoch %d is ready", msgEpoch)
		case <-time.After(time.Second * 10):
			return errors.New("timeout waiting for epoch to be ready")
		}
	}

	s.mu.RLock()
	pubKeys := s.shard2PublicKeys[shardID][msgEpoch]
	addr2Index := s.addr2Index[shardID][msgEpoch]
	s.mu.RUnlock()

	mask := bls.NewMask(pubKeys)
	if len(msg.GetBLSBitMap()) == 0 {
		stack := debug.Stack()
		utils.SSCLogger().Error().Interface("msg", msg).Msgf("empty bitmap, stack=%s", string(stack))
	}
	err := mask.SetMask(msg.GetBLSBitMap())
	if err != nil {
		return err
	}
	usedIndexes := make([]int, 0)
	for i := 0; i < mask.CountTotal(); i++ {
		if enabled, _ := mask.IndexEnabled(i); enabled {
			usedIndexes = append(usedIndexes, i)
		}
	}

	if mask.CountEnabled() < s.shard2Threshold[msg.GetShardId()][msgEpoch] {
		utils.SSCLogger().Error().Interface("addr2Index", addr2Index).Msgf("not enough signatures, epoch: %d, want: %d, get: %d, indexes: %v",
			msgEpoch, s.shard2Threshold[msg.GetShardId()][msgEpoch], mask.CountEnabled(), usedIndexes)
		return errors.New("not enough signatures")
	}
	sign := blslib.Sign{}
	err = sign.Deserialize(msg.GetSignatures())
	if err != nil {
		return err
	}
	m := common.Bytes2Hex(msg.Bytes())
	valid := sign.Verify(mask.AggregatePublic, m)
	if valid {
		return nil
	} else {
		memberKeys := make([]string, 0)
		for _, key := range pubKeys {
			memberKeys = append(memberKeys, key.Hex())
		}
		keys := mask.GetPubKeyFromMask(true)
		keyHexes := make([]string, 0, len(keys))
		for _, key := range keys {
			keyHexes = append(keyHexes, key.SerializeToHexStr())
		}
		keysStr := strings.Join(keyHexes, ", ")
		stack := debug.Stack()
		utils.SSCLogger().Error().
			Uint64("epoch", uint64(msgEpoch)).
			Interface("memberKeys", memberKeys).
			Msgf("BLS signature verification failed for msg: [%s], stack=%s", keysStr, string(stack))
		return errors.New("verify bls signature failed")
	}
}

func (s *blsSigner) UpdatePubKeys(shardID uint32, epoch api.Epoch, changed bool, addr2Index map[common.Address]int, pubKeys []bls.PublicKeyWrapper, threshold int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	utils.SSCLogger().Debug().Uint64("epoch", uint64(epoch)).Interface("addr2Index", addr2Index).Msgf("update pub keys, shard: %d, changed: %t", shardID, changed)
	if changed {
		if _, exists := s.shard2PublicKeys[shardID]; !exists {
			s.shard2PublicKeys[shardID] = make(map[api.Epoch][]bls.PublicKeyWrapper)
			s.addr2Index[shardID] = make(map[api.Epoch]map[common.Address]int)
			s.shard2Threshold[shardID] = make(map[api.Epoch]int)
		}
		s.shard2PublicKeys[shardID][epoch] = pubKeys
		s.addr2Index[shardID][epoch] = addr2Index
		s.shard2Threshold[shardID][epoch] = threshold
	} else {
		s.shard2PublicKeys[shardID][epoch] = s.shard2PublicKeys[shardID][s.currentEpoch]
		s.addr2Index[shardID][epoch] = s.addr2Index[shardID][s.currentEpoch]
		s.shard2Threshold[shardID][epoch] = s.shard2Threshold[shardID][s.currentEpoch]
	}
	s.currentEpoch = epoch

	// Close the wait channel to notify all waiting goroutines
	if s.epochReadyCh[shardID] == nil {
		s.epochReadyCh[shardID] = make(map[api.Epoch]chan struct{})
	}
	if readyCh, exists := s.epochReadyCh[shardID][epoch]; exists {
		close(readyCh)
	} else {
		readyCh = make(chan struct{})
		s.epochReadyCh[shardID][epoch] = readyCh
		close(readyCh)
	}
}

// getOrCreateEpochWaitCh returns a channel that will be closed when the epoch is ready
// Must be called with s.mu held
func (s *blsSigner) getOrCreateEpochWaitCh(epoch api.Epoch, shardId uint32) <-chan struct{} {
	if s.epochReadyCh[shardId] == nil {
		s.epochReadyCh[shardId] = make(map[api.Epoch]chan struct{})
	}
	if ch, exists := s.epochReadyCh[shardId][epoch]; exists {
		return ch
	}
	ch := make(chan struct{})
	s.epochReadyCh[shardId][epoch] = ch
	return ch
}

type txSigner struct {
	key     *ecdsa.PrivateKey
	addr    common.Address
	chainId *big.Int
}

func NewTxSigner(chainId *big.Int, privateKey *ecdsa.PrivateKey) api.TxSigner {
	addr := crypto.PubkeyToAddress(privateKey.PublicKey)
	return &txSigner{
		key:     privateKey,
		chainId: chainId,
		addr:    addr,
	}
}

func (t *txSigner) Address() common.Address {
	return t.addr
}

func (t *txSigner) Sign(tx *types.Transaction) (*types.Transaction, error) {
	return types.SignTx(tx, types.NewEIP155Signer(t.chainId), t.key)
}
