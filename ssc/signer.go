package ssc

import (
	"bytes"
	"crypto/ecdsa"
	"math/big"
	"runtime/debug"
	"strings"

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
	mgr.sscSigner = newBLSSigner(shardId, selfAddr, key)
	mgr.validatorSigner = newBLSSigner(shardId, selfAddr, key)
	return mgr
}

func (b *blsSignerMgr) GetSSCSigner() api.BLSSigner {
	return b.sscSigner
}

func (b *blsSignerMgr) GetValidatorSigner() api.BLSSigner {
	return b.validatorSigner
}

func (b *blsSignerMgr) UpdateSSCPubKeys(shardID uint32, epoch api.Epoch, addr2Index map[common.Address]int, pubKeys []bls.PublicKeyWrapper) {
	b.sscSigner.UpdatePubKeys(shardID, epoch, addr2Index, pubKeys)
}

func (b *blsSignerMgr) UpdateValidatorPubKeys(shardID uint32, epoch api.Epoch, addr2Index map[common.Address]int, pubKeys []bls.PublicKeyWrapper) {
	b.validatorSigner.UpdatePubKeys(shardID, epoch, addr2Index, pubKeys)
}

func newBLSSigner(shardId uint32, selfAddr common.Address, key *bls.PrivateKeyWrapper) api.BLSSigner {
	// return &fakeBLSSigner{} for testing
	return &blsSigner{
		addr2Index:       make(map[uint32]map[api.Epoch]map[common.Address]int),
		shard2PublicKeys: make(map[uint32]map[api.Epoch][]bls.PublicKeyWrapper),
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

func (f *fakeBLSSigner) UpdatePubKeys(shardID uint32, epoch api.Epoch, addr2Index map[common.Address]int, pubKeys []bls.PublicKeyWrapper) {
}

type blsSigner struct {
	addr2Index       map[uint32]map[api.Epoch]map[common.Address]int
	shard2PublicKeys map[uint32]map[api.Epoch][]bls.PublicKeyWrapper
	privateKey       *bls.PrivateKeyWrapper
	selfShardId      uint32
	selfAddr         common.Address
}

func (s *blsSigner) Address() common.Address {
	return s.selfAddr
}

func (s *blsSigner) Sign(msg api.MessageToSign) ([]byte, error) {
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
	utils.SSCLogger().Debug().Msgf("aggregating BLS signatures, num=%d", len(msgs))
	msg := msgs[0]
	// check if message is identical
	for i := 1; i < len(msgs); i++ {
		if !bytes.Equal(msg.Bytes(), msgs[i].Bytes()) {
			utils.SSCLogger().Error().Interface("msg", msg).Interface("msg_i", msgs[i]).Msg("messages are not identical")
			return nil, nil, errors.New("messages are not identical")
		}
	}
	pubKeys := s.shard2PublicKeys[s.selfShardId][msg.GetEpoch()]
	addr2Index := s.addr2Index[s.selfShardId][msg.GetEpoch()]
	if len(pubKeys) == 0 {
		stack := debug.Stack()
		utils.SSCLogger().Error().Interface("msg", msg).Msgf(""+
			"d %d, epoch %d, stack=%s", s.selfShardId, msg.GetEpoch(), string(stack))
		return nil, nil, errors.New("no public keys")
	}
	mask := bls.NewMask(pubKeys)
	usedIndexes := make([]int, 0)
	signs := make([]*blslib.Sign, 0)
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
			utils.SSCLogger().Error().Err(err).Interface("msg", msg).Msgf("failed to find address %s in shard %d, %s", msg.GetSenderAddr(), s.selfShardId, string(stack))
		}
		usedIndexes = append(usedIndexes, index)
		err = mask.SetBit(index, true)
		if err != nil {
			utils.SSCLogger().Error().Err(err).Interface("msg", msg).Msgf("failed to set bit %d in BLS mask, %d", index, mask.Len())
			return nil, nil, err
		}
	}
	keys := mask.GetPubKeyFromMask(true)
	keyHexes := make([]string, 0, len(keys))
	for _, key := range keys {
		keyHexes = append(keyHexes, key.SerializeToHexStr())
	}
	utils.SSCLogger().Debug().Interface("usedIndexes", usedIndexes).Msgf("aggregating BLS signatures, len=%d, msgLen=%d, epoch=%d", len(keys), len(msgs), msg.GetEpoch())
	aggregateSig := bls.AggregateSig(signs)
	return aggregateSig.Serialize(), mask.Bitmap, nil
}

func (s *blsSigner) Verify(msg api.BLSSignedMessage) error {
	if msg == nil {
		return errors.New("message is nil")
	}
	pubKeys := s.shard2PublicKeys[msg.GetShardId()][msg.GetEpoch()]
	if len(pubKeys) == 0 {
		return errors.New("no public keys")
	}
	mask := bls.NewMask(pubKeys)
	if len(msg.GetBLSBitMap()) == 0 {
		stack := debug.Stack()
		utils.SSCLogger().Error().Interface("msg", msg).Msgf("empty bitmap, stack=%s", string(stack))
	}
	err := mask.SetMask(msg.GetBLSBitMap())
	if err != nil {
		return err
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
		keys := mask.GetPubKeyFromMask(true)
		keyHexes := make([]string, 0, len(keys))
		for _, key := range keys {
			keyHexes = append(keyHexes, key.SerializeToHexStr())
		}
		keysStr := strings.Join(keyHexes, ", ")
		stack := debug.Stack()
		utils.SSCLogger().Error().Msgf("BLS signature verification failed for msg: [%s], stack=%s", keysStr, string(stack))
		return errors.New("verify bls signature failed")
	}
}

func (s *blsSigner) UpdatePubKeys(shardID uint32, epoch api.Epoch, addr2Index map[common.Address]int, pubKeys []bls.PublicKeyWrapper) {
	if _, exists := s.shard2PublicKeys[shardID]; !exists {
		s.shard2PublicKeys[shardID] = make(map[api.Epoch][]bls.PublicKeyWrapper)
		s.addr2Index[shardID] = make(map[api.Epoch]map[common.Address]int)
	}
	s.shard2PublicKeys[shardID][epoch] = pubKeys
	s.addr2Index[shardID][epoch] = addr2Index
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
