package ssc

import (
	"bytes"
	"crypto/ecdsa"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	blslib "github.com/harmony-one/bls/ffi/go/bls"
	"github.com/harmony-one/harmony/core/genesis"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/crypto/bls"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/pkg/errors"
	"math/big"
	"runtime/debug"
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

func (b *blsSignerMgr) UpdateSSCPubKeys(shardID uint32, addr2Index map[common.Address]int, pubKeys []bls.PublicKeyWrapper) {
	b.sscSigner.UpdatePubKeys(shardID, addr2Index, pubKeys)
}

func (b *blsSignerMgr) UpdateValidatorPubKeys(shardID uint32, addr2Index map[common.Address]int, pubKeys []bls.PublicKeyWrapper) {
	b.validatorSigner.UpdatePubKeys(shardID, addr2Index, pubKeys)
}

func newBLSSigner(shardId uint32, selfAddr common.Address, key *bls.PrivateKeyWrapper) api.BLSSigner {
	// return &fakeBLSSigner{} for testing
	return &blsSigner{
		addr2Index:       make(map[uint32]map[common.Address]int),
		shard2PublicKeys: make(map[uint32][]bls.PublicKeyWrapper),
		privateKey:       key,
		selfAddr:         selfAddr,
		selfShardId:      shardId,
	}
}

type fakeBLSSigner struct {
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

func (f *fakeBLSSigner) UpdatePubKeys(shardID uint32, addr2Index map[common.Address]int, pubKeys []bls.PublicKeyWrapper) {
}

type blsSigner struct {
	addr2Index       map[uint32]map[common.Address]int
	shard2PublicKeys map[uint32][]bls.PublicKeyWrapper
	privateKey       *bls.PrivateKeyWrapper
	selfShardId      uint32
	selfAddr         common.Address
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
	mask := bls.NewMask(s.shard2PublicKeys[s.selfShardId])
	signs := make([]*blslib.Sign, 0)
	for _, msg := range msgs {
		sign := blslib.Sign{}
		err := sign.Deserialize(msg.GetSignature())
		if err != nil {
			stack := debug.Stack()
			utils.SSCLogger().Error().Err(err).Interface("msg", msg).Msgf("failed to deserialize BLS signature, stack=%s", string(stack))
			return nil, nil, err
		}
		signs = append(signs, &sign)
		index := s.addr2Index[s.selfShardId][msg.GetSenderAddr()]
		err = mask.SetBit(index, true)
		if err != nil {
			utils.SSCLogger().Error().Err(err).Interface("msg", msg).Msgf("failed to set bit %d in BLS mask, %d", index, mask.Len())
			return nil, nil, err
		}
	}
	aggregateSig := bls.AggregateSig(signs)
	return aggregateSig.Serialize(), mask.Bitmap, nil
}

func (s *blsSigner) Verify(msg api.BLSSignedMessage) error {
	mask := bls.NewMask(s.shard2PublicKeys[msg.GetShardId()])
	err := mask.SetMask(msg.GetBLSBitMap())
	if err != nil {
		return err
	}
	keys, err := mask.GetSignedPubKeysFromBitmap(msg.GetBLSBitMap())
	if err != nil {
		return err
	}
	mergedKey := &blslib.PublicKey{}
	for _, key := range keys {
		mergedKey.Add(key.Object)
	}
	if !bytes.Equal(mask.AggregatePublic.Serialize(), mergedKey.Serialize()) {
		utils.SSCLogger().Error().Msg("aggregate public key mismatch")
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
		stack := debug.Stack()
		utils.SSCLogger().Error().Msgf("BLS signature verification failed for msg: %s", string(stack))
		return errors.New("verify bls signature failed")
	}
}

func (s *blsSigner) UpdatePubKeys(shardID uint32, addr2Index map[common.Address]int, pubKeys []bls.PublicKeyWrapper) {
	s.shard2PublicKeys[shardID] = pubKeys
	s.addr2Index[shardID] = addr2Index

	if _, exist := addr2Index[s.selfAddr]; exist && shardID == s.selfShardId {
		msg := &api.CXTCommitVote{}
		sign, err := s.Sign(msg)
		if err != nil {
			utils.SSCLogger().Error().Err(err).Msg("failed to sign self-signed CXTCommitVote message after updating pub keys")
			return
		}
		msg.BaseSSCMessage = &api.BaseSSCMessage{
			Signature:  sign,
			SenderAddr: s.selfAddr,
		}
		aggregate, bitMap, err := s.Aggregate([]api.SSCMessage{msg})
		if err != nil {
			utils.SSCLogger().Error().Err(err).Msg("failed to aggregate self-signed CXTCommitVote message after updating pub keys")
			return
		}
		sscMsg := &api.CXTCommitSSCVote{}
		sscMsg.BaseBLSSignedMessage = &api.BaseBLSSignedMessage{
			Signatures: aggregate,
			BLSBitMap:  bitMap,
			ShardId:    s.selfShardId,
		}

		if !bytes.Equal(sscMsg.Bytes(), msg.Bytes()) {
			utils.SSCLogger().Error().Interface("msg", msg).Interface("sscMsg", sscMsg).Msg("ssc msg is not equal to original msg")
		}

		err = s.Verify(sscMsg)
		if err != nil {
			utils.SSCLogger().Error().Err(err).Msg("failed to verify self-signed CXTCommitVote message after updating pub keys")
			return
		}
		utils.SSCLogger().Info().Msg("successfully updated BLS public keys and verified self-signed CXTCommitVote message")
	}
}

type txSigner struct {
	key     *ecdsa.PrivateKey
	addr    common.Address
	chainId *big.Int
}

func NewTxSigner(chainId *big.Int) api.TxSigner {
	key := genesis.SSCSubmitterKey
	addr := crypto.PubkeyToAddress(key.PublicKey)
	return &txSigner{
		key:     key,
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
