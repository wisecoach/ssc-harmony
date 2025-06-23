package ssc

import (
	"crypto/ecdsa"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	blslib "github.com/harmony-one/bls/ffi/go/bls"
	"github.com/harmony-one/harmony/core/genesis"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/crypto/bls"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/pkg/errors"
	"math/big"
)

func NewBLSSigner(shardId uint32, key *bls.PrivateKeyWrapper) api.BLSSigner {
	return &fakeBLSSigner{}
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

type blsSigner struct {
	addr2Index       map[common.Address]int
	shard2PublicKeys map[uint32][]bls.PublicKeyWrapper
	privateKey       *bls.PrivateKeyWrapper
	selfShardId      uint32
}

func (s *blsSigner) Sign(msg api.SSCMessage) ([]byte, error) {
	sig := s.privateKey.Pri.Sign(string(msg.Bytes()))
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
		sign := new(blslib.Sign)
		err := sign.Deserialize(msg.GetSignature())
		if err != nil {
			return nil, nil, err
		}
		signs = append(signs, sign)
		index := s.addr2Index[msg.GetSenderAddr()]
		err = mask.SetBit(index, true)
		if err != nil {
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
	sign := new(blslib.Sign)
	err = sign.Deserialize(msg.GetSignatures())
	if err != nil {
		return err
	}
	valid := sign.Verify(mask.AggregatePublic, string(msg.Bytes()))
	if valid {
		return nil
	} else {
		return errors.New("verify bls signature failed")
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
