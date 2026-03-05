package ssc

import (
	"encoding/json"
	"math/big"

	"github.com/harmony-one/harmony/core"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/core/vm"
	"github.com/harmony-one/harmony/hmy"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/harmony-one/harmony/ssc/lm"
	"github.com/pkg/errors"
)

func NewTxSubmitter(selfShard uint32, txSigner api.TxSigner, nodeAPI hmy.NodeAPI, config *api.Config) api.TxSubmitter {
	return &txSubmitter{
		lock:      lm.NewMutex(),
		selfShard: selfShard,
		txSigner:  txSigner,
		nodeAPI:   nodeAPI,
		config:    config,
	}
}

type txSubmitter struct {
	lock      lm.Mutex
	selfShard uint32
	txSigner  api.TxSigner
	nonce     uint64
	nodeAPI   hmy.NodeAPI
	config    *api.Config
}

func (t *txSubmitter) SubmitSimulationTx(simulation *api.CXTSimulation) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	input, _ := json.Marshal(simulation)
	tx := types.NewCrossShardTransaction(t.nonce, &vm.SimulationCommitAddr, t.selfShard, t.selfShard, big.NewInt(0), t.config.SimulationCommitGasLimit, t.config.SimulationCommitGasPrice, input)
	return t.signAndAddPendingTx(tx, "SimulationTx")
}

func (t *txSubmitter) SubmitCommitOrRollbackTx(proof *api.CXTCommitProof) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	input, _ := json.Marshal(proof)
	tx := types.NewCrossShardTransaction(t.nonce, &vm.CxtCommitOrRollbackAddr, t.selfShard, t.selfShard, big.NewInt(0), t.config.SimulationCommitGasLimit, t.config.SimulationCommitGasPrice, input)
	return t.signAndAddPendingTx(tx, "CommitOrRollbackTx")
}

// SubmitEmptyTx
// Deprecated
func (t *txSubmitter) SubmitEmptyTx() error {
	t.lock.Lock()
	defer t.lock.Unlock()
	tx := types.NewCrossShardTransaction(t.nonce, &vm.EmptyAddr, t.selfShard, t.selfShard, big.NewInt(0), t.config.SimulationCommitGasLimit, t.config.SimulationCommitGasPrice, []byte{})
	return t.signAndAddPendingTx(tx, "EmptyTx")
}

func (t *txSubmitter) SubmitNewEpoch(newEpoch *api.NewEpoch) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	input, _ := json.Marshal(newEpoch)
	tx := types.NewCrossShardTransaction(t.nonce, &vm.NewEpochAddr, t.selfShard, t.selfShard, big.NewInt(0), t.config.SimulationCommitGasLimit, t.config.SimulationCommitGasPrice, input)
	return t.signAndAddPendingTx(tx, "NewEpoch")
}

func (t *txSubmitter) SubmitUploadOpinions(uploadOpinions *api.SelfOpinions) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	input, _ := json.Marshal(uploadOpinions)
	tx := types.NewCrossShardTransaction(t.nonce, &vm.SLOpinionAddr, t.selfShard, t.selfShard, big.NewInt(0), t.config.SimulationCommitGasLimit, t.config.SimulationCommitGasPrice, input)
	return t.signAndAddPendingTx(tx, "UploadOpinions")
}

func (t *txSubmitter) signAndAddPendingTx(tx *types.Transaction, txType string) error {
	txHash := tx.Hash()
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("submit simulation tx, type=%s, nonce=%d", txType, t.nonce)
	signedTx, err := t.txSigner.Sign(tx)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
			Err(err).
			Uint64("Nonce", t.nonce).
			Str("originTxHash", txHash.Hex()).Msgf("sign tx failed, nonce=%d", t.nonce)
		return err
	}
	err = t.nodeAPI.AddPendingTransaction(signedTx)
	if err != nil {
		if errors.Is(err, core.ErrNonceTooLow) {
			t.nonce += 1
			newTx := types.NewCrossShardTransaction(t.nonce, tx.To(), tx.ShardID(), tx.ToShardID(), tx.Value(), tx.GasLimit(), tx.GasPrice(), tx.Data())
			return t.signAndAddPendingTx(newTx, txType)
		} else {
			utils.SSCLogger().Error().Str("txHash", signedTx.Hash().Hex()).
				Err(err).
				Uint64("Nonce", t.nonce).
				Str("originTxHash", txHash.Hex()).Msgf("add tx failed, nonce=%d", t.nonce)
			return err
		}
	}
	t.nonce += 1
	return nil
}
