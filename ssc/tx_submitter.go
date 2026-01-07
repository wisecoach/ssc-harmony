package ssc

import (
	"encoding/json"
	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/core"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/core/vm"
	"github.com/harmony-one/harmony/hmy"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/harmony-one/harmony/ssc/lm"
	"github.com/pkg/errors"
	"math/big"
)

type txSubmitter struct {
	lock      lm.Mutex
	selfShard uint32
	txSigner  api.TxSigner
	// TODO it need to be synchronized with all SSC members, for example, get the nonce from chain state when changed, or complete it when leader build
	nonce   uint64
	nodeAPI hmy.NodeAPI
	config  *api.Config
}

func (t *txSubmitter) SubmitSimulationTx(simulation *api.CXTSimulation) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	txHash := common.BytesToHash(simulation.TxHash)
	t.txSigner.Address()
	input, _ := json.Marshal(simulation)
	tx := types.NewCrossShardTransaction(t.nonce, &vm.SimulationCommitAddr, t.selfShard, t.selfShard, big.NewInt(0), t.config.SimulationCommitGasLimit, t.config.SimulationCommitGasPrice, input)
	signedTx, err := t.txSigner.Sign(tx)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
			Err(err).
			Uint64("Nonce", t.nonce).
			Str("originTxHash", txHash.Hex()).Msgf("sign tx failed, nonce=%d", t.nonce)
		return err
	}
	utils.SSCLogger().Info().Str("txHash", signedTx.Hash().Hex()).
		Uint64("Nonce", t.nonce).
		Str("originTxHash", txHash.Hex()).Msgf("submit simulation tx, nonce=%d", t.nonce)
	err = t.nodeAPI.AddPendingTransaction(signedTx)
	if errors.Is(err, core.ErrNonceTooLow) {

	}
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", signedTx.Hash().Hex()).
			Err(err).
			Uint64("Nonce", t.nonce).
			Str("originTxHash", txHash.Hex()).Msgf("add tx failed, nonce=%d", t.nonce)
		return err
	}
	t.nonce += 1
	return nil
}

func (t *txSubmitter) SubmitCommitOrRollbackTx(proof *api.CXTCommitProof) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	txHash := common.BytesToHash(proof.TxHash)
	t.txSigner.Address()
	input, _ := json.Marshal(proof)
	tx := types.NewCrossShardTransaction(t.nonce, &vm.CxtCommitOrRollbackAddr, t.selfShard, t.selfShard, big.NewInt(0), t.config.SimulationCommitGasLimit, t.config.SimulationCommitGasPrice, input)
	signedTx, err := t.txSigner.Sign(tx)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
			Err(err).
			Uint64("Nonce", t.nonce).
			Str("originTxHash", txHash.Hex()).Msgf("sign tx failed, nonce=%d", t.nonce)
		return err
	}
	utils.SSCLogger().Info().Str("txHash", signedTx.Hash().Hex()).
		Uint64("Nonce", t.nonce).
		Str("originTxHash", txHash.Hex()).Msgf("submit commit or rollback tx, nonce=%d", t.nonce)
	err = t.nodeAPI.AddPendingTransaction(signedTx)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", signedTx.Hash().Hex()).
			Err(err).
			Uint64("Nonce", t.nonce).
			Str("originTxHash", txHash.Hex()).Msgf("add tx failed, nonce=%d", t.nonce)
		return err
	}
	t.nonce += 1
	return nil
}

func (t *txSubmitter) SubmitEmptyTx() error {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.txSigner.Address()
	tx := types.NewCrossShardTransaction(t.nonce, &vm.EmptyAddr, t.selfShard, t.selfShard, big.NewInt(0), t.config.SimulationCommitGasLimit, t.config.SimulationCommitGasPrice, []byte{})
	signedTx, err := t.txSigner.Sign(tx)
	if err != nil {
		return err
	}
	err = t.nodeAPI.AddPendingTransaction(signedTx)
	if err != nil {
		return err
	}
	t.nonce += 1
	return nil
}
