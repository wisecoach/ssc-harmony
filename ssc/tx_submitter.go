package ssc

import (
	"encoding/json"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/core/vm"
	"github.com/harmony-one/harmony/hmy"
	"github.com/harmony-one/harmony/ssc/api"
	"math/big"
	"sync"
)

type txSubmitter struct {
	lock      sync.Mutex
	selfShard uint32
	txSigner  api.TxSigner
	nonce     uint64
	nodeAPI   hmy.NodeAPI
	config    *api.Config
}

func (t *txSubmitter) SubmitSimulationTx(simulation *api.CXTSimulation) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.txSigner.Address()
	input, _ := json.Marshal(simulation)
	tx := types.NewCrossShardTransaction(t.nonce, &vm.SimulationCommitAddr, t.selfShard, t.selfShard, big.NewInt(0), t.config.SimulationCommitGasLimit, t.config.SimulationCommitGasPrice, input)
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

func (t *txSubmitter) SubmitCommitOrRollbackTx(proof *api.CXTCommitProof) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.txSigner.Address()
	input, _ := json.Marshal(proof)
	tx := types.NewCrossShardTransaction(t.nonce, &vm.CxtCommitOrRollbackAddr, t.selfShard, t.selfShard, big.NewInt(0), t.config.SimulationCommitGasLimit, t.config.SimulationCommitGasPrice, input)
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
