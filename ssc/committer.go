package ssc

import (
	"encoding/json"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/harmony-one/harmony/ssc/perf"
)

// CommitterStateAccessor 封装 Committer 需要的 simulationState 原子读写操作。
// 所有操作内部自带锁，锁永远在 sscService 内部。
type CommitterStateAccessor struct {
	IsTxFinished       func(txHash common.Hash) bool
	SetStatus          func(txHash common.Hash, status api.CXTStatus)
	CloseTx            func(txHash common.Hash, success bool, reason string)
	RemoveOnChainPatch func(txHash common.Hash) // v6: CR 完成后清理 onChainPatches
}

// Committer 负责 Commit/Rollback 交易的链上执行。
// 所有区块链节点都会部署此模块。
type Committer struct {
	committee *CommitteeMechanism
	stats     *SimulationStats
	state     CommitterStateAccessor
}

func NewCommitter(
	committee *CommitteeMechanism,
	stats *SimulationStats,
	state CommitterStateAccessor,
) *Committer {
	return &Committer{
		committee: committee,
		stats:     stats,
		state:     state,
	}
}

// CommitOrRollbackWithProof 执行 Commit 或 Rollback，并记录各阶段耗时。
// 所有节点（含非 leader validator）都执行此函数。
func (c *Committer) CommitOrRollbackWithProof(commitProofBytes []byte, stateDB api.StateDB, blockNum uint64) error {
	t0 := time.Now()

	commitProof := &api.CXTCommitProof{}
	err := json.Unmarshal(commitProofBytes, commitProof)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to unmarshal commit proof")
		return err
	}
	tUnmarshal := time.Since(t0)

	txHash := commitProof.TxHash

	if c.state.IsTxFinished(txHash) {
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msg("cxt has committed or rollback")
		return nil
	}
	tIsFinished := time.Since(t0)

	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msg("commit or rollback with proof")

	var tCommitTx time.Duration

	if commitProof.Type == api.Commit {
		utils.SSCLogger().Info().Str("txHash", txHash.String()).
			Msgf("commit with proof, origin: [%v,%d], epoch=%v",
				commitProof.OriginShard == c.committee.SelfShard, commitProof.OriginShard, commitProof.Epochs)
		if commitProof.SimulationNum > 0 {
			utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
				Msgf("commit with proof after %d resimulation, origin: [%v,%d]",
					commitProof.SimulationNum, commitProof.OriginShard == c.committee.SelfShard, commitProof.OriginShard)
		}
		t0commitTx := time.Now()
		err := stateDB.CommitTx(txHash)
		tCommitTx = time.Since(t0commitTx)
		if err != nil {
			utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(err).Msg("failed to commit tx with proof")
			return err
		}
		c.state.SetStatus(txHash, api.CXT_COMMITTED)
		c.state.CloseTx(txHash, true, commitProof.Reason.String())
	} else if commitProof.Type == api.Rollback {
		utils.SSCLogger().Info().Str("txHash", txHash.String()).
			Msgf("rollback with proof, origin: [%v,%d], epoch=%v, reason: %s",
				commitProof.OriginShard == c.committee.SelfShard, commitProof.OriginShard, commitProof.Epochs, commitProof.Reason.String())
		if commitProof.SimulationNum > 0 {
			utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
				Msgf("rollback with proof after %d resimulation, origin: [%v,%d]",
					commitProof.SimulationNum, commitProof.OriginShard == c.committee.SelfShard, commitProof.OriginShard)
		}
		t0commitTx := time.Now()
		err := stateDB.RollbackTx(txHash)
		tCommitTx = time.Since(t0commitTx)
		if err != nil {
			utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(err).Msg("failed to rollback tx with proof")
			return err
		}
		c.state.SetStatus(txHash, api.CXT_ROLLBACKED)
		c.state.CloseTx(txHash, false, commitProof.Reason.String())
	}
	tCloseTx := time.Since(t0)

	if c.committee.SelfShard == commitProof.OriginShard && c.committee.IsLeader(commitProof.Epochs[c.committee.SelfShard]) {
		c.stats.setCxtStage(txHash, 6)
	}

	// v6: CR 完成后所有节点清理 onChainPatches
	if c.state.RemoveOnChainPatch != nil {
		c.state.RemoveOnChainPatch(txHash)
	}

	total := time.Since(t0)
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Str("type", commitProof.Type.String()).
		Str("unmarshal", tUnmarshal.String()).
		Str("isFinished", tIsFinished.String()).
		Str("commitTx", tCommitTx.String()).
		Str("closeTx", tCloseTx.String()).
		Str("total", total.String()).
		Msg("CommitOrRollbackWithProof timing breakdown")
	perf.RecordPkg("committer", "CommitOrRollbackWithProof", "total", total)

	return nil
}
