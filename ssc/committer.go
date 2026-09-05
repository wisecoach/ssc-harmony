package ssc

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/harmony-one/harmony/ssc/api/proto"
	"github.com/harmony-one/harmony/ssc/perf"
	"google.golang.org/protobuf/proto"
)

// CommitterStateAccessor 封装 Committer 需要的 simulationState 原子读写操作。
// 所有操作内部自带锁，锁永远在 sscService 内部。
type CommitterStateAccessor struct {
	IsTxFinished       func(txHash common.Hash) bool
	SetStatus          func(txHash common.Hash, status api.CXTStatus)
	CloseTx            func(txHash common.Hash, success bool, reason string)
	RemoveOnChainDAGPatch func(txHash common.Hash) // v6: CR 完成后清理 onChainDAGPatches
	// RecordChainTxCRCommit — 统计“链式(isChainTx)交易最终 CR commit”的次数（仅 origin leader 回调）。
	RecordChainTxCRCommit func(txHash common.Hash)
}

// Committer 负责 Commit/Rollback 交易的链上执行。
// 所有区块链节点都会部署此模块。
type Committer struct {
	committee *CommitteeMechanism
	stats     *SimulationStats
	state     CommitterStateAccessor

	// traceSvc — 指向所属 sscService，用于补全 tx block trace 的 StageCommitOrRollback 埋点
	traceSvc *sscService
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

	commitProofProto := &sscpb.CXTCommitProof{}
	err := proto.Unmarshal(commitProofBytes, commitProofProto)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to unmarshal commit proof")
		return err
	}
	commitProof := sscpb.CXTCommitProofFromProto(commitProofProto)
	tUnmarshal := time.Since(t0)

	txHash := commitProof.TxHash

	if c.state.IsTxFinished(txHash) {
		// BUG-12: tx 已 close（Closed=true），但仍需按 CR 语义释放其在全局锁中的残留锁。
		// 背景：早期 generation 的 rollback/close（如 ReasonCxtTimeoutForSp1/PoolTimeout）只置 Closed=true、
		//   不释放其他 generation 在 target 分片已 merge 到全局的锁；此 CR 是这些锁的唯一合法释放入口，
		//   命中 IsTxFinished 若直接 return 则锁永不释放（累积到 globalLocked，LOCK_STALE 无限增长）。
		// 解决：命中 IsTxFinished 仍调用 CommitTx/RollbackTx，让块尾 applyTo 通过全局反向索引释放剩余锁。
		//   对已释放锁幂等（no-op），符合"链上回滚/提交释放锁"约束。
		utils.SSCLogger().Info().Str("txHash", txHash.String()).
			Msgf("cxt has committed or rollback; force-releasing residual locks via %s",
				func() string {
					if commitProof.Type == api.Commit {
						return "CommitTx"
					}
					return "RollbackTx"
				}())
		if commitProof.Type == api.Commit {
			if err := stateDB.CommitTx(txHash); err != nil {
				utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(err).
					Msg("failed to force-commit residual locks (BUG-12)")
			}
		} else if commitProof.Type == api.Rollback {
			if err := stateDB.RollbackTx(txHash); err != nil {
				utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(err).
					Msg("failed to force-rollback residual locks (BUG-12)")
			}
		}
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
		// DSN-52: ReleaseOnly 表示「只释放锁、不 close 交易」——冲突让位后回重试池，
		// 而不是永久判死。
		if !commitProof.ReleaseOnly {
			c.state.SetStatus(txHash, api.CXT_ROLLBACKED)
			c.state.CloseTx(txHash, false, commitProof.Reason.String())
		} else {
			utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
				Msg("rollback with proof (ReleaseOnly): released locks, tx stays in retry (DSN-52)")
		}
	}
	tCloseTx := time.Since(t0)

	if c.committee.SelfShard == commitProof.OriginShard && c.committee.IsLeader(commitProof.Epochs[c.committee.SelfShard]) {
		c.stats.setCxtStage(txHash, 6)
		// 统计“链式(isChainTx)交易最终 CR commit”次数（仅 origin leader，避免多节点重复计数）。
		// 只在真正 Commit（非 Rollback/ReleaseOnly）时计入。
		if commitProof.Type == api.Commit && c.state.RecordChainTxCRCommit != nil {
			c.state.RecordChainTxCRCommit(txHash)
		}
	}

	// 补全 tx block trace：StageCommitOrRollback（CR 上链执行）阶段
	// closeTransaction 也会设置 CommitOrRollbackTime，但这里通过 recordTraceBlock
	// 同时记录块号并输出 debug "tx trace: stage=4"，与 [txLife] 口径一致。
	if c.traceSvc != nil {
		c.traceSvc.recordTraceBlock(txHash, StageCommitOrRollback, blockNum)
	}

	// v6: CR 完成后所有节点清理 onChainDAGPatches
	if c.state.RemoveOnChainDAGPatch != nil {
		c.state.RemoveOnChainDAGPatch(txHash)
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
