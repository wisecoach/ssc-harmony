package ssc

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/internal/utils"
)

// TraceStage 标识交易生命周期的阶段
type TraceStage int

const (
	StageSimulateCX               TraceStage = iota // 0: SimulateCXTransaction started
	StageSimulationTxSubmit                         // 1: SubmitSimulationTx called
	StageSimulationTxCommit                         // 2: VerifySimulation executed on-chain
	StageCommitOrRollbackTxSubmit                   // 3: SubmitCommitOrRollbackTx called
	StageCommitOrRollback                           // 4: CommitOrRollbackWithProof executed on-chain
)

// TxBlockTrace 记录交易各阶段的块高度，用于分析阶段间块跨度
type TxBlockTrace struct {
	TxHash common.Hash

	// 各阶段记录到的块高度（0 表示未触发）
	SimulateBlockNum                 uint64 // 初始模拟开始
	SimulationTxSubmitBlockNum       uint64 // 模拟提交交易入池
	SimulationTxCommitBlockNum       uint64 // 模拟提交交易上链执行（VerifySimulation）
	CommitOrRollbackTxSubmitBlockNum uint64 // 提交 commit/rollback 交易入池
	CommitOrRollbackBlockNum         uint64 // commit/rollback 交易上链执行
}

// Record 记录某个阶段对应的块高度
func (t *TxBlockTrace) Record(stage TraceStage, blockNum uint64) {
	switch stage {
	case StageSimulateCX:
		t.SimulateBlockNum = blockNum
	case StageSimulationTxSubmit:
		t.SimulationTxSubmitBlockNum = blockNum
	case StageSimulationTxCommit:
		t.SimulationTxCommitBlockNum = blockNum
	case StageCommitOrRollbackTxSubmit:
		t.CommitOrRollbackTxSubmitBlockNum = blockNum
	case StageCommitOrRollback:
		t.CommitOrRollbackBlockNum = blockNum
	}
}

// HasAll 检查是否所有 5 个阶段都有记录
func (t *TxBlockTrace) HasAll() bool {
	return t.SimulateBlockNum > 0 &&
		t.SimulationTxSubmitBlockNum > 0 &&
		t.SimulationTxCommitBlockNum > 0 &&
		t.CommitOrRollbackTxSubmitBlockNum > 0 &&
		t.CommitOrRollbackBlockNum > 0
}

// Gap 返回两个阶段间的块数差，负数表示顺序异常
func (t *TxBlockTrace) Gap(from, to TraceStage) int64 {
	var fromBlock, toBlock uint64
	switch from {
	case StageSimulateCX:
		fromBlock = t.SimulateBlockNum
	case StageSimulationTxSubmit:
		fromBlock = t.SimulationTxSubmitBlockNum
	case StageSimulationTxCommit:
		fromBlock = t.SimulationTxCommitBlockNum
	case StageCommitOrRollbackTxSubmit:
		fromBlock = t.CommitOrRollbackTxSubmitBlockNum
	case StageCommitOrRollback:
		fromBlock = t.CommitOrRollbackBlockNum
	}
	switch to {
	case StageSimulateCX:
		toBlock = t.SimulateBlockNum
	case StageSimulationTxSubmit:
		toBlock = t.SimulationTxSubmitBlockNum
	case StageSimulationTxCommit:
		toBlock = t.SimulationTxCommitBlockNum
	case StageCommitOrRollbackTxSubmit:
		toBlock = t.CommitOrRollbackTxSubmitBlockNum
	case StageCommitOrRollback:
		toBlock = t.CommitOrRollbackBlockNum
	}
	return int64(toBlock) - int64(fromBlock)
}

// Log 将 trace 信息以结构化日志输出
func (t *TxBlockTrace) Log() string {
	return fmt.Sprintf(
		"TxBlockTrace{tx=%s, "+
			"simulate=%d, simSubmit=%d, simCommit=%d, rollbackSubmit=%d, rollback=%d | "+
			"gaps: sim→submit=%d, submit→commit=%d, commit→rollbackSubmit=%d, rollbackSubmit→rollback=%d}",
		t.TxHash.Hex()[:12],
		t.SimulateBlockNum, t.SimulationTxSubmitBlockNum, t.SimulationTxCommitBlockNum,
		t.CommitOrRollbackTxSubmitBlockNum, t.CommitOrRollbackBlockNum,
		t.Gap(StageSimulateCX, StageSimulationTxSubmit),
		t.Gap(StageSimulationTxSubmit, StageSimulationTxCommit),
		t.Gap(StageSimulationTxCommit, StageCommitOrRollbackTxSubmit),
		t.Gap(StageCommitOrRollbackTxSubmit, StageCommitOrRollback),
	)
}

// ---------- sscService 上的记录方法 ----------

// recordTraceBlock 记录某个阶段对应的块高度，线程安全
func (s *sscService) recordTraceBlock(txHash common.Hash, stage TraceStage, blockNum uint64) {
	s.traceLock.Lock()
	defer s.traceLock.Unlock()

	trace, exists := s.txTraces[txHash]
	if !exists {
		trace = &TxBlockTrace{TxHash: txHash}
		s.txTraces[txHash] = trace
	}
	trace.Record(stage, blockNum)

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Uint64("blockNum", blockNum).
		Msgf("tx trace: stage=%d", stage)
}
