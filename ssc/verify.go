package ssc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/block"
	"github.com/harmony-one/harmony/core"
	corestate "github.com/harmony-one/harmony/core/state"
	"github.com/harmony-one/harmony/core/vm"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/harmony-one/harmony/ssc/lm"
)

// VerifyCommunicator 封装 Verifier 需要的跨节点通信和签名能力。
type VerifyCommunicator struct {
	comm      *Comm
	signerMgr api.BLSSignerMgr
	committee *CommitteeMechanism
	config    *api.Config
	ctx       context.Context
}

func NewVerifyCommunicator(comm *Comm, signerMgr api.BLSSignerMgr, committee *CommitteeMechanism, config *api.Config, ctx context.Context) *VerifyCommunicator {
	return &VerifyCommunicator{
		comm:      comm,
		signerMgr: signerMgr,
		committee: committee,
		config:    config,
		ctx:       ctx,
	}
}

func (vc *VerifyCommunicator) SendCommitVote(shardId uint32, vote *api.CXTCommitVote) {
	txHash := vote.TxHash
	signer := vc.signerMgr.GetValidatorSigner()

	sign, err := signer.Sign(vote)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
			Msg("failed to sign CXTCommitVote")
		return
	}
	vote.BaseSSCMessage.SenderAddr = signer.Address()
	vote.BaseSSCMessage.Signature = sign

	ctx, cancel := context.WithTimeout(vc.ctx, vc.config.CallTimeout)
	defer cancel()

	targetLeader := vc.committee.GetLeader(vote.Epochs[shardId], shardId)
	if targetLeader == nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
			Msg("failed to get leader")
		return
	}

	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
		Str("type", vote.Type.String()).Str("reason", vote.Reason.String()).
		Msgf("send CXTCommitVote to %s", targetLeader.Endpoint)
	vc.comm.Call(ctx, nil, targetLeader, api.Method_HandleCommitVote, vote)
}

// VerifierStateAccessor 封装 simulationState 的原子读写操作。
// 所有操作内部自带锁，调用方不需要关心锁的范围。
// 锁永远在 sscService 内部，Verifier 拿不到锁对象本身。
type VerifierStateAccessor struct {
	SetStatus           func(txHash common.Hash, status api.CXTStatus)
	IsTxFinished        func(txHash common.Hash) bool
	SetWaitingForResimu func(txHash common.Hash)
}

// Verifier 负责验证 SimulationTx 的链上执行。
// 所有区块链节点都会部署此模块。自管理 executionVerifyContexts 和 txLockedSimNum 存储。

// lockedSimNumEntry 用于 txLockedSimNum 的 per-tx 存储。
// num 在创建时写入，后续只读，无需 per-entry mutex。
type lockedSimNumEntry struct {
	num int
}

type Verifier struct {
	communicator *VerifyCommunicator
	timerMgr     *CXTTimerManager
	committee    *CommitteeMechanism
	bc           core.BlockChain
	vmService    api.Service // 提供给 SSCVM 的接口
	stats        *SimulationStats
	retrySchd    *retryScheduler
	state        VerifierStateAccessor

	// 自管理的独立存储
	verifyCtxLock           lm.RWMutex
	executionVerifyContexts map[common.Hash]*api.ExecutionVerifyContext
	txLockedSimNum          sync.Map // key: common.Hash, value: *lockedSimNumEntry (num 创建后不可变)
}

func NewVerifier(
	communicator *VerifyCommunicator,
	timerMgr *CXTTimerManager,
	committee *CommitteeMechanism,
	bc core.BlockChain,
	vmService api.Service,
	stats *SimulationStats,
	retrySchd *retryScheduler,
	state VerifierStateAccessor,
) *Verifier {
	return &Verifier{
		communicator:            communicator,
		timerMgr:                timerMgr,
		committee:               committee,
		bc:                      bc,
		vmService:               vmService,
		stats:                   stats,
		retrySchd:               retrySchd,
		state:                   state,
		verifyCtxLock:           lm.RWMutex{},
		executionVerifyContexts: make(map[common.Hash]*api.ExecutionVerifyContext),
	}
}

// ===== 存储访问方法 =====

func (v *Verifier) StoreVerifyContext(txHash common.Hash, ctx *api.ExecutionVerifyContext) {
	v.verifyCtxLock.Lock()
	v.executionVerifyContexts[txHash] = ctx
	v.verifyCtxLock.Unlock()
}

func (v *Verifier) getVerifyContext(txHash common.Hash) *api.ExecutionVerifyContext {
	v.verifyCtxLock.RLock()
	defer v.verifyCtxLock.RUnlock()
	return v.executionVerifyContexts[txHash]
}

// HasVerifyContext 判断某笔 tx 是否存在验证上下文。
func (v *Verifier) HasVerifyContext(txHash common.Hash) bool {
	v.verifyCtxLock.RLock()
	defer v.verifyCtxLock.RUnlock()
	_, exists := v.executionVerifyContexts[txHash]
	return exists
}

// getOrCreateLockedSimNum 获取或创建首次锁冲突 simulationNum。
// sync.Map + 不可变 entry 设计：num 创建后只读，无需每 entry 加锁。
func (v *Verifier) getOrCreateLockedSimNum(txHash common.Hash, simulationNum int) int {
	actual, _ := v.txLockedSimNum.LoadOrStore(txHash, &lockedSimNumEntry{num: simulationNum})
	return actual.(*lockedSimNumEntry).num
}

// deleteLockedSimNum 删除首次锁冲突 simulationNum 记录。
func (v *Verifier) deleteLockedSimNum(txHash common.Hash) {
	v.txLockedSimNum.Delete(txHash)
}

// Cleanup 清理某个 tx 的全部验证上下文。
// 供 sscService.closeTransaction 调用。
func (v *Verifier) Cleanup(txHash common.Hash) {
	t0 := time.Now()
	v.verifyCtxLock.Lock()
	delete(v.executionVerifyContexts, txHash)
	v.verifyCtxLock.Unlock()
	v.txLockedSimNum.Delete(txHash)
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
		Str("duration", time.Since(t0).String()).
		Msg("Verifier.Cleanup timing")
}

// GetResult 读取验证结果。实现 api.Service 中的 CXTStateSimulationDB 接口。
func (v *Verifier) GetResult(txHash common.Hash) (result []byte, leftOverGas uint64, err error) {
	v.verifyCtxLock.Lock()
	defer v.verifyCtxLock.Unlock()

	verifyContext := v.executionVerifyContexts[txHash]
	if verifyContext == nil {
		return nil, 0, api.ErrInvalidExecution
	}
	if len(verifyContext.DependentResults) <= verifyContext.CallFrame.PC {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
			Interface("verifyContext", verifyContext).
			Interface("callFrame", verifyContext.CallFrame).
			Int("dependentResultsLength", len(verifyContext.DependentResults)).
			Msg("get result failed, index out of range")
		return nil, 0, api.ErrInvalidExecution
	}
	ret := verifyContext.DependentResults[verifyContext.CallFrame.PC]
	if ret == nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
			Interface("dependentResults", verifyContext.DependentResults).
			Msg("get result failed, result is nil")
		return nil, 0, api.ErrInvalidExecution
	}
	result = ret.Result
	leftOverGas = ret.LeftOverGas
	utils.SSCLogger().Debug().
		Str("txHash", txHash.Hex()).
		Interface("callFrame", verifyContext.CallFrame).
		Msgf("get result, [%d/%d]: %v", verifyContext.CallFrame.PC+1, len(verifyContext.DependentResults), result)
	verifyContext.CallFrame.Next()
	return
}

// ===== 验证核心 =====

// VerifySimulation 是链上验证 SimulationTx 的核心入口。
func (v *Verifier) VerifySimulation(simulationBytes []byte, stateDB api.StateDB, header *block.Header) {
	simulation := &api.CXTSimulation{}
	err := json.Unmarshal(simulationBytes, simulation)
	if err != nil {
		txHash := simulation.TxHash
		utils.SSCLogger().Error().Err(err).Str("txHash", txHash.Hex()).
			Msgf("failed to unmarshal simulation, size: %d", len(simulationBytes))
		payload := &api.CXTInvalidSimulationPayload{Type: api.InvalidSerialization}
		payloadBytes, _ := json.Marshal(payload)
		vote := &api.CXTCommitVote{
			TxHash:         txHash,
			Type:           api.Rollback,
			ShardId:        v.committee.SelfShard,
			OriginShardId:  simulation.OriginShardId,
			Reason:         api.ReasonInvalidSimulation,
			Payload:        payloadBytes,
			BaseSSCMessage: api.BaseSSCMessage{Epochs: simulation.Epochs},
		}
		v.communicator.SendCommitVote(v.committee.SelfShard, vote)
		return
	}

	txHash := simulation.TxHash

	tVs0 := time.Now()
	var tVsPatch, tVsLockCheck, tVsExec, tVsLockState time.Duration

	// v6: 所有节点收到 SimTx 后，将 ChainPatch 存入 onChainPatches
	if simulation.ChainPatch != nil {
		v.retrySchd.AddOnChainPatch(txHash, simulation.SimulationNum, simulation.ChainPatch,
			simulation.UpstreamTxList)
	}

	if v.committee.SelfShard == simulation.OriginShardId {
		v.timerMgr.StartSp1Timer(txHash, simulation.Epochs, header.NumberU64(), v.committee.SelfShard)
	}
	tVsPatch = time.Since(tVs0)

	// 更新 simulationState 状态
	v.state.SetStatus(txHash, api.VERIFYING_SIMULATION)

	// 所有节点记录首次锁冲突 simulationNum
	v.getOrCreateLockedSimNum(txHash, simulation.SimulationNum)

	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
		Int("simulationNum", simulation.SimulationNum).
		Interface("epoch", simulation.Epochs).
		Msg("begin to verify simulation")

	if v.committee.SelfShard == simulation.OriginShardId && v.committee.IsLeader(simulation.Epochs[v.committee.SelfShard]) {
		v.stats.setCxtStage(txHash, 3)
	}

	defer func() {
		utils.SSCLogger().Info().Str("txHash", txHash.String()).
			Dur("total", time.Since(tVs0)).
			Dur("chainPatch", tVsPatch).
			Dur("lockCheck", tVsLockCheck).
			Dur("execVerify", tVsExec).
			Dur("lockState", tVsLockState).
			Msg("VerifySimulation timing breakdown")
	}()

	conflictLockCallIndexes := make([]api.CallIndex, 0)
	conflictLockKeys := make([]api.LockKey, 0)
	conflictCallStateIndex := -1
	var execErr error

	callStateMap := make(map[string]*api.CXTCallState)
	for _, callState := range simulation.CallStates {
		callStateMap[callState.CallIndex.ToString()] = callState
	}
	verifyContext := &api.ExecutionVerifyContext{Simulation: simulation, CallStateMap: callStateMap}
	v.StoreVerifyContext(txHash, verifyContext)

	v.state.SetStatus(txHash, api.VERIFYING_SIMULATION)

	// 检查是否为链式依赖交易（有 ChainPatch / 在 onChainPatches 中有匹配的 Patch）
	// 如果是，跳过锁冲突检查，因为 nonce 排序保证了执行顺序
	isChainTx := false
	if simulation.ChainPatch != nil {
		// v6: 直接从 SimTx 的 ChainPatch 判断是否为链式交易
		isChainTx = true
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
			Int("simNum", simulation.SimulationNum).
			Int("upstreamCount", len(simulation.UpstreamTxList)).
			Msg("VerifySimulation: chain tx detected (from SimTx ChainPatch), skipping lock conflict check")
	}

CallStates:
	for i, callState := range simulation.CallStates {
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Str("callIndex", callState.CallIndex.ToString()).
			Msgf("verify call state %d", i)

		simNumTag := fmt.Sprintf("[simNum=%d]", simulation.SimulationNum)

		// check if states in write set are locked by other tx
		// 链式交易跳过锁冲突检查，nonce 排序保证执行顺序
		if !isChainTx {
			for address, stateMap := range callState.RWSet.WriteState.State {
				for key := range stateMap {
					lockKey := api.FormKey(address, key)
					_, stateErr := stateDB.GetState(txHash, address, key)
					if stateErr != nil {
						if errors.Is(stateErr, api.ErrLockConflict_OnChain) {
							utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
								Str("simNum", fmt.Sprintf("%d", simulation.SimulationNum)).
								Str("conflictSource", stateErr.Error()).
								Msgf("VerifySimulation %s mu conflict (write) for key: %s", simNumTag, lockKey)
							conflictLockKeys = append(conflictLockKeys, lockKey)
							conflictLockCallIndexes = append(conflictLockCallIndexes, callState.CallIndex)
						} else {
							utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
								Str("callIndex", callState.CallIndex.ToString()).
								Err(stateErr).Msgf("failed to get state for key: %s", lockKey)
							return
						}
					}
				}
			}
		}
		// check if states in read set are locked by other tx or not consistent with current state
		// 链式交易跳过锁冲突检查，nonce 排序保证执行顺序
		if !isChainTx {
			for address, stateMap := range callState.RWSet.ReadState.State {
				for key := range stateMap {
					lockKey := api.FormKey(address, key)
					onChainValue, stateErr := stateDB.GetState(txHash, address, key)
					if stateErr != nil {
						if errors.Is(stateErr, api.ErrLockConflict_OnChain) {
							utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
								Str("simNum", fmt.Sprintf("%d", simulation.SimulationNum)).
								Str("conflictSource", stateErr.Error()).
								Msgf("VerifySimulation %s mu conflict (read) for key: %s", simNumTag, lockKey)
							conflictLockKeys = append(conflictLockKeys, lockKey)
							conflictLockCallIndexes = append(conflictLockCallIndexes, callState.CallIndex)
						} else {
							utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
								Str("callIndex", callState.CallIndex.ToString()).
								Err(stateErr).Msgf("failed to get state for key: %s", lockKey)
							return
						}
					}
					if stateErr == nil && bytes.Compare(onChainValue.Bytes(), callState.RWSet.ReadState.State[address][key].Bytes()) != 0 {
						conflictCallStateIndex = i
						break CallStates
					}
				}
			}
		}
		tVsLockCheck = time.Since(tVs0)

		// 链式交易：Patch vs stateDB 一致性检查
		// 如果上游失败，stateDB 值与 patch 期望值不匹配，自己也失败
		// DAG 多上游：遍历所有上游，任一能提供匹配值就算通过
		if isChainTx && len(simulation.UpstreamTxList) > 0 {
			for address, stateMap := range callState.RWSet.ReadState.State {
				for key := range stateMap {
					for _, upstream := range simulation.UpstreamTxList {
						expectedVal, found := v.retrySchd.ReadOnChainPatch(
							upstream.TxHash,
							upstream.SimulationNum,
							address, key)
						if found {
							actualVal, err := stateDB.GetState(txHash, address, key)
							if err == nil && expectedVal != actualVal {
								utils.SSCLogger().Warn().Str("txHash", txHash.Hex()).
									Str("address", address.Hex()).
									Str("key", key.Hex()).
									Str("expected", expectedVal.Hex()).
									Str("actual", actualVal.Hex()).
									Msg("VerifySimulation: upstream failed, patch mismatch")
								execErr = fmt.Errorf("upstream failed: patch mismatch for key %s", api.FormKey(address, key))
								break CallStates
							}
							break // found matching upstream, no need to check others
						}
					}
				}
			}
		}

		// execute the contract and check if the write set is consistent with the simulation's write set
		execErr = v.verifyExecuteForCallState(simulation, txHash, callState, stateDB.(*corestate.DB))
		if execErr != nil {
			break CallStates
		}
	}

	tVsExec = time.Since(tVs0)
	if execErr != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
			Err(execErr).Msg("failed to verify execution for call state")
		// sscvm.Call() 在 verifyExecuteForCallState 内通过 SetAndLockState 获取了 locker 锁，
		// 必须清理 locker.pendingStates，否则残留锁会在 Commit 时合并到 lockedStates → LOCK_STALE
		stateDB.RollbackTx(txHash)
		payload := &api.CXTInvalidSimulationPayload{Type: api.InvalidExecution}
		payloadBytes, _ := json.Marshal(payload)
		vote := &api.CXTCommitVote{
			TxHash:         txHash,
			Type:           api.Rollback,
			ShardId:        v.committee.SelfShard,
			OriginShardId:  simulation.OriginShardId,
			Reason:         api.ReasonInvalidSimulation,
			Payload:        payloadBytes,
			BaseSSCMessage: api.BaseSSCMessage{Epochs: simulation.Epochs},
		}
		v.communicator.SendCommitVote(v.committee.SelfShard, vote)
		return
	}

	if conflictCallStateIndex >= 0 {
		// [冲突] 第一步：先检查超限，避免不必要的 lockStateWithExecution
		if exceeded, lockedSimNum := v.checkRetryLimitExceeded(txHash, simulation); exceeded {
			// 超限：直接发回滚投票
			stateDB.RollbackTx(txHash)
			v.sendRollbackVoteForRetry(txHash, simulation.SimulationNum, simulation.Epochs, simulation.OriginShardId, lockedSimNum)
			return
		}

		// [冲突] 第二步：按 EnableLockOnConflict 配置决定是否上局部锁
		config := v.timerMgr.GetTimeoutConfig()
		if config != nil && config.EnableLockOnConflict {
			snapshot := stateDB.Snapshot()
			utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
				Str("callIndex", simulation.CallStates[conflictCallStateIndex].CallIndex.ToString()).
				Msgf("try to mu state with execution")
			err := v.lockStateWithExecution(simulation.Epochs, simulation.CallStates[conflictCallStateIndex], stateDB.(*corestate.DB))
			if err != nil {
				stateDB.RevertToSnapshot(snapshot)
				// lockStateWithExecution 通过 sscvm.Call() 调用了 SetAndLockState，
				// RevertToSnapshot 只回退 state 数据，不清 locker.pendingStates
				stateDB.RollbackTx(txHash)
				utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
					Err(err).Msg("failed to mu state with execution")
				payload := &api.CXTInvalidSimulationPayload{Type: api.InvalidExecution}
				payloadBytes, _ := json.Marshal(payload)
				vote := &api.CXTCommitVote{
					TxHash:         txHash,
					Type:           api.Rollback,
					ShardId:        v.committee.SelfShard,
					OriginShardId:  simulation.OriginShardId,
					Reason:         api.ReasonConflictRWSetFailedLock,
					Payload:        payloadBytes,
					BaseSSCMessage: api.BaseSSCMessage{Epochs: simulation.Epochs},
				}
				v.communicator.SendCommitVote(v.committee.SelfShard, vote)
				return
			}
			if v.committee.SelfShard == simulation.OriginShardId && v.committee.IsLeader(simulation.Epochs[v.committee.SelfShard]) {
				v.stats.setCxtStage(txHash, 4)
			}
		}

		// [冲突] 第三步：解锁 + 调度重试
		stateDB.RollbackTx(txHash)
		v.callForRetry(txHash, simulation)
		return
	}

	if len(conflictLockKeys) > 0 {
		ls := make(map[api.LockKey]interface{})
		for _, callState := range simulation.CallStates {
			for addr, account := range callState.RWSet.ReadState.State {
				for key := range account {
					ls[api.FormKey(addr, key)] = struct{}{}
				}
			}
			for addr, account := range callState.RWSet.WriteState.State {
				for key := range account {
					ls[api.FormKey(addr, key)] = struct{}{}
				}
			}
		}

		stateDB.RollbackTx(txHash)
		if exceeded, lockedSimNum := v.checkRetryLimitExceeded(txHash, simulation); exceeded {
			v.sendRollbackVoteForRetry(txHash, simulation.SimulationNum, simulation.Epochs, simulation.OriginShardId, lockedSimNum)
		} else {
			v.callForRetry(txHash, simulation)
		}
		return
	} else {
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
			Uint32("FromShard", simulation.OriginShardId).
			Int("size", len(simulationBytes)).
			Msgf("simulation is valid, mu the rwset and send commit vote")
		for _, callState := range simulation.CallStates {
			v.lockStateWithRWSet(txHash, callState, stateDB)
		}
		tVsLockState = time.Since(tVs0)
		if v.committee.SelfShard == simulation.OriginShardId && v.committee.IsLeader(simulation.Epochs[v.committee.SelfShard]) {
			v.stats.setCxtStage(txHash, 4)
		}
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
			Int("simulationNum", simulation.SimulationNum).
			Uint32("shardId", v.committee.SelfShard).
			Uint32("originShard", simulation.OriginShardId).
			Msgf("VerifySimulation: success, sending Commit vote")
		vote := &api.CXTCommitVote{
			TxHash:         txHash,
			ShardId:        v.committee.SelfShard,
			OriginShardId:  simulation.OriginShardId,
			Type:           api.Commit,
			Reason:         api.ReasonSuccess,
			Payload:        nil,
			BaseSSCMessage: api.BaseSSCMessage{Epochs: simulation.Epochs},
		}
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Uint32("FromShard", simulation.OriginShardId).
			Int("size", len(simulationBytes)).
			Msgf("send cxt commit vote")
		v.communicator.SendCommitVote(v.committee.SelfShard, vote)
	}
}

// checkRetryLimitExceeded 检查链上重试次数是否超限
func (v *Verifier) checkRetryLimitExceeded(txHash common.Hash, simulation *api.CXTSimulation) (bool, int) {
	lockedSimNum := 0
	lockedExists := false
	if actual, ok := v.txLockedSimNum.Load(txHash); ok {
		lockedSimNum = actual.(*lockedSimNumEntry).num
		lockedExists = true
	}
	config := v.timerMgr.GetTimeoutConfig()

	// 链上专用上限：从首次上锁到现在的重试次数
	chainExceeded := lockedExists &&
		simulation.SimulationNum > lockedSimNum+int(config.MaxOnChainRetries)

	// 总上限：全链路重试次数
	totalExceeded := simulation.SimulationNum > int(config.MaxRetriesTotal)

	return chainExceeded || totalExceeded, lockedSimNum
}

// sendRollbackVoteForRetry 发送 retry 超限的回滚投票
func (v *Verifier) sendRollbackVoteForRetry(txHash common.Hash, simulationNum int, epochs []api.Epoch, originShardId uint32, lockedSimNum int) {
	utils.SSCLogger().Warn().Str("txHash", txHash.Hex()).
		Int("simulationNum", simulationNum).
		Int("lockedSimulationNum", lockedSimNum).
		Msg("on-chain retry limit exceeded, rollback transaction")
	payload := &api.CXTInvalidSimulationPayload{Type: api.MaxRetryExceeded}
	payloadBytes, _ := json.Marshal(payload)
	vote := &api.CXTCommitVote{
		TxHash:         txHash,
		ShardId:        v.committee.SelfShard,
		Type:           api.Rollback,
		OriginShardId:  originShardId,
		Reason:         api.ReasonMaxOnChainRetriesExceeded,
		Payload:        payloadBytes,
		BaseSSCMessage: api.BaseSSCMessage{Epochs: epochs},
	}
	v.communicator.SendCommitVote(v.committee.SelfShard, vote)
}

// callForRetry 通过 retryScheduler 调度下一轮链下重试
func (v *Verifier) callForRetry(txHash common.Hash, simulation *api.CXTSimulation) {
	if !v.committee.IsLeader(simulation.Epochs[v.committee.SelfShard]) {
		return
	}
	v.stats.setCxtStage(txHash, 4)
	v.retrySchd.CallForRetry(&api.RetryTx{
		TxHash:        txHash,
		Epochs:        simulation.Epochs,
		RelatedShards: simulation.RelatedShards,
		SimulationNum: simulation.SimulationNum + 1,
		Condition:     api.Verify,
		OriginShardID: simulation.OriginShardId,
	})
}

// verifyExecuteForCallState 验证单个 call state 的执行
func (v *Verifier) verifyExecuteForCallState(simulation *api.CXTSimulation, txHash common.Hash, callState *api.CXTCallState, stateDB *corestate.DB) error {
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Uint32("FromShard", simulation.OriginShardId).
		Msgf("verify execution for call state")
	chainConfig := v.bc.Config()
	vmConfig := v.bc.GetVMConfig()
	verifyContext := v.getVerifyContext(txHash)
	if v.state.IsTxFinished(txHash) {
		return nil
	}
	if verifyContext == nil {
		return errors.New("execution verify context is nil")
	}
	defer func() {
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Uint32("FromShard", simulation.OriginShardId).
			Msgf("verify execution for call state done")
	}()

	var (
		origin    common.Address
		callIndex api.CallIndex
		gasPrice  *big.Int
		addr      common.Address
		input     []byte
		gas       uint64
		value     *big.Int
		header    *block.Header
		simuRet   []byte
	)

	if callState.CallIndex.Top() {
		req := callState.TopRequest
		header = v.bc.GetHeaderByHash(req.BlockHash)
		if header == nil {
			return fmt.Errorf("failed to get header for block hash %s", req.BlockHash.Hex())
		}
		origin = req.From
		callIndex = api.CallIndex{}
		gasPrice = req.Tx.GasPrice()
		addr = *req.Tx.To()
		input = req.Tx.Data()
		gas = req.Tx.GasLimit()
		value = req.Tx.Value()
		if callState.TopResult == nil {
			marshal, _ := json.Marshal(callState)
			return fmt.Errorf("top result is nil, callState: %s", string(marshal))
		}
		simuRet = callState.TopResult.Result
	} else {
		req := callState.CallRequest
		header = v.bc.GetHeaderByHash(req.BlockHash)
		if header == nil {
			return fmt.Errorf("failed to get header for block hash %s", req.BlockHash.Hex())
		}
		origin = req.Caller
		callIndex = req.CallIndex
		gasPrice = req.GasPrice
		addr = req.Addr
		input = req.Input
		gas = req.Gas
		value = req.Value
		if callState.CallResult == nil {
			marshal, _ := json.Marshal(callState)
			return fmt.Errorf("call result is nil, callState: %s", string(marshal))
		}
		simuRet = callState.CallResult.Result
	}

	verifyContext.CurrentState = newStateSet()
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Str("callIndex", callIndex.ToString()).
		Msgf("verify call state %s for shard %d", callState.CallIndex.ToString(), v.committee.SelfShard)
	verifyContext.DependentResults = callState.DependentResults
	verifyContext.CallFrame = &api.CallFrame{
		CallIndex: callIndex,
		PC:        0,
	}

	sender := vm.AccountRef(origin)
	vmCtx := core.NewSSCVMContext(origin, txHash, callIndex, gasPrice, header, v.bc, nil)
	sscvm := vm.NewSSCVM(vmCtx, stateDB, chainConfig, *vmConfig, v.vmService, vm.ExecutionVerify)

	ret, _, err := sscvm.Call(sender, addr, input, gas, value)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Str("txHash", txHash.Hex()).Msg("verify callState failed")
		return err
	}

	if bytes.Compare(ret, simuRet) != 0 {
		return fmt.Errorf("simulation result is not equal to execution result, expected: %v, got: %v", simuRet, ret)
	}
	if !verifyContext.CurrentState.Equal(callState.RWSet.WriteState) {
		return fmt.Errorf("simulation write set is not equal to execution write set, callFrame=%v, process: [%d/%d], expected: %v, got: %v",
			verifyContext.CallFrame, verifyContext.CallFrame.PC, len(verifyContext.DependentResults),
			callState.RWSet.WriteState, verifyContext.CurrentState)
	}
	return nil
}

// checkLockConflict 检查 simulationtx 是否与其他交易存在锁冲突
func (v *Verifier) checkLockConflict(simulation *api.CXTSimulation, stateDB api.StateDB) bool {
	txHash := simulation.TxHash
	conflictLockKeys := make([]api.LockKey, 0)
	simNumTag := fmt.Sprintf("[simNum=%d]", simulation.SimulationNum)

	for _, callState := range simulation.CallStates {
		for address, stateMap := range callState.RWSet.ReadState.State {
			for key := range stateMap {
				lockKey := api.FormKey(address, key)
				_, err := stateDB.GetState(txHash, address, key)
				if err != nil && errors.Is(err, api.ErrLockConflict_OnChain) {
					utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
						Str("simNum", fmt.Sprintf("%d", simulation.SimulationNum)).
						Str("conflictSource", err.Error()).
						Msgf("checkLockConflict %s mu conflict (read) for key: %s", simNumTag, lockKey)
					conflictLockKeys = append(conflictLockKeys, lockKey)
				}
			}
		}
		for address, stateMap := range callState.RWSet.WriteState.State {
			for key := range stateMap {
				lockKey := api.FormKey(address, key)
				_, err := stateDB.GetState(txHash, address, key)
				if err != nil && errors.Is(err, api.ErrLockConflict_OnChain) {
					utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
						Str("simNum", fmt.Sprintf("%d", simulation.SimulationNum)).
						Str("conflictSource", err.Error()).
						Msgf("checkLockConflict %s mu conflict (write) for key: %s", simNumTag, lockKey)
					conflictLockKeys = append(conflictLockKeys, lockKey)
				}
			}
		}
	}

	return len(conflictLockKeys) > 0
}

// HasOnChainSimTx 检查交易是否已有活跃的链上 SimTx（即之前上过链）
func (v *Verifier) HasOnChainSimTx(txHash common.Hash) bool {
	_, exists := v.txLockedSimNum.Load(txHash)
	return exists
}

// lockStateWithExecution 对某个 call state 执行上锁并重执行
func (v *Verifier) lockStateWithExecution(epochs []api.Epoch, callState *api.CXTCallState, state *corestate.DB) error {
	t0 := time.Now()
	var (
		topReq        *api.CXTSimulationRequest
		callReq       *api.CXTCallSSCRequest
		txHash        common.Hash
		caller        common.Address
		callIndex     api.CallIndex
		gasPrice      *big.Int
		addr          common.Address
		input         []byte
		gas           uint64
		value         *big.Int
		relatedShards []uint32
	)
	if callState.CallIndex.Top() {
		topReq = callState.TopRequest
		txHash = topReq.TxHash
		caller = topReq.From
		callIndex = api.CallIndex{}
		gasPrice = topReq.Tx.GasPrice()
		addr = *topReq.Tx.To()
		input = topReq.Tx.Data()
		gas = topReq.Tx.GasLimit()
		value = topReq.Tx.Value()
		relatedShards = callState.TopResult.RelatedShards
	} else {
		callReq = callState.CallRequest
		txHash = callReq.TxHash
		caller = callReq.Caller
		callIndex = callReq.CallIndex
		gasPrice = callReq.GasPrice
		addr = callReq.Addr
		input = callReq.Input
		gas = callReq.Gas
		value = callReq.Value
		relatedShards = callState.CallResult.RelatedShards
	}
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Msgf("lockStateWithExecution, callIndex: %s, relatedShards: %v", callIndex.ToString(), relatedShards)

	header := v.bc.CurrentHeader()
	vmCtx := core.NewSSCVMContext(caller, txHash, callIndex, gasPrice, header, v.bc, nil)
	chainConfig := v.bc.Config()
	vmConfig := v.bc.GetVMConfig()
	sscvm := vm.NewSSCVM(vmCtx, state, chainConfig, *vmConfig, v.vmService, vm.ExecutionVerify)

	sender := vm.AccountRef(caller)
	_, leftOverGas, err := sscvm.Call(sender, addr, input, gas, value)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Str("txHash", txHash.Hex()).
			Dur("cost", time.Since(t0)).
			Msgf("lockStateWithExecution: execution failed callIndex=%s, contract=%s", callIndex.ToString(), addr.Hex())
		return err
	}
	_ = leftOverGas
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
		Str("callIndex", callIndex.ToString()).
		Dur("cost", time.Since(t0)).
		Msg("lockStateWithExecution timing breakdown")
	return nil
}
func (v *Verifier) lockStateWithRWSet(txHash common.Hash, callState *api.CXTCallState, stateDB api.StateDB) {
	t0 := time.Now()
	keyCount := 0
	for address, stateMap := range callState.RWSet.WriteState.State {
		for key, value := range stateMap {
			stateDB.SetAndLockState(txHash, callState.CallIndex, address, key, value)
			keyCount++
		}
	}
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
		Str("callIndex", callState.CallIndex.ToString()).
		Int("keys", keyCount).
		Dur("cost", time.Since(t0)).
		Msg("lockStateWithRWSet timing breakdown")
}

// ========================================================================
// VerifierStateDB — API.Service 接口的验证阶段状态操作
// 这些方法通过 sscService 内嵌提升到 sscService
// ========================================================================

// SubSimuBalance 在验证阶段扣除余额。
func (v *Verifier) SubSimuBalance(txHash common.Hash, address common.Address, amount *big.Int) error {
	v.verifyCtxLock.Lock()
	defer v.verifyCtxLock.Unlock()

	verifyContext := v.executionVerifyContexts[txHash]
	if verifyContext == nil {
		return api.ErrInvalidExecution
	}

	if verifyContext.CurrentState.Balance == nil {
		verifyContext.CurrentState.Balance = make(map[common.Address]*big.Int)
	}
	if verifyContext.CurrentState.Balance[address] == nil {
		balance, exists := verifyContext.CallStateMap[verifyContext.CallFrame.CallIndex.ToString()].RWSet.ReadState.Balance[address]
		if !exists {
			return api.ErrInvalidExecution
		}
		verifyContext.CurrentState.Balance[address] = balance
	}
	verifyContext.CurrentState.Balance[address].Sub(verifyContext.CurrentState.Balance[address], amount)
	return nil
}

// AddSimuBalance 在验证阶段增加余额。
func (v *Verifier) AddSimuBalance(txHash common.Hash, address common.Address, balance *big.Int) error {
	v.verifyCtxLock.Lock()
	defer v.verifyCtxLock.Unlock()

	verifyContext := v.executionVerifyContexts[txHash]
	if verifyContext == nil {
		return api.ErrInvalidExecution
	}
	if verifyContext.CurrentState.Balance == nil {
		verifyContext.CurrentState.Balance = make(map[common.Address]*big.Int)
	}
	if verifyContext.CurrentState.Balance[address] == nil {
		bal, exists := verifyContext.CallStateMap[verifyContext.CallFrame.CallIndex.ToString()].RWSet.ReadState.Balance[address]
		if !exists {
			return api.ErrInvalidExecution
		}
		verifyContext.CurrentState.Balance[address] = bal
	}
	verifyContext.CurrentState.Balance[address].Add(verifyContext.CurrentState.Balance[address], balance)
	return nil
}

// GetSimuBalance 获取验证阶段余额。
func (v *Verifier) GetSimuBalance(txHash common.Hash, address common.Address) (*big.Int, error) {
	v.verifyCtxLock.RLock()
	defer v.verifyCtxLock.RUnlock()

	verifyContext := v.executionVerifyContexts[txHash]
	if verifyContext == nil {
		return nil, api.ErrInvalidExecution
	}
	if verifyContext.CurrentState.Balance == nil {
		verifyContext.CurrentState.Balance = make(map[common.Address]*big.Int)
	}
	if verifyContext.CurrentState.Balance[address] == nil {
		bal, exists := verifyContext.CallStateMap[verifyContext.CallFrame.CallIndex.ToString()].RWSet.ReadState.Balance[address]
		if !exists {
			return nil, api.ErrInvalidExecution
		}
		verifyContext.CurrentState.Balance[address] = bal
	}
	return verifyContext.CurrentState.Balance[address], nil
}

// GetSimuState 获取验证阶段状态值。
func (v *Verifier) GetSimuState(txHash common.Hash, address common.Address, key common.Hash) (common.Hash, error) {
	v.verifyCtxLock.RLock()
	defer v.verifyCtxLock.RUnlock()

	verifyContext := v.executionVerifyContexts[txHash]
	if verifyContext == nil {
		return common.Hash{}, api.ErrInvalidExecution
	}
	if verifyContext.CurrentState.State[address] != nil {
		return verifyContext.CurrentState.State[address][key], nil
	}
	state, exists := verifyContext.CallStateMap[verifyContext.CallFrame.CallIndex.ToString()].RWSet.ReadState.State[address]
	if !exists {
		return common.Hash{}, api.ErrInvalidExecution
	}
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Interface("callFrame", verifyContext.CallFrame).
		Msgf("get simu state [%s:%s] = %s", address.Hex(), key.Hex(), state[key].Hex())
	return state[key], nil
}

// SetSimuState 设置验证阶段状态值。
func (v *Verifier) SetSimuState(txHash common.Hash, address common.Address, key common.Hash, value common.Hash) error {
	v.verifyCtxLock.Lock()
	defer v.verifyCtxLock.Unlock()

	verifyContext := v.executionVerifyContexts[txHash]
	if verifyContext == nil {
		return api.ErrInvalidExecution
	}
	if verifyContext.CurrentState.State[address] == nil {
		verifyContext.CurrentState.State[address] = make(map[common.Hash]common.Hash)
	}
	verifyContext.CurrentState.State[address][key] = value
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Interface("callFrame", verifyContext.CallFrame).
		Msgf("set simu state [%s:%s] = %s", address.Hex(), key.Hex(), value.Hex())
	return nil
}
