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
	"github.com/harmony-one/harmony/ssc/perf"
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

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
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

// callVerifyContext 是每个 CallState 独立的验证上下文
// 用于并行验证时隔离 CurrentState/CallFrame/DependentResults
type callVerifyContext struct {
	currentState     *api.StateSet
	callFrame        *api.CallFrame
	dependentResults []*api.CXTCallSSCResult
	simuStateCount   int
	simuStateDur     time.Duration
}

// loadSubCtx 从根上下文的内嵌 subCtx 中加载子验证上下文。
func (v *Verifier) loadSubCtx(txHash common.Hash, callIndex api.CallIndex) *callVerifyContext {
	root := v.getVerifyContext(txHash)
	if root == nil {
		return nil
	}
	val, ok := root.SubCtx.Load(callIndex.ToString())
	if !ok {
		return nil
	}
	return val.(*callVerifyContext)
}

// storeSubCtx 在根上下文中存储子验证上下文。
func (v *Verifier) storeSubCtx(txHash common.Hash, callIndex api.CallIndex, ctx *callVerifyContext) {
	root := v.getVerifyContext(txHash)
	if root != nil {
		root.SubCtx.Store(callIndex.ToString(), ctx)
	}
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
	executionVerifyContexts sync.Map // key: common.Hash → *api.ExecutionVerifyContext（含内嵌 subCtx）
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
		communicator: communicator,
		timerMgr:     timerMgr,
		committee:    committee,
		bc:           bc,
		vmService:    vmService,
		stats:        stats,
		retrySchd:    retrySchd,
		state:        state,
	}
}

// ===== 存储访问方法 =====

func (v *Verifier) StoreVerifyContext(txHash common.Hash, ctx *api.ExecutionVerifyContext) {
	v.executionVerifyContexts.Store(txHash, ctx)
}

func (v *Verifier) getVerifyContext(txHash common.Hash) *api.ExecutionVerifyContext {
	if val, ok := v.executionVerifyContexts.Load(txHash); ok {
		return val.(*api.ExecutionVerifyContext)
	}
	return nil
}

// HasVerifyContext 判断某笔 tx 是否存在验证上下文。
func (v *Verifier) HasVerifyContext(txHash common.Hash) bool {
	_, exists := v.executionVerifyContexts.Load(txHash)
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
	v.executionVerifyContexts.Delete(txHash)
	v.txLockedSimNum.Delete(txHash)
	// 子上下文随根上下文一起清理
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Dur("cost", time.Since(t0)).
		Msg("Verifier.Cleanup timing")
}

// GetResult 读取验证结果。实现 api.Service 中的 CXTStateSimulationDB 接口。
func (v *Verifier) GetResult(txHash common.Hash, callIndex api.CallIndex) (result []byte, leftOverGas uint64, err error) {
	subCtx := v.loadSubCtx(txHash, callIndex)
	if subCtx == nil {
		return nil, 0, api.ErrInvalidExecution
	}
	if len(subCtx.dependentResults) <= subCtx.callFrame.PC {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
			Int("pc", subCtx.callFrame.PC).
			Int("dependentResultsLength", len(subCtx.dependentResults)).
			Msg("get result failed, index out of range")
		return nil, 0, api.ErrInvalidExecution
	}
	ret := subCtx.dependentResults[subCtx.callFrame.PC]
	if ret == nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
			Msg("get result failed, result is nil")
		return nil, 0, api.ErrInvalidExecution
	}
	result = ret.Result
	leftOverGas = ret.LeftOverGas
	utils.SSCLogger().Debug().
		Str("txHash", txHash.Hex()).
		Msgf("get result, [%d/%d]: %v", subCtx.callFrame.PC+1, len(subCtx.dependentResults), result)
	subCtx.callFrame.Next()
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
		go v.communicator.SendCommitVote(v.committee.SelfShard, vote)
		return
	}

	txHash := simulation.TxHash

	tVs0 := time.Now()
	var tVsPatch, tVsLockCheck, tVsExec, tVsLockState time.Duration

	if v.committee.SelfShard == simulation.OriginShardId {
		v.timerMgr.StartSp1Timer(txHash, simulation.Epochs, header.NumberU64(), v.committee.SelfShard)
	}
	tVsPatch = time.Since(tVs0)

	// 更新 simulationState 状态
	v.state.SetStatus(txHash, api.VERIFYING_SIMULATION)

	// 所有节点记录首次锁冲突 simulationNum
	v.getOrCreateLockedSimNum(txHash, simulation.SimulationNum)

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Int("simulationNum", simulation.SimulationNum).
		Interface("epoch", simulation.Epochs).
		Msg("begin to verify simulation")

	if v.committee.SelfShard == simulation.OriginShardId && v.committee.IsLeader(simulation.Epochs[v.committee.SelfShard]) {
		v.stats.setCxtStage(txHash, 3)
	}

	defer func() {
		if tVsExec > time.Millisecond*100 {
			utils.SSCLogger().Info().Str("txHash", txHash.String()).
				Int("size", len(simulationBytes)).
				Int("callStates", len(simulation.CallStates)).
				Dur("total", time.Since(tVs0)).
				Dur("chainPatch", tVsPatch).
				Dur("lockCheck", tVsLockCheck).
				Dur("execVerify", tVsExec).
				Dur("lockState", tVsLockState).
				Msg("VerifySimulation timing breakdown, abnormal")
		}
		utils.SSCLogger().Info().Str("txHash", txHash.String()).
			Int("size", len(simulationBytes)).
			Int("callStates", len(simulation.CallStates)).
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
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
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
					stateErr := stateDB.CheckLock(lockKey, txHash)
					if stateErr != nil {
						if errors.Is(stateErr, api.ErrLockConflict_OnChain) {
							utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
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
		// if !isChainTx {
		// 	for address, stateMap := range callState.RWSet.ReadState.State {
		// 		for key := range stateMap {
		// 			lockKey := api.FormKey(address, key)
		// 			onChainValue, stateErr := stateDB.GetState(txHash, address, key)
		// 			if stateErr != nil {
		// 				if errors.Is(stateErr, api.ErrLockConflict_OnChain) {
		// 					utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		// 						Str("simNum", fmt.Sprintf("%d", simulation.SimulationNum)).
		// 						Str("conflictSource", stateErr.Error()).
		// 						Msgf("VerifySimulation %s mu conflict (read) for key: %s", simNumTag, lockKey)
		// 					conflictLockKeys = append(conflictLockKeys, lockKey)
		// 					conflictLockCallIndexes = append(conflictLockCallIndexes, callState.CallIndex)
		// 				} else {
		// 					utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
		// 						Str("callIndex", callState.CallIndex.ToString()).
		// 						Err(stateErr).Msgf("failed to get state for key: %s", lockKey)
		// 					return
		// 				}
		// 			}
		// 			if stateErr == nil && bytes.Compare(onChainValue.Bytes(), callState.RWSet.ReadState.State[address][key].Bytes()) != 0 {
		// 				conflictCallStateIndex = i
		// 				utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
		// 					Str("callIndex", callState.CallIndex.ToString()).
		// 					Str("address", address.Hex()).
		// 					Str("key", key.Hex()).
		// 					Str("expected", callState.RWSet.ReadState.State[address][key].Hex()).
		// 					Str("actual", onChainValue.Hex()).
		// 					Uint64("blockNum", header.NumberU64()).
		// 					Msg("VerifySimulation: read state mismatch with on-chain state")
		// 				// 继续收集后续 CallState 的锁信息，但标记冲突点
		// 			}
		// 		}
		// 	}
		// }

		// 链式交易：Patch vs stateDB 一致性检查
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
	}
	tVsLockCheck = time.Since(tVs0)
	perf.RecordPkg("verify", "VerifySimulation", "lockCheck", tVsLockCheck)

	// execErr 为 nil 才需要继续执行 Phase 2/3
	if execErr == nil {
		// Phase 1.5: 预分配子上下文（所有 CallState 都需要）
		// 子上下文存储 CurrentState/CallFrame/DependentResults，供并行 execVerify 隔离
		execCallStates := len(simulation.CallStates)
		if conflictCallStateIndex >= 0 {
			execCallStates = conflictCallStateIndex // 冲突点之后的 CallState 不再执行
		}
		for i := 0; i < execCallStates; i++ {
			callState := simulation.CallStates[i]
			callIndex := callState.CallIndex
			subCtx := &callVerifyContext{
				currentState:     newStateSet(),
				callFrame:        &api.CallFrame{CallIndex: callIndex, PC: 0},
				dependentResults: callState.DependentResults,
			}
			v.storeSubCtx(txHash, callIndex, subCtx)
		}

		// Phase 1.5.5: Copy stateDB，每个 goroutine 独立实例避免 map 并发写
		tCopy0 := time.Now()
		db := stateDB.(*corestate.DB)
		copies := make([]*corestate.DB, execCallStates)
		for i := 0; i < execCallStates; i++ {
			copies[i] = db.Copy()
		}
		utils.SSCLogger().Info().Int("copies", execCallStates).
			Dur("cost", time.Since(tCopy0)).
			Str("txHash", txHash.Hex()).
			Msg("Phase 1.5.5: stateDB.Copy timing")

		// Phase 2: 并行 execVerify — 对所有通过 lockCheck 的 CallState 并行执行
		var wg sync.WaitGroup
		type execResult struct {
			idx int
			err error
		}
		results := make([]execResult, execCallStates)
		for i := 0; i < execCallStates; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				callState := simulation.CallStates[idx]
				err := v.verifyExecuteForCallState(simulation, txHash, callState, copies[idx])
				results[idx] = execResult{idx, err}
			}(i)
		}
		wg.Wait()

		// Phase 2.5: 检查 execVerify 结果
		tVsExec = time.Since(tVs0)
		perf.RecordPkg("verify", "VerifySimulation", "execVerify", tVsExec)
		for _, r := range results {
			if r.err != nil {
				execErr = r.err
				break
			}
		}
		// 清理子上下文（defer 已处理）
	}

	if execErr != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
			Err(execErr).Msg("failed to verify execution for call state")
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
		go v.communicator.SendCommitVote(v.committee.SelfShard, vote)
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
				go v.communicator.SendCommitVote(v.committee.SelfShard, vote)
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
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Uint32("FromShard", simulation.OriginShardId).
			Int("size", len(simulationBytes)).
			Msgf("simulation is valid, mu the rwset and send commit vote")
		for _, callState := range simulation.CallStates {
			v.lockStateWithRWSet(txHash, callState, stateDB)
		}

		// 验证通过后将 ChainPatch 存入 onChainPatches，供下游链式交易查询
		if simulation.ChainPatch != nil {
			v.retrySchd.AddOnChainPatch(txHash, simulation.SimulationNum, simulation.ChainPatch,
				simulation.UpstreamTxList)
		}
		tVsLockState = time.Since(tVs0)
		perf.RecordPkg("verify", "VerifySimulation", "lockState", tVsLockState)
		if v.committee.SelfShard == simulation.OriginShardId && v.committee.IsLeader(simulation.Epochs[v.committee.SelfShard]) {
			v.stats.setCxtStage(txHash, 4)
		}
		// [vsCommit] 记录 VerifySimulation 成功的时间点（含 blockNum），用于分析跨 shard 时间差
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
			Uint32("shardId", v.committee.SelfShard).
			Uint32("originShard", simulation.OriginShardId).
			Int("simulationNum", simulation.SimulationNum).
			Uint64("blockNum", header.NumberU64()).
			Msg("[vsCommit] VerifySimulation success")
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
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
			Msgf("send cxt commit vote (async)")
		go v.communicator.SendCommitVote(v.committee.SelfShard, vote)
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
	go v.communicator.SendCommitVote(v.committee.SelfShard, vote)
}

// callForRetry 通过 retryScheduler 调度下一轮链下重试
func (v *Verifier) callForRetry(txHash common.Hash, simulation *api.CXTSimulation) {
	if !v.committee.IsLeader(simulation.Epochs[v.committee.SelfShard]) {
		return
	}
	v.stats.setCxtStage(txHash, 4)
	go v.retrySchd.CallForRetry(&api.RetryTx{
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

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Str("callIndex", callIndex.ToString()).
		Msgf("verify call state %s for shard %d", callState.CallIndex.ToString(), v.committee.SelfShard)

	sender := vm.AccountRef(origin)
	vmCtx := core.NewSSCVMContext(origin, txHash, callIndex, gasPrice, header, v.bc, nil)
	sscvm := vm.NewSSCVM(vmCtx, stateDB, chainConfig, *vmConfig, v.vmService, vm.ExecutionVerify)

	tCall0 := time.Now()
	var ret []byte
	var err error
	ret, _, err = sscvm.Call(sender, addr, input, gas, value)
	tCall := time.Since(tCall0)
	execCtx := v.loadSubCtx(txHash, callIndex)
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Dur("call", tCall).
		Str("addr", addr.Hex()).
		Int("inputLen", len(input)).
		Int("simuStateCount", execCtx.simuStateCount).
		Dur("simuStateDur", execCtx.simuStateDur).
		Msg("verifyExecuteForCallState: sscvm.Call timing")
	if err != nil {
		utils.SSCLogger().Error().Err(err).Str("txHash", txHash.Hex()).Msg("verify callState failed")
		return err
	}

	if bytes.Compare(ret, simuRet) != 0 {
		return fmt.Errorf("simulation result is not equal to execution result, expected: %v, got: %v", simuRet, ret)
	}
	// 从子上下文读取执行结果
	execCtx = v.loadSubCtx(txHash, callIndex)
	if execCtx != nil {
		if !execCtx.currentState.Equal(callState.RWSet.WriteState) {
			return fmt.Errorf("simulation write set is not equal to execution write set, callIndex=%v, pc=%d, expected: %v, got: %v",
				callIndex, execCtx.callFrame.PC,
				callState.RWSet.WriteState, execCtx.currentState)
		}
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
					utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
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
					utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
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

	// 设置子上下文，供 GetResult 读取
	lockCtx := &callVerifyContext{
		currentState:     newStateSet(),
		callFrame:        &api.CallFrame{CallIndex: callIndex, PC: 0},
		dependentResults: callState.DependentResults,
	}
	if root := v.getVerifyContext(txHash); root != nil {
		root.SubCtx.Store(callIndex.ToString(), lockCtx)
		defer root.SubCtx.Delete(callIndex.ToString())
	}

	sender := vm.AccountRef(caller)
	_, leftOverGas, err := sscvm.Call(sender, addr, input, gas, value)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Str("txHash", txHash.Hex()).
			Dur("cost", time.Since(t0)).
			Msgf("lockStateWithExecution: execution failed callIndex=%s, contract=%s", callIndex.ToString(), addr.Hex())
		return err
	}
	_ = leftOverGas
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
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
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
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
func (v *Verifier) SubSimuBalance(txHash common.Hash, callIndex api.CallIndex, address common.Address, amount *big.Int) error {
	// 优先查子上下文（并行验证阶段）
	if subCtx := v.loadSubCtx(txHash, callIndex); subCtx != nil {
		if subCtx.currentState.Balance == nil {
			subCtx.currentState.Balance = make(map[common.Address]*big.Int)
		}
		if subCtx.currentState.Balance[address] == nil {
			root := v.getVerifyContext(txHash)
			if root == nil {
				return api.ErrInvalidExecution
			}
			callStateMap := root.CallStateMap[callIndex.ToString()]
			if callStateMap == nil || callStateMap.RWSet == nil {
				return api.ErrInvalidExecution
			}
			balance, exists := callStateMap.RWSet.ReadState.Balance[address]
			if !exists {
				return api.ErrInvalidExecution
			}
			subCtx.currentState.Balance[address] = balance
		}
		subCtx.currentState.Balance[address].Sub(subCtx.currentState.Balance[address], amount)
		return nil
	}

	return api.ErrInvalidExecution
}

// AddSimuBalance 在验证阶段增加余额。
func (v *Verifier) AddSimuBalance(txHash common.Hash, callIndex api.CallIndex, address common.Address, balance *big.Int) error {
	// 优先查子上下文（并行验证阶段）
	if subCtx := v.loadSubCtx(txHash, callIndex); subCtx != nil {
		if subCtx.currentState.Balance == nil {
			subCtx.currentState.Balance = make(map[common.Address]*big.Int)
		}
		if subCtx.currentState.Balance[address] == nil {
			root := v.getVerifyContext(txHash)
			if root == nil {
				return api.ErrInvalidExecution
			}
			callStateMap := root.CallStateMap[callIndex.ToString()]
			if callStateMap == nil || callStateMap.RWSet == nil {
				return api.ErrInvalidExecution
			}
			bal, exists := callStateMap.RWSet.ReadState.Balance[address]
			if !exists {
				return api.ErrInvalidExecution
			}
			subCtx.currentState.Balance[address] = bal
		}
		subCtx.currentState.Balance[address].Add(subCtx.currentState.Balance[address], balance)
		return nil
	}

	return api.ErrInvalidExecution
}

// GetSimuBalance 获取验证阶段余额。
func (v *Verifier) GetSimuBalance(txHash common.Hash, callIndex api.CallIndex, address common.Address) (*big.Int, error) {
	// 优先查子上下文（并行验证阶段）
	if subCtx := v.loadSubCtx(txHash, callIndex); subCtx != nil {
		if subCtx.currentState.Balance == nil {
			subCtx.currentState.Balance = make(map[common.Address]*big.Int)
		}
		if subCtx.currentState.Balance[address] == nil {
			root := v.getVerifyContext(txHash)
			if root == nil {
				return nil, api.ErrInvalidExecution
			}
			callStateMap := root.CallStateMap[callIndex.ToString()]
			if callStateMap == nil || callStateMap.RWSet == nil {
				return nil, api.ErrInvalidExecution
			}
			bal, exists := callStateMap.RWSet.ReadState.Balance[address]
			if !exists {
				return nil, api.ErrInvalidExecution
			}
			subCtx.currentState.Balance[address] = bal
		}
		return subCtx.currentState.Balance[address], nil
	}

	return nil, api.ErrInvalidExecution
}

// GetSimuState 获取验证阶段状态值。
func (v *Verifier) GetSimuState(txHash common.Hash, callIndex api.CallIndex, address common.Address, key common.Hash) (common.Hash, error) {
	// 优先查子上下文（并行验证阶段）
	if subCtx := v.loadSubCtx(txHash, callIndex); subCtx != nil {
		if subCtx.currentState.State[address] != nil {
			return subCtx.currentState.State[address][key], nil
		}
		// 子上下文中没有，去根上下文的 ReadState 查初始值
		root := v.getVerifyContext(txHash)
		if root == nil {
			return common.Hash{}, api.ErrInvalidExecution
		}
		state, exists := root.CallStateMap[callIndex.ToString()].RWSet.ReadState.State[address]
		if !exists {
			return common.Hash{}, api.ErrInvalidExecution
		}
		return state[key], nil
	}

	return common.Hash{}, api.ErrInvalidExecution
}

// SetSimuState 设置验证阶段状态值。
func (v *Verifier) SetSimuState(txHash common.Hash, callIndex api.CallIndex, address common.Address, key common.Hash, value common.Hash) error {
	// 优先查子上下文（并行验证阶段）
	if subCtx := v.loadSubCtx(txHash, callIndex); subCtx != nil {
		t0 := time.Now()
		if subCtx.currentState.State[address] == nil {
			subCtx.currentState.State[address] = make(map[common.Hash]common.Hash)
		}
		subCtx.currentState.State[address][key] = value
		subCtx.simuStateCount++
		subCtx.simuStateDur += time.Since(t0)
		return nil
	}

	return api.ErrInvalidExecution
}
