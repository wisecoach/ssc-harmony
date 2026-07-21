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

// verifyContextKey 是 executionVerifyContexts sync.Map 的 key。
// 复合 (txHash, simNum) 使不同 SimulationNum 的 SimTx 各有独立 VerifyContext。
type verifyContextKey struct {
	txHash common.Hash
	simNum int
}

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
}

// loadSubCtx 从根上下文的内嵌 subCtx 中加载子验证上下文。
func (v *Verifier) loadSubCtx(txHash common.Hash, simNum int, callIndex api.CallIndex) *callVerifyContext {
	root := v.getVerifyContext(txHash, simNum)
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
func (v *Verifier) storeSubCtx(txHash common.Hash, simNum int, callIndex api.CallIndex, ctx *callVerifyContext) {
	root := v.getVerifyContext(txHash, simNum)
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
	executionVerifyContexts sync.Map // key: verifyContextKey → *api.ExecutionVerifyContext（含内嵌 subCtx）
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

func (v *Verifier) StoreVerifyContext(txHash common.Hash, simNum int, ctx *api.ExecutionVerifyContext) {
	v.executionVerifyContexts.Store(verifyContextKey{txHash: txHash, simNum: simNum}, ctx)
}

func (v *Verifier) getVerifyContext(txHash common.Hash, simNum int) *api.ExecutionVerifyContext {
	if val, ok := v.executionVerifyContexts.Load(verifyContextKey{txHash: txHash, simNum: simNum}); ok {
		return val.(*api.ExecutionVerifyContext)
	}
	return nil
}

// HasVerifyContext 判断某笔 tx 的某个 simulationNum 是否存在验证上下文。
func (v *Verifier) HasVerifyContext(txHash common.Hash, simNum int) bool {
	_, exists := v.executionVerifyContexts.Load(verifyContextKey{txHash: txHash, simNum: simNum})
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

// Cleanup 清理某个 tx 的指定 simulationNum 的验证上下文。
// 供 sscService.closeTransaction 调用。
func (v *Verifier) Cleanup(txHash common.Hash, simNum int) {
	t0 := time.Now()
	v.executionVerifyContexts.Delete(verifyContextKey{txHash: txHash, simNum: simNum})
	v.txLockedSimNum.Delete(txHash)
	// 子上下文随根上下文一起清理
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Dur("cost", time.Since(t0)).
		Msg("Verifier.Cleanup timing")
}

// GetResult 读取验证结果。实现 api.Service 中的 CXTStateSimulationDB 接口。
func (v *Verifier) GetResult(txHash common.Hash, simNum int, callIndex api.CallIndex) (result []byte, leftOverGas uint64, err error) {
	subCtx := v.loadSubCtx(txHash, simNum, callIndex)
	if subCtx == nil {
		return nil, 0, api.ErrInvalidExecution
	}
	if len(subCtx.dependentResults) <= subCtx.callFrame.PC {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
			Int("pc", subCtx.callFrame.PC).
			Int("dependentResultsLength", len(subCtx.dependentResults)).
			Str("callIndex", callIndex.ToString()).
			Msg("get result failed, index out of range")
		return nil, 0, api.ErrInvalidExecution
	}
	ret := subCtx.dependentResults[subCtx.callFrame.PC]
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
		Int("pc", subCtx.callFrame.PC).
		Int("simNum", simNum).
		Int("dependentResultsLength", len(subCtx.dependentResults)).
		Str("callIndex", callIndex.ToString()).
		Msg("GetResult: read ok")
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
	v.verifySimulationParsed(simulation, simulationBytes, stateDB, header)
}

func (v *Verifier) verifySimulationParsed(simulation *api.CXTSimulation, simulationBytes []byte, stateDB api.StateDB, header *block.Header) {
	txHash := simulation.TxHash

	tVs0 := time.Now()
	var tVsPatch, tVsLockCheck, tVsExec, tVsLockState time.Duration

	// v6: 所有节点收到 SimTx 后，将 ChainPatch 存入 onChainPatches
	if simulation.ChainPatch != nil {
		v.retrySchd.AddOnChainPatch(txHash, simulation.SimulationNum, simulation.ChainPatch,
			simulation.UpstreamTxList)
	}

	if v.committee.SelfShard == simulation.OriginShardId {
		v.timerMgr.StartSp1Timer(txHash, simulation.SimulationNum, simulation.Epochs, header.NumberU64(), v.committee.SelfShard)
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
	v.StoreVerifyContext(txHash, simulation.SimulationNum, verifyContext)

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
				currentState:     newStateSetFromRead(callState.RWSet.ReadState),
				callFrame:        &api.CallFrame{CallIndex: callIndex, PC: 0},
				dependentResults: callState.DependentResults,
			}
			v.storeSubCtx(txHash, simulation.SimulationNum, callIndex, subCtx)
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
			err := v.lockStateWithExecution(simulation.Epochs, simulation.SimulationNum, simulation.CallStates[conflictCallStateIndex], stateDB.(*corestate.DB))
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
		tVsLockState = time.Since(tVs0)
		if v.committee.SelfShard == simulation.OriginShardId && v.committee.IsLeader(simulation.Epochs[v.committee.SelfShard]) {
			v.stats.setCxtStage(txHash, 4)
		}
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
	verifyContext := v.getVerifyContext(txHash, simulation.SimulationNum)
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
	vmCtx := core.NewSSCVMContext(origin, txHash, simulation.SimulationNum, callIndex, gasPrice, header, v.bc, nil)
	sscvm := vm.NewSSCVM(vmCtx, stateDB, chainConfig, *vmConfig, v.vmService, vm.ExecutionVerify)

	var ret []byte
	var err error
	ret, _, err = sscvm.Call(sender, addr, input, gas, value)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Str("txHash", txHash.Hex()).Msg("verify callState failed")
		return err
	}

	if bytes.Compare(ret, simuRet) != 0 {
		return fmt.Errorf("simulation result is not equal to execution result, expected: %v, got: %v", simuRet, ret)
	}
	// 从子上下文读取执行结果
	execCtx := v.loadSubCtx(txHash, simulation.SimulationNum, callIndex)
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
func (v *Verifier) lockStateWithExecution(epochs []api.Epoch, simNum int, callState *api.CXTCallState, state *corestate.DB) error {
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
	vmCtx := core.NewSSCVMContext(caller, txHash, simNum, callIndex, gasPrice, header, v.bc, nil)
	chainConfig := v.bc.Config()
	vmConfig := v.bc.GetVMConfig()
	sscvm := vm.NewSSCVM(vmCtx, state, chainConfig, *vmConfig, v.vmService, vm.ExecutionVerify)

	// 设置子上下文，供 GetResult 读取
	lockCtx := &callVerifyContext{
		currentState:     newStateSetFromRead(callState.RWSet.ReadState),
		callFrame:        &api.CallFrame{CallIndex: callIndex, PC: 0},
		dependentResults: callState.DependentResults,
	}
	if root := v.getVerifyContext(txHash, simNum); root != nil {
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
func (v *Verifier) SubSimuBalance(txHash common.Hash, simNum int, callIndex api.CallIndex, address common.Address, amount *big.Int) error {
	// 优先查子上下文（并行验证阶段）
	if subCtx := v.loadSubCtx(txHash, simNum, callIndex); subCtx != nil {
		if subCtx.currentState.Balance == nil {
			subCtx.currentState.Balance = make(map[common.Address]*big.Int)
		}
		if subCtx.currentState.Balance[address] == nil {
			root := v.getVerifyContext(txHash, simNum)
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
func (v *Verifier) AddSimuBalance(txHash common.Hash, simNum int, callIndex api.CallIndex, address common.Address, balance *big.Int) error {
	// 优先查子上下文（并行验证阶段）
	if subCtx := v.loadSubCtx(txHash, simNum, callIndex); subCtx != nil {
		if subCtx.currentState.Balance == nil {
			subCtx.currentState.Balance = make(map[common.Address]*big.Int)
		}
		if subCtx.currentState.Balance[address] == nil {
			root := v.getVerifyContext(txHash, simNum)
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
func (v *Verifier) GetSimuBalance(txHash common.Hash, simNum int, callIndex api.CallIndex, address common.Address) (*big.Int, error) {
	// 优先查子上下文（并行验证阶段）
	if subCtx := v.loadSubCtx(txHash, simNum, callIndex); subCtx != nil {
		if subCtx.currentState.Balance == nil {
			subCtx.currentState.Balance = make(map[common.Address]*big.Int)
		}
		if subCtx.currentState.Balance[address] == nil {
			root := v.getVerifyContext(txHash, simNum)
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
func (v *Verifier) GetSimuState(txHash common.Hash, simNum int, callIndex api.CallIndex, address common.Address, key common.Hash) (common.Hash, error) {
	// 优先查子上下文（并行验证阶段）
	if subCtx := v.loadSubCtx(txHash, simNum, callIndex); subCtx != nil {
		if subCtx.currentState.State[address] != nil {
			return subCtx.currentState.State[address][key], nil
		}
		// 子上下文中没有，去根上下文的 ReadState 查初始值
		root := v.getVerifyContext(txHash, simNum)
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
func (v *Verifier) SetSimuState(txHash common.Hash, simNum int, callIndex api.CallIndex, address common.Address, key common.Hash, value common.Hash) error {
	// 优先查子上下文（并行验证阶段）
	if subCtx := v.loadSubCtx(txHash, simNum, callIndex); subCtx != nil {
		if subCtx.currentState.State[address] == nil {
			subCtx.currentState.State[address] = make(map[common.Hash]common.Hash)
		}
		subCtx.currentState.State[address][key] = value
		return nil
	}

	return api.ErrInvalidExecution
}

// extractRWSet 提取单个 SimTx 的所有读写 key
func extractRWSet(sim *api.CXTSimulation) (writes map[api.LockKey]struct{}, reads map[api.LockKey]struct{}) {
	writes = make(map[api.LockKey]struct{})
	reads = make(map[api.LockKey]struct{})
	for _, cs := range sim.CallStates {
		for addr, sm := range cs.RWSet.WriteState.State {
			for k := range sm {
				writes[api.FormKey(addr, k)] = struct{}{}
			}
		}
		for addr, sm := range cs.RWSet.ReadState.State {
			for k := range sm {
				reads[api.FormKey(addr, k)] = struct{}{}
			}
		}
	}
	return
}

// BatchVerifySimulations 批量并行验证模拟交易。
//
// Phase 0:  提取所有 SimTxs 的 RWSet
// Phase 0.5:按序同块冲突仲裁（写写/读写/全局锁）
//
//	无冲突 → passed（可全并行验证）
//	有冲突 → failed（callForRetry）
func (v *Verifier) BatchVerifySimulations(simulations []api.CXTSimulation, stateDB api.StateDB, header *block.Header) {
	t0 := time.Now()
	n := len(simulations)
	utils.SSCLogger().Info().Int("batchSize", n).Msg("BatchVerifySimulations: start")

	// Phase 0: 提取 RWSet
	writeKeys := make([]map[api.LockKey]struct{}, n)
	readKeys := make([]map[api.LockKey]struct{}, n)
	for i := range simulations {
		w, r := extractRWSet(&simulations[i])
		writeKeys[i] = w
		readKeys[i] = r
	}

	// Phase 0.5: 同块冲突仲裁（串行，按 batch 顺序）
	committedWrites := make(map[api.LockKey]struct{})
	passed := make([]int, 0, n)
	failed := make([]int, 0, n)

SimLoop:
	for i := range simulations {
		sim := &simulations[i]

		// 0.5a: 写集 vs 已提交写集（同块写写冲突）
		for key := range writeKeys[i] {
			if _, exists := committedWrites[key]; exists {
				utils.SSCLogger().Debug().Str("txHash", sim.TxHash.Hex()).
					Str("key", string(key)).Msg("BatchVerify: intra-block write-write conflict")
				v.callForRetry(sim.TxHash, sim)
				failed = append(failed, i)
				continue SimLoop
			}
		}

		// 0.5b: 写集 vs 全局锁
		for key := range writeKeys[i] {
			if err := stateDB.CheckLock(key, sim.TxHash); err != nil {
				utils.SSCLogger().Debug().Str("txHash", sim.TxHash.Hex()).
					Str("key", string(key)).Err(err).Msg("BatchVerify: global lock conflict")
				v.callForRetry(sim.TxHash, sim)
				failed = append(failed, i)
				continue SimLoop
			}
		}

		// 0.5c: 读集 vs 已提交写集（同块读写冲突）
		for key := range readKeys[i] {
			if _, exists := committedWrites[key]; exists {
				utils.SSCLogger().Debug().Str("txHash", sim.TxHash.Hex()).
					Str("key", string(key)).Msg("BatchVerify: intra-block read-write conflict")
				v.callForRetry(sim.TxHash, sim)
				failed = append(failed, i)
				continue SimLoop
			}
		}

		// 无冲突：提交写集到已提交集合，加入 passed
		for key := range writeKeys[i] {
			committedWrites[key] = struct{}{}
		}
		passed = append(passed, i)
	}

	utils.SSCLogger().Info().Int("passed", len(passed)).Int("failed", len(failed)).
		Dur("phase0.5", time.Since(t0)).Msg("BatchVerify: conflict arbitration done")

	// 批量相位验证 passed SimTxs
	v.batchVerifyPassed(simulations, passed, stateDB, header)

	utils.SSCLogger().Info().Int("passed", len(passed)).Int("failed", len(failed)).
		Dur("total", time.Since(t0)).Msg("BatchVerifySimulations: done")
}

// batchVerifyPassed 对 passed 列表执行批量相位验证。
// Setup → lockCheck → subCtx → Copy → execVerify → lockState → cleanup
//
// 冲突和 exec 失败的 SimTxs 逐笔处理（不污染通过的 SimTxs）。
func (v *Verifier) batchVerifyPassed(simulations []api.CXTSimulation, passed []int, stateDB api.StateDB, header *block.Header) {
	t0 := time.Now()
	n := len(passed)
	if n == 0 {
		return
	}

	// 收集所有 passed SimTxs 的 CallStates
	type callStateRef struct {
		simIdx    int // index into simulations
		csIdx     int // index into simulation.CallStates
		callState *api.CXTCallState
	}
	var allCS []callStateRef
	for _, idx := range passed {
		sim := &simulations[idx]
		for ci, cs := range sim.CallStates {
			allCS = append(allCS, callStateRef{simIdx: idx, csIdx: ci, callState: cs})
		}
	}
	totalCS := len(allCS)

	// Setup: 每个 passed SimTx 的初始化（链式补丁、定时器、上下文）
	for _, idx := range passed {
		sim := &simulations[idx]
		txHash := sim.TxHash

		// 链式补丁
		if sim.ChainPatch != nil {
			v.retrySchd.AddOnChainPatch(txHash, sim.SimulationNum, sim.ChainPatch, sim.UpstreamTxList)
		}
		// Sp1 定时器
		if v.committee.SelfShard == sim.OriginShardId {
			v.timerMgr.StartSp1Timer(txHash, sim.SimulationNum, sim.Epochs, header.NumberU64(), v.committee.SelfShard)
		}
		// 状态
		v.state.SetStatus(txHash, api.VERIFYING_SIMULATION)
		v.getOrCreateLockedSimNum(txHash, sim.SimulationNum)

		if v.committee.SelfShard == sim.OriginShardId && v.committee.IsLeader(sim.Epochs[v.committee.SelfShard]) {
			v.stats.setCxtStage(txHash, 3)
		}

		// 验证上下文
		callStateMap := make(map[string]*api.CXTCallState)
		for _, callState := range sim.CallStates {
			callStateMap[callState.CallIndex.ToString()] = callState
		}
		verifyContext := &api.ExecutionVerifyContext{Simulation: &simulations[idx], CallStateMap: callStateMap}
		v.StoreVerifyContext(txHash, sim.SimulationNum, verifyContext)
	}

	// Phase 1: 跨 SimTx 并行 lockCheck — 逐 SimTx 追踪冲突
	var (
		lockWg         sync.WaitGroup
		conflictSimsMu sync.Mutex
		conflictSet    = make(map[int]struct{}) // 有冲突的 simIdx
	)
	for _, ref := range allCS {
		lockWg.Add(1)
		go func(r callStateRef) {
			defer lockWg.Done()
			txHash := simulations[r.simIdx].TxHash
			for address, sm := range r.callState.RWSet.WriteState.State {
				for key := range sm {
					lockKey := api.FormKey(address, key)
					if err := stateDB.CheckLock(lockKey, txHash); err != nil {
						conflictSimsMu.Lock()
						conflictSet[r.simIdx] = struct{}{}
						conflictSimsMu.Unlock()
						return
					}
				}
			}
		}(ref)
	}
	lockWg.Wait()
	tVsLockBatch := time.Since(t0)

	// 过滤掉冲突 SimTxs 的 CallStates
	var passedCS []callStateRef
	for _, ref := range allCS {
		if _, isConflict := conflictSet[ref.simIdx]; !isConflict {
			passedCS = append(passedCS, ref)
		}
	}

	// Phase 1.5: 子上下文预分配（仅对无冲突的 CallStates）
	for _, ref := range passedCS {
		sim := &simulations[ref.simIdx]
		txHash := sim.TxHash
		callIndex := ref.callState.CallIndex
		cs := ref.callState

		subCtx := &callVerifyContext{
			currentState:     newStateSetFromRead(cs.RWSet.ReadState),
			callFrame:        &api.CallFrame{CallIndex: callIndex, PC: 0},
			dependentResults: cs.DependentResults,
		}
		v.storeSubCtx(txHash, simulations[ref.simIdx].SimulationNum, callIndex, subCtx)
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
			Str("callIndex", callIndex.ToString()).
			Int("depLen", len(cs.DependentResults)).
			Msg("BatchVerify: subCtx allocated with dependentResults")
	}

	// Phase 2: stateDB.Copy() × totalCallStates（仅无冲突的）
	tCopy0 := time.Now()
	db := stateDB.(*corestate.DB)
	copies := make([]*corestate.DB, len(passedCS))
	for i := 0; i < len(passedCS); i++ {
		copies[i] = db.Copy()
	}
	utils.SSCLogger().Info().Int("copies", len(passedCS)).
		Dur("cost", time.Since(tCopy0)).
		Msg("BatchVerify: stateDB.Copy timing")

	// Phase 3: 全并行 execVerify — 逐笔追踪执行失败
	var (
		execWg      sync.WaitGroup
		execMu      sync.Mutex
		execFailMu  sync.Mutex
		execFailSet = make(map[int]struct{}) // exec 失败的 simIdx
	)
	type execResult struct {
		csRef int
		err   error
	}
	execResults := make([]execResult, len(passedCS))

	for i, ref := range passedCS {
		execWg.Add(1)
		go func(csIdx int, r callStateRef) {
			defer execWg.Done()
			sim := &simulations[r.simIdx]
			err := v.verifyExecuteForCallState(sim, sim.TxHash, r.callState, copies[csIdx])
			execMu.Lock()
			execResults[csIdx] = execResult{csRef: csIdx, err: err}
			execMu.Unlock()
			if err != nil {
				execFailMu.Lock()
				execFailSet[r.simIdx] = struct{}{}
				execFailMu.Unlock()
			}
		}(i, ref)
	}
	execWg.Wait()

	// Phase 4: 串行 lockStateWithRWSet（仅无冲突且无 exec 失败的 CallStates）
	for _, ref := range passedCS {
		_, isConflict := conflictSet[ref.simIdx]
		_, isExecFail := execFailSet[ref.simIdx]
		if isConflict || isExecFail {
			continue
		}
		v.lockStateWithRWSet(simulations[ref.simIdx].TxHash, ref.callState, stateDB)
	}

	// Phase 4.5: 冲突/失败的 SimTxs 发 retry
	for _, idx := range passed {
		sim := &simulations[idx]
		txHash := sim.TxHash

		if _, isConflict := conflictSet[idx]; isConflict {
			utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
				Msg("BatchVerify: lock conflict, retry")
			stateDB.RollbackTx(txHash)
			v.callForRetry(txHash, sim)
		} else if _, isExecFail := execFailSet[idx]; isExecFail {
			utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
				Msg("BatchVerify: exec failed, rollback")
			stateDB.RollbackTx(txHash)
			v.sendRollbackVoteForRetry(txHash, sim.SimulationNum, sim.Epochs, sim.OriginShardId, 0)
		}
	}

	// Phase 5: cleanup — 成功的 SimTxs 发 commit vote
	for _, idx := range passed {
		sim := &simulations[idx]
		txHash := sim.TxHash

		if _, isConflict := conflictSet[idx]; isConflict {
			continue
		}
		if _, isExecFail := execFailSet[idx]; isExecFail {
			continue
		}

		// 成功路径
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Uint32("FromShard", sim.OriginShardId).
			Msg("simulation is valid, mu the rwset and send commit vote")
		if v.committee.SelfShard == sim.OriginShardId && v.committee.IsLeader(sim.Epochs[v.committee.SelfShard]) {
			v.stats.setCxtStage(txHash, 4)
		}
		vote := &api.CXTCommitVote{
			TxHash:         txHash,
			Type:           api.Commit,
			ShardId:        v.committee.SelfShard,
			OriginShardId:  sim.OriginShardId,
			Payload:        nil,
			BaseSSCMessage: api.BaseSSCMessage{Epochs: sim.Epochs},
		}
		go v.communicator.SendCommitVote(v.committee.SelfShard, vote)
	}
	utils.SSCLogger().Info().Int("passed", n).Int("callStates", totalCS).
		Int("conflict", len(conflictSet)).Int("execFail", len(execFailSet)).
		Dur("lockCheck", tVsLockBatch).Dur("total", time.Since(t0)).
		Msg("BatchVerify: passed phase done")
}

func (v *Verifier) IsParallelBatchEnabled() bool {
	config := v.timerMgr.GetTimeoutConfig()
	return config != nil && config.EnableParallelBatch
}
