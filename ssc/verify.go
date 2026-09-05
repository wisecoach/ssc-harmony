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
	"github.com/harmony-one/harmony/ssc/api/proto"
	"github.com/harmony-one/harmony/ssc/perf"
	"google.golang.org/protobuf/proto"
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

// MulticastRollbackProof 由 origin 主动发送 Rollback 证明到所有相关分片，
// 释放某笔交易已 Commit 的全部链上锁（DSN-52：跨分片锁释放）。
//
// 背景：跨分片交易的锁是「先 Commit、后确认」——某个分片验证成功即上锁，但整笔交易
// 需要所有 related shard 验证通过才发 CRTx。若任一 shard（含 origin）验证失败，其它
// 已上锁 shard 的锁会变成孤儿（LOCK_STALE 级联死锁）。此方法让 origin 在自身验证
// 失败时主动广播 Rollback 证明，各分片通过 CommitOrRollbackWithProof 释放该交易锁。
func (vc *VerifyCommunicator) MulticastRollbackProof(simulation *api.CXTSimulation, reason api.CXTCommitReason) {
	if simulation == nil {
		return
	}
	proof := &api.CXTCommitProof{
		TxHash:        simulation.TxHash,
		SimulationNum: simulation.SimulationNum,
		Type:          api.Rollback,
		Reason:        reason,
		OriginShard:   simulation.OriginShardId,
		RelatedShards: simulation.RelatedShards,
		ReleaseOnly:   true, // DSN-52: 只释放锁、不 close 交易
		BaseSSCMessage: api.BaseSSCMessage{
			Epochs: simulation.Epochs,
		},
	}
	if len(proof.RelatedShards) == 0 {
		proof.RelatedShards = []uint32{simulation.OriginShardId}
	}

	members := make([]*api.Member, 0, len(proof.RelatedShards))
	for _, shardId := range proof.RelatedShards {
		leader := vc.committee.GetLeader(proof.Epochs[shardId], shardId)
		if leader == nil {
			utils.SSCLogger().Error().Str("txHash", simulation.TxHash.Hex()).
				Uint32("shardId", shardId).
				Msg("MulticastRollbackProof: leader not found")
			continue
		}
		members = append(members, leader)
	}
	ctx, cancel := context.WithTimeout(vc.ctx, vc.config.CallTimeout)
	defer cancel()
	if err := vc.comm.Multicast(ctx, members, api.Method_HandleCXTCommitProof, proof); err != nil {
		utils.SSCLogger().Error().Str("txHash", simulation.TxHash.Hex()).Err(err).
			Msg("MulticastRollbackProof failed")
		return
	}
	utils.SSCLogger().Warn().Str("txHash", simulation.TxHash.Hex()).
		Uint32("originShard", simulation.OriginShardId).
		Interface("relatedShards", proof.RelatedShards).
		Int("simulationNum", simulation.SimulationNum).
		Msg("MulticastRollbackProof: origin released cross-shard locks (DSN-52)")
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

	// traceSvc — 指向所属 sscService，用于补全 tx block trace 的阶段埋点
	// （StageSimulationTxCommit 等需要写入 sscService.txTraces，而 recordTraceBlock 是 sscService 的方法）
	traceSvc *sscService
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

// Stats 返回 Verifier 各存储的条目数，供监控分析资源释放情况。
func (v *Verifier) Stats() (execVerifyCtx, txLockedSimNum int) {
	if v == nil {
		return 0, 0
	}
	v.executionVerifyContexts.Range(func(_, _ interface{}) bool { execVerifyCtx++; return true })
	v.txLockedSimNum.Range(func(_, _ interface{}) bool { txLockedSimNum++; return true })
	return
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
	simulationProto := &sscpb.CXTSimulation{}
	err := proto.Unmarshal(simulationBytes, simulationProto)
	simulation := sscpb.CXTSimulationFromProto(simulationProto)
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

	// DSN-52 §4.2/§10.6：注册本交易的确定性优先级 + 分片元数据，
	// 供其它交易冲突时比较「谁该让位」、以及高优先级交易定向 Wound 持有者。
	if mgr := v.retrySchd.tempLockView.stateLockManager; mgr != nil {
		mgr.RegisterTxPriority(txHash, api.Priority{
			Nonce:         simulation.Nonce,
			OriginShardID: simulation.OriginShardId,
			TxHash:        txHash,
		})
		mgr.RegisterTxMeta(txHash, simulation.RelatedShards, simulation.Epochs, simulation.SimulationNum)
	}

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

	// 检查是否为链式依赖交易（真正有上游依赖才跳过锁冲突检查）。
	// 只以 `UpstreamTxList`（DAG 上游索引，DSN-56）判定：只有当确实存在上游依赖时才是链式交易，
	// 才跳过锁冲突检查。任何“SimTx 自身写集/其它 patch”都不能作为 chain 依据
	// （历史 BUG：曾把“每笔 SimTx 都带自己 WriteSet”误判为链式，导致锁检测整体失效）。
	isChainTx := len(simulation.UpstreamTxList) > 0
	if isChainTx {
		chainRetryStats.SigChainTxDetected.Add(1)
		chainRetryStats.MonitorChainTxDetected.Add(1)
		// 标记该 tx 为链式救起（isChainTx），供 CR 最终 commit 时统计“因 DAG 提前完成”。
		if v.retrySchd != nil {
			v.retrySchd.markChainTx(txHash)
		}
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Int("simNum", simulation.SimulationNum).
			Int("upstreamCount", len(simulation.UpstreamTxList)).
			Msg("VerifySimulation: chain tx detected (from upstream dependency), skipping lock conflict check")

		// DSN-57 就绪判定（确定性化）：verify 只依据**链上共识事实** onChainDAGPatches——
		// 上游 SimTx 的 patch 是否已在本分片 onChainDAGPatches 注册（即已上链验证成功）。
		// 若依赖的上游 patch 尚未上链，则本下游 SimTx 属时序违背，直接回滚。
		//
		// ⚠️ verify 由所有 validator 执行，必须对同一输入得出同一结论（否则各 validator
		// verdict 分裂，委员会永远凑不齐 vote/SSC vote，导致已 commit 的兄弟分片锁成孤儿）。
		// 因此这里**不得**依赖 leader 才拥有的内部状态（internal_pool 排队、offChainDAG
		// 内存节点）来做 fail-open/fail-closed 区分。让下游 SimTx 排在上游 SimTx 之后、
		// 保证“下游上链时其上游必已上链”，是 internal_pool 在放行/就绪门（上游已上链才放行）
		// 应完成的任务，不是 verify 的职责。
		if v.retrySchd != nil {
			for _, up := range simulation.UpstreamTxList {
				if v.retrySchd.upstreamOnChain(up.TxHash) {
					continue // 上游 patch 已在本分片 on-chain 注册 → 就绪
				}
				// 上游 patch 不在链上(onChainDAGPatches) → 时序违背，判不满足并回滚。
				chainRetryStats.SigUpstreamNotReady.Add(1)
				utils.SSCLogger().Warn().Str("txHash", txHash.Hex()).
					Str("upstreamTx", up.TxHash.Hex()).
					Int("upstreamSimNum", up.SimulationNum).
					Str("shard", fmt.Sprintf("%d", v.committee.SelfShard)).
					Msg("VerifySimulation: upstream patch not on-chain yet -> rollback (DSN-57, deterministic)")
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
		}
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
						expectedVal, found := v.retrySchd.ReadOnChainDAGPatch(
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
		// v3（CMH 完全替换 Wait-Die）：链上锁冲突**不再按优先级主动 die**。
		// Wait-Die 的 die 侧已弃用——低优先/高优先在链上冲突时都不杀，
		// 一律：记 waitEdge(方向不限) → 由 CMH 检测环 → 只对环内最低优先 victim 终局 die。
		// 只有 CMH 判出的环内 victim 才走 sendRollbackVoteForDie（复用投票路径，见 RollbackVictim）。
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Int("conflictKeys", len(conflictLockKeys)).
			Msg("DSN-53: chain-lock conflict, recording waitEdge for CMH (Wait-Die die disabled)")

		// DSN-53 兜底触发：链上锁冲突一律记 waitEdge(方向不限)，由 CMH 判环。
		if !v.committee.IsLeader(simulation.Epochs[v.committee.SelfShard]) {
			// 非 leader 不维护跨分片 waitEdges。
		} else if v.retrySchd != nil && v.retrySchd.deadlockDetector != nil && v.retrySchd.tempLockView != nil {
			tlv := v.retrySchd.tempLockView
			mgr := tlv.stateLockManager
			// 统一持有者查询：global 写 → global 读 → pending(同块) → TLV。
			// 冲突(尤其日志误标 "[TempLockView] locked by tx")实际可能来自同块 pending 持有者，
			// 只查 global 会记到无关持有者 → 记错边。这里按真实阻塞层找 holder。
			var db *corestate.DB
			if sdb, ok := stateDB.(*corestate.DB); ok {
				db = sdb
			}
			cur := api.Priority{Nonce: simulation.Nonce, OriginShardID: simulation.OriginShardId, TxHash: simulation.TxHash}
			for _, key := range conflictLockKeys {
				holder, layer, holderPri := common.Hash{}, api.LockLayerOnChain, api.Priority{}
				found := false
				if mgr != nil {
					if meta, ok := mgr.GetLockHolderMeta(key); ok && meta.TxHash != simulation.TxHash && meta.TxHash != (common.Hash{}) {
						holder, layer, holderPri, found = meta.TxHash, api.LockLayerOnChain, meta.Priority, true
					} else if h, ok := mgr.GetRLockHolder(key); ok && h != simulation.TxHash && h != (common.Hash{}) {
						hp, _ := mgr.GetTxPriority(h)
						holder, layer, holderPri, found = h, api.LockLayerOnChain, hp, true
					}
				}
				if !found && db != nil {
					if h, ok := db.FindPendingLockHolder(key); ok && h != simulation.TxHash && h != (common.Hash{}) {
						if mgr != nil {
							holderPri, _ = mgr.GetTxPriority(h)
						}
						holder, layer, found = h, api.LockLayerOnChain, true
					}
				}
				// v2：waitEdge 只来自真实链上锁(global+pending)。TLV 预约不作 waitEdge 来源，
				// 故不再回退 GetTempLockHolder。
				if found {
					_ = v.retrySchd.deadlockDetector.OnBlockedAt(simulation.TxHash, holder, v.committee.SelfShard, key,
						layer, cur, holderPri, simulation.Epochs[v.committee.SelfShard], header.NumberU64())
				}
			}
		}
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

		// 验证通过后把本 SimTx 导入 onChainDAGPatches（自身 WriteSet 由 CallStates 推导，不依赖扁平 ChainPatch），
		// 供下游链式交易按其 UpstreamTxList 反查。
		// 注意：SimTx 的自身 WriteSet 在“正常成功验证的本 SimTx”上非空；若为空（异常/无写集）也照常导入节点，
		// 仅带上游索引，供下游发现依赖关系。
		v.retrySchd.AddOnChainDAGPatch(txHash, simulation.SimulationNum, simOwnWriteSet(simulation),
			simulation.UpstreamTxList)
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
		// 补全 tx block trace：SimulationTxCommit（SimTx 上链执行 VerifySimulation）阶段
		// 此前该阶段从未记录，导致 [txLife] 的 submitToCommit / commitToCRSubmit 恒为 0，
		// 无法看到 SimTx 在交易池排队的时间（BUG-13 §6.1 定位的主瓶颈段）。
		if v.traceSvc != nil {
			v.traceSvc.recordTraceBlock(txHash, StageSimulationTxCommit, header.NumberU64())
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

// dsn52ShouldYield 判断当前交易在链上锁冲突时是否应「让位」（低优先级）。
//
// ⚠️ 已弃用（不再调用）：Wait-Die 完全弃用，链上冲突不再按优先级主动 die；
// 唯一 die 机制是 CMH 环 victim。本函数仅保留供对照/历史参考。
//
// DSN-52 §4.2：优先级 `Nonce > OriginShardID > TxHash` 是确定性全序，所有分片判定一致。
//   - 若任一冲突 key 的持有者优先级比当前交易高 → 当前交易让位（释放锁 + 回重试池）。
//   - 若持有者优先级不可知（保守）→ 让位，避免错误保留锁阻塞更高优先级交易。
//   - 若当前交易比所有冲突持有者都高 → 不让位（保留自身锁，等低优先级释放后胜出）。
func (v *Verifier) dsn52ShouldYield(simulation *api.CXTSimulation, conflictLockKeys []api.LockKey, stateDB api.StateDB) bool {
	if simulation == nil {
		return true
	}
	mgr := v.retrySchd.tempLockView.stateLockManager
	if mgr == nil {
		return true
	}
	cur := api.Priority{
		Nonce:         simulation.Nonce,
		OriginShardID: simulation.OriginShardId,
		TxHash:        simulation.TxHash,
	}
	var db *corestate.DB
	if sdb, ok := stateDB.(*corestate.DB); ok {
		db = sdb
	}
	for _, key := range conflictLockKeys {
		// 持有者可能来自 globalLockedStates（跨块）或 pendingStates（同块）。
		// 之前只看 global，导致同块 pending 冲突被误判（看不到持有者 → 不进让位分支）。
		holder, ok := mgr.GetLockHolder(key)
		if !ok && db != nil {
			if h, found := db.FindPendingLockHolder(key); found {
				holder, ok = h, true
			}
		}
		if !ok {
			continue
		}
		holderPri, ok := mgr.GetTxPriority(holder)
		if !ok {
			// 持有者优先级未知 → 保守让位
			return true
		}
		// holderPri.Less(cur) 表示持有者优先级更高 → 当前交易让位
		if holderPri.Less(cur) {
			return true
		}
	}
	return false
}

// woundLowerPriorityHolders 让当前交易 C（已判定优先级高于冲突持有者）主动 Wound 低优先级持有者 H。
//
// ⚠️ 已弃用（不再调用）：DSN-52 已从 Wound-Wait 改为 Wait-Die。
// 链上 wound（主动抢占已投过 commit vote 的持有者）违反投票协议“一票一投、不可重复”的语义，
// 可能导致交易“既被 commit 又被拉回重试”的双重处理。现冲突分支高优先级侧改为“等待（wait）”，
// 不再调用本函数；仅保留代码以便对照/回滚。验证信号：`onChainWound` 应为 0。
//
// 原语义：wound 直接释放 victim 的锁（已在链上，无需走 CRTx）。
//   - 持有者可能来自 globalLockedStates（跨块）或 pendingStates（同块未提交）。
//   - 两种都释放（releaseOrphanWriteLocks 清 global + ReleasePendingLocksFor 清 pending）。
//   - 然后把 victim 拉回重试流：重新上锁由 victim 重新 VerifySimulation 完成。
//
// 仅在以下条件才 wound：H 优先级更低、H 的 Patch 未 Finalized、且 H 不是 C 自己。
// 返回实际触发 wound 的持有者数（用于日志/监控）。
func (v *Verifier) woundLowerPriorityHolders(simulation *api.CXTSimulation, conflictLockKeys []api.LockKey, stateDB api.StateDB) int {
	if simulation == nil {
		return 0
	}
	mgr := v.retrySchd.tempLockView.stateLockManager
	if mgr == nil {
		return 0
	}
	cur := api.Priority{
		Nonce:         simulation.Nonce,
		OriginShardID: simulation.OriginShardId,
		TxHash:        simulation.TxHash,
	}
	isLeader := v.committee.IsLeader(simulation.Epochs[v.committee.SelfShard])
	var db *corestate.DB
	if sdb, ok := stateDB.(*corestate.DB); ok {
		db = sdb
	}
	woundedSet := make(map[common.Hash]struct{})
	count := 0
	for _, key := range conflictLockKeys {
		// 1) 先查 globalLockedStates（跨块）；2) 再查 pendingStates（同块未提交）
		holder, ok := mgr.GetLockHolderMeta(key)
		if !ok && db != nil {
			if h, found := db.FindPendingLockHolder(key); found {
				holder = LockHolderMeta{TxHash: h}
				holder.Priority, _ = mgr.GetTxPriority(h)
				if tm, ok2 := mgr.GetTxMeta(h); ok2 {
					holder.RelatedShards = tm.RelatedShards
					holder.Epochs = tm.Epochs
					holder.SimulationNum = tm.SimulationNum
				}
				if dag := mgr.offChainDAG(); dag != nil {
					holder.Finalized = dag.isPatchFinalized(h)
				}
				ok = true
			}
		}
		if !ok {
			continue
		}
		if holder.TxHash == simulation.TxHash {
			continue // 自己，不 wound
		}
		if holder.Finalized {
			continue // 已实质提交，不可踢（与 TLV canWound 一致）
		}
		if !cur.Less(holder.Priority) {
			// 当前交易并不比该持有者优先级更高（可能是优先级未知/持平）→ 保守不 wound
			continue
		}
		if _, seen := woundedSet[holder.TxHash]; seen {
			continue // 同一持有者只 wound 一次
		}
		woundedSet[holder.TxHash] = struct{}{}
		if isLeader {
			chainRetryStats.SigOnChainWound.Add(1)
			// ① 释放 victim 的锁：globalLockedStates（跨块）+ pendingStates（同块）
			released := releaseOrphanWriteLocks(mgr, holder.TxHash)
			if db != nil {
				released += db.ReleasePendingLocksFor(holder.TxHash)
			}
			// ② 把 victim 拉回重试流：wound 不负责重新上锁，重新上锁由 victim
			//    重新 VerifySimulation 完成（否则 victim 变成孤儿，永不重新验证）。
			v.retrySchd.CallForRetry(&api.RetryTx{
				TxHash:        holder.TxHash,
				Epochs:        holder.Epochs,
				RelatedShards: holder.RelatedShards,
				SimulationNum: holder.SimulationNum + 1,
				Condition:     api.Verify,
				OriginShardID: holder.Priority.OriginShardID,
				Nonce:         holder.Priority.Nonce,
			})
			utils.SSCLogger().Warn().Str("txHash", holder.TxHash.Hex()).
				Int("releasedLocks", released).
				Msg("DSN-52: wounded lower-priority holder, released global+pending locks, pulled back to retry")
		}
		count++
	}
	return count
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

// sendRollbackVoteForDie 发送 Wait-Die die 侧的终局回滚投票（用户决策：不恢复软重试）。
// 当前（低优先级/未知）交易在链上撞到更高优先级持有者 → 彻底回滚判死，释放自己在所有分片的锁。
// 降冲突交给链下/交易池提取策略（internal_pool / OnBlockCommitted），而非软重试。
func (v *Verifier) sendRollbackVoteForDie(txHash common.Hash, simulation *api.CXTSimulation) {
	utils.SSCLogger().Warn().Str("txHash", txHash.Hex()).
		Uint32("originShard", simulation.OriginShardId).
		Msg("DSN-52 Wait-Die: lower priority, sending terminal rollback vote (no retry)")
	payload := &api.CXTInvalidSimulationPayload{Type: api.InvalidExecution}
	payloadBytes, _ := json.Marshal(payload)
	vote := &api.CXTCommitVote{
		TxHash:         txHash,
		ShardId:        v.committee.SelfShard,
		Type:           api.Rollback,
		OriginShardId:  simulation.OriginShardId,
		Reason:         api.ReasonConflictRWSetFailedLock,
		Payload:        payloadBytes,
		BaseSSCMessage: api.BaseSSCMessage{Epochs: simulation.Epochs},
	}
	go v.communicator.SendCommitVote(v.committee.SelfShard, vote)
}

// RollbackVictim 供死锁检测器触发环内最低优先交易的终局 Rollback（复用 sendRollbackVoteForDie）。
func (v *Verifier) RollbackVictim(txHash common.Hash, epochs []api.Epoch, originShardId uint32, simulationNum int) {
	v.sendRollbackVoteForDie(txHash, &api.CXTSimulation{
		OriginShardId: originShardId,
		SimulationNum: simulationNum,
		BaseBLSSignedMessage: api.BaseBLSSignedMessage{
			Epochs: epochs,
		},
	})
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
		Nonce:         simulation.Nonce,
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
