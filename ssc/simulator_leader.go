package ssc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/core/vm"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
)

// =============================================================================
// Leader Functions — migrated from sscService (impl.go) to Simulator
// These functions handle the leader's role in cross-shard simulation:
//   - coordinating simulation across committee members
//   - aggregating results
//   - threshold signing
//   - submitting simulation commits
// =============================================================================

// StartSimulateCXTransaction 接收跨分片模拟请求（Leader 节点）。
// Leader 节点收到所有成员的 simulation request 后，协调全部分片执行。
//
// Original: sscService.StartSimulateCXTransaction (4b6d266c7)
// s. → sim.
// s.CommitteeMechanism → sim.committee
// s.ctx → sim.ctx
// s.Config → sim.config
// s.bc → sim.bc
// s.Comm → sim.communicator.comm
// s.signerMgr → sim.communicator.signerMgr
// s.SelfShard → sim.committee.SelfShard
// s.SelfAddr → sim.communicator.signerMgr.GetSSCSigner().Address()
// s.BLSSignerMgr.GetSSCSigner() → sim.communicator.signerMgr.GetSSCSigner()
// s.recordTraceBlock → omitted
// s.startSimulation → sim.state.StartSimulation
// s.stateLock → sim.state.* accessor
// s.getState(txHash) → sim.state.GetTxState(txHash) / sim.GetSimState(txHash)
// s.retryScheduler.CallForRetry → sim.state.CallForRetry
// s.retryScheduler.tempLockView.TryLock → sim.state.TempLockTryLock
// s.IsLeader → sim.committee.IsLeader
// s.GetLeader → sim.committee.GetLeader
// s.simuLock → sim.simuChMap (per-tx sync.Map)
// s.simuWaitingChs → sim.simuChMap + simuChannels
// s.simuResultCh → sim.simuChMap + simuChannels
func (sim *Simulator) StartSimulateCXTransaction(req *api.CXTSimulationRequest) *api.CXTSimulationSSCResult {
	var waitingCh chan *api.CXTSimulationSSCResult
	txHash := req.Tx.Hash()

	t0 := time.Now()
	var tCallMembers, tAggregate, tThresholdSign, tCommitSend time.Duration
	startTime := time.Now()
	utils.SSCLogger().Debug().Int("simulationNum", req.SimulationNum).Str("txHash", txHash.String()).Msg("start simulate cx transaction, start")
	defer func() {
		utils.SSCLogger().Debug().Int("simulationNum", req.SimulationNum).Str("txHash", txHash.String()).Dur("cost", time.Since(startTime)).Msg("start simulate cx transaction, end")
		utils.SSCLogger().Debug().Str("txHash", txHash.String()).
			Str("callMembers", tCallMembers.String()).
			Str("aggregate", tAggregate.String()).
			Str("thresholdSign", tThresholdSign.String()).
			Str("commitSend", tCommitSend.String()).
			Str("total", time.Since(t0).String()).
			Msg("StartSimulateCXTransaction timing breakdown")
	}()

	func() {
		ch := sim.getOrCreateChannels(txHash)
		ch.mu.Lock()
		if len(ch.waitingChs) > 0 || ch.resultCh != nil {
			// 已有等待者或已设置结果通道 → 加入等待队列
			waitingCh = make(chan *api.CXTSimulationSSCResult)
			ch.waitingChs = append(ch.waitingChs, waitingCh)
		}
		// else: 首次，不创建 waitingCh → 该 goroutine 自己跑模拟
		if ch.resultCh == nil {
			ch.resultCh = make(chan *api.CXTSimulationSSCResult, 1)
		}
		ch.mu.Unlock()
	}()

	if waitingCh != nil {
		return <-waitingCh
	}

	defer func() {
		if ch := sim.getChannels(txHash); ch != nil {
			ch.mu.Lock()
			ch.waitingChs = nil
			ch.mu.Unlock()
		}
	}()

	header := sim.bc.CurrentHeader()
	req.BlockHash = header.Hash()
	req.BlockNum = header.NumberU64()
	// sim.recordTraceBlock(txHash, StageSimulateCX, header.NumberU64()) — omitted
	committee := sim.committee.GetCommittee(req.Epochs[sim.committee.SelfShard], sim.committee.SelfShard)
	n := committee.Number
	t := committee.Threshold

	state, _, _ := sim.startSimulation(req)

	ctx, cancel := context.WithTimeout(sim.ctx, sim.config.CallTimeout)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(n)
	var (
		results []api.SSCMessage
		hasSelf bool
		lock    sync.Mutex
	)
	// 预分配容量，但长度动态控制
	results = make([]api.SSCMessage, 0, t)
	addrMap := make(map[int]bool)

	utils.SSCLogger().Debug().Int("simulationNum", req.SimulationNum).Str("txHash", txHash.String()).Msg("start simulate cx transaction, call to handle simulation request")

	for i := 0; i < n; i++ {
		member := committee.Members[i]
		go func(m *api.Member, c *api.ShardSimulateCommittee) {
			defer wg.Done()
			ret := new(api.CXTSimulationResult)
			callErr := sim.communicator.comm.Call(ctx, ret, m, api.Method_HandleSimulateRequest, req)
			if callErr != nil {
				if !errors.Is(callErr, context.Canceled) {
					utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(callErr).Msg("failed to call cxt ssc call")
				}
				return
			}

			lock.Lock()
			defer lock.Unlock()

			// 如果上下文已取消，直接退出
			if ctx.Err() != nil {
				utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(ctx.Err()).Msg("context cancelled")
				return
			}

			selfAddr := sim.communicator.signerMgr.GetSSCSigner().Address()
			if bytes.Compare(m.Address.Bytes(), selfAddr.Bytes()) == 0 {
				hasSelf = true
				if len(results) < t {
					addrMap[committee.ValidatorIndex[m.Address]] = true
					results = append([]api.SSCMessage{ret}, results...)
				} else {
					delete(addrMap, committee.ValidatorIndex[results[0].GetSenderAddr()])
					addrMap[committee.ValidatorIndex[m.Address]] = true
					results[0] = ret // 替换最后一个
				}
			} else {
				if len(results) < t {
					addrMap[committee.ValidatorIndex[m.Address]] = true
					results = append(results, ret)
				}
			}

			// 检查是否满足终止条件
			if hasSelf && len(results) == t {
				utils.SSCLogger().Debug().Interface("addrMap", addrMap).Str("txHash", txHash.String()).Msg("simulation result received, including self")
				cancel()
			}
		}(member, committee)
	}
	wg.Wait()
	tCallMembers = time.Since(t0)

	var (
		sscResult *api.CXTSimulationSSCResult
		err       error
	)
	if len(results) == t {
		leaderRet := results[0].(*api.CXTSimulationResult)
		if leaderRet.Err != "" {
			sscResult = &api.CXTSimulationSSCResult{
				Err:           leaderRet.Err,
				RelatedShards: []uint32{sim.committee.SelfShard},
				ConflictKeys:  leaderRet.ConflictKeys,
				BaseBLSSignedMessage: api.BaseBLSSignedMessage{
					Epochs: leaderRet.Epochs,
				},
			}
			utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(errors.New(leaderRet.Err)).Msg("failed to simulate transaction")
		} else {
			sscResult, err = sim.aggregateSimulationResults(results)
			if err != nil {
				sscResult = &api.CXTSimulationSSCResult{Err: err.Error(), RelatedShards: []uint32{sim.committee.SelfShard},
					BaseBLSSignedMessage: api.BaseBLSSignedMessage{
						Epochs: leaderRet.Epochs,
					}}
				utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(err).Msg("failed to aggregate simulation results")
				return sscResult
			}
		}
	} else {
		if len(results) == 0 {
			sscResult = &api.CXTSimulationSSCResult{Err: "no response from other shards", RelatedShards: []uint32{sim.committee.SelfShard},
				BaseBLSSignedMessage: api.BaseBLSSignedMessage{
					Epochs: req.Epochs,
				}}
		} else {
			errRet := results[len(results)-1].(*api.CXTSimulationResult)
			sscResult = &api.CXTSimulationSSCResult{Err: errRet.Err, RelatedShards: []uint32{sim.committee.SelfShard},
				BaseBLSSignedMessage: api.BaseBLSSignedMessage{
					Epochs: req.Epochs,
				}}
		}
	}

	simulationCommit := &api.SimulationCommit{
		SimulationNum: req.SimulationNum,
		TxHash:        req.Tx.Hash(),
		Nonce:         req.Tx.Nonce(),
		Sender:        req.From,
		RelatedShards: sscResult.RelatedShards,
		BaseBLSSignedMessage: api.BaseBLSSignedMessage{
			Epochs:  req.Epochs,
			ShardId: sim.committee.SelfShard,
		},
	}

	if len(sscResult.Err) == 0 {
		simulationCommit.Commit = true
		simulationCommit.Status = api.OK
		simulationCommit.Reason = api.OK.String()
		utils.SSCLogger().Debug().Str("txHash", simulationCommit.TxHash.Hex()).
			Msgf("simulation accomplished, send simulation commit, commit type: %v, simulationNum: %d, relatedShards: %v",
				simulationCommit.Commit, simulationCommit.SimulationNum, simulationCommit.RelatedShards)
	} else {
		simNumTag := fmt.Sprintf("[simNum=%d]", req.SimulationNum)
		if vm.IsLockConflictErr(sscResult.Err) {
			if len(sscResult.ConflictKeys) > 0 {
				utils.SSCLogger().Debug().Str("txHash", simulationCommit.TxHash.Hex()).
					Int("conflictKeys", len(sscResult.ConflictKeys)).
					Msgf("ForceSimulation: conflict keys=%d, full RWSet available for retry", len(sscResult.ConflictKeys))
			}
			utils.SSCLogger().Debug().Str("txHash", simulationCommit.TxHash.Hex()).
				Str("simNum", fmt.Sprintf("%d", req.SimulationNum)).
				Str("reason", sscResult.Err).
				Msgf("simulation %s state is locked by other tx, try to retry if self is leader: %v", simNumTag, sim.committee.IsLeader(req.Epochs[sim.committee.SelfShard]))
			simulationCommit.Commit = false
			simulationCommit.Status = api.LockConflict
			simulationCommit.Reason = sscResult.Err
			// don't send commit simulation if state is locked by other tx
			if sim.committee.IsLeader(req.Epochs[sim.committee.SelfShard]) {
				sim.state.CallForRetry(&api.RetryTx{
					TxHash:        txHash,
					Epochs:        req.Epochs,
					RelatedShards: sscResult.RelatedShards,
					SimulationNum: req.SimulationNum + 1,
					Condition:     api.Simulate,
					OriginShardID: state.OriginShardId,
				})
			}
			sim.state.SetTxStatus(txHash, api.WAITING_FOR_RESIMULATION_OFF_CHAIN)
			return sscResult
		} else {
			simulationCommit.Commit = false
			simulationCommit.Status = api.ExecutionFailed
			simulationCommit.Reason = sscResult.Err
		}
		utils.SSCLogger().Error().Str("txHash", simulationCommit.TxHash.Hex()).
			Str("simNum", fmt.Sprintf("%d", req.SimulationNum)).
			Err(errors.New(sscResult.Err)).Msgf("simulation %s accomplished, send simulation commit, commit type: %v, status: %v, simulationNum: %d, relatedShards: %v",
			simNumTag, simulationCommit.Commit, simulationCommit.Status.String(), simulationCommit.SimulationNum, simulationCommit.RelatedShards)
	}

	var chs []chan *api.CXTSimulationSSCResult

	func() {
		simState, _ := sim.GetSimState(txHash)
		if simState != nil {
			simState.SimulationCallStates[state.SimulationNum][0].TopSSCResult = sscResult
			simState.SimulationResult = sscResult
		}
		if ch := sim.getChannels(txHash); ch != nil {
			ch.mu.Lock()
			chs = ch.waitingChs
			ch.waitingChs = nil
			ch.mu.Unlock()
		}
	}()

	for _, ch := range chs {
		ch <- sscResult
	}

	sim.thresholdSignSimulationCommit(simulationCommit)
	tAggregate = time.Since(t0)
	tThresholdSign = time.Since(t0)

	leaders := make([]*api.Member, 0)
	selfAddr := sim.communicator.signerMgr.GetSSCSigner().Address()
	for _, shardId := range simulationCommit.RelatedShards {
		leader := sim.committee.GetLeader(req.Epochs[shardId], shardId)
		if leader == nil {
			utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
				Uint32("shardId", shardId).
				Msg("CommitSimulation: leader not found for shard (StartSimulateCXTransaction)")
			continue
		}
		if bytes.Equal(leader.Address.Bytes(), selfAddr.Bytes()) {
			utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
				Uint32("shardId", shardId).
				Msg("CommitSimulation: self is leader, calling locally (StartSimulateCXTransaction)")
			sim.sscService.CommitSimulation(simulationCommit)
			continue
		}
		leaders = append(leaders, leader)
	}
	ctxCS, cancelCS := context.WithTimeout(sim.ctx, sim.config.CallTimeout)
	defer cancelCS()

	if err := sim.communicator.comm.Multicast(ctxCS, leaders, api.Method_CommitSimulation, simulationCommit); err != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).
			Int("memberCount", len(leaders)).
			Msg("CommitSimulation Multicast failed (StartSimulateCXTransaction)")
	}

	var resultCh chan *api.CXTSimulationSSCResult
	func() {
		if ch := sim.getChannels(txHash); ch != nil {
			ch.mu.Lock()
			if ch.resultCh != nil {
				resultCh = ch.resultCh
				close(ch.resultCh)
				ch.resultCh = nil
			}
			ch.mu.Unlock()
		}
	}()
	if resultCh != nil {
		<-resultCh
	}

	sim.stats.setCxtStage(txHash, 1)
	tCommitSend = time.Since(t0)
	return sscResult
}

// aggregateSimulationResults 聚合所有成员的模拟结果。
//
// Original: sscService.aggregateSimulationResults (4b6d266c7)
// s.BLSSignerMgr.GetSSCSigner() → sim.communicator.signerMgr.GetSSCSigner()
// s.SelfShard → sim.committee.SelfShard
func (sim *Simulator) aggregateSimulationResults(results []api.SSCMessage) (*api.CXTSimulationSSCResult, error) {
	if len(results) == 0 {
		return nil, errors.New("no simulation results to aggregate")
	}

	result := results[0].(*api.CXTSimulationResult)
	aggregatedSig, bitMap, err := sim.communicator.signerMgr.GetSSCSigner().Aggregate(results)
	if err != nil {
		return nil, err
	}
	sscResult := &api.CXTSimulationSSCResult{
		RelatedShards: result.RelatedShards,
		Result:        result.Result,
		Receipt:       result.Receipt,
		UsedGas:       result.UsedGas,
		Err:           result.Err,
		TreeNode:      result.TreeNode,
		ConflictKeys:  result.ConflictKeys,
		BaseBLSSignedMessage: api.BaseBLSSignedMessage{
			ShardId:    sim.committee.SelfShard,
			Signatures: aggregatedSig,
			BLSBitMap:  bitMap,
			Epochs:     result.Epochs,
		},
	}
	return sscResult, nil
}

// thresholdSignSimulationCommit 门限签名 SimulationCommit。
//
// Original: sscService.thresholdSignSimulationCommit (4b6d266c7)
// s. → sim.
// s.CommitteeMechanism → sim.committee
// s.ctx → sim.ctx
// s.Config → sim.config
// s.Comm → sim.communicator.comm
// s.BLSSignerMgr.GetSSCSigner() → sim.communicator.signerMgr.GetSSCSigner()
// s.SelfShard → sim.committee.SelfShard
// s.SelfAddr → sim.communicator.signerMgr.GetSSCSigner().Address()
// s.stateLock → sim.state.*
// s.getState(txHash) → sim.state.GetTxState(txHash)
func (sim *Simulator) thresholdSignSimulationCommit(commit *api.SimulationCommit) {
	committee := sim.committee.GetCommittee(commit.Epochs[sim.committee.SelfShard], sim.committee.SelfShard)
	n := committee.Number
	t := committee.Threshold
	txHash := commit.TxHash
	state, err := sim.state.GetTxState(txHash)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(err).Msg("failed to get state")
		return
	}

	ctx, cancel := context.WithTimeout(state.Ctx, sim.config.CallTimeout)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(n)
	var (
		results []api.SSCMessage
		hasSelf bool
		lock    sync.Mutex
	)
	// 预分配容量，但长度动态控制
	results = make([]api.SSCMessage, 0, t)
	addrMap := make(map[common.Address]bool)

	for i := 0; i < n; i++ {
		member := committee.Members[i]
		go func(m *api.Member) {
			defer wg.Done()
			signature := make([]byte, 0)
			signErr := sim.communicator.comm.Call(ctx, &signature, member, api.Method_SignSimulationCommit, commit)
			if signErr != nil {
				if !errors.Is(signErr, context.Canceled) {
					utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(signErr).Msg("thresholdSignSimulationCommit")
				}
				return
			}
			ret := api.BaseSSCMessage{
				Epochs:     commit.Epochs,
				Signature:  signature,
				SenderAddr: member.Address,
			}

			utils.SSCLogger().Debug().
				Str("txHash", txHash.String()).
				Str("from", m.Address.String()).
				Msg("received simulation result")

			lock.Lock()
			defer lock.Unlock()

			// 如果上下文已取消，直接退出
			if ctx.Err() != nil {
				return
			}

			selfAddr := sim.communicator.signerMgr.GetSSCSigner().Address()
			if bytes.Compare(m.Address.Bytes(), selfAddr.Bytes()) == 0 {
				hasSelf = true
				if len(results) < t {
					addrMap[m.Address] = true
					results = append([]api.SSCMessage{ret}, results...)
				} else {
					delete(addrMap, results[0].GetSenderAddr())
					addrMap[m.Address] = true
					results[0] = ret // 替换最后一个
				}
			} else {
				if len(results) < t {
					addrMap[m.Address] = true
					results = append(results, ret)
				}
			}

			// 检查是否满足终止条件
			if hasSelf && len(results) == t {
				cancel()
			}
		}(member)
	}
	wg.Wait()

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Int("results", len(results)).
		Bool("ctxDone", ctx.Err() != nil).
		Msg("thresholdSignSimulationCommit: after wg.Wait")

	aggregatedSig, bitMap, err := sim.communicator.signerMgr.GetSSCSigner().Aggregate(results)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Str("txHash", txHash.Hex()).
			Msg("thresholdSignSimulationCommit: aggregate failed")
		return
	}
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Int("bitMapLen", len(bitMap)).
		Msg("thresholdSignSimulationCommit: aggregated OK")
	commit.Signatures = aggregatedSig
	commit.BLSBitMap = bitMap
	commit.ShardId = sim.committee.SelfShard
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Msg("thresholdSignSimulationCommit: done")
}

// aggregateSSCCommitVote 聚合 SSC Commit Vote 的签名。
//
// Original: sscService.aggregateSSCCommitVote (4b6d266c7)
// s.BLSSignerMgr.GetSSCSigner() → sim.communicator.signerMgr.GetSSCSigner()
// s.BLSSignerMgr.GetValidatorSigner() → sim.communicator.signerMgr.GetValidatorSigner()
// s.SelfShard → sim.committee.SelfShard
func (sim *Simulator) aggregateSSCCommitVote(votes []*api.CXTCommitVote, sscOrValidator bool) *api.CXTCommitSSCVote {
	if len(votes) == 0 {
		utils.SSCLogger().Error().Msg("no ssc commit votes to aggregate")
		return nil
	}
	msgs := make([]api.SSCMessage, 0, len(votes))
	for _, vote := range votes {
		msgs = append(msgs, vote)
	}
	result := votes[0]
	var (
		aggregatedSig []byte
		bitMap        []byte
		err           error
	)
	if sscOrValidator {
		aggregatedSig, bitMap, err = sim.communicator.signerMgr.GetSSCSigner().Aggregate(msgs)
	} else {
		aggregatedSig, bitMap, err = sim.communicator.signerMgr.GetValidatorSigner().Aggregate(msgs)
	}
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to aggregate ssc commit vote signatures")
		return nil
	}
	sscResult := &api.CXTCommitSSCVote{
		TxHash:        result.TxHash,
		SimulationNum: result.SimulationNum,
		ShardId:       result.ShardId,
		OriginShardId: result.OriginShardId,
		Type:          result.Type,
		Reason:        result.Reason,
		Payload:       result.Payload,
		BaseBLSSignedMessage: api.BaseBLSSignedMessage{
			ShardId:    sim.committee.SelfShard,
			Signatures: aggregatedSig,
			BLSBitMap:  bitMap,
			Epochs:     result.Epochs,
		},
	}
	return sscResult
}

// buildSignaturesForSimulation 为 Simulation 构建门限签名。
//
// Original: sscService.buildSignaturesForSimulation (4b6d266c7)
// s. → sim.
// s.CommitteeMechanism → sim.committee
// s.ctx → sim.ctx
// s.Config → sim.config
// s.Comm → sim.communicator.comm
// s.BLSSignerMgr.GetSSCSigner() → sim.communicator.signerMgr.GetSSCSigner()
// s.SelfShard → sim.committee.SelfShard
// s.SelfAddr → sim.communicator.signerMgr.GetSSCSigner().Address()
func (sim *Simulator) buildSignaturesForSimulation(state *api.CXTSimulationState, simulation *api.CXTSimulation) {
	txHash := simulation.TxHash
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("begin to build signatures for simulation")
	committee := sim.committee.GetCommittee(simulation.Epochs[sim.committee.SelfShard], sim.committee.SelfShard)
	n := committee.Number
	t := committee.Threshold

	ctx, cancel := context.WithTimeout(state.Ctx, sim.config.CallTimeout)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(n)
	var (
		results []api.SSCMessage
		hasSelf bool
		lock    sync.Mutex
	)
	// 预分配容量，但长度动态控制
	results = make([]api.SSCMessage, 0, t)
	addrMap := make(map[common.Address]bool)
	epochs := simulation.Epochs

	for i := 0; i < n; i++ {
		member := committee.Members[i]
		go func(m *api.Member) {
			defer wg.Done()
			signature := make([]byte, 0)
			err := sim.communicator.comm.Call(ctx, &signature, m, api.Method_SignCXTSimulation, simulation)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).Msgf("failed to call %s", m.Address.Hex())
				}
				return
			}
			ret := api.BaseSSCMessage{
				Signature:  signature,
				SenderAddr: m.Address,
				Epochs:     epochs,
			}

			lock.Lock()
			defer lock.Unlock()

			// 如果上下文已取消，直接退出
			if ctx.Err() != nil {
				return
			}

			selfAddr := sim.communicator.signerMgr.GetSSCSigner().Address()
			if bytes.Compare(m.Address.Bytes(), selfAddr.Bytes()) == 0 {
				hasSelf = true
				if len(results) < t {
					addrMap[m.Address] = true
					results = append([]api.SSCMessage{ret}, results...)
				} else {
					delete(addrMap, results[0].GetSenderAddr())
					addrMap[m.Address] = true
					results[0] = ret // 替换最后一个
				}
			} else {
				if len(results) < t {
					addrMap[m.Address] = true
					results = append(results, ret)
				}
			}

			// 检查是否满足终止条件
			if hasSelf && len(results) == t {
				cancel()
			}
		}(member)
	}
	wg.Wait()

	aggregatedSig, bitMap, err := sim.communicator.signerMgr.GetSSCSigner().Aggregate(results)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).Msg("aggregate signatures failed")
		return
	}
	simulation.Signatures = aggregatedSig
	simulation.BLSBitMap = bitMap
}

// StartReSimulation 重新模拟跨分片交易（Leader 节点）。
//
// Original: sscService.startReSimulation (impl.go:1980)
// s. → sim.
// s.stateLock.Lock/Unlock → removed (use accessors)
// s.getState(txHash) → sim.state.GetTxState(txHash) / sim.GetSimState(txHash)
// state → txState (for TxState fields) / simState (for SimulationState fields)
// s.SelfAddr → sim.communicator.signerMgr.GetSSCSigner().Address()
// s.SelfShard → sim.committee.SelfShard
// s.bc → sim.bc
// s.Config → sim.config
// s.ctx → sim.ctx
// s.GetCommittee → sim.committee.GetCommittee
// s.GetLeader → sim.committee.GetLeader
// s.IsLeader → sim.committee.IsLeader
// s.Comm.Call → sim.communicator.comm.Call
// s.Comm.Multicast → sim.communicator.comm.Multicast
// s.aggregateSimulationResults → sim.aggregateSimulationResults
// s.thresholdSignSimulationCommit → sim.thresholdSignSimulationCommit
// s.retryScheduler.CallForRetry → sim.state.CallForRetry
func (sim *Simulator) StartReSimulation(txHash common.Hash, simulationNum int) {
	t0 := time.Now()
	var tGetState, tCallMembers, tAggregate, tThresholdSign, tCommitSend time.Duration
	defer func() {
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Str("getState", tGetState.String()).
			Str("callMembers", tCallMembers.String()).
			Str("aggregate", tAggregate.String()).
			Str("thresholdSign", tThresholdSign.String()).
			Str("commitSend", tCommitSend.String()).
			Str("total", time.Since(t0).String()).
			Msg("StartReSimulation timing breakdown")
	}()

	txState, err := sim.state.GetTxState(txHash)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Str("txHash", txHash.Hex()).Msg("resimulation failed")
		return
	}

	ctx, cancel := context.WithTimeout(txState.Ctx, sim.config.CallTimeout)
	defer func() {
		cancel()
	}()

	simState, _ := sim.GetSimState(txHash)

	// DSN-25: 优先使用 OnBlockCommitted 缓存的 block hash，与 RetryCommit Phase 2 的 CheckLock 同版本
	header := sim.bc.CurrentHeader()
	if bh := sim.sscService.retryScheduler.GetCachedBlockHash(); bh != (common.Hash{}) {
		if h := sim.bc.GetHeaderByHash(bh); h != nil {
			header = h
		}
	}
	// request recall origin contract
	lastReq := simState.SimulationRequest
	if lastReq == nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msg("failed to get last simulation request")
		return
	}
	req := &api.CXTSimulationRequest{
		TxHash:        txHash,
		Epochs:        lastReq.Epochs,
		SimulationNum: simulationNum,
		Author:        lastReq.Author,
		BlockHash:     header.Hash(),
		BlockNum:      header.Number().Uint64(),
		Tx:            lastReq.Tx,
		From:          lastReq.From,
		GasPool:       0,
	}
	tGetState = time.Since(t0)

	startTime := time.Now()
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("recall simulation, simulationNum: %d, start", simulationNum)
	defer func() {
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("recall simulation finished, simulationNum: %d, duration: %v, end", simulationNum, time.Since(startTime))
	}()

	committee := sim.committee.GetCommittee(req.Epochs[sim.committee.SelfShard], sim.committee.SelfShard)
	n := committee.Number
	t := committee.Threshold

	wg := sync.WaitGroup{}
	wg.Add(n)
	var (
		results []api.SSCMessage
		hasSelf bool
		lock    sync.Mutex
	)
	// 预分配容量，但长度动态控制
	results = make([]api.SSCMessage, 0, t)
	addrMap := make(map[common.Address]bool)

	for i := 0; i < n; i++ {
		member := committee.Members[i]
		go func(m *api.Member) {
			defer wg.Done()
			ret := new(api.CXTSimulationResult)
			callErr := sim.communicator.comm.Call(ctx, ret, member, api.Method_HandleSimulateRequest, req)
			if callErr != nil {
				if !errors.Is(callErr, context.Canceled) {
					utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(callErr).Msg("failed to call cxt ssc call")
				}
				return
			}

			lock.Lock()
			defer lock.Unlock()

			// 如果上下文已取消，直接退出
			if ctx.Err() != nil {
				return
			}

			selfAddr := sim.communicator.signerMgr.GetSSCSigner().Address()
			if bytes.Compare(m.Address.Bytes(), selfAddr.Bytes()) == 0 {
				hasSelf = true
				if len(results) < t {
					addrMap[m.Address] = true
					results = append([]api.SSCMessage{ret}, results...)
				} else {
					delete(addrMap, results[0].GetSenderAddr())
					addrMap[m.Address] = true
					results[0] = ret // 替换最后一个
				}
			} else {
				if len(results) < t {
					addrMap[m.Address] = true
					results = append(results, ret)
				}
			}

			// 检查是否满足终止条件
			if hasSelf && len(results) == t {
				cancel()
			}
		}(member)
	}
	wg.Wait()
	tCallMembers = time.Since(t0)

	var (
		sscResult *api.CXTSimulationSSCResult
	)

	if len(results) == t {
		leaderRet := results[0].(*api.CXTSimulationResult)
		if leaderRet.Err != "" {
			sscResult = &api.CXTSimulationSSCResult{Err: leaderRet.Err, RelatedShards: []uint32{sim.committee.SelfShard},
				ConflictKeys: leaderRet.ConflictKeys,
				BaseBLSSignedMessage: api.BaseBLSSignedMessage{
					Epochs: leaderRet.Epochs,
				},
			}
			utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(errors.New(leaderRet.Err)).Msg("failed to simulate transaction")
		} else {
			sscResult, err = sim.aggregateSimulationResults(results)
			if err != nil {
				sscResult = &api.CXTSimulationSSCResult{Err: err.Error(), RelatedShards: []uint32{sim.committee.SelfShard},
					BaseBLSSignedMessage: api.BaseBLSSignedMessage{
						Epochs: leaderRet.Epochs,
					},
				}
				utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(err).Msg("failed to aggregate simulation results")
			} else {
				simState, _ := sim.GetSimState(txHash)
				if simState != nil {
					simState.SimulationResult = sscResult
					if len(simState.SimulationCallStates[simulationNum]) == 0 {
						sscResult = &api.CXTSimulationSSCResult{Err: "callStates size is 0", RelatedShards: []uint32{sim.committee.SelfShard},
							BaseBLSSignedMessage: api.BaseBLSSignedMessage{
								Epochs: leaderRet.Epochs,
							},
						}
					} else {
						simState.SimulationCallStates[simulationNum][0].TopSSCResult = sscResult
					}
				}
			}
		}
	} else {
		if len(results) == 0 {
			sscResult = &api.CXTSimulationSSCResult{Err: "no response from other shards", RelatedShards: []uint32{sim.committee.SelfShard},
				BaseBLSSignedMessage: api.BaseBLSSignedMessage{
					Epochs: req.Epochs,
				},
			}
		} else {
			errRet := results[len(results)-1].(*api.CXTSimulationResult)
			sscResult = &api.CXTSimulationSSCResult{Err: errRet.Err, RelatedShards: []uint32{sim.committee.SelfShard},
				BaseBLSSignedMessage: api.BaseBLSSignedMessage{
					Epochs: req.Epochs,
				},
			}
		}
	}

	// build and send simulation commit
	simulationCommit := &api.SimulationCommit{
		SimulationNum: req.SimulationNum,
		TxHash:        txHash,
		Nonce:         req.Tx.Nonce(),
		Sender:        req.From,
		RelatedShards: sscResult.RelatedShards,
		BaseBLSSignedMessage: api.BaseBLSSignedMessage{
			Epochs: req.Epochs,
		},
	}

	if len(sscResult.Err) == 0 {
		simulationCommit.Commit = true
		simulationCommit.Status = api.OK
		simulationCommit.Reason = api.OK.String()
		utils.SSCLogger().Debug().Str("txHash", simulationCommit.TxHash.Hex()).
			Msgf("resimulation accomplished, send simulation commit, commit type: %v, simulationNum: %d, relatedShards: %v",
				simulationCommit.Commit, simulationCommit.SimulationNum, simulationCommit.RelatedShards)
	} else {
		if vm.IsLockConflictErr(sscResult.Err) {
			if len(sscResult.ConflictKeys) > 0 {
				utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
					Int("conflictKeys", len(sscResult.ConflictKeys)).
					Msgf("ForceSimulation: resimulation conflict keys=%d, full RWSet available", len(sscResult.ConflictKeys))
			}
			utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("resimulation failed err=%s, it has subscribe to resimulate again, nextSimulationNum=%d", sscResult.Err, req.SimulationNum+1)
			simulationCommit.Commit = false
			simulationCommit.Status = api.LockConflict
			simulationCommit.Reason = sscResult.Err
			var originShardId uint32
			if state, es := sim.state.GetTxState(txHash); es == nil {
				originShardId = state.OriginShardId
			}
			if sim.committee.IsLeader(req.Epochs[sim.committee.SelfShard]) {
				sim.state.CallForRetry(&api.RetryTx{
					TxHash:        txHash,
					Epochs:        req.Epochs,
					RelatedShards: sscResult.RelatedShards,
					SimulationNum: req.SimulationNum + 1,
					Condition:     api.Simulate,
					OriginShardID: originShardId,
				})
			}
			return
		}

		simulationCommit.Commit = false
		simulationCommit.Status = api.ExecutionFailed
		simulationCommit.Reason = sscResult.Err
		utils.SSCLogger().Error().Str("txHash", simulationCommit.TxHash.Hex()).
			Err(errors.New(sscResult.Err)).Msgf("resimulation accomplished, send simulation commit, commit type: %v, status: %v, simulationNum: %d, relatedShards: %v",
			simulationCommit.Commit, simulationCommit.Status.String(), simulationCommit.SimulationNum, simulationCommit.RelatedShards)
	}

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Msg("startReSimulation: calling thresholdSignSimulationCommit")
	tAggregate = time.Since(t0)
	sim.thresholdSignSimulationCommit(simulationCommit)
	tThresholdSign = time.Since(t0)
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Msg("startReSimulation: after thresholdSignSimulationCommit")

	// 创建独立的 ctx 用于后续 Multicast，避免被 thresholdSignSimulationCommit 的 cancel 影响
	notifyCtx, notifyCancel := context.WithTimeout(txState.Ctx, sim.config.CallTimeout)
	defer notifyCancel()

	members := make([]*api.Member, 0, len(committee.Members))
	selfAddr := sim.communicator.signerMgr.GetSSCSigner().Address()
	for _, shardId := range simulationCommit.RelatedShards {
		leader := sim.committee.GetLeader(simulationCommit.Epochs[shardId], shardId)
		if leader == nil {
			utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
				Uint32("shardId", shardId).
				Msg("CommitSimulation: leader not found for shard")
			continue
		}
		if bytes.Equal(leader.Address.Bytes(), selfAddr.Bytes()) {
			// 自己就是目标 leader，直接本地调用，不走 RPC
			utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
				Uint32("shardId", shardId).
				Msg("CommitSimulation: self is leader, calling locally")
			sim.sscService.CommitSimulation(simulationCommit)
			continue
		}
		members = append(members, leader)
	}
	if err := sim.communicator.comm.Multicast(notifyCtx, members, api.Method_CommitSimulation, simulationCommit); err != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).
			Int("memberCount", len(members)).
			Msg("CommitSimulation Multicast failed")
	}
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Int("members", len(members)).
		Msg("startReSimulation: after Multicast")
	tCommitSend = time.Since(t0)
}

// =============================================================================
// Leader 函数（迁移自 simulator_member.go）
// =============================================================================

// HandleCXTSSCCall 处理来自分片内部的 SSC 跨分片调用请求。
// Leader 调用此方法，协调全部分片的 member 执行合约。
func (sim *Simulator) HandleCXTSSCCall(req *api.CXTCallSSCRequest) *api.CXTCallSSCResult {
	if req == nil {
		utils.SSCLogger().Error().Msg("nil request")
		return &api.CXTCallSSCResult{
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{}, Err: "nil request"}
	}
	signer := sim.communicator.signerMgr.GetSSCSigner()
	err := signer.Verify(req)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", req.TxHash.String()).Err(err).Interface("req", req).Msg("invalid cxt ssc call signature")
		return &api.CXTCallSSCResult{Err: err.Error(), BaseBLSSignedMessage: api.BaseBLSSignedMessage{Epochs: req.Epochs}}
	}

	startTime := time.Now()
	header := sim.bc.CurrentHeader()
	req.BlockHash = header.Hash()
	req.BlockNum = header.NumberU64()
	utils.SSCLogger().Debug().Str("txHash", req.TxHash.String()).Str("callIndex", req.CallIndex.ToString()).Msg("handle cxt ssc call, start")
	committee := sim.committee.GetCommittee(req.Epochs[sim.committee.SelfShard], sim.committee.SelfShard)
	t := committee.Threshold
	n := committee.Number
	txHash := req.TxHash

	// 通过 startCXT 初始化跨分片调用状态
	_, err = sim.startCXT(txHash, req.SimulationNum, req.OriginShardId, req.RelatedShards, nil, req)
	if err != nil {
		return &api.CXTCallSSCResult{
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{
				Epochs: req.Epochs,
			}, Err: fmt.Sprintf("error from targetShard %d, err=%s", req.TargetShardId, err.Error())}
	}

	ctx, cancel := context.WithTimeout(sim.ctx, sim.config.CallTimeout)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(n)
	var (
		results []api.SSCMessage
		hasSelf bool
		lock    sync.Mutex
	)
	results = make([]api.SSCMessage, 0, t)
	addrMap := make(map[common.Address]bool)

	for i := 0; i < n; i++ {
		member := committee.Members[i]
		go func(index int, m *api.Member) {
			defer wg.Done()
			ret := new(api.CXTCallResult)
			callErr := sim.communicator.comm.Call(ctx, ret, member, api.Method_HandleCXTCall, req)
			if callErr != nil {
				if !errIsContextCanceled(callErr) {
					utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(callErr).Msg("failed to call cxt ssc call")
				}
				return
			}

			lock.Lock()
			defer lock.Unlock()

			if ctx.Err() != nil {
				return
			}

			if bytes.Compare(m.Address.Bytes(), signer.Address().Bytes()) == 0 {
				hasSelf = true
				if len(results) < t {
					addrMap[m.Address] = true
					results = append([]api.SSCMessage{ret}, results...)
				} else {
					delete(addrMap, results[0].GetSenderAddr())
					addrMap[m.Address] = true
					results[0] = ret
				}
			} else {
				if len(results) < t {
					results = append(results, ret)
					addrMap[m.Address] = true
				}
			}

			if hasSelf && len(results) == t {
				cancel()
			}
		}(i, member)
	}
	wg.Wait()

	txState, _ := sim.state.GetTxState(txHash)

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Int("n", n).Int("t", t).Int("results", len(results)).Int("addrs", len(addrMap)).Interface("addrMap", addrMap).Msg("receive sigs")

	if len(results) < t {
		if len(results) == 0 {
			utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).Msgf("handle cxt ssc call failed, error from targetShard %d", req.TargetShardId)
			return &api.CXTCallSSCResult{
				TxHash:               txHash,
				RelatedShards:        txState.RelatedShards.Merge(req.RelatedShards),
				Err:                  fmt.Sprintf("handle ssc call failed, error from targetShard %d", req.TargetShardId),
				BaseBLSSignedMessage: api.BaseBLSSignedMessage{Epochs: req.Epochs, ShardId: req.ShardId},
			}
		}
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).Msgf("handle cxt ssc call failed, no enough ret: [%d, %d]", len(results), t)
		return &api.CXTCallSSCResult{
			TxHash:               txHash,
			RelatedShards:        txState.RelatedShards.Merge(req.RelatedShards),
			Err:                  "no enough ret",
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{Epochs: req.Epochs, ShardId: req.ShardId},
		}
	}

	var sscResult *api.CXTCallSSCResult
	leaderRet := results[0].(*api.CXTCallResult)
	if leaderRet.Err != "" {
		sscResult = &api.CXTCallSSCResult{
			TxHash:               txHash,
			Err:                  leaderRet.Err,
			RelatedShards:        leaderRet.RelatedShards.Merge(req.RelatedShards),
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{Epochs: req.Epochs, ShardId: req.ShardId}}
		utils.SSCLogger().Error().Str("txHash", txHash.String()).Msgf("failed to simulate transaction: %s", leaderRet.Err)
	} else {
		sscResult, err = sim.aggregateCXSSCCallResult(results)
		if err != nil {
			utils.SSCLogger().Error().
				Str("txHash", txHash.String()).Err(err).Msg("failed to aggregate cxt call results")
			return &api.CXTCallSSCResult{
				TxHash:               txHash,
				RelatedShards:        leaderRet.RelatedShards,
				Err:                  fmt.Sprintf("error from targetShard %d, err=%s", req.TargetShardId, err.Error()),
				BaseBLSSignedMessage: api.BaseBLSSignedMessage{Epochs: req.Epochs, ShardId: req.ShardId},
			}
		}
	}

	// 更新 SimulationState 中的 call state
	simState, _ := sim.GetSimState(txHash)
	if simState != nil {
		if simState.SimulationCallStates[req.SimulationNum] != nil {
			callState := simState.SimulationCallStates[req.SimulationNum].Get(req.CallIndex)
			if callState != nil {
				callState.CallSSCResult = sscResult
			}
		}
	}

	utils.SSCLogger().Debug().Str("txHash", req.TxHash.String()).Dur("cost", time.Since(startTime)).Msg("handle cxt ssc call, end")
	return sscResult
}

// aggregateCXSSCCallResult 聚合跨分片调用结果并添加 BLS 聚合签名。
func (sim *Simulator) aggregateCXSSCCallResult(results []api.SSCMessage) (*api.CXTCallSSCResult, error) {
	if len(results) == 0 {
		utils.SSCLogger().Error().Msg("no cxt call results to aggregate")
		return nil, fmt.Errorf("no cxt call results to aggregate")
	}
	result := results[0].(*api.CXTCallResult)

	// BLS 聚合签名
	aggregatedSig, bitMap, err := sim.communicator.signerMgr.GetSSCSigner().Aggregate(results)
	if err != nil {
		return nil, err
	}

	sscResult := &api.CXTCallSSCResult{
		TxHash:        result.TxHash,
		CallIndex:     result.CallIndex,
		RelatedShards: result.RelatedShards,
		Result:        result.Result,
		LeftOverGas:   result.LeftOverGas,
		BlockHash:     result.BlockHash,
		Err:           result.Err,
		TreeNode:      result.TreeNode,
		BaseBLSSignedMessage: api.BaseBLSSignedMessage{
			ShardId:    sim.committee.SelfShard,
			Epochs:     result.Epochs,
			Signatures: aggregatedSig,
			BLSBitMap:  bitMap,
		},
	}
	return sscResult, nil
}

// aggregateSSCCallRequest 聚合 SSC 调用请求
func (sim *Simulator) aggregateSSCCallRequest(requests []*api.CXTCallRequest) *api.CXTCallSSCRequest {
	if len(requests) == 0 {
		utils.SSCLogger().Error().Msg("no cxt call requests to aggregate")
		return nil
	}
	request := requests[0]
	committee := sim.committee.GetCommittee(request.Epochs[sim.committee.SelfShard], sim.committee.SelfShard)
	msgs := make([]api.SSCMessage, 0, len(requests))
	for _, msg := range requests {
		sender := msg.GetSenderAddr()
		_, inCommittee := committee.MemberIndex[sender]
		validatorIndex, inValidators := committee.ValidatorIndex[sender]
		if !inCommittee {
			memberIndexes := make([]int, 0, len(committee.Members))
			for _, memberIndex := range committee.MemberIndex {
				memberIndexes = append(memberIndexes, memberIndex)
			}
			utils.SSCLogger().Error().
				Interface("epochs", request.Epochs).
				Str("txHash", msg.TxHash.Hex()).
				Str("callIndex", msg.CallIndex.ToString()).
				Int("validatorIndex", validatorIndex).
				Interface("memberIndexes", memberIndexes).
				Str("addr", msg.GetSenderAddr().Hex()).
				Msg("cxt call request sender not in committee")
		}
		if !inValidators {
			validatorAddrs := make([]string, 0, len(committee.Validators))
			for _, validator := range committee.Validators {
				validatorAddrs = append(validatorAddrs, validator.Address.Hex())
			}
			utils.SSCLogger().Error().Str("txHash", msg.TxHash.Hex()).Str("callIndex", msg.CallIndex.ToString()).Interface("validatorIndexes", committee.ValidatorIndex).Str("addr", msg.GetSenderAddr().Hex()).Msgf("cxt call request sender not in validators, %v", validatorAddrs)
		} else {
			utils.SSCLogger().Debug().Str("txHash", msg.TxHash.Hex()).Str("callIndex", msg.CallIndex.ToString()).Str("addr", msg.GetSenderAddr().Hex()).Msgf("cxt call request sender in committee, validatorIndex=%d", validatorIndex)
		}
		msgs = append(msgs, msg)
	}
	aggregatedSig, bitMap, err := sim.communicator.signerMgr.GetSSCSigner().Aggregate(msgs)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Interface("requests", requests).Msg("failed to aggregate cxt call request signatures")
		return nil
	}
	utils.SSCLogger().Debug().Str("bitMap", common.Bytes2Hex(bitMap)).Msgf("aggregated cxt call request signatures, msgs=%d", len(msgs))
	sscRequest := &api.CXTCallSSCRequest{
		OriginShardId: request.OriginShardId,
		FromShardId:   request.FromShardId,
		TargetShardId: request.TargetShardId,
		SimulationNum: request.SimulationNum,
		RelatedShards: request.RelatedShards,
		TxHash:        request.TxHash,
		Nonce:         request.Nonce,
		TxSender:      request.TxSender,
		CallIndex:     request.CallIndex,
		Caller:        request.Caller,
		Addr:          request.Addr,
		Input:         request.Input,
		Gas:           request.Gas,
		GasPrice:      request.GasPrice,
		Value:         request.Value,
		BaseBLSSignedMessage: api.BaseBLSSignedMessage{
			ShardId:    sim.committee.SelfShard,
			Signatures: aggregatedSig,
			BLSBitMap:  bitMap,
			Epochs:     request.Epochs,
		},
		BlockHash: common.Hash{},
		BlockNum:  0,
	}
	return sscRequest
}

// errIsContextCanceled 检查错误是否为 context.Canceled
func errIsContextCanceled(err error) bool {
	if err == nil {
		return false
	}
	return err.Error() == context.Canceled.Error()
}
