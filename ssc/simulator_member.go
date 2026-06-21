package ssc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/core"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/core/vm"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
)

// ========================================================================
// Member 节点函数
// ========================================================================

// HandleCXTCall 处理来自其他分片 leader 的跨分片调用请求（Member 节点）。
// 验证签名、构造 SSCVM、执行合约调用。
func (sim *Simulator) HandleCXTCall(req *api.CXTCallSSCRequest) *api.CXTCallResult {
	signer := sim.communicator.signerMgr.GetSSCSigner()
	err := signer.Verify(req)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to verify cxt call request signature")
		return &api.CXTCallResult{
			Err: fmt.Sprintf("error from targetShard %d, err=%s", req.TargetShardId, err.Error()),
			BaseSSCMessage: api.BaseSSCMessage{
				Epochs:     req.Epochs,
				SenderAddr: signer.Address(),
			},
		}
	}

	txHash := req.TxHash
	// begin cxt call, update simulationState and callIndex
	callState, err := sim.startCall(req)
	if err != nil {
		return &api.CXTCallResult{
			Err: fmt.Sprintf("error from targetShard %d, err=%s", req.TargetShardId, err.Error()),
			BaseSSCMessage: api.BaseSSCMessage{
				Epochs:     req.Epochs,
				SenderAddr: signer.Address(),
			},
		}
	}

	// 更新 SimulationState
	simState, ok := sim.GetSimState(txHash)
	if !ok || simState == nil {
		return &api.CXTCallResult{
			Err: "transaction has been closed",
			BaseSSCMessage: api.BaseSSCMessage{
				Epochs:     req.Epochs,
				SenderAddr: signer.Address(),
			},
		}
	}
	simState.CurrentCallFrame = &api.CallFrame{
		CallIndex: req.CallIndex,
		PC:        0,
	}
	simState.CallStack.Push(simState.CurrentCallFrame)

	defer sim.endCall(req)
	callState.Lock.Lock()
	defer callState.Lock.Unlock()

	startTime := time.Now()
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Str("callIndex", req.CallIndex.ToString()).Str("addr", req.Addr.Hex()).Msg("handle cxt call, start")

	defer func() {
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Str("callIndex", req.CallIndex.ToString()).Dur("cost", time.Since(startTime)).Msg("handle cxt call, end")
	}()

	header := sim.bc.GetHeaderByHash(req.BlockHash)
	if header == nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Str("callIndex", req.CallIndex.ToString()).Msg("failed to get header")
		return &api.CXTCallResult{
			Err: "failed to get header",
			BaseSSCMessage: api.BaseSSCMessage{
				Epochs:     req.Epochs,
				SenderAddr: signer.Address(),
			},
		}
	}
	stateDB, err := sim.bc.StateAt(header.Root())
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to get stateDB")
		return &api.CXTCallResult{Err: "failed to get stateDB",
			BaseSSCMessage: api.BaseSSCMessage{
				Epochs:     req.Epochs,
				SenderAddr: signer.Address(),
			}}
	}
	chainConfig := sim.bc.Config()
	vmConfig := sim.bc.GetVMConfig()
	sender := vm.AccountRef(req.Caller)
	var executionType vm.ExecutionType
	if req.SimulationNum == 0 {
		executionType = vm.SimulationCall
	} else {
		executionType = vm.SimulationReCall
	}

	sim.timerMgr.StartPoolTimer(txHash, req.Epochs, header.NumberU64(), sim.committee.SelfShard)

	callNode := api.NewCallNode(req.TxHash, req.CallIndex, sim.committee.SelfShard)
	simState.CallForest.Insert(callNode)
	defer func() {
		callNode.Done()
	}()

	// create sscvm instance — 需要 sscService 作为 api.Service
	vmCtx := core.NewSSCVMContext(req.Caller, txHash, req.CallIndex, req.GasPrice, header, sim.bc, nil)
	sscvm := vm.NewSSCVM(vmCtx, stateDB, chainConfig, *vmConfig, sim.sscService, executionType)

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("call contract with %s, callIndex=%s, simulationNum=%d",
		executionType.String(), req.CallIndex.ToString(), req.SimulationNum)
	ret, leftOverGas, err := sscvm.CallFromOtherShard(req.FromShardId, sender, req.Addr, req.Input, req.Gas, req.Value)

	if ret == nil && err == nil {
		utils.SSCLogger().Error().Err(err).Str("txHash", txHash.Hex()).Str("callIndex", req.CallIndex.ToString()).Msgf("successfully return nil, addr=%s, leftOverGas=%d, ret=%s", req.Addr, leftOverGas, ret)
	}

	txState, _ := sim.state.GetTxState(txHash)
	simState, _ = sim.GetSimState(txHash)
	relatedShards := func() api.RelatedShards {
		if txState != nil {
			return txState.RelatedShards.Merge(req.RelatedShards).Add(req.FromShardId)
		}
		return req.RelatedShards
	}()

	result := &api.CXTCallResult{
		TxHash:        txHash,
		CallIndex:     req.CallIndex,
		RelatedShards: relatedShards,
		Result:        ret,
		LeftOverGas:   leftOverGas,
		BlockHash:     header.Hash(),
		TreeNode:      callNode.ToData(),
	}
	if callState.LockedByOtherTx != nil {
		result.Err = api.ErrLockConflict_OnChain.Error()
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Str("callIndex", req.CallIndex.ToString()).Msgf("return locked by other tx")
	}
	if err != nil {
		result.Err = fmt.Sprintf("error from targetShard %d, err=%s", req.TargetShardId, err.Error())
		utils.SSCLogger().Error().Err(err).Msg("failed to call contract")
	}
	sign, err := signer.Sign(result)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to sign cxt call result")
		return &api.CXTCallResult{
			Err:           "failed to sign cxt call result",
			RelatedShards: []uint32{sim.committee.SelfShard},
			BaseSSCMessage: api.BaseSSCMessage{
				Epochs:     req.Epochs,
				SenderAddr: signer.Address(),
			},
		}
	}
	result.BaseSSCMessage = api.BaseSSCMessage{
		Signature:  sign,
		SenderAddr: signer.Address(),
		Epochs:     req.Epochs,
	}
	utils.SSCLogger().Debug().
		Str("txHash", txHash.Hex()).
		Str("callIndex", req.CallIndex.ToString()).
		Msgf("cxt call result: %s, err: %s", common.Bytes2Hex(result.Result), result.Err)
	return result
}

// startCallForMember 在 member 节点上开始跨分片调用。
func (sim *Simulator) startCallForMember(req *api.CXTCallSSCRequest) (*api.SimulationCallState, error) {
	txHash := req.TxHash
	// 直接调 Simulator.startCXT 创建 TxState + SimulationState
	_, err := sim.startCXT(txHash, req.SimulationNum, req.OriginShardId, req.RelatedShards, nil, req)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to start cxt for member")
		return nil, err
	}
	// 读取 call state
	simState, ok := sim.GetSimState(txHash)
	if !ok || simState == nil {
		return nil, fmt.Errorf("simulation state not found after startCallForMember")
	}
	callState := simState.SimulationCallStates[req.SimulationNum].Get(req.CallIndex)
	return callState, nil
}

// endCall 结束跨分片调用，更新 CallStack。
func (sim *Simulator) endCall(req *api.CXTCallSSCRequest) {
	txHash := req.TxHash
	simState, ok := sim.GetSimState(txHash)
	if !ok || simState == nil {
		return
	}
	simState.CallStack.Pop()
	if frame := simState.CallStack.Top(); frame != nil {
		simState.CurrentCallFrame = frame
	}
}

// ========================================================================
// 以下 8 个函数由 impl.go 搬迁而来（RequestCallCXT 调用链）
// ========================================================================

// RequestCallCXT 发起跨分片调用请求（Member 端）。
// 从 impl.go 搬迁：s.stateLock → sim.state, s.getState → sim.state.GetTxState, 等
func (sim *Simulator) RequestCallCXT(req *api.CXTCallRequest) *api.CXTCallSSCResult {
	txHash := req.TxHash
	startTime := time.Now()

	// 获取当前分片委员会信息
	committee := sim.committee.GetCommittee(req.Epochs[sim.committee.SelfShard], sim.committee.SelfShard)
	threshold := committee.Threshold

	txState, err := sim.state.GetTxState(txHash)
	if err != nil {
		return &api.CXTCallSSCResult{
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{
				Epochs: req.Epochs,
			},
			Err: fmt.Sprintf("failed to get state=%v", err)}
	}

	// 创建带超时的 context，避免永久阻塞
	ctx, cancel := context.WithTimeout(txState.Ctx, sim.config.CallTimeout)
	defer cancel()

	utils.SSCLogger().Info().
		Str("txHash", txHash.Hex()).
		Str("callIndex", req.CallIndex.ToString()).
		Interface("epochs", req.Epochs).
		Uint32("targetShard", req.TargetShardId).
		Msg("RequestCallCXT: start")

	defer func() {
		utils.SSCLogger().Info().
			Str("txHash", txHash.Hex()).
			Str("callIndex", req.CallIndex.ToString()).
			Interface("epochs", req.Epochs).
			Dur("cost", time.Since(startTime)).
			Msg("RequestCallCXT: end")
	}()

	// ========== 阶段 1: 获取或创建调用状态 ==========

	_, dependentCall, err := sim.getOrCreateCallerState(txHash, req, committee)
	if err != nil {
		return &api.CXTCallSSCResult{
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{
				Epochs: req.Epochs,
			},
			Err: fmt.Sprintf("failed to get caller state: %v", err),
		}
	}

	// ========== 阶段 2: 注册当前请求 ==========

	callReq := &safeCallRequest{
		req:       req,
		waitingCh: make(chan *api.CXTCallSSCResult, 1), // 带缓冲，避免发送方阻塞
	}

	// 尝试注册请求，返回是否达到阈值
	reachThreshold := sim.registerRequest(dependentCall, callReq, threshold)
	utils.SSCLogger().Info().
		Str("txHash", txHash.Hex()).
		Str("callIndex", req.CallIndex.ToString()).
		Interface("epochs", req.Epochs).
		Msgf("RequestCallCXT: register request, reached threshold: %v", reachThreshold)

	// ========== 阶段 3: 根据是否达到阈值分支处理 ==========

	if reachThreshold {
		// 当前请求使计数达到阈值，负责聚合并发起远程调用
		result := sim.executeThresholdAction(ctx, txHash, req, dependentCall, committee)
		return result
	} else {
		// 未达到阈值，等待其他请求或超时
		result := sim.waitForResult(ctx, callReq, dependentCall)
		return result
	}
}

// getOrCreateCallerState 获取或创建 caller 状态和 dependentCall
func (sim *Simulator) getOrCreateCallerState(
	txHash common.Hash,
	req *api.CXTCallRequest,
	committee *api.ShardSimulateCommittee,
) (*api.SimulationCallState, *safeDependentCall, error) {

	txState, err := sim.state.GetTxState(txHash)
	if err != nil {
		return nil, nil, err
	}

	simState, ok := sim.GetSimState(txHash)
	if !ok || simState == nil {
		return nil, nil, errors.New("simulation state not found")
	}

	callStates := simState.SimulationCallStates[req.SimulationNum]
	callerState := callStates.Get(req.CallIndex[:len(req.CallIndex)-1])

	// 如果 callerState 不存在，放入 pending 队列等待
	if callerState == nil {
		pendingResult := sim.handlePendingRequest(txHash, req, txState.Ctx)
		if pendingResult != nil {
			txState, err = sim.state.GetTxState(txHash)
			if err != nil {
				return nil, nil, err
			}
			simState, ok = sim.GetSimState(txHash)
			if !ok || simState == nil {
				return nil, nil, errors.New("simulation state not found after pending")
			}
			callStates = simState.SimulationCallStates[req.SimulationNum]
			callerState = callStates.Get(req.CallIndex[:len(req.CallIndex)-1])
		}
	}

	// 再次检查，如果仍不存在则返回错误
	if callerState == nil {
		return nil, nil, errors.New("caller state not found and pending wait failed")
	}

	// 获取或创建 dependentCall（使用独立的 depCallLock 保护，避免与合约执行竞争）
	dependentCall := sim.getOrCreateDependentCall(txHash, callerState, req.CallIndex.ToString(), committee.Threshold)

	return callerState, dependentCall, nil
}

// handlePendingRequest 处理 callerState 不存在的情况
func (sim *Simulator) handlePendingRequest(
	txHash common.Hash,
	req *api.CXTCallRequest,
	cxtCtx context.Context,
) *api.CXTCallSSCResult {

	waitingCh := make(chan *api.CXTCallSSCResult, 1)

	sim.pendingLock.Lock()
	sim.pendingRequests[txHash] = append(sim.pendingRequests[txHash], &pendingCXTRequest{
		req:       req,
		waitingCh: waitingCh,
	})
	sim.pendingLock.Unlock()

	utils.SSCLogger().Debug().
		Str("txHash", txHash.Hex()).
		Str("callIndex", req.CallIndex.ToString()).
		Msg("RequestCallCXT: no caller state, waiting in pending queue")

	// 等待被唤醒或超时
	select {
	case result := <-waitingCh:
		return result
	case <-cxtCtx.Done():
		// 超时，从 pending 队列移除
		sim.pendingLock.Lock()
		pendingList := sim.pendingRequests[txHash]
		newList := make([]*pendingCXTRequest, 0, len(pendingList))
		for _, p := range pendingList {
			if p.waitingCh != waitingCh {
				newList = append(newList, p)
			}
		}
		sim.pendingRequests[txHash] = newList
		sim.pendingLock.Unlock()
		return &api.CXTCallSSCResult{
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{
				Epochs: req.Epochs,
			}, Err: fmt.Sprintf("pending request timeout: %v", cxtCtx.Err())}
	}
}

// getOrCreateDependentCall 获取或创建 dependentCall
// 使用独立的 depCallLock 保护 DependentCXTCalls 访问，避免与合约执行的 callerState.Lock 竞争
func (sim *Simulator) getOrCreateDependentCall(
	txHash common.Hash,
	callerState *api.SimulationCallState,
	callIndexKey string,
	threshold int,
) *safeDependentCall {

	callerState.StateLock.Lock()
	defer callerState.StateLock.Unlock()

	dependentCall := callerState.DependentCXTCalls[callIndexKey]
	isNew := false
	if dependentCall == nil {
		dependentCall = &api.DependentCXTCall{
			CallIndex:     api.CallIndex{},
			Requests:      make([]*api.CXTCallRequest, 0, threshold),
			SignedRequest: nil,
			SSCResult:     nil,
			WaitCh:        make(chan struct{}), // ✅ 在创建时初始化 waitCh，所有请求共享
		}
		callerState.DependentCXTCalls[callIndexKey] = dependentCall
		isNew = true
	}

	// 🔍 调试日志：打印 dependentCall 和 WaitCh 的地址
	utils.SSCLogger().Info().
		Str("txHash", txHash.Hex()).
		Str("callIndex", callIndexKey).
		Bool("isNew", isNew).
		Str("depCallAddr", fmt.Sprintf("%p", dependentCall)).
		Str("waitChAddr", fmt.Sprintf("%p", dependentCall.WaitCh)).
		Msg("getOrCreateDependentCall")

	safeCall := &safeDependentCall{
		DependentCXTCall: dependentCall,
		lock:             sync.RWMutex{},
		// waitCh 不再需要，直接使用 dependentCall.WaitCh
	}
	return safeCall
}

// registerRequest 注册当前请求，返回是否达到阈值和是否已有结果
func (sim *Simulator) registerRequest(
	dependentCall *safeDependentCall,
	callReq *safeCallRequest,
	threshold int,
) (reachThreshold bool) {

	dependentCall.lock.Lock()
	defer dependentCall.lock.Unlock()

	// 如果已经满足threshold，则返回
	if len(dependentCall.Requests) >= threshold {
		return false
	}

	// 添加请求和等待 channel
	dependentCall.Requests = append(dependentCall.Requests, callReq.req)

	// 检查是否达到阈值
	// 修复：使用 == 而不是 >=，确保只有恰好达到阈值的请求触发执行，避免重复触发
	if len(dependentCall.Requests) == threshold {
		return true
	}

	return false
}

// executeThresholdAction 达到阈值时执行聚合和远程调用
func (sim *Simulator) executeThresholdAction(
	ctx context.Context,
	txHash common.Hash,
	req *api.CXTCallRequest,
	dependentCall *safeDependentCall,
	committee *api.ShardSimulateCommittee,
) *api.CXTCallSSCResult {

	// 聚合请求
	var sscReq *api.CXTCallSSCRequest
	func() {
		startTime := time.Now()
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msgf("RequestCallCXT: aggregateSSCCallRequest start")
		dependentCall.lock.RLock()
		defer func() {
			dependentCall.lock.RUnlock()
			utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Dur("cost", time.Since(startTime)).Msgf("RequestCallCXT: aggregateSSCCallRequest end")
		}()
		sscReq = sim.aggregateSSCCallRequest(dependentCall.Requests)
	}()

	if sscReq == nil {
		return &api.CXTCallSSCResult{
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{
				Epochs: req.Epochs,
			},
			Err: "failed to aggregate requests",
		}
	}

	// 保存聚合后的请求
	dependentCall.lock.Lock()
	dependentCall.SignedRequest = sscReq
	dependentCall.lock.Unlock()

	utils.SSCLogger().Info().
		Str("txHash", txHash.Hex()).
		Str("callIndex", req.CallIndex.ToString()).
		Interface("epochs", req.Epochs).
		Uint32("targetShard", req.TargetShardId).
		Msg("RequestCallCXT: executing threshold action")

	// 获取 leader 并发起远程调用
	leader := sim.committee.GetLeader(req.Epochs[req.TargetShardId], req.TargetShardId)

	ret := new(api.CXTCallSSCResult)
	defer func() {
		// 保存结果并通知所有等待的请求
		dependentCall.lock.Lock()
		dependentCall.SSCResult = ret
		utils.SSCLogger().Info().
			Str("txHash", txHash.Hex()).
			Str("callIndex", req.CallIndex.ToString()).
			Str("waitChAddr", fmt.Sprintf("%p", dependentCall.WaitCh)).
			Msg("RequestCallCXT: closing WaitCh to notify waiters")
		dependentCall.CloseOnce.Do(func() {
			close(dependentCall.WaitCh) // ✅ 使用 DependentCXTCall.WaitCh
		})
		dependentCall.lock.Unlock()
	}()

	// 检查状态是否仍然有效
	txState, err := sim.state.GetTxState(txHash)

	if err != nil || txState == nil {
		utils.SSCLogger().Error().
			Str("txHash", txHash.Hex()).
			Str("callIndex", req.CallIndex.ToString()).
			Msg("RequestCallCXT: transaction has been closed")
		return &api.CXTCallSSCResult{
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{
				Epochs: req.Epochs,
			}, Err: "transaction has been closed"}
	}

	// 发起远程调用
	callCtx, callCancel := context.WithTimeout(ctx, sim.config.CallTimeout)
	defer callCancel()

	utils.SSCLogger().Info().
		Str("txHash", txHash.Hex()).
		Str("callIndex", req.CallIndex.ToString()).
		Str("leader", leader.Endpoint).
		Msg("RequestCallCXT: initiating remote call")

	err = sim.communicator.comm.Call(callCtx, ret, leader, api.Method_HandleCXTSSCCall, sscReq)
	if err != nil {
		utils.SSCLogger().Error().
			Err(err).
			Str("txHash", txHash.Hex()).
			Msg("RequestCallCXT: remote call failed")
		return &api.CXTCallSSCResult{
			Err:                  fmt.Sprintf("remote call error: %v", err),
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{Epochs: req.Epochs},
		}
	}

	if ret.Err != "" {
		utils.SSCLogger().Error().
			Str("txHash", txHash.Hex()).
			Str("err", ret.Err).
			Str("callIndex", req.CallIndex.ToString()).
			Str("relatedShards", fmt.Sprintf("%v", ret.RelatedShards)).
			Msg("RequestCallCXT: remote call returned error")
	}

	return ret
}

// waitForResult 等待结果（带超时）
func (sim *Simulator) waitForResult(
	ctx context.Context,
	callReq *safeCallRequest,
	dependentCall *safeDependentCall,
) *api.CXTCallSSCResult {

	utils.SSCLogger().Info().
		Str("txHash", callReq.req.TxHash.Hex()).
		Str("callIndex", callReq.req.CallIndex.ToString()).
		Str("waitChAddr", fmt.Sprintf("%p", dependentCall.WaitCh)).
		Msg("RequestCallCXT: waiting for result")

	select {
	case <-dependentCall.WaitCh: // ✅ 使用 DependentCXTCall.WaitCh
		utils.SSCLogger().Info().
			Str("txHash", callReq.req.TxHash.Hex()).
			Str("callIndex", callReq.req.CallIndex.ToString()).
			Str("waitChAddr", fmt.Sprintf("%p", dependentCall.WaitCh)).
			Bool("hasResult", dependentCall.SSCResult != nil).
			Msg("RequestCallCXT: wait finished, got result")
		return dependentCall.SSCResult
	case <-ctx.Done():
		utils.SSCLogger().Warn().
			Err(ctx.Err()).
			Str("txHash", callReq.req.TxHash.Hex()).
			Str("callIndex", callReq.req.CallIndex.ToString()).
			Str("waitChAddr", fmt.Sprintf("%p", dependentCall.WaitCh)).
			Msg("RequestCallCXT: wait timeout")
		return &api.CXTCallSSCResult{
			Err:                  fmt.Sprintf("wait timeout: %v", ctx.Err()),
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{Epochs: callReq.req.Epochs},
		}
	}
}

// ========================================================================
// startCall ⸺ 从 impl.go 搬迁而来
// ========================================================================

// startCall 启动跨分片调用，创建或获取 SimulationCallState。
// 搬迁自 sscService.startCall (impl.go:865-959)
func (sim *Simulator) startCall(req *api.CXTCallSSCRequest) (*api.SimulationCallState, error) {
	// try to start cxt if not exist
	txHash := req.TxHash
	_, err := sim.startCXT(txHash, req.SimulationNum, req.OriginShardId, req.RelatedShards, nil, req)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to start cxt")
		return nil, err
	}

	tx, err := sim.state.GetTxState(txHash)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to get state")
		return nil, err
	}
	simState, _ := sim.GetSimState(txHash)
	if simState == nil {
		utils.SSCLogger().Error().Msg("failed to get simulation state")
		return nil, fmt.Errorf("simulation state not found")
	}

	// check if callIndex is already in progress, don't start again
	if simState.SimulationCallStates[tx.SimulationNum].Get(req.CallIndex) != nil {
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("callIndex has been started, simulationNum=%d, callIndex=%s", tx.SimulationNum, req.CallIndex.ToString())
		return simState.SimulationCallStates[tx.SimulationNum].Get(req.CallIndex), nil
	}
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Str("simulatedCallStates", simState.SimulationCallStates[tx.SimulationNum].ToString()).
		Msgf("start call, simulationNum=%d, callIndex=%s", tx.SimulationNum, req.CallIndex.ToString())
	callState := simState.SimulationCallStates[req.SimulationNum].Get(req.CallIndex)

	if callState == nil {
		callState = &api.SimulationCallState{
			SimulationNum:     req.SimulationNum,
			BlockNum:          req.BlockNum,
			BlockHash:         req.BlockHash,
			CallIndex:         req.CallIndex,
			CallRequest:       req,
			DependentCXTCalls: make(map[string]*api.DependentCXTCall),
			RWSet:             newRWSet(),
			Result:            nil,
			CallSSCResult:     nil,
			Executed:          false,
			Lock:              sync.Mutex{},
		}
		header := sim.bc.GetHeaderByHash(req.BlockHash)
		// need to sync state
		if header == nil {
			utils.SSCLogger().Debug().Msgf("block header not found, waiting for sync")
			sim.waitForSync(txHash, callState)
			utils.SSCLogger().Debug().Msgf("sync completed, block header founded")
			header = sim.bc.GetHeaderByHash(req.BlockHash)
		}
		stateAt, err := sim.bc.StateAt(header.Root())
		if err != nil {
			utils.SSCLogger().Error().Err(err).Msg("failed to get stateDB")
			return nil, err
		}
		callState.DB = stateAt

		// re-read simState after potential sync
		tx, err = sim.state.GetTxState(txHash)
		if err != nil {
			utils.SSCLogger().Error().Err(err).Msg("failed to get state")
			return nil, err
		}
		simState, _ = sim.GetSimState(txHash)
		if simState != nil && simState.SimulationCallStates[req.SimulationNum] != nil {
			simState.SimulationCallStates[req.SimulationNum] = simState.SimulationCallStates[req.SimulationNum].Add(callState)
		}

		// 处理 pending requests
		sim.pendingLock.Lock()
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Str("callIndex", req.CallIndex.ToString()).Msgf("add callState, simulationNum=%d, callIndex=%s", tx.SimulationNum, req.CallIndex.ToString())
		pendingList := sim.pendingRequests[txHash]
		sim.pendingRequests[txHash] = make([]*pendingCXTRequest, 0)
		if len(pendingList) > 0 {
			for _, p := range pendingList {
				if p.req.CallIndex[:len(p.req.CallIndex)-1].ToString() == callState.CallIndex.ToString() {
					utils.SSCLogger().Info().
						Str("txHash", txHash.Hex()).
						Str("callIndex", req.CallIndex.ToString()).
						Msg("processing pending CXT request")
					go func(req *api.CXTCallRequest, ch chan *api.CXTCallSSCResult) {
						result := sim.RequestCallCXT(req) // 递归调用，此时状态已存在
						utils.SSCLogger().Info().
							Str("txHash", txHash.Hex()).
							Str("callIndex", req.CallIndex.ToString()).
							Msg("finished processing pending CXT request")
						ch <- result
					}(p.req, p.waitingCh)
				} else {
					sim.pendingRequests[txHash] = append(sim.pendingRequests[txHash], p)
				}
			}
		}
		sim.pendingLock.Unlock()
	}
	return callState, nil
}

// ========================================================================
// Member 节点函数（迁移自 simulator_leader.go）
// ========================================================================

// CallCXTContract 调用跨分片合约。
//
// Original: sscService.CallCXTContract (4b6d266c7)
func (sim *Simulator) CallCXTContract(req *api.CXTCallRequest) *api.CXTCallSSCResult {
	txHash := req.TxHash
	targetShardId := req.TargetShardId
	var (
		cxtState      *api.TxState
		simState      *api.SimulationState
		dependentCall *api.DependentCXTCall
		err           error
		committee     *api.ShardSimulateCommittee
	)
	sscResult := func() *api.CXTCallSSCResult {
		cxtState, err = sim.state.GetTxState(txHash)
		if err != nil {
			utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).Msgf("failed to get state")
			return &api.CXTCallSSCResult{
				BaseBLSSignedMessage: api.BaseBLSSignedMessage{
					Epochs: req.Epochs,
				}, Err: err.Error()}
		}
		simState, _ = sim.GetSimState(txHash)

		req.Nonce = cxtState.Nonce
		req.TxSender = cxtState.TxSender.Bytes()
		req.SimulationNum = cxtState.SimulationNum
		req.OriginShardId = cxtState.OriginShardId
		req.FromShardId = sim.committee.SelfShard
		req.Epochs = cxtState.Epochs
		committee = sim.committee.GetCommittee(req.Epochs[sim.committee.SelfShard], sim.committee.SelfShard)

		// 检查 CurrentCallFrame 是否为 nil，避免空指针
		if simState == nil || simState.CurrentCallFrame == nil {
			utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
				Msg("CurrentCallFrame is nil, this should not happen")
			return &api.CXTCallSSCResult{
				BaseBLSSignedMessage: api.BaseBLSSignedMessage{
					Epochs: req.Epochs,
				}, Err: "CurrentCallFrame is nil"}
		}

		req.CallIndex = make(api.CallIndex, len(simState.CurrentCallFrame.CallIndex))
		copy(req.CallIndex, simState.CurrentCallFrame.CallIndex)
		req.CallIndex = append(req.CallIndex, simState.CurrentCallFrame.PC)
		req.RelatedShards = cxtState.RelatedShards.Add(req.TargetShardId)
		simState.CurrentCallFrame.Next()

		// if it's recall and callState is locked
		if cxtState.SimulationNum > 0 && req.CallIndex.Compare(simState.LockedCallIndex) <= 0 {
			utils.SSCLogger().Debug().Str("txHash", txHash.String()).
				Str("callIndex", req.CallIndex.ToString()).
				Msg("recall contract has been locked, reuse it")
			callStates := simState.SimulationCallStates[req.SimulationNum]
			callerState := callStates.Get(req.CallIndex[:len(req.CallIndex)-1])
			if callerState == nil {
				return &api.CXTCallSSCResult{
					BaseBLSSignedMessage: api.BaseBLSSignedMessage{
						Epochs: req.Epochs,
					}, Err: "no call state found"}
			}

			callerState.StateLock.Lock()
			defer callerState.StateLock.Unlock()
			lockedCallStates := simState.SimulationCallStates[cxtState.SimulationNum-1]
			dependentCall = lockedCallStates.Get(req.CallIndex).DependentCXTCalls[req.CallIndex.ToString()]
			callerState.DependentCXTCalls[req.CallIndex.ToString()] = dependentCall
			ret := callerState.DependentCXTCalls[req.CallIndex.ToString()].SSCResult
			return ret
		}
		callStates := simState.SimulationCallStates[req.SimulationNum]
		// get caller's call state
		callerState := callStates.Get(req.CallIndex[:len(req.CallIndex)-1])
		if callerState == nil {
			utils.SSCLogger().Debug().Str("txHash", txHash.String()).
				Interface("callStates", callStates).
				Str("callIndex", req.CallIndex.ToString()).
				Msg("no caller's call state found")
			return &api.CXTCallSSCResult{
				BaseBLSSignedMessage: api.BaseBLSSignedMessage{
					Epochs: req.Epochs,
				}, Err: "no call state found"}
		}

		// 使用独立的 depCallLock 保护 DependentCXTCalls 访问
		callerState.StateLock.Lock()
		defer callerState.StateLock.Unlock()
		dependentCall = callerState.DependentCXTCalls[req.CallIndex.ToString()]
		if dependentCall != nil && callerState.Executed {
			ret := callerState.CallSSCResult
			return ret
		}
		if dependentCall == nil {
			dependentCall = &api.DependentCXTCall{
				CallIndex:     req.CallIndex,
				Requests:      make([]*api.CXTCallRequest, 0, committee.Threshold),
				SignedRequest: nil,
				SSCResult:     nil,
				WaitCh:        make(chan struct{}), // ✅ 初始化 WaitCh
			}
			callerState.DependentCXTCalls[req.CallIndex.ToString()] = dependentCall
		}
		return nil
	}()

	if sscResult != nil {
		if simState != nil {
			subNode := new(api.CallNode)
			subNode.FromData(txHash, sscResult.TreeNode)
			simState.CallForest.Insert(subNode)
		}
		return sscResult
	}

	sign, err := sim.communicator.signerMgr.GetSSCSigner().Sign(req)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.String()).
			Str("callIndex", req.CallIndex.ToString()).Err(err).Msg("failed to sign cxt call ssc request")
		return nil
	}
	req.BaseSSCMessage.SenderAddr = sim.communicator.signerMgr.GetSSCSigner().Address()
	req.BaseSSCMessage.Signature = sign
	req.BaseSSCMessage.Epochs = cxtState.Epochs

	leader := sim.committee.GetLeader(req.Epochs[sim.committee.SelfShard], sim.committee.SelfShard)
	startTime := time.Now()
	utils.SSCLogger().Info().
		Str("txHash", txHash.String()).
		Int("simulationNum", req.SimulationNum).
		Str("reqCallIndex", req.CallIndex.ToString()).
		Interface("relatedShards", req.RelatedShards).
		Uint32("selfShard", sim.committee.SelfShard).
		Uint32("targetShard", targetShardId).
		Str("addr", req.Addr.Hex()).
		Uint64("gas", req.Gas).
		Str("callIndex", req.CallIndex.ToString()).Msg("call cxt contract, start")

	ctx, cancel := context.WithTimeout(cxtState.Ctx, sim.config.CallTimeout)
	defer cancel()
	ret := new(api.CXTCallSSCResult)
	err = sim.communicator.comm.Call(ctx, ret, leader, api.Method_RequestCallCXT, req)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			utils.SSCLogger().Error().Err(err).Msg("failed to call cxt contract")
		}
		utils.SSCLogger().Info().Str("txHash", txHash.String()).
			Str("callIndex", req.CallIndex.ToString()).
			Uint32("targetShard", targetShardId).
			Dur("cost", time.Since(startTime)).
			Msg("call cxt contract, end")
		ret = &api.CXTCallSSCResult{
			Err: fmt.Sprintf("error from targetShard %d, err=%s", targetShardId, err.Error()),
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{
				Epochs: req.Epochs,
			},
		}
		dependentCall.SSCResult = ret
	} else {
		if ret.Err != "" {
			if ret.Err != context.Canceled.Error() {
				utils.SSCLogger().Error().Str("txHash", txHash.String()).
					Str("callIndex", req.CallIndex.ToString()).Err(errors.New(ret.Err)).Msgf("failed to call cxt contract, targetShardId: %d", req.TargetShardId)
			}
		} else {
			err = sim.communicator.signerMgr.GetSSCSigner().Verify(ret)
			if err != nil {
				utils.SSCLogger().Error().Str("txHash", txHash.String()).
					Str("callIndex", req.CallIndex.ToString()).Err(err).Msg("failed to verify cxt call ssc result signature")
				utils.SSCLogger().Info().Str("txHash", txHash.String()).
					Str("callIndex", req.CallIndex.ToString()).
					Uint32("targetShard", targetShardId).
					Dur("cost", time.Since(startTime)).
					Msg("call cxt contract, end")
				// ✅ 即使验证失败，也要合并 RelatedShards
				if ret.RelatedShards != nil && len(ret.RelatedShards) > 0 {
					sim.state.MergeRelatedShards(txHash, ret.RelatedShards)
				}
				ret = &api.CXTCallSSCResult{
					Err: fmt.Sprintf("error from targetShard %d, err=%s", targetShardId, err.Error()),
					BaseBLSSignedMessage: api.BaseBLSSignedMessage{
						Epochs: req.Epochs,
					},
				}
				dependentCall.SSCResult = ret
				subNode := new(api.CallNode)
				subNode.FromData(txHash, ret.TreeNode)
				simState.CallForest.Insert(subNode)
				return ret
			}
		}
		func() {
			dependentCall.SSCResult = ret
			// ✅ 合并 RelatedShards（无论成功失败）
			oldRelatedShards := cxtState.RelatedShards
			cxtState.RelatedShards = cxtState.RelatedShards.Merge(ret.RelatedShards)
			utils.SSCLogger().Info().Str("txHash", txHash.String()).
				Str("callIndex", req.CallIndex.ToString()).
				Uint32("targetShard", targetShardId).
				Str("returnRelatedShards", fmt.Sprintf("%v", ret.RelatedShards)).
				Str("old", fmt.Sprintf("%v", oldRelatedShards)).
				Str("new", fmt.Sprintf("%v", cxtState.RelatedShards)).
				Dur("cost", time.Since(startTime)).
				Msg("call cxt contract, end")
		}()
	}
	subNode := new(api.CallNode)
	subNode.FromData(txHash, ret.TreeNode)
	simState.CallForest.Insert(subNode)
	// wait for sub execution finished
	subNode.Wait(sim.committee.SelfShard)
	return ret
}

// ========================================================================
// Member 节点函数（迁移自 simulator_leader.go）- HandleSimulateRequest
// ========================================================================

// HandleSimulateRequest 处理模拟请求（非 Leader 节点）。
//
// Original: sscService.HandleSimulateRequest (4b6d266c7)
func (sim *Simulator) HandleSimulateRequest(ctx context.Context, req *api.CXTSimulationRequest) *api.CXTSimulationResult {
	startTime := time.Now()
	txHash := req.TxHash
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Int("simulationNum", req.SimulationNum).Msg("handle simulate request, start")
	defer func() {
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Dur("cost", time.Since(startTime)).Msg("handle simulate request, end")
	}()
	simuState, callState, err := sim.startSimulation(req)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).Msg("failed to start simulation")
		return &api.CXTSimulationResult{
			Err:            err.Error(),
			BaseSSCMessage: api.BaseSSCMessage{Epochs: req.Epochs},
		}
	}

	// 获取 SimulationState 来设置 CurrentCallFrame
	simState, ok := sim.GetSimState(txHash)
	if !ok || simState == nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msg("failed to get simulation state")
		return &api.CXTSimulationResult{
			Err:            "simulation state not found",
			BaseSSCMessage: api.BaseSSCMessage{Epochs: req.Epochs},
		}
	}
	simState.CurrentCallFrame = &api.CallFrame{
		CallIndex: api.CallIndex{},
		PC:        0,
	}
	simState.CallStack.Push(simState.CurrentCallFrame)

	defer func() {
		simState.CallStack.Pop()
		simState.CurrentCallFrame = simState.CallStack.Top()
	}()

	callState.Lock.Lock()
	defer callState.Lock.Unlock()

	relatedShards := simuState.RelatedShards
	simState.SimulateCh = make(chan struct{})
	ch := simState.SimulateCh

	rootNode := api.NewCallNode(txHash, api.CallIndex{}, sim.committee.SelfShard)
	simState.CallForest.Insert(rootNode)

	defer func() {
		rootNode.Done()
		rootNode.Wait(sim.committee.SelfShard)
		close(ch)
	}()

	header := sim.bc.GetHeaderByHash(req.BlockHash)
	if header == nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msg("header not found")
		return &api.CXTSimulationResult{
			Err:            "header not found",
			BaseSSCMessage: api.BaseSSCMessage{Epochs: req.Epochs},
		}
	}
	stateDB, _ := sim.bc.StateAt(header.Root())
	if stateDB == nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msg("stateDB not found")
		return &api.CXTSimulationResult{Err: "stateDB not found",
			BaseSSCMessage: api.BaseSSCMessage{Epochs: req.Epochs}}
	}
	chainConfig := sim.bc.Config()
	vmConfig := sim.bc.GetVMConfig()

	sim.timerMgr.StartPoolTimer(txHash, req.Epochs, header.NumberU64(), sim.committee.SelfShard)

	var (
		gp            *core.GasPool
		tx            *types.Transaction
		executionType vm.ExecutionType
	)

	if req.SimulationNum == 0 {
		tx = req.Tx
		gp = new(core.GasPool).AddGas(req.GasPool)
		executionType = vm.SimulationCall
		// init cxt stateDB
	} else {
		tx = simState.SimulationRequest.Tx
		gp = new(core.GasPool).AddGas(simState.SimulationRequest.GasPool)
		executionType = vm.SimulationReCall
	}

	var signer types.Signer
	if tx.IsEthCompatible() {
		if !chainConfig.IsEthCompatible(header.Epoch()) {
			return &api.CXTSimulationResult{Err: "ethereum compatible transactions not supported at current epoch",
				BaseSSCMessage: api.BaseSSCMessage{Epochs: req.Epochs}}
		}
		signer = types.NewEIP155Signer(chainConfig.EthCompatibleChainID)
	} else {
		signer = types.MakeSigner(chainConfig, header.Epoch())
	}
	msg, err := tx.AsMessage(signer)

	if err != nil {
		return &api.CXTSimulationResult{Err: err.Error(),
			BaseSSCMessage: api.BaseSSCMessage{Epochs: req.Epochs}}
	}

	utils.SSCLogger().Debug().
		Str("txHash", txHash.String()).
		Str("from", msg.From().String()).
		Msg("simulate contract with sscvm")

	var ret *api.CXTSimulationResult
	vmCtx := core.NewSSCVMContext(msg.From(), tx.Hash(), api.CallIndex{}, tx.GasPrice(), header, sim.bc, req.Author)
	vmCtx.TxType = types.CXTransaction
	sscvm := vm.NewSSCVM(vmCtx, stateDB, chainConfig, *vmConfig, sim.sscService, executionType)
	result, err := core.NewSSCStateTransition(sscvm, msg, gp).TransitionDb()
	if err != nil {
		ret = &api.CXTSimulationResult{Err: err.Error(), BaseSSCMessage: api.BaseSSCMessage{Epochs: req.Epochs}}
	} else {
		newSimuState, err := sim.state.GetTxState(txHash)
		if err != nil {
			utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).Msg("failed to get state")
			return &api.CXTSimulationResult{
				Err:            err.Error(),
				BaseSSCMessage: api.BaseSSCMessage{Epochs: req.Epochs},
			}
		}
		relatedShards = newSimuState.RelatedShards

		ret = &api.CXTSimulationResult{
			RelatedShards: relatedShards,
			Result:        result.ReturnData,
			Receipt:       nil,
			UsedGas:       result.UsedGas,
			Err:           "",
			BaseSSCMessage: api.BaseSSCMessage{
				Epochs: req.Epochs,
			},
		}
		if result.VMErr != nil {
			if vm.IsLockConflictErr(result.VMErr.Error()) && sim.timerMgr.GetTimeoutConfig() != nil && sim.timerMgr.GetTimeoutConfig().ForceSimulation {
				// ForceSimulation: conflict but execution completed, mark conflict keys
				utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
					Msgf("ForceSimulation: lock conflict, continuing execution")
				if callState.LockedByOtherTx != nil && len(callState.LockedKeys) > 0 {
					ret.ConflictKeys = callState.LockedKeys
				}
				ret.Err = "" // don't report error — we have full RWSet
			} else {
				ret.Err = result.VMErr.Error()
			}
		}
		if callState.LockedByOtherTx != nil && ret.Err == "" {
			if sim.timerMgr.GetTimeoutConfig() != nil && sim.timerMgr.GetTimeoutConfig().ForceSimulation {
				// Already handled above
			} else {
				ret.Err = api.ErrLockConflict_OnChain.Error()
			}
		}
	}
	sign, err := sim.communicator.signerMgr.GetSSCSigner().Sign(ret)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Str("txHash", txHash.Hex()).Msg("failed to sign simulation result")
		return nil
	}
	ret.BaseSSCMessage.Signature = sign
	ret.BaseSSCMessage.SenderAddr = sim.communicator.signerMgr.GetSSCSigner().Address()

	return ret
}
