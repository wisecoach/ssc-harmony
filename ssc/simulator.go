package ssc

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/core"
	corestate "github.com/harmony-one/harmony/core/state"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
)

// SimulatorStateAccessor 封装 Simulator 需要的 sscService 公共状态访问。
// 所有操作内部自带锁，Simulator 不持有 sscService 的锁对象。
type SimulatorStateAccessor struct {
	// GetTxState 读取交易公共状态（只读，不持有锁）
	GetTxState func(txHash common.Hash) (*api.TxState, error)
	// SetTxStatus 更新交易状态
	SetTxStatus func(txHash common.Hash, status api.CXTStatus)
	// MergeRelatedShards 合并 RelatedShards（返回合并后的值）
	MergeRelatedShards func(txHash common.Hash, newShards api.RelatedShards) api.RelatedShards
	// SetSimulationNum 更新 SimulationNum
	SetSimulationNum func(txHash common.Hash, num int)
	// CreateTxState 创建新的 TxState（在 startSimulation 时调用）
	CreateTxState func(txHash common.Hash, txState *api.TxState)
	// InitCommitState 初始化 txHash 对应的 CommitState（内部加锁）
	InitCommitState func(txHash common.Hash) *api.CommitState
	// GetCommitState 获取 txHash 对应的 CommitState（只读，调用方确保 txHash 存在）
	GetCommitState func(txHash common.Hash) *api.CommitState
	// GetFinishedTxs 获取 finishedTxs 指针
	GetFinishedTxs func() map[common.Hash]bool

	// CallForRetry 调度重试（原 s.retryScheduler.CallForRetry）
	CallForRetry func(tx *api.RetryTx)
}

// simuChannels 每笔交易独立的模拟结果通知通道，替代全局 simuLock 保护
type simuChannels struct {
	mu         sync.Mutex
	waitingChs []chan *api.CXTSimulationSSCResult
	resultCh   chan *api.CXTSimulationSSCResult
}

// pendingRequestList 每笔交易独立的 pending 请求列表，替代全局 pendingLock 保护
type pendingRequestList struct {
	mu   sync.Mutex
	reqs []*pendingCXTRequest
}

// Simulator 负责跨分片交易的模拟执行。
// 自管理 SimulationState、callStatesInWaiting、worker pool 等存储。
type Simulator struct {
	state SimulatorStateAccessor

	// 自管理存储：SimulationState（per-tx sync.Map 替代全局 lm.RWMutex）
	simStates sync.Map // key: common.Hash, value: *api.SimulationState

	// 自管理存储：call 同步
	syncLock            sync.Mutex
	callStatesInWaiting map[common.Hash][]*api.SimulationCallState

	// 自管理存储：simulation 结果通知（per-tx sync.Map）
	simuChMap sync.Map // key: common.Hash, value: *simuChannels

	// 自管理存储：pending requests（per-tx sync.Map）
	pendingReqsMap sync.Map // key: common.Hash, value: *pendingRequestList

	// worker pool
	simulateSemaphore chan struct{}
	simulateTaskPQ    *simulationReqPriorityQueue
	workerWg          sync.WaitGroup
	numWorkers        int

	// simulation statistics
	simuStatsLock     sync.Mutex
	simulatingCount   int64
	totalSimulations  int64
	totalSimuDuration time.Duration

	// 依赖
	sscService   *sscService
	communicator *SimulatorCommunicator
	timerMgr     *CXTTimerManager
	committee    *CommitteeMechanism
	config       *api.Config
	bc           core.BlockChain
	stats        *SimulationStats
	ctx          context.Context
}

// SimulatorCommunicator 封装 Simulator 需要的跨节点通信能力。
type SimulatorCommunicator struct {
	comm      *Comm
	signerMgr api.BLSSignerMgr
	committee *CommitteeMechanism
	config    *api.Config
	ctx       context.Context
}

func NewSimulatorCommunicator(comm *Comm, signerMgr api.BLSSignerMgr, committee *CommitteeMechanism, config *api.Config, ctx context.Context) *SimulatorCommunicator {
	return &SimulatorCommunicator{
		comm:      comm,
		signerMgr: signerMgr,
		committee: committee,
		config:    config,
		ctx:       ctx,
	}
}

func NewSimulator(
	state SimulatorStateAccessor,
	sscService *sscService,
	communicator *SimulatorCommunicator,
	timerMgr *CXTTimerManager,
	committee *CommitteeMechanism,
	config *api.Config,
	bc core.BlockChain,
	stats *SimulationStats,
	ctx context.Context,
	numWorkers int,
) *Simulator {
	return &Simulator{
		state:               state,
		sscService:          sscService,
		callStatesInWaiting: make(map[common.Hash][]*api.SimulationCallState),
		simulateSemaphore:   make(chan struct{}, numWorkers),
		simulateTaskPQ:      newSimulationReqPriorityQueue(),
		numWorkers:          numWorkers,
		communicator:        communicator,
		timerMgr:            timerMgr,
		committee:           committee,
		config:              config,
		bc:                  bc,
		stats:               stats,
		ctx:                 ctx,
	}
}

// ===== SimulationState 存储访问 =====

// GetSimState 读取 SimulationState（sync.Map 原子读）
func (sim *Simulator) GetSimState(txHash common.Hash) (*api.SimulationState, bool) {
	val, ok := sim.simStates.Load(txHash)
	if !ok {
		return nil, false
	}
	return val.(*api.SimulationState), true
}

// SetSimState 写入 SimulationState（sync.Map 原子写）
func (sim *Simulator) SetSimState(txHash common.Hash, state *api.SimulationState) {
	sim.simStates.Store(txHash, state)
}

// DeleteSimState 删除 SimulationState（sync.Map 原子删）
func (sim *Simulator) DeleteSimState(txHash common.Hash) {
	sim.simStates.Delete(txHash)
}

// Stats 返回 Simulator 各存储的条目数，供监控分析资源释放情况。
func (sim *Simulator) Stats() (simStates, callStatesWaiting, simuChMap, pendingReqs, queueLen int) {
	if sim == nil {
		return 0, 0, 0, 0, 0
	}
	sim.simStates.Range(func(_, _ interface{}) bool { simStates++; return true })
	sim.simuChMap.Range(func(_, _ interface{}) bool { simuChMap++; return true })
	sim.pendingReqsMap.Range(func(_, _ interface{}) bool { pendingReqs++; return true })
	sim.syncLock.Lock()
	callStatesWaiting = len(sim.callStatesInWaiting)
	sim.syncLock.Unlock()
	if sim.simulateTaskPQ != nil {
		queueLen = sim.simulateTaskPQ.Len()
	}
	return
}

func (sim *Simulator) GetBlockHash(txHash common.Hash) (common.Hash, bool) {
	state, ok := sim.GetSimState(txHash)
	if !ok {
		return common.Hash{}, false
	}
	if state.SimulationRequest != nil {
		return state.SimulationRequest.BlockHash, true
	}
	if len(state.SimulationCallStates) > 0 {
		for _, callState := range state.SimulationCallStates[len(state.SimulationCallStates)-1] {
			return callState.BlockHash, true
		}
	}
	return common.Hash{}, false
}

// Cleanup 清理 Simulator 中该交易的所有资源。
// 由 sscService.closeTransaction 调用。
func (sim *Simulator) Cleanup(txHash common.Hash) {
	t0 := time.Now()
	sim.DeleteSimState(txHash)
	tDelSim := time.Since(t0)

	// 清理 pending requests（原子删除，无需 per-entry lock）
	sim.pendingReqsMap.Delete(txHash)
	tPending := time.Since(t0)

	// 清理 simulation 结果 channel（原子删除，无需 per-entry lock）
	sim.simuChMap.Delete(txHash)

	// 清理 call states in waiting
	sim.syncLock.Lock()
	delete(sim.callStatesInWaiting, txHash)
	sim.syncLock.Unlock()

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Str("delSimState", tDelSim.String()).
		Str("pendingLock", tPending.String()).
		Msg("Simulator.Cleanup timing")
}

// ===== callStatesInWaiting 存储访问 =====

// BuildCallStates 从 SimulationCallStates 构建 []*api.CXTCallState。
// 在 CommitSimulation 中调用，用于构建 Simulation tx 的 call states。
func (sim *Simulator) BuildCallStates(txHash common.Hash, simulationNum int) ([]*api.CXTCallState, error) {
	simState, ok := sim.GetSimState(txHash)
	if !ok || simState == nil {
		return nil, fmt.Errorf("simulation state not found for %s", txHash.Hex())
	}

	simulationCallStates := simState.SimulationCallStates[simulationNum]
	callStates := make([]*api.CXTCallState, 0, len(simulationCallStates))
	for i, simulationCallState := range simulationCallStates {
		depentResults := make([]*api.CXTCallSSCResult, 0)
		for _, dc := range simulationCallState.DependentCXTCalls {
			depentResults = append(depentResults, dc.SSCResult)
			if dc.SSCResult == nil {
				return nil, fmt.Errorf("dependent callIndex %s has no result", dc.CallIndex.ToString())
			}
		}
		sort.Slice(depentResults, func(i, j int) bool {
			return depentResults[i].CallIndex.Compare(depentResults[j].CallIndex) < 0
		})
		callState := &api.CXTCallState{
			CallIndex:        simulationCallState.CallIndex,
			TopRequest:       simulationCallState.TopRequest,
			CallRequest:      simulationCallState.CallRequest,
			RWSet:            simulationCallState.RWSet,
			DependentResults: depentResults,
			CallResult:       simulationCallState.CallSSCResult,
			TopResult:        simulationCallState.TopSSCResult,
		}
		callStates = append(callStates, callState)
		if callState.TopResult == nil && callState.CallResult == nil {
			return nil, fmt.Errorf("callIndex %s has no result", simulationCallState.CallIndex.ToString())
		}
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("build commit simulation [%d/%d], callIndex: %s, rwset: %v",
			i+1, len(simulationCallStates), simulationCallState.CallIndex.ToString(), simulationCallState.RWSet.WriteState.State)
	}
	return callStates, nil
}

// PopCallStatesInWaiting 取出并删除等待中的 callStates
func (sim *Simulator) PopCallStatesInWaiting(txHash common.Hash) []*api.SimulationCallState {
	sim.syncLock.Lock()
	defer sim.syncLock.Unlock()
	states := sim.callStatesInWaiting[txHash]
	delete(sim.callStatesInWaiting, txHash)
	return states
}

// AddCallStatesInWaiting 添加等待中的 callState
func (sim *Simulator) AddCallStatesInWaiting(txHash common.Hash, callState *api.SimulationCallState) {
	sim.syncLock.Lock()
	defer sim.syncLock.Unlock()
	sim.callStatesInWaiting[txHash] = append(sim.callStatesInWaiting[txHash], callState)
}

// ===== simuChannels 存储访问（per-tx sync.Map） =====

// getOrCreateChannels 获取或创建 per-tx 的 simuChannels
func (sim *Simulator) getOrCreateChannels(txHash common.Hash) *simuChannels {
	val, _ := sim.simuChMap.LoadOrStore(txHash, &simuChannels{})
	return val.(*simuChannels)
}

// getChannels 只读获取（不创建）
func (sim *Simulator) getChannels(txHash common.Hash) *simuChannels {
	val, ok := sim.simuChMap.Load(txHash)
	if !ok {
		return nil
	}
	return val.(*simuChannels)
}

// GetSimuResultCh 获取 simulation 结果 channel
func (sim *Simulator) GetSimuResultCh(txHash common.Hash) chan *api.CXTSimulationSSCResult {
	ch := sim.getChannels(txHash)
	if ch == nil {
		return nil
	}
	ch.mu.Lock()
	ret := ch.resultCh
	ch.mu.Unlock()
	return ret
}

// SetSimuResultCh 设置 simulation 结果 channel
func (sim *Simulator) SetSimuResultCh(txHash common.Hash, ch chan *api.CXTSimulationSSCResult) {
	c := sim.getOrCreateChannels(txHash)
	c.mu.Lock()
	c.resultCh = ch
	c.mu.Unlock()
}

// DeleteSimuResultCh 删除 simulation 结果 channel
func (sim *Simulator) DeleteSimuResultCh(txHash common.Hash) {
	if ch := sim.getChannels(txHash); ch != nil {
		ch.mu.Lock()
		ch.resultCh = nil
		ch.mu.Unlock()
	}
}

// AddSimuWaitingCh 添加等待通知的 channel
func (sim *Simulator) AddSimuWaitingCh(txHash common.Hash, ch chan *api.CXTSimulationSSCResult) {
	c := sim.getOrCreateChannels(txHash)
	c.mu.Lock()
	c.waitingChs = append(c.waitingChs, ch)
	c.mu.Unlock()
}

// PopSimuWaitingChs 取出并删除所有等待的 channel
func (sim *Simulator) PopSimuWaitingChs(txHash common.Hash) []chan *api.CXTSimulationSSCResult {
	c := sim.getChannels(txHash)
	if c == nil {
		return nil
	}
	c.mu.Lock()
	chs := c.waitingChs
	c.waitingChs = nil
	c.mu.Unlock()
	return chs
}

// ===== pendingRequests 存储访问（per-tx sync.Map） =====

// getOrCreatePendingReqs 获取或创建 per-tx 的 pendingRequestList
func (sim *Simulator) getOrCreatePendingReqs(txHash common.Hash) *pendingRequestList {
	val, _ := sim.pendingReqsMap.LoadOrStore(txHash, &pendingRequestList{})
	return val.(*pendingRequestList)
}

// getPendingReqs 只读获取（不创建）
func (sim *Simulator) getPendingReqs(txHash common.Hash) *pendingRequestList {
	val, ok := sim.pendingReqsMap.Load(txHash)
	if !ok {
		return nil
	}
	return val.(*pendingRequestList)
}

// PopPendingRequests 取出并删除 pending requests
func (sim *Simulator) PopPendingRequests(txHash common.Hash) []*pendingCXTRequest {
	val, ok := sim.pendingReqsMap.LoadAndDelete(txHash)
	if !ok {
		return nil
	}
	l := val.(*pendingRequestList)
	l.mu.Lock()
	reqs := l.reqs
	l.reqs = nil
	l.mu.Unlock()
	return reqs
}

// AddPendingRequest 添加 pending request
func (sim *Simulator) AddPendingRequest(txHash common.Hash, req *pendingCXTRequest) {
	l := sim.getOrCreatePendingReqs(txHash)
	l.mu.Lock()
	l.reqs = append(l.reqs, req)
	l.mu.Unlock()
}

// ===== Worker Pool =====

// StartWorkers 启动 worker goroutine pool
func (sim *Simulator) StartWorkers() {
	for i := 0; i < sim.numWorkers; i++ {
		sim.workerWg.Add(1)
		go sim.worker(i)
	}
}

// StopWorkers 停止所有 worker
func (sim *Simulator) StopWorkers() {
	sim.simulateTaskPQ.Stop()
	sim.workerWg.Wait()
}

// ===== Simulation Entry Points =====

// SimulateCXTransaction 接收模拟请求并放入优先级队列
func (sim *Simulator) SimulateCXTransaction(req *api.CXTSimulationRequest) {
	req.Epochs = sim.committee.CopyCurrentEpochs()
	leader := sim.committee.GetLeader(req.Epochs[sim.committee.SelfShard], sim.committee.SelfShard)

	// increment simulating count when request arrives
	atomic.AddInt64(&sim.simulatingCount, 1)
	currentCount := atomic.LoadInt64(&sim.simulatingCount)

	utils.SSCLogger().Debug().Str("txHash", req.Tx.Hash().String()).Str("leader", leader.Endpoint).
		Int64("simulatingCount", currentCount).
		Msg("simulate cx transaction, send to pq")

	// 将任务发送到优先级队列（按 SimulationNum 降序执行）
	sim.stats.QueuePushCount.Add(1)
	sim.simulateTaskPQ.Push(&simulateTask{req: req, pushTime: time.Now()})
	sim.stats.setCxtStage(req.Tx.Hash(), 0)

	utils.SSCLogger().Debug().Str("txHash", req.Tx.Hash().String()).Str("leader", leader.Endpoint).
		Int64("simulatingCount", currentCount).
		Msg("simulate cx transaction, in chan")
}

// worker 处理模拟任务
func (sim *Simulator) worker(id int) {
	defer sim.workerWg.Done()
	utils.SSCLogger().Debug().Int("workerId", id).Msg("worker started")

	for {
		task, ok := sim.simulateTaskPQ.PopOrWait()
		if !ok {
			utils.SSCLogger().Debug().Int("workerId", id).Msg("worker stopped")
			return
		}

		// 队列等待统计
		waitTime := time.Since(task.pushTime)
		sim.stats.QueueWaitTotalNs.Add(waitTime.Nanoseconds())
		sim.stats.QueueWaitCount.Add(1)
		sim.stats.QueuePopCount.Add(1)
		sim.processSimulationTask(task, id)
	}
}

// processSimulationTask 处理单个模拟任务
func (sim *Simulator) processSimulationTask(task *simulateTask, workerId int) {
	startTime := time.Now()
	req := task.req
	leader := sim.committee.GetLeader(req.Epochs[sim.committee.SelfShard], sim.committee.SelfShard)

	var queueWaitDur, p2pCallDur time.Duration
	defer func() {
		// decrement simulating count and update statistics
		atomic.AddInt64(&sim.simulatingCount, -1)
		duration := time.Since(startTime)
		sim.stats.SimTotalNs.Add(duration.Nanoseconds())
		sim.stats.SimCount.Add(1)
		sim.simuStatsLock.Lock()
		sim.totalSimulations++
		sim.totalSimuDuration += duration
		avgDuration := sim.totalSimuDuration / time.Duration(sim.totalSimulations)
		currentCount := atomic.LoadInt64(&sim.simulatingCount)
		sim.simuStatsLock.Unlock()

		utils.SSCLogger().Debug().
			Str("txHash", req.Tx.Hash().String()).
			Interface("epochs", req.Epochs).
			Int("workerId", workerId).
			Dur("cost", duration).
			Dur("avgCost", avgDuration).
			Int64("currentCount", currentCount).
			Int64("totalCount", sim.totalSimulations).
			Msg("simulate cx transaction, end")

		// timing breakdown
		utils.SSCLogger().Debug().
			Str("txHash", req.Tx.Hash().String()).
			Str("queueWait", queueWaitDur.String()).
			Str("p2pCall", p2pCallDur.String()).
			Str("total", time.Since(startTime).String()).
			Msg("processSimulationTask timing breakdown")
	}()

	// queueWait: pushTime → now
	queueWaitDur = time.Since(task.pushTime)

	ctx, cancel := context.WithTimeout(sim.ctx, sim.config.CallTimeout)
	defer cancel()

	ret := new(api.CXTSimulationSSCResult)
	p2pStart := time.Now()
	err := sim.communicator.comm.Call(ctx, ret, leader, api.Method_StartSimulateCXTransaction, req)
	p2pCallDur = time.Since(p2pStart)
	sim.stats.P2pCallTotalNs.Add(p2pCallDur.Nanoseconds())
	sim.stats.P2pCallCount.Add(1)

	if err != nil {
		utils.SSCLogger().Error().Err(err).Str("txHash", req.Tx.Hash().String()).
			Msg("failed to simulate cx transaction")
		return
	}

	// 处理结果 — 结果已通过 RPC 返回给 leader，无需额外处理
	_ = ret
}

// ========================================================================
// 状态初始化方法：startCXT ⸺ 从 impl.go 搬迁而来
// ========================================================================

// startCXT 启动跨分片交易上下文，创建 TxState + SimulationState。
// 搬迁自 sscService.startCXT (impl.go:507-598)
func (sim *Simulator) startCXT(txHash common.Hash, simulationNum int, originShardId uint32, relatedShards api.RelatedShards,
	topRequest *api.CXTSimulationRequest, callRequest *api.CXTCallSSCRequest) (bool, error) {

	tx, _ := sim.state.GetTxState(txHash)

	// don't start if the simulationNum is not larger than current one
	if tx != nil && tx.SimulationNum >= simulationNum {
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Msgf("the context has been started, current simulationNum %d, new simulationNum %d", tx.SimulationNum, simulationNum)
		return false, nil
	}

	// set cxt timeout ctx
	ctx, cancel := context.WithTimeout(sim.ctx, sim.config.CallTimeout)

	// 创建 SimulationState（模拟专属）
	newSim := &api.SimulationState{
		CallStack:            api.NewCallStack(txHash, simulationNum),
		SimulationRequest:    topRequest,
		SimulationResult:     nil,
		SimulationCallStates: make(map[int]api.SimulationCallStates),
		LockedCallIndex:      api.MINCallIndex,
		CallForest:           api.NewCallForest(),
	}

	// 创建 TxState（公共字段）
	newTx := &api.TxState{
		TxHash:        txHash,
		Status:        api.SIMULATING,
		SimulationNum: simulationNum,
		OriginShardId: originShardId,
		RelatedShards: relatedShards.Add(sim.committee.SelfShard),
		RetrySignals:  make(map[int]map[uint32]*api.RetrySignal),
		Ctx:           ctx,
		CtxCancel:     cancel,
	}

	// start a new resimulation state
	if simulationNum > 0 {
		simState, _ := sim.GetSimState(txHash)
		// extend from originRequest
		if topRequest != nil {
			utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
				Msgf("extend from origin request, simulationNum %d", topRequest.SimulationNum)
			originReq := func() *api.CXTSimulationRequest {
				if simState != nil {
					return simState.SimulationRequest
				}
				return topRequest
			}()
			topRequest.Tx = originReq.Tx
			topRequest.From = originReq.From
			topRequest.Author = originReq.Author
			topRequest.GasPool = originReq.GasPool
		}
		newSim.SimulationRequest = topRequest
		if simState != nil {
			newSim.SimulationCallStates = simState.SimulationCallStates
		}
		newTx.RelatedShards = newTx.RelatedShards.Merge(func() api.RelatedShards {
			if tx != nil {
				return tx.RelatedShards
			}
			return api.RelatedShards{}
		}())
		newSim.CallStack = api.NewCallStack(txHash, simulationNum)
		newTx.Status = api.RESIMULATING
	}

	if topRequest != nil {
		newTx.Nonce = topRequest.Tx.Nonce()
		newTx.TxSender = topRequest.From
		newTx.Epochs = topRequest.Epochs
	}
	if callRequest != nil {
		newTx.Nonce = callRequest.Nonce
		newTx.TxSender = common.BytesToAddress(callRequest.TxSender)
		newTx.Epochs = callRequest.Epochs
	}

	// 存储 TxState 和 SimulationState
	newSim.SimulationCallStates[simulationNum] = make(api.SimulationCallStates, 0)
	sim.state.CreateTxState(txHash, newTx)
	sim.SetSimState(txHash, newSim)

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Msgf("startCXT for tx %s, simulationNum %d, FromShard %d, relatedShards %v, oldRelatedShards %v",
			txHash.Hex(), simulationNum, originShardId, newTx.RelatedShards, relatedShards)

	return true, nil
}

// ========================================================================
// startSimulation ⸺ 从 impl.go 搬迁而来
// ========================================================================

// startSimulation 启动模拟执行，创建 TxState + SimulationCallState，处理 pending 请求。
// 搬迁自 sscService.startSimulation (impl.go:392-504)
func (sim *Simulator) startSimulation(req *api.CXTSimulationRequest) (*api.TxState, *api.SimulationCallState, error) {
	txHash := req.TxHash
	_, err := sim.startCXT(txHash, req.SimulationNum, sim.committee.SelfShard, make([]uint32, 0), req, nil)
	if err != nil {
		return nil, nil, err
	}

	// 读取 TxState 和 SimulationState
	tx, err := sim.state.GetTxState(txHash)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to get state")
		return nil, nil, err
	}
	// check if callIndex is already in progress, don't start again
	simState, _ := sim.GetSimState(txHash)
	if simState != nil {
		if callState := simState.SimulationCallStates[tx.SimulationNum].Get(api.CallIndex{}); callState != nil {
			utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("simulation has been started, simulationNum=%d", tx.SimulationNum)
			return tx, callState, nil
		}
	}

	callState := &api.SimulationCallState{
		SimulationNum:     req.SimulationNum,
		BlockNum:          req.BlockNum,
		BlockHash:         req.BlockHash,
		CallIndex:         api.CallIndex{},
		TopRequest:        req,
		DependentCXTCalls: make(map[string]*api.DependentCXTCall),
		RWSet:             newRWSet(),
		Result:            nil,
		CallSSCResult:     nil,
		Executed:          false,
		Lock:              sync.Mutex{},
	}

	header := sim.bc.GetHeaderByHash(callState.BlockHash)
	if header == nil {
		sim.waitForSync(txHash, callState)
		header = sim.bc.GetHeaderByHash(callState.BlockHash)
		if header == nil {
			utils.SSCLogger().Debug().
				Str("txHash", txHash.Hex()).
				Msg("start simulation 1.4")
			utils.SSCLogger().Error().Msg("failed to get block header")
			return nil, nil, errors.New("failed to get block header")
		}
	}

	db, err := sim.bc.StateAt(header.Root())
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to get state")
		return nil, nil, err
	}
	callState.DB = db

	// 初始化 commitStates
	sim.state.InitCommitState(txHash)

	// 更新 SimulationState
	simState, ok := sim.GetSimState(txHash)
	if !ok || simState == nil {
		utils.SSCLogger().Error().Msg("failed to start simulation: simulation state not found")
		return nil, nil, errors.New("simulation state not found")
	}
	simState.CurrentCallFrame = &api.CallFrame{
		CallIndex: api.CallIndex{},
		PC:        0,
	}
	simState.SimulationCallStates[req.SimulationNum] = simState.SimulationCallStates[req.SimulationNum].Add(callState)

	// 处理 pending requests
	pendingList := sim.PopPendingRequests(txHash)
	if len(pendingList) > 0 {
		for _, p := range pendingList {
			utils.SSCLogger().Debug().
				Str("txHash", txHash.Hex()).
				Str("callIndex", p.req.CallIndex.ToString()).
				Msg("found pending CXT request")
			if p.req.CallIndex[:len(p.req.CallIndex)-1].ToString() == callState.CallIndex.ToString() {
				utils.SSCLogger().Debug().
					Str("txHash", txHash.Hex()).
					Str("callIndex", p.req.CallIndex.ToString()).
					Msg("processing pending CXT request")
				go func(req *api.CXTCallRequest, ch chan *api.CXTCallSSCResult) {
					result := sim.RequestCallCXT(req)
					utils.SSCLogger().Debug().
						Str("txHash", txHash.Hex()).
						Str("callIndex", req.CallIndex.ToString()).
						Msg("finished processing pending CXT request")
					ch <- result
				}(p.req, p.waitingCh)
			} else {
				sim.AddPendingRequest(txHash, p)
			}
		}
	}
	return tx, callState, nil
}

// ========================================================================
// waitForSync ⸺ 从 impl.go 搬迁而来
// ========================================================================

// waitForSync 等待区块同步，将 callState 放入等待队列并阻塞直到同步完成。
// 搬迁自 sscService.waitForSync (impl.go:980-1004)
func (sim *Simulator) waitForSync(txHash common.Hash, callState *api.SimulationCallState) {
	callState.SyncedCh = make(chan struct{})

	// 使用自管理的 callStatesInWaiting 存储
	sim.AddCallStatesInWaiting(callState.BlockHash, callState)

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Str("callIndex", callState.CallIndex.ToString()).
		Str("blockHash", callState.BlockHash.Hex()).
		Uint64("blockNum", callState.BlockNum).
		Msgf("callState is waiting for sync")

	select {
	case <-callState.SyncedCh:
		utils.SSCLogger().Debug().Msgf("callState synced %s", callState.CallIndex.ToString())
	case <-sim.ctx.Done():
		utils.SSCLogger().Debug().Msg("simulator context done")
	}
}

// ========================================================================
// SimulatorStateDB — API.Service 接口的模拟执行部分
// 这些方法通过 sscService 内嵌提升到 sscService
// ========================================================================

// GetCallState 获取当前 call frame 对应的 call state。
func (sim *Simulator) GetCallState(txHash common.Hash) *api.SimulationCallState {
	cxtState, err := sim.state.GetTxState(txHash)
	if err != nil || cxtState == nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msgf("get nil tx state")
		return nil
	}
	simState, ok := sim.GetSimState(txHash)
	if !ok || simState == nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msgf("get nil simulation state")
		return nil
	}
	if simState.SimulationCallStates == nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msgf("get nil simulation callstates")
		return nil
	}
	simulationCallState := simState.SimulationCallStates[cxtState.SimulationNum]
	if simulationCallState == nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msgf("get nil simulationCallState")
		return nil
	}
	callState := simState.SimulationCallStates[cxtState.SimulationNum].Get(simState.CurrentCallFrame.CallIndex)
	if callState == nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
			Interface("callStates", simState.SimulationCallStates[cxtState.SimulationNum]).
			Interface("currentFrame", simState.CurrentCallFrame).
			Msgf("get nil callState")
		return nil
	}
	return callState
}

// GetRWSet 获取当前 call frame 对应的 RWSet。
func (sim *Simulator) GetRWSet(txHash common.Hash) *api.RWSet {
	cxtState, err := sim.state.GetTxState(txHash)
	if err != nil || cxtState == nil {
		return nil
	}
	simState, ok := sim.GetSimState(txHash)
	if !ok || simState == nil {
		return nil
	}
	callState := simState.SimulationCallStates[cxtState.SimulationNum].Get(simState.CurrentCallFrame.CallIndex)
	if callState == nil {
		return nil
	}
	return callState.RWSet
}

// EndCTX 清理模拟状态。
func (sim *Simulator) EndCTX(txHash common.Hash) {
	sim.DeleteSimState(txHash)
}

// CreateAccount 在 RWSet 中创建账户。
func (sim *Simulator) CreateAccount(txHash common.Hash, address common.Address) {
	rwset := sim.GetRWSet(txHash)
	if rwset == nil {
		return
	}
	rwset.CurrentState.Balance[address] = new(big.Int)
}

// SubBalance 从 RWSet 中扣除余额。
func (sim *Simulator) SubBalance(db api.StateDB, txHash common.Hash, address common.Address, balance *big.Int) {
	rwset := sim.GetRWSet(txHash)
	if rwset == nil {
		return
	}
	bal := rwset.CurrentState.Balance[address]
	if bal == nil {
		bal = db.GetBalance(address)
		rwset.ReadState.Balance[address] = bal
	}
	newBal := bal.Sub(bal, balance)
	rwset.CurrentState.Balance[address] = newBal
	rwset.WriteState.Balance[address] = newBal
}

// AddBalance 向 RWSet 中添加余额。
func (sim *Simulator) AddBalance(db api.StateDB, txHash common.Hash, address common.Address, balance *big.Int) {
	rwset := sim.GetRWSet(txHash)
	if rwset == nil {
		return
	}
	bal := rwset.CurrentState.Balance[address]
	if bal == nil {
		bal = db.GetBalance(address)
		rwset.ReadState.Balance[address] = bal
	}
	newBal := bal.Add(bal, balance)
	rwset.CurrentState.Balance[address] = newBal
	rwset.WriteState.Balance[address] = newBal
}

// GetBalance 从 RWSet 中获取余额。
func (sim *Simulator) GetBalance(db api.StateDB, txHash common.Hash, address common.Address) *big.Int {
	rwset := sim.GetRWSet(txHash)
	if rwset == nil {
		return common.Big0
	}
	bal := rwset.CurrentState.Balance[address]
	if bal == nil {
		bal = db.GetBalance(address)
		rwset.ReadState.Balance[address] = bal
		rwset.CurrentState.Balance[address] = bal
	}
	return bal
}

// currentSimNum 返回当前交易正在模拟的轮次 (simulationNum)。
// 成员执行 HandleSimulateRequest 时会为该轮创建/更新 TxState.SimulationNum，
// simDAGPatches 按 (tx,simNum) 定位本轮子图，故用此值反查。
func (sim *Simulator) currentSimNum(txHash common.Hash) int {
	if tx, err := sim.state.GetTxState(txHash); err == nil && tx != nil {
		return tx.SimulationNum
	}
	return 0
}

// readUpstreamValue 统一查询一笔 DAG 救援 tx 的上游是否已写出指定 key 的值（DSN-56：
// 上游写集只从 DAG 反查，不再有扁平 ChainPatch 直查路径）。
// 反查顺序：① 本 tx 在 offChainDAG 的节点沿 UpstreamTxList 递归（leader）；② 本轮
// simDAGPatches 子图（成员按 (tx,simNum) 反查）。两者同构，任一命中即视为被上游覆盖。
func (sim *Simulator) readUpstreamValue(txHash common.Hash, address common.Address, key common.Hash) (common.Hash, bool) {
	rs := sim.sscService.retryScheduler
	if rs == nil {
		return common.Hash{}, false
	}
	// ① leader：沿本 tx 的 DAG 节点 + UpstreamTxList 递归反查上游写集。
	if nodeRef := rs.GetDAGNodeRef(txHash); nodeRef != nil {
		if val, found := rs.readPatchChain(nodeRef.TxHash, nodeRef.SimulationNum, address, key); found {
			return val, true
		}
	}
	// ② 成员/leader 兜底：查本轮 simDAGPatches —— 撞锁 key 若被本轮模拟子图里的上游写过，则用其值（不撞锁）。
	// 成员通常没有 offChainDAG 节点（GetDAGNodeRef=nil），故这里作为覆盖上游写集的关键路径；
	// leader 即使 DAG 链未覆盖，也回退到同构的 simDAGPatches，保证与成员结果一致。
	if val, found := rs.ReadSimDAGPatch(txHash, sim.currentSimNum(txHash), address, key); found {
		return val, true
	}
	return common.Hash{}, false
}

// currentTxPriority 返回当前交易在链下模拟中的确定性优先级（与链上一致：Nonce>Origin>TxHash）。
// 优先用链上注册的 txPriority（最一致）；未注册时回退到 TxState。
func (sim *Simulator) currentTxPriority(txHash common.Hash) (api.Priority, bool) {
	if sim.sscService != nil && sim.sscService.retryScheduler != nil {
		if mgr := sim.sscService.retryScheduler.tempLockView.stateLockManager; mgr != nil {
			if p, ok := mgr.GetTxPriority(txHash); ok {
				return p, true
			}
		}
	}
	tx, err := sim.state.GetTxState(txHash)
	if err == nil && tx != nil {
		return api.Priority{Nonce: tx.Nonce, OriginShardID: tx.OriginShardId, TxHash: txHash}, true
	}
	return api.Priority{}, false
}

// isLowerPriorityLock 判断当前交易撞到的锁（TLV 或 SLM/on-chain+pending）是否由更低优先级的持有者持有。
// 链下 wound：若是，则当前交易优先级更高，可「忽略」该锁继续模拟、构建 SimTx（由链下 TLV wound 保证
// 最终能拿到锁）；链上冲突则交给 Verify 的 Wait-Die（低优先级终局回滚），不在链下判死。
func (sim *Simulator) isLowerPriorityLock(txHash common.Hash, lockKey api.LockKey, db api.StateDB) bool {
	cur, ok := sim.currentTxPriority(txHash)
	if !ok {
		return false
	}
	if sim.sscService == nil || sim.sscService.retryScheduler == nil {
		return false
	}
	tlv := sim.sscService.retryScheduler.tempLockView
	return lowerPriorityLock(tlv, tlv.stateLockManager, db, txHash, lockKey, cur)
}

// lowerPriorityLock 判断某 key 是否被比 cur（当前交易）更低优先级的持有者持有。
//
// 供链下模拟（Simulator.isLowerPriorityLock）使用：撞到低优先级持有者的锁时，高优先级交易可以
// 「忽略」该锁继续模拟（TLV wound 语义）——由链下 retryScheduler 的 TryLockWithPriority 真正抢占 TLV 锁。
//
// 覆盖：TLV 持有者 / global 写锁（含 Finalized 判定）/ global 读锁 / pending 写锁 / pending 读锁。
func lowerPriorityLock(tlv *TempLockView, mgr *stateLockManager, db api.StateDB, txHash common.Hash, lockKey api.LockKey, cur api.Priority) bool {
	// 1) TLV 持有者（tempLockEntry 已带 Priority）
	if tlv != nil {
		if h, pri, found := tlv.GetTempLockHolder(lockKey); found && h != txHash {
			if cur.Less(pri) {
				return true
			}
		}
	}
	// 2) SLM/on-chain 持有者：globalLockedStates + globalRLockedStates + pendingStates（写/读锁）
	if mgr != nil {
		if meta, ok2 := mgr.GetLockHolderMeta(lockKey); ok2 && meta.TxHash != txHash {
			if !meta.Finalized && cur.Less(meta.Priority) {
				return true
			}
		} else {
			// global 读锁持有者（读锁可多持有者，返回其一）
			if h, ok4 := mgr.GetRLockHolder(lockKey); ok4 && h != txHash {
				if hp, ok5 := mgr.GetTxPriority(h); ok5 && cur.Less(hp) {
					return true
				}
			}
			// pending 写锁 / 读锁
			if sdb, isDB := db.(*corestate.DB); isDB {
				if h, found := sdb.FindPendingLockHolder(lockKey); found && h != txHash {
					if hp, ok3 := mgr.GetTxPriority(h); ok3 && cur.Less(hp) {
						return true
					}
				}
			}
		}
	}
	return false
}

// IsKeyAvailable 三层仲裁：TLV 检查 → SLM 检查 → DAG 上游补救（readUpstreamValue）。
// 用于 GetState/SetState 中 stateDB 读取前的锁冲突预检。
//
// DSN-52 §17.7 最终分层：若冲突锁由**更低优先级**持有者持有，则当前（高优先级）交易可
// 「忽略」该锁（视为可用）继续模拟——配合链下 TLV wound（RetryCommit Phase1
// TryLockWithPriority 真正抢占 TLV 锁）；on-chain 冲突由严格门拦截，链上 Verify 只做
// Wait-Die（低优先级终局回滚，高优先级 wait，**不 wound**）。不在模拟层判死。
//
// 返回：
//
//	available: key 是否可访问
//	patchedVal: 非 nil 表示上游 DAG 已写出此 key（readUpstreamValue 命中），调用方应使用此值
//	err: 非 nil 表示锁冲突且无 DAG 上游覆盖、且持有者不是更低优先级
func (sim *Simulator) IsKeyAvailable(txHash common.Hash, db api.StateDB, address common.Address, key common.Hash) (available bool, patchedVal *common.Hash, err error) {
	lockKey := api.FormKey(address, key)
	tlv := sim.sscService.retryScheduler.tempLockView

	// Step 1: TLV 检查 — 是否有其他交易预约了这个 key
	tlvLocked := tlv.HasConflict(txHash, lockKey)

	// Step 2: SLM 检查 — stateLockManager 是否锁了这个 key
	slmLocked := false
	if err := db.CheckLock(lockKey, txHash); err != nil {
		slmLocked = true
	}

	// 两者都未锁 → 可用
	if !tlvLocked && !slmLocked {
		return true, nil, nil
	}

	// 链下 wound：撞到更低优先级持有者的锁 → 当前（高优先级）可忽略（视为可用）继续模拟；
	// on-chain 冲突由严格门（RetryCommit Phase2）拦截，链上 Verify 只做 Wait-Die，不在此上链判 die。
	if sim.isLowerPriorityLock(txHash, lockKey, db) {
		return true, nil, nil
	}

	// Step 3: DAG 上游补救 — 看该交易的上游是否已写出此 key（DSN-56：只从 DAG 反查，
	// 即 readUpstreamValue → offChainDAG 链 / DSN-54 simDAGPatches）。
	val, found := sim.readUpstreamValue(txHash, address, key)
	if found {
		return true, &val, nil // DAG 上游已覆盖 ✅
	}

	// 真未覆盖（genuinely-uncovered）：DAG/simDAGPatches 都反查不到 → 保留锁冲突。
	// DSN-54 打点：此类是本方案预期残留的"真冲突"，区别于 covered-but-invisible（修复目标，应消失）。
	if rs := sim.sscService.retryScheduler; rs != nil {
		chainRetryStats.SigSimDAGPatchMiss.Add(1)
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Str("address", address.Hex()).
			Str("key", key.Hex()).
			Msg("IsKeyAvailable: genuinely-uncovered lock conflict (simDAGPatch miss)")
	}
	return false, nil, api.ErrLockConflict_OnChain
}

// GetState 读取模拟执行中的状态值。
// Step 0: 查 DAG（RetryScheduler offChainDAG 链式依赖路径，leader）
// Step 1: 三层仲裁 IsKeyAvailable（内部含 DSN-54 simDAGPatches 反查）→ 查 callState RWSet
func (sim *Simulator) GetState(db api.StateDB, txHash common.Hash, address common.Address, key common.Hash) (common.Hash, error) {
	// Step 0: 递归查 DAG（RetryScheduler offChainDAG 链式依赖路径，leader 侧）
	rs := sim.sscService.retryScheduler
	chainPatchRef := rs.GetDAGNodeRef(txHash)
	if chainPatchRef != nil {
		val, found := rs.readPatchChain(
			chainPatchRef.TxHash,
			chainPatchRef.SimulationNum,
			address, key)
		if found {
			// 上游写集命中：缓存到 callState 的 RWSet
			if callState := sim.GetCallState(txHash); callState != nil {
				callState.StateLock.Lock()
				if callState.RWSet.ReadState.State[address] == nil {
					callState.RWSet.ReadState.State[address] = make(map[common.Hash]common.Hash)
				}
				if callState.RWSet.CurrentState.State[address] == nil {
					callState.RWSet.CurrentState.State[address] = make(map[common.Hash]common.Hash)
				}
				callState.RWSet.ReadState.State[address][key] = val
				callState.RWSet.CurrentState.State[address][key] = val
				callState.StateLock.Unlock()
			}
			utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
				Str("address", address.Hex()).
				Str("key", key.Hex()).
				Str("value", val.Hex()).
				Msg("GetState: DAG upstream hit, returning patched value")
			return val, nil
		}
	}

	// Step 1: 查 callState RWSet
	callState := sim.GetCallState(txHash)
	if callState == nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
			Msg("failed to get state: call state not found")
		return common.Hash{}, api.ErrInvalidExecution
	}

	callState.StateLock.Lock()
	defer func() {
		callState.StateLock.Unlock()
	}()

	rwset := callState.RWSet
	if rwset.ReadState.State[address] == nil {
		rwset.ReadState.State[address] = make(map[common.Hash]common.Hash)
	}
	if rwset.CurrentState.State[address] == nil {
		rwset.CurrentState.State[address] = make(map[common.Hash]common.Hash)
	}
	if value, exists := rwset.CurrentState.State[address][key]; !exists {
		// Step 1a: 三层仲裁 — TLV → SLM → DAG 上游（IsKeyAvailable 内含 readUpstreamValue）
		_, patchVal, err := sim.IsKeyAvailable(txHash, db, address, key)
		if err != nil {
			// 锁冲突且无 Patch 覆盖
			callState.LockedByOtherTx = err
			callState.LockedKeys = append(callState.LockedKeys, api.FormKey(address, key))
			return common.Hash{}, nil // ForceSimulation兼容：返回零值继续执行
		}

		var val common.Hash
		if patchVal != nil {
			// Step 2b: Patch 提供了值，不走 stateDB
			val = *patchVal
		} else if sim.isLowerPriorityLock(txHash, api.FormKey(address, key), db) {
			// 链下 wound：该锁由更低优先级持有者持有 → 当前（高优先级）可忽略锁直接无锁读取
			// （配合链下 TLV wound；on-chain 冲突由严格门拦截，不在模拟层判 die）。
			val, err = db.GetStateWithoutLock(address, key)
			if err != nil {
				return common.Hash{}, err
			}
		} else {
			// Step 2c: 无锁冲突，走 stateDB 正常读
			val, err = db.GetState(txHash, address, key)
			if err != nil {
				// 非锁冲突的错误（如 state trie 不存在）
				return common.Hash{}, err
			}
		}
		rwset.CurrentState.State[address][key] = val
		rwset.ReadState.State[address][key] = val
		return val, nil
	} else {
		return value, nil
	}
}

// SetState 写入模拟执行中的状态值。
func (sim *Simulator) SetState(db api.StateDB, txHash common.Hash, address common.Address, key common.Hash, value common.Hash) error {
	callState := sim.GetCallState(txHash)
	if callState == nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
			Msg("failed to set state: call state not found")
		return api.ErrInvalidExecution
	}
	callState.StateLock.Lock()
	defer func() {
		callState.StateLock.Unlock()
	}()

	rwset := callState.RWSet
	if rwset.CurrentState.State[address] == nil {
		rwset.CurrentState.State[address] = make(map[common.Hash]common.Hash)
	}
	if rwset.WriteState.State[address] == nil {
		rwset.WriteState.State[address] = make(map[common.Hash]common.Hash)
	}
	if rwset.ReadState.State[address] == nil {
		rwset.ReadState.State[address] = make(map[common.Hash]common.Hash)
	}
	if _, exists := rwset.ReadState.State[address][key]; !exists {
		// Step 0: 先查 DAG 上游（offChainDAG 链式依赖路径，leader），避免触发 stateDB 锁检查
		rs := sim.sscService.retryScheduler
		chainPatchRef := rs.GetDAGNodeRef(txHash)
		var val common.Hash
		var patchFound bool
		if chainPatchRef != nil {
			val, patchFound = rs.readPatchChain(
				chainPatchRef.TxHash,
				chainPatchRef.SimulationNum,
				address, key)
		}
		// Step 1: 三层仲裁 — TLV → SLM → DAG 上游（IsKeyAvailable 内含 readUpstreamValue/simDAGPatches）
		if !patchFound {
			_, patchVal, err := sim.IsKeyAvailable(txHash, db, address, key)
			if err != nil {
				// 锁冲突且无 Patch 覆盖
				callState.LockedByOtherTx = err
				callState.LockedKeys = append(callState.LockedKeys, api.FormKey(address, key))
				// ForceSimulation 兼容：用当前写入值继续执行
				rwset.CurrentState.State[address][key] = value
				rwset.WriteState.State[address][key] = value
				return nil
			}

			if patchVal != nil {
				// Patch 提供了值，不走 stateDB
				val = *patchVal
			} else if sim.isLowerPriorityLock(txHash, api.FormKey(address, key), db) {
				// 链下 wound：该锁由更低优先级持有者持有 → 当前（高优先级）可忽略锁直接无锁读取
				// （配合链下 TLV wound；on-chain 冲突由严格门拦截，不在模拟层判 die）。
				val, err = db.GetStateWithoutLock(address, key)
				if err != nil {
					utils.SSCLogger().Error().Err(err).Str("txHash", txHash.Hex()).
						Msgf("failed to set state: get conflict state (ignored lower-prio holder) [%s:%s]", address.Hex(), key.Hex())
					return err
				}
			} else {
				// 无锁冲突，走 stateDB 正常读
				val, err = db.GetState(txHash, address, key)
				if err != nil {
					utils.SSCLogger().Error().Err(err).Str("txHash", txHash.Hex()).
						Msgf("failed to set state: get conflict state [%s:%s]", address.Hex(), key.Hex())
					return err
				}
			}
		}
		rwset.ReadState.State[address][key] = val
	}
	rwset.CurrentState.State[address][key] = value
	rwset.WriteState.State[address][key] = value
	return nil
}
