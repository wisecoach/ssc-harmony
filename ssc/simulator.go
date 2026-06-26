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
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/harmony-one/harmony/ssc/lm"
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
	// GetCommitStates 获取 commitStates 指针（用于写入 CommitState）
	GetCommitStates func() map[common.Hash]*api.CommitState
	// GetCommitLock 获取 commitLock（用于写入 CommitState）
	GetCommitLock func() *lm.RWMutex
	// GetFinishedTxs 获取 finishedTxs 指针
	GetFinishedTxs func() map[common.Hash]bool

	// CallForRetry 调度重试（原 s.retryScheduler.CallForRetry）
	CallForRetry func(tx *api.RetryTx)
}

// Simulator 负责跨分片交易的模拟执行。
// 自管理 SimulationState、callStatesInWaiting、worker pool 等存储。
type Simulator struct {
	state SimulatorStateAccessor

	// 自管理存储：SimulationState
	simStates map[common.Hash]*api.SimulationState
	simLock   lm.RWMutex

	// 自管理存储：call 同步
	syncLock            lm.Mutex
	callStatesInWaiting map[common.Hash][]*api.SimulationCallState

	// 自管理存储：simulation 结果通知
	simuLock       lm.Mutex
	simuWaitingChs map[common.Hash][]chan *api.CXTSimulationSSCResult
	simuResultCh   map[common.Hash]chan *api.CXTSimulationSSCResult

	// 自管理存储：pending requests
	pendingLock     sync.Mutex
	pendingRequests map[common.Hash][]*pendingCXTRequest

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
		simStates:           make(map[common.Hash]*api.SimulationState),
		callStatesInWaiting: make(map[common.Hash][]*api.SimulationCallState),
		simuWaitingChs:      make(map[common.Hash][]chan *api.CXTSimulationSSCResult),
		simuResultCh:        make(map[common.Hash]chan *api.CXTSimulationSSCResult),
		pendingRequests:     make(map[common.Hash][]*pendingCXTRequest),
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

// GetSimState 读取 SimulationState（带锁）
func (sim *Simulator) GetSimState(txHash common.Hash) (*api.SimulationState, bool) {
	sim.simLock.RLock()
	defer sim.simLock.RUnlock()
	state, ok := sim.simStates[txHash]
	return state, ok
}

// SetSimState 写入 SimulationState（带锁）
func (sim *Simulator) SetSimState(txHash common.Hash, state *api.SimulationState) {
	sim.simLock.Lock()
	defer sim.simLock.Unlock()
	sim.simStates[txHash] = state
}

// DeleteSimState 删除 SimulationState（带锁）
func (sim *Simulator) DeleteSimState(txHash common.Hash) {
	sim.simLock.Lock()
	defer sim.simLock.Unlock()
	delete(sim.simStates, txHash)
}

func (sim *Simulator) GetBlockHash(txHash common.Hash) (common.Hash, bool) {
	sim.simLock.RLock()
	defer sim.simLock.RUnlock()
	state, ok := sim.simStates[txHash]
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
	sim.DeleteSimState(txHash)

	// 清理 pending requests
	sim.pendingLock.Lock()
	delete(sim.pendingRequests, txHash)
	sim.pendingLock.Unlock()

	// 清理 simulation 结果 channel
	sim.simuLock.Lock()
	delete(sim.simuResultCh, txHash)
	delete(sim.simuWaitingChs, txHash)
	sim.simuLock.Unlock()

	// 清理 call states in waiting
	sim.syncLock.Lock()
	delete(sim.callStatesInWaiting, txHash)
	sim.syncLock.Unlock()
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

// ===== simuResultCh 存储访问 =====

// GetSimuResultCh 获取 simulation 结果 channel
func (sim *Simulator) GetSimuResultCh(txHash common.Hash) chan *api.CXTSimulationSSCResult {
	sim.simuLock.Lock()
	defer sim.simuLock.Unlock()
	return sim.simuResultCh[txHash]
}

// SetSimuResultCh 设置 simulation 结果 channel
func (sim *Simulator) SetSimuResultCh(txHash common.Hash, ch chan *api.CXTSimulationSSCResult) {
	sim.simuLock.Lock()
	defer sim.simuLock.Unlock()
	sim.simuResultCh[txHash] = ch
}

// DeleteSimuResultCh 删除 simulation 结果 channel
func (sim *Simulator) DeleteSimuResultCh(txHash common.Hash) {
	sim.simuLock.Lock()
	defer sim.simuLock.Unlock()
	delete(sim.simuResultCh, txHash)
}

// AddSimuWaitingCh 添加等待通知的 channel
func (sim *Simulator) AddSimuWaitingCh(txHash common.Hash, ch chan *api.CXTSimulationSSCResult) {
	sim.simuLock.Lock()
	defer sim.simuLock.Unlock()
	sim.simuWaitingChs[txHash] = append(sim.simuWaitingChs[txHash], ch)
}

// PopSimuWaitingChs 取出并删除所有等待的 channel
func (sim *Simulator) PopSimuWaitingChs(txHash common.Hash) []chan *api.CXTSimulationSSCResult {
	sim.simuLock.Lock()
	defer sim.simuLock.Unlock()
	chs := sim.simuWaitingChs[txHash]
	delete(sim.simuWaitingChs, txHash)
	return chs
}

// ===== pendingRequests 存储访问 =====

// PopPendingRequests 取出并删除 pending requests
func (sim *Simulator) PopPendingRequests(txHash common.Hash) []*pendingCXTRequest {
	sim.pendingLock.Lock()
	defer sim.pendingLock.Unlock()
	reqs := sim.pendingRequests[txHash]
	delete(sim.pendingRequests, txHash)
	return reqs
}

// AddPendingRequest 添加 pending request
func (sim *Simulator) AddPendingRequest(txHash common.Hash, req *pendingCXTRequest) {
	sim.pendingLock.Lock()
	defer sim.pendingLock.Unlock()
	sim.pendingRequests[txHash] = append(sim.pendingRequests[txHash], req)
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

	utils.SSCLogger().Info().Str("txHash", req.Tx.Hash().String()).Str("leader", leader.Endpoint).
		Int64("simulatingCount", currentCount).
		Msg("simulate cx transaction, send to pq")

	// 将任务发送到优先级队列（按 SimulationNum 降序执行）
	sim.stats.QueuePushCount.Add(1)
	sim.simulateTaskPQ.Push(&simulateTask{req: req, pushTime: time.Now()})
	sim.stats.setCxtStage(req.Tx.Hash(), 0)

	utils.SSCLogger().Info().Str("txHash", req.Tx.Hash().String()).Str("leader", leader.Endpoint).
		Int64("simulatingCount", currentCount).
		Msg("simulate cx transaction, in chan")
}

// worker 处理模拟任务
func (sim *Simulator) worker(id int) {
	defer sim.workerWg.Done()
	utils.SSCLogger().Info().Int("workerId", id).Msg("worker started")

	for {
		task, ok := sim.simulateTaskPQ.PopOrWait()
		if !ok {
			utils.SSCLogger().Info().Int("workerId", id).Msg("worker stopped")
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

		utils.SSCLogger().Info().
			Str("txHash", req.Tx.Hash().String()).
			Interface("epochs", req.Epochs).
			Int("workerId", workerId).
			Dur("cost", duration).
			Dur("avgCost", avgDuration).
			Int64("currentCount", currentCount).
			Int64("totalCount", sim.totalSimulations).
			Msg("simulate cx transaction, end")
	}()

	ctx, cancel := context.WithTimeout(sim.ctx, sim.config.CallTimeout)
	defer cancel()

	ret := new(api.CXTSimulationSSCResult)
	p2pStart := time.Now()
	err := sim.communicator.comm.Call(ctx, ret, leader, api.Method_StartSimulateCXTransaction, req)
	sim.stats.P2pCallTotalNs.Add(time.Since(p2pStart).Nanoseconds())
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
		newTx.TxSender, _ = topRequest.Tx.SenderAddress()
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
			utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msgf("simulation has been started, simulationNum=%d", tx.SimulationNum)
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
			utils.SSCLogger().Info().
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
	commitStates := sim.state.GetCommitStates()
	commitLock := sim.state.GetCommitLock()
	func() {
		commitLock.Lock()
		defer commitLock.Unlock()
		commitState := commitStates[txHash]
		if commitState == nil {
			commitStates[txHash] = &api.CommitState{
				CommitVotes:     make(map[int]map[uint32][]*api.CXTCommitVote),
				CommitSSCVotes:  make(map[int]map[uint32]*api.CXTCommitSSCVote),
				RollbackVotes:   make(map[uint32][]*api.CXTCommitVote),
				RollbackSSCVote: nil,
			}
		}
	}()

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
			utils.SSCLogger().Info().
				Str("txHash", txHash.Hex()).
				Str("callIndex", p.req.CallIndex.ToString()).
				Msg("found pending CXT request")
			if p.req.CallIndex[:len(p.req.CallIndex)-1].ToString() == callState.CallIndex.ToString() {
				utils.SSCLogger().Info().
					Str("txHash", txHash.Hex()).
					Str("callIndex", p.req.CallIndex.ToString()).
					Msg("processing pending CXT request")
				go func(req *api.CXTCallRequest, ch chan *api.CXTCallSSCResult) {
					result := sim.RequestCallCXT(req)
					utils.SSCLogger().Info().
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

	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
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

// GetState 读取模拟执行中的状态值。
// Step 0: 查 RetryScheduler patches（链式依赖路径）
// Step 1: 查 SimulationState ChainPatch（直接 patch 路径）
// Step 2: 查 callState RWSet
func (sim *Simulator) GetState(db api.StateDB, txHash common.Hash, address common.Address, key common.Hash) (common.Hash, error) {
	// Step 0: 递归查 RetryScheduler patches（链式依赖路径）
	rs := sim.sscService.retryScheduler
	chainPatchRef := rs.GetChainPatchRef(txHash)
	if chainPatchRef != nil {
		val, found := rs.readPatchChain(
			chainPatchRef.TxHash,
			chainPatchRef.SimulationNum,
			address, key)
		if found {
			// Patch 命中：缓存到 callState 的 RWSet
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
				Msg("GetState: ChainPatchRef hit, returning patched value")
			return val, nil
		}
	}

	// Step 1: 检查 SimulationState ChainPatch（直接 patch 路径，兼容旧逻辑）
	simState, ok := sim.GetSimState(txHash)
	if ok && simState != nil && simState.ChainPatch != nil {
		if addrState, ok := simState.ChainPatch.WriteState.State[address]; ok {
			if val, exists := addrState[key]; exists {
				utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
					Str("address", address.Hex()).
					Str("key", key.Hex()).
					Str("value", val.Hex()).
					Msg("GetState: ChainPatch hit, returning patched value")
				return val, nil
			}
		}
	}

	// Step 2: 查 callState RWSet
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
		val, err := db.GetState(txHash, address, key)
		if err != nil {
			if errors.Is(err, api.ErrLockConflict_OnChain) {
				callState.LockedByOtherTx = err
				callState.LockedKeys = append(callState.LockedKeys, api.FormKey(address, key))
			} else {
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
		// Step 0: 先查 Patch 路径，避免触发 stateDB 锁检查
		rs := sim.sscService.retryScheduler
		chainPatchRef := rs.GetChainPatchRef(txHash)
		var val common.Hash
		var patchFound bool
		if chainPatchRef != nil {
			val, patchFound = rs.readPatchChain(
				chainPatchRef.TxHash,
				chainPatchRef.SimulationNum,
				address, key)
		}
		// Step 1: 没命中 Patch 则查 SimulationState ChainPatch
		if !patchFound {
			simState, simOk := sim.GetSimState(txHash)
			if simOk && simState != nil && simState.ChainPatch != nil {
				if addrState, addrOk := simState.ChainPatch.WriteState.State[address]; addrOk {
					var exists bool
					val, exists = addrState[key]
					if exists {
						patchFound = true
						utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
							Str("address", address.Hex()).
							Str("key", key.Hex()).
							Str("value", val.Hex()).
							Msg("SetState: ChainPatch hit, reading patched value")
					}
				}
			}
		}
		// Step 2: Patch 未命中，从 stateDB 读（可能触发 Lockable）
		if !patchFound {
			var err error
			val, err = db.GetState(txHash, address, key)
			if err != nil {
				utils.SSCLogger().Error().Err(err).Str("txHash", txHash.Hex()).
					Msgf("failed to set state: get conflict state [%s:%s]", address.Hex(), key.Hex())
				if errors.Is(err, api.ErrLockConflict_OnChain) {
					callState.LockedByOtherTx = err
					callState.LockedKeys = append(callState.LockedKeys, api.FormKey(address, key))
					// ForceSimulation: return the stale value from stateDB to continue execution
					rwset.CurrentState.State[address][key] = value
					rwset.WriteState.State[address][key] = value
					return nil
				} else {
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
