package ssc

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/event"
	"github.com/harmony-one/harmony/block"
	"github.com/harmony-one/harmony/core"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/core/vm"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/harmony-one/harmony/ssc/lm"
)

type pendingCXTRequest struct {
	req       *api.CXTCallRequest
	waitingCh chan *api.CXTCallSSCResult
}

type simulateTask struct {
	req      *api.CXTSimulationRequest
	pushTime time.Time // 入队时间，用于计算队列等待
}

// pqItem 优先级队列元素，含 sequence 用于 FIFO tiebreak
type pqItem struct {
	task     *simulateTask
	sequence uint64
}

type priorityQueueHeap []*pqItem

func (pq priorityQueueHeap) Len() int { return len(pq) }

func (pq priorityQueueHeap) Less(i, j int) bool {
	// SimulationNum 大的优先（重试多的交易接近超时，优先调度）
	// 相同则 sequence 小的先出（=先到先得）
	if pq[i].task.req.SimulationNum != pq[j].task.req.SimulationNum {
		return pq[i].task.req.SimulationNum > pq[j].task.req.SimulationNum
	}
	return pq[i].sequence < pq[j].sequence
}

func (pq priorityQueueHeap) Swap(i, j int) { pq[i], pq[j] = pq[j], pq[i] }

func (pq *priorityQueueHeap) Push(x interface{}) {
	*pq = append(*pq, x.(*pqItem))
}

func (pq *priorityQueueHeap) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

// simulationReqPriorityQueue 并发安全的优先级队列，基于 container/heap + sync.Cond
type simulationReqPriorityQueue struct {
	items   priorityQueueHeap
	seq     uint64     // 自增序号，用于 FIFO tiebreak
	cond    *sync.Cond // 等待/唤醒机制
	stopped bool       // 停止标志
}

func newSimulationReqPriorityQueue() *simulationReqPriorityQueue {
	return &simulationReqPriorityQueue{
		items: make(priorityQueueHeap, 0),
		cond:  sync.NewCond(&sync.Mutex{}),
	}
}

// Push 插入任务，Broadcast 唤醒所有等待的 worker
func (pq *simulationReqPriorityQueue) Push(task *simulateTask) {
	pq.cond.L.Lock()
	pq.seq++
	heap.Push(&pq.items, &pqItem{task: task, sequence: pq.seq})
	pq.cond.Broadcast()
	pq.cond.L.Unlock()
}

// PopOrWait 从队列取出最高优先级的任务。
// 队列为空时阻塞等待，直到有任务或队列被停止。
// 第二个返回值 false 表示队列已关闭，调用方应退出。
func (pq *simulationReqPriorityQueue) PopOrWait() (*simulateTask, bool) {
	pq.cond.L.Lock()
	for pq.items.Len() == 0 && !pq.stopped {
		pq.cond.Wait()
	}
	if pq.stopped && pq.items.Len() == 0 {
		pq.cond.L.Unlock()
		return nil, false
	}
	item := heap.Pop(&pq.items).(*pqItem)
	pq.cond.L.Unlock()
	return item.task, true
}

// Stop 设置停止标志并 Broadcast 唤醒所有 worker，让它们退出
func (pq *simulationReqPriorityQueue) Stop() {
	pq.cond.L.Lock()
	pq.stopped = true
	pq.cond.Broadcast()
	pq.cond.L.Unlock()
}

type sscService struct {
	*CommitteeMechanism
	*Verifier
	*Committer
	Comm           *Comm
	BLSSignerMgr   api.BLSSignerMgr
	txSigner       api.TxSigner
	lockStateMgr   *stateLockManager
	tempLockView   *TempLockView
	timerMgr       *CXTTimerManager
	retryScheduler *retryScheduler

	Config *api.Config
	bc     core.BlockChain

	stateLock       lm.RWMutex // cmLock for simulationState, executionVerifyContexts, finishedTxs
	simulationState map[common.Hash]*api.CXTSimulationState
	finishedTxs     map[common.Hash]bool

	// 验证上下文 now managed by Verifier
	// verifyCtxLock           lm.RWMutex
	// executionVerifyContexts map[common.Hash]*api.ExecutionVerifyContext
	// txLockedSimNum          map[common.Hash]int   // moved to Verifier

	commitLock   lm.RWMutex // cmLock for commitStates
	commitStates map[common.Hash]*api.CommitState

	syncLock            lm.Mutex // cmLock for callStatesInWaiting
	callStatesInWaiting map[common.Hash][]*api.SimulationCallState

	simuLock       lm.Mutex // cmLock for simuWaitingChs, simuResultCh
	simuWaitingChs map[common.Hash][]chan *api.CXTSimulationSSCResult
	simuResultCh   map[common.Hash]chan *api.CXTSimulationSSCResult // used to notify simulation finished

	pendingLock     sync.Mutex
	pendingRequests map[common.Hash][]*pendingCXTRequest // use to store the pending requests

	ctx               context.Context
	chainHeadCh       chan core.ChainHeadEvent
	chainHeadSub      event.Subscription
	simulateSemaphore chan struct{}               // semaphore to limit concurrent simulation (increased limit for worker pool)
	simulateTaskPQ    *simulationReqPriorityQueue // priority task queue for worker pool
	workerWg          sync.WaitGroup              // wait group for workers
	numWorkers        int                         // number of worker goroutines
	stats             *SimulationStats            // experiment statistics

	// simulation statistics
	simuStatsLock     sync.Mutex
	simulatingCount   int64 // current number of simulations in progress
	totalSimulations  int64 // total number of simulations completed
	totalSimuDuration time.Duration

	// tx block trace: 记录各阶段块高度用于分析延迟
	txTraces map[common.Hash]*TxBlockTrace
}

func (s *sscService) GetShardID(address common.Address) uint32 {
	return s.CommitteeMechanism.GetShardID(address)
}

// newBaseService 创建基础服务实例（内部使用）
func newBaseService(ctx context.Context, config *api.Config, cm *CommitteeMechanism, sscConfig *api.ShardSimulateCommitteeConfig,
	signerMgr api.BLSSignerMgr, bc core.BlockChain, txSigner api.TxSigner, comm *Comm) *sscService {
	// 设置 worker 数量和信号量上限
	numWorkers := config.SimulationLimit          // worker 数量
	simulateLimit := config.SimulationLimit * 100 // 提高信号量上限，允许更多并发

	service := &sscService{
		CommitteeMechanism:  cm,
		Comm:                comm,
		BLSSignerMgr:        signerMgr,
		txSigner:            txSigner,
		Config:              config,
		bc:                  bc,
		stats:               newSimulationStats(),
		stateLock:           lm.NewRWMutex(),
		commitLock:          lm.NewRWMutex(),
		simuLock:            lm.NewMutex(),
		syncLock:            lm.NewMutex(),
		simulationState:     make(map[common.Hash]*api.CXTSimulationState),
		commitStates:        make(map[common.Hash]*api.CommitState),
		callStatesInWaiting: make(map[common.Hash][]*api.SimulationCallState),
		finishedTxs:         make(map[common.Hash]bool),
		simuWaitingChs:      make(map[common.Hash][]chan *api.CXTSimulationSSCResult),
		simuResultCh:        make(map[common.Hash]chan *api.CXTSimulationSSCResult),
		pendingRequests:     make(map[common.Hash][]*pendingCXTRequest),
		pendingLock:         sync.Mutex{},
		ctx:                 ctx,
		chainHeadCh:         make(chan core.ChainHeadEvent, 10),
		chainHeadSub:        nil,
		simulateSemaphore:   make(chan struct{}, simulateLimit),
		simulateTaskPQ:      newSimulationReqPriorityQueue(),
		workerWg:            sync.WaitGroup{},
		numWorkers:          numWorkers,
		txTraces:            make(map[common.Hash]*TxBlockTrace),
	}
	lockStateMgr := newStateLockManager(service)
	service.lockStateMgr = lockStateMgr
	service.tempLockView = lockStateMgr.GetTempLockView()
	service.timerMgr = NewTimerManager(sscConfig.Timeout, service)
	service.retryScheduler = NewRetryScheduler(ctx, service, comm, service.SelfShard, service.tempLockView)
	// 创建 Verifier（自管理 executionVerifyContexts + txLockedSimNum）
	verifyComm := NewVerifyCommunicator(comm, signerMgr, service.CommitteeMechanism, config, ctx)
	service.Verifier = NewVerifier(
		verifyComm,
		service.timerMgr,
		service.CommitteeMechanism,
		bc,
		service, // vmService — sscService 实现了 api.Service
		service.stats,
		service.retryScheduler,
		VerifierStateAccessor{
			GetState: func(txHash common.Hash) (*api.CXTSimulationState, error) {
				service.stateLock.RLock()
				defer service.stateLock.RUnlock()
				return service.getState(txHash)
			},
			SetStatus: func(txHash common.Hash, status api.CXTStatus) {
				service.stateLock.Lock()
				defer service.stateLock.Unlock()
				if state, err := service.getState(txHash); err == nil {
					state.Status = status
				}
			},
			IsTxFinished: func(txHash common.Hash) bool {
				service.stateLock.RLock()
				defer service.stateLock.RUnlock()
				_, finished := service.finishedTxs[txHash]
				return finished
			},
			SetWaitingForResimu: func(txHash common.Hash) {
				service.stateLock.Lock()
				defer service.stateLock.Unlock()
				if state, err := service.getState(txHash); err == nil {
					state.Status = api.WAITING_FOR_RESIMULATING_ON_CHAIN
				}
			},
		},
	)
	// 创建 Committer（自管理状态访问）
	service.Committer = NewCommitter(
		service.CommitteeMechanism,
		service.stats,
		CommitterStateAccessor{
			IsTxFinished: func(txHash common.Hash) bool {
				service.stateLock.RLock()
				defer service.stateLock.RUnlock()
				_, finished := service.finishedTxs[txHash]
				return finished
			},
			SetStatus: func(txHash common.Hash, status api.CXTStatus) {
				service.stateLock.Lock()
				defer service.stateLock.Unlock()
				if state, err := service.getState(txHash); err == nil {
					state.Status = status
				}
			},
			CloseTx: func(txHash common.Hash, success bool, reason string) {
				service.closeTransaction(txHash, success, reason)
			},
		},
	)
	subscription := bc.SubscribeChainHeadEvent(service.chainHeadCh)
	service.chainHeadSub = subscription
	utils.SSCLogger().Info().Msg("ssc service start...")
	// 启动 worker pool
	service.startWorkers()
	go service.loop()
	return service
}

func (s *sscService) BindBlockChain(bcValue *atomic.Value) {
	bc := bcValue.Load().(core.BlockChain)
	s.bc = bc
	subscription := bc.SubscribeChainHeadEvent(s.chainHeadCh)
	s.chainHeadSub = subscription
	go s.loop()
}

func (s *sscService) BlockCommitted(block *types.Block) error {
	utils.SSCLogger().Info().Uint64("blockNum", block.NumberU64()).Msg("block committed")
	s.timerMgr.BlockCommitted(block.NumberU64())
	err := s.CommitteeMechanism.HandleBlockCommitted(block)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to handle block committed")
		return err
	}
	s.retryScheduler.OnBlockCommitted(block)
	return nil
}

func (s *sscService) loop() {
	statsTicker := time.NewTicker(30 * time.Second)
	defer statsTicker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			statsTicker.Stop()
			s.simuLock.Lock()
			s.stats.SetBlockTraces(s.txTraces)
			s.simuLock.Unlock()
			s.stats.Dump(s.lockStateMgr)
			utils.SSCLogger().Info().Msg("ssc service stop...")
			s.simulateTaskPQ.Stop()
			s.workerWg.Wait()
			return
		case <-s.chainHeadSub.Err():
			utils.SSCLogger().Error().Msg("chain head subscription error")
			return
		case head := <-s.chainHeadCh:
			utils.SSCLogger().Info().Uint64("blockNum", head.Block.NumberU64()).Msg("new block committed")
			b := head.Block
			var callStatesInWaiting []*api.SimulationCallState
			func() {
				s.syncLock.Lock()
				defer s.syncLock.Unlock()
				callStatesInWaiting = s.callStatesInWaiting[b.Header().Hash()]
				delete(s.callStatesInWaiting, b.Header().Hash())
			}()
			for _, callState := range callStatesInWaiting {
				var txHash common.Hash
				if callState.CallRequest != nil {
					txHash = callState.CallRequest.TxHash
				} else if callState.TopRequest != nil {
					txHash = callState.TopRequest.TxHash
				}
				utils.SSCLogger().Info().
					Str("txHash", txHash.Hex()).
					Str("callIndex", callState.CallIndex.ToString()).
					Str("blockHash", b.Hash().Hex()).
					Uint64("blockNum", b.NumberU64()).
					Msg("call state synced")
				callState.SyncedCh <- struct{}{}
			}
		case <-statsTicker.C:
			s.stats.sampleQueueLen(s.stats.QueuePushCount.Load(), s.stats.QueuePopCount.Load())
			s.simuLock.Lock()
			s.stats.SetBlockTraces(s.txTraces)
			s.simuLock.Unlock()
			s.stats.Dump(s.lockStateMgr)
		}
	}
}

func (s *sscService) startSimulation(req *api.CXTSimulationRequest) (*api.CXTSimulationState, *api.SimulationCallState, error) {
	txHash := req.TxHash
	_, err := s.startCXT(txHash, req.SimulationNum, s.SelfShard, make([]uint32, 0), req, nil)
	if err != nil {
		return nil, nil, err
	}

	s.stateLock.Lock()
	state, err := s.getState(txHash)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to get state")
		s.stateLock.Unlock()
		return nil, nil, err
	}
	// check if callIndex is already in progress, don't start again
	if callState := state.SimulationCallStates[state.SimulationNum].Get(api.CallIndex{}); callState != nil {
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msgf("simulation has been started, simulationNum=%d", state.SimulationNum)
		s.stateLock.Unlock()
		return state, callState, nil
	}
	s.stateLock.Unlock()

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

	header := s.bc.GetHeaderByHash(callState.BlockHash)
	if header == nil {
		s.waitForSync(txHash, callState)
		header = s.bc.GetHeaderByHash(callState.BlockHash)
		if header == nil {

			utils.SSCLogger().Info().
				Str("txHash", txHash.Hex()).
				Msg("start simulation 1.4")
			utils.SSCLogger().Error().Msg("failed to get block header")
			return nil, nil, errors.New("failed to get block header")
		}
	}

	db, err := s.bc.StateAt(header.Root())
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to get state")
		return nil, nil, err
	}
	callState.DB = db

	func() {
		s.commitLock.Lock()
		defer s.commitLock.Unlock()
		commitState := s.commitStates[txHash]
		if commitState == nil {
			s.commitStates[txHash] = &api.CommitState{
				CommitVotes:     make(map[int]map[uint32][]*api.CXTCommitVote),
				CommitSSCVotes:  make(map[int]map[uint32]*api.CXTCommitSSCVote),
				RollbackVotes:   make(map[uint32][]*api.CXTCommitVote),
				RollbackSSCVote: nil,
			}
		}
	}()

	s.stateLock.Lock()
	state, err = s.getState(txHash)
	if err != nil {
		s.stateLock.Unlock()
		utils.SSCLogger().Error().Err(err).Msg("failed to start simulation because of failed state")
		return nil, nil, err
	}
	state.CurrentCallFrame = &api.CallFrame{
		CallIndex: api.CallIndex{},
		PC:        0,
	}
	state.SimulationCallStates[req.SimulationNum] = state.SimulationCallStates[req.SimulationNum].Add(callState)
	s.stateLock.Unlock()

	s.pendingLock.Lock()
	defer s.pendingLock.Unlock()
	pendingList := s.pendingRequests[txHash]
	s.pendingRequests[txHash] = make([]*pendingCXTRequest, 0)
	if len(pendingList) > 0 {
		// 异步处理，避免阻塞当前 goroutine 和锁
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
					result := s.RequestCallCXT(req) // 递归调用，此时状态已存在
					utils.SSCLogger().Info().
						Str("txHash", txHash.Hex()).
						Str("callIndex", req.CallIndex.ToString()).
						Msg("finished processing pending CXT request")
					ch <- result
				}(p.req, p.waitingCh)
			} else {
				s.pendingRequests[txHash] = append(s.pendingRequests[txHash], p)
			}
		}
	}
	return state, callState, nil
}

func (s *sscService) startCXT(txHash common.Hash, simulationNum int, originShardId uint32, relatedShards api.RelatedShards,
	topRequest *api.CXTSimulationRequest, callRequest *api.CXTCallSSCRequest) (bool, error) {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	state, _ := s.getState(txHash)

	// don't start if the simulationNum is not larger than current one
	if state != nil && state.SimulationNum >= simulationNum {
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Msgf("the context has been started, current simulationNum %d, new simulationNum %d", state.SimulationNum, simulationNum)
		return false, nil
	}

	// set cxt timeout ctx
	ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)

	newState := &api.CXTSimulationState{
		CallStack:            api.NewCallStack(txHash, simulationNum),
		Status:               api.SIMULATING,
		SimulationRequest:    topRequest,
		SimulationResult:     nil,
		SimulationCallStates: make(map[int]api.SimulationCallStates),
		SimulationNum:        simulationNum,
		LockedCallIndex:      api.MINCallIndex,
		OriginShardId:        originShardId,
		RelatedShards:        relatedShards.Add(s.SelfShard),
		CallForest:           api.NewCallForest(),
		ReSimulationSignals:  make(map[int]map[uint32]*api.ReSimulationSignal),
		Ctx:                  ctx,
		CtxCancel:            cancel,
	}

	// start a new resimulation state
	if simulationNum > 0 {
		if state == nil {
			state = newState
		}
		// extend from originRequest
		if topRequest != nil {
			utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
				Msgf("extend from origin request, simulationNum %d", topRequest.SimulationNum)
			originReq := state.SimulationRequest
			topRequest.Tx = originReq.Tx
			topRequest.From = originReq.From
			topRequest.Author = originReq.Author
			topRequest.GasPool = originReq.GasPool
		}
		newState.SimulationRequest = topRequest
		newState.SimulationCallStates = state.SimulationCallStates
		newState.RelatedShards = newState.RelatedShards.Merge(state.RelatedShards)
		newState.CallStack = api.NewCallStack(txHash, simulationNum)
		newState.Status = api.RESIMULATING
	}

	if topRequest != nil {
		newState.Nonce = topRequest.Tx.Nonce()
		newState.TxSender, _ = topRequest.Tx.SenderAddress()
		newState.Epochs = topRequest.Epochs
	}
	if callRequest != nil {
		newState.Nonce = callRequest.Nonce
		newState.TxSender = common.BytesToAddress(callRequest.TxSender)
		newState.Epochs = callRequest.Epochs
	}

	newState.SimulationCallStates[simulationNum] = make(api.SimulationCallStates, 0)
	s.simulationState[txHash] = newState

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Msgf("startCXT for tx %s, simulationNum %d, FromShard %d, relatedShards %v, oldRelatedShards %v",
			txHash.Hex(), simulationNum, originShardId, newState.RelatedShards, relatedShards)

	return true, nil
}

func (s *sscService) handleTxSp1Timeout(info txInfo) {
	utils.SSCLogger().Info().Str("txHash", info.txHash.Hex()).Msgf("cxt has timeout for sp1, send rollback vote to ssc leader")
	payload := &api.CXTInvalidSimulationPayload{
		Type: api.CXTTimeout,
	}
	payloadBytes, _ := json.Marshal(payload)
	vote := &api.CXTCommitVote{
		BaseSSCMessage: api.BaseSSCMessage{
			Epochs: info.epochs,
		},
		TxHash:        info.txHash,
		ShardId:       s.SelfShard,
		OriginShardId: s.SelfShard,
		Type:          api.Rollback,
		Reason:        api.ReasonCxtTimeoutForSp1,
		Payload:       payloadBytes,
	}
	s.sendCXTCommitVote(s.SelfShard, vote)
}

// block committed will be called after apply the block, so we can close the transaction here after check if the tx's simulation is not onchain
func (s *sscService) handleTxPoolTimeout(info txInfo) {
	txHash := info.txHash
	existsOnChain := s.Verifier.HasVerifyContext(txHash)
	s.stateLock.RLock()
	_, err := s.getState(txHash)
	s.stateLock.RUnlock()
	if err != nil {
		return
	}
	if existsOnChain {
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
			Msgf("cxt has timeout for pool, but the tx is already onchain")
		return
	}
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
		Msgf("simulation is pool timeout, status=PoolTimeout, timeout for [%d, %d], current=%d, epoch=%v", info.blockNum, info.poolTimeout, info.poolTimeout, info.epochs)
	s.closeTransaction(txHash, false, "PoolTimeout")
}

func newStateSet() *api.StateSet {
	return &api.StateSet{
		Balance: make(map[common.Address]*big.Int),
		State:   make(map[common.Address]map[common.Hash]common.Hash),
	}
}

func newRWSet() *api.RWSet {
	return &api.RWSet{
		ReadState:    newStateSet(),
		WriteState:   newStateSet(),
		CurrentState: newStateSet(),
	}
}

func (s *sscService) SimulateCXTransaction(req *api.CXTSimulationRequest) {
	req.Epochs = s.CopyCurrentEpochs()
	leader := s.GetLeader(req.Epochs[s.SelfShard], s.SelfShard)

	// increment simulating count when request arrives
	atomic.AddInt64(&s.simulatingCount, 1)
	currentCount := atomic.LoadInt64(&s.simulatingCount)

	utils.SSCLogger().Info().Str("txHash", req.Tx.Hash().String()).Str("leader", leader.Endpoint).
		Int64("simulatingCount", currentCount).
		Msg("simulate cx transaction, send to pq")

	// 将任务发送到优先级队列（按 SimulationNum 降序执行）
	s.stats.QueuePushCount.Add(1)
	s.simulateTaskPQ.Push(&simulateTask{req: req, pushTime: time.Now()})
	s.stats.setCxtStage(req.Tx.Hash(), 0)

	utils.SSCLogger().Info().Str("txHash", req.Tx.Hash().String()).Str("leader", leader.Endpoint).
		Int64("simulatingCount", currentCount).
		Msg("simulate cx transaction, in chan")
}

// startWorkers 启动 worker pool
func (s *sscService) startWorkers() {
	for i := 0; i < s.numWorkers; i++ {
		s.workerWg.Add(1)
		go s.worker(i)
	}
	utils.SSCLogger().Info().Int("numWorkers", s.numWorkers).Msg("worker pool started")
}

// worker 处理模拟任务
func (s *sscService) worker(id int) {
	defer s.workerWg.Done()
	utils.SSCLogger().Info().Int("workerId", id).Msg("worker started")

	for {
		task, ok := s.simulateTaskPQ.PopOrWait()
		if !ok {
			utils.SSCLogger().Info().Int("workerId", id).Msg("worker stopped")
			return
		}
		// 队列等待统计
		waitTime := time.Since(task.pushTime)
		s.stats.QueueWaitTotalNs.Add(waitTime.Nanoseconds())
		s.stats.QueueWaitCount.Add(1)
		s.stats.QueuePopCount.Add(1)
		s.processSimulationTask(task, id)
	}
}

// processSimulationTask 处理单个模拟任务
func (s *sscService) processSimulationTask(task *simulateTask, workerId int) {
	startTime := time.Now()
	req := task.req
	leader := s.GetLeader(req.Epochs[s.SelfShard], s.SelfShard)

	defer func() {
		// decrement simulating count and update statistics
		atomic.AddInt64(&s.simulatingCount, -1)
		duration := time.Since(startTime)
		s.stats.SimTotalNs.Add(duration.Nanoseconds())
		s.stats.SimCount.Add(1)
		s.simuStatsLock.Lock()
		s.totalSimulations++
		s.totalSimuDuration += duration
		avgDuration := s.totalSimuDuration / time.Duration(s.totalSimulations)
		currentCount := atomic.LoadInt64(&s.simulatingCount)
		s.simuStatsLock.Unlock()

		utils.SSCLogger().Info().
			Str("txHash", req.Tx.Hash().String()).
			Interface("epochs", req.Epochs).
			Int("workerId", workerId).
			Dur("cost", duration).
			Dur("avgCost", avgDuration).
			Int64("currentCount", currentCount).
			Int64("totalCount", s.totalSimulations).
			Msg("simulate cx transaction, end")
	}()

	ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
	defer cancel()

	ret := new(api.CXTSimulationSSCResult)
	p2pStart := time.Now()
	err := s.Comm.Call(ctx, ret, leader, api.Method_StartSimulateCXTransaction, req)
	s.stats.P2pCallTotalNs.Add(time.Since(p2pStart).Nanoseconds())
	s.stats.P2pCallCount.Add(1)

	if err != nil {
		utils.SSCLogger().Error().Str("txHash", req.Tx.Hash().Hex()).Err(err).Msgf("failed to call start simulate cx transaction")
		s.stats.SimFailCount.Add(1)
		ret = &api.CXTSimulationSSCResult{Err: err.Error(), RelatedShards: []uint32{s.SelfShard},
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{
				Epochs: req.Epochs,
			},
		}
	} else {
		s.stats.SimSuccessCount.Add(1)
	}
}

func (s *sscService) CallCXTContract(req *api.CXTCallRequest) *api.CXTCallSSCResult {
	txHash := req.TxHash
	targetShardId := req.TargetShardId
	var (
		cxtState      *api.CXTSimulationState
		dependentCall *api.DependentCXTCall
		err           error
		committee     *api.ShardSimulateCommittee
	)
	sscResult := func() *api.CXTCallSSCResult {
		s.stateLock.Lock()
		defer s.stateLock.Unlock()

		cxtState, err = s.getState(txHash)
		if err != nil {
			utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).Msgf("failed to get state")
			return &api.CXTCallSSCResult{
				BaseBLSSignedMessage: api.BaseBLSSignedMessage{
					Epochs: req.Epochs,
				}, Err: err.Error()}
		}

		req.Nonce = cxtState.Nonce
		req.TxSender = cxtState.TxSender.Bytes()
		req.SimulationNum = cxtState.SimulationNum
		req.OriginShardId = cxtState.OriginShardId
		req.FromShardId = s.SelfShard
		req.Epochs = cxtState.Epochs
		committee = s.GetCommittee(req.Epochs[s.SelfShard], s.SelfShard)

		// 检查 CurrentCallFrame 是否为 nil，避免空指针
		if cxtState.CurrentCallFrame == nil {
			utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
				Msg("CurrentCallFrame is nil, this should not happen")
			return &api.CXTCallSSCResult{
				BaseBLSSignedMessage: api.BaseBLSSignedMessage{
					Epochs: req.Epochs,
				}, Err: "CurrentCallFrame is nil"}
		}

		req.CallIndex = make(api.CallIndex, len(cxtState.CurrentCallFrame.CallIndex))
		copy(req.CallIndex, cxtState.CurrentCallFrame.CallIndex)
		req.CallIndex = append(req.CallIndex, cxtState.CurrentCallFrame.PC)
		req.RelatedShards = cxtState.RelatedShards.Add(req.TargetShardId)
		cxtState.CurrentCallFrame.Next()

		// if it's recall and callState is locked
		if cxtState.SimulationNum > 0 && req.CallIndex.Compare(cxtState.LockedCallIndex) <= 0 {
			utils.SSCLogger().Debug().Str("txHash", txHash.String()).
				Str("callIndex", req.CallIndex.ToString()).
				Msg("recall contract has been locked, reuse it")
			callStates := cxtState.SimulationCallStates[req.SimulationNum]
			callerState := callStates.Get(req.CallIndex[:len(req.CallIndex)-1])

			callerState.StateLock.Lock()
			defer callerState.StateLock.Unlock()
			lockedCallStates := cxtState.SimulationCallStates[cxtState.SimulationNum-1]
			dependentCall = lockedCallStates.Get(req.CallIndex).DependentCXTCalls[req.CallIndex.ToString()]
			callerState.DependentCXTCalls[req.CallIndex.ToString()] = dependentCall
			ret := callerState.DependentCXTCalls[req.CallIndex.ToString()].SSCResult
			return ret
		}
		callStates := cxtState.SimulationCallStates[req.SimulationNum]
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
		if cxtState != nil {
			subNode := new(api.CallNode)
			subNode.FromData(txHash, sscResult.TreeNode)
			cxtState.CallForest.Insert(subNode)
		}
		return sscResult
	}

	sign, err := s.signerMgr.GetSSCSigner().Sign(req)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.String()).
			Str("callIndex", req.CallIndex.ToString()).Err(err).Msg("failed to sign cxt call ssc request")
		return nil
	}
	req.BaseSSCMessage.SenderAddr = s.SelfAddr
	req.BaseSSCMessage.Signature = sign
	req.BaseSSCMessage.Epochs = cxtState.Epochs

	leader := s.GetLeader(req.Epochs[s.SelfShard], s.SelfShard)
	startTime := time.Now()
	utils.SSCLogger().Info().
		Str("txHash", txHash.String()).
		Int("simulationNum", req.SimulationNum).
		Str("reqCallIndex", req.CallIndex.ToString()).
		Interface("relatedShards", req.RelatedShards).
		Uint32("selfShard", s.SelfShard).
		Uint32("targetShard", targetShardId).
		Str("addr", req.Addr.Hex()).
		Uint64("gas", req.Gas).
		Str("callIndex", req.CallIndex.ToString()).Msg("call cxt contract, start")

	ctx, cancel := context.WithTimeout(cxtState.Ctx, s.Config.CallTimeout)
	defer cancel()
	ret := new(api.CXTCallSSCResult)
	err = s.Comm.Call(ctx, ret, leader, api.Method_RequestCallCXT, req)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			utils.SSCLogger().Error().Err(err).Msg("failed to call cxt contract")
		}
		utils.SSCLogger().Info().Str("txHash", txHash.String()).
			Str("callIndex", req.CallIndex.ToString()).
			Uint32("targetShard", targetShardId).
			Str("relatedShards", fmt.Sprintf("%v", cxtState.RelatedShards)).
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
			err = s.signerMgr.GetSSCSigner().Verify(ret)
			if err != nil {
				utils.SSCLogger().Error().Str("txHash", txHash.String()).
					Str("callIndex", req.CallIndex.ToString()).Err(err).Msg("failed to verify cxt call ssc result signature")
				utils.SSCLogger().Info().Str("txHash", txHash.String()).
					Str("callIndex", req.CallIndex.ToString()).
					Uint32("targetShard", targetShardId).
					Str("relatedShards", fmt.Sprintf("%v", cxtState.RelatedShards)).
					Dur("cost", time.Since(startTime)).
					Msg("call cxt contract, end")
				// ✅ 即使验证失败，也要合并 RelatedShards
				if ret.RelatedShards != nil && len(ret.RelatedShards) > 0 {
					func() {
						s.stateLock.Lock()
						defer s.stateLock.Unlock()
						cxtState.RelatedShards = cxtState.RelatedShards.Merge(ret.RelatedShards)
						utils.SSCLogger().Debug().Str("txHash", txHash.String()).Interface("relatedShards", cxtState.RelatedShards).Msg("merged RelatedShards from failed verification")
					}()
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
				cxtState.CallForest.Insert(subNode)
				return ret
			}
		}
		func() {
			s.stateLock.Lock()
			defer s.stateLock.Unlock()
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
	cxtState.CallForest.Insert(subNode)
	// wait for sub execution finished
	subNode.Wait(s.SelfShard)
	return ret
}

// MOVED TO verify.go: func (v *Verifier) VerifySimulation(...)
//
//	func (s *sscService) VerifySimulation(simulationBytes []byte, stateDB api.StateDB, header *block.Header) {
//	  ... 搬到 verify.go ...
//	}
func (s *sscService) VerifySimulation(simulationBytes []byte, stateDB api.StateDB, header *block.Header) {
	s.Verifier.VerifySimulation(simulationBytes, stateDB, header)
}

// checkRetryLimitExceeded 检查链上重试次数是否超限
// 所有节点（含非 leader validator）都可调用，不依赖 CXTSimulationState
// 返回 (是否超限, 记录的 lockedSimNum)
// MOVED TO verify.go
/*
func (s *sscService) checkRetryLimitExceeded(...)
func (s *sscService) sendRollbackVoteForRetry(...)
func (s *sscService) checkLockConflict(...)
func (s *sscService) lockStateWithExecution(...)
func (s *sscService) lockStateWithRWSet(...)
*/

// MOVED TO verify.go (Verifier.lockStateWithRWSet)
func (s *sscService) lockStateWithRWSet(txHash common.Hash, callState *api.CXTCallState, stateDB api.StateDB) {
	s.Verifier.lockStateWithRWSet(txHash, callState, stateDB)
}

// MOVED TO verify.go (VerifyCommunicator.SendCommitVote)
func (s *sscService) sendCXTCommitVote(shardId uint32, vote *api.CXTCommitVote) {
	// s.Verifier.communicator.SendCommitVote(...) — 但我们不能直接访问
	// 因为 communicator 是 Verifier 的私有字段
	// 重新实现：用 s 自己的字段
	var signer api.BLSSigner
	if vote.Type == api.Recall {
		signer = s.BLSSignerMgr.GetSSCSigner()
	} else {
		signer = s.BLSSignerMgr.GetValidatorSigner()
	}
	sign, err := signer.Sign(vote)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", vote.TxHash.Hex()).
			Msg("failed to sign CXTCommitVote")
		return
	}
	vote.BaseSSCMessage.SenderAddr = signer.Address()
	vote.BaseSSCMessage.Signature = sign

	ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
	defer cancel()

	targetLeader := s.GetLeader(vote.Epochs[shardId], shardId)
	if targetLeader == nil {
		utils.SSCLogger().Error().Str("txHash", vote.TxHash.Hex()).
			Msg("failed to get leader")
		return
	}
	utils.SSCLogger().Info().Str("txHash", vote.TxHash.Hex()).
		Str("type", vote.Type.String()).Str("reason", vote.Reason.String()).
		Msgf("send CXTCommitVote to %s", targetLeader.Endpoint)
	s.Comm.Call(ctx, nil, targetLeader, api.Method_HandleCommitVote, vote)
}

// MOVED TO committer.go — Committer.CommitOrRollbackWithProof
func (s *sscService) CommitOrRollbackWithProof(commitProofBytes []byte, stateDB api.StateDB, blockNum uint64) error {
	return s.Committer.CommitOrRollbackWithProof(commitProofBytes, stateDB, blockNum)
}

func (s *sscService) NewEpoch(newEpochBytes []byte, sscvm api.VM, stateDB api.StateDB, blockNum uint64) error {
	newEpoch := &api.NewEpoch{}
	err := json.Unmarshal(newEpochBytes, newEpoch)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to unmarshal new epoch")
		return err
	}
	// utils.SSCLogger().Info().Uint32("selfShardId", s.SelfShard).Interface("newEpoch", newEpoch).Msg("new epoch")

	if s.IsLeader(newEpoch.Committee.Epoch - 1) {

		startTime := time.Now()
		wg := sync.WaitGroup{}

		committees := s.GetLatestCommittees()
		for shardId, committee := range committees {
			for _, validator := range committee.Validators {
				wg.Add(1)
				go func(validator *api.Member, shardId uint32) {
					defer wg.Done()
					catllErr := s.comm.Call(s.ctx, nil, validator, api.Method_HandleNewEpoch, newEpoch, blockNum)
					if catllErr != nil {
						utils.SSCLogger().Error().Err(catllErr).Msg("failed to call new epoch")
					}
				}(validator, shardId)
			}
		}
		utils.SSCLogger().Info().Dur("cost", time.Since(startTime)).Msgf("leader broadcast new epoch to every validator, epoch=%d, blockNum=%d", newEpoch.Committee.Epoch, blockNum)
	}

	return nil
}

func (s *sscService) StartSimulateCXTransaction(req *api.CXTSimulationRequest) *api.CXTSimulationSSCResult {
	var waitingCh chan *api.CXTSimulationSSCResult
	txHash := req.Tx.Hash()

	startTime := time.Now()
	utils.SSCLogger().Info().Int("simulationNum", req.SimulationNum).Str("txHash", txHash.String()).Msg("start simulate cx transaction, start")
	defer func() {
		utils.SSCLogger().Info().Int("simulationNum", req.SimulationNum).Str("txHash", txHash.String()).Dur("cost", time.Since(startTime)).Msg("start simulate cx transaction, end")
	}()

	func() {
		s.simuLock.Lock()
		defer s.simuLock.Unlock()
		if s.simuWaitingChs[txHash] == nil {
			s.simuWaitingChs[txHash] = make([]chan *api.CXTSimulationSSCResult, 0)
		} else {
			waitingCh = make(chan *api.CXTSimulationSSCResult)
			s.simuWaitingChs[txHash] = append(s.simuWaitingChs[txHash], waitingCh)
		}
		if s.simuResultCh[txHash] == nil {
			s.simuResultCh[txHash] = make(chan *api.CXTSimulationSSCResult, 1)
		}
	}()

	if waitingCh != nil {
		return <-waitingCh
	}

	defer func() {
		s.simuLock.Lock()
		delete(s.simuWaitingChs, txHash)
		s.simuLock.Unlock()
	}()

	header := s.bc.CurrentHeader()
	req.BlockHash = header.Hash()
	req.BlockNum = header.NumberU64()
	s.recordTraceBlock(txHash, StageSimulateCX, header.NumberU64())
	committee := s.GetCommittee(req.Epochs[s.SelfShard], s.SelfShard)
	n := committee.Number
	t := committee.Threshold

	state, _, _ := s.startSimulation(req)

	ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
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

	utils.SSCLogger().Info().Int("simulationNum", req.SimulationNum).Str("txHash", txHash.String()).Msg("start simulate cx transaction, call to handle simulation request")

	for i := 0; i < n; i++ {
		member := committee.Members[i]
		go func(m *api.Member, c *api.ShardSimulateCommittee) {
			defer wg.Done()
			ret := new(api.CXTSimulationResult)
			callErr := s.Comm.Call(ctx, ret, m, api.Method_HandleSimulateRequest, req)
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

			if bytes.Compare(m.Address.Bytes(), s.SelfAddr.Bytes()) == 0 {
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
				utils.SSCLogger().Info().Interface("addrMap", addrMap).Str("txHash", txHash.String()).Msg("simulation result received, including self")
				cancel()
			}
		}(member, committee)
	}
	wg.Wait()

	var (
		sscResult *api.CXTSimulationSSCResult
		err       error
	)
	if len(results) == t {
		leaderRet := results[0].(*api.CXTSimulationResult)
		if leaderRet.Err != "" {
			sscResult = &api.CXTSimulationSSCResult{
				Err:           leaderRet.Err,
				RelatedShards: []uint32{s.SelfShard},
				BaseBLSSignedMessage: api.BaseBLSSignedMessage{
					Epochs: leaderRet.Epochs,
				},
			}
			utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(errors.New(leaderRet.Err)).Msg("failed to simulate transaction")
		} else {
			sscResult, err = s.aggregateSimulationResults(results)
			if err != nil {
				sscResult = &api.CXTSimulationSSCResult{Err: err.Error(), RelatedShards: []uint32{s.SelfShard},
					BaseBLSSignedMessage: api.BaseBLSSignedMessage{
						Epochs: leaderRet.Epochs,
					}}
				utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(err).Msg("failed to aggregate simulation results")
				return sscResult
			}
		}
	} else {
		if len(results) == 0 {
			sscResult = &api.CXTSimulationSSCResult{Err: "no response from other shards", RelatedShards: []uint32{s.SelfShard},
				BaseBLSSignedMessage: api.BaseBLSSignedMessage{
					Epochs: req.Epochs,
				}}
		} else {
			errRet := results[len(results)-1].(*api.CXTSimulationResult)
			sscResult = &api.CXTSimulationSSCResult{Err: errRet.Err, RelatedShards: []uint32{s.SelfShard},
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
			ShardId: s.SelfShard,
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
		if vm.IsLockedByOtherTxErr(sscResult.Err) {
			utils.SSCLogger().Debug().Str("txHash", simulationCommit.TxHash.Hex()).
				Msgf("simulation's state is locked by other tx, try to retry if self is leader: %v", s.IsLeader(req.Epochs[s.SelfShard]))
			simulationCommit.Commit = false
			simulationCommit.Status = api.LockConflict
			simulationCommit.Reason = sscResult.Err
			// don't send commit simulation if state is locked by other tx
			if s.IsLeader(req.Epochs[s.SelfShard]) {
				s.retryScheduler.CallForRetry(&api.RetryTx{
					TxHash:        txHash,
					Epochs:        req.Epochs,
					RelatedShards: sscResult.RelatedShards,
					SimulationNum: req.SimulationNum + 1,
					Condition:     api.Simulate,
					OriginShardID: state.OriginShardId,
				})
			}
			s.stateLock.Lock()
			state, err = s.getState(txHash)
			if err != nil {
				s.stateLock.Unlock()
				sscResult = &api.CXTSimulationSSCResult{Err: err.Error(), RelatedShards: []uint32{s.SelfShard},
					BaseBLSSignedMessage: api.BaseBLSSignedMessage{
						Epochs: req.Epochs,
					},
				}
				utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(err).Msg("failed to get state")
				return sscResult
			}
			state.Status = api.WAITING_FOR_RESIMULATION_OFF_CHAIN
			s.stateLock.Unlock()
			return sscResult
		} else {
			simulationCommit.Commit = false
			simulationCommit.Status = api.ExecutionFailed
			simulationCommit.Reason = sscResult.Err
		}
		utils.SSCLogger().Error().Str("txHash", simulationCommit.TxHash.Hex()).
			Err(errors.New(sscResult.Err)).Msgf("simulation accomplished, send simulation commit, commit type: %v, status: %v, simulationNum: %d, relatedShards: %v",
			simulationCommit.Commit, simulationCommit.Status.String(), simulationCommit.SimulationNum, simulationCommit.RelatedShards)
	}

	s.stateLock.Lock()
	state, err = s.getState(req.Tx.Hash())
	if err != nil {
		s.stateLock.Unlock()
		sscResult = &api.CXTSimulationSSCResult{Err: err.Error(), RelatedShards: []uint32{s.SelfShard},
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{
				Epochs: req.Epochs,
			},
		}
		utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(err).Msg("failed to get state")
		return sscResult
	}
	s.stateLock.Unlock()

	var chs []chan *api.CXTSimulationSSCResult

	func() {
		s.simuLock.Lock()
		defer s.simuLock.Unlock()
		state.SimulationCallStates[state.SimulationNum][0].TopSSCResult = sscResult
		state.SimulationResult = sscResult
		chs = s.simuWaitingChs[txHash]
	}()

	for _, ch := range chs {
		ch <- sscResult
	}

	// TempLockView pre-check: 在 SubmitSimulationTx 前检查锁冲突
	// 仿真结束知道 RWSet 了，用 TempLockView 预判 on-chain 是否会锁冲突
	// 如果冲突，不进 on-chain 直接进 retryPool，避免浪费区块资源
	if simulationCommit.Commit && s.IsLeader(req.Epochs[s.SelfShard]) {
		if callStates, ok := state.SimulationCallStates[state.SimulationNum]; ok && len(callStates) > 0 {
			callState := callStates[0]
			if callState != nil && callState.RWSet != nil {
				reads := make([]api.LockKey, 0)
				writes := make([]api.LockKey, 0)
				for addr, account := range callState.RWSet.ReadState.State {
					for key := range account {
						reads = append(reads, api.FormKey(addr, key))
					}
				}
				for addr, account := range callState.RWSet.WriteState.State {
					for key := range account {
						writes = append(writes, api.FormKey(addr, key))
					}
				}
				if !s.retryScheduler.tempLockView.TryLock(txHash, reads, writes) {
					utils.SSCLogger().Warn().Str("txHash", txHash.Hex()).
						Int("reads", len(reads)).Int("writes", len(writes)).
						Msg("TempLockView pre-check failed, deferring to retry pool")
					simulationCommit.Commit = false
					simulationCommit.Status = api.LockConflict
					simulationCommit.Reason = "temp lock conflict"
					s.stateLock.Lock()
					state, err = s.getState(txHash)
					if err == nil {
						state.Status = api.WAITING_FOR_RESIMULATION_OFF_CHAIN
					}
					s.stateLock.Unlock()
					s.retryScheduler.CallForRetry(&api.RetryTx{
						TxHash:        txHash,
						Epochs:        req.Epochs,
						RelatedShards: sscResult.RelatedShards,
						SimulationNum: req.SimulationNum + 1,
						Condition:     api.Simulate,
						OriginShardID: state.OriginShardId,
					})
					return sscResult
				}
			}
		}
	}

	s.thresholdSignSimulationCommit(simulationCommit)

	leaders := make([]*api.Member, 0)
	for _, shardId := range simulationCommit.RelatedShards {
		leaders = append(leaders, s.GetLeader(req.Epochs[shardId], shardId))
	}
	ctxCS, cancelCS := context.WithTimeout(s.ctx, s.Config.CallTimeout)
	defer cancelCS()

	s.Comm.Multicast(ctxCS, leaders, api.Method_CommitSimulation, simulationCommit)

	var resultCh chan *api.CXTSimulationSSCResult
	func() {
		s.simuLock.Lock()
		defer s.simuLock.Unlock()
		if s.simuResultCh[txHash] != nil {
			resultCh = s.simuResultCh[txHash]
			close(s.simuResultCh[txHash])
			delete(s.simuResultCh, txHash)
		}
	}()
	if resultCh != nil {
		<-resultCh
	}

	s.stats.setCxtStage(txHash, 1)
	return sscResult
}

func (s *sscService) thresholdSignSimulationCommit(commit *api.SimulationCommit) {
	committee := s.GetCommittee(commit.Epochs[s.SelfShard], s.SelfShard)
	n := committee.Number
	t := committee.Threshold
	txHash := commit.TxHash
	s.stateLock.RLock()
	state, err := s.getState(txHash)
	s.stateLock.RUnlock()
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(err).Msg("failed to get state")
		return
	}

	ctx, cancel := context.WithTimeout(state.Ctx, s.Config.CallTimeout)
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
			signErr := s.Comm.Call(ctx, &signature, member, api.Method_SignSimulationCommit, commit)
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

			if bytes.Compare(m.Address.Bytes(), s.SelfAddr.Bytes()) == 0 {
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

	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
		Int("results", len(results)).
		Bool("ctxDone", ctx.Err() != nil).
		Msg("thresholdSignSimulationCommit: after wg.Wait")

	aggregatedSig, bitMap, err := s.BLSSignerMgr.GetSSCSigner().Aggregate(results)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Str("txHash", txHash.Hex()).
			Msg("thresholdSignSimulationCommit: aggregate failed")
		return
	}
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
		Int("bitMapLen", len(bitMap)).
		Msg("thresholdSignSimulationCommit: aggregated OK")
	commit.Signatures = aggregatedSig
	commit.BLSBitMap = bitMap
	commit.ShardId = s.SelfShard
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
		Msg("thresholdSignSimulationCommit: done")
}

func (s *sscService) aggregateSimulationResults(results []api.SSCMessage) (*api.CXTSimulationSSCResult, error) {
	if len(results) == 0 {
		return nil, errors.New("no simulation results to aggregate")
	}

	result := results[0].(*api.CXTSimulationResult)
	aggregatedSig, bitMap, err := s.BLSSignerMgr.GetSSCSigner().Aggregate(results)
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
		BaseBLSSignedMessage: api.BaseBLSSignedMessage{
			ShardId:    s.SelfShard,
			Signatures: aggregatedSig,
			BLSBitMap:  bitMap,
			Epochs:     result.Epochs,
		},
	}
	return sscResult, nil
}

func (s *sscService) HandleSimulateRequest(ctx context.Context, req *api.CXTSimulationRequest) *api.CXTSimulationResult {
	startTime := time.Now()
	txHash := req.TxHash
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Int("simulationNum", req.SimulationNum).Msg("handle simulate request, start")
	defer func() {
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Dur("cost", time.Since(startTime)).Msg("handle simulate request, end")
	}()
	simuState, callState, err := s.startSimulation(req)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).Msg("failed to start simulation")
		return &api.CXTSimulationResult{
			Err:            err.Error(),
			BaseSSCMessage: api.BaseSSCMessage{Epochs: req.Epochs},
		}
	}

	s.stateLock.Lock()
	simuState.CurrentCallFrame = &api.CallFrame{
		CallIndex: api.CallIndex{},
		PC:        0,
	}
	simuState.CallStack.Push(simuState.CurrentCallFrame)
	s.stateLock.Unlock()

	defer func() {
		s.stateLock.Lock()
		defer s.stateLock.Unlock()
		simuState.CallStack.Pop()
		simuState.CurrentCallFrame = simuState.CallStack.Top()
	}()

	callState.Lock.Lock()
	defer callState.Lock.Unlock()

	relatedShards := simuState.RelatedShards
	simuState.SimulateCh = make(chan struct{})
	ch := simuState.SimulateCh

	rootNode := api.NewCallNode(txHash, api.CallIndex{}, s.SelfShard)
	simuState.CallForest.Insert(rootNode)

	defer func() {
		rootNode.Done()
		rootNode.Wait(s.SelfShard)
		close(ch)
	}()

	header := s.bc.GetHeaderByHash(req.BlockHash)
	if header == nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msg("header not found")
		return &api.CXTSimulationResult{
			Err:            "header not found",
			BaseSSCMessage: api.BaseSSCMessage{Epochs: req.Epochs},
		}
	}
	stateDB, _ := s.bc.StateAt(header.Root())
	if stateDB == nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msg("stateDB not found")
		return &api.CXTSimulationResult{Err: "stateDB not found",
			BaseSSCMessage: api.BaseSSCMessage{Epochs: req.Epochs}}
	}
	chainConfig := s.bc.Config()
	vmConfig := s.bc.GetVMConfig()

	s.timerMgr.StartPoolTimer(txHash, req.Epochs, header.NumberU64(), s.SelfShard)

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
		tx = simuState.SimulationRequest.Tx
		gp = new(core.GasPool).AddGas(simuState.SimulationRequest.GasPool)
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
	vmCtx := core.NewSSCVMContext(msg.From(), tx.Hash(), api.CallIndex{}, tx.GasPrice(), header, s.bc, req.Author)
	vmCtx.TxType = types.CXTransaction
	sscvm := vm.NewSSCVM(vmCtx, stateDB, chainConfig, *vmConfig, s, executionType)
	result, err := core.NewSSCStateTransition(sscvm, msg, gp).TransitionDb()
	if err != nil {
		ret = &api.CXTSimulationResult{Err: err.Error(), BaseSSCMessage: api.BaseSSCMessage{Epochs: req.Epochs}}
	} else {
		s.stateLock.RLock()
		newSimuState, err := s.getState(txHash)
		if err != nil {
			utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).Msg("failed to get stateDB")
			s.stateLock.RUnlock()
			return &api.CXTSimulationResult{
				Err:            err.Error(),
				BaseSSCMessage: api.BaseSSCMessage{Epochs: req.Epochs},
			}
		}
		relatedShards = newSimuState.RelatedShards
		s.stateLock.RUnlock()

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
			ret.Err = result.VMErr.Error()
		}
		if callState.LockedByOtherTx != nil {
			ret.Err = api.ErrLockedByOtherTx.Error()
		}
	}
	sign, err := s.signerMgr.GetSSCSigner().Sign(ret)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Str("txHash", txHash.Hex()).Msg("failed to sign simulation result")
		return nil
	}
	ret.BaseSSCMessage.Signature = sign
	ret.BaseSSCMessage.SenderAddr = s.SelfAddr

	return ret
}

// safeCallRequest 封装单个请求的上下文信息
type safeCallRequest struct {
	req       *api.CXTCallRequest
	waitingCh chan *api.CXTCallSSCResult
}

// safeDependentCall 线程安全的依赖调用聚合器
type safeDependentCall struct {
	*api.DependentCXTCall
	lock sync.RWMutex
	// waitCh 已移至 DependentCXTCall.WaitCh，所有请求共享同一个 channel
}

func (s *sscService) RequestCallCXT(req *api.CXTCallRequest) *api.CXTCallSSCResult {
	txHash := req.TxHash
	startTime := time.Now()

	// 获取当前分片委员会信息
	committee := s.GetCommittee(req.Epochs[s.SelfShard], s.SelfShard)
	threshold := committee.Threshold

	s.stateLock.RLock()
	state, err := s.getState(txHash)
	s.stateLock.RUnlock()
	if err != nil {
		return &api.CXTCallSSCResult{
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{
				Epochs: req.Epochs,
			},
			Err: fmt.Sprintf("failed to get state=%v", err)}
	}

	// 创建带超时的 context，避免永久阻塞
	ctx, cancel := context.WithTimeout(state.Ctx, s.Config.CallTimeout)
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

	_, dependentCall, err := s.getOrCreateCallerState(txHash, req, committee)
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
	reachThreshold := s.registerRequest(dependentCall, callReq, threshold)
	utils.SSCLogger().Info().
		Str("txHash", txHash.Hex()).
		Str("callIndex", req.CallIndex.ToString()).
		Interface("epochs", req.Epochs).
		Msgf("RequestCallCXT: register request, reached threshold: %v", reachThreshold)

	// ========== 阶段 3: 根据是否达到阈值分支处理 ==========

	if reachThreshold {
		// 当前请求使计数达到阈值，负责聚合并发起远程调用
		result := s.executeThresholdAction(ctx, txHash, req, dependentCall, committee)
		return result
	} else {
		// 未达到阈值，等待其他请求或超时
		result := s.waitForResult(ctx, callReq, dependentCall)
		return result
	}
}

// getOrCreateCallerState 获取或创建 caller 状态和 dependentCall
func (s *sscService) getOrCreateCallerState(
	txHash common.Hash,
	req *api.CXTCallRequest,
	committee *api.ShardSimulateCommittee,
) (*api.SimulationCallState, *safeDependentCall, error) {

	s.stateLock.RLock()
	cxtState, err := s.getState(txHash)
	if err != nil {
		s.stateLock.RUnlock()
		return nil, nil, err
	}

	callStates := cxtState.SimulationCallStates[req.SimulationNum]
	callerState := callStates.Get(req.CallIndex[:len(req.CallIndex)-1])
	s.stateLock.RUnlock()

	// 如果 callerState 不存在，放入 pending 队列等待
	if callerState == nil {
		pendingResult := s.handlePendingRequest(txHash, req, cxtState.Ctx)
		if pendingResult != nil {
			s.stateLock.RLock()
			cxtState, err = s.getState(txHash)
			if err != nil {
				s.stateLock.RUnlock()
				return nil, nil, err
			}
			callStates = cxtState.SimulationCallStates[req.SimulationNum]
			callerState = callStates.Get(req.CallIndex[:len(req.CallIndex)-1])
			s.stateLock.RUnlock()
		}
	}

	// 再次检查，如果仍不存在则返回错误
	if callerState == nil {
		return nil, nil, errors.New("caller state not found and pending wait failed")
	}

	// 获取或创建 dependentCall（使用独立的 depCallLock 保护，避免与合约执行竞争）
	dependentCall := s.getOrCreateDependentCall(txHash, callerState, req.CallIndex.ToString(), committee.Threshold)

	return callerState, dependentCall, nil
}

// handlePendingRequest 处理 callerState 不存在的情况
func (s *sscService) handlePendingRequest(
	txHash common.Hash,
	req *api.CXTCallRequest,
	cxtCtx context.Context,
) *api.CXTCallSSCResult {

	waitingCh := make(chan *api.CXTCallSSCResult, 1)

	s.pendingLock.Lock()
	if s.pendingRequests == nil {
		s.pendingRequests = make(map[common.Hash][]*pendingCXTRequest)
	}
	s.pendingRequests[txHash] = append(s.pendingRequests[txHash], &pendingCXTRequest{
		req:       req,
		waitingCh: waitingCh,
	})
	s.pendingLock.Unlock()

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
		s.pendingLock.Lock()
		pendingList := s.pendingRequests[txHash]
		newList := make([]*pendingCXTRequest, 0, len(pendingList))
		for _, p := range pendingList {
			if p.waitingCh != waitingCh {
				newList = append(newList, p)
			}
		}
		s.pendingRequests[txHash] = newList
		s.pendingLock.Unlock()
		return &api.CXTCallSSCResult{
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{
				Epochs: req.Epochs,
			}, Err: fmt.Sprintf("pending request timeout: %v", cxtCtx.Err())}
	}
}

// getOrCreateDependentCall 获取或创建 dependentCall
// 使用独立的 depCallLock 保护 DependentCXTCalls 访问，避免与合约执行的 callerState.Lock 竞争
func (s *sscService) getOrCreateDependentCall(
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
func (s *sscService) registerRequest(
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
func (s *sscService) executeThresholdAction(
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
		sscReq = s.aggregateSSCCallRequest(dependentCall.Requests)
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
	leader := s.GetLeader(req.Epochs[req.TargetShardId], req.TargetShardId)

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
	s.stateLock.RLock()
	cxtState, err := s.getState(txHash)
	s.stateLock.RUnlock()

	if err != nil || cxtState == nil {
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
	callCtx, callCancel := context.WithTimeout(ctx, s.Config.CallTimeout)
	defer callCancel()

	utils.SSCLogger().Info().
		Str("txHash", txHash.Hex()).
		Str("callIndex", req.CallIndex.ToString()).
		Str("leader", leader.Endpoint).
		Msg("RequestCallCXT: initiating remote call")

	err = s.Comm.Call(callCtx, ret, leader, api.Method_HandleCXTSSCCall, sscReq)
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
func (s *sscService) waitForResult(
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

func (s *sscService) aggregateSSCCallRequest(requests []*api.CXTCallRequest) *api.CXTCallSSCRequest {
	if len(requests) == 0 {
		utils.SSCLogger().Error().Msg("no cxt call requests to aggregate")
		return nil
	}
	request := requests[0]
	committee := s.GetCommittee(request.Epochs[s.SelfShard], s.SelfShard)
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
			utils.SSCLogger().Info().Str("txHash", msg.TxHash.Hex()).Str("callIndex", msg.CallIndex.ToString()).Str("addr", msg.GetSenderAddr().Hex()).Msgf("cxt call request sender in committee, validatorIndex=%d", validatorIndex)
		}
		msgs = append(msgs, msg)
	}
	aggregatedSig, bitMap, err := s.BLSSignerMgr.GetSSCSigner().Aggregate(msgs)
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
			ShardId:    s.SelfShard,
			Signatures: aggregatedSig,
			BLSBitMap:  bitMap,
			Epochs:     request.Epochs,
		},
		BlockHash: common.Hash{},
		BlockNum:  0,
	}
	return sscRequest
}

func (s *sscService) HandleCXTCall(req *api.CXTCallSSCRequest) *api.CXTCallResult {
	err := s.signerMgr.GetSSCSigner().Verify(req)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to verify cxt call request signature")
		return &api.CXTCallResult{
			Err: fmt.Sprintf("error from targetShard %d, err=%s", req.TargetShardId, err.Error()),
			BaseSSCMessage: api.BaseSSCMessage{
				Epochs:     req.Epochs,
				SenderAddr: s.SelfAddr,
			},
		}
	}

	txHash := req.TxHash
	// begin cxt call, update simulationState and callIndex
	callState, err := s.startCall(req)
	if err != nil {
		return &api.CXTCallResult{
			Err: fmt.Sprintf("error from targetShard %d, err=%s", req.TargetShardId, err.Error()),
			BaseSSCMessage: api.BaseSSCMessage{
				Epochs:     req.Epochs,
				SenderAddr: s.SelfAddr,
			},
		}
	}
	s.stateLock.Lock()
	simuState, stateErr := s.getState(txHash)
	if stateErr != nil {
		s.stateLock.Unlock()
		return &api.CXTCallResult{
			Err: "transaction has been closed",
			BaseSSCMessage: api.BaseSSCMessage{
				Epochs:     req.Epochs,
				SenderAddr: s.SelfAddr,
			},
		}
	}
	simuState.CurrentCallFrame = &api.CallFrame{
		CallIndex: req.CallIndex,
		PC:        0,
	}
	simuState.CallStack.Push(simuState.CurrentCallFrame)
	s.stateLock.Unlock()

	defer s.endCall(req)
	callState.Lock.Lock()
	defer callState.Lock.Unlock()

	startTime := time.Now()
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Str("callIndex", req.CallIndex.ToString()).Str("addr", req.Addr.Hex()).Msg("handle cxt call, start")
	defer func() {
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Str("callIndex", req.CallIndex.ToString()).Dur("cost", time.Since(startTime)).Msg("handle cxt call, end")
	}()

	header := s.bc.GetHeaderByHash(req.BlockHash)
	if header == nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Str("callIndex", req.CallIndex.ToString()).Msg("failed to get header")
		return &api.CXTCallResult{
			Err: "failed to get header",
			BaseSSCMessage: api.BaseSSCMessage{
				Epochs:     req.Epochs,
				SenderAddr: s.SelfAddr,
			},
		}
	}
	state, err := s.bc.StateAt(header.Root())
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to get stateDB")
		return &api.CXTCallResult{Err: "failed to get stateDB",
			BaseSSCMessage: api.BaseSSCMessage{
				Epochs:     req.Epochs,
				SenderAddr: s.SelfAddr,
			}}
	}
	chainConfig := s.bc.Config()
	vmConfig := s.bc.GetVMConfig()
	sender := vm.AccountRef(req.Caller)
	var executionType vm.ExecutionType
	if req.SimulationNum == 0 {
		executionType = vm.SimulationCall
	} else {
		executionType = vm.SimulationReCall
	}

	s.timerMgr.StartPoolTimer(txHash, req.Epochs, header.NumberU64(), s.SelfShard)

	callNode := api.NewCallNode(req.TxHash, req.CallIndex, s.SelfShard)
	simuState.CallForest.Insert(callNode)
	defer func() {
		callNode.Done()
	}()
	// create sscvm instance
	vmCtx := core.NewSSCVMContext(req.Caller, txHash, req.CallIndex, req.GasPrice, header, s.bc, nil)
	sscvm := vm.NewSSCVM(vmCtx, state, chainConfig, *vmConfig, s, executionType)

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("call contract with %s, callIndex=%s, simulationNum=%d",
		executionType.String(), req.CallIndex.ToString(), req.SimulationNum)
	ret, leftOverGas, err := sscvm.CallFromOtherShard(req.FromShardId, sender, req.Addr, req.Input, req.Gas, req.Value)

	if ret == nil && err == nil {
		utils.SSCLogger().Error().Err(err).Str("txHash", txHash.Hex()).Str("callIndex", req.CallIndex.ToString()).Msgf("successfully return nil, addr=%s, leftOverGas=%d, ret=%s", req.Addr, leftOverGas, ret)
	}

	s.stateLock.RLock()
	simuState, stateErr = s.getState(txHash)
	if stateErr != nil {
		s.stateLock.RUnlock()
		return &api.CXTCallResult{
			Err:           "transaction has been closed",
			RelatedShards: []uint32{s.SelfShard},
			BaseSSCMessage: api.BaseSSCMessage{
				Epochs:     req.Epochs,
				SenderAddr: s.SelfAddr,
			},
		}
	}
	relatedShards := simuState.RelatedShards.Merge(req.RelatedShards).Add(req.FromShardId)
	s.stateLock.RUnlock()
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
		result.Err = api.ErrLockedByOtherTx.Error()
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Str("callIndex", req.CallIndex.ToString()).Msgf("return locked by other tx")
	}
	if err != nil {
		result.Err = fmt.Sprintf("error from targetShard %d, err=%s", req.TargetShardId, err.Error())
		utils.SSCLogger().Error().Err(err).Msg("failed to call contract")
	}
	sign, err := s.signerMgr.GetSSCSigner().Sign(result)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to sign cxt call result")
		return &api.CXTCallResult{
			Err:           "failed to sign cxt call result",
			RelatedShards: []uint32{s.SelfShard},
			BaseSSCMessage: api.BaseSSCMessage{
				Epochs:     req.Epochs,
				SenderAddr: s.SelfAddr,
			},
		}
	}
	result.BaseSSCMessage = api.BaseSSCMessage{
		Signature:  sign,
		SenderAddr: s.SelfAddr,
		Epochs:     req.Epochs,
	}
	utils.SSCLogger().Debug().
		Str("txHash", txHash.Hex()).
		Str("callIndex", req.CallIndex.ToString()).
		Msgf("cxt call result: %s, err: %s", common.Bytes2Hex(result.Result), result.Err)
	return result
}

func (s *sscService) startCall(req *api.CXTCallSSCRequest) (*api.SimulationCallState, error) {
	// try to start cxt if not exist
	txHash := req.TxHash
	_, err := s.startCXT(txHash, req.SimulationNum, req.OriginShardId, req.RelatedShards, nil, req)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to start cxt")
		return nil, err
	}

	s.stateLock.Lock()
	state, err := s.getState(txHash)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to get state")
		s.stateLock.Unlock()
		return nil, err
	}
	// check if callIndex is already in progress, don't start again
	if state.SimulationCallStates[state.SimulationNum].Get(req.CallIndex) != nil {
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("callIndex has been started, simulationNum=%d, callIndex=%s", state.SimulationNum, req.CallIndex.ToString())
		s.stateLock.Unlock()
		return state.SimulationCallStates[state.SimulationNum].Get(req.CallIndex), nil
	}
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Str("simulatedCallStates", state.SimulationCallStates[state.SimulationNum].ToString()).
		Msgf("start call, simulationNum=%d, callIndex=%s", state.SimulationNum, req.CallIndex.ToString())
	callState := state.SimulationCallStates[req.SimulationNum].Get(req.CallIndex)
	s.stateLock.Unlock()

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
		header := s.bc.GetHeaderByHash(req.BlockHash)
		// need to sync state
		if header == nil {
			utils.SSCLogger().Debug().Msgf("block header not found, waiting for sync")
			s.waitForSync(txHash, callState)
			utils.SSCLogger().Debug().Msgf("sync completed, block header founded")
			header = s.bc.GetHeaderByHash(req.BlockHash)
		}
		stateAt, err := s.bc.StateAt(header.Root())
		if err != nil {
			utils.SSCLogger().Error().Err(err).Msg("failed to get stateDB")
			return nil, err
		}
		callState.DB = stateAt
		s.stateLock.Lock()
		state, err = s.getState(txHash)
		if err != nil {
			utils.SSCLogger().Error().Err(err).Msg("failed to get state")
			s.stateLock.Unlock()
			return nil, err
		}
		if state.SimulationCallStates[req.SimulationNum] != nil {
			state.SimulationCallStates[req.SimulationNum] = state.SimulationCallStates[req.SimulationNum].Add(callState)
		}
		s.stateLock.Unlock()
		s.pendingLock.Lock()
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Str("callIndex", req.CallIndex.ToString()).Msgf("add callState, simulationNum=%d, callIndex=%s", state.SimulationNum, req.CallIndex.ToString())
		pendingList := s.pendingRequests[txHash]
		s.pendingRequests[txHash] = make([]*pendingCXTRequest, 0)
		if len(pendingList) > 0 {
			// 异步处理，避免阻塞当前 goroutine 和锁
			for _, p := range pendingList {
				if p.req.CallIndex[:len(p.req.CallIndex)-1].ToString() == callState.CallIndex.ToString() {
					utils.SSCLogger().Info().
						Str("txHash", txHash.Hex()).
						Str("callIndex", req.CallIndex.ToString()).
						Msg("processing pending CXT request")
					go func(req *api.CXTCallRequest, ch chan *api.CXTCallSSCResult) {
						result := s.RequestCallCXT(req) // 递归调用，此时状态已存在
						utils.SSCLogger().Info().
							Str("txHash", txHash.Hex()).
							Str("callIndex", req.CallIndex.ToString()).
							Msg("finished processing pending CXT request")
						ch <- result
					}(p.req, p.waitingCh)
				} else {
					s.pendingRequests[txHash] = append(s.pendingRequests[txHash], p)
				}
			}
		}
		s.pendingLock.Unlock()
	}
	return callState, nil
}

func (s *sscService) endCall(req *api.CXTCallSSCRequest) {
	txHash := req.TxHash
	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	state, err := s.getState(txHash)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).Msg("failed to get state during endCall")
		return
	}
	// CallStack.Pop() 可能返回 nil（栈为空时）
	state.CallStack.Pop()
	if frame := state.CallStack.Top(); frame != nil {
		state.CurrentCallFrame = frame
	} else {
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msgf("callStack.Pop() returned nil, execution successfully, simulationNum=%d, callIndex=%s, popNum=%d, poshNum=%d", state.SimulationNum, req.CallIndex.ToString(), state.CallStack.PopNum, state.CallStack.PushNum)
	}
}

func (s *sscService) waitForSync(txHash common.Hash, callState *api.SimulationCallState) {
	callState.SyncedCh = make(chan struct{})

	func() {
		s.syncLock.Lock()
		defer s.syncLock.Unlock()
		callStates, exists := s.callStatesInWaiting[callState.BlockHash]
		if !exists {
			callStates = make([]*api.SimulationCallState, 0)
		}
		s.callStatesInWaiting[callState.BlockHash] = append(callStates, callState)
	}()

	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
		Str("callIndex", callState.CallIndex.ToString()).
		Str("blockHash", callState.BlockHash.Hex()).
		Uint64("blockNum", callState.BlockNum).
		Msgf("callState is waiting for sync")

	select {
	case <-callState.SyncedCh:
		utils.SSCLogger().Debug().Msgf("callState synced %s", callState.CallIndex.ToString())
	case <-s.ctx.Done():
		utils.SSCLogger().Debug().Msg("sscService context done")
	}
}

func (s *sscService) SignSimulationCommit(commit *api.SimulationCommit) []byte {
	signature, err := s.BLSSignerMgr.GetSSCSigner().Sign(commit)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Str("txHash", commit.TxHash.Hex()).Msgf("failed to sign simulation commit, txHash: %s", commit.TxHash.Hex())
		return nil
	}
	return signature
}

func (s *sscService) SignCXTSimulation(simulation *api.CXTSimulation) []byte {
	txHash := simulation.TxHash
	// stop pool timer
	s.timerMgr.removePoolTx(txHash)
	s.stateLock.Lock()
	if state, err := s.getState(txHash); err == nil {
		state.RelatedShards = simulation.RelatedShards
	}
	s.stateLock.Unlock()
	signature, err := s.BLSSignerMgr.GetSSCSigner().Sign(simulation)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Str("txHash", simulation.TxHash.Hex()).Msgf("failed to sign simulation commit, txHash: %s", simulation.TxHash.Hex())
		return nil
	}
	return signature
}

// HandleCommitVote
//
//	@Description: handle the vote from self or other shard's ssc member
//	1. only accept the vote from other shard if vote is rollback for invalidSimulation
//	2. waiting for threshold votes, threshold is half of the committee members if vote is recall, otherwise threshold is half of all member in shard
//	3. if threshold votes are received, send the vote to origin-shard's ssc leader
func (s *sscService) HandleCommitVote(vote *api.CXTCommitVote) {
	// 1
	var threshold int
	sscOrValidator := true
	if vote.Type == api.Recall {
		threshold = s.GetCommittee(vote.Epochs[vote.ShardId], vote.ShardId).Threshold
		sscOrValidator = true
	} else {
		threshold = s.GetCommittee(vote.Epochs[vote.ShardId], vote.ShardId).ValidatorThreshold
		sscOrValidator = false
	}
	if vote.ShardId != s.SelfShard && vote.Type == api.Rollback && vote.Reason == api.ReasonInvalidSimulation {
		utils.SSCLogger().Warn().Str("txHash", vote.TxHash.Hex()).
			Msgf("received invalid simulation rollback vote from shard %d, ignore", vote.OriginShardId)
		return
	}
	// 2
	reachThreshold := false
	var sscVote *api.CXTCommitSSCVote
	txHash := vote.TxHash
	s.stateLock.Lock()
	if state, err := s.getState(txHash); err == nil {
		state.Status = api.BUILDING_COMMIT_PROOF
	}
	s.stateLock.Unlock()

	func() {
		s.commitLock.Lock()
		defer s.commitLock.Unlock()
		if s.commitStates[txHash] == nil {
			s.commitStates[txHash] = &api.CommitState{
				CommitVotes:     make(map[int]map[uint32][]*api.CXTCommitVote),
				CommitSSCVotes:  make(map[int]map[uint32]*api.CXTCommitSSCVote),
				RollbackVotes:   make(map[uint32][]*api.CXTCommitVote),
				RollbackSSCVote: nil,
			}
		}
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msgf("handle commit vote, type=%s", vote.Type)
		if vote.Type == api.Commit {
			if s.commitStates[txHash].CommitVotes[vote.SimulationNum] == nil {
				s.commitStates[txHash].CommitVotes[vote.SimulationNum] = make(map[uint32][]*api.CXTCommitVote)
			}
			if s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId] == nil {
				s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId] = make([]*api.CXTCommitVote, 0)
			}
			if len(s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId]) >= threshold {
				utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msgf("already received enough votes from shard %d, ignore", vote.ShardId)
				return
			}
			s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId] = append(s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId], vote)
			utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msgf("received %s vote from shard %d, [%d/%d]",
				vote.Type.String(), vote.OriginShardId, len(s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId]), threshold)
			reachThreshold = len(s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId]) == threshold
			if reachThreshold {
				utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msgf("reach threshold for commit votes from shard %d, begin to aggregate ssc commit vote", vote.ShardId)
				sscVote = s.aggregateSSCCommitVote(s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId], sscOrValidator)
			}
		}
		if vote.Type == api.Rollback {
			utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msgf("handle rollback vote")
			if s.commitStates[txHash].RollbackVotes[vote.ShardId] == nil {
				s.commitStates[txHash].RollbackVotes[vote.ShardId] = make([]*api.CXTCommitVote, 0)
			}
			if len(s.commitStates[txHash].RollbackVotes[vote.ShardId]) >= threshold {
				utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msgf("already received enough rollback votes from shard %d, ignore", vote.ShardId)
				return
			}
			s.commitStates[txHash].RollbackVotes[vote.ShardId] = append(s.commitStates[txHash].RollbackVotes[vote.ShardId], vote)
			utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msgf("received %s vote from shard %d, [%d/%d]",
				vote.Type.String(), vote.OriginShardId, len(s.commitStates[txHash].RollbackVotes[vote.ShardId]), threshold)
			reachThreshold = len(s.commitStates[txHash].RollbackVotes[vote.ShardId]) == threshold
			if reachThreshold {
				sscVote = s.aggregateSSCCommitVote(s.commitStates[txHash].RollbackVotes[vote.ShardId], sscOrValidator)
			}
		}
	}()

	if reachThreshold {
		if sscVote == nil {
			utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
				Msg("aggregated ssc vote is nil, skip sending")
			return
		}
		// 3
		leader := s.GetLeader(vote.Epochs[vote.OriginShardId], vote.OriginShardId)

		ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
		defer cancel()

		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msgf("send SSC commit vote to leader: %s, shard=%d, originShard=%d, type=%s", leader.Endpoint, vote.ShardId, vote.OriginShardId, vote.Type.String())
		err := s.Comm.Call(ctx, nil, leader, api.Method_HandleCXTCommitSSCVote, sscVote)
		if err != nil {
			utils.SSCLogger().Error().Err(err).Msg("failed to send commit vote")
			return
		}
	}
}

func (s *sscService) aggregateSSCCommitVote(votes []*api.CXTCommitVote, sscOrValidator bool) *api.CXTCommitSSCVote {
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
		aggregatedSig, bitMap, err = s.BLSSignerMgr.GetSSCSigner().Aggregate(msgs)
	} else {
		aggregatedSig, bitMap, err = s.BLSSignerMgr.GetValidatorSigner().Aggregate(msgs)
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
			ShardId:    s.SelfShard,
			Signatures: aggregatedSig,
			BLSBitMap:  bitMap,
			Epochs:     result.Epochs,
		},
	}
	return sscResult
}

// HandleCXTRecallProof
//
//	@Description: handle the recall proof from other shard's ssc member
//	1. start new simulation number
//	2. get the smallest callIndex as lockedCallIndex
func (s *sscService) HandleCXTRecallProof(proof *api.CXTCommitProof) {
	// 1
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	txHash := proof.TxHash
	simuState, err := s.getState(txHash)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", proof.TxHash.Hex()).Err(err).Msg("handle cxt recall proof failed")
		return
	}
	simuState.SimulationNum = proof.SimulationNum + 1
	if simuState.SimulationCallStates[simuState.SimulationNum] == nil {
		simuState.SimulationCallStates[simuState.SimulationNum] = make(api.SimulationCallStates, 0)
	}

	// 2
	conflictCallIndexes := make([]api.CallIndex, 0)
	for _, vote := range proof.Votes {
		if vote.Type == api.Recall {
			payload := &api.CXTConflictRWSetPayload{}
			_ = json.Unmarshal(vote.Payload, payload)
			conflictCallIndexes = append(conflictCallIndexes, payload.ConflictCallIndex)
		}
	}
	sort.Slice(conflictCallIndexes, func(i, j int) bool {
		return conflictCallIndexes[i].Compare(conflictCallIndexes[j]) < 0
	})
	simuState.LockedCallIndex = conflictCallIndexes[0]
}

func (s *sscService) HandleCXTSSCCall(req *api.CXTCallSSCRequest) *api.CXTCallSSCResult {
	if req == nil {
		utils.SSCLogger().Error().Msg("nil request")
		return &api.CXTCallSSCResult{
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{}, Err: "nil request"}
	}
	err := s.BLSSignerMgr.GetSSCSigner().Verify(req)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", req.TxHash.String()).Err(err).Interface("req", req).Msg("invalid cxt ssc call signature")
		return &api.CXTCallSSCResult{Err: err.Error(), BaseBLSSignedMessage: api.BaseBLSSignedMessage{Epochs: req.Epochs}}
	}

	// select the consistent state
	startTime := time.Now()
	header := s.bc.CurrentHeader()
	req.BlockHash = header.Hash()
	req.BlockNum = header.NumberU64()
	utils.SSCLogger().Info().Str("txHash", req.TxHash.String()).Str("callIndex", req.CallIndex.ToString()).Msg("handle cxt ssc call, start")
	committee := s.GetCommittee(req.Epochs[s.SelfShard], s.SelfShard)
	t := committee.Threshold
	n := committee.Number
	txHash := req.TxHash
	_, err = s.startCall(req)
	if err != nil {
		return &api.CXTCallSSCResult{
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{
				Epochs: req.Epochs,
			}, Err: fmt.Sprintf("error from targetShard %d, err=%s", req.TargetShardId, err.Error())}
	}
	s.stateLock.Lock()
	state, err := s.getState(txHash)
	s.stateLock.Unlock()
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).Msg("failed to get state")
		return nil
	}

	ctx, cancel := context.WithTimeout(state.Ctx, s.Config.CallTimeout)
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
		go func(index int, m *api.Member) {
			defer wg.Done()
			ret := new(api.CXTCallResult)
			callErr := s.Comm.Call(ctx, ret, member, api.Method_HandleCXTCall, req)
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

			if bytes.Compare(m.Address.Bytes(), s.SelfAddr.Bytes()) == 0 {
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
					results = append(results, ret)
					addrMap[m.Address] = true
				}
			}

			// 检查是否满足终止条件
			if hasSelf && len(results) == t {
				cancel()
			}
		}(i, member)
	}
	wg.Wait()

	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Int("n", n).Int("t", t).Int("results", len(results)).Int("addrs", len(addrMap)).Interface("addrMap", addrMap).Msg("receive sigs")

	if len(results) < t {
		if len(results) == 0 {
			utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).Msgf("handle cxt ssc call failed, error from targetShard %d", req.TargetShardId)
			return &api.CXTCallSSCResult{
				TxHash:               txHash,
				RelatedShards:        state.RelatedShards.Merge(req.RelatedShards),
				Err:                  fmt.Sprintf("handle ssc call failed, error from targetShard %d", req.TargetShardId),
				BaseBLSSignedMessage: api.BaseBLSSignedMessage{Epochs: req.Epochs, ShardId: req.ShardId},
			}
		}
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).Msgf("handle cxt ssc call failed, no enough ret: [%d, %d]", len(results), t)
		return &api.CXTCallSSCResult{
			TxHash:               txHash,
			RelatedShards:        state.RelatedShards.Merge(req.RelatedShards),
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
		utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(errors.New(leaderRet.Err)).Msg("failed to simulate transaction")
	} else {
		sscResult, err = s.aggregateCXSSCCallResult(results)
		if err != nil {
			utils.SSCLogger().Error().
				Str("txHash", txHash.String()).Err(err).Msg("failed to aggregate cxt call results")
			result1 := results[1].(*api.CXTCallResult)
			if strings.Contains(result1.Err, "locked by") {
				utils.SSCLogger().Error().
					Str("txHash", txHash.String()).
					Str("otherErr", result1.Err).
					Msg("leader is not locked, but other member found key is locked by other tx")
			}
			return &api.CXTCallSSCResult{
				TxHash:               txHash,
				RelatedShards:        leaderRet.RelatedShards,
				Err:                  fmt.Sprintf("error from targetShard %d, err=%s", req.TargetShardId, err.Error()),
				BaseBLSSignedMessage: api.BaseBLSSignedMessage{Epochs: req.Epochs, ShardId: req.ShardId},
			}
		}
	}

	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	state, err = s.getState(txHash)
	if err != nil {
		return &api.CXTCallSSCResult{
			TxHash:               txHash,
			RelatedShards:        leaderRet.RelatedShards.Merge(req.RelatedShards),
			Err:                  fmt.Sprintf("error from targetShard %d, err=%s", req.TargetShardId, err.Error()),
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{Epochs: req.Epochs, ShardId: req.ShardId},
		}
	}
	if state.SimulationCallStates[req.SimulationNum] == nil {
		return &api.CXTCallSSCResult{
			TxHash:               txHash,
			RelatedShards:        leaderRet.RelatedShards.Merge(req.RelatedShards),
			Err:                  fmt.Sprintf("no related simulation num"),
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{Epochs: req.Epochs, ShardId: req.ShardId},
		}
	}
	callStates := state.SimulationCallStates[req.SimulationNum]
	if callStates.Get(req.CallIndex) == nil {
		return &api.CXTCallSSCResult{
			TxHash:               txHash,
			RelatedShards:        leaderRet.RelatedShards.Merge(req.RelatedShards),
			Err:                  fmt.Sprintf("no related call index"),
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{Epochs: req.Epochs, ShardId: req.ShardId},
		}
	}
	callState := callStates.Get(req.CallIndex)
	if callState == nil {
		return &api.CXTCallSSCResult{
			TxHash:               txHash,
			RelatedShards:        leaderRet.RelatedShards.Merge(req.RelatedShards),
			Err:                  fmt.Sprintf("no related call index"),
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{Epochs: req.Epochs, ShardId: req.ShardId},
		}
	}
	callState.CallSSCResult = sscResult
	utils.SSCLogger().Info().Str("txHash", req.TxHash.String()).Dur("cost", time.Since(startTime)).Msg("handle cxt ssc call, end")
	return sscResult
}

func (s *sscService) aggregateCXSSCCallResult(results []api.SSCMessage) (*api.CXTCallSSCResult, error) {
	if len(results) == 0 {
		utils.SSCLogger().Error().Msg("no cxt call results to aggregate")
		return nil, errors.New("no cxt call results to aggregate")
	}
	result := results[0].(*api.CXTCallResult)
	aggregatedSig, bitMap, err := s.BLSSignerMgr.GetSSCSigner().Aggregate(results)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to aggregate cxt call results signatures")
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
			ShardId:    s.SelfShard,
			Signatures: aggregatedSig,
			BLSBitMap:  bitMap,
			Epochs:     result.Epochs,
		},
	}
	return sscResult, nil
}

func (s *sscService) CommitSimulation(commit *api.SimulationCommit) {
	txHash := commit.TxHash
	switch commit.Status {
	case api.OK:
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msgf("simulation committed, status=%s", commit.Status.String())
	case api.LockConflict:
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msgf("simulation failed, status=%s", commit.Status.String())
		return
	case api.ExecutionFailed:
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msgf("simulation failed, status=%s, ", commit.Status.String())
		s.closeTransaction(txHash, false, api.ExecutionFailed.String())
		return
	case api.PoolTimeout: // TODO deprecated, just close transaction if pool timeout
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msgf("simulation is pool timeout, status=%s", commit.Status.String())
		s.closeTransaction(txHash, false, api.PoolTimeout.String())
		return
	}

	s.stateLock.Lock()
	simuState, err := s.getState(txHash)
	if err != nil {
		s.stateLock.Unlock()
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).Msg("commit simulation failed")
		return
	}
	// update related shards
	oldRelatedShards := simuState.RelatedShards
	simuState.RelatedShards = simuState.RelatedShards.Merge(commit.RelatedShards)
	utils.SSCLogger().Debug().Str("txHash", txHash.String()).
		Str("result", fmt.Sprintf("%v", simuState.RelatedShards)).
		Str("old", fmt.Sprintf("%v", oldRelatedShards)).
		Str("new", fmt.Sprintf("%v", commit.RelatedShards)).
		Msg("update related shards")
	simulationCallStates := simuState.SimulationCallStates[commit.SimulationNum]
	s.stateLock.Unlock()

	// build commit simulation
	callStates := make([]*api.CXTCallState, 0)
	for i, simulationCallState := range simulationCallStates {
		depentResults := make([]*api.CXTCallSSCResult, 0)
		for _, dc := range simulationCallState.DependentCXTCalls {
			depentResults = append(depentResults, dc.SSCResult)
			if dc.SSCResult == nil {
				utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msgf("dependent callIndex %s has no result", dc.CallIndex.ToString())
				s.closeTransaction(txHash, false, fmt.Sprintf("no result for dependent callIndex %s", dc.CallIndex.ToString()))
				return
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
			utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msgf("callIndex %s has no result", simulationCallState.CallIndex.ToString())
			s.closeTransaction(txHash, false, fmt.Sprintf("no result for callIndex %s", simulationCallState.CallIndex.ToString()))
			return
		}
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("build commit simulation [%d/%d], callIndex: %s, rwset: %v", i+1, len(simulationCallStates), simulationCallState.CallIndex.ToString(), simulationCallState.RWSet.WriteState.State)
	}

	simulation := &api.CXTSimulation{
		SimulationNum: commit.SimulationNum,
		TxHash:        commit.TxHash,
		Nonce:         commit.Nonce,
		Sender:        commit.Sender,
		ShardId:       commit.ShardId,
		OriginShardId: simuState.OriginShardId,
		RelatedShards: commit.RelatedShards,
		CallStates:    callStates,
		BaseBLSSignedMessage: api.BaseBLSSignedMessage{
			Epochs: commit.Epochs,
		},
	}

	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msgf("build commit simulation completed")

	s.buildSignaturesForSimulation(simuState, simulation)

	// NEW: 如果 commit.UseCRSigner=true, 用 CR signer 的 nonce 提交
	// 确保 CR tx(crN) → SimTx(crN+1) 在同一个区块按序执行
	if commit.UseCRSigner {
		err = s.txSubmitter.SubmitSimulationTxWithSigner(simulation, "CommitOrRollbackTx")
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
			Msg("commit simulation: submitted with CR signer (hot key chain)")
	} else {
		err = s.txSubmitter.SubmitSimulationTx(simulation)
	}
	s.recordTraceBlock(txHash, StageSimulationTxSubmit, s.bc.CurrentHeader().NumberU64())
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to submit simulation tx")
		return
	}

	// SimTx 提交成功后，检查是否可以链式触发依赖它的 retry tx（DAG chaining）
	if s.IsLeader(commit.Epochs[s.SelfShard]) {
		// 从 simulationCallStates 中提取完整 WriteSet 用于 chainNextSim
		writeSet := &api.RWSet{WriteState: api.NewStateSet()}
		for _, callState := range simulationCallStates {
			if callState.RWSet != nil && callState.RWSet.WriteState != nil {
				for addr, state := range callState.RWSet.WriteState.State {
					if writeSet.WriteState.State[addr] == nil {
						writeSet.WriteState.State[addr] = make(map[common.Hash]common.Hash)
					}
					for key, val := range state {
						writeSet.WriteState.State[addr][key] = val
					}
				}
			}
		}
		s.retryScheduler.chainNextSim(txHash, writeSet)
	}

	// OLD: CR tx 提交后链式触发（已迁移到 SimTx 提交后）
	// 现在 chain 在 SimTx 提交时触发，不再等 CR 提交
	/*
		// NEW: CR tx 提交成功后，检查是否可以链式触发等待中的 retry tx（hot key chain）
		// 无论是普通 CR 还是链式 CR 都触发——因为 CR 解锁了 key，retry pool 里的 tx 可能等这些 key
		if s.IsLeader(commit.Epochs[s.SelfShard]) {
			// 从 simulationCallStates 中提取完整 WriteSet 用于 chainHotKeyCR
			writeSet := &api.RWSet{WriteState: api.NewStateSet()}
			for _, callState := range simulationCallStates {
				if callState.RWSet != nil && callState.RWSet.WriteState != nil {
					for addr, state := range callState.RWSet.WriteState.State {
						if writeSet.WriteState.State[addr] == nil {
							writeSet.WriteState.State[addr] = make(map[common.Hash]common.Hash)
						}
						for key, val := range state {
							writeSet.WriteState.State[addr][key] = val
						}
					}
				}
			}
			s.retryScheduler.chainHotKeyCR(txHash, writeSet)
		}
	*/

	s.stats.setCxtStage(txHash, 2)
	s.stateLock.Lock()
	simuState, err = s.getState(txHash)
	if err != nil {
		s.stateLock.Unlock()
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).Msg("commit simulation failed")
		return
	}
	simuState.Status = api.SIMULATION_COMMMITTING
	s.stateLock.Unlock()
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msgf("simulation committed")
}

func (s *sscService) buildSignaturesForSimulation(state *api.CXTSimulationState, simulation *api.CXTSimulation) {
	txHash := simulation.TxHash
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("begin to build signatures for simulation")
	committee := s.GetCommittee(simulation.Epochs[s.SelfShard], s.SelfShard)
	n := committee.Number
	t := committee.Threshold

	ctx, cancel := context.WithTimeout(state.Ctx, s.Config.CallTimeout)
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
			err := s.Comm.Call(ctx, &signature, m, api.Method_SignCXTSimulation, simulation)
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

			if bytes.Compare(m.Address.Bytes(), s.SelfAddr.Bytes()) == 0 {
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

	aggregatedSig, bitMap, err := s.BLSSignerMgr.GetSSCSigner().Aggregate(results)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).Msg("aggregate signatures failed")
		return
	}
	simulation.Signatures = aggregatedSig
	simulation.BLSBitMap = bitMap
}

// HandleCXTCommitSSCVote
// @Description: handle the vote from other shard's ssc leader
// 1. collect all the related shards for cross-shard transaction
// 2. if one vote is rollback, send rollback commit proof to all shards
// 3. if one vote is recall, broadcast recall commit proof and begin to recall the simulation
// 4. if all votes are commit, send commit proof to all shards
func (s *sscService) HandleCXTCommitSSCVote(vote *api.CXTCommitSSCVote) {
	if vote == nil {
		utils.SSCLogger().Error().Msg("HandleCXTCommitSSCVote called with nil vote")
		return
	}
	txHash := vote.TxHash
	s.stateLock.RLock()
	state, err := s.getState(txHash)
	if err != nil {
		s.stateLock.RUnlock()
		if errors.Is(err, api.ErrTxHasBeenClosed) {
			utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msg("handle commit ssc vote: tx already closed, drop vote")
		} else {
			utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).Msg("handle commit ssc vote failed: state not found")
		}
		return
	}
	relatedShards := state.RelatedShards
	s.stateLock.RUnlock()

	if !relatedShards.Contains(vote.ShardId) {
		utils.SSCLogger().Error().Msgf("shard %d is not related to tx %s", vote.ShardId, txHash.String())
		return
	}
	var sscVotes map[uint32]*api.CXTCommitSSCVote

	func() {
		s.commitLock.Lock()
		defer s.commitLock.Unlock()
		commitState := s.commitStates[txHash]
		if vote.Type == api.Commit {
			if commitState.CommitSSCVotes[vote.SimulationNum] == nil {
				commitState.CommitSSCVotes[vote.SimulationNum] = make(map[uint32]*api.CXTCommitSSCVote)
			}
			commitState.CommitSSCVotes[vote.SimulationNum][vote.ShardId] = vote
			sscVotes = commitState.CommitSSCVotes[vote.SimulationNum]
		}
		if vote.Type == api.Rollback {
			commitState.RollbackSSCVote = vote
			sscVotes = map[uint32]*api.CXTCommitSSCVote{
				vote.ShardId: vote,
			}
		}
	}()

	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msgf("received %s ssc vote [%d/%d], %d in %v, simulationNum=%d",
		vote.Type, len(sscVotes), len(relatedShards), vote.ShardId, relatedShards, vote.SimulationNum)

	if vote.Type == api.Rollback || len(sscVotes) == len(relatedShards) {
		commitType := api.Commit
		commitReason := api.ReasonSuccess
		sscVoteList := make([]*api.CXTCommitSSCVote, 0)
		recallShards := make([]uint32, 0)
		for _, v := range sscVotes {
			if v.Type == api.Rollback {
				commitType = api.Rollback
				commitReason = v.Reason
				sscVoteList = append(make([]*api.CXTCommitSSCVote, 0), v)
				break
			}
			if v.Type == api.Recall && commitType != api.Rollback {
				commitType = api.Recall
				commitReason = v.Reason
				recallShards = append(recallShards, v.ShardId)
			}
			sscVoteList = append(sscVoteList, v)
		}
		sort.Slice(sscVoteList, func(i, j int) bool {
			return sscVoteList[i].ShardId < sscVoteList[j].ShardId
		})

		ctx, cancel := context.WithTimeout(state.Ctx, s.Config.CallTimeout)
		defer cancel()
		members := make([]*api.Member, 0, len(relatedShards))
		for _, shardId := range relatedShards {
			members = append(members, s.GetLeader(vote.Epochs[shardId], shardId))
		}

		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msgf("receive all related ssc votes, commit type: %v, reason: %v, simulationNum: %d, relatedShards: %v",
			commitType, commitReason, vote.SimulationNum, relatedShards)

		switch commitType {
		case api.Commit:
			proof := &api.CXTCommitProof{
				TxHash:        txHash,
				SimulationNum: vote.SimulationNum,
				Type:          commitType,
				Reason:        commitReason,
				Votes:         sscVoteList,
				OriginShard:   vote.OriginShardId,
				RelatedShards: relatedShards,
				BaseSSCMessage: api.BaseSSCMessage{
					Epochs: vote.Epochs,
				},
			}
			_ = s.Comm.Multicast(ctx, members, api.Method_HandleCXTCommitProof, proof)
		case api.Rollback:
			proof := &api.CXTCommitProof{
				TxHash:        txHash,
				SimulationNum: vote.SimulationNum,
				Type:          commitType,
				Reason:        commitReason,
				Votes:         sscVoteList,
				OriginShard:   vote.OriginShardId,
				RelatedShards: relatedShards,
				BaseSSCMessage: api.BaseSSCMessage{
					Epochs: vote.Epochs,
				},
			}
			_ = s.Comm.Multicast(ctx, members, api.Method_HandleCXTCommitProof, proof)
		case api.Recall:
			proof := &api.CXTCommitProof{
				TxHash:        txHash,
				SimulationNum: vote.SimulationNum,
				Type:          commitType,
				Reason:        commitReason,
				Votes:         sscVoteList,
				OriginShard:   vote.OriginShardId,
				RelatedShards: relatedShards,
				BaseSSCMessage: api.BaseSSCMessage{
					Epochs: vote.Epochs,
				},
			}
			_ = s.Comm.Multicast(ctx, members, api.Method_HandleCXTCommitProof, proof)
			go func() {
				s.startReSimulation(proof.TxHash, proof.SimulationNum+1)
			}()
		default:
			utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msgf("unsupportted vote type: %s", vote.Type)
		}
	}
}

func (s *sscService) SignalReSimulation(signal *api.ReSimulationSignals) {
	s.retryScheduler.HandleReSimulationSignal(signal)
}

func (s *sscService) HandleChainSimSignal(signal *api.ReSimulationSignal) {
	s.retryScheduler.HandleChainSimSignal(signal)
}

func (s *sscService) startReSimulation(txHash common.Hash, simulationNum int) {
	s.stateLock.Lock()
	state, err := s.getState(txHash)
	s.stateLock.Unlock()
	if err != nil {
		utils.SSCLogger().Error().Err(err).Str("txHash", txHash.Hex()).Msg("resimulation failed")
		return
	}

	ctx, cancel := context.WithTimeout(state.Ctx, s.Config.CallTimeout)
	defer func() {
		cancel()
	}()

	header := s.bc.CurrentHeader()
	// request recall origin contract
	lastReq := state.SimulationRequest
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

	startTime := time.Now()
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msgf("recall simulation, simulationNum: %d, start", simulationNum)
	defer func() {
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msgf("recall simulation finished, simulationNum: %d, duration: %v, end", simulationNum, time.Since(startTime))
	}()

	committee := s.GetCommittee(req.Epochs[s.SelfShard], s.SelfShard)
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
			callErr := s.Comm.Call(ctx, ret, member, api.Method_HandleSimulateRequest, req)
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

			if bytes.Compare(m.Address.Bytes(), s.SelfAddr.Bytes()) == 0 {
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

	var (
		sscResult *api.CXTSimulationSSCResult
	)

	if len(results) == t {
		leaderRet := results[0].(*api.CXTSimulationResult)
		if leaderRet.Err != "" {
			sscResult = &api.CXTSimulationSSCResult{Err: leaderRet.Err, RelatedShards: []uint32{s.SelfShard},
				BaseBLSSignedMessage: api.BaseBLSSignedMessage{
					Epochs: leaderRet.Epochs,
				},
			}
			utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(errors.New(leaderRet.Err)).Msg("failed to simulate transaction")
		} else {
			sscResult, err = s.aggregateSimulationResults(results)
			if err != nil {
				sscResult = &api.CXTSimulationSSCResult{Err: err.Error(), RelatedShards: []uint32{s.SelfShard},
					BaseBLSSignedMessage: api.BaseBLSSignedMessage{
						Epochs: leaderRet.Epochs,
					},
				}
				utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(err).Msg("failed to aggregate simulation results")
			} else {
				s.stateLock.Lock()
				state.SimulationResult = sscResult
				if len(state.SimulationCallStates[simulationNum]) == 0 {
					sscResult = &api.CXTSimulationSSCResult{Err: "callStates size is 0", RelatedShards: []uint32{s.SelfShard},
						BaseBLSSignedMessage: api.BaseBLSSignedMessage{
							Epochs: leaderRet.Epochs,
						},
					}
				} else {
					state.SimulationCallStates[simulationNum][0].TopSSCResult = sscResult
				}
				s.stateLock.Unlock()
			}
		}
	} else {
		if len(results) == 0 {
			sscResult = &api.CXTSimulationSSCResult{Err: "no response from other shards", RelatedShards: []uint32{s.SelfShard},
				BaseBLSSignedMessage: api.BaseBLSSignedMessage{
					Epochs: req.Epochs,
				},
			}
		} else {
			errRet := results[len(results)-1].(*api.CXTSimulationResult)
			sscResult = &api.CXTSimulationSSCResult{Err: errRet.Err, RelatedShards: []uint32{s.SelfShard},
				BaseBLSSignedMessage: api.BaseBLSSignedMessage{
					Epochs: errRet.Epochs,
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

	// 检查是否有 ChainPatch — 如果有则应该用 CR signer 提交
	// 确保 SimTx 按正确的 nonce 顺序执行
	s.stateLock.RLock()
	if state, err := s.getState(txHash); err == nil && state != nil && state.ChainPatch != nil {
		simulationCommit.UseCRSigner = true
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
			Int("patchSize", len(state.ChainPatch.WriteState.State)).
			Msg("startReSimulation: ChainPatch detected, UseCRSigner=true")
	}
	s.stateLock.RUnlock()

	if len(sscResult.Err) == 0 {
		simulationCommit.Commit = true
		simulationCommit.Status = api.OK
		simulationCommit.Reason = api.OK.String()
		utils.SSCLogger().Debug().Str("txHash", simulationCommit.TxHash.Hex()).
			Msgf("resimulation accomplished, send simulation commit, commit type: %v, simulationNum: %d, relatedShards: %v",
				simulationCommit.Commit, simulationCommit.SimulationNum, simulationCommit.RelatedShards)
	} else {
		if vm.IsLockedByOtherTxErr(sscResult.Err) {
			utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("resimulation failed err=%s, it has subscribe to resimulate again, nextSimulationNum=%d", sscResult.Err, req.SimulationNum+1)
			simulationCommit.Commit = false
			simulationCommit.Status = api.LockConflict
			simulationCommit.Reason = sscResult.Err
			var originShardId uint32
			s.stateLock.Lock()
			if state, es := s.getState(txHash); es == nil {
				originShardId = state.OriginShardId
			}
			s.stateLock.Unlock()
			if s.IsLeader(req.Epochs[s.SelfShard]) {
				s.retryScheduler.CallForRetry(&api.RetryTx{
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

	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
		Bool("useCRSigner", simulationCommit.UseCRSigner).
		Msg("startReSimulation: calling thresholdSignSimulationCommit")
	s.thresholdSignSimulationCommit(simulationCommit)
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
		Msg("startReSimulation: after thresholdSignSimulationCommit")
	members := make([]*api.Member, 0, len(committee.Members))
	for _, shardId := range simulationCommit.RelatedShards {
		members = append(members, s.GetLeader(simulationCommit.Epochs[shardId], shardId))
	}
	_ = s.Comm.Multicast(ctx, members, api.Method_CommitSimulation, simulationCommit)
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
		Int("members", len(members)).
		Msg("startReSimulation: after Multicast")
}

func (s *sscService) HandleCXTCommitProof(proof *api.CXTCommitProof) {
	utils.SSCLogger().Debug().Str("txHash", proof.TxHash.Hex()).Msgf("handle commit or rollback proof, type: %v, simulationNum: %d", proof.Type, proof.SimulationNum)
	// if proof is commit or rollback, send commit proof transaction
	if proof.Type == api.Commit || proof.Type == api.Rollback {
		err := s.txSubmitter.SubmitCommitOrRollbackTx(proof)
		s.recordTraceBlock(proof.TxHash, StageCommitOrRollbackTxSubmit, s.bc.CurrentHeader().NumberU64())
		if err != nil {
			utils.SSCLogger().Error().Err(err).Msg("failed to submit commit or rollback tx")
			return
		}
		s.stats.setCxtStage(proof.TxHash, 5)
		if proof.TxHash[0] == 0 {
			s.txSubmitter.SubmitCommitOrRollbackTx(proof)
		}
		s.stateLock.Lock()
		state, err := s.getState(proof.TxHash)
		if err != nil {
			utils.SSCLogger().Error().Err(err).Msg("failed to get state")
			s.stateLock.Unlock()
			return
		}
		if proof.Type == api.Commit {
			state.Status = api.CXT_COMMITTING
		} else {
			state.Status = api.CXT_ROLLBACKING
		}
		s.stateLock.Unlock()
	}

	if proof.Type == api.Recall {
		committee := s.GetCommittee(proof.Epochs[s.SelfShard], s.SelfShard)
		ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
		defer cancel()
		_ = s.Comm.Multicast(ctx, committee.Members, api.Method_HandleCXTRecallProof, proof)
	}
}

func (s *sscService) getState(txHash common.Hash) (*api.CXTSimulationState, error) {
	if state := s.simulationState[txHash]; state != nil {
		return state, nil
	}
	if _, exists := s.finishedTxs[txHash]; exists {
		return nil, api.ErrTxHasBeenClosed
	}
	return nil, api.ErrTxNotExist
}

func (s *sscService) closeTransaction(txHash common.Hash, commitOrRollback bool, reason string) {
	s.stateLock.Lock()
	state, _ := s.getState(txHash)
	if state != nil {
		state.CtxCancel()
		if s.IsLeader(state.Epochs[s.SelfShard]) {
			utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Uint32("shardId", s.SelfShard).Interface("relatedShards", state.RelatedShards).Msgf("leader close transaction, commit: %v, status: %s, reason: %s", commitOrRollback, state.Status, reason)
		}
	}
	delete(s.simulationState, txHash)
	s.finishedTxs[txHash] = commitOrRollback
	s.stateLock.Unlock()
	s.commitLock.Lock()
	delete(s.commitStates, txHash)
	s.commitLock.Unlock()

	s.Verifier.Cleanup(txHash)

	// 清除定时器：无论提交还是回滚，不再需要 sp1/pool 定时器
	s.timerMgr.RemoveTx(txHash)

	s.retryScheduler.StaleTx(txHash)
}

func (s *sscService) closeTransactions(txs map[common.Hash]bool) {
	utils.SSCLogger().Info().Interface("txs", txs).Msgf("close transactions, num=%d", len(txs))
	s.stateLock.Lock()
	for txHash, commitOrRollback := range txs {
		state, _ := s.getState(txHash)
		if state != nil {
			state.CtxCancel()
		}
		delete(s.simulationState, txHash)
		s.finishedTxs[txHash] = commitOrRollback
	}
	s.stateLock.Unlock()
	s.commitLock.Lock()
	for txHash := range txs {
		delete(s.commitStates, txHash)
	}
	s.commitLock.Unlock()
	for txHash := range txs {
		s.Verifier.Cleanup(txHash)
		s.retryScheduler.StaleTx(txHash)
	}
}

func (s *sscService) StateLockManager() api.StateLockManager {
	return s.lockStateMgr
}

func (s *sscService) AddRetryTx(tx *api.RetryTx) {
	if !s.IsLeader(tx.Epochs[s.SelfShard]) {
		return
	}
	retryTx := &api.RetryTx{
		TxHash:        tx.TxHash,
		Epochs:        tx.Epochs,
		Sender:        tx.Sender,
		Nonce:         tx.Nonce,
		GasPrice:      tx.GasPrice,
		OriginShardID: tx.OriginShardID,
		ReadSet:       nil,
		WriteSet:      nil,
		RelatedShards: tx.RelatedShards,
		SimulationNum: tx.SimulationNum,
		Condition:     tx.Condition,
	}
	readSet := make([]api.LockKey, 0)
	writeSet := make([]api.LockKey, 0)

	s.stateLock.Lock()
	state, err := s.getState(tx.TxHash)
	if err != nil {
		s.stateLock.Unlock()
		utils.SSCLogger().Error().Str("txHash", tx.TxHash.Hex()).Err(err).Msg("failed to get state")
		return
	}
	for _, callState := range state.SimulationCallStates[tx.SimulationNum-1] {
		for addr, account := range callState.RWSet.ReadState.State {
			for key, _ := range account {
				readSet = append(readSet, api.FormKey(addr, key))
			}
		}
		for addr, account := range callState.RWSet.WriteState.State {
			for key, _ := range account {
				writeSet = append(writeSet, api.FormKey(addr, key))
			}
		}
	}
	retryTx.ReadSet = readSet
	retryTx.WriteSet = writeSet
	s.stateLock.Unlock()

	s.retryScheduler.AddToRetry(retryTx)
}

func (s *sscService) RetryCommit(txHash common.Hash) *api.RetryCommitResp {
	locked := s.retryScheduler.RetryCommit(txHash)
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msgf("retry commit, locked: %v", locked)
	return &api.RetryCommitResp{
		Locked: locked,
		TxHash: txHash,
	}
}

func (s *sscService) RetryCancel(txHash common.Hash) {
	s.retryScheduler.RetryCancel(txHash)
}
