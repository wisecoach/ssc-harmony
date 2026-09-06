package ssc

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/event"
	"github.com/harmony-one/harmony/core"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/harmony-one/harmony/ssc/api/proto"
	"github.com/harmony-one/harmony/ssc/perf"
	"google.golang.org/protobuf/proto"
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

// Len 返回当前队列长度（线程安全），供监控统计使用。
func (pq *simulationReqPriorityQueue) Len() int {
	pq.cond.L.Lock()
	defer pq.cond.L.Unlock()
	return pq.items.Len()
}

type sscService struct {
	*CommitteeMechanism
	*Verifier
	*Committer
	*Simulator
	Comm           *Comm
	BLSSignerMgr   api.BLSSignerMgr
	txSigner       api.TxSigner
	lockStateMgr   *stateLockManager
	tempLockView   *TempLockView
	timerMgr       *CXTTimerManager
	retryScheduler *retryScheduler

	Config *api.Config
	bc     core.BlockChain

	// internalPool DSN-48：最简内部交易池（由 main.go 注入，无条件使用）
	internalPool *SSCInternalPool

	txStates sync.Map // key: common.Hash, value: *api.TxState, 带 per-tx mu

	// 验证上下文 now managed by Verifier
	// verifyCtxLock           lm.RWMutex
	// executionVerifyContexts map[common.Hash]*api.ExecutionVerifyContext
	// txLockedSimNum          map[common.Hash]int   // moved to Verifier

	commitLock   sync.RWMutex // cmLock for commitStates
	commitStates map[common.Hash]*api.CommitState

	ctx          context.Context
	chainHeadCh  chan core.ChainHeadEvent
	chainHeadSub event.Subscription
	stats        *SimulationStats // experiment statistics

	// tx block trace: 记录各阶段块高度用于分析延迟
	txTraces  map[common.Hash]*TxBlockTrace
	traceLock sync.Mutex // 保护 txTraces，不借用 Simulator.simuLock

	// 当前块号缓存（BlockCommitted 时更新，避免关闭后调用 bc.CurrentHeader）
	currentBlockNum uint64
}

func (s *sscService) GetShardID(address common.Address) uint32 {
	return s.CommitteeMechanism.GetShardID(address)
}

// ExtractSSCTransactions 从内部池按类型顺序提取一批内部交易（DSN-48），供 worker 写入区块。
func (s *sscService) ExtractSSCTransactions(maxTotal int) [][]*types.SSCInternalTx {
	if s == nil || s.internalPool == nil {
		return nil
	}
	return s.internalPool.Extract(maxTotal)
}

// DiscardSSCInternalTx 从内部池删除一条执行失败的内部交易（DSN-48）。
// worker 出块时若 ApplySSCInternalTransaction 失败，调用此方法移除该 tx，
// 避免其每块被重复提取/重试。
func (s *sscService) DiscardSSCInternalTx(tx *types.SSCInternalTx) {
	if s == nil || s.internalPool == nil {
		return
	}
	s.internalPool.Remove(tx)
}

// GetModuleStatus 汇总 SSC 各模块当前维护的数据量快照。
// 供实验结束后通过监控 gRPC 服务调用，用于分析各模块是否正确释放资源。
func (s *sscService) GetModuleStatus() *api.ModuleStatus {
	status := &api.ModuleStatus{}
	if s == nil {
		return status
	}

	// sscService 顶层存储
	s.txStates.Range(func(_, _ interface{}) bool { status.TxStates++; return true })
	s.commitLock.RLock()
	status.CommitStates = len(s.commitStates)
	s.commitLock.RUnlock()
	s.traceLock.Lock()
	status.TxTraces = len(s.txTraces)
	s.traceLock.Unlock()
	if s.internalPool != nil {
		status.InternalPool = s.internalPool.Len()
	}

	// RetryScheduler
	if s.retryScheduler != nil {
		status.RetryScheduler = s.retryScheduler.Stats()
	}

	// 状态锁管理器（已有 Stats）
	if s.lockStateMgr != nil {
		status.StateLock.GlobalLocked, status.StateLock.GlobalRLocked,
			status.StateLock.GlobalFinishedTxs, status.StateLock.GlobalLockStart = s.lockStateMgr.Stats()
	}

	// 临时锁视图（已有 Stats）
	if s.tempLockView != nil {
		status.TempLockView.WriteLocks, status.TempLockView.ReadLocks,
			status.TempLockView.TxSets, status.TempLockView.Wounded = s.tempLockView.Stats()
	}

	// Simulator
	if s.Simulator != nil {
		status.Simulator.SimStates, status.Simulator.CallStatesWaiting,
			status.Simulator.SimuChMap, status.Simulator.PendingReqsMap,
			status.Simulator.QueueLen = s.Simulator.Stats()
	}

	// Verifier
	if s.Verifier != nil {
		status.Verifier.ExecutionVerifyContexts, status.Verifier.TxLockedSimNum = s.Verifier.Stats()
	}

	// 定时器管理器
	if s.timerMgr != nil {
		status.Timer.Txs, status.Timer.Sp1Buckets,
			status.Timer.PoolTimeoutBuckets = s.timerMgr.Stats()
	}

	// DAG 使用情况（累计值）
	status.DAG.ChainTxDetected = int(chainRetryStats.MonitorChainTxDetected.Load())
	status.DAG.RetryCommitPatchHit = int(chainRetryStats.MonitorRetryCommitPatchHit.Load())
	status.DAG.RetryCommitPatchMiss = int(chainRetryStats.MonitorRetryCommitPatchMiss.Load())
	status.DAG.ChainTxCRCommitted = int(chainRetryStats.MonitorChainTxCRCommitted.Load())
	status.DAG.DagPerBlockMaxSameKey = int(chainRetryStats.MonitorDagPerBlockMaxSameKey.Load())

	return status
}

// newBaseService 创建基础服务实例（内部使用）
func newBaseService(ctx context.Context, config *api.Config, cm *CommitteeMechanism, sscConfig *api.ShardSimulateCommitteeConfig,
	signerMgr api.BLSSignerMgr, bc core.BlockChain, txSigner api.TxSigner, comm *Comm) *sscService {
	// 设置 worker 数量和信号量上限
	numWorkers := config.SimulationLimit // worker 数量
	numWorkers = 32

	service := &sscService{
		CommitteeMechanism: cm,
		Comm:               comm,
		BLSSignerMgr:       signerMgr,
		txSigner:           txSigner,
		Config:             config,
		bc:                 bc,
		stats:              newSimulationStats(),
		commitLock:         sync.RWMutex{},
		commitStates:       make(map[common.Hash]*api.CommitState),
		ctx:                ctx,
		chainHeadCh:        make(chan core.ChainHeadEvent, 10),
		chainHeadSub:       nil,
		txTraces:           make(map[common.Hash]*TxBlockTrace),
	}
	// DSN-48：internalPool 由外部（main.go）创建后注入，保证与 txSubmitter 共享同一实例

	lockStateMgr := newStateLockManager(service)
	service.lockStateMgr = lockStateMgr
	service.tempLockView = lockStateMgr.GetTempLockView()
	service.timerMgr = NewTimerManager(sscConfig.Timeout, service)
	service.retryScheduler = newRetryScheduler(ctx, bc, &RetrySchedulerStateAccessor{
		IsLeader: func(epoch api.Epoch) bool { return service.CommitteeMechanism.IsLeader(epoch) },
		GetLeader: func(epoch api.Epoch, shardId uint32) *api.Member {
			return service.CommitteeMechanism.GetLeader(epoch, shardId)
		},
		GetCommittee: func(epoch api.Epoch, shardId uint32) *api.ShardSimulateCommittee {
			return service.CommitteeMechanism.GetCommittee(epoch, shardId)
		},
		GetBlockHash: func(txHash common.Hash) (common.Hash, bool) {
			return service.Simulator.GetBlockHash(txHash)
		},
		ShardNum:            func() uint32 { return service.CommitteeMechanism.ShardNum() },
		RetryAddCount:       func() { service.stats.RetryAddCount.Add(1) },
		SampleRetryPool:     func(size int) { service.stats.sampleRetryPool(size) },
		RetryReadySignal:    func() { service.stats.RetryReadySignal.Add(1) },
		RetryNotReadySignal: func() { service.stats.RetryNotReadySignal.Add(1) },
		RetrySuccessCount:   func() { service.stats.RetrySuccessCount.Add(1) },
		RetryFailCount:      func() { service.stats.RetryFailCount.Add(1) },
		GetSimState: func(txHash common.Hash) (*api.SimulationState, bool) {
			return service.Simulator.GetSimState(txHash)
		},
		TriggerReSimulation: func(txHash common.Hash, simulationNum int) {
			service.Simulator.StartReSimulation(txHash, simulationNum)
		},
		IsOnChain: func(txHash common.Hash) bool {
			return service.Verifier != nil && service.Verifier.HasOnChainSimTx(txHash)
		},
		// DSN-58 §4.3：某 related 分片是否已放好 simNum 的腿 = origin 已收到其 CommitSSCVote。
		LegSscVoted: func(txHash common.Hash, shard uint32, simNum int) bool {
			service.commitLock.RLock()
			defer service.commitLock.RUnlock()
			cs, ok := service.commitStates[txHash]
			if !ok || cs.CommitSSCVotes == nil {
				return false
			}
			m := cs.CommitSSCVotes[simNum]
			if m == nil {
				return false
			}
			return m[shard] != nil
		},
		CloseTransaction: func(txHash common.Hash, commitOrRollback bool, reason string) {
			service.closeTransaction(txHash, commitOrRollback, reason)
		},
		LockStatesStats: func() (int, int, int, int) {
			return service.lockStateMgr.Stats()
		},
		RollbackVictim: func(txHash common.Hash) {
			service.rollbackVictim(txHash)
		},
		SubmitVictimTx: func(victimTx *api.VictimTx) {
			if service.txSubmitter != nil {
				_ = service.txSubmitter.SubmitVictimTx(victimTx)
			}
		},
	}, comm, service.SelfShard, service.tempLockView, sscConfig.Timeout, signerMgr)

	// 创建 Simulator（自管理 SimulationState + callStatesInWaiting + worker pool）
	simComm := NewSimulatorCommunicator(comm, signerMgr, service.CommitteeMechanism, config, ctx)
	service.Simulator = NewSimulator(
		SimulatorStateAccessor{
			GetTxState: func(txHash common.Hash) (*api.TxState, error) {
				if val, ok := service.txStates.Load(txHash); ok {
					tx := val.(*api.TxState)
					tx.Mu.Lock()
					defer tx.Mu.Unlock()
					if tx.Closed {
						return nil, api.ErrTxHasBeenClosed
					}
					return tx, nil
				}
				return nil, api.ErrTxNotExist
			},
			SetTxStatus: func(txHash common.Hash, status api.CXTStatus) {
				if val, ok := service.txStates.Load(txHash); ok {
					tx := val.(*api.TxState)
					tx.Mu.Lock()
					tx.Status = status
					tx.Mu.Unlock()
				}
			},
			MergeRelatedShards: func(txHash common.Hash, newShards api.RelatedShards) api.RelatedShards {
				if val, ok := service.txStates.Load(txHash); ok {
					tx := val.(*api.TxState)
					tx.Mu.Lock()
					tx.RelatedShards = tx.RelatedShards.Merge(newShards)
					ret := tx.RelatedShards
					tx.Mu.Unlock()
					return ret
				}
				return newShards
			},
			SetSimulationNum: func(txHash common.Hash, num int) {
				if val, ok := service.txStates.Load(txHash); ok {
					tx := val.(*api.TxState)
					tx.Mu.Lock()
					tx.SimulationNum = num
					tx.Mu.Unlock()
				}
			},
			CreateTxState: func(txHash common.Hash, txState *api.TxState) {
				service.txStates.Store(txHash, txState)
			},
			InitCommitState: func(txHash common.Hash) *api.CommitState {
				service.commitLock.Lock()
				defer service.commitLock.Unlock()
				if service.commitStates[txHash] == nil {
					service.commitStates[txHash] = &api.CommitState{
						CommitVotes:     make(map[int]map[uint32][]*api.CXTCommitVote),
						CommitSSCVotes:  make(map[int]map[uint32]*api.CXTCommitSSCVote),
						RollbackVotes:   make(map[uint32][]*api.CXTCommitVote),
						RollbackSSCVote: nil,
					}
				}
				return service.commitStates[txHash]
			},
			GetCommitState: func(txHash common.Hash) *api.CommitState {
				return service.commitStates[txHash]
			},
			GetFinishedTxs: func() map[common.Hash]bool {
				return nil
			},
			CallForRetry: func(tx *api.RetryTx) {
				service.retryScheduler.CallForRetry(tx)
			},
		},
		service,
		simComm,
		service.timerMgr,
		service.CommitteeMechanism,
		config,
		bc,
		service.stats,
		ctx,
		numWorkers,
	)

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
			SetStatus: func(txHash common.Hash, status api.CXTStatus) {
				if val, ok := service.txStates.Load(txHash); ok {
					tx := val.(*api.TxState)
					tx.Mu.Lock()
					tx.Status = status
					tx.Mu.Unlock()
				}
			},
			IsTxFinished: func(txHash common.Hash) bool {
				if val, ok := service.txStates.Load(txHash); ok {
					tx := val.(*api.TxState)
					tx.Mu.Lock()
					closed := tx.Closed
					tx.Mu.Unlock()
					return closed
				}
				return false
			},
			SetWaitingForResimu: func(txHash common.Hash) {
				if val, ok := service.txStates.Load(txHash); ok {
					tx := val.(*api.TxState)
					tx.Mu.Lock()
					tx.Status = api.WAITING_FOR_RESIMULATING_ON_CHAIN
					tx.Mu.Unlock()
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
				if val, ok := service.txStates.Load(txHash); ok {
					tx := val.(*api.TxState)
					tx.Mu.Lock()
					closed := tx.Closed
					tx.Mu.Unlock()
					return closed
				}
				return false
			},
			SetStatus: func(txHash common.Hash, status api.CXTStatus) {
				if val, ok := service.txStates.Load(txHash); ok {
					tx := val.(*api.TxState)
					tx.Mu.Lock()
					tx.Status = status
					tx.Mu.Unlock()
				}
			},
			CloseTx: func(txHash common.Hash, success bool, reason string) {
				service.closeTransaction(txHash, success, reason)
			},
			RemoveOnChainDAGPatch: func(txHash common.Hash) {
				service.retryScheduler.RemoveOnChainDAGPatch(txHash)
			},
			RecordChainTxCRCommit: func(txHash common.Hash) {
				if service.retryScheduler == nil {
					return
				}
				// 只有被标记为链式救起(isChainTx)的交易，其最终 CR commit 才计入
				// “因 DAG 提前完成的交易数量”。
				if service.retryScheduler.isChainTxMarked(txHash) {
					chainRetryStats.SigChainTxCRCommitted.Add(1)
					chainRetryStats.MonitorChainTxCRCommitted.Add(1)
					service.retryScheduler.clearChainTxMark(txHash)
				}
			},
		},
	)
	// 给 Verifier / Committer 回填所属 sscService，用于补全 tx block trace 阶段埋点
	// （StageSimulationTxCommit / StageCommitOrRollback 需要写入 service.txTraces）
	service.Verifier.traceSvc = service
	service.Committer.traceSvc = service

	subscription := bc.SubscribeChainHeadEvent(service.chainHeadCh)
	service.chainHeadSub = subscription
	utils.SSCLogger().Info().Msg("ssc service start...")
	// 启动 worker pool
	service.Simulator.StartWorkers()
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
	t0 := time.Now()
	var tTimerMgr, tHandleBlock, tOnBlockCommitted time.Duration

	// 缓存当前块号，供 closeTransaction 等后续路径使用（避免 bc 关闭后取不到）
	s.currentBlockNum = block.NumberU64()
	defer func() {
		utils.SSCLogger().Debug().
			Uint64("blockNum", block.NumberU64()).
			Dur("timerMgr", tTimerMgr).
			Dur("handleBlock", tHandleBlock).
			Dur("onBlockCommitted", tOnBlockCommitted).
			Dur("total", time.Since(t0)).
			Msg("[BlockTiming] BlockCommitted breakdown")
	}()
	utils.SSCLogger().Debug().Uint64("blockNum", block.NumberU64()).Msg("block committed")
	s.timerMgr.OnBlockCommitted(block.NumberU64())
	tTimerMgr = time.Since(t0)
	err := s.CommitteeMechanism.OnBlockCommitted(block)
	tHandleBlock = time.Since(t0)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to handle block committed")
		return err
	}
	s.retryScheduler.OnBlockCommitted(block)
	tOnBlockCommitted = time.Since(t0)

	// 通知 txSubmitter：块已确认，释放 inFlight 计数
	s.txSubmitter.OnBlockCommitted(block)

	// DSN-48：区块提交后清理内部池中已进块的内部交易
	if s.internalPool != nil {
		s.internalPool.OnBlockCommitted(block)
	}

	// 所有模块的 Record() 已完成，flush + reset PerfAggregator
	perf.GetAggregator().OnBlockCommitted(block)

	return nil
}

func (s *sscService) loop() {
	statsTicker := time.NewTicker(30 * time.Second)
	defer statsTicker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			statsTicker.Stop()
			s.traceLock.Lock()
			s.stats.SetBlockTraces(s.txTraces)
			s.traceLock.Unlock()
			s.stats.Dump(s.lockStateMgr)
			utils.SSCLogger().Info().Msg("ssc service stop...")
			s.Simulator.StopWorkers()
			return
		case <-s.chainHeadSub.Err():
			utils.SSCLogger().Error().Msg("chain head subscription error")
			return
		case head := <-s.chainHeadCh:
			tBlockCh := time.Now()
			utils.SSCLogger().Debug().Uint64("blockNum", head.Block.NumberU64()).Msg("new block committed")
			b := head.Block
			callStatesInWaiting := s.Simulator.PopCallStatesInWaiting(b.Header().Hash())
			for _, callState := range callStatesInWaiting {
				var txHash common.Hash
				if callState.CallRequest != nil {
					txHash = callState.CallRequest.TxHash
				} else if callState.TopRequest != nil {
					txHash = callState.TopRequest.TxHash
				}
				utils.SSCLogger().Debug().
					Str("txHash", txHash.Hex()).
					Str("callIndex", callState.CallIndex.ToString()).
					Str("blockHash", b.Hash().Hex()).
					Uint64("blockNum", b.NumberU64()).
					Msg("call state synced")
				callState.SyncedCh <- struct{}{}
			}
			utils.SSCLogger().Debug().
				Uint64("blockNum", b.NumberU64()).
				Dur("callStateSync", time.Since(tBlockCh)).
				Msg("[BlockTiming] chainHeadCh handler")
		case <-statsTicker.C:
			s.stats.sampleQueueLen(s.stats.QueuePushCount.Load(), s.stats.QueuePopCount.Load())
			s.traceLock.Lock()
			s.stats.SetBlockTraces(s.txTraces)
			s.traceLock.Unlock()
			s.stats.Dump(s.lockStateMgr)
		}
	}
}

func (s *sscService) handleTxSp1Timeout(info txInfo) {
	utils.SSCLogger().Debug().Str("txHash", info.txHash.Hex()).Msgf("cxt has timeout for sp1, send rollback vote to ssc leader")
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
	if val, ok := s.txStates.Load(txHash); ok {
		tx := val.(*api.TxState)
		tx.Mu.Lock()
		exists := !tx.Closed
		tx.Mu.Unlock()
		if !exists {
			return
		}
	} else {
		return
	}
	if existsOnChain {
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
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

func (s *sscService) sendCXTCommitVote(shardId uint32, vote *api.CXTCommitVote) {
	signer := s.BLSSignerMgr.GetValidatorSigner()
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
	utils.SSCLogger().Debug().Str("txHash", vote.TxHash.Hex()).
		Str("type", vote.Type.String()).Str("reason", vote.Reason.String()).
		Msgf("send CXTCommitVote to %s", targetLeader.Endpoint)
	s.Comm.Call(ctx, nil, targetLeader, api.Method_HandleCommitVote, vote)
}

func (s *sscService) NewEpoch(newEpochBytes []byte, sscvm api.VM, stateDB api.StateDB, blockNum uint64) error {
	newEpochProto := &sscpb.NewEpoch{}
	err := proto.Unmarshal(newEpochBytes, newEpochProto)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to unmarshal new epoch")
		return err
	}
	newEpoch := sscpb.NewEpochFromProto(newEpochProto)

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
		utils.SSCLogger().Debug().Dur("cost", time.Since(startTime)).Msgf("leader broadcast new epoch to every validator, epoch=%d, blockNum=%d", newEpoch.Committee.Epoch, blockNum)
	}

	return nil
}

// safeCallRequest 封装 CXTCallRequest 与等待 channel，用于注册-等待模式
type safeCallRequest struct {
	req       *api.CXTCallRequest
	waitingCh chan *api.CXTCallSSCResult
}

// safeDependentCall 封装 DependentCXTCall 与锁，用于并发安全访问
type safeDependentCall struct {
	*api.DependentCXTCall
	lock sync.RWMutex
	// waitCh 已移至 DependentCXTCall.WaitCh，所有请求共享同一个 channel
}

func (s *sscService) SignSimulationCommit(commit *api.SimulationCommit) []byte {
	sign, err := s.signerMgr.GetSSCSigner().Sign(commit)
	if err != nil {
		return nil
	}
	return sign
}

func (s *sscService) SignCXTSimulation(simulation *api.CXTSimulation) []byte {
	txHash := simulation.TxHash
	// stop pool timer
	s.timerMgr.removePoolTx(txHash)
	if val, ok := s.txStates.Load(txHash); ok {
		tx := val.(*api.TxState)
		tx.Mu.Lock()
		tx.RelatedShards = simulation.RelatedShards
		tx.Mu.Unlock()
	}
	signature, err := s.BLSSignerMgr.GetSSCSigner().Sign(simulation)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Str("txHash", simulation.TxHash.Hex()).Msgf("failed to sign simulation commit, txHash: %s", simulation.TxHash.Hex())
		return nil
	}
	return signature
}

// SignDeadlockProbe 供同分片 SSC 成员对探针做 BLS 签名（阈值聚合前）。
func (s *sscService) SignDeadlockProbe(probe *api.DeadlockProbe) []byte {
	if s == nil || s.BLSSignerMgr == nil || probe == nil {
		return nil
	}
	sig, err := s.BLSSignerMgr.GetSSCSigner().Sign(probe)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Str("init", probe.Init.Hex()).
			Msg("failed to sign deadlock probe")
		return nil
	}
	return sig
}

// HandleCommitVote
//
//	@Description: handle the vote from self or other shard's ssc member
//	1. only accept the vote from other shard if vote is rollback for invalidSimulation
//	2. waiting for threshold votes, threshold is half of the committee members if vote is recall, otherwise threshold is half of all member in shard
//	3. if threshold votes are received, send the vote to origin-shard's ssc leader
func (s *sscService) HandleCommitVote(vote *api.CXTCommitVote) {
	// 1
	threshold := s.GetCommittee(vote.Epochs[vote.ShardId], vote.ShardId).ValidatorThreshold
	sscOrValidator := false
	if vote.ShardId != s.SelfShard && vote.Type == api.Rollback && vote.Reason == api.ReasonInvalidSimulation {
		utils.SSCLogger().Warn().Str("txHash", vote.TxHash.Hex()).
			Msgf("received invalid simulation rollback vote from shard %d, ignore", vote.OriginShardId)
		return
	}

	t0 := time.Now()
	var tVoteTracking, tAggregate, tSendVote time.Duration
	defer func() {
		utils.SSCLogger().Debug().Str("txHash", vote.TxHash.Hex()).
			Str("voteTracking", tVoteTracking.String()).
			Str("aggregate", tAggregate.String()).
			Str("sendVote", tSendVote.String()).
			Str("total", time.Since(t0).String()).
			Msg("HandleCommitVote timing breakdown")
	}()

	// 2
	reachThreshold := false
	var sscVote *api.CXTCommitSSCVote
	txHash := vote.TxHash
	if val, ok := s.txStates.Load(txHash); ok {
		tx := val.(*api.TxState)
		tx.Mu.Lock()
		tx.Status = api.BUILDING_COMMIT_PROOF
		tx.Mu.Unlock()
	}

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
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("handle commit vote, type=%s", vote.Type)
		if vote.Type == api.Commit {
			if s.commitStates[txHash].CommitVotes[vote.SimulationNum] == nil {
				s.commitStates[txHash].CommitVotes[vote.SimulationNum] = make(map[uint32][]*api.CXTCommitVote)
			}
			if s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId] == nil {
				s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId] = make([]*api.CXTCommitVote, 0)
			}
			if len(s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId]) >= threshold {
				utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("already received enough votes from shard %d, ignore", vote.ShardId)
				return
			}
			s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId] = append(s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId], vote)
			utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
				Uint32("voteShard", vote.ShardId).
				Uint32("originShard", vote.OriginShardId).
				Int("voteCount", len(s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId])).
				Int("threshold", threshold).
				Msg("[voteRecv] commit vote received")
			utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("received %s vote from shard %d, [%d/%d]",
				vote.Type.String(), vote.OriginShardId, len(s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId]), threshold)
			reachThreshold = len(s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId]) == threshold
			if reachThreshold {
				utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("reach threshold for commit votes from shard %d, begin to aggregate ssc commit vote", vote.ShardId)
				sscVote = s.aggregateSSCCommitVote(s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId], sscOrValidator)
				tAggregate = time.Since(t0)
			}
		}
		if vote.Type == api.Rollback {
			utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("handle rollback vote")
			if s.commitStates[txHash].RollbackVotes[vote.ShardId] == nil {
				s.commitStates[txHash].RollbackVotes[vote.ShardId] = make([]*api.CXTCommitVote, 0)
			}
			if len(s.commitStates[txHash].RollbackVotes[vote.ShardId]) >= threshold {
				utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("already received enough rollback votes from shard %d, ignore", vote.ShardId)
				return
			}
			s.commitStates[txHash].RollbackVotes[vote.ShardId] = append(s.commitStates[txHash].RollbackVotes[vote.ShardId], vote)
			utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("received %s vote from shard %d, [%d/%d]",
				vote.Type.String(), vote.OriginShardId, len(s.commitStates[txHash].RollbackVotes[vote.ShardId]), threshold)
			reachThreshold = len(s.commitStates[txHash].RollbackVotes[vote.ShardId]) == threshold
			if reachThreshold {
				sscVote = s.aggregateSSCCommitVote(s.commitStates[txHash].RollbackVotes[vote.ShardId], sscOrValidator)
				tAggregate = time.Since(t0)
			}
		}
	}()
	tVoteTracking = time.Since(t0)

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

		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("send SSC commit vote to leader: %s, shard=%d, originShard=%d, type=%s", leader.Endpoint, vote.ShardId, vote.OriginShardId, vote.Type.String())
		err := s.Comm.Call(ctx, nil, leader, api.Method_HandleCXTCommitSSCVote, sscVote)
		tSendVote = time.Since(t0)
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

// simOwnWriteSet 从 SimTx 的 CallStates 汇总其自身的 WriteSet（DSN-56：
// SimTx 不再携带扁平 ChainPatch，自身的写集只由 CallStates 推导）。
func simOwnWriteSet(sim *api.CXTSimulation) *api.RWSet {
	out := &api.RWSet{WriteState: api.NewStateSet()}
	for _, cs := range sim.CallStates {
		if cs == nil || cs.RWSet == nil || cs.RWSet.WriteState == nil {
			continue
		}
		for addr, state := range cs.RWSet.WriteState.State {
			if out.WriteState.State[addr] == nil {
				out.WriteState.State[addr] = make(map[common.Hash]common.Hash)
			}
			for key, val := range state {
				out.WriteState.State[addr][key] = val
			}
		}
	}
	return out
}

// CommitSimulation handles the simulation commit from leader, called when a SimTx is
// received by each shard leader
func (s *sscService) CommitSimulation(commit *api.SimulationCommit) {
	txHash := commit.TxHash
	// [commitSimStart] 各 shard leader 收到 SimTx 的时刻
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
		Uint32("shardId", s.SelfShard).
		Int("simNum", commit.SimulationNum).
		Uint64("blockNum", s.bc.CurrentHeader().NumberU64()).
		Msg("[commitSimStart] CommitSimulation called")
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Int("simNum", commit.SimulationNum).
		Bool("commit", commit.Commit).
		Msg("CommitSimulation: called")
	switch commit.Status {
	case api.OK:
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("simulation committed, status=%s", commit.Status.String())
	case api.LockConflict:
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msgf("simulation failed, status=%s", commit.Status.String())
		return
	case api.ExecutionFailed:
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msgf("simulation failed, status=%s, ", commit.Status.String())
		s.closeTransaction(txHash, false, api.ExecutionFailed.String())
		return
	case api.PoolTimeout:
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msgf("simulation is pool timeout, status=%s", commit.Status.String())
		s.closeTransaction(txHash, false, api.PoolTimeout.String())
		return
	}

	t0 := time.Now()
	var tBuildCallStates, tBuildSignatures, tOnChainDAGPatch, tSubmitTx time.Duration
	defer func() {
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Str("buildCallStates", tBuildCallStates.String()).
			Str("buildSignatures", tBuildSignatures.String()).
			Str("onChainDAGPatch", tOnChainDAGPatch.String()).
			Str("submitTx", tSubmitTx.String()).
			Str("total", time.Since(t0).String()).
			Msg("CommitSimulation timing breakdown")
	}()

	var tx *api.TxState
	// update related shards
	if val, ok := s.txStates.Load(txHash); ok {
		tx = val.(*api.TxState)
		tx.Mu.Lock()
		tx.RelatedShards = tx.RelatedShards.Merge(commit.RelatedShards)
		tx.Mu.Unlock()
	} else {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(api.ErrTxNotExist).Msg("commit simulation failed")
		return
	}

	// build commit simulation — 从 Simulator 获取 callStates
	callStates, err := s.Simulator.BuildCallStates(txHash, commit.SimulationNum)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).Msg("commit simulation failed: no call states")
		return
	}

	// 从 callStates 中提取 WriteSet，作为本 SimTx 的 own WriteSet（入 offChainDAG / onChainDAGPatches）
	writeSet := &api.RWSet{WriteState: api.NewStateSet()}
	for _, cs := range callStates {
		if cs.RWSet != nil && cs.RWSet.WriteState != nil {
			for addr, state := range cs.RWSet.WriteState.State {
				if writeSet.WriteState.State[addr] == nil {
					writeSet.WriteState.State[addr] = make(map[common.Hash]common.Hash)
				}
				for key, val := range state {
					writeSet.WriteState.State[addr][key] = val
				}
			}
		}
	}

	// 检查当前 SimTx 是否有上游依赖（链式重试）。
	// DSN-57 分片收敛：每个分片只带“本分片 offChainDAG 确有节点”的上游(LocalUpstreamTxRef)，
	// 避免把救援发起分片的异地上游带进本分片 SimTx（否则本分片 verify 见不到上游 → absent 误回滚）。
	// 本分片无本地上游 → 返回 nil → 本 SimTx 按非链处理（规则 B）。
	var upstreamTxList []api.TxSimKey
	if ul := s.retryScheduler.LocalUpstreamTxRef(txHash); len(ul) > 0 {
		upstreamTxList = ul
		chainRetryStats.ChainLengthMu.Lock()
		chainRetryStats.ChainCommitCnt[commit.SimulationNum]++
		chainRetryStats.ChainLengthMu.Unlock()
	}
	simulation := &api.CXTSimulation{
		SimulationNum:  commit.SimulationNum,
		TxHash:         commit.TxHash,
		Nonce:          commit.Nonce,
		Sender:         commit.Sender,
		ShardId:        s.SelfShard, // 每个节点只建/广播自己分片的 SimTx
		OriginShardId:  tx.OriginShardId,
		RelatedShards:  commit.RelatedShards,
		CallStates:     callStates,
		UpstreamTxList: upstreamTxList,
		BaseBLSSignedMessage: api.BaseBLSSignedMessage{
			Epochs: commit.Epochs,
		},
	}

	// DSN-52 方案1：首次/重模拟成功后、构建 SimTx 前，对该分片完整 RWSet 占 TLV 锁。
	// 目的：让并行的首次模拟互相协调——只有能拿到 TLV 锁的交易才构建 SimTx，冲突方回重试池，
	// 从而避免大量「注定在链上失败」的冲突 SimTx 被构建（链下先协调，而非上链 Wait-Die 判死）。
	// 生命周期：成功后 TLV 锁保留，由 OnBlockCommitted 在 SimTx 上链时释放（当前不设超时解锁，
	// 避免掩盖问题——用户决策 2b）。
	tlv := s.retryScheduler.tempLockView
	var readKeys, writeKeys []api.LockKey
	for _, cs := range callStates {
		if cs.RWSet == nil {
			continue
		}
		if cs.RWSet.ReadState != nil {
			for addr, state := range cs.RWSet.ReadState.State {
				for key := range state {
					readKeys = append(readKeys, api.FormKey(addr, key))
				}
			}
		}
		if cs.RWSet.WriteState != nil {
			for addr, state := range cs.RWSet.WriteState.State {
				for key := range state {
					writeKeys = append(writeKeys, api.FormKey(addr, key))
				}
			}
		}
	}
	if len(readKeys)+len(writeKeys) > 0 {
		priority := api.Priority{Nonce: commit.Nonce, OriginShardID: tx.OriginShardId, TxHash: txHash}
		locked, _ := tlv.TryLockWithPriority(txHash, priority, readKeys, writeKeys)
		if !locked {
			// 拿不到 TLV 锁（与并行模拟冲突、且无法 wound）→ 不构建 SimTx，回重试池。
			utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
				Int("readKeys", len(readKeys)).
				Int("writeKeys", len(writeKeys)).
				Msg("DSN-52 方案1: first-sim failed to acquire TLV lock, not building SimTx, back to retry")
			s.retryScheduler.CallForRetry(&api.RetryTx{
				TxHash:        txHash,
				Epochs:        commit.Epochs,
				RelatedShards: commit.RelatedShards,
				SimulationNum: commit.SimulationNum + 1,
				Condition:     api.Simulate,
				OriginShardID: tx.OriginShardId,
				Nonce:         commit.Nonce,
			})
			return
		}
	}

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("build commit simulation completed")
	tBuildCallStates = time.Since(t0)

	// v5 Wound-Wait: 提交 SimTx 前锁死 Patch（不可再被 Wound）。
	// 注：finalizePatch 仅对“已被下游消费(Consumed)”的节点生效(见 patchpool)；首次构建(Free)的
	// 节点保持 Free 以继续可被后续下游消费。DSN-62 (A) 曾试图对 Free 也 finalize，导致 DAG 链式
	// 救援失效(下游无法消费上游)而回退；保持原语义，交由 DSN-62 (B) 处理“被 Wound 的 Consumed 上游”。
	s.retryScheduler.offChainDAG.finalizePatch(txHash)

	s.buildSignaturesForSimulation(tx.Ctx, simulation)
	tBuildSignatures = time.Since(t0)

	// 提交 SimTx 前，先入链下单一 DAG（offChainDAG.nodes，记录本节点 WriteSet + 上游依赖）
	// 链上 onChainDAGPatches 在 VerifySimulation 验证通过后导入（见 verify.go）
	// 注：AddNode 只建节点（状态 PatchFree），由后续 MarkReady 建立 keyIndex 并触发 subscriber 扫描
	s.retryScheduler.offChainDAG.AddNode(txHash, commit.SimulationNum, writeSet, upstreamTxList)
	tOnChainDAGPatch = time.Since(t0)

	err = s.txSubmitter.SubmitSimulationTx(simulation)
	tSubmitTx = time.Since(t0)
	s.recordTraceBlock(txHash, StageSimulationTxSubmit, s.bc.CurrentHeader().NumberU64())
	// [simTxSubmit] SimTx 提交时间点，配合 [vsCommit] 计算 submit→verify 的 shard 间时间差
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
		Uint32("shardId", s.SelfShard).
		Uint64("blockNum", s.bc.CurrentHeader().NumberU64()).
		Int("simulationNum", commit.SimulationNum).
		Msg("[simTxSubmit] SimTx submitted to local tx pool")
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to submit simulation tx")
		return
	}

	// SimTx 提交成功后，统计链式深度
	if len(upstreamTxList) > 0 {
		// 是链式交易。chainCommitDist 记录提交轮次(simulationNum)分布（重试轮次，非 DAG 深度）。
		chainRetryStats.ChainLengthMu.Lock()
		chainRetryStats.ChainCommitCnt[commit.SimulationNum]++
		chainRetryStats.ChainLengthMu.Unlock()

		// chainDepthCommitDist 按**真实 DAG 深度**(OffChainPatchNode.Depth) 分桶，
		// 而非 simulationNum——避免把重试轮次误当成链深。
		if d := s.retryScheduler.chainNodeDepth(txHash); d > 0 {
			chainRetryStats.ChainDepthMu.Lock()
			chainRetryStats.ChainDepthCommitCnt[d]++
			chainRetryStats.ChainDepthMu.Unlock()
		}
	}

	// SimTx 提交成功后，检查是否可以链式触发依赖它的 retry tx（DAG chaining）
	if s.IsLeader(commit.Epochs[s.SelfShard]) {
		// MarkReady：建立 keyIndex 反查（进入“可被匹配”状态）+ 触发 subscriber 增量扫描
		// 合并了原 addPatch + OnPatchPoolUpdated 两步写入
		s.retryScheduler.offChainDAG.MarkReady(txHash, func(ws *api.RWSet) {
			s.retryScheduler.scanPatchSubscribers(ws)
		})
	}
	s.stats.setCxtStage(txHash, 2)
	if val, ok := s.txStates.Load(txHash); ok {
		tx = val.(*api.TxState)
		tx.Mu.Lock()
		tx.Status = api.SIMULATION_COMMMITTING
		tx.Mu.Unlock()
	} else {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(api.ErrTxNotExist).Msg("commit simulation failed")
		return
	}
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("simulation committed")
}

func (s *sscService) buildSignaturesForSimulation(ctx context.Context, simulation *api.CXTSimulation) {
	txHash := simulation.TxHash
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("begin to build signatures for simulation")
	committee := s.GetCommittee(simulation.Epochs[s.SelfShard], s.SelfShard)
	n := committee.Number
	t := committee.Threshold

	ctx, cancel := context.WithTimeout(ctx, s.Config.CallTimeout)
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
	var tx *api.TxState
	var relatedShards api.RelatedShards
	if val, ok := s.txStates.Load(txHash); ok {
		tx = val.(*api.TxState)
		tx.Mu.Lock()
		if tx.Closed {
			tx.Mu.Unlock()
			utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msg("handle commit ssc vote: tx already closed, drop vote")
			return
		}
		relatedShards = tx.RelatedShards
		tx.Mu.Unlock()
	} else {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(api.ErrTxNotExist).Msg("handle commit ssc vote failed: state not found")
		return
	}

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

	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
		Uint32("fromShard", vote.ShardId).
		Uint32("originShard", vote.OriginShardId).
		Int("sscVoteCount", len(sscVotes)).
		Int("relatedShardCount", len(relatedShards)).
		Msg("[sscVoteRecv] ssc commit vote received")
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("received %s ssc vote [%d/%d], %d in %v, simulationNum=%d",
		vote.Type, len(sscVotes), len(relatedShards), vote.ShardId, relatedShards, vote.SimulationNum)

	// DSN-58 §4.3：刚收到某分片的 Commit sscVote（该分片腿已放好，从 needing 移出），
	// 其余 needing 若早已 ready，此刻 needing 全 ready 就应触发 tryToReSimulation（只扇出 needing）。
	// 注意：commitLock 已在上面 func() 内释放，此处调用不会与 LegSscVoted 的 RLock 死锁。
	if vote.Type == api.Commit && s.retryScheduler != nil {
		s.retryScheduler.MaybeTriggerReSim(txHash)
	}

	if vote.Type == api.Rollback || len(sscVotes) == len(relatedShards) {
		commitType := api.Commit
		commitReason := api.ReasonSuccess
		sscVoteList := make([]*api.CXTCommitSSCVote, 0)
		for _, v := range sscVotes {
			if v.Type == api.Rollback {
				commitType = api.Rollback
				commitReason = v.Reason
				sscVoteList = append(make([]*api.CXTCommitSSCVote, 0), v)
				break
			}
			sscVoteList = append(sscVoteList, v)
		}
		sort.Slice(sscVoteList, func(i, j int) bool {
			return sscVoteList[i].ShardId < sscVoteList[j].ShardId
		})

		ctx, cancel := context.WithTimeout(tx.Ctx, s.Config.CallTimeout)
		defer cancel()
		members := make([]*api.Member, 0, len(relatedShards))
		for _, shardId := range relatedShards {
			members = append(members, s.GetLeader(vote.Epochs[shardId], shardId))
		}

		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("receive all related ssc votes, commit type: %v, reason: %v, simulationNum: %d, relatedShards: %v",
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
		default:
			utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msgf("unsupportted vote type: %s", vote.Type)
		}
	}
}

func (s *sscService) SignalReSimulation(signal *api.RetrySignals) {
	s.retryScheduler.HandleReSimulationSignal(signal)
}

// DetectDeadlockProbe 处理来自其他分片的优先级感知反向 CMH 探针。
func (s *sscService) DetectDeadlockProbe(probe *api.DeadlockProbe) *api.DeadlockProbeAck {
	if s == nil || s.retryScheduler == nil || s.retryScheduler.deadlockDetector == nil {
		return &api.DeadlockProbeAck{Accepted: false}
	}
	return s.retryScheduler.deadlockDetector.HandleProbe(probe)
}

// rollbackVictim 环内 victim 终局 Rollback：从 retryPool 取元数据并复用 Wait-Die 投票路径。
func (s *sscService) rollbackVictim(txHash common.Hash) {
	if s == nil || s.Verifier == nil || s.retryScheduler == nil {
		return
	}
	// 优先从 retryPool 取（常规等待中）；若已上链/不在池，则从锁元数据取。
	if val, ok := s.retryScheduler.retryPool.Load(txHash); ok {
		if rt, ok := val.(*api.RetryTx); ok && rt != nil {
			s.Verifier.RollbackVictim(txHash, rt.Epochs, rt.OriginShardID, rt.SimulationNum)
			return
		}
	}
	mgr := s.retryScheduler.tempLockView.stateLockManager
	if mgr == nil {
		return
	}
	tm, ok := mgr.GetTxMeta(txHash)
	if !ok {
		return
	}
	pri, ok := mgr.GetTxPriority(txHash)
	if !ok {
		return
	}
	s.Verifier.RollbackVictim(txHash, tm.Epochs, pri.OriginShardID, tm.SimulationNum)
}

// HandleVictimTx 处理块内 VictimTx（CMH 判环后打进本分片的控制交易）。
// 每个 validator 执行到它时，都作为本分片 validator 向本分片 leader 投一张
// rollback 票（复用 sendRollbackVoteForDie 的委员会投票路径），从而凑够门限、
// 沿既有 rollback 链路终局回滚 victim。不碰状态、不抢锁。
func (s *sscService) HandleVictimTx(victimTxBytes []byte) error {
	if s == nil || s.Verifier == nil {
		return nil
	}
	p := &sscpb.VictimTx{}
	if err := proto.Unmarshal(victimTxBytes, p); err != nil {
		utils.SSCLogger().Error().Err(err).Msg("[sscService] failed to unmarshal VictimTx")
		return err
	}
	v := sscpb.VictimTxFromProto(p)
	if v == nil || v.TxHash == (common.Hash{}) {
		return errors.New("[sscService] invalid VictimTx: empty tx hash")
	}
	utils.SSCLogger().Info().
		Str("victim", v.TxHash.Hex()).
		Uint32("originShard", v.OriginShardId).
		Int("simulationNum", v.SimulationNum).
		Int("proofLen", len(v.Proof)).
		Msg("[sscService] VictimTx: casting rollback vote")
	s.Verifier.RollbackVictim(v.TxHash, v.Epochs, v.OriginShardId, v.SimulationNum)
	return nil
}

func (s *sscService) HandleRetrySignal(signal *api.RetrySignal) {
	s.retryScheduler.HandleRetrySignal(signal)
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
		if val, ok := s.txStates.Load(proof.TxHash); ok {
			tx := val.(*api.TxState)
			tx.Mu.Lock()
			if proof.Type == api.Commit {
				tx.Status = api.CXT_COMMITTING
			} else {
				tx.Status = api.CXT_ROLLBACKING
			}
			tx.Mu.Unlock()
		} else {
			utils.SSCLogger().Error().Err(api.ErrTxNotExist).Msg("failed to get state")
			return
		}
	}
}

func (s *sscService) closeTransaction(txHash common.Hash, commitOrRollback bool, reason string) {
	t0close := time.Now()

	var tx *api.TxState
	if val, ok := s.txStates.Load(txHash); ok {
		tx = val.(*api.TxState)
		tx.Mu.Lock()
		tx.Closed = true
		tx.Mu.Unlock()
		s.txStates.Delete(txHash)
	}

	if tx != nil {
		tx.CtxCancel()
		if s.IsLeader(tx.Epochs[s.SelfShard]) {
			utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
				Uint32("shardId", s.SelfShard).
				Interface("relatedShards", tx.RelatedShards).
				Int("simulationNum", tx.SimulationNum).
				Msgf("leader close transaction, commit: %v, status: %s, reason: %s", commitOrRollback, tx.Status, reason)

			// 去向分布：DAG 救起(isChainTx)的交易终局记账（仅 leader，避免多 validator 重复）。
			//  - commit: 已在 CR commit 处计 chainTxCRCommitted；
			//  - 非 commit 终局: 打 [chainTxFate] 标记，供脚本统计“被救起却没提交”的去向(reason)。
			if s.retryScheduler != nil && s.retryScheduler.isChainTxMarked(txHash) {
				if !commitOrRollback {
					utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
						Str("reason", reason).
						Msg("[chainTxFate] DAG-rescued tx closed WITHOUT commit")
				}
				s.retryScheduler.clearChainTxMark(txHash)
			}
		}
	}

	tStateLock := time.Since(t0close)

	// 清除 Simulator 的资源（SimulationState + pending + channels）
	s.Simulator.Cleanup(txHash)
	tSimCleanup := time.Since(t0close)

	s.commitLock.Lock()
	delete(s.commitStates, txHash)
	s.commitLock.Unlock()

	s.Verifier.Cleanup(txHash)
	tVerCleanup := time.Since(t0close)

	// 清除定时器：无论提交还是回滚，不再需要 sp1/pool 定时器
	s.timerMgr.RemoveTx(txHash)

	s.retryScheduler.StaleTx(txHash)

	// DAG: Clean up consumedPatches — 释放残留的 Consumed Patch
	if consumedTxHashesVal, exists := s.retryScheduler.consumedPatches.Load(txHash); exists {
		consumedTxHashes := consumedTxHashesVal.([]common.Hash)
		for _, txh := range consumedTxHashes {
			s.retryScheduler.offChainDAG.releasePatch(txh)
		}
		s.retryScheduler.consumedPatches.Delete(txHash)
	}

	// v4/v4b: Clean up off-chain DAG —— 唯一链下清理入口。
	// Remove 原子删除 node + keyIndex + subscriber + txSubKeys，
	// 替代原 removePatch(localPatches) + patches.Delete 两处（消除 patches 泄漏）。
	s.retryScheduler.offChainDAG.Remove(txHash)

	// DSN-54: 交易终结兜底清理 simDAGPatches（成员侧模拟期 patch 子图），与 offChainDAG.Remove 对齐。
	s.retryScheduler.RemoveSimDAGPatches(txHash)

	// 清理 stateLockManager 的 finished 标记，避免 globalFinishedTxs 无限增长。
	// globalFinishedTxs 仅在提交时写入、仅用于统计计数，事务关闭后即可安全删除。
	if s.lockStateMgr != nil {
		s.lockStateMgr.globalFinishedTxs.Delete(txHash)
	}

	// A09: Clean up passive pool
	s.retryScheduler.RemoveFromPassivePool(txHash)

	// 交易全生命周期耗时分析
	s.traceLock.Lock()
	trace, hasTrace := s.txTraces[txHash]
	if !hasTrace {
		trace = &TxBlockTrace{
			TxHash:       txHash,
			SimulateTime: time.Now(),
		}
		s.txTraces[txHash] = trace
	}
	if trace.CommitOrRollbackTime.IsZero() {
		trace.CommitOrRollbackTime = time.Now()
	}
	if trace.CommitOrRollbackBlockNum == 0 && s.currentBlockNum != 0 {
		trace.CommitOrRollbackBlockNum = s.currentBlockNum
	}
	if trace.SimulateTime != trace.CommitOrRollbackTime {
		totalLife := trace.CommitOrRollbackTime.Sub(trace.SimulateTime)
		var simToSubmit, submitToCommit, commitToCRSubmit, crSubmitToCR time.Duration
		if !trace.SimulationTxSubmitTime.IsZero() {
			simToSubmit = trace.SimulationTxSubmitTime.Sub(trace.SimulateTime)
		}
		if !trace.SimulationTxSubmitTime.IsZero() && !trace.SimulationTxCommitTime.IsZero() {
			submitToCommit = trace.SimulationTxCommitTime.Sub(trace.SimulationTxSubmitTime)
		}
		if !trace.SimulationTxCommitTime.IsZero() && !trace.CommitOrRollbackTxSubmitTime.IsZero() {
			commitToCRSubmit = trace.CommitOrRollbackTxSubmitTime.Sub(trace.SimulationTxCommitTime)
		}
		if !trace.CommitOrRollbackTxSubmitTime.IsZero() && !trace.CommitOrRollbackTime.IsZero() {
			crSubmitToCR = trace.CommitOrRollbackTime.Sub(trace.CommitOrRollbackTxSubmitTime)
		}
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
			Dur("simToSubmit", simToSubmit).
			Dur("submitToCommit", submitToCommit).
			Dur("commitToCRSubmit", commitToCRSubmit).
			Dur("crSubmitToCR", crSubmitToCR).
			Dur("totalLife", totalLife).
			Int("simToSubmitGap", blockGap(trace.SimulateBlockNum, trace.SimulationTxSubmitBlockNum)).
			Int("submitToCommitGap", blockGap(trace.SimulationTxSubmitBlockNum, trace.SimulationTxCommitBlockNum)).
			Int("commitToCRGap", blockGap(trace.SimulationTxCommitBlockNum, trace.CommitOrRollbackTxSubmitBlockNum)).
			Int("crSubmitToCRGap", blockGap(trace.CommitOrRollbackTxSubmitBlockNum, trace.CommitOrRollbackBlockNum)).
			Int("totalGap", blockGap(trace.SimulateBlockNum, trace.CommitOrRollbackBlockNum)).
			Bool("commit", commitOrRollback).
			Uint64("simBlock", trace.SimulateBlockNum).
			Uint64("submitBlock", trace.SimulationTxSubmitBlockNum).
			Uint64("commitBlock", trace.SimulationTxCommitBlockNum).
			Uint64("crSubmitBlock", trace.CommitOrRollbackTxSubmitBlockNum).
			Uint64("crBlock", trace.CommitOrRollbackBlockNum).
			Msg("[txLife] transaction lifetime breakdown")
	}
	s.traceLock.Unlock()

	totalClose := time.Since(t0close)

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Bool("commit", commitOrRollback).
		Str("stateLock", tStateLock.String()).
		Str("simCleanup", tSimCleanup.String()).
		Str("verCleanup", tVerCleanup.String()).
		Str("totalClose", totalClose.String()).
		Msg("closeTransaction timing breakdown")
}

// blockGap 计算两个块号之间的间隔（-1 表示未记录，0 表示同一块内完成）
func blockGap(from, to uint64) int {
	if from == 0 || to == 0 {
		return -1
	}
	if to < from {
		return -1
	}
	return int(to - from)
}

func (s *sscService) closeTransactions(txs map[common.Hash]bool) {
	utils.SSCLogger().Info().Interface("txs", txs).Msgf("close transactions, num=%d", len(txs))
	var closedTxs []*api.TxState
	s.txStates.Range(func(key, val any) bool {
		txHash := key.(common.Hash)
		if _, ok := txs[txHash]; ok {
			tx := val.(*api.TxState)
			tx.Mu.Lock()
			tx.Closed = txs[txHash]
			closedTxs = append(closedTxs, tx)
			tx.Mu.Unlock()
			s.txStates.Delete(txHash)
		}
		return true
	})
	for _, tx := range closedTxs {
		tx.CtxCancel()
	}
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
	s.retryScheduler.AddToRetry(tx)
}

func (s *sscService) AddToPassivePool(txHash common.Hash) {
	s.retryScheduler.AddToPassivePool(txHash)
}

func (s *sscService) RetryCommit(txHash common.Hash) *api.RetryCommitResp {
	resp := s.retryScheduler.RetryCommit(txHash)
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("retry commit, locked: %v", resp.Locked)
	return resp
}

func (s *sscService) RetryCommitDAG(txHash common.Hash) *api.RetryCommitResp {
	resp := s.retryScheduler.RetryCommitDAG(txHash)
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("retry commit(DAG), locked: %v", resp.Locked)
	return resp
}

func (s *sscService) RetryCancel(txHash common.Hash) {
	s.retryScheduler.RetryCancel(txHash)
}

// StoreSimDAGPatch — DSN-54：成员侧接收 leader 广播的模拟期链下 DAG patch 子图，
// 写入本地 retryScheduler.simDAGPatches，供本轮 (tx,simNum) 模拟读被锁 key 时反查。
func (s *sscService) StoreSimDAGPatch(req *api.StoreSimDAGPatchRequest) {
	if s == nil || s.retryScheduler == nil || req == nil || req.Subgraph == nil {
		return
	}
	s.retryScheduler.StoreSimDAGPatch(req.Subgraph)
}
