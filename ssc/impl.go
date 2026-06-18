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

	stateLock   lm.RWMutex // cmLock for txStates, finishedTxs
	txStates    map[common.Hash]*api.TxState
	finishedTxs map[common.Hash]bool

	// 验证上下文 now managed by Verifier
	// verifyCtxLock           lm.RWMutex
	// executionVerifyContexts map[common.Hash]*api.ExecutionVerifyContext
	// txLockedSimNum          map[common.Hash]int   // moved to Verifier

	commitLock   lm.RWMutex // cmLock for commitStates
	commitStates map[common.Hash]*api.CommitState

	ctx          context.Context
	chainHeadCh  chan core.ChainHeadEvent
	chainHeadSub event.Subscription
	stats        *SimulationStats // experiment statistics

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
	numWorkers := config.SimulationLimit // worker 数量

	service := &sscService{
		CommitteeMechanism: cm,
		Comm:               comm,
		BLSSignerMgr:       signerMgr,
		txSigner:           txSigner,
		Config:             config,
		bc:                 bc,
		stats:              newSimulationStats(),
		stateLock:          lm.NewRWMutex(),
		commitLock:         lm.NewRWMutex(),
		txStates:           make(map[common.Hash]*api.TxState),
		commitStates:       make(map[common.Hash]*api.CommitState),
		finishedTxs:        make(map[common.Hash]bool),
		ctx:                ctx,
		chainHeadCh:        make(chan core.ChainHeadEvent, 10),
		chainHeadSub:       nil,
		txTraces:           make(map[common.Hash]*TxBlockTrace),
	}
	lockStateMgr := newStateLockManager(service)
	service.lockStateMgr = lockStateMgr
	service.tempLockView = lockStateMgr.GetTempLockView()
	service.timerMgr = NewTimerManager(sscConfig.Timeout, service)
	service.retryScheduler = NewRetryScheduler(ctx, &RetrySchedulerStateAccessor{
		IsLeader: func(epoch api.Epoch) bool { return service.CommitteeMechanism.IsLeader(epoch) },
		GetLeader: func(epoch api.Epoch, shardId uint32) *api.Member {
			return service.CommitteeMechanism.GetLeader(epoch, shardId)
		},
		ShardNum:            func() uint32 { return service.CommitteeMechanism.ShardNum() },
		RetryAddCount:       func() { service.stats.RetryAddCount.Add(1) },
		SampleRetryPool:     func(size int) { service.stats.sampleRetryPool(size) },
		RetryReadySignal:    func() { service.stats.RetryReadySignal.Add(1) },
		RetryNotReadySignal: func() { service.stats.RetryNotReadySignal.Add(1) },
		RetrySuccessCount:   func() { service.stats.RetrySuccessCount.Add(1) },
		RetryFailCount:      func() { service.stats.RetryFailCount.Add(1) },
		SetChainPatch: func(txHash common.Hash, patch *api.RWSet) {
			service.stateLock.Lock()
			defer service.stateLock.Unlock()
			if tx := service.txStates[txHash]; tx != nil {
				// ChainPatch 存储在 Simulator 中
				sim, ok := service.Simulator.GetSimState(txHash)
				if ok && sim != nil {
					sim.ChainPatch = patch
				}
			}
		},
		GetSimState: func(txHash common.Hash) (*api.SimulationState, bool) {
			return service.Simulator.GetSimState(txHash)
		},
		TriggerReSimulation: func(txHash common.Hash, simulationNum int) {
			service.Simulator.StartReSimulation(txHash, simulationNum)
		},
	}, comm, service.SelfShard, service.tempLockView)

	// 创建 Simulator（自管理 SimulationState + callStatesInWaiting + worker pool）
	simComm := NewSimulatorCommunicator(comm, signerMgr, service.CommitteeMechanism, config, ctx)
	service.Simulator = NewSimulator(
		SimulatorStateAccessor{
			GetTxState: func(txHash common.Hash) (*api.TxState, error) {
				service.stateLock.RLock()
				defer service.stateLock.RUnlock()
				if tx := service.txStates[txHash]; tx != nil {
					return tx, nil
				}
				if _, exists := service.finishedTxs[txHash]; exists {
					return nil, api.ErrTxHasBeenClosed
				}
				return nil, api.ErrTxNotExist
			},
			SetTxStatus: func(txHash common.Hash, status api.CXTStatus) {
				service.stateLock.Lock()
				defer service.stateLock.Unlock()
				if tx := service.txStates[txHash]; tx != nil {
					tx.Status = status
				}
			},
			MergeRelatedShards: func(txHash common.Hash, newShards api.RelatedShards) api.RelatedShards {
				service.stateLock.Lock()
				defer service.stateLock.Unlock()
				if tx := service.txStates[txHash]; tx != nil {
					tx.RelatedShards = tx.RelatedShards.Merge(newShards)
					return tx.RelatedShards
				}
				return newShards
			},
			SetSimulationNum: func(txHash common.Hash, num int) {
				service.stateLock.Lock()
				defer service.stateLock.Unlock()
				if tx := service.txStates[txHash]; tx != nil {
					tx.SimulationNum = num
				}
			},
			CreateTxState: func(txHash common.Hash, txState *api.TxState) {
				service.stateLock.Lock()
				defer service.stateLock.Unlock()
				service.txStates[txHash] = txState
			},
			GetCommitStates: func() map[common.Hash]*api.CommitState {
				return service.commitStates
			},
			GetCommitLock: func() *lm.RWMutex {
				return &service.commitLock
			},
			GetFinishedTxs: func() map[common.Hash]bool {
				return service.finishedTxs
			},
			CallForRetry: func(tx *api.RetryTx) {
				service.retryScheduler.CallForRetry(tx)
			},
			TempLockTryLock: func(txHash common.Hash, reads, writes []api.LockKey) bool {
				return service.retryScheduler.tempLockView.TryLock(txHash, reads, writes)
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
				service.stateLock.Lock()
				defer service.stateLock.Unlock()
				if tx := service.txStates[txHash]; tx != nil {
					tx.Status = status
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
				if tx := service.txStates[txHash]; tx != nil {
					tx.Status = api.WAITING_FOR_RESIMULATING_ON_CHAIN
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
				if tx := service.txStates[txHash]; tx != nil {
					tx.Status = status
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
			s.stats.SetBlockTraces(s.txTraces)
			s.stats.Dump(s.lockStateMgr)
			utils.SSCLogger().Info().Msg("ssc service stop...")
			s.Simulator.StopWorkers()
			return
		case <-s.chainHeadSub.Err():
			utils.SSCLogger().Error().Msg("chain head subscription error")
			return
		case head := <-s.chainHeadCh:
			utils.SSCLogger().Info().Uint64("blockNum", head.Block.NumberU64()).Msg("new block committed")
			b := head.Block
			callStatesInWaiting := s.Simulator.PopCallStatesInWaiting(b.Header().Hash())
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
			s.stats.SetBlockTraces(s.txTraces)
			s.stats.Dump(s.lockStateMgr)
		}
	}
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
	_, exists := s.txStates[txHash]
	s.stateLock.RUnlock()
	if !exists {
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

func (s *sscService) sendCXTCommitVote(shardId uint32, vote *api.CXTCommitVote) {
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
	s.stateLock.Lock()
	if tx := s.txStates[txHash]; tx != nil {
		tx.RelatedShards = simulation.RelatedShards
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
	if tx := s.txStates[txHash]; tx != nil {
		tx.Status = api.BUILDING_COMMIT_PROOF
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
	s.Simulator.HandleCXTRecallProof(proof)
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
	case api.PoolTimeout:
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msgf("simulation is pool timeout, status=%s", commit.Status.String())
		s.closeTransaction(txHash, false, api.PoolTimeout.String())
		return
	}

	// update related shards
	s.stateLock.Lock()
	tx, err := s.getTxStateLocked(txHash)
	if err != nil {
		s.stateLock.Unlock()
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).Msg("commit simulation failed")
		return
	}
	tx.RelatedShards = tx.RelatedShards.Merge(commit.RelatedShards)
	s.stateLock.Unlock()

	// build commit simulation — 从 Simulator 获取 callStates
	callStates, err := s.Simulator.BuildCallStates(txHash, commit.SimulationNum)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).Msg("commit simulation failed: no call states")
		return
	}

	simulation := &api.CXTSimulation{
		SimulationNum: commit.SimulationNum,
		TxHash:        commit.TxHash,
		Nonce:         commit.Nonce,
		Sender:        commit.Sender,
		ShardId:       commit.ShardId,
		OriginShardId: tx.OriginShardId,
		RelatedShards: commit.RelatedShards,
		CallStates:    callStates,
		BaseBLSSignedMessage: api.BaseBLSSignedMessage{
			Epochs: commit.Epochs,
		},
	}

	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msgf("build commit simulation completed")

	s.buildSignaturesForSimulation(tx.Ctx, simulation)

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
		// 从 callStates 中提取完整 WriteSet 用于 chainNextSim
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
		s.retryScheduler.chainNextSim(txHash, writeSet)
	}

	s.stats.setCxtStage(txHash, 2)
	s.stateLock.Lock()
	tx, err = s.getTxStateLocked(txHash)
	if err != nil {
		s.stateLock.Unlock()
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).Msg("commit simulation failed")
		return
	}
	tx.Status = api.SIMULATION_COMMMITTING
	s.stateLock.Unlock()
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msgf("simulation committed")
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
	s.stateLock.RLock()
	tx, err := s.getTxStateLocked(txHash)
	if err != nil {
		s.stateLock.RUnlock()
		if errors.Is(err, api.ErrTxHasBeenClosed) {
			utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msg("handle commit ssc vote: tx already closed, drop vote")
		} else {
			utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).Msg("handle commit ssc vote failed: state not found")
		}
		return
	}
	relatedShards := tx.RelatedShards
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

		ctx, cancel := context.WithTimeout(tx.Ctx, s.Config.CallTimeout)
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
				s.Simulator.StartReSimulation(proof.TxHash, proof.SimulationNum+1)
			}()
		default:
			utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msgf("unsupportted vote type: %s", vote.Type)
		}
	}
}

func (s *sscService) SignalReSimulation(signal *api.RetrySignals) {
	s.retryScheduler.HandleReSimulationSignal(signal)
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
		s.stateLock.Lock()
		tx, err := s.getTxStateLocked(proof.TxHash)
		if err != nil {
			utils.SSCLogger().Error().Err(err).Msg("failed to get state")
			s.stateLock.Unlock()
			return
		}
		if proof.Type == api.Commit {
			tx.Status = api.CXT_COMMITTING
		} else {
			tx.Status = api.CXT_ROLLBACKING
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

// getTxState 读取交易公共状态（自动加锁）
func (s *sscService) getTxState(txHash common.Hash) (*api.TxState, error) {
	s.stateLock.RLock()
	defer s.stateLock.RUnlock()
	return s.getTxStateLocked(txHash)
}

// getTxStateLocked 读取交易公共状态（调用方需持有 stateLock）
func (s *sscService) getTxStateLocked(txHash common.Hash) (*api.TxState, error) {
	if tx := s.txStates[txHash]; tx != nil {
		return tx, nil
	}
	if _, exists := s.finishedTxs[txHash]; exists {
		return nil, api.ErrTxHasBeenClosed
	}
	return nil, api.ErrTxNotExist
}

// getState 兼容旧代码：从 TxState + SimulationState 构造 CXTSimulationState 返回。
// TODO: 逐步迁移后删除此函数。
func (s *sscService) getState(txHash common.Hash) (*api.CXTSimulationState, error) {
	tx, err := s.getTxStateLocked(txHash)
	if err != nil {
		return nil, err
	}
	sim, _ := s.Simulator.GetSimState(txHash)
	return &api.CXTSimulationState{
		Nonce:         tx.Nonce,
		TxSender:      tx.TxSender,
		Epochs:        tx.Epochs,
		Status:        tx.Status,
		SimulationNum: tx.SimulationNum,
		OriginShardId: tx.OriginShardId,
		RelatedShards: tx.RelatedShards,
		RetrySignals:  tx.RetrySignals,
		Ctx:           tx.Ctx,
		CtxCancel:     tx.CtxCancel,
		// SimulationState 字段（可能为 nil）
		CurrentCallFrame: func() *api.CallFrame {
			if sim != nil {
				return sim.CurrentCallFrame
			}
			return nil
		}(),
		CallStack: func() *api.CallStack {
			if sim != nil {
				return sim.CallStack
			}
			return nil
		}(),
		SimulationRequest: func() *api.CXTSimulationRequest {
			if sim != nil {
				return sim.SimulationRequest
			}
			return nil
		}(),
		SimulationResult: func() *api.CXTSimulationSSCResult {
			if sim != nil {
				return sim.SimulationResult
			}
			return nil
		}(),
		SimulationCallStates: func() map[int]api.SimulationCallStates {
			if sim != nil {
				return sim.SimulationCallStates
			}
			return nil
		}(),
		LockedCallIndex: func() api.CallIndex {
			if sim != nil {
				return sim.LockedCallIndex
			}
			return nil
		}(),
		CallForest: func() *api.CallForest {
			if sim != nil {
				return sim.CallForest
			}
			return nil
		}(),
		ChainPatch: func() *api.RWSet {
			if sim != nil {
				return sim.ChainPatch
			}
			return nil
		}(),
		SimulateCh: func() chan struct{} {
			if sim != nil {
				return sim.SimulateCh
			}
			return nil
		}(),
	}, nil
}

func (s *sscService) closeTransaction(txHash common.Hash, commitOrRollback bool, reason string) {
	s.stateLock.Lock()
	tx, _ := s.getTxStateLocked(txHash)
	if tx != nil {
		tx.CtxCancel()
		if s.IsLeader(tx.Epochs[s.SelfShard]) {
			utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Uint32("shardId", s.SelfShard).Interface("relatedShards", tx.RelatedShards).Msgf("leader close transaction, commit: %v, status: %s, reason: %s", commitOrRollback, tx.Status, reason)
		}
	}
	delete(s.txStates, txHash)
	s.finishedTxs[txHash] = commitOrRollback
	s.stateLock.Unlock()

	// 清除 Simulator 的资源（SimulationState + pending + channels）
	s.Simulator.Cleanup(txHash)

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
		if tx := s.txStates[txHash]; tx != nil {
			tx.CtxCancel()
			s.Simulator.Cleanup(txHash)
			s.finishedTxs[txHash] = commitOrRollback
			delete(s.txStates, txHash)
		}
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
	s.retryScheduler.AddToRetry(tx)
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
