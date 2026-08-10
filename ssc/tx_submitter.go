package ssc

import (
	"container/heap"
	"context"
	"encoding/json"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/core"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/core/vm"
	"github.com/harmony-one/harmony/hmy"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/harmony-one/harmony/ssc/perf"
	"github.com/pkg/errors"
)

type TxType string

const (
	EmptyTx            TxType = "EmptyTx"
	SimulationTx       TxType = "SimulationTx"
	CommitOrRollbackTx TxType = "CommitOrRollbackTx"
	UploadOpinionsTx   TxType = "UploadOpinionsTx"
	NewEpochTx         TxType = "NewEpochTx"
)

const maxFlightLimit int32 = 1000

// Priority 返回交易类型的优先级（1-5，越大优先级越高）
func (t TxType) Priority() int {
	switch t {
	case EmptyTx:
		return 1
	case SimulationTx:
		return 2
	case CommitOrRollbackTx:
		return 3
	case UploadOpinionsTx:
		return 4
	case NewEpochTx:
		return 5
	default:
		return 0
	}
}

// txTypeToHeapIndex 将交易类型映射到 heap 索引（0=最高优先级）
func txTypeToHeapIndex(txType TxType) int {
	switch txType {
	case NewEpochTx: // 优先级 5 -> 索引 0（最高）
		return 0
	case UploadOpinionsTx: // 优先级 4 -> 索引 1
		return 1
	case CommitOrRollbackTx: // 优先级 3 -> 索引 2
		return 2
	case SimulationTx: // 优先级 2 -> 索引 3
		return 3
	case EmptyTx: // 优先级 1 -> 索引 4（最低）
		return 4
	default:
		return 4
	}
}

// txTask 表示待提交的交易任务
type txTask struct {
	txBuilder    func(nonce uint64, gasPrice *big.Int) *types.Transaction
	txType       TxType
	originTxHash common.Hash
	nonce        uint64 // 排序用的 nonce（SimTx 已在 CXTSimulation.Nonce 中，非 SimTx 在入堆时快照）
	shardId      uint32 // 排序用的 shardId
	done         chan error
}

// TxPriorityQueue 优先级队列，包含 5 个独立 channel（索引 0=最高优先级）
type TxPriorityQueue struct {
	queues [5]chan *txTask
}

// taskHeap MinHeap，按 (nonce 小优先, shardId 小优先) 排序
type taskHeap struct {
	items []*txTask
}

func newTaskHeap() *taskHeap {
	return &taskHeap{items: make([]*txTask, 0)}
}

func (h *taskHeap) Len() int { return len(h.items) }

func (h *taskHeap) Less(i, j int) bool {
	a, b := h.items[i], h.items[j]
	// 1. nonce 小优先
	if a.nonce != b.nonce {
		return a.nonce < b.nonce
	}
	// 2. shardId 小优先
	return a.shardId < b.shardId
}

func (h *taskHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *taskHeap) Push(x interface{}) {
	h.items = append(h.items, x.(*txTask))
}

func (h *taskHeap) Pop() interface{} {
	old := h.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	h.items = old[0 : n-1]
	return item
}

func NewTxSubmitter(selfShard uint32, simSigner, crSigner api.TxSigner, nodeAPI hmy.NodeAPI, config *api.Config) api.TxSubmitter {
	t := &txSubmitter{
		selfShard: selfShard,
		simSigner: simSigner,
		crSigner:  crSigner,
		nodeAPI:   nodeAPI,
		config:    config,
		queue: TxPriorityQueue{
			queues: [5]chan *txTask{
				make(chan *txTask, 256), // 索引 0: NewEpochTx (优先级 5)
				make(chan *txTask, 256), // 索引 1: UploadOpinionsTx (优先级 4)
				make(chan *txTask, 256), // 索引 2: CommitOrRollbackTx (优先级 3)
				make(chan *txTask, 256), // 索引 3: SimulationTx (优先级 2)
				make(chan *txTask, 256), // 索引 4: EmptyTx (优先级 1)
			},
		},
		heaps: [5]*taskHeap{
			newTaskHeap(),
			newTaskHeap(),
			newTaskHeap(),
			newTaskHeap(),
			newTaskHeap(),
		},
		submittedTxHashes: make(map[common.Hash]struct{}),
		ctx:               context.Background(),
	}
	t.signal = sync.NewCond(&t.mu)
	// 启动后台提交协程
	go t.processQueue()
	return t
}

type txSubmitter struct {
	lock      sync.Mutex
	selfShard uint32
	simSigner api.TxSigner
	crSigner  api.TxSigner
	simNonce  uint64
	crNonce   uint64
	nodeAPI   hmy.NodeAPI
	config    *api.Config
	queue     TxPriorityQueue
	ctx       context.Context

	// 监控计数器
	pendingCount   atomic.Int64 // 队列中待处理的任务数
	completedCount atomic.Int64 // 已完成的任务总数（成功 + 失败）
	failedCount    atomic.Int64 // 失败的任务数

	// 排序 Buffer + Flight 控制
	heaps             [5]*taskHeap            // 5 个独立 MinHeap（按类型索引）
	mu                sync.Mutex              // 保护 heaps + signal 的条件变量
	signal            *sync.Cond              // 条件变量：等待新 task 或 flight 空位
	inFlight          atomic.Int32            // 已提交到 TxPool 但未被块确认的交易数
	submittedTxHashes map[common.Hash]struct{} // 已提交但未确认的 txHash（锁保护）
	submittedLock     sync.Mutex
}

// getSigner 根据交易类型返回对应的签名器
func (t *txSubmitter) getSigner(txType TxType) api.TxSigner {
	if txType == CommitOrRollbackTx {
		return t.crSigner
	}
	return t.simSigner
}

// getNonce 根据交易类型返回对应的 nonce
func (t *txSubmitter) getNonce(txType TxType) uint64 {
	if txType == CommitOrRollbackTx {
		return t.crNonce
	}
	return t.simNonce
}

// incNonce 根据交易类型自增对应的 nonce
func (t *txSubmitter) incNonce(txType TxType) {
	if txType == CommitOrRollbackTx {
		t.crNonce++
	} else {
		t.simNonce++
	}
}

// SubmitSimulationTx 将 SimulationTx 任务放入优先级队列，阻塞等待处理完成
func (t *txSubmitter) SubmitSimulationTx(simulation *api.CXTSimulation) error {
	task := &txTask{
		txBuilder: func(nonce uint64, gasPrice *big.Int) *types.Transaction {
			input, _ := json.Marshal(simulation)
			intrinsicGas, _ := vm.IntrinsicGas(input, false, false, false, false)
			gasLimit := t.config.SimulationCommitGasLimit + intrinsicGas*2
			return types.NewCrossShardTransaction(nonce, &vm.SimulationCommitAddr, t.selfShard, t.selfShard, big.NewInt(0), gasLimit, gasPrice, input)
		},
		txType:       SimulationTx,
		originTxHash: simulation.TxHash,
		nonce:        simulation.Nonce,
		shardId:      simulation.ShardId,
		done:         make(chan error, 1),
	}
	utils.Logger().Info().
		Str("txHash", simulation.TxHash.Hex()).
		Uint64("Nonce", t.GetNonce(SimulationTx)).
		Int64("pendingCnt", t.pendingCount.Load()).
		Int("simulationNum", simulation.SimulationNum).
		Uint64("blockNum", t.nodeAPI.Blockchain().CurrentBlock().NumberU64()).
		Str("originTxHash", simulation.TxHash.Hex()).
		Msg("[SimulationTx] submitting")
	t.pendingCount.Add(1)
	t.queue.queues[txTypeToHeapIndex(SimulationTx)] <- task
	err := <-task.done
	if err != nil {
		t.failedCount.Add(1)
	}
	t.completedCount.Add(1)
	t.pendingCount.Add(-1)
	return err
}

// SubmitSimulationTxWithSigner 用指定的 signer 类型提交 SimulationTx。
// 当 signerType="CommitOrRollbackTx" 时用 CR signer 的 nonce 提交，确保 CR(crN) → SimTx(crN+1) 排序。
func (t *txSubmitter) SubmitSimulationTxWithSigner(simulation *api.CXTSimulation, signerType string) error {
	task := &txTask{
		txBuilder: func(nonce uint64, gasPrice *big.Int) *types.Transaction {
			input, _ := json.Marshal(simulation)
			intrinsicGas, _ := vm.IntrinsicGas(input, false, false, false, false)
			gasLimit := t.config.SimulationCommitGasLimit + intrinsicGas*2
			return types.NewCrossShardTransaction(nonce, &vm.SimulationCommitAddr, t.selfShard, t.selfShard, big.NewInt(0), gasLimit, gasPrice, input)
		},
		txType:       TxType(signerType),
		originTxHash: simulation.TxHash,
		nonce:        simulation.Nonce,
		shardId:      simulation.ShardId,
		done:         make(chan error, 1),
	}
	utils.Logger().Info().
		Str("txHash", simulation.TxHash.Hex()).
		Uint64("Nonce", t.GetNonce(TxType(signerType))).
		Str("signerType", signerType).
		Msg("[SimulationTx] submitting with CR signer (hot key chain)")
	t.pendingCount.Add(1)
	t.queue.queues[txTypeToHeapIndex(TxType(signerType))] <- task
	err := <-task.done
	if err != nil {
		t.failedCount.Add(1)
	}
	t.completedCount.Add(1)
	t.pendingCount.Add(-1)
	return err
}

// SubmitCommitOrRollbackTx 将 CommitOrRollbackTx 任务放入优先级队列
func (t *txSubmitter) SubmitCommitOrRollbackTx(proof *api.CXTCommitProof) error {
	task := &txTask{
		txBuilder: func(nonce uint64, gasPrice *big.Int) *types.Transaction {
			input, _ := json.Marshal(proof)
			return types.NewCrossShardTransaction(nonce, &vm.CxtCommitOrRollbackAddr, t.selfShard, t.selfShard, big.NewInt(0), t.config.SimulationCommitGasLimit, gasPrice, input)
		},
		txType:       CommitOrRollbackTx,
		originTxHash: proof.TxHash,
		nonce:        0, // nonce 在 processTask 中分配
		shardId:      t.selfShard,
		done:         make(chan error, 1),
	}
	t.pendingCount.Add(1)
	t.queue.queues[txTypeToHeapIndex(CommitOrRollbackTx)] <- task
	go func() {
		err := <-task.done
		if err != nil {
			t.failedCount.Add(1)
		}
		t.completedCount.Add(1)
		t.pendingCount.Add(-1)
	}()
	return nil
}

// SubmitEmptyTx
// Deprecated
func (t *txSubmitter) SubmitEmptyTx() error {
	task := &txTask{
		txBuilder: func(nonce uint64, gasPrice *big.Int) *types.Transaction {
			return types.NewCrossShardTransaction(nonce, &vm.EmptyAddr, t.selfShard, t.selfShard, big.NewInt(0), t.config.SimulationCommitGasLimit, gasPrice, []byte{})
		},
		txType:       EmptyTx,
		originTxHash: common.Hash{},
		nonce:        0,
		shardId:      t.selfShard,
		done:         make(chan error, 1),
	}
	t.pendingCount.Add(1)
	t.queue.queues[txTypeToHeapIndex(EmptyTx)] <- task
	go func() {
		err := <-task.done
		if err != nil {
			t.failedCount.Add(1)
		}
		t.completedCount.Add(1)
		t.pendingCount.Add(-1)
	}()
	return nil
}

// SubmitNewEpoch 将 NewEpochTx 任务放入优先级队列
func (t *txSubmitter) SubmitNewEpoch(newEpoch *api.NewEpoch) error {
	task := &txTask{
		txBuilder: func(nonce uint64, gasPrice *big.Int) *types.Transaction {
			input, _ := json.Marshal(newEpoch)
			return types.NewCrossShardTransaction(nonce, &vm.NewEpochAddr, t.selfShard, t.selfShard, big.NewInt(0), t.config.SimulationCommitGasLimit, gasPrice, input)
		},
		txType:       NewEpochTx,
		originTxHash: common.Hash{},
		nonce:        0,
		shardId:      t.selfShard,
		done:         make(chan error, 1),
	}
	t.pendingCount.Add(1)
	t.queue.queues[txTypeToHeapIndex(NewEpochTx)] <- task
	go func() {
		err := <-task.done
		if err != nil {
			t.failedCount.Add(1)
		}
		t.completedCount.Add(1)
		t.pendingCount.Add(-1)
	}()
	return nil
}

// SubmitUploadOpinions 将 UploadOpinionsTx 任务放入优先级队列
func (t *txSubmitter) SubmitUploadOpinions(uploadOpinions *api.SelfOpinions) error {
	task := &txTask{
		txBuilder: func(nonce uint64, gasPrice *big.Int) *types.Transaction {
			input, _ := json.Marshal(uploadOpinions)
			return types.NewCrossShardTransaction(nonce, &vm.SLOpinionAddr, t.selfShard, t.selfShard, big.NewInt(0), t.config.SimulationCommitGasLimit, gasPrice, input)
		},
		txType:       UploadOpinionsTx,
		originTxHash: common.Hash{},
		nonce:        0,
		shardId:      t.selfShard,
		done:         make(chan error, 1),
	}
	t.pendingCount.Add(1)
	t.queue.queues[txTypeToHeapIndex(UploadOpinionsTx)] <- task
	go func() {
		err := <-task.done
		if err != nil {
			t.failedCount.Add(1)
		}
		t.completedCount.Add(1)
		t.pendingCount.Add(-1)
	}()
	return nil
}

// processQueue 后台协程：两个 goroutine 各司其职
//
// drainer goroutine: 从 chan 收 task → push 到对应 heap，然后 Broadcast 通知 worker
// worker goroutine:  按优先级遍历 heap → pop → 提交，flight 满时 Wait
//
// 外部 OnBlockCommitted 调用 ReleaseFlight 释放 inFlight 后也会 Broadcast
func (t *txSubmitter) processQueue() {
	// drainer: chan → heap
	go func() {
		for {
			t.drainOne()
		}
	}()

	// worker: heap → 同步提交
	t.mu.Lock()
	defer t.mu.Unlock()
	for {
		// submitNext 要求调用时已持有 t.mu，返回时也持有 t.mu
		for t.submitNext() {
		}
	}
}

// drainOne 阻塞等待任意 chan，收到后 push 到对应 heap 并 Broadcast
func (t *txSubmitter) drainOne() {
	var task *txTask
	select {
	case task = <-t.queue.queues[0]:
	case task = <-t.queue.queues[1]:
	case task = <-t.queue.queues[2]:
	case task = <-t.queue.queues[3]:
	case task = <-t.queue.queues[4]:
	}
	t.pushToHeap(task)
	t.signal.Broadcast()
}

// submitNext 严格按优先级从高到低检查 heap，有任务且 flight 有空位则提交一笔
//
// 必须在 t.mu 锁内调用。调用后 mu 可能被释放又重获（processTask 期间短暂释放）。
// 返回 true = 提交了一笔（mu 已重获），false = 所有 heap 空或 flight 满（mu 已重获）。
func (t *txSubmitter) submitNext() bool {
	for i := 0; i < 5; i++ {
		for t.heaps[i].Len() > 0 && t.inFlight.Load() < maxFlightLimit {
			task := heap.Pop(t.heaps[i]).(*txTask)
			t.inFlight.Add(1)
			t.mu.Unlock()
			err := t.processTask(task)
			t.mu.Lock()
			if err != nil {
				// 提交失败，把 inFlight 还回去
				t.inFlight.Add(-1)
			}
			// 一笔提交后回到外层，给其他优先级机会
			return true
		}
	}
	// 所有 heap 空 or flight 满，等待
	for {
		// 检查所有 heap 和 inFlight（防 Broadcast 在 Wait 前到达导致信号丢失）
		hasWork := false
		for i := 0; i < 5; i++ {
			if t.heaps[i].Len() > 0 && t.inFlight.Load() < maxFlightLimit {
				hasWork = true
				break
			}
		}
		if hasWork {
			return true
		}
		t.signal.Wait()
		// Wait 返回后重获 mu，回到 for 开头重检查条件
	}
}

// pushToHeap 将 task 放入对应类型的 heap（上限 10000）
func (t *txSubmitter) pushToHeap(task *txTask) {
	t.mu.Lock()
	defer t.mu.Unlock()
	heapIdx := txTypeToHeapIndex(task.txType)
	if t.heaps[heapIdx].Len() >= 10000 {
		utils.SSCLogger().Warn().
			Int("heapIdx", heapIdx).
			Str("txType", string(task.txType)).
			Msg("[TxSubmitter] heap full, dropping task")
		if task.done != nil {
			task.done <- errors.New("heap full")
		}
		return
	}
	heap.Push(t.heaps[heapIdx], task)
}

// processTask 处理单个交易任务，负责 nonce 分配和重试
func (t *txSubmitter) processTask(task *txTask) error {
	t.lock.Lock()
	currentNonce := t.getNonce(task.txType)
	signer := t.getSigner(task.txType)
	if signer == nil {
		t.lock.Unlock()
		task.done <- errors.New("signer not configured for tx type: " + string(task.txType))
		return errors.New("signer not configured for tx type: " + string(task.txType))
	}
	gasPrice := new(big.Int).Set(t.config.SimulationCommitGasPrice)
	t.lock.Unlock()

	err := t.submitWithRetry(task.txBuilder, task.txType, task.originTxHash, currentNonce, gasPrice, 0, signer)
	task.done <- err
	return err
}

// submitWithRetry 处理交易提交重试，支持 gasPrice 递增和 nonce 调整
func (t *txSubmitter) submitWithRetry(
	txBuilder func(nonce uint64, gasPrice *big.Int) *types.Transaction,
	txType TxType,
	originTxHash common.Hash,
	nonce uint64,
	gasPrice *big.Int,
	retryCount int,
	signer api.TxSigner,
) error {
	t0 := time.Now()
	defer func() {
		perf.RecordPkg("txSubmitter", "submitWithRetry", "total", time.Since(t0))
	}()
	tx := txBuilder(nonce, gasPrice)
	txHash := tx.Hash()
	if needed, _ := vm.IntrinsicGas(tx.Data(), false, false, false, false); needed > tx.GasLimit() {
		return core.ErrIntrinsicGas
	}

	signedTx, err := signer.Sign(tx)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
			Err(err).
			Uint64("Nonce", nonce).
			Str("originTxHash", originTxHash.Hex()).Msgf("sign tx failed, nonce=%d", nonce)
		return err
	}

	utils.SSCLogger().Debug().Str("txHash", signedTx.Hash().Hex()).
		Uint64("Nonce", nonce).
		Str("originTxHash", originTxHash.Hex()).
		Int("retry", retryCount).Msgf("submitting %s, nonce=%d, gasPrice=%s", txType, nonce, gasPrice.String())

	err = t.nodeAPI.AddPendingTransaction(signedTx)
	if err != nil {
		if errors.Is(err, core.ErrNonceTooLow) {
			// nonce 太小：说明当前 nonce 已过期，需要递增 nonce 重试
			utils.SSCLogger().Warn().Uint64("Nonce", nonce).
				Err(err).Msgf("nonce too low, retry with nonce+1")
			return t.submitWithRetry(txBuilder, txType, originTxHash, nonce+1, gasPrice, retryCount, signer)
		} else if errors.Is(err, core.ErrNonceTooHigh) {
			// nonce 太大：说明中间有跳号，需要递减 nonce 重试
			utils.SSCLogger().Warn().Uint64("Nonce", nonce).
				Err(err).Msgf("nonce too high, retry with nonce-1")
			if nonce > 0 {
				return t.submitWithRetry(txBuilder, txType, originTxHash, nonce-1, gasPrice, retryCount, signer)
			}
			return errors.New("nonce too high but nonce is 0")
		} else if errors.Is(err, core.ErrUnderpriced) {
			nextGasPrice := t.increaseGasPrice(gasPrice)
			retryCount++

			if retryCount >= 3 {
				// 重试 3 次后放弃
				utils.SSCLogger().Error().Str("txHash", signedTx.Hash().Hex()).
					Err(err).
					Uint64("Nonce", nonce).
					Int("retry", retryCount).
					Str("originTxHash", originTxHash.Hex()).Msgf("failed to submit %s after %d retries", txType, retryCount)
				return err
			}

			// 使用更高 gasPrice 重试（同一 nonce，用于替换交易）
			return t.submitWithRetry(txBuilder, txType, originTxHash, nonce, nextGasPrice, retryCount, signer)
		} else {
			// 其他错误：记录日志并返回
			utils.SSCLogger().Error().Str("txHash", signedTx.Hash().Hex()).
				Err(err).
				Uint64("Nonce", nonce).
				Str("originTxHash", originTxHash.Hex()).Msgf("failed to submit %s, nonce=%d", txType, nonce)
			return err
		}
	}

	// 提交成功，记录 txHash 到 submittedTxHashes
	// BUG: 必须用 signedTx.Hash()（签名后），OnBlockCommitted 用 block.Transactions()[i].Hash()（也是签名后）匹配。
	// 若用 tx.Hash()（raw 未签名），区块内交易 hash 不同 → matched 恒 0 → inFlight 涨满 maxFlightLimit → worker 停止提交 → 交易冻结。
	t.submittedLock.Lock()
	t.submittedTxHashes[signedTx.Hash()] = struct{}{}
	t.submittedLock.Unlock()

	// 递增对应类型的 nonce
	t.lock.Lock()
	if t.getNonce(txType) == nonce {
		t.incNonce(txType)
	}
	t.lock.Unlock()

	utils.SSCLogger().Debug().Str("txHash", signedTx.Hash().Hex()).
		Uint64("Nonce", nonce).
		Str("originTxHash", originTxHash.Hex()).Msgf("successfully submitted %s, nonce=%d", txType, nonce)
	return nil
}

// increaseGasPrice 递增 gasPrice（RBF 机制，增加约 10%）
func (t *txSubmitter) increaseGasPrice(currentGasPrice *big.Int) *big.Int {
	// 增加 10%，最少增加 1 Gwei
	increase := new(big.Int).Div(currentGasPrice, big.NewInt(10))
	if increase.Cmp(big.NewInt(1e9)) < 0 {
		increase = big.NewInt(1e9)
	}
	return new(big.Int).Add(currentGasPrice, increase)
}

// OnBlockCommitted 块确认回调：匹配块中交易减 submittedTxHashes 和 inFlight
func (t *txSubmitter) OnBlockCommitted(block *types.Block) {
	matched := 0
	t.submittedLock.Lock()
	for _, tx := range block.Transactions() {
		txHash := tx.Hash()
		if _, ok := t.submittedTxHashes[txHash]; ok {
			delete(t.submittedTxHashes, txHash)
			matched++
		}
	}
	remain := len(t.submittedTxHashes)
	t.submittedLock.Unlock()

	t.inFlight.Add(-int32(matched))
	t.signal.Broadcast()

	utils.SSCLogger().Info().
		Uint64("blockNum", block.NumberU64()).
		Int("matched", matched).
		Int32("inFlight", t.inFlight.Load()).
		Int("submittedTxHashesRemain", remain).
		Msg("[TxSubmitter] OnBlockCommitted")
}

// GetNonce 获取指定交易类型的当前 nonce（用于调试/监控）
func (t *txSubmitter) GetNonce(txType TxType) uint64 {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.getNonce(txType)
}

// GetStats 获取完整统计信息
func (t *txSubmitter) GetStats() (pending, completed, failed int64, simNonce, crNonce uint64) {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.pendingCount.Load(), t.completedCount.Load(), t.failedCount.Load(), t.simNonce, t.crNonce
}

// SetNonce 设置指定交易类型的 nonce（用于初始化/恢复）
func (t *txSubmitter) SetNonce(txType TxType, nonce uint64) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if txType == CommitOrRollbackTx {
		t.crNonce = nonce
	} else {
		t.simNonce = nonce
	}
}

// GetPendingCount 获取队列中待处理的任务数
func (t *txSubmitter) GetPendingCount() int64 {
	return t.pendingCount.Load()
}

// GetCompletedCount 获取已完成的任务总数（成功 + 失败）
func (t *txSubmitter) GetCompletedCount() int64 {
	return t.completedCount.Load()
}

// GetFailedCount 获取失败的任务数
func (t *txSubmitter) GetFailedCount() int64 {
	return t.failedCount.Load()
}

// GetQueueSize 获取当前队列长度（与 GetPendingCount 相同，别名）
func (t *txSubmitter) GetQueueSize() int64 {
	return t.pendingCount.Load()
}
