package ssc

import (
	"context"
	"encoding/json"
	"math/big"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/core"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/core/vm"
	"github.com/harmony-one/harmony/hmy"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/harmony-one/harmony/ssc/lm"
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

// txTypeToChannelIndex 将交易类型映射到对应的 channel 索引（0=最高优先级）
func txTypeToChannelIndex(txType TxType) int {
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
		return 4 // 未知类型放入最低优先级
	}
}

// txTask 表示待提交的交易任务
type txTask struct {
	txBuilder    func(nonce uint64, gasPrice *big.Int) *types.Transaction
	txType       TxType
	originTxHash common.Hash
	done         chan error
}

// priorityQueue 优先级队列，包含 5 个独立 channel（索引 0=最高优先级）
type priorityQueue struct {
	queues [5]chan *txTask
}

func NewTxSubmitter(selfShard uint32, txSigner api.TxSigner, nodeAPI hmy.NodeAPI, config *api.Config) api.TxSubmitter {
	t := &txSubmitter{
		lock:      lm.NewMutex(),
		selfShard: selfShard,
		txSigner:  txSigner,
		nodeAPI:   nodeAPI,
		config:    config,
		queue: priorityQueue{
			queues: [5]chan *txTask{
				make(chan *txTask, 256), // 索引 0: NewEpochTx (优先级 5)
				make(chan *txTask, 256), // 索引 1: UploadOpinionsTx (优先级 4)
				make(chan *txTask, 256), // 索引 2: CommitOrRollbackTx (优先级 3)
				make(chan *txTask, 256), // 索引 3: SimulationTx (优先级 2)
				make(chan *txTask, 256), // 索引 4: EmptyTx (优先级 1)
			},
		},
		ctx: context.Background(),
	}
	// 启动后台提交协程
	go t.processQueue()
	return t
}

type txSubmitter struct {
	lock      lm.Mutex
	selfShard uint32
	txSigner  api.TxSigner
	nonce     uint64
	nodeAPI   hmy.NodeAPI
	config    *api.Config
	queue     priorityQueue // 优先级队列（5 个独立 channel）
	ctx       context.Context

	// 监控计数器
	pendingCount   atomic.Int64 // 队列中待处理的任务数
	completedCount atomic.Int64 // 已完成的任务总数（成功 + 失败）
	failedCount    atomic.Int64 // 失败的任务数
}

func (t *txSubmitter) SubmitSimulationTx(simulation *api.CXTSimulation) error {
	task := &txTask{
		txBuilder: func(nonce uint64, gasPrice *big.Int) *types.Transaction {
			input, _ := json.Marshal(simulation)
			return types.NewCrossShardTransaction(nonce, &vm.SimulationCommitAddr, t.selfShard, t.selfShard, big.NewInt(0), t.config.SimulationCommitGasLimit, gasPrice, input)
		},
		txType:       SimulationTx,
		originTxHash: simulation.TxHash,
		done:         make(chan error, 1),
	}
	utils.Logger().Info().
		Str("txHash", simulation.TxHash.Hex()).
		Uint64("Nonce", t.GetNonce()).
		Int64("pendingCnt", t.pendingCount.Load()).
		Str("originTxHash", simulation.TxHash.Hex()).
		Msg("[SimulationTx] submitting")
	t.pendingCount.Add(1)
	t.queue.queues[txTypeToChannelIndex(SimulationTx)] <- task
	err := <-task.done
	if err != nil {
		t.failedCount.Add(1)
	}
	t.completedCount.Add(1)
	t.pendingCount.Add(-1)
	return err
}

func (t *txSubmitter) SubmitCommitOrRollbackTx(proof *api.CXTCommitProof) error {
	task := &txTask{
		txBuilder: func(nonce uint64, gasPrice *big.Int) *types.Transaction {
			input, _ := json.Marshal(proof)
			return types.NewCrossShardTransaction(nonce, &vm.CxtCommitOrRollbackAddr, t.selfShard, t.selfShard, big.NewInt(0), t.config.SimulationCommitGasLimit, gasPrice, input)
		},
		txType:       CommitOrRollbackTx,
		originTxHash: proof.TxHash,
		done:         make(chan error, 1),
	}
	t.pendingCount.Add(1)
	t.queue.queues[txTypeToChannelIndex(CommitOrRollbackTx)] <- task
	go func() {
		err := <-task.done
		if err != nil {
			t.failedCount.Add(1)
		}
	}()
	t.completedCount.Add(1)
	t.pendingCount.Add(-1)
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
		done:         make(chan error, 1),
	}
	t.pendingCount.Add(1)
	t.queue.queues[txTypeToChannelIndex(EmptyTx)] <- task
	go func() {
		err := <-task.done
		if err != nil {
			t.failedCount.Add(1)
		}
	}()
	t.completedCount.Add(1)
	t.pendingCount.Add(-1)
	return nil
}

func (t *txSubmitter) SubmitNewEpoch(newEpoch *api.NewEpoch) error {
	task := &txTask{
		txBuilder: func(nonce uint64, gasPrice *big.Int) *types.Transaction {
			input, _ := json.Marshal(newEpoch)
			return types.NewCrossShardTransaction(nonce, &vm.NewEpochAddr, t.selfShard, t.selfShard, big.NewInt(0), t.config.SimulationCommitGasLimit, gasPrice, input)
		},
		txType:       NewEpochTx,
		originTxHash: common.Hash{},
		done:         make(chan error, 1),
	}
	t.pendingCount.Add(1)
	t.queue.queues[txTypeToChannelIndex(NewEpochTx)] <- task
	go func() {
		err := <-task.done
		if err != nil {
			t.failedCount.Add(1)
		}
	}()
	t.completedCount.Add(1)
	t.pendingCount.Add(-1)
	return nil
}

func (t *txSubmitter) SubmitUploadOpinions(uploadOpinions *api.SelfOpinions) error {
	task := &txTask{
		txBuilder: func(nonce uint64, gasPrice *big.Int) *types.Transaction {
			input, _ := json.Marshal(uploadOpinions)
			return types.NewCrossShardTransaction(nonce, &vm.SLOpinionAddr, t.selfShard, t.selfShard, big.NewInt(0), t.config.SimulationCommitGasLimit, gasPrice, input)
		},
		txType:       UploadOpinionsTx,
		originTxHash: common.Hash{},
		done:         make(chan error, 1),
	}
	t.pendingCount.Add(1)
	t.queue.queues[txTypeToChannelIndex(UploadOpinionsTx)] <- task
	go func() {
		err := <-task.done
		if err != nil {
			t.failedCount.Add(1)
		}
	}()
	t.completedCount.Add(1)
	t.pendingCount.Add(-1)
	return nil
}

// processQueue 后台协程，按优先级处理交易队列
// 总是优先处理高优先级 channel 中的任务
func (t *txSubmitter) processQueue() {
	for {
		// 按优先级从高到低轮询（索引 0=最高优先级）
		var task *txTask
		for i := 0; i < 5; i++ {
			select {
			case task = <-t.queue.queues[i]:
				goto process
			default:
				// 当前 channel 为空，继续检查下一个
			}
		}
		// 所有 channel 都为空，阻塞等待任意 channel
		select {
		case task = <-t.queue.queues[0]:
		case task = <-t.queue.queues[1]:
		case task = <-t.queue.queues[2]:
		case task = <-t.queue.queues[3]:
		case task = <-t.queue.queues[4]:
		}
	process:
		if task != nil {
			t.processTask(task)
		}
	}
}

// processTask 处理单个交易任务，负责 nonce 分配和重试
func (t *txSubmitter) processTask(task *txTask) {
	// 获取当前 nonce（只在成功提交后才自增）
	t.lock.Lock()
	currentNonce := t.nonce
	gasPrice := new(big.Int).Set(t.config.SimulationCommitGasPrice)
	t.lock.Unlock()

	err := t.submitWithRetry(task.txBuilder, task.txType, task.originTxHash, currentNonce, gasPrice, 0)
	task.done <- err
}

// submitWithRetry 处理交易提交重试，支持 gasPrice 递增和 nonce 调整
func (t *txSubmitter) submitWithRetry(
	txBuilder func(nonce uint64, gasPrice *big.Int) *types.Transaction,
	txType TxType,
	originTxHash common.Hash,
	nonce uint64,
	gasPrice *big.Int,
	retryCount int,
) error {
	tx := txBuilder(nonce, gasPrice)
	txHash := tx.Hash()

	signedTx, err := t.txSigner.Sign(tx)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
			Err(err).
			Uint64("Nonce", nonce).
			Str("originTxHash", originTxHash.Hex()).Msgf("sign tx failed, nonce=%d", nonce)
		return err
	}

	utils.SSCLogger().Info().Str("txHash", signedTx.Hash().Hex()).
		Uint64("Nonce", nonce).
		Str("originTxHash", originTxHash.Hex()).
		Int("retry", retryCount).Msgf("submitting %s, nonce=%d, gasPrice=%s", txType, nonce, gasPrice.String())

	err = t.nodeAPI.AddPendingTransaction(signedTx)
	if err != nil {
		if errors.Is(err, core.ErrNonceTooLow) {
			// nonce 太小：说明当前 nonce 已过期，需要递增 nonce 重试
			utils.SSCLogger().Warn().Uint64("Nonce", nonce).
				Err(err).Msgf("nonce too low, retry with nonce+1")
			return t.submitWithRetry(txBuilder, txType, originTxHash, nonce+1, gasPrice, retryCount)
		} else if errors.Is(err, core.ErrNonceTooHigh) {
			// nonce 太大：说明中间有跳号，需要递减 nonce 重试
			utils.SSCLogger().Warn().Uint64("Nonce", nonce).
				Err(err).Msgf("nonce too high, retry with nonce-1")
			if nonce > 0 {
				return t.submitWithRetry(txBuilder, txType, originTxHash, nonce-1, gasPrice, retryCount)
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

			// utils.SSCLogger().Warn().Uint64("Nonce", nonce).
			//	Err(err).
			//	Str("oldGasPrice", gasPrice.String()).
			//	Str("newGasPrice", nextGasPrice.String()).
			//	Int("retry", retryCount).Msgf("retrying %s with higher gasPrice", txType)

			// 使用更高 gasPrice 重试（同一 nonce，用于替换交易）
			return t.submitWithRetry(txBuilder, txType, originTxHash, nonce, nextGasPrice, retryCount)
		} else {
			// 其他错误：记录日志并返回
			utils.SSCLogger().Error().Str("txHash", signedTx.Hash().Hex()).
				Err(err).
				Uint64("Nonce", nonce).
				Str("originTxHash", originTxHash.Hex()).Msgf("failed to submit %s, nonce=%d", txType, nonce)
			return err
		}
	}

	// 提交成功，递增 nonce
	t.lock.Lock()
	if t.nonce == nonce {
		t.nonce = nonce + 1
	}
	t.lock.Unlock()

	utils.SSCLogger().Info().Str("txHash", signedTx.Hash().Hex()).
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

// GetNonce 获取当前 nonce（用于调试/监控）
func (t *txSubmitter) GetNonce() uint64 {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.nonce
}

// SetNonce 设置 nonce（用于初始化/恢复）
func (t *txSubmitter) SetNonce(nonce uint64) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.nonce = nonce
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

// GetStats 获取完整统计信息
func (t *txSubmitter) GetStats() (pending, completed, failed int64, nonce uint64) {
	return t.pendingCount.Load(), t.completedCount.Load(), t.failedCount.Load(), t.GetNonce()
}
