package ssc

import (
	"context"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/pkg/errors"
)

func NewRetryScheduler(ctx context.Context, sscService *sscService, comm *Comm, selfShard uint32, tempLockView *TempLockView) *retryScheduler {
	rs := &retryScheduler{
		ctx:          ctx,
		tempLockView: tempLockView,
		sscService:   sscService,
		comm:         comm,
		selfShard:    selfShard,
		retryPool:    make(map[common.Hash]*api.RetryTx),
		staleTxs:     make(map[common.Hash]struct{}),
		signals:      make(map[common.Hash]map[int]map[uint32]*api.ReSimulationSignal),
		mu:           sync.RWMutex{},
	}
	go rs.cycle()
	return rs
}

// retryScheduler 实现
type retryScheduler struct {
	mu sync.RWMutex

	// 待重试交易池：txHash → RetryTx
	retryPool map[common.Hash]*api.RetryTx
	staleTxs  map[common.Hash]struct{}
	signals   map[common.Hash]map[int]map[uint32]*api.ReSimulationSignal // txHash -> simulationNum -> relatedShards -> signal

	// 依赖组件（通过接口解耦）
	ctx          context.Context
	tempLockView *TempLockView
	sscService   *sscService
	comm         *Comm
	selfShard    uint32
}

func (rs *retryScheduler) cycle() {
	for {
		<-time.After(time.Second * 3)
		func() {
			rs.mu.RLock()
			rs.mu.RUnlock()
			rs.printState()
		}()
	}
}

func (rs *retryScheduler) printState() {
	// print all txs need to retry
	txs2retry := make(map[common.Hash][]uint32)
	for hash, tx := range rs.retryPool {
		txs2retry[hash] = make([]uint32, 0)
		if signals, exists := rs.signals[hash]; exists {
			for fromShard, signal := range signals[tx.SimulationNum] {
				if signal.Ready {
					txs2retry[hash] = append(txs2retry[hash], fromShard)
				}
			}
		}
	}
	utils.SSCLogger().Debug().
		Int("num", len(txs2retry)).
		Interface("txs2readyShard", txs2retry).
		Msg("retry txs")
}

func (rs *retryScheduler) CallForRetry(tx *api.RetryTx) {
	if !rs.sscService.IsLeader() {
		return
	}
	utils.SSCLogger().Debug().
		Str("txHash", tx.TxHash.Hex()).
		Interface("relatedShards", tx.RelatedShards).
		Msg("call for retry")
	for _, shard := range tx.RelatedShards {
		leader := rs.sscService.GetLeader(tx.Epoch, shard)
		err := rs.comm.Call(rs.ctx, nil, leader, api.Method_AddRetryTx, tx)
		if err != nil {
			utils.SSCLogger().Error().Err(err).Msg("call for retry failed")
			return
		}
	}
}

func (rs *retryScheduler) AddToRetry(tx *api.RetryTx) {
	if !rs.sscService.IsLeader() {
		return
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if tx == nil || tx.TxHash == (common.Hash{}) {
		return
	}

	// 防重复
	if _, exists := rs.retryPool[tx.TxHash]; exists {
		utils.SSCLogger().Debug().Str("tx", tx.TxHash.Hex()).Msg("retry tx already exists")
		return
	}

	// 立即检查是否已 stale（如刚收到就过期）
	if rs.isStale(tx) {
		utils.SSCLogger().Debug().Str("tx", tx.TxHash.Hex()).Msg("skip adding stale tx to retry pool")
		return
	}

	rs.retryPool[tx.TxHash] = tx
	utils.SSCLogger().Debug().
		Str("txHash", tx.TxHash.Hex()).
		Interface("relatedShards", tx.RelatedShards).
		Int("reads", len(tx.ReadSet)).
		Int("writes", len(tx.WriteSet)).
		Int("poolSize", len(rs.retryPool)).
		Msg("added to retry pool")
}

// OnBlockCommitted 在区块提交后调用，尝试提升重试池中的交易
func (rs *retryScheduler) OnBlockCommitted(block *types.Block) {
	if !rs.sscService.IsLeader() {
		return
	}
	// 首先处理临时锁
	rs.tempLockView.OnBlockCommitted(block)

	rs.mu.Lock()
	defer rs.mu.Unlock()

	staleNum := len(rs.staleTxs)
	// 清理 stale 交易
	for txHash, _ := range rs.staleTxs {
		delete(rs.retryPool, txHash)
	}
	rs.staleTxs = make(map[common.Hash]struct{})

	shard2SignalReadyNum := make(map[uint32]int)
	shard2RetrySignals := make(map[uint32]*api.ReSimulationSignals)
	for i := uint32(0); i < rs.sscService.ShardNum(); i++ {
		shard2SignalReadyNum[i] = 0
		shard2RetrySignals[i] = &api.ReSimulationSignals{
			OriginShard: i,
			FromShard:   rs.selfShard,
			Signals:     make([]*api.ReSimulationSignal, 0),
		}
	}

	// 判断交易是否可以重新模拟，并加入到signals中
	for txHash, tx := range rs.retryPool {
		signal := &api.ReSimulationSignal{
			TxHash:        tx.TxHash,
			Epoch:         tx.Epoch,
			SimulationNum: tx.SimulationNum,
			Condition:     tx.Condition,
		}
		if rs.tempLockView.CanLock(txHash, tx.ReadSet, tx.WriteSet) {
			signal.Ready = true
			shard2SignalReadyNum[tx.OriginShardID]++
		} else {
			signal.Ready = false
		}
		signals := shard2RetrySignals[tx.OriginShardID]
		signals.Signals = append(signals.Signals, signal)
	}

	// 提交 promoted 交易到下一轮模拟
	for _, signals := range shard2RetrySignals {
		if len(signals.Signals) > 0 {
			utils.SSCLogger().Debug().
				Int("num", len(signals.Signals)).
				Uint32("shard", signals.OriginShard).
				Msg("promoted txs to next round")
			go rs.sendReSimulationSignals(signals)
		}
	}

	utils.SSCLogger().Debug().
		Int("poolSize", len(rs.retryPool)).
		Uint64("blockNum", block.NumberU64()).
		Int("staleNum", staleNum).
		Int("readyNum", shard2SignalReadyNum[rs.selfShard]).
		Msg("retryScheduler onBlockCommitted")
}

func (rs *retryScheduler) sendReSimulationSignals(signals *api.ReSimulationSignals) {
	leader := rs.sscService.CurrentLeader(signals.OriginShard)

	err := rs.comm.Call(rs.ctx, nil, leader, api.Method_SignalReSimulation, signals)
	if err != nil {
		utils.SSCLogger().Error().Msg("send signals resimulation failed")
	}
}
func (rs *retryScheduler) HandleReSimulationSignal(signals *api.ReSimulationSignals) error {
	if !rs.sscService.IsLeader() {
		return nil
	}
	if rs.selfShard != signals.FromShard {
		utils.SSCLogger().Error().Msgf("handle signal from other shard, origin=%d, self=%d", signals.FromShard, rs.selfShard)
		return errors.New("handle signal from other shard")
	}

	ready := 0
	for _, signal := range signals.Signals {
		if signal.Ready {
			ready++
		}
	}
	utils.SSCLogger().Debug().
		Int("num", len(signals.Signals)).
		Uint32("shard", signals.OriginShard).
		Int("ready", ready).
		Msg("received signals from leader")

	rs.mu.Lock()
	defer rs.mu.Unlock()

	for _, signal := range signals.Signals {
		txHash := signal.TxHash
		rs.setSignal(signals.FromShard, signal)
		retryTx := rs.retryPool[txHash]
		readyCnt := 0
		cachedSignals := rs.signals[txHash][retryTx.SimulationNum]
		for _, s := range cachedSignals {
			if s.Ready {
				readyCnt++
			}
		}
		utils.SSCLogger().Debug().
			Interface("signal", signal).
			Interface("retryTx", retryTx).
			Interface("cachedSignals", cachedSignals).
			Int("readyCnt", readyCnt).
			Str("tx", txHash.Hex()).
			Msg("check signals")
		if readyCnt == len(retryTx.RelatedShards) {
			return rs.tryToReSimulation(retryTx)
		}
	}

	return nil
}

func (rs *retryScheduler) tryToReSimulation(retryTx *api.RetryTx) error {
	txHash := retryTx.TxHash

	resps := make(map[uint32]*api.RetryCommitResp)

	wg := sync.WaitGroup{}
	wg.Add(len(retryTx.RelatedShards))
	lock := sync.Mutex{}
	for _, shard := range retryTx.RelatedShards {
		go func(shard uint32) {
			defer wg.Done()
			resp := new(api.RetryCommitResp)
			err := rs.comm.Call(rs.ctx, resp, rs.sscService.GetLeader(retryTx.Epoch, shard), api.Method_RetryCommit, txHash)
			if err != nil {
				utils.SSCLogger().Error().Err(err).Msg("retry commit failed")
				return
			}
			lock.Lock()
			defer lock.Unlock()
			resps[shard] = resp
		}(shard)
	}
	wg.Wait()

	success := true
	if len(resps) != len(retryTx.RelatedShards) {
		success = false
	}
	for _, resp := range resps {
		if !resp.Locked {
			success = false
			break
		}
	}
	if success {
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msg("retry commit success")
		go rs.sscService.startReSimulation(retryTx.TxHash, retryTx.SimulationNum)
		delete(rs.signals, txHash)
		delete(rs.retryPool, txHash)
	} else {
		for shardId, resp := range resps {
			// 如果失败，则将临时上锁的交易解锁
			if resp.Locked {
				go func(shardId uint32) {
					err := rs.comm.Call(rs.ctx, nil, rs.sscService.GetLeader(retryTx.Epoch, shardId), api.Method_RetryCancel, resp.TxHash)
					if err != nil {
						utils.SSCLogger().Error().Err(err).Msg("retry cancel failed")
						return
					}
				}(shardId)
			} else {
				rs.mu.Lock()
				if rs.signals[txHash] != nil &&
					rs.signals[txHash][retryTx.SimulationNum] != nil &&
					rs.signals[txHash][retryTx.SimulationNum][shardId] != nil {
					rs.signals[txHash][retryTx.SimulationNum][shardId].Ready = false
				}
				rs.mu.Unlock()
			}
		}
	}
	return nil
}

func (rs *retryScheduler) setSignal(fromShard uint32, signal *api.ReSimulationSignal) {
	m := rs.signals[signal.TxHash]
	if m == nil {
		m = make(map[int]map[uint32]*api.ReSimulationSignal)
		rs.signals[signal.TxHash] = m
	}
	m2 := m[signal.SimulationNum]
	if m2 == nil {
		m2 = make(map[uint32]*api.ReSimulationSignal)
		m[signal.SimulationNum] = m2
	}
	m2[fromShard] = signal
	utils.SSCLogger().Debug().
		Str("tx", signal.TxHash.Hex()).
		Int("simulationNum", signal.SimulationNum).
		Uint32("fromShard", fromShard).
		Bool("ready", signal.Ready).
		Msg("set signal")
}

func (rs *retryScheduler) isStale(tx *api.RetryTx) bool {
	_, exists := rs.staleTxs[tx.TxHash]
	return exists
}

func (rs *retryScheduler) StaleTx(txHash common.Hash) {
	if !rs.sscService.IsLeader() {
		return
	}

	rs.tempLockView.GarbageCollect(txHash)

	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.staleTxs[txHash] = struct{}{}
}

func (rs *retryScheduler) RetryCommit(txHash common.Hash) bool {
	if !rs.sscService.IsLeader() {
		return false
	}
	retryTx := rs.retryPool[txHash]
	if retryTx == nil {
		return false
	}
	locked := rs.tempLockView.TryLock(txHash, retryTx.ReadSet, retryTx.WriteSet)
	if !locked {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msg("retryCommit failed: try lock failed")
		return false
	}
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msg("retryCommit")
	return true
}

func (rs *retryScheduler) RetryCancel(txHash common.Hash) {
	if !rs.sscService.IsLeader() {
		return
	}
	rs.tempLockView.GarbageCollect(txHash)
}
