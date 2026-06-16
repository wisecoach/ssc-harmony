package ssc

import (
	"bytes"
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

	// NOTE: hotKeySet, updateHotKeys, isHotKey 等已废弃
	// 由 chainNextSim（全量 key 依赖检查）替代

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
		<-time.After(time.Second * 60)
		// func() {
		// 	rs.mu.RLock()
		// 	rs.printState()
		// 	rs.mu.RUnlock()
		// }()
	}
}

func (rs *retryScheduler) printState() {
	// print all txs need to retry
	txs2retry := make(map[common.Hash][]bool)
	for hash, tx := range rs.retryPool {
		txs2retry[hash] = make([]bool, 0)
		if signals, exists := rs.signals[hash]; exists {
			for _, signal := range signals[tx.SimulationNum] {
				txs2retry[hash] = append(txs2retry[hash], signal.Ready)
			}
		}
	}
	utils.SSCLogger().Info().
		Int("num", len(txs2retry)).
		Interface("txs2readyShard", txs2retry).
		Msg("retry txs")
}

func (rs *retryScheduler) CallForRetry(tx *api.RetryTx) {
	if !rs.sscService.IsLeader(tx.Epochs[rs.selfShard]) {
		return
	}
	utils.SSCLogger().Info().
		Str("txHash", tx.TxHash.Hex()).
		Uint32("originShardID", tx.OriginShardID).
		Interface("relatedShards", tx.RelatedShards).
		Msg("call for retry")
	for _, shard := range tx.RelatedShards {
		leader := rs.sscService.GetLeader(tx.Epochs[shard], shard)
		err := rs.comm.Call(rs.ctx, nil, leader, api.Method_AddRetryTx, tx)
		if err != nil {
			utils.SSCLogger().Error().Err(err).Msg("call for retry failed")
			return
		}
	}
}

func (rs *retryScheduler) AddToRetry(tx *api.RetryTx) {
	if tx == nil || tx.TxHash == (common.Hash{}) {
		return
	}
	if !rs.sscService.IsLeader(tx.Epochs[rs.selfShard]) {
		return
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()

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
	rs.sscService.stats.RetryAddCount.Add(1)
	rs.sscService.stats.sampleRetryPool(len(rs.retryPool))
	utils.SSCLogger().Info().
		Str("txHash", tx.TxHash.Hex()).
		Interface("relatedShards", tx.RelatedShards).
		Int("reads", len(tx.ReadSet)).
		Int("writes", len(tx.WriteSet)).
		Int("poolSize", len(rs.retryPool)).
		Msg("added to retry pool")
}

// OnBlockCommitted 在区块提交后调用，尝试提升重试池中的交易
func (rs *retryScheduler) OnBlockCommitted(block *types.Block) {
	// 首先处理临时锁
	rs.tempLockView.OnBlockCommitted(block)

	// 定期更新热键集合
	// OLD: updateHotKeys 已废弃，由 chainNextSim 替代
	// rs.updateHotKeys()

	rs.mu.Lock()
	defer rs.mu.Unlock()

	staleNum := len(rs.staleTxs)
	// 清理 stale 交易
	for txHash, _ := range rs.staleTxs {
		delete(rs.retryPool, txHash)
	}
	rs.staleTxs = make(map[common.Hash]struct{})

	rs.sscService.stats.sampleRetryPool(len(rs.retryPool))

	shard2SignalReadyNum := make(map[uint32]int)
	shard2Epoch2RetrySignals := make(map[uint32]map[api.Epoch]*api.ReSimulationSignals)
	for i := uint32(0); i < rs.sscService.ShardNum(); i++ {
		shard2SignalReadyNum[i] = 0
		shard2Epoch2RetrySignals[i] = make(map[api.Epoch]*api.ReSimulationSignals)
	}

	// 判断交易是否可以重新模拟，并加入到signals中
	for txHash, tx := range rs.retryPool {
		signal := &api.ReSimulationSignal{
			TxHash:        tx.TxHash,
			Epoch:         tx.Epochs[tx.OriginShardID],
			SimulationNum: tx.SimulationNum,
			Condition:     tx.Condition,
		}
		if rs.tempLockView.CanLock(txHash, tx.ReadSet, tx.WriteSet) {
			signal.Ready = true
			rs.sscService.stats.RetryReadySignal.Add(1)
			shard2SignalReadyNum[tx.OriginShardID]++
		} else {
			signal.Ready = false
			rs.sscService.stats.RetryNotReadySignal.Add(1)
		}
		signals := shard2Epoch2RetrySignals[tx.OriginShardID][signal.Epoch]
		if signals == nil {
			signals = &api.ReSimulationSignals{
				OriginShard: tx.OriginShardID,
				FromShard:   rs.selfShard,
				Epoch:       signal.Epoch,
				Signals:     make([]*api.ReSimulationSignal, 0),
			}
			shard2Epoch2RetrySignals[tx.OriginShardID][signal.Epoch] = signals
		}
		signals.Signals = append(signals.Signals, signal)
	}

	// 提交 promoted 交易到下一轮模拟 — 暂不启用正常 retry 路径
	// 由 HotKey chain 接管 retry 信号的触发
	for _, epoch2Signals := range shard2Epoch2RetrySignals {
		for _, signals := range epoch2Signals {
			if len(signals.Signals) > 0 {
				utils.SSCLogger().Debug().
					Int("num", len(signals.Signals)).
					Uint32("shard", signals.OriginShard).
					Msg("normal retry signals blocked (hot key chain only)")
			}
		}
	}

	utils.SSCLogger().Info().
		Int("poolSize", len(rs.retryPool)).
		Uint64("blockNum", block.NumberU64()).
		Int("staleNum", staleNum).
		Int("readyNum", shard2SignalReadyNum[rs.selfShard]).
		Msg("retryScheduler onBlockCommitted")
}

func (rs *retryScheduler) sendReSimulationSignals(signals *api.ReSimulationSignals) {
	leader := rs.sscService.GetLeader(signals.Epoch, signals.OriginShard)

	err := rs.comm.Call(rs.ctx, nil, leader, api.Method_SignalReSimulation, signals)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("send signals resimulation failed")
	}
}

// OLD: updateHotKeys — 已废弃，由 chainNextSim 替代
/*
func (rs *retryScheduler) updateHotKeys() { ... }
func (rs *retryScheduler) isHotKey(key api.LockKey) bool { ... }
*/

// chainNextSim 在 SimTx 提交后调用。
// 分析 SimTx 的 WriteSet，找到依赖这些 key 的 retry tx，
// 发送带 ChainPatch 的 HandleChainSimSignal。
func (rs *retryScheduler) chainNextSim(upstreamTxHash common.Hash, writeSet *api.RWSet) {
	if writeSet == nil || len(writeSet.WriteState.State) == 0 {
		utils.SSCLogger().Debug().Str("txHash", upstreamTxHash.Hex()).
			Msg("chainNextSim: empty writeSet, no downstream possible")
		return
	}

	utils.SSCLogger().Info().Str("txHash", upstreamTxHash.Hex()).
		Int("writeAddrCount", len(writeSet.WriteState.State)).
		Msg("chainNextSim: scanning retry pool for downstream dependencies")

	// 构建加速查表：LockKey → addr:key
	keySet := make(map[api.LockKey]struct{})
	for addr, state := range writeSet.WriteState.State {
		for key := range state {
			keySet[api.FormKey(addr, key)] = struct{}{}
		}
	}

	// 1. RLock 下收集所有匹配的 retry tx（不阻塞，不做 RPC）
	type matchedTx struct {
		txHash  common.Hash
		retryTx *api.RetryTx
	}
	var matched []matchedTx

	rs.mu.RLock()
	for txHash, retryTx := range rs.retryPool {
		if bytes.Equal(retryTx.TxHash.Bytes(), upstreamTxHash.Bytes()) {
			continue // 跳过自己
		}
		if dependsOn(retryTx, keySet) {
			matched = append(matched, matchedTx{txHash: txHash, retryTx: retryTx})
		}
	}
	rs.mu.RUnlock()

	if len(matched) == 0 {
		utils.SSCLogger().Debug().Str("txHash", upstreamTxHash.Hex()).
			Msg("chainNextSim: no downstream dependencies found")
		return
	}

	utils.SSCLogger().Info().Str("txHash", upstreamTxHash.Hex()).
		Int("downstreamCount", len(matched)).
		Msg("chainNextSim: found downstream dependencies")

	// 2. 逐个发送 chain signal（不在 RLock 内做 RPC）
	for _, m := range matched {
		rs.sendChainSignal(m.txHash, m.retryTx, writeSet)
	}
}

// dependsOn 判断 retryTx 是否依赖 patch keySet 中的任意 key。
// 依赖定义：retryTx.ReadSet ∪ retryTx.WriteSet ∩ patchKeySet ≠ ∅
func dependsOn(retryTx *api.RetryTx, keySet map[api.LockKey]struct{}) bool {
	for _, lockKey := range retryTx.ReadSet {
		if _, exists := keySet[lockKey]; exists {
			return true
		}
	}
	for _, lockKey := range retryTx.WriteSet {
		if _, exists := keySet[lockKey]; exists {
			return true
		}
	}
	return false
}

// sendChainSignal 发送带 ChainPatch 的 chain signal 到 retry tx 的 origin shard。
func (rs *retryScheduler) sendChainSignal(txHash common.Hash, retryTx *api.RetryTx, writeSet *api.RWSet) {
	signal := &api.ReSimulationSignal{
		TxHash:        retryTx.TxHash,
		Epoch:         retryTx.Epochs[retryTx.OriginShardID],
		SimulationNum: retryTx.SimulationNum,
		Condition:     retryTx.Condition,
		Ready:         true,
		FromShard:     rs.selfShard,
		ChainPatch:    writeSet,
	}

	originLeader := rs.sscService.GetLeader(retryTx.Epochs[retryTx.OriginShardID], retryTx.OriginShardID)
	err := rs.comm.Call(rs.ctx, nil, originLeader, api.Method_HandleChainSimSignal, signal)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Str("txHash", txHash.Hex()).
			Uint32("originShard", retryTx.OriginShardID).
			Msg("sendChainSignal: failed to send signal")
	} else {
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
			Uint32("originShard", retryTx.OriginShardID).
			Int("patchAddrCount", len(writeSet.WriteState.State)).
			Msg("sendChainSignal: sent chain signal")
	}
}

// HandleChainSimSignal 接收来自 SimTx 提交 shard 的 chain signal，
// 在 origin shard 存储 ChainPatch 后通过 signal aggregation 触发 tryToReSimulation。
func (rs *retryScheduler) HandleChainSimSignal(signal *api.ReSimulationSignal) {
	utils.SSCLogger().Info().Str("txHash", signal.TxHash.Hex()).
		Bool("hasPatch", signal.ChainPatch != nil).
		Msg("HandleChainSimSignal: received")

	if signal.ChainPatch != nil {
		rs.sscService.stateLock.Lock()
		state, err := rs.sscService.getState(signal.TxHash)
		if err == nil && state != nil {
			state.ChainPatch = signal.ChainPatch
			utils.SSCLogger().Info().Str("txHash", signal.TxHash.Hex()).
				Int("patchSize", len(signal.ChainPatch.WriteState.State)).
				Msg("HandleChainSimSignal: stored ChainPatch")
		}
		rs.sscService.stateLock.Unlock()
	}

	// 收到一个 chain signal 就直接 tryToReSimulation
	// chain 路径的 signal 是强信号，不需要等所有 shard 的信号聚合
	rs.mu.RLock()
	retryTx, exists := rs.retryPool[signal.TxHash]
	rs.mu.RUnlock()
	if exists && retryTx != nil {
		go rs.tryToReSimulation(retryTx)
	} else {
		utils.SSCLogger().Warn().Str("txHash", signal.TxHash.Hex()).
			Msg("HandleChainSimSignal: retry tx not found in pool")
	}
}

// OLD: chainHotKeyCR — replaced by chainNextSim
// 原有逻辑基于 HotKey + CR 提交触发，已迁移到 chainNextSim (SimTx 提交 + 全量依赖)
/*
func (rs *retryScheduler) chainHotKeyCR(crTxHash common.Hash, writeSet *api.RWSet) {
	...
}

func (rs *retryScheduler) HandleHotKeyRetrySignal(signal *api.ReSimulationSignal) {
	...
}
*/
func (rs *retryScheduler) HandleReSimulationSignal(signals *api.ReSimulationSignals) error {
	if !rs.sscService.IsLeader(signals.Epoch) {
		return nil
	}
	if rs.selfShard != signals.OriginShard {
		utils.SSCLogger().Error().Msgf("handle signal from other shard, origin=%d, self=%d", signals.FromShard, rs.selfShard)
		return errors.New("h" +
			"andle signal from other shard")
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
		readyCnt := 0
		cachedSignals := rs.signals[txHash][signal.SimulationNum]
		for _, s := range cachedSignals {
			if s.Ready {
				readyCnt++
			}
		}
		retryTx := rs.retryPool[txHash]
		if retryTx != nil {
			if readyCnt == len(retryTx.RelatedShards) {
				go rs.tryToReSimulation(retryTx)
			}
		}
	}

	return nil
}

func (rs *retryScheduler) tryToReSimulation(retryTx *api.RetryTx) {
	txHash := retryTx.TxHash

	resps := make(map[uint32]*api.RetryCommitResp)

	wg := sync.WaitGroup{}
	wg.Add(len(retryTx.RelatedShards))
	lock := sync.Mutex{}
	for _, shard := range retryTx.RelatedShards {
		go func(shard uint32) {
			defer wg.Done()
			resp := new(api.RetryCommitResp)
			err := rs.comm.Call(rs.ctx, resp, rs.sscService.GetLeader(retryTx.Epochs[shard], shard), api.Method_RetryCommit, txHash)
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
		rs.sscService.stats.RetrySuccessCount.Add(1)
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msg("retry commit success")
		go rs.sscService.startReSimulation(retryTx.TxHash, retryTx.SimulationNum)
		rs.mu.Lock()
		delete(rs.signals, txHash)
		delete(rs.retryPool, txHash)
		rs.mu.Unlock()
	} else {
		rs.sscService.stats.RetryFailCount.Add(1)
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msg("retry commit failed")
		for shardId, resp := range resps {
			// 如果失败，则将临时上锁的交易解锁
			if resp.Locked {
				go func(shardId uint32) {
					err := rs.comm.Call(rs.ctx, nil, rs.sscService.GetLeader(retryTx.Epochs[shardId], shardId), api.Method_RetryCancel, resp.TxHash)
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
	rs.tempLockView.GarbageCollect(txHash)

	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.staleTxs[txHash] = struct{}{}
}

func (rs *retryScheduler) RetryCommit(txHash common.Hash) bool {
	rs.mu.RLock()
	retryTx := rs.retryPool[txHash]
	if retryTx == nil {
		rs.mu.RUnlock()
		return false
	}
	rs.mu.RUnlock()
	if !rs.sscService.IsLeader(retryTx.Epochs[rs.selfShard]) {
		return false
	}
	locked := rs.tempLockView.TryLock(txHash, retryTx.ReadSet, retryTx.WriteSet)
	if !locked {
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msg("retryCommit failed: try lock failed")
		return false
	}
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msg("retryCommit")
	return true
}

func (rs *retryScheduler) RetryCancel(txHash common.Hash) {
	rs.tempLockView.GarbageCollect(txHash)
}
