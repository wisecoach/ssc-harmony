package ssc

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/core"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/pkg/errors"
)

// ============================================================================
// 链式 Retry 全路径统计计数器
// 用于分析 retry 漏斗瓶颈：哪些环节失败最多，Wound-Wait 实际影响面。
// 通过 grep "=== CHAIN_RETRY_STATS ===" 一次性拉取所有 shard 的汇总。
// ============================================================================
var chainRetryStats struct {
	// chainNextSim 阶段
	SigChainNextScan        atomic.Int64 // chainNextSim 被调用的次数
	SigChainNoDownstream    atomic.Int64 // 无下游依赖
	SigChainSelected        atomic.Int64 // 选中并发送 chain signal 的 retryTx 数
	SigChainReservedSkipped atomic.Int64 // reservation 过滤跳过的次数

	// HandleRetrySignal 阶段
	SigRetrySignalReceived  atomic.Int64 // 收到 chain signal
	SigRetrySignalTxMissing atomic.Int64 // signal 对应的 retryTx 不在 pool 中

	// tryToReSimulation 阶段
	SigTryReSimStarted    atomic.Int64 // tryToReSimulation 被调用
	SigRetryCommitCall    atomic.Int64 // RetryCommit RPC 调用数
	SigRetryCommitFail    atomic.Int64 // RetryCommit 返回 locked=false
	SigRetryCommitRpcErr  atomic.Int64 // RetryCommit RPC 通信失败
	SigRetryCommitLocked  atomic.Int64 // 所有 related shard 都 locked=true
	SigRetryCommitWounded atomic.Int64 // locked=true 但检测到被 Wound，放弃
	SigTriggerReSim       atomic.Int64 // 成功 TriggerReSimulation
	SigRetryCommitFailed  atomic.Int64 // 全量失败（部分 locked）

	// RetryCommit 处理层面（被调用端）
	SigRetryCommitCalled         atomic.Int64 // RetryCommit 被远程调用
	SigRetryCommitWoundedPre     atomic.Int64 // 进入时发现已被 Wound
	SigRetryCommitPatchHit       atomic.Int64 // PatchPool 命中，跳过锁竞争
	SigRetryCommitPatchMiss      atomic.Int64 // PatchPool 匹配但 TryConsume 失败
	SigRetryCommitPrevWounded    atomic.Int64 // 之前被 Wound 跳过这轮
	SigRetryCommitTryLockOk      atomic.Int64 // TryLockWithPriority 成功
	SigRetryCommitTryLockFail    atomic.Int64 // TryLockWithPriority 返回 locked=false
	SigRetryCommitTryLockWounded atomic.Int64 // TryLockWithPriority 返回 wounded=true

	// 链式长度分布：按 SimulationNum 分桶统计
	// key=SimulationNum, value=被 chain 的 retryTx 数量
	// 用于衡量 HotKeyRetry 的链式依赖深度
	ChainLengthMu  sync.Mutex
	ChainLengthCnt map[int]int64 // simNum → 计数
	ChainCommitCnt map[int]int64 // simNum → 成功提交计数

	// 链式深度分布：按 ChainDepth 分桶统计
	ChainDepthMu        sync.Mutex
	ChainDepthCnt       map[int]int64
	ChainDepthCommitCnt map[int]int64

	// 被动池统计
	SigPassiveAdd     atomic.Int64 // 进入被动池次数
	SigPassiveWaken   atomic.Int64 // 被动池唤醒次数
	SigPassiveTimeout atomic.Int64 // 被动池超时 close 数
}

func initChainRetryStats() {
	chainRetryStats.ChainLengthCnt = make(map[int]int64)
	chainRetryStats.ChainCommitCnt = make(map[int]int64)
	chainRetryStats.ChainDepthCnt = make(map[int]int64)
	chainRetryStats.ChainDepthCommitCnt = make(map[int]int64)
}

// dumpChainRetryStats 输出链式 Retry 统计到日志。
// 读取并重置所有计数器，用于差分分析。
func dumpChainRetryStats() {
	chainRetryStats.ChainLengthMu.Lock()
	lenCnt := make(map[int]int64, len(chainRetryStats.ChainLengthCnt))
	for k, v := range chainRetryStats.ChainLengthCnt {
		lenCnt[k] = v
	}
	commitCnt := make(map[int]int64, len(chainRetryStats.ChainCommitCnt))
	for k, v := range chainRetryStats.ChainCommitCnt {
		commitCnt[k] = v
	}
	chainRetryStats.ChainLengthCnt = make(map[int]int64)
	chainRetryStats.ChainCommitCnt = make(map[int]int64)
	chainRetryStats.ChainLengthMu.Unlock()

	chainRetryStats.ChainDepthMu.Lock()
	depthCnt := make(map[int]int64, len(chainRetryStats.ChainDepthCnt))
	for k, v := range chainRetryStats.ChainDepthCnt {
		depthCnt[k] = v
	}
	depthCommitCnt := make(map[int]int64, len(chainRetryStats.ChainDepthCommitCnt))
	for k, v := range chainRetryStats.ChainDepthCommitCnt {
		depthCommitCnt[k] = v
	}
	chainRetryStats.ChainDepthCnt = make(map[int]int64)
	chainRetryStats.ChainDepthCommitCnt = make(map[int]int64)
	chainRetryStats.ChainDepthMu.Unlock()

	utils.SSCLogger().Info().
		Int64("chainNextScan", chainRetryStats.SigChainNextScan.Swap(0)).
		Int64("chainNoDownstream", chainRetryStats.SigChainNoDownstream.Swap(0)).
		Int64("chainSelected", chainRetryStats.SigChainSelected.Swap(0)).
		Int64("chainReservedSkipped", chainRetryStats.SigChainReservedSkipped.Swap(0)).
		Int64("retrySignalReceived", chainRetryStats.SigRetrySignalReceived.Swap(0)).
		Int64("retrySignalTxMissing", chainRetryStats.SigRetrySignalTxMissing.Swap(0)).
		Int64("tryReSimStarted", chainRetryStats.SigTryReSimStarted.Swap(0)).
		Int64("retryCommitCall", chainRetryStats.SigRetryCommitCall.Swap(0)).
		Int64("retryCommitFail", chainRetryStats.SigRetryCommitFail.Swap(0)).
		Int64("retryCommitRpcErr", chainRetryStats.SigRetryCommitRpcErr.Swap(0)).
		Int64("retryCommitLocked", chainRetryStats.SigRetryCommitLocked.Swap(0)).
		Int64("retryCommitWounded", chainRetryStats.SigRetryCommitWounded.Swap(0)).
		Int64("triggerReSim", chainRetryStats.SigTriggerReSim.Swap(0)).
		Int64("retryCommitFailed", chainRetryStats.SigRetryCommitFailed.Swap(0)).
		Int64("retryCommitCalled", chainRetryStats.SigRetryCommitCalled.Swap(0)).
		Int64("retryCommitWoundedPre", chainRetryStats.SigRetryCommitWoundedPre.Swap(0)).
		Int64("retryCommitPatchHit", chainRetryStats.SigRetryCommitPatchHit.Swap(0)).
		Int64("retryCommitPatchMiss", chainRetryStats.SigRetryCommitPatchMiss.Swap(0)).
		Int64("retryCommitPrevWounded", chainRetryStats.SigRetryCommitPrevWounded.Swap(0)).
		Int64("retryCommitTryLockOk", chainRetryStats.SigRetryCommitTryLockOk.Swap(0)).
		Int64("retryCommitTryLockFail", chainRetryStats.SigRetryCommitTryLockFail.Swap(0)).
		Int64("retryCommitTryLockWounded", chainRetryStats.SigRetryCommitTryLockWounded.Swap(0)).
		Interface("chainLengthDist", lenCnt).
		Interface("chainCommitDist", commitCnt).
		Interface("chainDepthDist", depthCnt).
		Interface("chainDepthCommitDist", depthCommitCnt).
		Msg("=== CHAIN_RETRY_STATS ===")
}

// RetrySchedulerStateAccessor 通过回调函数注入共享依赖，
// 使 retryScheduler 成为独立模块，不直接引用 sscService。
type RetrySchedulerStateAccessor struct {
	// 委员会
	IsLeader     func(epoch api.Epoch) bool
	GetLeader    func(epoch api.Epoch, shardId uint32) *api.Member
	ShardNum     func() uint32
	GetBlockHash func(txHash common.Hash) (common.Hash, bool)

	// 统计
	RetryAddCount       func()
	SampleRetryPool     func(size int)
	RetryReadySignal    func()
	RetryNotReadySignal func()
	RetrySuccessCount   func()
	RetryFailCount      func()

	// 状态写入
	SetChainPatch func(txHash common.Hash, patch *api.RWSet)

	// SimulationState 访问（用于从 SimulationCallStates 提取 RWSet）
	GetSimState func(txHash common.Hash) (*api.SimulationState, bool)

	// 触发重试模拟
	// 触发重试模拟
	TriggerReSimulation func(txHash common.Hash, simulationNum int)

	// 链上状态查询
	IsOnChain func(txHash common.Hash) bool // 交易是否有活跃的链上 SimTx

	// 直接关闭交易（用于重试超限时替代 PoolTimeout 兜底）
	CloseTransaction func(txHash common.Hash, commitOrRollback bool, reason string)
}

func NewRetryScheduler(ctx context.Context, bc core.BlockChain, state *RetrySchedulerStateAccessor, comm *Comm, selfShard uint32, tempLockView *TempLockView) *retryScheduler {
	rs := &retryScheduler{
		ctx:                   ctx,
		tempLockView:          tempLockView,
		bc:                    bc,
		state:                 state,
		comm:                  comm,
		selfShard:             selfShard,
		retryPool:             make(map[common.Hash]*api.RetryTx),
		passivePool:           make(map[common.Hash]struct{}),
		passivePoolEnterBlock: make(map[common.Hash]uint64),
		staleTxs:              make(map[common.Hash]struct{}),
		signals:               make(map[common.Hash]map[int]map[uint32]*api.RetrySignal),
		patches:               make(map[common.Hash]map[int]*api.ChainNode),
		patchPool: &api.PatchPool{
			Patches:  make(map[common.Hash]*api.ChainPatchNode),
			KeyIndex: make(map[api.LockKey]map[common.Hash]struct{}),
		},
		consumedPatches: make(map[common.Hash]common.Hash),
		reSimInFlight:   make(map[common.Hash]struct{}),
		onChainPatches:  make(map[common.Hash]map[int]*api.ChainNode),
		mu:              sync.RWMutex{},
	}

	dumpChainRetryStats()

	// NOTE: hotKeySet, updateHotKeys, isHotKey 等已废弃
	// 由 chainNextSim（全量 key 依赖检查）替代

	go rs.cycle()
	return rs
}

// retryScheduler 实现
type retryScheduler struct {
	mu sync.RWMutex

	// 待重试交易池：txHash → RetryTx
	retryPool             map[common.Hash]*api.RetryTx
	passivePool           map[common.Hash]struct{} // 被动池：标记"别 poll 了"
	passivePoolEnterBlock map[common.Hash]uint64   // 进入被动池的区块号（用于超时）
	staleTxs              map[common.Hash]struct{}
	signals               map[common.Hash]map[int]map[uint32]*api.RetrySignal // txHash -> simulationNum -> relatedShards -> signal

	// 链式 Patch 存储
	// patches[txHash][simulationNum] = ChainNode
	// 仅 leader 维护，用于链式触发缓存（链下）
	patches map[common.Hash]map[int]*api.ChainNode

	// onChainPatches: 所有节点共享，从 SimTx 的 ChainPatch 字段同步
	// onChainPatches[txHash][simulationNum] = ChainNode（含 Patch + UpstreamTxHash）
	// 用于 VerifySimulation 递归查上游 Patch
	// 写入：收到 SimTx 多播时；清理：Committer CR 完成时
	onChainPatches map[common.Hash]map[int]*api.ChainNode

	// PatchPool: 分片本地 Patch 池，用于 v4 机制
	patchPool *api.PatchPool

	// consumedPatches maps retryTxHash → consumed SimTx hash
	// Used to release consumed patches on retry failure
	consumedPatches map[common.Hash]common.Hash

	// reSimInFlight 防止同一笔 tx 的 StartReSimulation 被并发调用
	// 在 tryToReSimulation 成功获取锁后设置，TriggerReSimulation 后清理
	reSimInFlight map[common.Hash]struct{}

	// 依赖组件（通过接口解耦）
	ctx          context.Context
	bc           core.BlockChain
	tempLockView *TempLockView
	// 重试限制
	maxRetriesTotal int // 从 TimeoutConfig 传入，AddToRetry 时检查
	state           *RetrySchedulerStateAccessor
	comm            *Comm
	selfShard       uint32
}

// ─── 被动池接口 ────────────────────────────────────────────────

func (rs *retryScheduler) AddToPassivePool(txHash common.Hash) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.passivePool[txHash] = struct{}{}
	rs.passivePoolEnterBlock[txHash] = rs.bc.CurrentHeader().NumberU64()
	chainRetryStats.SigPassiveAdd.Add(1)
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
		Uint64("enterBlock", rs.passivePoolEnterBlock[txHash]).
		Msg("AddToPassivePool: entered passive pool")
}

func (rs *retryScheduler) RemoveFromPassivePool(txHash common.Hash) {
	t0 := time.Now()
	rs.mu.Lock()
	defer rs.mu.Unlock()
	delete(rs.passivePool, txHash)
	delete(rs.passivePoolEnterBlock, txHash)
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
		Str("duration", time.Since(t0).String()).
		Msg("RemoveFromPassivePool timing")
}

func (rs *retryScheduler) IsInPassivePool(txHash common.Hash) bool {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	_, exists := rs.passivePool[txHash]
	return exists
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
	if !rs.state.IsLeader(tx.Epochs[rs.selfShard]) {
		return
	}

	utils.SSCLogger().Info().
		Str("txHash", tx.TxHash.Hex()).
		Uint32("originShardID", tx.OriginShardID).
		Interface("relatedShards", tx.RelatedShards).
		Msg("call for retry")
	for _, shard := range tx.RelatedShards {
		leader := rs.state.GetLeader(tx.Epochs[shard], shard)
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
	if !rs.state.IsLeader(tx.Epochs[rs.selfShard]) {
		return
	}

	// 清理该交易在首次模拟中通过 GetState/SetState 获取的 TempLockView 锁
	rs.tempLockView.GarbageCollect(tx.TxHash)

	// 从 SimulationCallStates 提取 RWSet
	readSet := make([]api.LockKey, 0)
	writeSet := make([]api.LockKey, 0)
	if sim, ok := rs.state.GetSimState(tx.TxHash); ok && sim != nil {
		if callStates, exists := sim.SimulationCallStates[tx.SimulationNum-1]; exists {
			for _, callState := range callStates {
				if callState.RWSet != nil {
					for addr, account := range callState.RWSet.ReadState.State {
						for key := range account {
							readSet = append(readSet, api.FormKey(addr, key))
						}
					}
					for addr, account := range callState.RWSet.WriteState.State {
						for key := range account {
							writeSet = append(writeSet, api.FormKey(addr, key))
						}
					}
				}
			}
		}
	}
	tx.ReadSet = readSet
	tx.WriteSet = writeSet

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
	rs.state.RetryAddCount()
	rs.state.SampleRetryPool(len(rs.retryPool))
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

	rs.state.SampleRetryPool(len(rs.retryPool))

	shard2SignalReadyNum := make(map[uint32]int)
	shard2Epoch2RetrySignals := make(map[uint32]map[api.Epoch]*api.RetrySignals)
	for i := uint32(0); i < rs.state.ShardNum(); i++ {
		shard2SignalReadyNum[i] = 0
		shard2Epoch2RetrySignals[i] = make(map[api.Epoch]*api.RetrySignals)
	}

	// 判断交易是否可以重新模拟，并加入到signals中
	for txHash, tx := range rs.retryPool {
		// v2: 被动池中的 tx 不参与 poll（等 O 的 RetryCommit 唤醒）
		if _, inPassive := rs.passivePool[txHash]; inPassive {
			continue
		}
		signal := &api.RetrySignal{
			TxHash:        tx.TxHash,
			Epoch:         tx.Epochs[tx.OriginShardID],
			SimulationNum: tx.SimulationNum,
			Condition:     tx.Condition,
		}
		if rs.tempLockView.CanLock(txHash, tx.ReadSet, tx.WriteSet) {
			signal.Ready = true
			rs.state.RetryReadySignal()
			shard2SignalReadyNum[tx.OriginShardID]++
		} else {
			signal.Ready = false
			rs.state.RetryNotReadySignal()
		}
		signals := shard2Epoch2RetrySignals[tx.OriginShardID][signal.Epoch]
		if signals == nil {
			signals = &api.RetrySignals{
				OriginShard: tx.OriginShardID,
				FromShard:   rs.selfShard,
				Epoch:       signal.Epoch,
				Signals:     make([]*api.RetrySignal, 0),
			}
			shard2Epoch2RetrySignals[tx.OriginShardID][signal.Epoch] = signals
		}
		signals.Signals = append(signals.Signals, signal)
	}

	// 提交 promoted 交易到下一轮模拟
	for _, epoch2Signals := range shard2Epoch2RetrySignals {
		for _, signals := range epoch2Signals {
			if len(signals.Signals) > 0 {
				utils.SSCLogger().Info().
					Int("num", len(signals.Signals)).
					Uint32("shard", signals.OriginShard).
					Msg("promoted txs to next round")
				go rs.sendReSimulationSignals(signals)
			}
		}
	}

	// v2: 扫描被动池超时
	currentBlock := block.NumberU64()
	var expiredPassive []common.Hash
	for txHash, enterBlock := range rs.passivePoolEnterBlock {
		if rs.maxRetriesTotal > 0 && currentBlock >= enterBlock+uint64(rs.maxRetriesTotal) {
			expiredPassive = append(expiredPassive, txHash)
		}
	}
	for _, txHash := range expiredPassive {
		chainRetryStats.SigPassiveTimeout.Add(1)
		delete(rs.passivePool, txHash)
		delete(rs.passivePoolEnterBlock, txHash)
	}
	// 在锁外调用 closeTransaction
	for _, txHash := range expiredPassive {
		if rs.state.CloseTransaction != nil {
			rs.state.CloseTransaction(txHash, false, api.PoolTimeout.String())
		}
	}

	utils.SSCLogger().Info().
		Int("poolSize", len(rs.retryPool)).
		Int("passivePoolSize", len(rs.passivePool)).
		Int("passiveExpired", len(expiredPassive)).
		Uint64("blockNum", currentBlock).
		Int("staleNum", staleNum).
		Int("readyNum", shard2SignalReadyNum[rs.selfShard]).
		Msg("retryScheduler onBlockCommitted")
}

func (rs *retryScheduler) sendReSimulationSignals(signals *api.RetrySignals) {
	leader := rs.state.GetLeader(signals.Epoch, signals.OriginShard)

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
func (rs *retryScheduler) chainNextSim(upstreamTxHash common.Hash, upstreamSimNum int, writeSet *api.RWSet) {
	chainRetryStats.SigChainNextScan.Add(1)

	if writeSet == nil || len(writeSet.WriteState.State) == 0 {
		utils.SSCLogger().Info().Str("txHash", upstreamTxHash.Hex()).
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

	// 1. RLock 下收集所有匹配的 retry tx
	type matchedTx struct {
		txHash  common.Hash
		retryTx *api.RetryTx
	}
	var allMatched []matchedTx

	rs.mu.RLock()
	for txHash, retryTx := range rs.retryPool {
		if bytes.Equal(retryTx.TxHash.Bytes(), upstreamTxHash.Bytes()) {
			continue
		}
		if dependsOn(retryTx, keySet) {
			allMatched = append(allMatched, matchedTx{txHash: txHash, retryTx: retryTx})
		}
	}
	rs.mu.RUnlock()

	if len(allMatched) == 0 {
		chainRetryStats.SigChainNoDownstream.Add(1)
		utils.SSCLogger().Info().Str("txHash", upstreamTxHash.Hex()).
			Msg("chainNextSim: no downstream dependencies found")
		return
	}

	// 2. 状态预留（reservation）：每次匹配一个 retryTx 后，把它的所有 key 加入 reservedSet，
	//    后续匹配跳过依赖 reserved key 的其他 retryTx，避免多个 retry 争抢同一组状态。
	//
	//    只有不冲突的 retryTx 才会被选中，确保每个被 chain 的 retryTx 独占其依赖的状态，
	//    不再被其他 chain signal 同时争抢。
	reservedKeys := make(map[api.LockKey]struct{})
	var selected []matchedTx

	for _, m := range allMatched {
		if dependsOnReserved(m.retryTx, reservedKeys) {
			chainRetryStats.SigChainReservedSkipped.Add(1)
			utils.SSCLogger().Info().Str("txHash", m.txHash.Hex()).
				Int("reservedKeys", len(reservedKeys)).
				Msg("chainNextSim: skipped due to reserved state conflict")
			continue
		}
		selected = append(selected, m)
		// 将 retryTx 涉及的所有 key 加入 reservedSet
		for _, lockKey := range m.retryTx.ReadSet {
			reservedKeys[lockKey] = struct{}{}
		}
		for _, lockKey := range m.retryTx.WriteSet {
			reservedKeys[lockKey] = struct{}{}
		}
	}

	utils.SSCLogger().Info().Str("txHash", upstreamTxHash.Hex()).
		Int("totalMatched", len(allMatched)).
		Int("selected", len(selected)).
		Int("reservedKeys", len(reservedKeys)).
		Msg("chainNextSim: reservation completed")

	// 3. 逐个发送 chain signal（不在 RLock 内做 RPC）
	for _, m := range selected {
		chainRetryStats.SigChainSelected.Add(1)
		rs.sendChainSignal(m.txHash, m.retryTx, writeSet, upstreamTxHash, upstreamSimNum)
	}
}

// dependsOnReserved 判断 retryTx 是否依赖 reservedKeys 中的任意 key。
// 如果依赖，则跳过此 retryTx（让给其他不冲突的交易）。
func dependsOnReserved(retryTx *api.RetryTx, reservedKeys map[api.LockKey]struct{}) bool {
	for _, lockKey := range retryTx.ReadSet {
		if _, exists := reservedKeys[lockKey]; exists {
			return true
		}
	}
	for _, lockKey := range retryTx.WriteSet {
		if _, exists := reservedKeys[lockKey]; exists {
			return true
		}
	}
	return false
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

// HandleRetrySignal 接收来自 SimTx 提交 shard 的 chain signal，
// 在 origin shard 存储 ChainPatch 后通过 signal aggregation 触发 tryToReSimulation。
func (rs *retryScheduler) HandleRetrySignal(signal *api.RetrySignal) {
	chainRetryStats.SigRetrySignalReceived.Add(1)
	utils.SSCLogger().Info().Str("txHash", signal.TxHash.Hex()).
		Bool("hasPatch", signal.ChainPatch != nil).
		Msg("HandleRetrySignal: received")

	if signal.ChainPatch != nil {
		rs.state.SetChainPatch(signal.TxHash, signal.ChainPatch)
	}

	// 收到一个 chain signal 就直接 tryToReSimulation
	// chain 路径的 signal 是强信号，不需要等所有 shard 的信号聚合
	rs.mu.RLock()
	retryTx, exists := rs.retryPool[signal.TxHash]
	rs.mu.RUnlock()
	if exists && retryTx != nil {
		// 统计链式长度：retryTx.SimulationNum 代表重试次数=链深
		chainRetryStats.ChainLengthMu.Lock()
		chainRetryStats.ChainLengthCnt[retryTx.SimulationNum]++
		chainRetryStats.ChainLengthMu.Unlock()

		go rs.tryToReSimulation(retryTx)
	} else {
		chainRetryStats.SigRetrySignalTxMissing.Add(1)
		utils.SSCLogger().Warn().Str("txHash", signal.TxHash.Hex()).
			Msg("HandleRetrySignal: retry tx not found in pool")
	}
}

// OLD: chainHotKeyCR — replaced by chainNextSim
// 原有逻辑基于 HotKey + CR 提交触发，已迁移到 chainNextSim (SimTx 提交 + 全量依赖)
/*
func (rs *retryScheduler) chainHotKeyCR(crTxHash common.Hash, writeSet *api.RWSet) {
	...
}

func (rs *retryScheduler) HandleHotKeyRetrySignal(signal *api.RetrySignal) {
	...
}
*/
func (rs *retryScheduler) HandleReSimulationSignal(signals *api.RetrySignals) error {
	if !rs.state.IsLeader(signals.Epoch) {
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
	chainRetryStats.SigTryReSimStarted.Add(1)

	// 日志：当前 retryPool 状态和最高优先级交易
	func() {
		rs.mu.RLock()
		defer rs.mu.RUnlock()
		if len(rs.retryPool) == 0 {
			return
		}
		// 找最高优先级交易
		var bestTx common.Hash
		var bestPri api.Priority
		first := true
		for txh, tx := range rs.retryPool {
			pri := api.Priority{
				Nonce:         tx.Nonce,
				OriginShardID: tx.OriginShardID,
				TxHash:        txh,
			}
			if first || pri.Less(bestPri) {
				bestPri = pri
				bestTx = txh
				first = false
			}
		}
		utils.SSCLogger().Info().
			Int("retryPoolSize", len(rs.retryPool)).
			Str("txHash", txHash.Hex()[:20]).
			Str("bestTx", bestTx.Hex()[:20]).
			Uint64("bestNonce", bestPri.Nonce).
			Uint32("bestOriginShardID", bestPri.OriginShardID).
			Msg("tryToReSimulation: retry pool stats")
	}()

	// 防重入：同一笔 tx 的 reSim 已经在进行中则跳过
	rs.mu.Lock()
	if _, inFlight := rs.reSimInFlight[txHash]; inFlight {
		rs.mu.Unlock()
		chainRetryStats.SigRetryCommitTryLockFail.Add(1)
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
			Msg("tryToReSimulation: already in flight, skipping")
		return
	}
	rs.reSimInFlight[txHash] = struct{}{}
	rs.mu.Unlock()

	// 函数退出时清理 inFlight 标记
	defer func() {
		rs.mu.Lock()
		delete(rs.reSimInFlight, txHash)
		rs.mu.Unlock()
	}()

	resps := make(map[uint32]*api.RetryCommitResp)

	wg := sync.WaitGroup{}
	wg.Add(len(retryTx.RelatedShards))
	lock := sync.Mutex{}
	for _, shard := range retryTx.RelatedShards {
		go func(shard uint32) {
			defer wg.Done()
			resp := new(api.RetryCommitResp)
			err := rs.comm.Call(rs.ctx, resp, rs.state.GetLeader(retryTx.Epochs[shard], shard), api.Method_RetryCommit, txHash)
			if err != nil {
				chainRetryStats.SigRetryCommitRpcErr.Add(1)
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
			chainRetryStats.SigRetryCommitFail.Add(1)
			success = false
			break
		}
	}
	if success {
		// 二次验证：检查本地是否被 Wound
		if rs.tempLockView.IsWounded(txHash) {
			chainRetryStats.SigRetryCommitWounded.Add(1)
			utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
				Msg("retry commit success but local tx was wounded, aborting")
			// 释放所有锁
			for shardId, resp := range resps {
				if resp.Locked {
					go func(shardId uint32) {
						_ = rs.comm.Call(rs.ctx, nil, rs.state.GetLeader(retryTx.Epochs[shardId], shardId), api.Method_RetryCancel, resp.TxHash)
					}(shardId)
				}
			}
			return
		}
		rs.state.RetrySuccessCount()
		chainRetryStats.SigRetryCommitLocked.Add(1)
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
			Uint64("nonce", retryTx.Nonce).
			Uint32("originShardId", retryTx.OriginShardID).
			Int("simulationNum", retryTx.SimulationNum).
			Msg("retry commit success")
		go rs.state.TriggerReSimulation(retryTx.TxHash, retryTx.SimulationNum)
		chainRetryStats.SigTriggerReSim.Add(1)
		rs.mu.Lock()
		delete(rs.signals, txHash)
		delete(rs.retryPool, txHash)
		delete(rs.consumedPatches, txHash)
		rs.mu.Unlock()
	} else {
		rs.state.RetryFailCount()
		chainRetryStats.SigRetryCommitFailed.Add(1)
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msg("retry commit failed")

		// v2: 如果失败原因是链上锁冲突，O 通知已 Locked=true 的分片进被动池
		var hasOnChainConflict bool
		var lockedShards []uint32
		for shardId, resp := range resps {
			if resp.OnChainLockConflict {
				hasOnChainConflict = true
			}
			if resp.Locked {
				lockedShards = append(lockedShards, shardId)
			}
		}
		if hasOnChainConflict && len(lockedShards) > 0 {
			for _, shardId := range lockedShards {
				leader := rs.state.GetLeader(retryTx.Epochs[shardId], shardId)
				if leader == nil {
					continue
				}
				if shardId == rs.selfShard {
					// 本地调用
					rs.AddToPassivePool(txHash)
				} else {
					// RPC 调用
					go func(leader *api.Member) {
						if err := rs.comm.Call(rs.ctx, nil, leader, api.Method_AddToPassivePool, txHash); err != nil {
							utils.SSCLogger().Error().Err(err).Str("txHash", txHash.Hex()).
								Msg("tryToReSimulation: failed to notify passive pool")
						}
					}(leader)
				}
			}
		}

		// v4: Release consumed PatchPool entry so other retryTxs can use it
		rs.mu.Lock()
		if consumedSimTx, exists := rs.consumedPatches[txHash]; exists {
			rs.patchPool.Release(consumedSimTx)
			delete(rs.consumedPatches, txHash)
			utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
				Str("releasedPatch", consumedSimTx.Hex()).
				Msg("retry commit failed: released consumed patch")
		}
		rs.mu.Unlock()
		for shardId, resp := range resps {
			// 如果失败，则将临时上锁的交易解锁
			if resp.Locked {
				go func(shardId uint32) {
					err := rs.comm.Call(rs.ctx, nil, rs.state.GetLeader(retryTx.Epochs[shardId], shardId), api.Method_RetryCancel, resp.TxHash)
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

func (rs *retryScheduler) setSignal(fromShard uint32, signal *api.RetrySignal) {
	m := rs.signals[signal.TxHash]
	if m == nil {
		m = make(map[int]map[uint32]*api.RetrySignal)
		rs.signals[signal.TxHash] = m
	}
	m2 := m[signal.SimulationNum]
	if m2 == nil {
		m2 = make(map[uint32]*api.RetrySignal)
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
	t0 := time.Now()
	rs.tempLockView.GarbageCollect(txHash)

	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.staleTxs[txHash] = struct{}{}
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
		Str("duration", time.Since(t0).String()).
		Msg("retryScheduler.StaleTx timing")
}

func (rs *retryScheduler) RetryCommit(txHash common.Hash) *api.RetryCommitResp {
	chainRetryStats.SigRetryCommitCalled.Add(1)

	// v2: 如果在被动池中，移出（被 O 的 RetrySignal 唤醒）
	if _, inPassive := rs.passivePool[txHash]; inPassive {
		delete(rs.passivePool, txHash)
		delete(rs.passivePoolEnterBlock, txHash)
		chainRetryStats.SigPassiveWaken.Add(1)
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
			Msg("retryCommit: woken from passive pool")
	}

	// Check if this tx has been Wounded — if so, return failure immediately
	if rs.tempLockView.IsWounded(txHash) {
		chainRetryStats.SigRetryCommitWoundedPre.Add(1)
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msg("retryCommit: wounded by higher priority tx")
		return &api.RetryCommitResp{Locked: false, TxHash: txHash}
	}

	rs.mu.RLock()
	retryTx := rs.retryPool[txHash]
	rs.mu.RUnlock()
	if retryTx == nil {
		return &api.RetryCommitResp{Locked: false, TxHash: txHash}
	}
	if !rs.state.IsLeader(retryTx.Epochs[rs.selfShard]) {
		return &api.RetryCommitResp{Locked: false, TxHash: txHash}
	}

	// 构建优先级
	priority := api.Priority{
		Nonce:         retryTx.Nonce,
		OriginShardID: retryTx.OriginShardID,
		TxHash:        txHash,
	}

	// v4: Check local PatchPool for conflict with this retryTx
	if matched, node := rs.patchPool.HasConflict(retryTx); matched {
		if patch := rs.patchPool.TryConsume(node.TxHash, txHash, priority); patch != nil {
			chainRetryStats.SigRetryCommitPatchHit.Add(1)
			rs.state.SetChainPatch(txHash, patch)
			rs.mu.Lock()
			rs.consumedPatches[txHash] = node.TxHash
			rs.mu.Unlock()
			utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
				Str("patchFrom", node.TxHash.Hex()).
				Msg("retryCommit: found local patch in PatchPool, skipping lock conflict")
			return &api.RetryCommitResp{Locked: true, TxHash: txHash}
		}
	}
	// PatchPool tryConsume failed — already consumed by another retryTx
	if matched, _ := rs.patchPool.HasConflict(retryTx); matched {
		chainRetryStats.SigRetryCommitPatchMiss.Add(1)
	}

	// Check wounded BEFORE any lock operation — if we were wounded in a previous
	// round, don't try to lock again (the lock is held by higher priority tx).
	if rs.tempLockView.IsWounded(txHash) {
		chainRetryStats.SigRetryCommitPrevWounded.Add(1)
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msg("retryCommit: previously wounded, skip this round")
		return &api.RetryCommitResp{Locked: false, TxHash: txHash}
	}

	// Normal lock competition path with Wound-Wait priority
	locked, wounded := rs.tempLockView.TryLockWithPriority(txHash, priority, retryTx.ReadSet, retryTx.WriteSet)
	if wounded {
		chainRetryStats.SigRetryCommitTryLockWounded.Add(1)
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msg("retryCommit: wounded by higher priority tx")
		return &api.RetryCommitResp{Locked: false, TxHash: txHash}
	}
	if !locked {
		chainRetryStats.SigRetryCommitTryLockFail.Add(1)
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msg("retryCommit failed: try lock failed")
		return &api.RetryCommitResp{Locked: false, TxHash: txHash}
	}
	stateDB, err := rs.getStateDB(txHash)
	if err != nil {
		chainRetryStats.SigRetryCommitTryLockFail.Add(1)
		utils.SSCLogger().Error().Err(err).Str("txHash", txHash.Hex()).Msgf("retryCommit failed: stateDB is not exists")
		return &api.RetryCommitResp{Locked: false, TxHash: txHash}
	}

	// 检查 stateDB 层面是否可锁：对 WriteSet 和 ReadSet 的每个 key 检查
	for _, key := range retryTx.WriteSet {
		if err := stateDB.CheckLock(key, txHash); err != nil {
			chainRetryStats.SigRetryCommitTryLockFail.Add(1)
			utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
				Str("key", string(key)).Err(err).
				Msg("retryCommit failed: stateDB lock conflict")
			// v2: 标记 on-chain 锁冲突，让 O 决定是否进被动池
			return &api.RetryCommitResp{
				Locked:              false,
				TxHash:              txHash,
				OnChainLockConflict: errors.Is(err, api.ErrLockConflict_OnChain),
			}
		}
	}
	for _, key := range retryTx.ReadSet {
		if err := stateDB.CheckLock(key, txHash); err != nil {
			chainRetryStats.SigRetryCommitTryLockFail.Add(1)
			utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
				Str("key", string(key)).Err(err).
				Msg("retryCommit failed: stateDB lock conflict (read)")
			// v2: 标记 on-chain 锁冲突，让 O 决定是否进被动池
			return &api.RetryCommitResp{
				Locked:              false,
				TxHash:              txHash,
				OnChainLockConflict: errors.Is(err, api.ErrLockConflict_OnChain),
			}
		}
	}

	// Clear wounded flag after successful lock — only now it's safe
	rs.tempLockView.ClearWounded(txHash)
	chainRetryStats.SigRetryCommitTryLockOk.Add(1)
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msg("retryCommit")
	return &api.RetryCommitResp{Locked: true, TxHash: txHash}
}

func (rs *retryScheduler) getStateDB(txHash common.Hash) (api.StateDB, error) {
	stateAt, err := rs.bc.State()
	if err != nil {
		return nil, err
	}
	return stateAt, nil
}

func (rs *retryScheduler) RetryCancel(txHash common.Hash) {
	rs.tempLockView.GarbageCollect(txHash)
}

// sendChainSignal 发送链式重试信号到 origin shard。
func (rs *retryScheduler) sendChainSignal(txHash common.Hash, retryTx *api.RetryTx, writeSet *api.RWSet, upstreamTxHash common.Hash, upstreamSimNum int) {
	// 构建 ChainNode
	chainNode := &api.ChainNode{
		TxHash:         txHash,
		SimulationNum:  retryTx.SimulationNum,
		Patch:          writeSet,
		UpstreamTxHash: upstreamTxHash,
		UpstreamSimNum: upstreamSimNum,
	}

	// 存储到 patches
	rs.mu.Lock()
	if rs.patches[txHash] == nil {
		rs.patches[txHash] = make(map[int]*api.ChainNode)
	}
	rs.patches[txHash][retryTx.SimulationNum] = chainNode
	rs.mu.Unlock()

	// 构建 RetrySignal
	signal := &api.RetrySignal{
		TxHash:        txHash,
		FromShard:     rs.selfShard,
		Epoch:         retryTx.Epochs[rs.selfShard],
		SimulationNum: retryTx.SimulationNum,
		Condition:     api.Simulate,
		Ready:         true,
		ChainPatch:    writeSet,
	}

	// 发送到 origin shard
	originShard := retryTx.OriginShardID
	if originShard == rs.selfShard {
		// 本地处理
		rs.HandleRetrySignal(signal)
	} else {
		// RPC 发送
		originLeader := rs.state.GetLeader(retryTx.Epochs[originShard], originShard)
		if originLeader == nil {
			utils.SSCLogger().Warn().Str("txHash", txHash.Hex()).
				Uint32("originShard", originShard).
				Msg("sendChainSignal: origin leader not found")
			return
		}
		go func() {
			err := rs.comm.Call(rs.ctx, nil, originLeader, api.Method_HandleRetrySignal, signal)
			if err != nil {
				utils.SSCLogger().Warn().Err(err).
					Str("txHash", txHash.Hex()).
					Msg("sendChainSignal: failed to send retry signal")
			}
		}()
	}
}

// readPatchChain 递归查 patches 链，读到就停。
func (rs *retryScheduler) readPatchChain(txHash common.Hash, simNum int,
	address common.Address, key common.Hash) (common.Hash, bool) {

	rs.mu.RLock()
	node, ok := rs.patches[txHash][simNum]
	rs.mu.RUnlock()

	if !ok {
		return common.Hash{}, false
	}

	// 先查自己的 patch
	if node.Patch != nil {
		if addrState, ok := node.Patch.WriteState.State[address]; ok {
			if val, exists := addrState[key]; exists {
				return val, true
			}
		}
	}

	// 自己没有，递归查上游
	if node.UpstreamTxHash != (common.Hash{}) {
		return rs.readPatchChain(node.UpstreamTxHash, node.UpstreamSimNum, address, key)
	}

	return common.Hash{}, false
}

// GetChainPatchRef 返回指定 tx 的链式 patch 引用（TxSimKey）。
// 用于 VerifySimulation 判断是否跳过锁冲突检查。
// 注意：返回的 TxSimKey 是当前 tx 自己的 hash，不是上游的。
// 要获取上游信息请使用 GetUpstreamTxRef。
func (rs *retryScheduler) GetChainPatchRef(txHash common.Hash) *api.TxSimKey {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	for _, node := range rs.patches[txHash] {
		if node != nil {
			return &api.TxSimKey{
				TxHash:        node.TxHash,
				SimulationNum: node.SimulationNum,
			}
		}
	}
	return nil
}

// GetUpstreamTxRef 获取指定 tx 在 patches 中的上游引用信息。
// 如果该 tx 是链式交易，返回上游的 TxHash + SimulationNum。
// 用于 CommitSimulation 构建 SimTx 时填写 UpstreamTxHash。
func (rs *retryScheduler) GetUpstreamTxRef(txHash common.Hash) (common.Hash, int) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	for _, node := range rs.patches[txHash] {
		if node != nil && node.UpstreamTxHash != (common.Hash{}) {
			return node.UpstreamTxHash, node.UpstreamSimNum
		}
	}
	return common.Hash{}, 0
}

// AddOnChainPatch 将 SimTx 的 ChainPatch 以 ChainNode 形式存入 onChainPatches。
// 所有节点在收到 SimTx 多播时调用。
func (rs *retryScheduler) AddOnChainPatch(txHash common.Hash, simNum int, chainPatch *api.RWSet, upstreamTxHash common.Hash, upstreamSimNum int) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.onChainPatches[txHash] == nil {
		rs.onChainPatches[txHash] = make(map[int]*api.ChainNode)
	}
	rs.onChainPatches[txHash][simNum] = &api.ChainNode{
		TxHash:         txHash,
		SimulationNum:  simNum,
		Patch:          chainPatch,
		UpstreamTxHash: upstreamTxHash,
		UpstreamSimNum: upstreamSimNum,
	}
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
		Int("simNum", simNum).
		Bool("hasUpstream", upstreamTxHash != (common.Hash{})).
		Str("upstreamTxHash", upstreamTxHash.Hex()).
		Msg("AddOnChainPatch: stored from SimTx")
}

// GetOnChainPatch 从 onChainPatches 查询指定 SimTx 的 ChainNode。
func (rs *retryScheduler) GetOnChainPatch(txHash common.Hash, simNum int) *api.ChainNode {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	if patches, ok := rs.onChainPatches[txHash]; ok {
		return patches[simNum]
	}
	return nil
}

// RemoveOnChainPatch 从 onChainPatches 删除指定交易的 ChainNode。
// 在 Committer.CommitOrRollbackWithProof 中调用（CR 完成后链上清理）。
func (rs *retryScheduler) RemoveOnChainPatch(txHash common.Hash) {
	t0 := time.Now()
	rs.mu.Lock()
	defer rs.mu.Unlock()
	delete(rs.onChainPatches, txHash)
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
		Str("duration", time.Since(t0).String()).
		Msg("RemoveOnChainPatch timing")
}

// ReadOnChainPatch 从 onChainPatches 递归读取指定 SimTx 的某 key 期望值。
// 只查单个 key，不合并全部上游 WriteSet（高性能路径）。
// 返回 (value, found)，found=false 表示该 key 在上游 WriteSet 中不存在。
func (rs *retryScheduler) ReadOnChainPatch(txHash common.Hash, simNum int, address common.Address, key common.Hash) (common.Hash, bool) {
	rs.mu.RLock()
	bySim, ok := rs.onChainPatches[txHash]
	if !ok {
		rs.mu.RUnlock()
		return common.Hash{}, false
	}
	node := bySim[simNum]
	rs.mu.RUnlock()
	if node == nil || node.Patch == nil || node.Patch.WriteState == nil {
		return common.Hash{}, false
	}
	// 查当前 ChainNode 的 Patch
	if addrState, ok := node.Patch.WriteState.State[address]; ok {
		if val, ok := addrState[key]; ok {
			return val, true
		}
	}
	// 递归查上游
	if node.UpstreamTxHash != (common.Hash{}) {
		return rs.ReadOnChainPatch(node.UpstreamTxHash, node.UpstreamSimNum, address, key)
	}
	return common.Hash{}, false
}

// mergeRWSet 合并两个 RWSet。冲突时 new 覆盖 old。
func mergeRWSet(new *api.RWSet, old *api.RWSet) *api.RWSet {
	if old == nil {
		return new
	}
	if new == nil {
		return old
	}
	merged := &api.RWSet{
		ReadState:    api.NewStateSet(),
		WriteState:   api.NewStateSet(),
		CurrentState: api.NewStateSet(),
	}
	// 先复制 old 的所有 write
	for addr, state := range old.WriteState.State {
		if merged.WriteState.State[addr] == nil {
			merged.WriteState.State[addr] = make(map[common.Hash]common.Hash)
		}
		for k, v := range state {
			merged.WriteState.State[addr][k] = v
		}
	}
	// new 覆盖
	for addr, state := range new.WriteState.State {
		if merged.WriteState.State[addr] == nil {
			merged.WriteState.State[addr] = make(map[common.Hash]common.Hash)
		}
		for k, v := range state {
			merged.WriteState.State[addr][k] = v
		}
	}
	return merged
}

// OnPatchPoolUpdated scans the retryPool for conflicts with PatchPool entries.
// Called after each Add to PatchPool, or periodically.
func (rs *retryScheduler) OnPatchPoolUpdated() {
	rs.mu.RLock()
	pool := rs.patchPool
	// Snapshot retryPool keys under lock to avoid concurrent map iteration
	retryKeys := make([]common.Hash, 0, len(rs.retryPool))
	retryTxs := make(map[common.Hash]*api.RetryTx, len(rs.retryPool))
	for txHash, retryTx := range rs.retryPool {
		retryKeys = append(retryKeys, txHash)
		retryTxs[txHash] = retryTx
	}
	rs.mu.RUnlock()

	for _, txHash := range retryKeys {
		retryTx := retryTxs[txHash]
		matched, node := pool.HasConflict(retryTx)
		if matched {
			priority := api.Priority{
				Nonce:         retryTx.Nonce,
				OriginShardID: retryTx.OriginShardID,
				TxHash:        txHash,
			}
			// Temporary exclusive: mark Consumed to prevent other retryTxs from taking it
			patch := pool.TryConsume(node.TxHash, txHash, priority)
			if patch != nil {
				rs.mu.Lock()
				rs.consumedPatches[txHash] = node.TxHash
				rs.mu.Unlock()
				// Send RetrySignal with local Patch
				rs.sendChainSignal(txHash, retryTx, patch, node.TxHash, 0)
			}
		}
	}
}
