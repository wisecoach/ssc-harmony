package ssc

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/core"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/harmony-one/harmony/ssc/perf"
	"github.com/pkg/errors"
)

// ============================================================================
// 链式 Retry 全路径统计计数器
// 用于分析 retry 漏斗瓶颈：哪些环节失败最多，Wound-Wait 实际影响面。
// 通过 grep "=== CHAIN_RETRY_STATS ===" 一次性拉取所有 shard 的汇总。
// ============================================================================
// LockWaitStore 管理 LockWait Pool 的三张 map，自带 sync.Mutex 与 rs.mu 解耦。
// LockWait Pool 中的交易已从 retryPool 中删除，
// OnBlockCommitted 扫描 stateDB 解锁后移回 retryPool。
type LockWaitStore struct {
	mu         sync.Mutex
	pool       map[common.Hash]struct{}
	enterBlock map[common.Hash]uint64
	txs        map[common.Hash]*api.RetryTx
}

func newLockWaitStore() *LockWaitStore {
	return &LockWaitStore{
		pool:       make(map[common.Hash]struct{}),
		enterBlock: make(map[common.Hash]uint64),
		txs:        make(map[common.Hash]*api.RetryTx),
	}
}

func (s *LockWaitStore) Add(txHash common.Hash, enterBlock uint64, retryTx *api.RetryTx) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pool[txHash] = struct{}{}
	s.enterBlock[txHash] = enterBlock
	s.txs[txHash] = retryTx
}

func (s *LockWaitStore) Remove(txHash common.Hash) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pool, txHash)
	delete(s.enterBlock, txHash)
	delete(s.txs, txHash)
}

// ForEach 遍历所有 tx，f 在锁外执行，返回 true 表示从池中移除。
// 避免遍历耗时操作（如 stateDB.CheckLock）占用锁。
func (s *LockWaitStore) ForEach(f func(txHash common.Hash, retryTx *api.RetryTx) (remove bool)) {
	s.mu.Lock()
	// 收集快照
	type entry struct {
		hash common.Hash
		tx   *api.RetryTx
	}
	all := make([]entry, 0, len(s.pool))
	for h := range s.pool {
		if t, ok := s.txs[h]; ok {
			all = append(all, entry{hash: h, tx: t})
		}
	}
	s.mu.Unlock()

	// 锁外执行回调
	var toRemove []common.Hash
	for _, e := range all {
		if f(e.hash, e.tx) {
			toRemove = append(toRemove, e.hash)
		}
	}

	// 锁内删除
	if len(toRemove) > 0 {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, h := range toRemove {
			delete(s.pool, h)
			delete(s.enterBlock, h)
			delete(s.txs, h)
		}
	}
}

// ForEachEntry 遍历所有 entry，f 在锁外执行。
// 用于超时扫描——不需要改 map 时用这个。
func (s *LockWaitStore) ForEachEntry(f func(txHash common.Hash, enterBlock uint64)) {
	s.mu.Lock()
	entries := make([]struct {
		hash common.Hash
		blk  uint64
	}, 0, len(s.enterBlock))
	for h, b := range s.enterBlock {
		entries = append(entries, struct {
			hash common.Hash
			blk  uint64
		}{hash: h, blk: b})
	}
	s.mu.Unlock()
	for _, e := range entries {
		f(e.hash, e.blk)
	}
}

func (s *LockWaitStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pool)
}

// ============================================================================
// 链式 Retry 全路径统计计数器
// 用于分析 retry 漏斗瓶颈：哪些环节失败最多，Wound-Wait 实际影响面。
// 通过 grep "=== CHAIN_RETRY_STATS ===" 一次性拉取所有 shard 的汇总。
// ============================================================================
var chainRetryStats struct {
	// VerifySimulation 阶段
	SigChainTxDetected atomic.Int64 // VerifySimulation 判定为链式交易（跳过锁检查）的次数（差分，随 dump 清零）

	// 累计计数（不随 dump 重置），供 ssc_getModuleStatus 监控在实验结束时读取。
	MonitorChainTxDetected      atomic.Int64
	MonitorRetryCommitPatchHit  atomic.Int64
	MonitorRetryCommitPatchMiss atomic.Int64

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
	SigOnChainWound       atomic.Int64 // DSN-52 §10.3：高优先级主动 Wound 低优先级链上持有者的次数
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

	SigChainDepthCapped atomic.Int64 // DSN-50: 因链深上限放弃链式救援的次数（差分）
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

	utils.SSCLogger().Debug().
		Int64("chainTxDetected", chainRetryStats.SigChainTxDetected.Swap(0)).
		Int64("retrySignalReceived", chainRetryStats.SigRetrySignalReceived.Swap(0)).
		Int64("retrySignalTxMissing", chainRetryStats.SigRetrySignalTxMissing.Swap(0)).
		Int64("tryReSimStarted", chainRetryStats.SigTryReSimStarted.Swap(0)).
		Int64("retryCommitCall", chainRetryStats.SigRetryCommitCall.Swap(0)).
		Int64("retryCommitFail", chainRetryStats.SigRetryCommitFail.Swap(0)).
		Int64("retryCommitRpcErr", chainRetryStats.SigRetryCommitRpcErr.Swap(0)).
		Int64("retryCommitLocked", chainRetryStats.SigRetryCommitLocked.Swap(0)).
		Int64("retryCommitWounded", chainRetryStats.SigRetryCommitWounded.Swap(0)).
		Int64("onChainWound", chainRetryStats.SigOnChainWound.Swap(0)).
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
		Int64("chainDepthCapped", chainRetryStats.SigChainDepthCapped.Swap(0)).
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

	// stateLockManager 统计（OnBlockCommitted 日志需要）
	LockStatesStats func() (globalLocked, globalRLocked, globalFinished, globalLockStart int)
}

func newRetryScheduler(ctx context.Context, bc core.BlockChain, state *RetrySchedulerStateAccessor, comm *Comm, selfShard uint32, tempLockView *TempLockView, config *api.TimeoutConfig) *retryScheduler {
	rs := &retryScheduler{
		retryPool:      sync.Map{},
		passivePool:    sync.Map{},
		staleTxs:       sync.Map{},
		signals:        sync.Map{},
		onChainPatches: sync.Map{},
		// offChainDAG 为单一链下 DAG 存储（nodes/keyIndex/subscriber/txSubKeys 均为零值 sync.Map，可直接使用）
		offChainDAG:     offChainDAG{},
		consumedPatches: sync.Map{},
		reSimInFlight:   sync.Map{},
		lockWait:        newLockWaitStore(),
		woundedRetryTxs: sync.Map{},
		ctx:             ctx,
		bc:              bc,
		tempLockView:    tempLockView,
		maxRetriesTotal: int(config.MaxRetriesTotal),
		maxChainDepth:   defaultMaxChainDepth(config.MaxChainDepth),
		state:           state,
		comm:            comm,
		selfShard:       selfShard,
	}

	dumpChainRetryStats()

	// NOTE: hotKeySet, updateHotKeys, isHotKey 等已废弃
	// 由 chainNextSim（全量 key 依赖检查）替代

	go rs.cycle()
	return rs
}

// patchMap 是 patches 和 onChainPatches 的内层结构，自带锁实现 per-tx 粒度保护
type patchMap struct {
	mu sync.Mutex
	m  map[int]*api.ChainNode
}

// signalMap 是 signals 的内层结构，自带锁实现 per-tx 粒度保护
// 结构：signals[txHash][simulationNum][shardId] = Signal
type signalMap struct {
	mu sync.Mutex
	m  map[int]map[uint32]*api.RetrySignal
}

// retryScheduler 实现
type retryScheduler struct {
	// retryPool — 待重试交易池
	retryPool sync.Map // key: txHash common.Hash, value: *api.RetryTx
	// passivePool — 被动池，标记"别 poll 了"，等待 O 推 RetryCommit 唤醒
	passivePool sync.Map // key: txHash common.Hash, value: struct{}{}
	// staleTxs — 已 stale 的交易标记，OnBlockCommitted 清理
	staleTxs sync.Map // key: txHash common.Hash, value: struct{}{}
	// signals — 信号聚合存储，每笔交易每个 simulationNum 有各 shard 的信号
	signals sync.Map // key: txHash common.Hash, value: *signalMap

	// onChainPatches — 所有节点共享的链上 Patch 缓存
	//      写入：收到 SimTx 多播时；清理：Committer CR 完成时
	//      内层 map[int]*api.ChainNode 由 patchMap.mu 保护
	onChainPatches sync.Map // key: txHash common.Hash, value: *patchMap

	// offChainDAG — 链下单一 DAG 存储（合并原 localPatches 与 patches）
	//      承担调度匹配（nodes/keyIndex/subscriber/txSubKeys）与读取/构建上游（UpstreamTxList）双职责
	offChainDAG offChainDAG

	// consumedPatches — retryTxHash → consumed SimTx hashes []common.Hash，用于失败时批量释放 Patch
	consumedPatches sync.Map // key: txHash common.Hash, value: []common.Hash

	// reSimInFlight — 防止同一笔 tx 的 StartReSimulation 被并发调用
	reSimInFlight sync.Map // key: txHash common.Hash, value: struct{}{}

	// lockWait — 等待 stateDB 解锁的交易（自带锁，与 rs 解耦）
	lockWait *LockWaitStore

	// woundedRetryTxs — 被 Wound 的交易，OnBlockCommitted 时恢复
	woundedRetryTxs sync.Map // key: txHash common.Hash, value: struct{}{}

	// 缓存：OnBlockCommitted 时缓存的 stateDB 和 block hash
	// 供 RetryCommit Phase 2 + StartReSimulation 共用，消除 CheckLock 与 Lockable 的 race
	cachedState     atomic.Value // api.StateDB
	cachedBlockNum  uint64
	cachedBlockHash common.Hash

	// 依赖组件（通过接口解耦）
	ctx          context.Context
	bc           core.BlockChain
	tempLockView *TempLockView
	// 重试限制
	maxRetriesTotal int // 从 TimeoutConfig 传入，AddToRetry 时检查
	// DSN-50: 链下 DAG 链深上限（从 TimeoutConfig 传入；<=0 用默认 5）
	maxChainDepth int
	state         *RetrySchedulerStateAccessor
	comm          *Comm
	selfShard     uint32
}

// ─── 被动池接口 ────────────────────────────────────────────────

func (rs *retryScheduler) AddToPassivePool(txHash common.Hash) {
	rs.passivePool.Store(txHash, struct{}{})
	chainRetryStats.SigPassiveAdd.Add(1)
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Msg("AddToPassivePool: entered passive pool")
}

func (rs *retryScheduler) RemoveFromPassivePool(txHash common.Hash) {
	t0 := time.Now()
	rs.passivePool.Delete(txHash)
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Str("duration", time.Since(t0).String()).
		Msg("RemoveFromPassivePool timing")
}

func (rs *retryScheduler) IsInPassivePool(txHash common.Hash) bool {
	_, exists := rs.passivePool.Load(txHash)
	return exists
}

func (rs *retryScheduler) cycle() {
	// 定期输出 CHAIN_RETRY_STATS（DAG / retry pipeline 差分统计），
	// 供 remote_ssc_retry_stats.py / local-ssc-retry-stats.py 解析。
	// 之前只在新服务创建时 dump 一次（全 0），cycle 内从不输出，
	// 导致 DAG 使用情况（PatchHit/PatchMiss、链式提交分布）无法观测。
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-rs.ctx.Done():
			dumpChainRetryStats()
			return
		case <-ticker.C:
			dumpChainRetryStats()
		}
	}
}

func (rs *retryScheduler) printState() {
	// printState uses sync.Map retryPool/signals — disabled for now
	// txs2retry := make(map[common.Hash][]bool)
	// rs.retryPool.Range(func(hash, txVal interface{}) bool {
	// 	tx := txVal.(*api.RetryTx)
	// 	...
	// 	return true
	// })
	utils.SSCLogger().Debug().Msg("retry txs (printState disabled)")
}

// Stats 返回 retryScheduler 各池/索引的条目数，供监控分析资源释放情况。
func (rs *retryScheduler) Stats() api.RetrySchedulerStatus {
	var st api.RetrySchedulerStatus
	if rs == nil {
		return st
	}
	rs.retryPool.Range(func(_, _ interface{}) bool { st.RetryPool++; return true })
	rs.passivePool.Range(func(_, _ interface{}) bool { st.PassivePool++; return true })
	rs.staleTxs.Range(func(_, _ interface{}) bool { st.StaleTxs++; return true })
	rs.signals.Range(func(_, _ interface{}) bool { st.Signals++; return true })
	// 链下已合并为单一 offChainDAG；Patches 与 LocalPatches 现在统计同一个 nodes 集合（保留字段以兼容监控）
	rs.offChainDAG.nodes.Range(func(_, _ interface{}) bool { st.Patches++; return true })
	rs.onChainPatches.Range(func(_, _ interface{}) bool { st.OnChainPatches++; return true })
	rs.offChainDAG.nodes.Range(func(_, _ interface{}) bool { st.LocalPatches++; return true })
	rs.offChainDAG.keyIndex.Range(func(_, _ interface{}) bool { st.KeyIndex++; return true })
	rs.offChainDAG.subscriber.Range(func(_, _ interface{}) bool { st.Subscriber++; return true })
	rs.offChainDAG.txSubKeys.Range(func(_, _ interface{}) bool { st.TxSubKeys++; return true })
	rs.consumedPatches.Range(func(_, _ interface{}) bool { st.ConsumedPatches++; return true })
	rs.reSimInFlight.Range(func(_, _ interface{}) bool { st.ReSimInFlight++; return true })
	rs.woundedRetryTxs.Range(func(_, _ interface{}) bool { st.WoundedRetryTxs++; return true })
	if rs.lockWait != nil {
		st.LockWait = rs.lockWait.Len()
	}
	return st
}

func (rs *retryScheduler) CallForRetry(tx *api.RetryTx) {
	if !rs.state.IsLeader(tx.Epochs[rs.selfShard]) {
		return
	}

	utils.SSCLogger().Debug().
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

	// 从 SimulationCallStates 提取 RWSet（含 ForceSimulation 的 LockedKeys）
	// 注意：GetState 撞锁时（simulator.go），冲突 key 只记入 callState.LockedKeys，
	// 不会进 RWSet.WriteState。若不并入，retry 订阅不到这些 key，
	// OnBlockCommitted 的 querySubscribers(releasedKeys) 永远选不中该交易 → 永不唤醒 → 超时。
	readSet := make([]api.LockKey, 0)
	writeSet := make([]api.LockKey, 0)
	writeSeen := make(map[api.LockKey]struct{})
	addWrite := func(k api.LockKey) {
		if _, ok := writeSeen[k]; ok {
			return
		}
		writeSeen[k] = struct{}{}
		writeSet = append(writeSet, k)
	}
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
							addWrite(api.FormKey(addr, key))
						}
					}
				}
				// ForceSimulation 冲突 key（GetState/SetState 撞锁时记录）
				for _, k := range callState.LockedKeys {
					addWrite(k)
				}
			}
		}
	}
	tx.ReadSet = readSet
	tx.WriteSet = writeSet

	// 防重复
	if _, exists := rs.retryPool.Load(tx.TxHash); exists {
		utils.SSCLogger().Debug().Str("tx", tx.TxHash.Hex()).Msg("retry tx already exists")
		return
	}

	// 立即检查是否已 stale（如刚收到就过期）
	if rs.isStale(tx) {
		utils.SSCLogger().Debug().Str("tx", tx.TxHash.Hex()).Msg("skip adding stale tx to retry pool")
		return
	}

	rs.retryPool.Store(tx.TxHash, tx)
	// Subscribe to PatchPool keys for incremental scanning
	rs.offChainDAG.subscribeRetryTx(tx.TxHash, tx.ReadSet, tx.WriteSet)
	rs.state.RetryAddCount()
	utils.SSCLogger().Debug().
		Str("txHash", tx.TxHash.Hex()).
		Interface("relatedShards", tx.RelatedShards).
		Int("reads", len(tx.ReadSet)).
		Int("writes", len(tx.WriteSet)).
		Msg("added to retry pool")
}

// OnBlockCommitted 在区块提交后调用，尝试提升重试池中的交易
func (rs *retryScheduler) OnBlockCommitted(block *types.Block) {
	t0 := time.Now()
	t0Stale := t0
	t0Cache := t0

	defer func() {
		perf.RecordPkg("retryScheduler", "OnBlockCommitted", "total", time.Since(t0))
	}()

	// 首先处理临时锁并获取释放的 key
	releasedKeys := rs.tempLockView.OnBlockCommitted(block)

	// 定期更新热键集合
	// OLD: updateHotKeys 已废弃，由 subscriber 替代
	// rs.updateHotKeys()

	var staleNum int
	rs.staleTxs.Range(func(txHash, _ interface{}) bool {
		staleNum++
		txh := txHash.(common.Hash)
		rs.offChainDAG.unsubscribeRetryTx(txh)
		rs.retryPool.Delete(txh)
		return true
	})
	rs.staleTxs.Range(func(txHash, _ interface{}) bool {
		rs.staleTxs.Delete(txHash)
		return true
	})
	perf.RecordPkg("retryScheduler", "OnBlockCommitted", "staleCleanup", time.Since(t0Stale))

	currentBlock := block.NumberU64()
	t0Cache = time.Now()

	// 缓存当前区块的 stateDB
	// - 信号聚合时预检本分片 stateDB 锁状态（原有用途）
	// - RetryCommit Phase 2 的 CheckLock（DSN-25 新增）
	// - StartReSimulation 的 block hash（DSN-25 新增）
	var currentStateDB api.StateDB
	if rs.bc != nil {
		if stateAt, err := rs.bc.State(); err == nil {
			currentStateDB = stateAt
			rs.cachedState.Store(stateAt)
			rs.cachedBlockNum = currentBlock
			rs.cachedBlockHash = block.Hash()
		}
	}
	perf.RecordPkg("retryScheduler", "OnBlockCommitted", "cacheState", time.Since(t0Cache))

	t0Query := time.Now()
	shard2SignalReadyNum := make(map[uint32]int)
	shard2Epoch2RetrySignals := make(map[uint32]map[api.Epoch]*api.RetrySignals)
	for i := uint32(0); i < rs.state.ShardNum(); i++ {
		shard2SignalReadyNum[i] = 0
		shard2Epoch2RetrySignals[i] = make(map[api.Epoch]*api.RetrySignals)
	}

	// 通过 subscriber 索引查哪些 retryTx 依赖被释放的 key，替代全量 retryPool.Range
	// 然后按优先级 reservation：同一 key 只 promotion 票数最高的 tx，避免多 tx 争锁
	var checkedCount int
	var selected int
	var reservedKeys int
	candidates := rs.offChainDAG.querySubscribers(releasedKeys)
	perf.RecordPkg("retryScheduler", "OnBlockCommitted", "querySubscribers", time.Since(t0Query))

	// DSN-52 §14：TLV releasedKeys 在负载停后可能长期为 0，导致 retryPool 永不 promote
	// （即使 on-chain 锁已被释放/wound）。这里兜底扫描 retryPool 中「on-chain 锁全部空闲」的
	// 交易，并入 promote 候选，让 wound 循环能持续排空 pool、交易得以收敛。
	t0Scan := time.Now()
	candidateSet := make(map[common.Hash]struct{}, len(candidates))
	for _, h := range candidates {
		candidateSet[h] = struct{}{}
	}
	onChainFreeAdded := 0
	if currentStateDB != nil {
		rs.retryPool.Range(func(txHashVal, txVal interface{}) bool {
			txHash := txHashVal.(common.Hash)
			if _, inPassive := rs.passivePool.Load(txHash); inPassive {
				return true
			}
			if _, done := candidateSet[txHash]; done {
				return true
			}
			tx, ok := txVal.(*api.RetryTx)
			if !ok || tx == nil {
				return true
			}
			// 仅当该交易的所有 on-chain 锁空闲时才纳入候选（链上不允许 wound，冲突交给 Verify 的 Wait-Die）
			for _, key := range tx.WriteSet {
				if err := currentStateDB.CheckLock(key, txHash); err != nil {
					return true // 仍有 on-chain 锁冲突，跳过
				}
			}
			for _, key := range tx.ReadSet {
				if err := currentStateDB.CheckLock(key, txHash); err != nil {
					return true
				}
			}
			candidateSet[txHash] = struct{}{}
			onChainFreeAdded++
			return true
		})
	}
	perf.RecordPkg("retryScheduler", "OnBlockCommitted", "onChainFreeScan", time.Since(t0Scan))

	t0Sort := time.Now()
	// 按优先级排序：nonce 越小优先级越高
	type prioTx struct {
		txHash common.Hash
		tx     *api.RetryTx
	}
	var sorted []prioTx
	for txHash := range candidateSet {
		txVal, exists := rs.retryPool.Load(txHash)
		if !exists {
			continue
		}
		tx := txVal.(*api.RetryTx)
		if _, inPassive := rs.passivePool.Load(txHash); inPassive {
			continue
		}
		sorted = append(sorted, prioTx{txHash: txHash, tx: tx})
	}
	// 按 nonce 升序（高优先级在前）
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].tx.Nonce < sorted[j].tx.Nonce
	})
	perf.RecordPkg("retryScheduler", "OnBlockCommitted", "sortCandidates", time.Since(t0Sort))

	t0Reserve := time.Now()
	// Reservation: 选中一笔 tx 后，锁定其所有 key，跳过依赖这些 key 的其他 tx
	reservedKeySet := make(map[api.LockKey]struct{})
	var selectedTx []prioTx
	for _, pt := range sorted {
		// 检查 pt 是否依赖已预留的 key
		conflict := false
		for _, key := range pt.tx.ReadSet {
			if _, exists := reservedKeySet[key]; exists {
				conflict = true
				break
			}
		}
		if !conflict {
			for _, key := range pt.tx.WriteSet {
				if _, exists := reservedKeySet[key]; exists {
					conflict = true
					break
				}
			}
		}
		if conflict {
			continue
		}
		// 选中，预留其 key
		selectedTx = append(selectedTx, pt)
		for _, key := range pt.tx.ReadSet {
			reservedKeySet[key] = struct{}{}
		}
		for _, key := range pt.tx.WriteSet {
			reservedKeySet[key] = struct{}{}
		}
	}
	perf.RecordPkg("retryScheduler", "OnBlockCommitted", "reservation", time.Since(t0Reserve))

	utils.SSCLogger().Debug().
		Int("candidateCount", len(candidateSet)).
		Int("onChainFreeAdded", onChainFreeAdded).
		Int("selected", len(selectedTx)).
		Int("reservedKeys", len(reservedKeySet)).
		Msg("[retryScheduler] OnBlockCommitted: reservation completed")

	t0CheckLock := time.Now()
	for _, pt := range selectedTx {
		txHash := pt.txHash
		tx := pt.tx
		checkedCount++
		selected++
		reservedKeys += len(reservedKeySet)
		signal := &api.RetrySignal{
			TxHash:        tx.TxHash,
			Epoch:         tx.Epochs[tx.OriginShardID],
			SimulationNum: tx.SimulationNum,
			Condition:     tx.Condition,
		}
		if rs.tempLockView.CanLock(txHash, tx.ReadSet, tx.WriteSet) {
			signal.Ready = false
			// 预检本分片 stateDB 锁：如果本 shard stateDB 锁冲突，不标记 Ready，
			// 避免浪费跨 shard RPC（tryToReSimulation → RetryCommit）
			if currentStateDB != nil {
				dbOK := true
				for _, key := range tx.WriteSet {
					if err := currentStateDB.CheckLock(key, txHash); err != nil {
						dbOK = false
						break
					}
				}
				if dbOK {
					for _, key := range tx.ReadSet {
						if err := currentStateDB.CheckLock(key, txHash); err != nil {
							dbOK = false
							break
						}
					}
				}
				signal.Ready = dbOK
			}
			if signal.Ready {
				rs.state.RetryReadySignal()
				shard2SignalReadyNum[tx.OriginShardID]++
			} else {
				rs.state.RetryNotReadySignal()
			}
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
	perf.RecordPkg("retryScheduler", "OnBlockCommitted", "stateCheckLock", time.Since(t0CheckLock))

	t0Send := time.Now()
	// 提交 promoted 交易到下一轮模拟
	for _, epoch2Signals := range shard2Epoch2RetrySignals {
		for _, signals := range epoch2Signals {
			if len(signals.Signals) > 0 {
				utils.SSCLogger().Debug().
					Int("num", len(signals.Signals)).
					Uint32("shard", signals.OriginShard).
					Msg("promoted txs to next round")
				go rs.sendReSimulationSignals(signals)
			}
		}
	}
	perf.RecordPkg("retryScheduler", "OnBlockCommitted", "sendSignals", time.Since(t0Send))

	t0Wound := time.Now()
	// 恢复被 Wound 的交易（OnBlockCommitted 时重新激活）
	var recoveredWounded []common.Hash
	rs.woundedRetryTxs.Range(func(txHashVal, _ interface{}) bool {
		txHash := txHashVal.(common.Hash)
		retryTxVal, exists := rs.retryPool.Load(txHash)
		if !exists {
			rs.woundedRetryTxs.Delete(txHash)
			return true
		}
		retryTx := retryTxVal.(*api.RetryTx)
		retryTx.Status = api.RetryActive
		rs.offChainDAG.subscribeRetryTx(txHash, retryTx.ReadSet, retryTx.WriteSet)
		rs.tempLockView.ClearWounded(txHash)
		rs.woundedRetryTxs.Delete(txHash)
		recoveredWounded = append(recoveredWounded, txHash)
		return true
	})
	perf.RecordPkg("retryScheduler", "OnBlockCommitted", "woundedRecovery", time.Since(t0Wound))

	t0HotKey := time.Now()
	// 统计本区块 retryPool 的热 key 重复度：每个 key 被多少笔 tx 写
	hotKeyStats := make(map[api.LockKey]int)
	hotKeyReadStats := make(map[api.LockKey]int)
	rs.retryPool.Range(func(_, txVal interface{}) bool {
		tx := txVal.(*api.RetryTx)
		for _, lockKey := range tx.WriteSet {
			hotKeyStats[lockKey]++
		}
		for _, lockKey := range tx.ReadSet {
			hotKeyReadStats[lockKey]++
		}
		return true
	})
	var maxWriteContention, maxReadContention int
	for _, cnt := range hotKeyStats {
		if cnt > maxWriteContention {
			maxWriteContention = cnt
		}
	}
	for _, cnt := range hotKeyReadStats {
		if cnt > maxReadContention {
			maxReadContention = cnt
		}
	}
	hotWriteKeys := 0
	for _, cnt := range hotKeyStats {
		if cnt >= 2 {
			hotWriteKeys++
		}
	}
	_ = maxWriteContention
	_ = maxReadContention
	_ = hotWriteKeys
	perf.RecordPkg("retryScheduler", "OnBlockCommitted", "hotKeyStats", time.Since(t0HotKey))

	t0LockWait := time.Now()
	// v2: 扫描 LockWait Pool — 使用已缓存的 currentStateDB 检查 stateDB 解锁
	// currentStateDB 在 lock 外已缓存，这里只做 map 操作（在 rs.mu.Lock() 保护下）
	if currentStateDB != nil {
		rs.lockWait.ForEach(func(txHash common.Hash, retryTx *api.RetryTx) bool {
			dbOK := true
			for _, key := range retryTx.WriteSet {
				if err := currentStateDB.CheckLock(key, txHash); err != nil {
					dbOK = false
					break
				}
			}
			if dbOK {
				for _, key := range retryTx.ReadSet {
					if err := currentStateDB.CheckLock(key, txHash); err != nil {
						dbOK = false
						break
					}
				}
			}
			if dbOK {
				rs.retryPool.Store(txHash, retryTx)
				utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
					Msg("lockWaitPool: stateDB unlocked, moved back to retryPool")
				return true
			}
			return false
		})
	}

	// v2: lockWaitPool 超时扫描
	var expiredLockWait []common.Hash
	rs.lockWait.ForEachEntry(func(txHash common.Hash, enterBlock uint64) {
		if rs.maxRetriesTotal > 0 && currentBlock >= enterBlock+uint64(rs.maxRetriesTotal) {
			expiredLockWait = append(expiredLockWait, txHash)
		}
	})
	for _, txHash := range expiredLockWait {
		rs.lockWait.Remove(txHash)
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Msg("lockWaitPool: expired")
		if rs.state.CloseTransaction != nil {
			rs.state.CloseTransaction(txHash, false, api.PoolTimeout.String())
		}
	}
	perf.RecordPkg("retryScheduler", "OnBlockCommitted", "lockWaitScan", time.Since(t0LockWait))

	// 清理 expiredLockWait 中使用的变量
	_ = expiredLockWait

	t0Stats := time.Now()
	// TLV + all system stats snapshot (Info level for experiment analysis)
	wLocks, rLocks, txSets, wounded := rs.tempLockView.Stats()

	// PatchPool (链下已合并为单一 offChainDAG；localPatches 与 chainPatches 均统计同一 nodes 集合)
	var localPatchCount, chainPatchCount, keyIdxCount, subCount, consumedCount int
	rs.offChainDAG.nodes.Range(func(_, _ any) bool { localPatchCount++; return true })
	rs.offChainDAG.nodes.Range(func(_, _ any) bool { chainPatchCount++; return true })
	rs.offChainDAG.keyIndex.Range(func(_, _ any) bool { keyIdxCount++; return true })
	rs.offChainDAG.subscriber.Range(func(_, _ any) bool { subCount++; return true })
	rs.consumedPatches.Range(func(_, _ any) bool { consumedCount++; return true })

	// 重试系统
	var retryCount, passiveCount, staleCount2, reSimInFlightCount, woundedRetryCount int
	rs.retryPool.Range(func(_, _ any) bool { retryCount++; return true })
	rs.passivePool.Range(func(_, _ any) bool { passiveCount++; return true })
	rs.staleTxs.Range(func(_, _ any) bool { staleCount2++; return true })
	rs.reSimInFlight.Range(func(_, _ any) bool { reSimInFlightCount++; return true })
	rs.woundedRetryTxs.Range(func(_, _ any) bool { woundedRetryCount++; return true })

	// 信号 & 链上 Patch
	var signalTxCount, onChainPatchCount int
	rs.signals.Range(func(_, _ any) bool { signalTxCount++; return true })
	rs.onChainPatches.Range(func(_, _ any) bool { onChainPatchCount++; return true })

	// stateLockManager 全局状态
	var globalLocked, globalRLocked, globalFinished, globalLockStart int
	if rs.state != nil && rs.state.LockStatesStats != nil {
		globalLocked, globalRLocked, globalFinished, globalLockStart = rs.state.LockStatesStats()
	}
	perf.RecordPkg("retryScheduler", "OnBlockCommitted", "statsRange", time.Since(t0Stats))

	_ = staleCount2
	_ = wLocks

	utils.SSCLogger().Info().
		Uint64("blockNum", currentBlock).
		// TLV
		Int("tlvWrites", wLocks).
		Int("tlvReads", rLocks).
		Int("tlvTxSets", txSets).
		Int("tlvWounded", wounded).
		// PatchPool
		Int("localPatches", localPatchCount).
		Int("chainPatches", chainPatchCount).
		Int("keyIndex", keyIdxCount).
		Int("subscriber", subCount).
		Int("consumedPatches", consumedCount).
		// Retry
		Int("retryPool", retryCount).
		Int("passivePool", passiveCount).
		Int("staleTxs", staleCount2).
		Int("reSimInFlight", reSimInFlightCount).
		Int("woundedRetryTxs", woundedRetryCount).
		Int("lockWait", rs.lockWait.Len()).
		// Signals
		Int("signals", signalTxCount).
		Int("onChainPatches", onChainPatchCount).
		// Global lock states
		Int("globalLocked", globalLocked).
		Int("globalRLocked", globalRLocked).
		Int("globalFinished", globalFinished).
		Int("globalLockStart", globalLockStart).
		Msg("[retryScheduler] OnBlockCommitted stats")
}

func (rs *retryScheduler) sendReSimulationSignals(signals *api.RetrySignals) {
	leader := rs.state.GetLeader(signals.Epoch, signals.OriginShard)

	err := rs.comm.Call(rs.ctx, nil, leader, api.Method_SignalReSimulation, signals)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("send signals resimulation failed")
	}
}

// OLD: updateHotKeys — 已废弃，由 OnPatchPoolUpdated 替代
/*
func (rs *retryScheduler) updateHotKeys() { ... }
func (rs *retryScheduler) isHotKey(key api.LockKey) bool { ... }
*/

// HandleRetrySignal 接收来自 SimTx 提交 shard 的 chain signal，
// 在 origin shard 存储 ChainPatch 后通过 signal aggregation 触发 tryToReSimulation。
func (rs *retryScheduler) HandleRetrySignal(signal *api.RetrySignal) {
	chainRetryStats.SigRetrySignalReceived.Add(1)
	utils.SSCLogger().Debug().Str("txHash", signal.TxHash.Hex()).
		Bool("hasPatch", signal.ChainPatch != nil).
		Msg("HandleRetrySignal: received")

	if signal.ChainPatch != nil {
		rs.state.SetChainPatch(signal.TxHash, signal.ChainPatch)
	}

	// 收到一个 chain signal 就直接 tryToReSimulation
	// chain 路径的 signal 是强信号，不需要等所有 shard 的信号聚合
	retryTxVal, exists := rs.retryPool.Load(signal.TxHash)
	var retryTx *api.RetryTx
	if exists {
		retryTx = retryTxVal.(*api.RetryTx)
	}
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

	for _, signal := range signals.Signals {
		txHash := signal.TxHash
		rs.setSignal(signals.FromShard, signal)
		readyCnt := 0
		sigPMVal, _ := rs.signals.Load(txHash)
		sigPM, _ := sigPMVal.(*signalMap)
		if sigPM != nil {
			sigPM.mu.Lock()
			cachedSignals := sigPM.m[signal.SimulationNum]
			for _, s := range cachedSignals {
				if s.Ready {
					readyCnt++
				}
			}
			sigPM.mu.Unlock()
		}
		retryTxVal2, _ := rs.retryPool.Load(txHash)
		retryTx, _ := retryTxVal2.(*api.RetryTx)
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
		var bestTx common.Hash
		var bestPri api.Priority
		first := true
		rs.retryPool.Range(func(txh, txVal interface{}) bool {
			tx := txVal.(*api.RetryTx)
			pri := api.Priority{
				Nonce:         tx.Nonce,
				OriginShardID: tx.OriginShardID,
				TxHash:        txh.(common.Hash),
			}
			if first || pri.Less(bestPri) {
				bestPri = pri
				bestTx = txh.(common.Hash)
				first = false
			}
			return true
		})
		if !first {
			utils.SSCLogger().Debug().
				Str("txHash", txHash.Hex()[:20]).
				Str("bestTx", bestTx.Hex()[:20]).
				Uint64("bestNonce", bestPri.Nonce).
				Uint32("bestOriginShardID", bestPri.OriginShardID).
				Msg("tryToReSimulation: retry pool stats")
		}
	}()

	// 防重入：同一笔 tx 的 reSim 已经在进行中则跳过
	if _, inFlight := rs.reSimInFlight.Load(txHash); inFlight {
		chainRetryStats.SigRetryCommitTryLockFail.Add(1)
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Msg("tryToReSimulation: already in flight, skipping")
		return
	}
	rs.reSimInFlight.Store(txHash, struct{}{})

	// 函数退出时清理 inFlight 标记
	defer func() {
		rs.reSimInFlight.Delete(txHash)
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
			utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
				Msg("retry commit success but local tx was wounded, aborting")
			// 记录为 wounded 状态
			retryTx.Status = api.RetryWounded
			rs.woundedRetryTxs.Store(txHash, struct{}{})
			rs.offChainDAG.unsubscribeRetryTx(txHash)
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
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Uint64("nonce", retryTx.Nonce).
			Uint32("originShardId", retryTx.OriginShardID).
			Int("simulationNum", retryTx.SimulationNum).
			Msg("retry commit success")
		go rs.state.TriggerReSimulation(retryTx.TxHash, retryTx.SimulationNum)
		chainRetryStats.SigTriggerReSim.Add(1)
		rs.signals.Delete(txHash)
		rs.retryPool.Delete(txHash)
		rs.offChainDAG.unsubscribeRetryTx(txHash)
		rs.consumedPatches.Delete(txHash)
	} else {
		rs.state.RetryFailCount()
		chainRetryStats.SigRetryCommitFailed.Add(1)
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msg("retry commit failed")

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
					rs.AddToPassivePool(txHash)
				} else {
					go func(leader *api.Member) {
						if err := rs.comm.Call(rs.ctx, nil, leader, api.Method_AddToPassivePool, txHash); err != nil {
							utils.SSCLogger().Error().Err(err).Str("txHash", txHash.Hex()).
								Msg("tryToReSimulation: failed to notify passive pool")
						}
					}(leader)
				}
			}
		}

		// v2: 如果存在 OnChainLockConflict，移到 LockWait Pool
		// LockWait Pool 中交易不在 retryPool 中，OnBlockCommitted 扫描 stateDB 解锁后移回
		if hasOnChainConflict {
			rs.lockWait.Add(txHash, rs.bc.CurrentHeader().NumberU64(), retryTx)
			rs.retryPool.Delete(txHash)
			rs.signals.Delete(txHash)
			utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
				Msg("tryToReSimulation: moved to lockWaitPool (stateDB conflict, no Patch)")
		}

		// v4: Release consumed PatchPool entries so other retryTxs can use them
		if consumedTxHashesVal, exists := rs.consumedPatches.Load(txHash); exists {
			consumedTxHashes := consumedTxHashesVal.([]common.Hash)
			for _, consumedSimTx := range consumedTxHashes {
				rs.offChainDAG.releasePatch(consumedSimTx)
				utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
					Str("releasedPatch", consumedSimTx.Hex()).
					Msg("retry commit failed: released consumed patch")
			}
			rs.consumedPatches.Delete(txHash)
			// Re-subscribe retryTx to subscriber index (was unsubscribed when consumed)
			if retryTx != nil {
				retryTx.Status = api.RetryActive
				rs.offChainDAG.subscribeRetryTx(txHash, retryTx.ReadSet, retryTx.WriteSet)
			}
		}
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
				if sigPMVal2, _ := rs.signals.Load(txHash); sigPMVal2 != nil {
					sigPM2 := sigPMVal2.(*signalMap)
					sigPM2.mu.Lock()
					if sigPM2.m[retryTx.SimulationNum] != nil &&
						sigPM2.m[retryTx.SimulationNum][shardId] != nil {
						sigPM2.m[retryTx.SimulationNum][shardId].Ready = false
					}
					sigPM2.mu.Unlock()
				}
			}
		}
	}
}

func (rs *retryScheduler) setSignal(fromShard uint32, signal *api.RetrySignal) {
	sigPMVal, _ := rs.signals.LoadOrStore(signal.TxHash, &signalMap{m: make(map[int]map[uint32]*api.RetrySignal)})
	sigPM := sigPMVal.(*signalMap)
	sigPM.mu.Lock()
	m2 := sigPM.m[signal.SimulationNum]
	if m2 == nil {
		m2 = make(map[uint32]*api.RetrySignal)
		sigPM.m[signal.SimulationNum] = m2
	}
	m2[fromShard] = signal
	sigPM.mu.Unlock()
	utils.SSCLogger().Debug().
		Str("tx", signal.TxHash.Hex()).
		Int("simulationNum", signal.SimulationNum).
		Uint32("fromShard", fromShard).
		Bool("ready", signal.Ready).
		Msg("set signal")
}

func (rs *retryScheduler) isStale(tx *api.RetryTx) bool {
	_, exists := rs.staleTxs.Load(tx.TxHash)
	return exists
}

func (rs *retryScheduler) StaleTx(txHash common.Hash) {
	t0 := time.Now()
	rs.tempLockView.GarbageCollect(txHash)

	rs.staleTxs.Store(txHash, struct{}{})
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Str("duration", time.Since(t0).String()).
		Msg("retryScheduler.StaleTx timing")
}

func (rs *retryScheduler) RetryCommit(txHash common.Hash) *api.RetryCommitResp {
	chainRetryStats.SigRetryCommitCalled.Add(1)
	t0 := time.Now()
	defer func() {
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Dur("cost", time.Since(t0)).
			Msg("retryCommit timing")
	}()

	// v2: 如果在被动池中，移出（被 O 的 RetrySignal 唤醒）
	if _, inPassive := rs.passivePool.Load(txHash); inPassive {
		rs.passivePool.Delete(txHash)
		chainRetryStats.SigPassiveWaken.Add(1)
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Msg("retryCommit: woken from passive pool")
	}

	// Check if this tx has been Wounded — if so, return failure immediately
	if rs.tempLockView.IsWounded(txHash) {
		chainRetryStats.SigRetryCommitWoundedPre.Add(1)
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msg("retryCommit: wounded by higher priority tx")
		return &api.RetryCommitResp{Locked: false, TxHash: txHash}
	}

	retryTxVal2, _ := rs.retryPool.Load(txHash)
	retryTx, _ := retryTxVal2.(*api.RetryTx)
	if retryTx == nil {
		return &api.RetryCommitResp{Locked: false, TxHash: txHash}
	}
	if !rs.state.IsLeader(retryTx.Epochs[rs.selfShard]) {
		return &api.RetryCommitResp{Locked: false, TxHash: txHash}
	}

	// Check if PatchPool has already consumed patches for this tx via OnPatchPoolUpdated.
	// If so, the retryTx already has covering patches — skip TLV/stateDB locks.
	if _, consumed := rs.consumedPatches.Load(txHash); consumed {
		chainRetryStats.SigRetryCommitPatchHit.Add(1)
		chainRetryStats.MonitorRetryCommitPatchHit.Add(1)
		rs.tempLockView.ClearWounded(txHash)
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Msg("retryCommit: consumed patches found, skipping TLV locks")
		return &api.RetryCommitResp{Locked: true, TxHash: txHash}
	}

	// 构建优先级
	priority := api.Priority{
		Nonce:         retryTx.Nonce,
		OriginShardID: retryTx.OriginShardID,
		TxHash:        txHash,
	}

	// ──────────────────────────────────────────────
	// Phase 1: TempLockView 锁竞争
	// ──────────────────────────────────────────────
	t0TLV := time.Now()
	locked, wounded := rs.tempLockView.TryLockWithPriority(txHash, priority, retryTx.ReadSet, retryTx.WriteSet)
	perf.RecordPkg("retryScheduler", "RetryCommit", "tlvTryLock", time.Since(t0TLV))
	if wounded {
		chainRetryStats.SigRetryCommitTryLockWounded.Add(1)
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msg("retryCommit: wounded by higher priority tx")
		return &api.RetryCommitResp{Locked: false, TxHash: txHash}
	}
	if !locked {
		chainRetryStats.SigRetryCommitTryLockFail.Add(1)
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Int("writeKeys", len(retryTx.WriteSet)).
			Int("readKeys", len(retryTx.ReadSet)).
			Msg("retryCommit failed: try lock failed")

		// ── Phase 1b: PatchPool DAG 补救 — 用 Patch 覆盖 TLV 冲突 key ──
		// TLV TryLock 失败，说明至少有一个 key 被其他 tx 锁着。
		// 取 ReadSet ∪ WriteSet 作为冲突 key 集（与 OnPatchPoolUpdated 对齐），
		// 查 PatchPool 是否有 Patch 联合覆盖全部 key。
		// 如果可以覆盖，则直接跳过 TLV 锁，用 Patch 中的值进行重试模拟。
		t0Dag1 := time.Now()
		allKeys := make([]api.LockKey, 0, len(retryTx.ReadSet)+len(retryTx.WriteSet))
		allKeys = append(allKeys, retryTx.ReadSet...)
		allKeys = append(allKeys, retryTx.WriteSet...)
		// 注：RetryCommit 只“消费/补救”，不构建新的上游边，故 maxSimNum=0（不做严格更早过滤），避免回归既有 rescue；仅排除自己防自环。
		patches := rs.offChainDAG.findCoveringSet(allKeys, txHash, 0)
		perf.RecordPkg("retryScheduler", "RetryCommit", "dagSearchPhase1b", time.Since(t0Dag1))
		if len(patches) > 0 && rs.offChainDAG.isFullyCovered(allKeys, patches) && chainDepthOf(patches) <= rs.maxChainDepth {
			t0Cons := time.Now()
			var consumedTxHashes []common.Hash
			var merged *api.RWSet
			allConsumed := true
			for _, node := range patches {
				patch := rs.offChainDAG.tryConsumePatch(node.TxHash, txHash, priority)
				if patch == nil {
					allConsumed = false
					break
				}
				consumedTxHashes = append(consumedTxHashes, node.TxHash)
				merged = mergeRWSet(patch, merged)
			}
			if allConsumed {
				perf.RecordPkg("retryScheduler", "RetryCommit", "dagConsumePhase1b", time.Since(t0Cons))
				chainRetryStats.SigRetryCommitPatchHit.Add(1)
				chainRetryStats.MonitorRetryCommitPatchHit.Add(1)
				rs.state.SetChainPatch(txHash, merged)
				rs.consumedPatches.Store(txHash, consumedTxHashes)
				utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
					Int("patchCount", len(patches)).
					Int("coveredKeys", len(retryTx.WriteSet)).
					Msg("retryCommit: found DAG patches in PatchPool, rescued from TLV lock conflict")
				return &api.RetryCommitResp{Locked: true, TxHash: txHash}
			}
			// 部分消费失败 → 回滚
			for _, txh := range consumedTxHashes {
				rs.offChainDAG.releasePatch(txh)
			}
			chainRetryStats.SigRetryCommitPatchMiss.Add(1)
			chainRetryStats.MonitorRetryCommitPatchMiss.Add(1)
		}

		// DSN-50: 因覆盖不全或链深上限放弃救援，记录深度上限次数（若超深）
		if len(patches) > 0 && chainDepthOf(patches) > rs.maxChainDepth {
			chainRetryStats.SigChainDepthCapped.Add(1)
		}
		return &api.RetryCommitResp{Locked: false, TxHash: txHash}
	}

	// ──────────────────────────────────────────────
	// Phase 2: stateDB 真实锁检查
	// ──────────────────────────────────────────────
	// 优先使用 OnBlockCommitted 缓存的 stateDB（DSN-25）：与 StartReSimulation 使用同一版本
	t0State := time.Now()
	stateDB, ok := rs.getCachedStateDB()
	if !ok {
		// 缓存不可用（启动初期）→ fallback 到 live bc.State()
		var err error
		stateDB, err = rs.getStateDB(txHash)
		if err != nil {
			rs.tempLockView.GarbageCollect(txHash)
			chainRetryStats.SigRetryCommitTryLockFail.Add(1)
			utils.SSCLogger().Error().Err(err).Str("txHash", txHash.Hex()).Msgf("retryCommit failed: stateDB is not exists")
			return &api.RetryCommitResp{Locked: false, TxHash: txHash}
		}
	}

	// 收集所有 stateDB 和 TempLock 层面冲突的 key。
	// 严格门：链上锁冲突一律视为真实冲突（链上不允许 wound，冲突交给 Verify 的 Wait-Die 处理）；
	// 冲突是否可被 TLV wound / DAG patch 化解，由 Phase 1（TryLockWithPriority）与 Phase 1b/2b（PatchPool）处理。
	tCheckLock := time.Now()
	var conflictKeys []api.LockKey
	for _, key := range retryTx.WriteSet {
		if err := stateDB.CheckLock(key, txHash); err != nil {
			conflictKeys = append(conflictKeys, key)
		} else if rs.tempLockView.HasConflict(txHash, key) {
			// TLV 也有冲突 → 加入 conflictKeys 让 PatchPool 覆盖
			conflictKeys = append(conflictKeys, key)
		}
	}
	for _, key := range retryTx.ReadSet {
		if err := stateDB.CheckLock(key, txHash); err != nil {
			conflictKeys = append(conflictKeys, key)
		} else if rs.tempLockView.HasConflict(txHash, key) {
			conflictKeys = append(conflictKeys, key)
		}
	}
	perf.RecordPkg("retryScheduler", "RetryCommit", "stateCheckLock", time.Since(t0State))
	if tCheckLockDur := time.Since(tCheckLock); tCheckLockDur > 50*time.Millisecond {
		utils.SSCLogger().Warn().Str("txHash", txHash.Hex()).
			Dur("checkLock", tCheckLockDur).
			Int("writeKeys", len(retryTx.WriteSet)).
			Int("readKeys", len(retryTx.ReadSet)).
			Msg("RetryCommit Phase2 CheckLock: slow")
	}

	if len(conflictKeys) == 0 {
		// 全部通过 → 成功
		rs.tempLockView.ClearWounded(txHash)
		chainRetryStats.SigRetryCommitTryLockOk.Add(1)
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msg("retryCommit")
		return &api.RetryCommitResp{Locked: true, TxHash: txHash}
	}

	// ──────────────────────────────────────────────
	// Phase 2b: PatchPool DAG 补救 — 多 Patch 联合覆盖冲突 key
	// ──────────────────────────────────────────────
	chainRetryStats.SigRetryCommitTryLockFail.Add(1)
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Int("conflictKeys", len(conflictKeys)).
		Msg("retryCommit failed: stateDB lock conflict")

	// DSN-50: 找一组 Patch 联合覆盖完整已知读写集（而非仅 conflictKeys），并做覆盖完整性 + 链深闸门
	// 注：RetryCommit 只“消费/补救”，不构建新边，maxSimNum=0 保留既有 rescue；仅排除自己防自环。
	needKeys := make([]api.LockKey, 0, len(retryTx.ReadSet)+len(retryTx.WriteSet))
	needKeys = append(needKeys, retryTx.ReadSet...)
	needKeys = append(needKeys, retryTx.WriteSet...)
	t0Dag2 := time.Now()
	patches2 := rs.offChainDAG.findCoveringSet(needKeys, txHash, 0)
	perf.RecordPkg("retryScheduler", "RetryCommit", "dagSearchPhase2b", time.Since(t0Dag2))
	if len(patches2) > 0 && rs.offChainDAG.isFullyCovered(needKeys, patches2) && chainDepthOf(patches2) <= rs.maxChainDepth {
		// 原子消费：逐个 TryConsume，任一失败则全部 Release
		t0Cons2 := time.Now()
		var consumedTxHashes []common.Hash
		var merged *api.RWSet
		allConsumed := true
		for _, node := range patches2 {
			patch := rs.offChainDAG.tryConsumePatch(node.TxHash, txHash, priority)
			if patch == nil {
				allConsumed = false
				break
			}
			consumedTxHashes = append(consumedTxHashes, node.TxHash)
			merged = mergeRWSet(patch, merged)
		}
		if !allConsumed {
			// 部分消费失败 → 回滚已消费的
			for _, txh := range consumedTxHashes {
				rs.offChainDAG.releasePatch(txh)
			}
			chainRetryStats.SigRetryCommitPatchMiss.Add(1)
			chainRetryStats.MonitorRetryCommitPatchMiss.Add(1)
		} else {
			perf.RecordPkg("retryScheduler", "RetryCommit", "dagConsumePhase2b", time.Since(t0Cons2))
			// 全部消费成功
			chainRetryStats.SigRetryCommitPatchHit.Add(1)
			chainRetryStats.MonitorRetryCommitPatchHit.Add(1)
			rs.state.SetChainPatch(txHash, merged)
			rs.consumedPatches.Store(txHash, consumedTxHashes)
			rs.tempLockView.ClearWounded(txHash)
			utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
				Int("patchCount", len(patches2)).
				Int("coveredKeys", len(conflictKeys)).
				Msg("retryCommit: found DAG patches in PatchPool, skipping lock conflict")
			return &api.RetryCommitResp{Locked: true, TxHash: txHash}
		}
	}

	// DSN-50: 因覆盖不全或链深上限放弃救援，记录深度上限次数（若超深）
	if len(patches2) > 0 && chainDepthOf(patches2) > rs.maxChainDepth {
		chainRetryStats.SigChainDepthCapped.Add(1)
	}
	// 无 Patch 覆盖 → 释放 tempLockView 锁 + OnChainLockConflict
	rs.tempLockView.GarbageCollect(txHash)
	return &api.RetryCommitResp{
		Locked:              false,
		TxHash:              txHash,
		OnChainLockConflict: true,
	}
}

func (rs *retryScheduler) getStateDB(txHash common.Hash) (api.StateDB, error) {
	stateAt, err := rs.bc.State()
	if err != nil {
		return nil, err
	}
	return stateAt, nil
}

// getCachedStateDB 返回 OnBlockCommitted 时缓存的 stateDB。
// 和 RetryCommit Phase 2 + StartReSimulation 使用同一版本 → 消除 CheckLock 与 Lockable 的 race。
// 缓存不可用时（启动初期）fallback 到 live bc.State()。
func (rs *retryScheduler) getCachedStateDB() (api.StateDB, bool) {
	v := rs.cachedState.Load()
	if v == nil {
		return nil, false
	}
	return v.(api.StateDB), true
}

// GetCachedBlockHash 返回 OnBlockCommitted 时缓存的 block hash。
// 供 StartReSimulation 使用缓存的 state 版本执行模拟。
func (rs *retryScheduler) GetCachedBlockHash() common.Hash {
	return rs.cachedBlockHash
}

func (rs *retryScheduler) RetryCancel(txHash common.Hash) {
	rs.tempLockView.GarbageCollect(txHash)
}

// sendChainSignal 发送链式重试信号到 origin shard。
func (rs *retryScheduler) sendChainSignal(txHash common.Hash, retryTx *api.RetryTx, writeSet *api.RWSet, upstreamTxList []api.TxSimKey) {
	// 存储到链下单一 DAG（offChainDAG.nodes），供后续 readPatchChain/GetUpstreamTxRef 读取。
	// 原 sendChainSignal 会在此构造一份 api.ChainNode 存入 patches；
	// 合并后 offChainDAG.AddNode 已同时记录 WriteSet + UpstreamTxList，无需再落地第二份存储。
	rs.offChainDAG.AddNode(txHash, retryTx.SimulationNum, writeSet, upstreamTxList)

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

// readPatchChain 递归查 offChainDAG 链，读到就停。
// 带 visited 集合防止 DAG 成环导致无限递归（stack overflow）。
func (rs *retryScheduler) readPatchChain(txHash common.Hash, simNum int,
	address common.Address, key common.Hash) (common.Hash, bool) {
	return rs.readPatchChainVisited(txHash, simNum, address, key, make(map[api.TxSimKey]struct{}))
}

func (rs *retryScheduler) readPatchChainVisited(txHash common.Hash, simNum int,
	address common.Address, key common.Hash, visited map[api.TxSimKey]struct{}) (common.Hash, bool) {

	id := api.TxSimKey{TxHash: txHash, SimulationNum: simNum}
	if _, ok := visited[id]; ok {
		// DAG 环：该节点已访问过，避免无限递归
		return common.Hash{}, false
	}
	visited[id] = struct{}{}

	nodeVal, ok := rs.offChainDAG.nodes.Load(txHash)
	if !ok {
		return common.Hash{}, false
	}
	node := nodeVal.(*OffChainPatchNode)
	if node.SimulationNum != simNum {
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

	// 自己没有，遍历所有上游递归查
	for _, up := range node.UpstreamTxList {
		if val, found := rs.readPatchChainVisited(up.TxHash, up.SimulationNum, address, key, visited); found {
			return val, true
		}
	}

	return common.Hash{}, false
}

// GetChainPatchRef 返回指定 tx 的链式 patch 引用（TxSimKey）。
// 用于 VerifySimulation 判断是否跳过锁冲突检查。
// 注意：返回的 TxSimKey 是当前 tx 自己的 hash，不是上游的。
// 要获取上游信息请使用 GetUpstreamTxRef。
func (rs *retryScheduler) GetChainPatchRef(txHash common.Hash) *api.TxSimKey {
	nodeVal, ok := rs.offChainDAG.nodes.Load(txHash)
	if !ok {
		return nil
	}
	node := nodeVal.(*OffChainPatchNode)
	return &api.TxSimKey{
		TxHash:        node.TxHash,
		SimulationNum: node.SimulationNum,
	}
}

// GetUpstreamTxRef 获取指定 tx 在 offChainDAG 中的所有上游引用。
// 返回 UpstreamTxList，用于 CommitSimulation 构建 SimTx 时填写 UpstreamTxList。
func (rs *retryScheduler) GetUpstreamTxRef(txHash common.Hash) []api.TxSimKey {
	nodeVal, ok := rs.offChainDAG.nodes.Load(txHash)
	if !ok {
		return nil
	}
	node := nodeVal.(*OffChainPatchNode)
	if len(node.UpstreamTxList) > 0 {
		return node.UpstreamTxList
	}
	return nil
}

// AddOnChainPatch 将 SimTx 的 ChainPatch 以 ChainNode 形式存入 onChainPatches。
// 所有节点在收到 SimTx 多播时调用。
func (rs *retryScheduler) AddOnChainPatch(txHash common.Hash, simNum int, chainPatch *api.RWSet, upstreamTxList []api.TxSimKey) {
	pmVal, _ := rs.onChainPatches.LoadOrStore(txHash, &patchMap{m: make(map[int]*api.ChainNode)})
	pm := pmVal.(*patchMap)
	pm.mu.Lock()
	pm.m[simNum] = &api.ChainNode{
		TxHash:         txHash,
		SimulationNum:  simNum,
		Patch:          chainPatch,
		UpstreamTxList: upstreamTxList,
	}
	pm.mu.Unlock()
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Int("simNum", simNum).
		Int("upstreamCount", len(upstreamTxList)).
		Msg("AddOnChainPatch: stored from SimTx")
}

// GetOnChainPatch 从 onChainPatches 查询指定 SimTx 的 ChainNode。
func (rs *retryScheduler) GetOnChainPatch(txHash common.Hash, simNum int) *api.ChainNode {
	pmVal, _ := rs.onChainPatches.Load(txHash)
	pm, _ := pmVal.(*patchMap)
	if pm == nil {
		return nil
	}
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.m[simNum]
}

// RemoveOnChainPatch 从 onChainPatches 删除指定交易的 ChainNode。
// 在 Committer.CommitOrRollbackWithProof 中调用（CR 完成后链上清理）。
func (rs *retryScheduler) RemoveOnChainPatch(txHash common.Hash) {
	t0 := time.Now()
	rs.onChainPatches.Delete(txHash)
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Str("duration", time.Since(t0).String()).
		Msg("RemoveOnChainPatch timing")
}

// ReadOnChainPatch 从 onChainPatches 递归读取指定 SimTx 的某 key 期望值。
// 只查单个 key，不合并全部上游 WriteSet（高性能路径）。
// 返回 (value, found)，found=false 表示该 key 在上游 WriteSet 中不存在。
// DAG 多上游：先查当前 node，再遍历所有上游递归查。
// 带 visited 集合防止 DAG 成环导致无限递归（stack overflow）。
func (rs *retryScheduler) ReadOnChainPatch(txHash common.Hash, simNum int, address common.Address, key common.Hash) (common.Hash, bool) {
	return rs.readOnChainPatchVisited(txHash, simNum, address, key, make(map[api.TxSimKey]struct{}))
}

func (rs *retryScheduler) readOnChainPatchVisited(txHash common.Hash, simNum int, address common.Address, key common.Hash, visited map[api.TxSimKey]struct{}) (common.Hash, bool) {
	id := api.TxSimKey{TxHash: txHash, SimulationNum: simNum}
	if _, ok := visited[id]; ok {
		// DAG 环：该节点已访问过，避免无限递归
		return common.Hash{}, false
	}
	visited[id] = struct{}{}

	pmVal, _ := rs.onChainPatches.Load(txHash)
	pm, _ := pmVal.(*patchMap)
	if pm == nil {
		return common.Hash{}, false
	}
	pm.mu.Lock()
	node := pm.m[simNum]
	pm.mu.Unlock()
	if node == nil || node.Patch == nil || node.Patch.WriteState == nil {
		return common.Hash{}, false
	}
	// 查当前 ChainNode 的 Patch
	if addrState, ok := node.Patch.WriteState.State[address]; ok {
		if val, ok := addrState[key]; ok {
			return val, true
		}
	}
	// 遍历所有上游递归查
	for _, up := range node.UpstreamTxList {
		if val, found := rs.readOnChainPatchVisited(up.TxHash, up.SimulationNum, address, key, visited); found {
			return val, true
		}
	}
	return common.Hash{}, false
}

// OnPatchPoolUpdated 通知 RS 扫描新增 Patch 的 subscriber。
func (rs *retryScheduler) OnPatchPoolUpdated(writeSet *api.RWSet) {
	rs.scanPatchSubscribers(writeSet)
}
