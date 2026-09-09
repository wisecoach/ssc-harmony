package ssc

import (
	"context"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/core"
	corestate "github.com/harmony-one/harmony/core/state"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	sscpb "github.com/harmony-one/harmony/ssc/api/proto"
	"github.com/harmony-one/harmony/ssc/perf"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

// reservationAntiStarvationBlocks — DSN-58：一笔 retry 交易在 OnBlockCommitted reservation 里
// 连续被跳过（同 key 每块被更高优先者占掉名额）达到该块数后，当块强制放行一次，打破
// “某条跨分片腿永远不被 admission → 永不发 ready → origin ready 聚合门永不闭”的活性死锁。
const reservationAntiStarvationBlocks = 50

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
	// SigUpstreamNotReady — DSN-57 fail-closed：链式 SimTx 因“本分片该管的上游”未上链而回滚的次数。
	SigUpstreamNotReady atomic.Int64

	// 累计计数（不随 dump 重置），供 ssc_getModuleStatus 监控在实验结束时读取。
	MonitorChainTxDetected      atomic.Int64
	MonitorRetryCommitPatchHit  atomic.Int64
	MonitorRetryCommitPatchMiss atomic.Int64
	MonitorChainTxCRCommitted   atomic.Int64 // 链式(isChainTx)交易最终 CR commit 的次数（累计，不随 dump 清零）
	// MonitorDagPerBlockMaxSameKey — 单个块内同一 key 最多被几笔 SimTx 写入（累计运行最大值，
	// 仅 leader 计；供 ssc_getModuleStatus 直接读取，勿把日志差分值求和）。
	MonitorDagPerBlockMaxSameKey atomic.Int64

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

	// DSN-54: simDAGPatch 反查打点（covered-but-invisible 口径）。
	// SigSimDAGPatchHit — 成员模拟撞锁后能按 (tx,simNum) 从 simDAGPatches 反查到上游写值（应增长的修复目标）。
	// SigSimDAGPatchMiss — 撞锁且 simDAGPatches 反查不到（genuinely-uncovered，预期残留）。
	SigSimDAGPatchHit  atomic.Int64
	SigSimDAGPatchMiss atomic.Int64

	// DSN-55: chain-ready 局部 admission 打点。
	// SigChainReadyAdmission — 本块因 chain-ready(本 shard 已消费 DAG) 前插而被选中放行的 tx 次数（应随 DAG 救起增长）。
	// SigChainReadyCandidate — 每块进入候选集合的 chain-ready retryTx 次数（观测饥饿是否缓解）。
	SigChainReadyAdmission atomic.Int64
	SigChainReadyCandidate atomic.Int64
	// SigDAGHoldProtected — DSN-55 rev2：DAG 救援交易得锁后 protectHeldLocks 保锁的次数。
	SigDAGHoldProtected atomic.Int64
	// SigUpstreamHoldProtected — DSN-61：RetryCommit 救援下游时连带把“被消费的上游”也
	// protectHeldLocks 保锁的次数（按被保护的上游计数），避免上游被更高优先第三方 Wound
	// 而无法先于下游上链（否则下游被有界扣留 50 块后仍 DSN-57 回滚）。
	SigUpstreamHoldProtected atomic.Int64
	// SigUpstreamWoundedInvalidated — DSN-62 (B)：上游 U 被 Wound 时，把它从“已被某下游消费的
	// 上游”中无效化/移除的次数。避免下游 D 一直链在一个被 Wound、注定上不了链的 U 上而 50 块后回滚。
	SigUpstreamWoundedInvalidated atomic.Int64
	// SigUpstreamCommittedGate — DSN-64：CR Commit 成功被记入 committedOnChain 的次数
	// （即“上游已 commit、被记为可放行下游”的门命中次数）。
	SigUpstreamCommittedGate atomic.Int64
	// SigAntiStarvationAdmit — DSN-58：reservation 反饥饿强制放行的次数（交易被跳过>=N 块后被当块放行）。
	SigAntiStarvationAdmit atomic.Int64

	// SigChainTxCRCommitted — 被 DAG 链式救起（isChainTx，即 VerifySimulation 时 UpstreamTxList>0）
	// 的交易，最终走到 CR Commit 的交易数（仅 origin leader 累计，避免多节点重复）。
	SigChainTxCRCommitted atomic.Int64
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
	chainRetryStats.ChainLengthMu.Lock()
	chainRetryStats.ChainLengthCnt = make(map[int]int64)
	chainRetryStats.ChainCommitCnt = make(map[int]int64)
	chainRetryStats.ChainLengthMu.Unlock()
	chainRetryStats.ChainDepthMu.Lock()
	chainRetryStats.ChainDepthCnt = make(map[int]int64)
	chainRetryStats.ChainDepthCommitCnt = make(map[int]int64)
	chainRetryStats.ChainDepthMu.Unlock()
}

// recordDagNodeDepth 线程安全地累计一次“真实 DAG 节点深度”分布。
// 与 dumpChainRetryStats 共用 ChainDepthMu；懒初始化防止单测等未走 initChainRetryStats 的路径 panic。
func recordDagNodeDepth(depth int) {
	chainRetryStats.ChainDepthMu.Lock()
	if chainRetryStats.ChainDepthCnt == nil {
		chainRetryStats.ChainDepthCnt = make(map[int]int64)
	}
	chainRetryStats.ChainDepthCnt[depth]++
	chainRetryStats.ChainDepthMu.Unlock()
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
		Int64("chainTxCRCommitted", chainRetryStats.SigChainTxCRCommitted.Swap(0)).
		Int64("simDAGPatchHit", chainRetryStats.SigSimDAGPatchHit.Swap(0)).
		Int64("simDAGPatchMiss", chainRetryStats.SigSimDAGPatchMiss.Swap(0)).
		Int64("chainReadyAdmission", chainRetryStats.SigChainReadyAdmission.Swap(0)).
		Int64("chainReadyCandidate", chainRetryStats.SigChainReadyCandidate.Swap(0)).
		Int64("dagHoldProtected", chainRetryStats.SigDAGHoldProtected.Swap(0)).
		Int64("upstreamHoldProtected", chainRetryStats.SigUpstreamHoldProtected.Swap(0)).
		Int64("upstreamWoundedInvalidated", chainRetryStats.SigUpstreamWoundedInvalidated.Swap(0)).
		Int64("upstreamCommittedGate", chainRetryStats.SigUpstreamCommittedGate.Swap(0)).
		Int64("antiStarvationAdmit", chainRetryStats.SigAntiStarvationAdmit.Swap(0)).
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
	GetCommittee func(epoch api.Epoch, shardId uint32) *api.ShardSimulateCommittee
	ShardNum     func() uint32
	GetBlockHash func(txHash common.Hash) (common.Hash, bool)

	// 统计
	RetryAddCount       func()
	SampleRetryPool     func(size int)
	RetryReadySignal    func()
	RetryNotReadySignal func()
	RetrySuccessCount   func()
	RetryFailCount      func()

	// SimulationState 访问（用于从 SimulationCallStates 提取 RWSet）
	GetSimState func(txHash common.Hash) (*api.SimulationState, bool)

	// 触发重试模拟
	// 触发重试模拟
	TriggerReSimulation func(txHash common.Hash, simulationNum int)

	// 链上状态查询
	IsOnChain func(txHash common.Hash) bool // 交易是否有活跃的链上 SimTx

	// LegSscVoted — DSN-58 §4.3：origin 侧查询某 related 分片是否已放好 simNum 的腿
	// （是否已收到该分片的 CommitSSCVote：commitStates[simNum][shard] 存在）。nil = 未接线（按全 related 处理）。
	LegSscVoted func(txHash common.Hash, shard uint32, simNum int) bool

	// 直接关闭交易（用于重试超限时替代 PoolTimeout 兜底）
	CloseTransaction func(txHash common.Hash, commitOrRollback bool, reason string)

	// stateLockManager 统计（OnBlockCommitted 日志需要）
	LockStatesStats func() (globalLocked, globalRLocked, globalFinished, globalLockStart int)

	// 死锁检测：触发 victim 终局 Rollback（复用 sendRollbackVoteForDie）
	RollbackVictim func(txHash common.Hash)

	// 死锁检测（VictimTx 方案）：把一条 VictimTx 提交进本分片内部池，
	// 进块后让本分片每个 validator 各自投 rollback 票。由 impl 接到 txSubmitter。
	SubmitVictimTx func(victimTx *api.VictimTx)
}

func newRetryScheduler(ctx context.Context, bc core.BlockChain, state *RetrySchedulerStateAccessor, comm *Comm, selfShard uint32, tempLockView *TempLockView, config *api.TimeoutConfig, signerMgr api.BLSSignerMgr) *retryScheduler {
	rs := &retryScheduler{
		retryPool:         sync.Map{},
		passivePool:       sync.Map{},
		staleTxs:          sync.Map{},
		signals:           sync.Map{},
		onChainDAGPatches: sync.Map{},
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
	// 链下 DAG / PatchPool 开关：默认关（config.EnableDAG 默认 false）。
	// 关闭后整个 off-chain DAG 子系统空转（角色 A+B 一起禁用），isPatchFinalized 恒 true。
	rs.offChainDAG.SetEnabled(config != nil && config.EnableDAG)

	dumpChainRetryStats()

	// DSN-53 反向 CMH 死锁检测组件（独立于 SLM/TLV/DAG，只服务探针）
	if signerMgr != nil {
		rs.deadlockDetector = newDeadlockDetector(
			selfShard,
			state,
			comm,
			tempLockView,
			&rs.offChainDAG,
			tempLockView.stateLockManager,
			signerMgr,
		)
		rs.deadlockDetector.rollbackVictim = rs.triggerVictimRollback
		// VictimTx 方案：判环后由本（最后一跳）分片构造 VictimTx 打进本分片，
		// 让本分片每个 validator 各自投 rollback 票（替代只由 leader 直投）。
		rs.deadlockDetector.victimTxSubmit = rs.triggerVictimTxSubmit
		// edgeValid 重验证需同时看 global+pending（pending 与 global 等价，都是链上锁）。
		// 注入 pending holder 查询（指向 OnBlockCommitted 缓存的 stateDB）。
		rs.deadlockDetector.pendingHolder = func(key api.LockKey) (common.Hash, bool) {
			if sdb, ok := rs.getCachedStateDB(); ok {
				if db, ok2 := sdb.(*corestate.DB); ok2 {
					return db.FindPendingLockHolder(key)
				}
			}
			return common.Hash{}, false
		}
	}
	// NOTE: hotKeySet, updateHotKeys, isHotKey 等已废弃
	// 由 chainNextSim（全量 key 依赖检查）替代

	go rs.cycle()
	return rs
}

// patchMap 是 patches 和 onChainDAGPatches 的内层结构，自带锁实现 per-tx 粒度保护
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

	// onChainDAGPatches — 所有节点共享的链上 Patch 缓存
	//      写入：收到 SimTx 多播时；清理：Committer CR 完成时
	//      内层 map[int]*api.ChainNode 由 patchMap.mu 保护
	onChainDAGPatches sync.Map // key: txHash common.Hash, value: *patchMap

	// committedOnChain — DSN-64：已真正 CR Commit 成功的 txHash 集合(写集已落盘、锁已释放)。
	// 与 onChainDAGPatches 不同：onChainDAGPatches 在该 tx CR 完成后被 RemoveOnChainDAGPatch 清掉，
	// 导致晚到的下游 D(引用该 U)verify 时看不到 U → DSN-57 误回滚。这里持久记录“该 U 已 commit”，
	// 使 upstreamOnChain 对已 commit 的 U 仍判就绪，放 D 上链读最终值。仅在 CR Commit(非 Rollback)时写入。
	committedOnChain sync.Map // key: txHash common.Hash, value: struct{}

	// offChainDAG — 链下单一 DAG 存储（合并原 localPatches 与 patches）
	//      承担调度匹配（nodes/keyIndex/subscriber/txSubKeys）与读取/构建上游（UpstreamTxList）双职责
	offChainDAG offChainDAG

	// consumedPatches — retryTxHash → consumed SimTx hashes []common.Hash，用于失败时批量释放 Patch
	consumedPatches sync.Map // key: txHash common.Hash, value: []common.Hash

	// queuedSimCheck — DSN-57 探针：给定上游 txHash，判断它是否仍在本分片 internalPool 排队（未上链）。
	// 由 sscService 在 internalPool 注入后接线（SetQueuedSimCheck）。nil = 未接线。
	queuedSimCheck func(common.Hash) bool

	// simDAGPatches — DSN-54：成员侧模拟期链下 DAG patch 子图池。
	//      写入：RetryCommit 消费上游后 leader 广播（StoreSimDAGPatch）；
	//      读取：成员模拟读被锁 key 时按 (tx,simNum) 反查上游写集；
	//      清理：交易 close/stale 整组清（RemoveSimDAGPatches），更大 simNum 覆盖旧轮。
	//      key: api.TxSimKey (txHash, simulationNum), value: *api.SimPatchSubgraph
	simDAGPatches sync.Map

	// chainTxMarks — 被 DAG 链式救起（VerifySimulation 判为 isChainTx）的 txHash 标记，
	// 用于在 CR 最终 commit 时统计“因 DAG 提前完成的交易数”。
	chainTxMarks sync.Map // key: txHash common.Hash, value: struct{}{}

	// reSimInFlight — 防止同一笔 tx 的 StartReSimulation 被并发调用
	reSimInFlight sync.Map // key: txHash common.Hash, value: struct{}{}

	// lockWait — 等待 stateDB 解锁的交易（自带锁，与 rs 解耦）
	lockWait *LockWaitStore

	// woundedRetryTxs — 被 Wound 的交易，OnBlockCommitted 时恢复
	woundedRetryTxs sync.Map // key: txHash common.Hash, value: struct{}{}

	// reservationSkipCnt — DSN-58 reservation 反饥饿：交易在 OnBlockCommitted reservation 里
	// 连续被跳过的块数（txHash → int64）。累计 >= reservationAntiStarvationBlocks 时当块强制放行一次。
	// 只由 OnBlockCommitted（单 leader 串行）读写。
	reservationSkipCnt sync.Map

	// 缓存：OnBlockCommitted 时缓存的 stateDB 和 block hash
	// 供 RetryCommit Phase 2 + StartReSimulation 共用，消除 CheckLock 与 Lockable 的 race
	cachedState     atomic.Value // api.StateDB
	cachedBlockNum  uint64
	cachedBlockHash common.Hash

	// 依赖组件（通过接口解耦）
	ctx              context.Context
	bc               core.BlockChain
	tempLockView     *TempLockView
	deadlockDetector *deadlockDetector
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
	rs.onChainDAGPatches.Range(func(_, _ interface{}) bool { st.OnChainDAGPatches++; return true })
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
		Int("simulationNum", tx.SimulationNum).
		Int("reads", len(tx.ReadSet)).
		Int("writes", len(tx.WriteSet)).
		Str("readKeys", previewLockKeys(tx.ReadSet)).
		Str("writeKeys", previewLockKeys(tx.WriteSet)).
		Msg("added to retry pool")
}

// previewLockKeys 返回前 max 个 key 的可读预览（用于诊断 retry 订阅是否覆盖到冲突 key）。
func previewLockKeys(keys []api.LockKey) string {
	const max = 12
	var b strings.Builder
	for i, k := range keys {
		if i >= max {
			b.WriteString("...")
			break
		}
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(string(k))
	}
	return b.String()
}

// recordPerBlockSameKeyWrites 统计单个区块内同一个 key 最多被几笔 SimTx 写入
// （“一个 key 一个块最多被改几次”）。只在 DAG 开启且本分片 leader 上统计，
// 避免多 validator 重复计数。
//
// 意义：验证 DAG-patch 是否真的打破了“reservation 每块只放行同一 key 最高优先一笔”
// 的限制——若 DAG 未生效，该值会恒为 1；若同 key 链式 SimTx 能落进同一块，该值应 >1。
func (rs *retryScheduler) recordPerBlockSameKeyWrites(block *types.Block) {
	if rs == nil || !rs.offChainDAG.isOn() {
		return
	}
	ssc := block.SSCTransactions()
	if len(ssc) <= int(types.InternalTxTypeSimTx) {
		return
	}
	sims := ssc[types.InternalTxTypeSimTx]
	if len(sims) < 2 {
		return
	}
	isLeader := false
	simTxCount := 0
	count := make(map[api.LockKey]int)
	maxCnt := 0
	for _, tx := range sims {
		if tx == nil {
			continue
		}
		sim := &sscpb.CXTSimulation{}
		if err := proto.Unmarshal(tx.Payload, sim); err != nil {
			continue
		}
		apiSim := sscpb.CXTSimulationFromProto(sim)
		if apiSim == nil {
			continue
		}
		simTxCount++
		if !isLeader && rs.state != nil && len(apiSim.Epochs) > int(rs.selfShard) &&
			rs.state.IsLeader(apiSim.Epochs[rs.selfShard]) {
			isLeader = true
		}
		for _, cs := range apiSim.CallStates {
			if cs == nil || cs.RWSet == nil || cs.RWSet.WriteState == nil {
				continue
			}
			for addr, st := range cs.RWSet.WriteState.State {
				for key := range st {
					lk := api.FormKey(addr, key)
					count[lk]++
					if count[lk] > maxCnt {
						maxCnt = count[lk]
					}
				}
			}
		}
	}
	if !isLeader || maxCnt == 0 {
		return
	}
	if m := chainRetryStats.MonitorDagPerBlockMaxSameKey.Load(); int64(maxCnt) > m {
		chainRetryStats.MonitorDagPerBlockMaxSameKey.Store(int64(maxCnt))
	}
	utils.SSCLogger().Info().Uint64("block", block.NumberU64()).
		Int("simTx", simTxCount).
		Int("maxSameKeyWrites", maxCnt).
		Msg("[dagPerBlock] max writes to a single key in this block")
}

// isChainReadyLocal — DSN-55：判断某 retryTx 在本分片是否“chain-ready”（已消费 DAG patch、
// 目标就是构建 SimTx 上链）。仅作为本分片 OnBlockCommitted admission 的局部优先键，
// 不进入跨分片字段、不影响全局 Priority。生命周期随 consumedPatches 置位/清除。
func (rs *retryScheduler) isChainReadyLocal(txHash common.Hash) bool {
	if rs == nil {
		return false
	}
	_, consumed := rs.consumedPatches.Load(txHash)
	return consumed
}

// OnBlockCommitted 在区块提交后调用，尝试提升重试池中的交易
// bumpReservationSkip 累加一笔交易在 reservation 里连续被跳过的块数，返回累加后的值。
// 只由 OnBlockCommitted（单 leader 串行）调用。
func (rs *retryScheduler) bumpReservationSkip(txHash common.Hash) int64 {
	val, _ := rs.reservationSkipCnt.LoadOrStore(txHash, int64(0))
	n := val.(int64) + 1
	rs.reservationSkipCnt.Store(txHash, n)
	return n
}

// clearReservationSkip 清除一笔交易的 reservation 跳过计数（该交易被放行时调用）。
func (rs *retryScheduler) clearReservationSkip(txHash common.Hash) {
	rs.reservationSkipCnt.Delete(txHash)
}

// needingShards — DSN-58 §4.3：返回交易 T 中“仍需放行（尚未放好腿）”的 related 分片，
// 即没有收到其 sscVote(commitStates[simNum][shard]) 的分片。已放好腿的分片不算 needing。
// 未接 LegSscVoted 时退化为全部 related（保持旧行为）。
func (rs *retryScheduler) needingShards(retryTx *api.RetryTx) []uint32 {
	if retryTx == nil {
		return nil
	}
	if rs.state == nil || rs.state.LegSscVoted == nil {
		out := make([]uint32, 0, len(retryTx.RelatedShards))
		out = append(out, retryTx.RelatedShards...)
		return out
	}
	var out []uint32
	for _, s := range retryTx.RelatedShards {
		if !rs.state.LegSscVoted(retryTx.TxHash, s, retryTx.SimulationNum) {
			out = append(out, s)
		}
	}
	return out
}

// readyAmongNeeding — 统计 needing 分片里有多少已发 ready retrySignal（rs.signals[simNum][shard].Ready）。
func (rs *retryScheduler) readyAmongNeeding(retryTx *api.RetryTx, needing []uint32) int {
	if retryTx == nil {
		return 0
	}
	sigPMVal, _ := rs.signals.Load(retryTx.TxHash)
	sigPM, _ := sigPMVal.(*signalMap)
	if sigPM == nil {
		return 0
	}
	cnt := 0
	sigPM.mu.Lock()
	m := sigPM.m[retryTx.SimulationNum]
	for _, s := range needing {
		if sig := m[s]; sig != nil && sig.Ready {
			cnt++
		}
	}
	sigPM.mu.Unlock()
	return cnt
}

// MaybeTriggerReSim — DSN-58 §4.3 统一触发判定：needing 全 ready（或 needing 为空已全部放好）时，
// 只要 needing>0 就 tryToReSimulation（内部只扇出到 needing）。供「收到 retrySignal」和「收到 sscVote」两处调用。
func (rs *retryScheduler) MaybeTriggerReSim(txHash common.Hash) {
	retryTxVal, exists := rs.retryPool.Load(txHash)
	if !exists {
		return
	}
	retryTx, _ := retryTxVal.(*api.RetryTx)
	if retryTx == nil {
		return
	}
	needing := rs.needingShards(retryTx)
	if len(needing) == 0 {
		// 所有相关分片都已放好腿 → 走既有 CR 全票路径（HandleCXTCommitSSCVote），无需重验。
		return
	}
	if rs.readyAmongNeeding(retryTx, needing) >= len(needing) {
		go rs.tryToReSimulation(retryTx)
	}
}

func (rs *retryScheduler) OnBlockCommitted(block *types.Block) {
	t0 := time.Now()
	t0Stale := t0
	t0Cache := t0

	defer func() {
		perf.RecordPkg("retryScheduler", "OnBlockCommitted", "total", time.Since(t0))
	}()

	// 统计单个块内同一 key 最多被几笔 SimTx 写入（DAG “一个 key 一块改几次”指标，仅 leader 计）
	rs.recordPerBlockSameKeyWrites(block)

	// 首先处理临时锁并获取释放的 key
	releasedKeys := rs.tempLockView.OnBlockCommitted(block)

	// DSN-53：CRTx 上链 → 跨分片清理 waitEdge（holder commit/die）
	if rs.deadlockDetector != nil {
		rs.deadlockDetector.OnBlockCommitted(block)
	}

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
		if rs.isChainReadyLocal(txHash) {
			chainRetryStats.SigChainReadyCandidate.Add(1)
		}
	}
	// DSN-55：本分片局部 admission 提权 —— chain-ready(已消费 DAG) 先放行，同档内仍按 nonce(全局 Priority)。
	// 不改全局 Priority / wound 语义；chain-ready 状态随 consumedPatches 置位/清除，只持续到放行上链/失败回退。
	sort.SliceStable(sorted, func(i, j int) bool {
		ci := rs.isChainReadyLocal(sorted[i].txHash)
		cj := rs.isChainReadyLocal(sorted[j].txHash)
		if ci != cj {
			return ci // chain-ready 优先（局部 admission）
		}
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
		var conflictKey api.LockKey
		for _, key := range pt.tx.ReadSet {
			if _, exists := reservedKeySet[key]; exists {
				conflict = true
				conflictKey = key
				break
			}
		}
		if !conflict {
			for _, key := range pt.tx.WriteSet {
				if _, exists := reservedKeySet[key]; exists {
					conflict = true
					conflictKey = key
					break
				}
			}
		}
		forceAdmit := false
		if conflict {
			// 候选已被更高优先的交易预留了冲突 key → 本块跳过（低优先饥饿的观测点）。
			// DSN-58：记录连续被跳过块数；达到阈值则当块强制放行一次，避免“某条腿永远不被
			// admission → 永不发 ready → origin ready 聚合门永不闭”的活性死锁。
			skip := rs.bumpReservationSkip(pt.txHash)
			utils.SSCLogger().Debug().
				Str("txHash", pt.txHash.Hex()).
				Uint64("nonce", pt.tx.Nonce).
				Int64("skipBlocks", skip).
				Str("conflictKey", string(conflictKey)).
				Msg("[retryScheduler] reservation skipped candidate (key already reserved by higher-prio)")
			if skip >= reservationAntiStarvationBlocks {
				forceAdmit = true
				rs.clearReservationSkip(pt.txHash)
				chainRetryStats.SigAntiStarvationAdmit.Add(1)
				utils.SSCLogger().Info().
					Str("txHash", pt.txHash.Hex()).
					Uint64("nonce", pt.tx.Nonce).
					Int64("skipBlocks", skip).
					Msg("[retryScheduler] reservation anti-starvation: force-admit tx (starved >= N blocks)")
			}
			if !forceAdmit {
				continue
			}
		} else {
			rs.clearReservationSkip(pt.txHash)
		}
		// 选中（正常选中或反饥饿强制放行），预留其 key
		selectedTx = append(selectedTx, pt)
		if rs.isChainReadyLocal(pt.txHash) {
			chainRetryStats.SigChainReadyAdmission.Add(1)
		}
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
	var signalTxCount, onChainDAGPatchCount int
	rs.signals.Range(func(_, _ any) bool { signalTxCount++; return true })
	rs.onChainDAGPatches.Range(func(_, _ any) bool { onChainDAGPatchCount++; return true })

	// stateLockManager 全局状态
	var globalLocked, globalRLocked, globalFinished, globalLockStart int
	if rs.state != nil && rs.state.LockStatesStats != nil {
		globalLocked, globalRLocked, globalFinished, globalLockStart = rs.state.LockStatesStats()
	}
	perf.RecordPkg("retryScheduler", "OnBlockCommitted", "statsRange", time.Since(t0Stats))

	_ = staleCount2
	_ = wLocks

	// DSN-53 死锁探针监控计数
	var deadlockStats DeadlockDetectorStatus
	if rs.deadlockDetector != nil {
		deadlockStats = rs.deadlockDetector.Stats()
	}
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
		// Deadlock probe (DSN-53)
		Int("deadlockWaitEdges", deadlockStats.WaitEdges).
		Int("deadlockSeenProbes", deadlockStats.SeenProbes).
		Int("deadlockProbeSent", int(deadlockStats.ProbeSent)).
		Int("deadlockProbeDropped", int(deadlockStats.ProbeDropped)).
		Int("deadlockProbeInvalidSig", int(deadlockStats.ProbeInvalidSig)).
		Int("deadlockRingDetected", int(deadlockStats.RingDetected)).
		Int("deadlockVictimDied", int(deadlockStats.VictimDied)).
		// Signals
		Int("signals", signalTxCount).
		Int("onChainDAGPatches", onChainDAGPatchCount).
		// Global lock states
		Int("globalLocked", globalLocked).
		Int("globalRLocked", globalRLocked).
		Int("globalFinished", globalFinished).
		Int("globalLockStart", globalLockStart).
		Msg("[retryScheduler] OnBlockCommitted stats")
}

// triggerVictimRollback 触发环内 victim 的终局 Rollback（复用 sendRollbackVoteForDie 路径）。
func (rs *retryScheduler) triggerVictimRollback(txHash common.Hash) {
	if rs.state == nil || rs.state.RollbackVictim == nil {
		return
	}
	// victim 终局回滚：若 victim 是 DAG 保护过的交易，先对称撤销其保护（不可被 wound 的锁 /
	// 被保护的上游 / origin DAG 标记），否则 rollback 仍会被“不可被 wound”卡住、环打不破。
	rs.revokeDAGProtectionOnRollback(txHash)
	rs.state.RollbackVictim(txHash)
}

// triggerVictimTxSubmit 是 deadlockDetector.victimTxSubmit 的实现：
// 由最后一跳（本）分片 leader 根据 victim 元数据构造 VictimTx，连同判环的完整
// 签名证据链，通过 state.SubmitVictimTx 打进本分片内部池（进块后让本分片每个
// validator 各自投 rollback 票）。
func (rs *retryScheduler) triggerVictimTxSubmit(victim common.Hash, proof []*api.DeadlockProbe) {
	if rs.state == nil || rs.state.SubmitVictimTx == nil {
		return
	}
	if victim == (common.Hash{}) {
		return
	}
	// 元数据：优先从 retryPool 取；若已上链/不在池，回退到锁管理器（与 rollbackVictim 同口径）。
	nonce, origin, related, epochs, simNum := uint64(0), uint32(0), []uint32(nil), []api.Epoch(nil), 0
	if val, ok := rs.retryPool.Load(victim); ok {
		if rt, ok := val.(*api.RetryTx); ok && rt != nil {
			nonce, origin, related, epochs, simNum =
				rt.Nonce, rt.OriginShardID, []uint32(rt.RelatedShards), rt.Epochs, rt.SimulationNum
		}
	} else if rs.tempLockView != nil && rs.tempLockView.stateLockManager != nil {
		mgr := rs.tempLockView.stateLockManager
		if tm, ok := mgr.GetTxMeta(victim); ok {
			related = []uint32(tm.RelatedShards)
			epochs = tm.Epochs
			simNum = tm.SimulationNum
		}
		if pri, ok := mgr.GetTxPriority(victim); ok {
			nonce = pri.Nonce
			origin = pri.OriginShardID
		}
	}
	vtx := &api.VictimTx{
		TxHash:        victim,
		Nonce:         nonce,
		OriginShardId: origin,
		RelatedShards: related,
		Epochs:        epochs,
		SimulationNum: simNum,
		Proof:         proof,
	}
	utils.SSCLogger().Info().
		Str("victim", victim.Hex()).
		Uint32("originShard", origin).
		Int("proofLen", len(proof)).
		Msg("[retryScheduler] submitting VictimTx for CMH victim")
	rs.state.SubmitVictimTx(vtx)
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

// markChainTx 标记一笔交易为“DAG 链式救起”（isChainTx）。
// 在 VerifySimulation 判定为 chain tx（UpstreamTxList>0）时调用，
// 供 CR 最终 commit 时统计“因 DAG 提前完成”的交易数。
func (rs *retryScheduler) markChainTx(txHash common.Hash) {
	if rs != nil {
		rs.chainTxMarks.Store(txHash, struct{}{})
	}
}

// isChainTxMarked 判断该交易是否被标记为链式救起的交易。
func (rs *retryScheduler) isChainTxMarked(txHash common.Hash) bool {
	if rs == nil {
		return false
	}
	_, ok := rs.chainTxMarks.Load(txHash)
	return ok
}

// clearChainTxMark 清除链式标记（CR commit 统计完/交易终结后调用）。
func (rs *retryScheduler) clearChainTxMark(txHash common.Hash) {
	if rs != nil {
		rs.chainTxMarks.Delete(txHash)
	}
}

// revokeDAGProtectionOnRollback — RetryCommit(DAG) 设了保护，RetryRollback 必须对称撤销。
// RetryCommit 路径（tryToReSimulation 里 isChainTxMarked → RetryCommitDAG）成功得锁后会：
//  1. protectHeldLocks(txHash)        —— 把已持 TLV 写锁提到最高优先（不可被 wound）；
//  2. protectUpstreamHeldLocks(...)    —— 把被消费的上游 markChainProtected（不可被 wound）；
//  3. markChainTx(txHash)              —— origin 把整单标记为 DAG。
//
// 当该交易被 rollback / victim / RetryCancel / 失败清理时，若不把这三者撤销，会留下
// “永不让位的僵尸保护”，使死锁无法用 wound/CMH 自愈（详见 HANDOFF-20260907-dsn-unfinished… §3.5/§4）。
// 本方法做三件事的对称撤销（幂等，no-op 安全）：
//   - unprotectHeldLocks：还原/移除该 tx 被提到最高优先的 TLV 锁（可再被 wound）；
//   - unmarkChainProtected：撤销其已被保护的上游（释放后可被正常仲裁/消费）；
//   - clearChainTxMark：整单不再按 DAG 处理（可被 wound / 被 CMH 选为 victim）。
//
// 注意：被释放的补丁本身走 releasePatch（其中已 unmarkChainProtected）；这里额外兜底本分片
// consumedPatches 里仍登记的上游，保证覆盖所有 rollback 入口。
func (rs *retryScheduler) revokeDAGProtectionOnRollback(txHash common.Hash) {
	if rs == nil {
		return
	}
	if rs.tempLockView != nil {
		rs.tempLockView.unprotectHeldLocks(txHash)
	}
	if rs.offChainDAG.isOn() {
		if v, ok := rs.consumedPatches.Load(txHash); ok {
			if ups, ok2 := v.([]common.Hash); ok2 {
				for _, u := range ups {
					rs.offChainDAG.unmarkChainProtected(u)
				}
			}
		}
	}
	rs.clearChainTxMark(txHash)
}

// chainNodeDepth 返回指定 tx 在 offChainDAG 中节点的真实 DAG 深度（node.Depth）。
// DAG 关闭/节点不存在返回 0。用于按真实深度统计链深分布（而非 simulationNum）。
func (rs *retryScheduler) chainNodeDepth(txHash common.Hash) int {
	if rs == nil || !rs.offChainDAG.isOn() {
		return 0
	}
	if nv, ok := rs.offChainDAG.nodes.Load(txHash); ok {
		if n := nv.(*OffChainPatchNode); n != nil {
			return n.Depth
		}
	}
	return 0
}

// HandleRetrySignal 接收来自 SimTx 提交 shard 的 chain signal，
// 在 origin shard 把该 tx 的上游节点落进 offChainDAG 后，通过 signal aggregation 触发 tryToReSimulation。
func (rs *retryScheduler) HandleRetrySignal(signal *api.RetrySignal) {
	chainRetryStats.SigRetrySignalReceived.Add(1)
	utils.SSCLogger().Debug().Str("txHash", signal.TxHash.Hex()).
		Bool("hasUpstream", len(signal.UpstreamTxList) > 0).
		Msg("HandleRetrySignal: received")

	// DAG 禁用时链下 patch 整体关闭。
	if rs.offChainDAG.isOn() && len(signal.UpstreamTxList) > 0 {
		// DSN-57 分片收敛(规则 A)：signal 携带的上游是“救援发起分片”消费的，origin 只应保留
		// 自己 offChainDAG 里确有节点(本分片持有该上游 patch)的那些；异地上游不写进 origin 节点。
		var local []api.TxSimKey
		for _, up := range signal.UpstreamTxList {
			if _, ok := rs.offChainDAG.nodes.Load(up.TxHash); ok {
				local = append(local, up)
			}
		}
		if len(local) > 0 {
			// 把本 tx 的、且属于 origin 的上游节点持久进 offChainDAG，供后续 CommitSimulation 读取。
			// DSN-56：own Patch 传 nil（本 tx 尚未产出写集），之后由 CommitSimulation 以自身 WriteSet 覆盖补全。
			rs.offChainDAG.AddNode(signal.TxHash, signal.SimulationNum, nil, local)
		} else {
			utils.SSCLogger().Debug().Str("txHash", signal.TxHash.Hex()).
				Msg("HandleRetrySignal: upstreams not local to origin shard (DSN-65, whole-tx DAG mark only)")
		}
		// DSN-65 (P2)：整单 DAG 标志不再依赖“上游是否 origin 本地”。只要收到携带上游的 chain
		// signal（任一分片确实消费了上游），origin 就把整单标记为 DAG —— 供 tryToReSimulation
		// 对本交易所有 needing 分片统一走 RetryCommitDAG，使 DAG 足迹整单一致（不再因 origin
		// 本地无该上游而漏标 → 别的腿不保护 → 撕裂）。HandleRetrySignal 一定跑在 origin shard，
		// 而 chainTxCRCommitted / [chainTxFate] 也在 origin leader 记账。
		rs.markChainTx(signal.TxHash)
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
		rs.setSignal(signals.FromShard, signal)
		// DSN-58 §4.3：统一按 needing（无 sscVote 的分片）判定，needing 全 ready 才触发。
		// 收到来自某分片的 ready retrySignal 后重算一次。
		rs.MaybeTriggerReSim(signal.TxHash)
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

	// DSN-58 §4.3：只扇出 RetryCommit 到“仍需放行（无 sscVote）”的分片 needing，
	// 已放好腿(有 sscVote)的分片不参与重验/重放（避免对已放好腿分片 RetryCommit → 其不在 retryPool → Locked:false）。
	needing := rs.needingShards(retryTx)
	if len(needing) == 0 {
		// 所有相关分片都已放好腿 → 由 HandleCXTCommitSSCVote 走既有 CR 全票路径，无需重验。
		return
	}

	resps := make(map[uint32]*api.RetryCommitResp)

	// DSN-55 rev2：若是 DAG 救援 attempt（origin 已 markChainTx），对 needing shard 用
	// RetryCommitDAG，让它们在取得 TLV 锁后提权保锁，避免跨分片“A 保 B 抢”撕裂。
	retryCommitMethod := api.Method_RetryCommit
	if rs.isChainTxMarked(txHash) {
		retryCommitMethod = api.Method_RetryCommitDAG
	}

	wg := sync.WaitGroup{}
	wg.Add(len(needing))
	lock := sync.Mutex{}
	for _, shard := range needing {
		go func(shard uint32) {
			defer wg.Done()
			resp := new(api.RetryCommitResp)
			err := rs.comm.Call(rs.ctx, resp, rs.state.GetLeader(retryTx.Epochs[shard], shard), retryCommitMethod, txHash)
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
	if len(resps) != len(needing) {
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

		// DSN-62 对称撤销：本次 RetryCommit(DAG) attempt 失败。RetryCommit 得锁时设的保护
		// （protectHeldLocks / markChainProtected / markChainTx）必须在失败清理时一并撤销，
		// 否则留下“不可被 wound”的僵尸锁/上游，使交易被彻底卡死且无法用 wound/CMH 自愈。
		// 下面的 releasePatch / RetryCancel 各自会 unmark/撤销，这里统一兜底本分片(含 origin)
		// 的 chain 标记与本地被保护锁，保证覆盖所有失败入口。
		rs.revokeDAGProtectionOnRollback(txHash)

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
	// Fix-B / RetryRollback 对称撤销：StaleTx 是 commit/rollback 终局的统一回收入口(closeTransaction)。
	// 若该 tx 曾作为 DAG 腿被 protectHeldLocks / 上游 markChainProtected / origin markChainTx 保护，
	// 必须在终局回收时撤掉这些“免 wound”标记并释放保护，否则残留的不可被 wound 的锁/节点会
	// 继续挡住其它交易（stuck-DAG 锁死他人）。先撤保护再 GC，保证没有最高优先哨兵锁残留。
	rs.revokeDAGProtectionOnRollback(txHash)
	rs.tempLockView.GarbageCollect(txHash)

	rs.staleTxs.Store(txHash, struct{}{})

	// DSN-54: tx stale → 整组清 simDAGPatches（对齐 offChainDAG 清理时机）
	rs.RemoveSimDAGPatches(txHash)

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Str("duration", time.Since(t0).String()).
		Msg("retryScheduler.StaleTx timing")
}

// RetryCommit 普通重试提交（非 DAG 救援 attempt）。
func (rs *retryScheduler) RetryCommit(txHash common.Hash) *api.RetryCommitResp {
	return rs.retryCommit(txHash, false)
}

// RetryCommitDAG — DSN-55 rev2：DAG 救援 attempt 的 RetryCommit。
// 与 RetryCommit 唯一区别：得锁(TLV)成功后一律 protectHeldLocks 提权保锁，避免被后来者 wound。
func (rs *retryScheduler) RetryCommitDAG(txHash common.Hash) *api.RetryCommitResp {
	return rs.retryCommit(txHash, true)
}

// protectUpstreamHeldLocks — DSN-61：RetryCommit 救援下游 D(消费其上游 U)时，除保护 D 外，
// 连带把被消费的上游 U 标记为“被动链保护”（DSN-62 (D) markChainProtected），使其不可被 Wound
// 但保持可被消费。动机：上游 U 若被更高优先第三方 Wound → 拉回重试 → 迟迟无法先于 D 上链
// (AddOnChainDAGPatch) → D 即使被保锁不被 Wound，也会被有界扣留 50 块后强制放行 → verify 判
// “上游未上链”DSN-57 回滚。把 U 也保护可让“U→D”整条链都免疫被 Wound，上游得以先于下游稳定上链。
// 范围克制：只保护 D 实际消费到的上游(consumedTxHashes)及其祖先链，不做全量保护；U 走上链/失败
// GC 节点被 Remove 后保护随节点消失。
// DSN-62 (C)：保护**沿被消费的上游链递归传递**——U 要上链必须先让 U 自己的上游 UU 上链，只保 U
// 不保 UU，链头 UU 仍可被 Wound 而断链。故保护 U 后继续沿 U 的 UpstreamTxList 向上保护其祖先，
// 用 visited 防环、depth ≤ maxChainDepth 封顶，只作用于“实际被消费的链式子图”。
func (rs *retryScheduler) protectUpstreamHeldLocks(upstreams []common.Hash) {
	if rs == nil || rs.tempLockView == nil || len(upstreams) == 0 {
		return
	}
	rs.protectUpstreamHeldLocksBFS(upstreams)
}

func (rs *retryScheduler) protectUpstreamHeldLocksBFS(roots []common.Hash) {
	visited := make(map[common.Hash]struct{}, len(roots))
	// queue 的元素带当前深度（根=1，用于链深上限）
	type item struct {
		h     common.Hash
		depth int
	}
	queue := make([]item, 0, len(roots))
	for _, r := range roots {
		if _, seen := visited[r]; seen {
			continue
		}
		visited[r] = struct{}{}
		queue = append(queue, item{h: r, depth: 1})
	}
	maxDepth := rs.maxChainDepth
	for len(queue) > 0 {
		it := queue[0]
		queue = queue[1:]
		u := it.h
		// DSN-62 (D)：改为“被动链保护”——markChainProtected(U) 使 U 不可被 Wound 但保持可被消费。
		// 不再用 protectHeldLocks(U) 把 U 顶到最高优先：那会让 U 反过来主动 wound 别的更低优先
		// 上游(它也可能是别条链的上游) → 制造新的级联(见 Q3)。被动保护既保住 U，又不误伤他人。
		rs.offChainDAG.markChainProtected(u)
		chainRetryStats.SigUpstreamHoldProtected.Add(1)
		// 向上扩展 U 自己的上游（U 是某更早节点的下游时才有）
		if it.depth >= maxDepth {
			continue
		}
		ups := rs.GetUpstreamTxRef(u)
		for _, up := range ups {
			if _, seen := visited[up.TxHash]; seen {
				continue
			}
			visited[up.TxHash] = struct{}{}
			queue = append(queue, item{h: up.TxHash, depth: it.depth + 1})
		}
	}
}

// notifyWoundedUpstream — DSN-62 (B')：上游 U 被 Wound 时，把其残留的可消费 Patch 节点从 DAG
// **无条件移除**（若有下游 D 已消费 U，则先把 U 从 D 的 consumedPatches 解绑）。
//
// 为什么要无条件移除（不再像初版那样仅当 U 已 Consumed）：
// 级联的根子是“U 在 **Free** 阶段就被 Wound、但其陈旧 Free 节点仍留在 DAG 里” → 之后大量下游 D
// 仍可消费这个 doomed 的 U → 全在等一个上不了链的 U → 50 块后成片回滚。初版只处理“已 Consumed
// 才被 wound”的少数，漏掉 Free-级联这个大头。这里对任何被 wound 的 U 一律移除其 DAG 节点：
//   - 已 Consumed → 先解绑消费方 D；
//   - 未 Consumed(Free) → 直接移除，杜绝后来者消费这个 doomed 节点；
//
// U 被 wound 后当前 SimTx 尝试作废，其重试重新模拟后会以**新** patch 重新 AddNode。
func (rs *retryScheduler) notifyWoundedUpstream(U common.Hash) {
	if rs == nil || !rs.offChainDAG.isOn() {
		return
	}
	nv, ok := rs.offChainDAG.nodes.Load(U)
	if !ok {
		return
	}
	node, ok := nv.(*OffChainPatchNode)
	if !ok {
		return
	}
	D := node.Consumer
	if D != (common.Hash{}) {
		// 把 U 从下游 D 的 consumedPatches 里去掉，使 D 不再误以为仍持有 U 这个上游
		if cpv, ok := rs.consumedPatches.Load(D); ok {
			if list, ok2 := cpv.([]common.Hash); ok2 {
				kept := make([]common.Hash, 0, len(list))
				for _, x := range list {
					if x != U {
						kept = append(kept, x)
					}
				}
				if len(kept) > 0 {
					rs.consumedPatches.Store(D, kept)
				} else {
					rs.consumedPatches.Delete(D)
				}
			}
		}
	}
	// 无效化 U 的节点（无条件移除），使其不再被 findCoveringSet 选为可消费上游。
	rs.offChainDAG.Remove(U)
	chainRetryStats.SigUpstreamWoundedInvalidated.Add(1)
	utils.SSCLogger().Debug().Str("upstream", U.Hex()).
		Str("consumer", D.Hex()).
		Msg("notifyWoundedUpstream: wounded upstream invalidated (DSN-62 B')")
}

func (rs *retryScheduler) retryCommit(txHash common.Hash, dagAttempt bool) *api.RetryCommitResp {
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
		// Fix-B：本分片已消费上游 = 这条腿本地就是 DAG 链（不依赖 origin 是否标记）。
		// 打上本地 chain 标记，使后续任何对该 tx 的 leg 锁都不可被 wound。
		rs.markChainTx(txHash)
		// DSN-54：已消费上游 → 广播本轮 simDAGPatch 子图给本分片成员
		rs.broadcastSimDAGPatch(retryTx)
		// DSN-55 rev2 / Fix-B：DAG 腿得锁后一律保锁（不可被 wound）。
		// 不再以 dagAttempt 门控：既然本分片确实消费了上游，它就是 DAG 链的一部分，
		// 必须保锁以尽快完成 commit，避免被更高优先第三方 Wound 后卡住（B→X→A 级联）。
		rs.tempLockView.protectHeldLocks(txHash)
		chainRetryStats.SigDAGHoldProtected.Add(1)
		// DSN-61：连带保护本 tx 已消费的上游，避免上游被第三方 Wound 而无法先于 D 上链。
		if uv, ok := rs.consumedPatches.Load(txHash); ok {
			if ups, ok2 := uv.([]common.Hash); ok2 {
				rs.protectUpstreamHeldLocks(ups)
			}
		}
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
		// TLV TryLock 失败，说明至少有一个 key 被其他 tx 在链下预约层占着。
		// 只对“被 TLV 占用的 key”评估 patch 覆盖（不取完整 Read∪Write 集）：
		// 未被上锁的 key 重跑可直接读写，无需上游 patch。此前要求覆盖全部 key 是历史 bug，
		// 会让只被抢少数热 key 的交易永远凑不齐覆盖集而饥饿。
		t0Dag1 := time.Now()
		// stateDB 传 nil：Phase1b 只处理 TLV（链下）层的冲突。
		tlvBlockedKeys := rs.collectBlockedKeys(retryTx, nil)
		// 注：RetryCommit 只“消费/补救”，不构建新的上游边，故 maxSimNum=0（不做严格更早过滤），避免回归既有 rescue；仅排除自己防自环。
		patches := rs.offChainDAG.findCoveringSet(tlvBlockedKeys, txHash, 0)
		perf.RecordPkg("retryScheduler", "RetryCommit", "dagSearchPhase1b", time.Since(t0Dag1))
		if len(tlvBlockedKeys) > 0 && len(patches) > 0 && rs.offChainDAG.isFullyCovered(tlvBlockedKeys, patches) && chainDepthOf(patches) <= rs.maxChainDepth {
			t0Cons := time.Now()
			var consumedTxHashes []common.Hash
			var upstreamTxList []api.TxSimKey
			allConsumed := true
			for _, node := range patches {
				// DSN-56：消费上游不 merged 成扁平写集，只建立 DAG 上游边。
				if patch := rs.offChainDAG.tryConsumePatch(node.TxHash, txHash, priority); patch == nil {
					allConsumed = false
					break
				}
				consumedTxHashes = append(consumedTxHashes, node.TxHash)
				upstreamTxList = append(upstreamTxList, api.TxSimKey{
					TxHash:        node.TxHash,
					SimulationNum: node.SimulationNum,
				})
			}
			if allConsumed {
				perf.RecordPkg("retryScheduler", "RetryCommit", "dagConsumePhase1b", time.Since(t0Cons))
				chainRetryStats.SigRetryCommitPatchHit.Add(1)
				chainRetryStats.MonitorRetryCommitPatchHit.Add(1)
				// 断链修复：RetryCommit 自身 DAG 救援(Phase1b)成功后，把本 tx 的“上游边
				// (UpstreamTxList)”持久进 offChainDAG——否则 CommitSimulation 里
				// GetUpstreamTxRef(txHash) 读不到上游 → SimTx.UpstreamTxList 为空 → Verify 不判 chain。
				// DSN-56：own Patch 传 nil（本 tx 尚未产出写集），之后由 CommitSimulation 以自身 WriteSet 覆盖补全。
				rs.offChainDAG.AddNode(txHash, retryTx.SimulationNum, nil, upstreamTxList)
				rs.consumedPatches.Store(txHash, consumedTxHashes)
				// Fix-B：本分片消费了上游 = 本地确认这条腿是 DAG 链 → 打本地 chain 标记，
				// 使本分片后续对该 tx 的任何 leg 锁都不可被 wound（不依赖 origin 是否已标记）。
				rs.markChainTx(txHash)
				utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
					Int("patchCount", len(patches)).
					Int("coveredKeys", len(tlvBlockedKeys)).
					Int("upstreamCount", len(upstreamTxList)).
					Msg("retryCommit: found DAG patches in PatchPool, rescued from TLV lock conflict")
				// DSN-54：消费上游成功 → 广播本轮 simDAGPatch 子图给本分片成员
				rs.broadcastSimDAGPatch(retryTx)
				// DSN-61：救援下游时连带保护被消费的上游（本路径 D 尚未持 TLV 写锁，故只保上游）。
				rs.protectUpstreamHeldLocks(consumedTxHashes)
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

		// DSN-53：Phase1 是 TLV 预约/调度层，只做 wound 调度，**不记 waitEdge**。
		// CMH 的持久边只来自真实链上锁（Phase2/verify 的 global+pending），TLV 预约不作 waitEdge 来源。
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
		// 全部通过 → 成功（Phase2 普通成功，TLV 已持有）
		rs.tempLockView.ClearWounded(txHash)
		chainRetryStats.SigRetryCommitTryLockOk.Add(1)
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msg("retryCommit")
		// DSN-55 rev2 / Fix-B：DAG attempt 得锁后提权保锁。除了 origin 传入的 dagAttempt，
		// 若本分片本地已标记该 tx 为 DAG 链（本分片曾消费上游/验过 chain 腿），同样保锁——
		// 避免该 tx 在其它分片用了 DAG、而本分片这条(普通)腿却因不知情被 wound（B→X→A 级联）。
		if dagAttempt || rs.isChainTxMarked(txHash) {
			rs.tempLockView.protectHeldLocks(txHash)
			chainRetryStats.SigDAGHoldProtected.Add(1)
		}
		return &api.RetryCommitResp{Locked: true, TxHash: txHash}
	}

	// ──────────────────────────────────────────────
	// Phase 2b: PatchPool DAG 补救 — 多 Patch 联合覆盖冲突 key
	// ──────────────────────────────────────────────
	chainRetryStats.SigRetryCommitTryLockFail.Add(1)
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Int("conflictKeys", len(conflictKeys)).
		Msg("retryCommit failed: stateDB lock conflict")

	// 覆盖范围修正：只对真正被占用的 conflictKeys（SLM 链上/pending + TLV 链下）评估 patch 覆盖。
	// 未上锁的 key 重跑可直接读写 stateDB，无需上游 patch。此前用完整 Read∪Write 集做覆盖
	// 判定是历史 bug：只被抢 1-2 个热 key 的交易永远凑不齐“覆盖全部 key”的 patch 组而饥饿。
	// 注：RetryCommit 只“消费/补救”，不构建新边，maxSimNum=0 保留既有 rescue；仅排除自己防自环。
	needKeys := conflictKeys
	t0Dag2 := time.Now()
	patches2 := rs.offChainDAG.findCoveringSet(needKeys, txHash, 0)
	perf.RecordPkg("retryScheduler", "RetryCommit", "dagSearchPhase2b", time.Since(t0Dag2))
	if len(needKeys) > 0 && len(patches2) > 0 && rs.offChainDAG.isFullyCovered(needKeys, patches2) && chainDepthOf(patches2) <= rs.maxChainDepth {
		// 原子消费：逐个 TryConsume，任一失败则全部 Release
		t0Cons2 := time.Now()
		var consumedTxHashes []common.Hash
		var upstreamTxList []api.TxSimKey
		allConsumed := true
		for _, node := range patches2 {
			// DSN-56：消费上游不 merged 成扁平写集，只建立 DAG 上游边。
			if patch := rs.offChainDAG.tryConsumePatch(node.TxHash, txHash, priority); patch == nil {
				allConsumed = false
				break
			}
			consumedTxHashes = append(consumedTxHashes, node.TxHash)
			upstreamTxList = append(upstreamTxList, api.TxSimKey{
				TxHash:        node.TxHash,
				SimulationNum: node.SimulationNum,
			})
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
			// 断链修复：RetryCommit 自身 DAG 救援(Phase2b)成功后，把本 tx 的“上游边
			// (UpstreamTxList)”持久进 offChainDAG——否则 CommitSimulation 里
			// GetUpstreamTxRef(txHash) 读不到上游 → SimTx.UpstreamTxList 为空 → Verify 不判 chain。
			// DSN-56：own Patch 传 nil（本 tx 尚未产出写集），之后由 CommitSimulation 以自身 WriteSet 覆盖补全。
			rs.offChainDAG.AddNode(txHash, retryTx.SimulationNum, nil, upstreamTxList)
			rs.consumedPatches.Store(txHash, consumedTxHashes)
			rs.tempLockView.ClearWounded(txHash)
			// Fix-B：本分片消费了上游 = 本地确认这条腿是 DAG 链 → 打本地 chain 标记，
			// 使本分片后续对该 tx 的任何 leg 锁都不可被 wound（不依赖 origin 是否已标记）。
			rs.markChainTx(txHash)
			utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
				Int("patchCount", len(patches2)).
				Int("coveredKeys", len(conflictKeys)).
				Int("upstreamCount", len(upstreamTxList)).
				Msg("retryCommit: found DAG patches in PatchPool, skipping lock conflict")
			// DSN-54：消费上游成功 → 广播本轮 simDAGPatch 子图给本分片成员
			rs.broadcastSimDAGPatch(retryTx)
			// DSN-55 rev2：DAG 救援交易得锁后(Phase2b 持 TLV) → 提权保锁，避免被后来者 wound。
			rs.tempLockView.protectHeldLocks(txHash)
			chainRetryStats.SigDAGHoldProtected.Add(1)
			// DSN-61：连带保护本 tx 刚消费的上游，保证 U 先于 D 稳定上链、不被第三方 Wound。
			rs.protectUpstreamHeldLocks(consumedTxHashes)
			return &api.RetryCommitResp{Locked: true, TxHash: txHash}
		}
	}

	// DSN-50: 因覆盖不全或链深上限放弃救援，记录深度上限次数（若超深）
	if len(patches2) > 0 && chainDepthOf(patches2) > rs.maxChainDepth {
		chainRetryStats.SigChainDepthCapped.Add(1)
	}
	// 无 Patch 覆盖 → 释放 tempLockView 锁 + OnChainLockConflict

	// DSN-53 记边（Phase 2 on-chain 层）：只要链上冲突就记边（补全等待图，方向不限）。
	// 探测与否由 deadlockDetector.shouldTriggerProbe 决定（仅高被低才找 victim）。
	// 找 key 的链上持有者：globalLockedStates(SLM 写/读) + pendingStates(stateDB 同块未提交) 两张 map 同时查。
	// pendingStates 对后续交易与 globalLockedStates 等价（都是链上锁），日志里 pending 冲突误标为 "[TempLockView] locked by tx"。
	var coreDB *corestate.DB
	if sdb, ok := stateDB.(*corestate.DB); ok {
		coreDB = sdb
	}
	if rs.deadlockDetector != nil {
		var epochVal api.Epoch
		if len(retryTx.Epochs) > int(rs.selfShard) {
			epochVal = retryTx.Epochs[rs.selfShard]
		}
		for _, key := range conflictKeys {
			holder, holderPri := common.Hash{}, api.Priority{}
			found := false
			mgr := rs.tempLockView.stateLockManager
			if mgr != nil {
				if meta, ok := mgr.GetLockHolderMeta(key); ok && meta.TxHash != txHash && meta.TxHash != (common.Hash{}) {
					holder, holderPri, found = meta.TxHash, meta.Priority, true
				} else if h, ok := mgr.GetRLockHolder(key); ok && h != txHash && h != (common.Hash{}) {
					hp, _ := mgr.GetTxPriority(h)
					holder, holderPri, found = h, hp, true
				}
			}
			if !found && coreDB != nil {
				if h, ok := coreDB.FindPendingLockHolder(key); ok && h != txHash && h != (common.Hash{}) {
					if mgr != nil {
						holderPri, _ = mgr.GetTxPriority(h)
					}
					holder, found = h, true
				}
			}
			if found {
				_ = rs.deadlockDetector.OnBlockedAt(txHash, holder, rs.selfShard, key,
					api.LockLayerOnChain, priority, holderPri, epochVal, rs.cachedBlockNum)
			}
		}
	}
	rs.tempLockView.GarbageCollect(txHash)
	return &api.RetryCommitResp{
		Locked:              false,
		TxHash:              txHash,
		OnChainLockConflict: true,
	}
}

// onChainBlocked 链上锁判定(Lockable)发现请求方 req 想拿 key 却被真实链上锁 holder 占住时调用。
// v2 语义：链上记边(方向不限) + 高被低才探测；仅分片 leader 维护 waitEdges。
// TLV 只作触发(不在此记边)。holder 来自 Lockable 已定位的 global/pending 持有者。
func (rs *retryScheduler) onChainBlocked(req, holder common.Hash, key api.LockKey, holderPri api.Priority) {
	if rs == nil || rs.deadlockDetector == nil || rs.tempLockView == nil || rs.tempLockView.stateLockManager == nil {
		return
	}
	if holder == (common.Hash{}) || holder == req {
		return
	}
	// leader-only：waitEdges / 探针只在分片 leader 维护（探针也只发给 leader）。
	if rs.state == nil || rs.state.IsLeader == nil {
		return
	}
	ep := api.Epoch(rs.deadlockDetector.currentEpoch.Load())
	if !rs.state.IsLeader(ep) {
		return
	}
	reqPri, ok := rs.tempLockView.stateLockManager.GetTxPriority(req)
	if !ok {
		return
	}
	_ = rs.deadlockDetector.OnBlocked(req, holder, rs.selfShard, key, api.LockLayerOnChain, reqPri, holderPri)
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
	// RetryRollback 对称撤销：RetryCommit(DAG) 得锁后设的保护（protectHeldLocks /
	// markChainProtected / markChainTx）必须在取消/回滚时撤销，否则留下僵尸保护导致死锁无法自愈。
	rs.revokeDAGProtectionOnRollback(txHash)
	rs.tempLockView.GarbageCollect(txHash)
}

// sendChainSignal 发送链式重试信号到 origin shard。
// DSN-56：本 tx 尚未产出自身写集，offChainDAG.AddNode 只落“上游边(UpstreamTxList)”，
// own Patch 留空（之后由 CommitSimulation 以自身 WriteSet 覆盖）；不再构造/携带扁平 merged ChainPatch。
func (rs *retryScheduler) sendChainSignal(txHash common.Hash, retryTx *api.RetryTx, upstreamTxList []api.TxSimKey) {
	// 存储到链下单一 DAG（offChainDAG.nodes）：记录本 tx 的上游边，供 readPatchChain/GetUpstreamTxRef 读取。
	rs.offChainDAG.AddNode(txHash, retryTx.SimulationNum, nil, upstreamTxList)

	// 构建 RetrySignal：只带上游边(UpstreamTxList)
	signal := &api.RetrySignal{
		TxHash:         txHash,
		FromShard:      rs.selfShard,
		Epoch:          retryTx.Epochs[rs.selfShard],
		SimulationNum:  retryTx.SimulationNum,
		Condition:      api.Simulate,
		Ready:          true,
		UpstreamTxList: upstreamTxList,
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
	if !rs.offChainDAG.isOn() {
		return common.Hash{}, false
	}
	return rs.readPatchChainVisited(txHash, simNum, address, key, make(map[api.TxSimKey]struct{}))
}

func (rs *retryScheduler) readPatchChainVisited(txHash common.Hash, simNum int,
	address common.Address, key common.Hash, visited map[api.TxSimKey]struct{}) (common.Hash, bool) {
	if !rs.offChainDAG.isOn() {
		return common.Hash{}, false
	}

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

// GetDAGNodeRef 返回指定 tx 的链式 patch 引用（TxSimKey）。
// 用于 VerifySimulation 判断是否跳过锁冲突检查。
// 注意：返回的 TxSimKey 是当前 tx 自己的 hash，不是上游的。
// 要获取上游信息请使用 GetUpstreamTxRef。
func (rs *retryScheduler) GetDAGNodeRef(txHash common.Hash) *api.TxSimKey {
	if !rs.offChainDAG.isOn() {
		return nil
	}
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
	if !rs.offChainDAG.isOn() {
		return nil
	}
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

// LocalUpstreamTxRef 返回指定 tx 的、**只属于本分片**的上游引用（DSN-57 分片收敛）。
// 规则(A)：仅保留“本分片 offChainDAG 里确实存在节点”的上游（即本分片处理/持有该上游 patch）。
// 规则(B)：若过滤后为空 → 返回 nil（本分片不作为“依赖异地上游”的链式 tx，按非链/fail-open 处理）。
// 避免把救援发起分片的异地上游灌进 origin / 其它分片的 SimTx（否则 verify 在本分片看不到上游 → absent 误回滚）。
func (rs *retryScheduler) LocalUpstreamTxRef(txHash common.Hash) []api.TxSimKey {
	all := rs.GetUpstreamTxRef(txHash)
	if len(all) == 0 {
		return nil
	}
	out := make([]api.TxSimKey, 0, len(all))
	for _, up := range all {
		if _, ok := rs.offChainDAG.nodes.Load(up.TxHash); ok {
			out = append(out, up)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// AddOnChainDAGPatch 在 SimTx 通过 VerifySimulation 后，把它导入 verifier 侧的
// onChainDAGPatches（DSN-56）：ownWS 是该 SimTx 自身的 WriteSet（由 CallStates 推导，
// 无扁平 ChainPatch），UpstreamTxList 记录其 DAG 上游边。下游链式 SimTx 按其
// UpstreamTxList 反查这里的上游节点做一致性判定。
func (rs *retryScheduler) AddOnChainDAGPatch(txHash common.Hash, simNum int, ownWS *api.RWSet, upstreamTxList []api.TxSimKey) {
	if !rs.offChainDAG.isOn() {
		return
	}
	pmVal, _ := rs.onChainDAGPatches.LoadOrStore(txHash, &patchMap{m: make(map[int]*api.ChainNode)})
	pm := pmVal.(*patchMap)
	pm.mu.Lock()
	pm.m[simNum] = &api.ChainNode{
		TxHash:         txHash,
		SimulationNum:  simNum,
		Patch:          ownWS,
		UpstreamTxList: upstreamTxList,
	}
	pm.mu.Unlock()
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Int("simNum", simNum).
		Int("upstreamCount", len(upstreamTxList)).
		Msg("AddOnChainDAGPatch: imported verified SimTx into onChainDAGPatches")
}

// GetOnChainDAGPatch 从 onChainDAGPatches 查询指定 SimTx 的 ChainNode。
func (rs *retryScheduler) GetOnChainDAGPatch(txHash common.Hash, simNum int) *api.ChainNode {
	if !rs.offChainDAG.isOn() {
		return nil
	}
	pmVal, _ := rs.onChainDAGPatches.Load(txHash)
	pm, _ := pmVal.(*patchMap)
	if pm == nil {
		return nil
	}
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.m[simNum]
}

// upstreamOnChain — 就绪谓词：某上游 tx 是否已“可被下游依赖地就绪”。
// 判据 = 该 tx 在 onChainDAGPatches（SimTx 已上链验证成功，patch 仍在）**或** 在 committedOnChain
// （该 tx 已真正 CR Commit，写集已落盘、锁已释放——即使其临时 onChainDAGPatch 已被清理，D 也可
// 放行去读最终状态）。供 verify(DSN-57) 与 internal_pool 的 SimTx 就绪门查询。
func (rs *retryScheduler) upstreamOnChain(txHash common.Hash) bool {
	if rs == nil || !rs.offChainDAG.isOn() {
		return false
	}
	if _, ok := rs.onChainDAGPatches.Load(txHash); ok {
		return true
	}
	// DSN-64：已 commit 的上游也算就绪（其写集已落盘、锁已释放，D 可读最终值）。
	if _, ok := rs.committedOnChain.Load(txHash); ok {
		return true
	}
	return false
}

// MarkCommittedOnChain — DSN-64：CR Commit 成功后记录该 tx 已上链提交。由 Committer 在 CommitTx
// 成功后回调（见 committer.go）。此记录持久（不在 RemoveOnChainDAGPatch 时清除），使已 commit 的
// 上游不再因临时 patch 被清而误伤晚到的下游 D。
func (rs *retryScheduler) MarkCommittedOnChain(txHash common.Hash) {
	if rs == nil || !rs.offChainDAG.isOn() {
		return
	}
	rs.committedOnChain.Store(txHash, struct{}{})
	chainRetryStats.SigUpstreamCommittedGate.Add(1)
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Msg("MarkCommittedOnChain: tx CR-committed, recorded as on-chain for downstreams (DSN-64)")
}

// upstreamQueued — DSN-57 探针：某上游 tx 是否仍在本分片 internalPool 排队（尚未上链）。
func (rs *retryScheduler) upstreamQueued(txHash common.Hash) bool {
	if rs == nil || rs.queuedSimCheck == nil {
		return false
	}
	return rs.queuedSimCheck(txHash)
}

// SetQueuedSimCheck 注入“上游是否在本分片 internalPool 排队”的探针回调（由 sscService 接线）。
func (rs *retryScheduler) SetQueuedSimCheck(f func(common.Hash) bool) {
	if rs == nil {
		return
	}
	rs.queuedSimCheck = f
}

// RemoveOnChainDAGPatch 从 onChainDAGPatches 删除指定交易的 ChainNode。
// 在 Committer.CommitOrRollbackWithProof 中调用（CR 完成后链上清理）。
func (rs *retryScheduler) RemoveOnChainDAGPatch(txHash common.Hash) {
	if !rs.offChainDAG.isOn() {
		return
	}
	t0 := time.Now()
	rs.onChainDAGPatches.Delete(txHash)
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Str("duration", time.Since(t0).String()).
		Msg("RemoveOnChainDAGPatch timing")
}

// ReadOnChainDAGPatch 从 onChainDAGPatches 递归读取指定 SimTx 的某 key 期望值。
// 只查单个 key，不合并全部上游 WriteSet（高性能路径）。
// 返回 (value, found)，found=false 表示该 key 在上游 WriteSet 中不存在。
// DAG 多上游：先查当前 node，再遍历所有上游递归查。
// 带 visited 集合防止 DAG 成环导致无限递归（stack overflow）。
func (rs *retryScheduler) ReadOnChainDAGPatch(txHash common.Hash, simNum int, address common.Address, key common.Hash) (common.Hash, bool) {
	if !rs.offChainDAG.isOn() {
		return common.Hash{}, false
	}
	return rs.readOnChainDAGPatchVisited(txHash, simNum, address, key, make(map[api.TxSimKey]struct{}))
}

func (rs *retryScheduler) readOnChainDAGPatchVisited(txHash common.Hash, simNum int, address common.Address, key common.Hash, visited map[api.TxSimKey]struct{}) (common.Hash, bool) {
	if !rs.offChainDAG.isOn() {
		return common.Hash{}, false
	}
	id := api.TxSimKey{TxHash: txHash, SimulationNum: simNum}
	if _, ok := visited[id]; ok {
		// DAG 环：该节点已访问过，避免无限递归
		return common.Hash{}, false
	}
	visited[id] = struct{}{}

	pmVal, _ := rs.onChainDAGPatches.Load(txHash)
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
		if val, found := rs.readOnChainDAGPatchVisited(up.TxHash, up.SimulationNum, address, key, visited); found {
			return val, true
		}
	}
	return common.Hash{}, false
}

// ============================================================================
// DSN-54: 成员侧模拟期链下 DAG patch 子图池 (simDAGPatches)
// ============================================================================

// StoreSimDAGPatch 将 leader 广播的子图按 (txHash, simulationNum) 导入 simDAGPatches。
// 同一 simNum 幂等覆盖；更大 simNum 覆盖旧轮（同一 tx 进入新一轮重试时旧轮子图作废）。
func (rs *retryScheduler) StoreSimDAGPatch(subgraph *api.SimPatchSubgraph) {
	if rs == nil || !rs.offChainDAG.isOn() || subgraph == nil {
		return
	}
	key := api.TxSimKey{TxHash: subgraph.TxHash, SimulationNum: subgraph.SimulationNum}
	rs.simDAGPatches.Store(key, subgraph)
	if l := utils.SSCLogger(); l != nil {
		l.Debug().Str("txHash", subgraph.TxHash.Hex()).
			Int("simNum", subgraph.SimulationNum).
			Int("nodes", len(subgraph.Nodes)).
			Msg("StoreSimDAGPatch: imported simDAGPatch subgraph")
	}
}

// GetSimDAGPatch 返回 (txHash, simulationNum) 对应的模拟期 patch 子图；不存在返回 nil。
func (rs *retryScheduler) GetSimDAGPatch(txHash common.Hash, simNum int) *api.SimPatchSubgraph {
	if !rs.offChainDAG.isOn() {
		return nil
	}
	val, ok := rs.simDAGPatches.Load(api.TxSimKey{TxHash: txHash, SimulationNum: simNum})
	if !ok {
		return nil
	}
	subgraph, _ := val.(*api.SimPatchSubgraph)
	return subgraph
}

// ReadSimDAGPatch 在 (txHash, simulationNum) 的模拟期子图中反查写了 (address,key) 的上游节点
// 并返回其写值。子图已含“被消费上游 + 传递闭包”的所有节点，故直接扫描全部节点写集即可，
// 无需递归（leader 构建时已保证确定性、防环）。命中 → 成员可用该值继续模拟（不撞锁）。
func (rs *retryScheduler) ReadSimDAGPatch(txHash common.Hash, simNum int, address common.Address, key common.Hash) (common.Hash, bool) {
	subgraph := rs.GetSimDAGPatch(txHash, simNum)
	if subgraph == nil {
		return common.Hash{}, false
	}
	for _, node := range subgraph.Nodes {
		if node == nil || node.Writes == nil || node.Writes.WriteState == nil {
			continue
		}
		if addrState, ok := node.Writes.WriteState.State[address]; ok {
			if val, ok := addrState[key]; ok {
				// covered-but-invisible 命中打点：说明该 key 本可被 simDAGPatch 覆盖
				chainRetryStats.SigSimDAGPatchHit.Add(1)
				return val, true
			}
		}
	}
	return common.Hash{}, false
}

// RemoveSimDAGPatches 删除某笔交易在 simDAGPatches 中的全部轮次子图。
// 在 tx close/stale 时调用，与 offChainDAG.Remove / closeTransaction 对齐。
func (rs *retryScheduler) RemoveSimDAGPatches(txHash common.Hash) {
	if !rs.offChainDAG.isOn() {
		return
	}
	rs.simDAGPatches.Range(func(key, _ interface{}) bool {
		simKey := key.(api.TxSimKey)
		if simKey.TxHash == txHash {
			rs.simDAGPatches.Delete(simKey)
		}
		return true
	})
	if l := utils.SSCLogger(); l != nil {
		l.Debug().Str("txHash", txHash.Hex()).
			Msg("RemoveSimDAGPatches: cleared simDAGPatches for tx")
	}
}

// buildRetrySimPatchSubgraph 依据本交易本轮已消费的上游（consumedPatches[txHash]）构建
// 模拟期 patch 子图：每个 consumed hash 先映射回 offChainDAG 节点取 simNum，再收集其传递闭包。
func (rs *retryScheduler) buildRetrySimPatchSubgraph(txHash common.Hash, simNum int, consumedTxHashes []common.Hash) *api.SimPatchSubgraph {
	if !rs.offChainDAG.isOn() || len(consumedTxHashes) == 0 {
		return nil
	}
	rootKeys := make([]api.TxSimKey, 0, len(consumedTxHashes))
	for _, h := range consumedTxHashes {
		nodeVal, ok := rs.offChainDAG.nodes.Load(h)
		if !ok {
			continue
		}
		node := nodeVal.(*OffChainPatchNode)
		rootKeys = append(rootKeys, api.TxSimKey{TxHash: h, SimulationNum: node.SimulationNum})
	}
	if len(rootKeys) == 0 {
		return nil
	}
	return rs.offChainDAG.buildSimPatchSubgraph(txHash, simNum, rootKeys)
}

// broadcastSimDAGPatch — DSN-54 (D1/D6)：leader 在 RetryCommit 消费上游、即将返回 Locked:true 前，
// 把该交易本轮模拟所需的上游 patch 子图（含传递闭包）先本地导入、再广播给本分片其它成员。
// 成员收到后 StoreSimDAGPatch 写入本地 simDAGPatches，供随后 StartReSimulation 触发的那轮模拟读取。
// 仅在 DAG 开启且确有 consumed 上游时执行；失败仅告警不阻塞 RetryCommit 主流程。
func (rs *retryScheduler) broadcastSimDAGPatch(retryTx *api.RetryTx) {
	if !rs.offChainDAG.isOn() || retryTx == nil {
		return
	}
	consumedVal, ok := rs.consumedPatches.Load(retryTx.TxHash)
	if !ok {
		return
	}
	consumedHashes, _ := consumedVal.([]common.Hash)
	if len(consumedHashes) == 0 {
		return
	}
	sub := rs.buildRetrySimPatchSubgraph(retryTx.TxHash, retryTx.SimulationNum, consumedHashes)
	if sub == nil || len(sub.Nodes) == 0 {
		return
	}

	// 1) leader 本地也导入（leader 自身也是 member，也通过 HandleSimulateRequest 参与模拟）
	//    作为自投递失败时的兜底（幂等覆盖）。
	rs.StoreSimDAGPatch(sub)

	// 2) 广播给本分片 committee 全体成员（含 leader 自己，index 0 = self）。
	//    leader 作为 member 与其它成员走同一条 StoreSimDAGPatch 导入路径，
	//    与 StartReSimulation 广播 HandleSimulateRequest 给全体成员(含自己)的写法一致。
	var epochVal api.Epoch
	if len(retryTx.Epochs) > int(rs.selfShard) {
		epochVal = retryTx.Epochs[rs.selfShard]
	}
	committee := rs.state.GetCommittee(epochVal, rs.selfShard)
	if committee == nil || len(committee.Members) == 0 {
		return
	}
	members := make([]*api.Member, 0, len(committee.Members))
	for _, m := range committee.Members {
		if m == nil {
			continue
		}
		members = append(members, m)
	}
	if len(members) == 0 {
		return
	}
	req := &api.StoreSimDAGPatchRequest{Subgraph: sub}
	ctx, cancel := context.WithTimeout(rs.ctx, 3*time.Second)
	defer cancel()
	if err := rs.comm.Multicast(ctx, members, api.Method_StoreSimDAGPatch, req); err != nil {
		utils.SSCLogger().Warn().Err(err).Str("txHash", retryTx.TxHash.Hex()).
			Int("simNum", retryTx.SimulationNum).
			Int("members", len(members)).
			Msg("broadcastSimDAGPatch: multicast failed")
	}
	utils.SSCLogger().Debug().Str("txHash", retryTx.TxHash.Hex()).
		Int("simNum", retryTx.SimulationNum).
		Int("nodes", len(sub.Nodes)).
		Int("members", len(members)).
		Msg("broadcastSimDAGPatch: sent simDAGPatch subgraph to members")
}

// OnPatchPoolUpdated 通知 RS 扫描新增 Patch 的 subscriber。
func (rs *retryScheduler) OnPatchPoolUpdated(writeSet *api.RWSet) {
	rs.scanPatchSubscribers(writeSet)
}
