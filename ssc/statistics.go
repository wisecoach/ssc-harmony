package ssc

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
)

// ============================================================================
// SimulationStats — 跨分片模拟实验全局统计
//
// 全原子计数器 + 定时 dump，用于诊断模拟流程中的瓶颈。
// 所有数据在实验运行结束后汇总到日志，供后续分析。
// ============================================================================

// SimulationStats 收集模拟过程中各阶段的性能指标。
// 大部分字段用 atomic 避免并发竞争，少量需要锁保护的字段用 mu。
type SimulationStats struct {
	mu sync.Mutex

	// =========================================================================
	// 队列层面（Q1-Q4）
	// =========================================================================

	// QueueWaitTotalNs / QueueWaitCount — 每个 tx 从 Push 到 PopOrWait 的等待时间
	QueueWaitTotalNs atomic.Int64
	QueueWaitCount   atomic.Int64

	// QueuePushCount / QueuePopCount — 推入和弹出的总数，差值为当前队列长度
	QueuePushCount atomic.Int64
	QueuePopCount  atomic.Int64

	// QueueEmptyPopCount — worker 取任务时队列为空的次数（= worker 空闲次数）
	QueueEmptyPopCount atomic.Int64

	// =========================================================================
	// Worker 执行层面（W1-W3）
	// =========================================================================

	// SimTotalNs / SimCount — 单次 processSimulationTask 的总耗时
	SimTotalNs atomic.Int64
	SimCount   atomic.Int64

	// P2pCallTotalNs / P2pCallCount — 跨分片 Comm.Call 的耗时
	P2pCallTotalNs atomic.Int64
	P2pCallCount   atomic.Int64

	// 模拟结果分类
	SimSuccessCount atomic.Int64 // CXT 成功
	SimFailCount    atomic.Int64 // 模拟执行失败

	// =========================================================================
	// 锁系统层面（L1-L6）
	// =========================================================================

	// LockableFailBase / LockableFailPending / LockableFailRlock — Lockable 失败细分
	LockableFailBase    atomic.Int64 // base snapshot 中有冲突写锁
	LockableFailPending atomic.Int64 // pending 中有冲突写锁
	LockableFailRlock   atomic.Int64 // 被读锁阻塞

	// SnapshotHit / SnapshotMiss — GetLockerAt 快照命中率
	SnapshotHit  atomic.Int64
	SnapshotMiss atomic.Int64

	// TempLockTryTotal / TempLockTryFail — TempLockView.TryLock 总调用和失败
	TempLockTryTotal atomic.Int64
	TempLockTryFail  atomic.Int64

	// PendingUnlockTotal / PendingUnlockBatch — Commit 时批量解锁
	PendingUnlockTotal atomic.Int64
	PendingUnlockBatch atomic.Int64

	// GlobalLockedSize — 每次 dump 时采样的全局锁数量（mutex 保护）
	GlobalLockedSamples []int

	// =========================================================================
	// 定时器层面（T1-T3）
	// =========================================================================

	Sp1TimerStarted  atomic.Int64
	Sp1TimerFired    atomic.Int64
	Sp1TimerRemoved  atomic.Int64
	PoolTimerStarted atomic.Int64
	PoolTimerFired   atomic.Int64
	PoolTimerRemoved atomic.Int64

	// =========================================================================
	// 重试层面（R1-R4）
	// =========================================================================

	RetryAddCount       atomic.Int64
	RetryReadySignal    atomic.Int64
	RetryNotReadySignal atomic.Int64
	RetrySuccessCount   atomic.Int64
	RetryFailCount      atomic.Int64

	// RetryPoolSize — 采样本采样（mutex 保护）
	RetryPoolSamples []int

	// =========================================================================
	// 内部状态
	// =========================================================================

	// HotKeyConflicts — LockKey → 冲突次数的热点统计
	// 使用 sync.Map 避免在 RLock 内加锁
	HotKeyConflicts sync.Map

	// CxtTxStages — Per-tx CXT 流程进度追踪（仅 origin shard leader 统计）
	// key=txHash(common.Hash) → value=*atomic.Int32（当前 stage 0-6）
	// 只进不退：setCxtStage 仅在 new > old 时更新
	// Stage 6 设置后自动从 map 删除（计入 CxtTxCompleted）
	CxtTxStages    sync.Map
	CxtTxTotal     atomic.Int64 // 累计追踪过的唯一 tx 数
	CxtTxCompleted atomic.Int64 // 已完成 tx 数（达到 stage 6 后清理）

	// LockHeldStats — LockKey → 锁持有时间汇总
	// 在 unlock 时计算: 总持有时间(纳秒) + 解锁次数
	LockHeldStats sync.Map

	// txBlockTraces — 各交易各阶段块高度记录
	// 由 loop() 定期从 sscService.txTraces snapshot 注入
	txBlockTraces map[common.Hash]*TxBlockTrace

	startTime   time.Time
	lastDumpSeq int64 // 上次 dump 时的序列号，用于差分
}

// lockHeldInfo 记录某个 LockKey 的锁持有时间汇总。
type lockHeldInfo struct {
	totalNs atomic.Int64 // 累计持有纳秒
	count   atomic.Int64 // 解锁次数
}

// newSimulationStats 创建并初始化统计实例。
func newSimulationStats() *SimulationStats {
	return &SimulationStats{
		startTime: time.Now(),
	}
}

// RecordLockConflict 记录一次 Lockable 失败的 LockKey 热点。
// 可在 RLock 内部安全调用（使用 sync.Map）。
func (s *SimulationStats) RecordLockConflict(key string) {
	actual, _ := s.HotKeyConflicts.LoadOrStore(key, new(atomic.Int64))
	actual.(*atomic.Int64).Add(1)
}

// RecordLockHeld 记录某个 LockKey 的一次锁持有时间（纳秒）。
// 在 unlock/unlockRLock 时调用（已在锁内部，用 sync.Map 安全）。
func (s *SimulationStats) RecordLockHeld(key string, heldNs int64) {
	info, _ := s.LockHeldStats.LoadOrStore(key, &lockHeldInfo{})
	h := info.(*lockHeldInfo)
	h.totalNs.Add(heldNs)
	h.count.Add(1)
}

// setCxtStage 更新指定 tx 的 CXT 进度 stage。
// CAS 循环 + 只进不退：只有 newStage > currentStage 时才更新。
// 当 stage == 6（已完成）时，从 map 删除并递增 CxtTxCompleted。
func (s *SimulationStats) setCxtStage(txHash common.Hash, stage int32) {
	actual, loaded := s.CxtTxStages.LoadOrStore(txHash, new(atomic.Int32))
	if !loaded {
		s.CxtTxTotal.Add(1) // 首次见到的 tx
	}
	cur := actual.(*atomic.Int32)
	for {
		old := cur.Load()
		if stage <= old {
			return // 只进不退
		}
		if cur.CompareAndSwap(old, stage) {
			// stage 6 = 完成，从 map 清理
			if stage >= 6 {
				s.CxtTxStages.Delete(txHash)
				s.CxtTxCompleted.Add(1)
			}
			return
		}
	}
}

// sampleQueueLen 采样当前队列长度到日志（由 dump 协程调用）。
func (s *SimulationStats) sampleQueueLen(push, pop int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.GlobalLockedSamples = append(s.GlobalLockedSamples, int(push-pop))
}

// sampleRetryPool 采样重试池大小。
func (s *SimulationStats) sampleRetryPool(size int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.RetryPoolSamples = append(s.RetryPoolSamples, size)
}

// Dump 打印当前所有统计数据的摘要到日志。
func (s *SimulationStats) Dump(mgr *stateLockManager) {
	elapsed := time.Since(s.startTime)
	queueWaitNs := s.QueueWaitTotalNs.Load()
	queueWaitCnt := s.QueueWaitCount.Load()
	simTotalNs := s.SimTotalNs.Load()
	simCnt := s.SimCount.Load()
	p2pNs := s.P2pCallTotalNs.Load()
	p2pCnt := s.P2pCallCount.Load()

	queueAvg := time.Duration(0)
	if queueWaitCnt > 0 {
		queueAvg = time.Duration(queueWaitNs / queueWaitCnt)
	}
	simAvg := time.Duration(0)
	if simCnt > 0 {
		simAvg = time.Duration(simTotalNs / simCnt)
	}
	p2pAvg := time.Duration(0)
	if p2pCnt > 0 {
		p2pAvg = time.Duration(p2pNs / p2pCnt)
	}

	pushCnt := s.QueuePushCount.Load()
	popCnt := s.QueuePopCount.Load()

	// 处理 mu 保护的采样数据
	s.mu.Lock()
	lockedSamples := make([]int, len(s.GlobalLockedSamples))
	copy(lockedSamples, s.GlobalLockedSamples)
	retrySamples := make([]int, len(s.RetryPoolSamples))
	copy(retrySamples, s.RetryPoolSamples)
	lockedAvg := 0
	if len(lockedSamples) > 0 {
		sum := 0
		for _, v := range lockedSamples {
			sum += v
		}
		lockedAvg = sum / len(lockedSamples)
	}
	retryAvg := 0
	if len(retrySamples) > 0 {
		sum := 0
		for _, v := range retrySamples {
			sum += v
		}
		retryAvg = sum / len(retrySamples)
	}
	s.mu.Unlock()

	// stuck 锁扫描：统计全局锁状态中持续持有超过 30 秒的 key
	var stuckLockedWriteCount, stuckLockedReadCount int
	var stuckLocks []string
	if mgr != nil {
		mgr.mu.RLock()
		threshold := 30 * time.Second
		now := time.Now()
		for lockKey, ls := range mgr.lockedStates.lockedStates {
			if now.Sub(ls.lockTime) > threshold {
				stuckLockedWriteCount++
				stuckLocks = append(stuckLocks, fmt.Sprintf("%s (held=%s, by=%s)", string(lockKey), now.Sub(ls.lockTime).Round(time.Second), ls.lockedBy.Hex()))
			}
		}
		for lockKey, rls := range mgr.lockedStates.rlockedStates {
			if now.Sub(rls.lockTime) > threshold {
				stuckLockedReadCount++
				by := rls.lockedBy[0].Hex()
				if len(rls.lockedBy) > 1 {
					by += fmt.Sprintf(" (+%d)", len(rls.lockedBy)-1)
				}
				stuckLocks = append(stuckLocks, fmt.Sprintf("%s (held=%s, by=%s)", string(lockKey), now.Sub(rls.lockTime).Round(time.Second), by))
			}
		}
		mgr.mu.RUnlock()
	}

	// per-tx stage 分布：遍历所有 in-flight tx，统计各 stage 的数量
	var stageDist [7]int
	s.CxtTxStages.Range(func(key, value interface{}) bool {
		stage := value.(*atomic.Int32).Load()
		if stage >= 0 && stage < 7 {
			stageDist[stage]++
		}
		return true
	})

	if len(stuckLocks) > 5 {
		stuckLocks = stuckLocks[:5]
	}

	utils.SSCLogger().Info().
		Dur("elapsed", elapsed).
		Int64("queuePush", pushCnt).
		Int64("queuePop", popCnt).
		Int64("queueLen", pushCnt-popCnt).
		Int64("queueEmptyPop", s.QueueEmptyPopCount.Load()).
		Dur("queueAvgWait", queueAvg).
		Int64("queueWaitCount", queueWaitCnt).
		Int64("simCount", simCnt).
		Dur("simAvgTime", simAvg).
		Int64("p2pCallCount", p2pCnt).
		Dur("p2pAvgTime", p2pAvg).
		Int64("simSuccess", s.SimSuccessCount.Load()).
		Int64("simFail", s.SimFailCount.Load()).
		Int64("lockableFailBase", s.LockableFailBase.Load()).
		Int64("lockableFailPending", s.LockableFailPending.Load()).
		Int64("lockableFailRlock", s.LockableFailRlock.Load()).
		Int64("snapshotHit", s.SnapshotHit.Load()).
		Int64("snapshotMiss", s.SnapshotMiss.Load()).
		Int64("tempLockTryTotal", s.TempLockTryTotal.Load()).
		Int64("tempLockTryFail", s.TempLockTryFail.Load()).
		Int64("pendingUnlockTotal", s.PendingUnlockTotal.Load()).
		Int64("pendingUnlockBatch", s.PendingUnlockBatch.Load()).
		Int("lockedGlobalAvg", lockedAvg).
		Int("lockedSamples", len(lockedSamples)).
		Int64("sp1Started", s.Sp1TimerStarted.Load()).
		Int64("sp1Fired", s.Sp1TimerFired.Load()).
		Int64("sp1Removed", s.Sp1TimerRemoved.Load()).
		Int64("poolStarted", s.PoolTimerStarted.Load()).
		Int64("poolFired", s.PoolTimerFired.Load()).
		Int64("poolRemoved", s.PoolTimerRemoved.Load()).
		Int64("retryAdd", s.RetryAddCount.Load()).
		Int64("retryReady", s.RetryReadySignal.Load()).
		Int64("retryNotReady", s.RetryNotReadySignal.Load()).
		Int64("retrySuccess", s.RetrySuccessCount.Load()).
		Int64("retryFail", s.RetryFailCount.Load()).
		Int64("cxtTxTotal", s.CxtTxTotal.Load()).
		Int("cxtTxStage0", stageDist[0]).
		Int("cxtTxStage1", stageDist[1]).
		Int("cxtTxStage2", stageDist[2]).
		Int("cxtTxStage3", stageDist[3]).
		Int("cxtTxStage4", stageDist[4]).
		Int("cxtTxStage5", stageDist[5]).
		Int64("cxtTxCompleted", s.CxtTxCompleted.Load()).
		Int64("retryPoolAvg", int64(retryAvg)).
		Int("retrySamples", len(retrySamples)).
		Interface("hotKeys", s.topHotKeys(3)).
		Interface("longestLocks", s.topLockedKeys(3, mgr)).
		Int("stuckLockedWriteCount", stuckLockedWriteCount).
		Int("stuckLockedReadCount", stuckLockedReadCount).
		Strs("stuckLocks", stuckLocks).
		Msg("=== STATS DUMP ===")

	s.dumpBlockSpanTraces()
}

// dumpBlockSpanTraces 输出 block height trace 中总跨度最大的前 5 条交易
func (s *SimulationStats) dumpBlockSpanTraces() {
	s.mu.Lock()
	traces := s.txBlockTraces
	s.mu.Unlock()

	if len(traces) == 0 {
		return
	}

	type spanEntry struct {
		hash  string
		trace *TxBlockTrace
		span  uint64
	}
	entries := make([]spanEntry, 0, len(traces))
	for _, trace := range traces {
		if !trace.HasAll() {
			continue
		}
		span := trace.CommitOrRollbackBlockNum - trace.SimulateBlockNum
		entries = append(entries, spanEntry{
			hash:  trace.TxHash.Hex()[:16],
			trace: trace,
			span:  span,
		})
	}

	if len(entries) == 0 {
		utils.SSCLogger().Info().Msg("[TX_TRACE] no completed traces yet")
		return
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].span > entries[j].span
	})

	n := 5
	if len(entries) < n {
		n = len(entries)
	}

	utils.SSCLogger().Info().
		Int("totalTraces", len(entries)).
		Msgf("[TX_TRACE] === TOP %d BLOCK SPAN TRACES ===", n)

	for i := 0; i < n; i++ {
		e := entries[i]
		utils.SSCLogger().Info().
			Str("tx", e.hash).
			Uint64("simulate", e.trace.SimulateBlockNum).
			Uint64("simSubmit", e.trace.SimulationTxSubmitBlockNum).
			Uint64("simCommit", e.trace.SimulationTxCommitBlockNum).
			Uint64("rollbackSubmit", e.trace.CommitOrRollbackTxSubmitBlockNum).
			Uint64("rollback", e.trace.CommitOrRollbackBlockNum).
			Uint64("totalSpan", e.span).
			Int64("gapSimToSubmit", e.trace.Gap(StageSimulateCX, StageSimulationTxSubmit)).
			Int64("gapSubmitToCommit", e.trace.Gap(StageSimulationTxSubmit, StageSimulationTxCommit)).
			Int64("gapCommitToRollbackSubmit", e.trace.Gap(StageSimulationTxCommit, StageCommitOrRollbackTxSubmit)).
			Int64("gapRollbackSubmitToRollback", e.trace.Gap(StageCommitOrRollbackTxSubmit, StageCommitOrRollback)).
			Msgf("[TX_TRACE] #%d: span=%d blocks", i+1, e.span)
	}
}

// SetBlockTraces 注入块高度 trace snapshot
func (s *SimulationStats) SetBlockTraces(traces map[common.Hash]*TxBlockTrace) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.txBlockTraces = make(map[common.Hash]*TxBlockTrace, len(traces))
	for k, v := range traces {
		s.txBlockTraces[k] = v
	}
}

// topLockedKeys 返回持有时间最长的 N 个 LockKey（按累计持有时间降序）。
// 格式: ["key1 (avg=Xms, cnt=N, by=txHash)", "key2 (avg=Yms, cnt=M, by=txHash)", ...]
// 如果 mgr 不为 nil，则显示当前持有该 key 的 txHash。
func (s *SimulationStats) topLockedKeys(n int, mgr *stateLockManager) []string {
	type kv struct {
		key     string
		totalNs int64
		count   int64
	}
	var entries []kv
	s.LockHeldStats.Range(func(k, v interface{}) bool {
		h := v.(*lockHeldInfo)
		entries = append(entries, kv{
			key:     k.(string),
			totalNs: h.totalNs.Load(),
			count:   h.count.Load(),
		})
		return true
	})
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].totalNs > entries[j].totalNs
	})
	top := n
	if len(entries) < top {
		top = len(entries)
	}
	result := make([]string, 0, top)
	for i := 0; i < top; i++ {
		e := entries[i]
		avgMs := e.totalNs / (e.count + 1) / 1e6
		by := ""
		if mgr != nil {
			mgr.mu.RLock()
			if ls, exists := mgr.lockedStates.lockedStates[api.LockKey(e.key)]; exists {
				by = ls.lockedBy.Hex()
			} else if rls, exists := mgr.lockedStates.rlockedStates[api.LockKey(e.key)]; exists {
				if len(rls.lockedBy) > 0 {
					by = rls.lockedBy[0].Hex()
				}
			}
			mgr.mu.RUnlock()
		}
		if by != "" {
			result = append(result, fmt.Sprintf("%s (avg=%dms, cnt=%d, by=%s)", e.key, avgMs, e.count, by))
		} else {
			result = append(result, fmt.Sprintf("%s (avg=%dms, cnt=%d)", e.key, avgMs, e.count))
		}
	}
	return result
}

// topHotKeys 返回冲突次数最多的 N 个 LockKey。
func (s *SimulationStats) topHotKeys(n int) []string {
	// 不能用 defer s.mu.Lock() 因为 HotKeyConflicts 是 sync.Map（已线程安全）
	type kv struct {
		key string
		val int64
	}
	var entries []kv
	s.HotKeyConflicts.Range(func(k, v interface{}) bool {
		entries = append(entries, kv{
			key: k.(string),
			val: v.(*atomic.Int64).Load(),
		})
		return true
	})
	// 按冲突次数降序
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].val > entries[j].val
	})
	top := n
	if len(entries) < top {
		top = len(entries)
	}
	result := make([]string, 0, top)
	for i := 0; i < top; i++ {
		result = append(result, entries[i].key)
	}
	return result
}

// DiffDump 打印上次 dump 以来的增量（适用于高频 dump 场景）。
func (s *SimulationStats) DiffDump() {
	// 简单实现：直接全量 dump，统计脚本后续处理
	s.Dump(nil)
}
