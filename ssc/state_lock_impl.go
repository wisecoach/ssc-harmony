// Copyright 2026 Harmony One
// This file implements versioned state locker with snapshot support.
// It allows retrieving locker state at a specific stateRoot, solving the
// consistency issue between stateDB (versioned) and locker (real-time global).
//
// Key features:
// - Snapshot-based versioning: saves locker state at each stateDB.Commit()
// - LRU + time-based eviction: automatically cleans up old snapshots
// - Thread-safe: all operations are protected by RWMutex
// - Backward compatible: original journal/revert mechanism unchanged

package ssc

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
)

// ============================================================================
// lockedTxKey — 用于对锁状态按 (nonce, sender) 排序
// 死锁预防：全局写锁顺序按此结构排序，确保 Lock 不产生循环等待。
// ============================================================================

// lockedTxKey 定义全局锁的顺序键。
// compareLockedTx 提供排序比较，先比较 nonce，再比较 sender 地址。
type lockedTxKey struct {
	nonce         uint64
	simulationNum int
	sender        common.Address
}

// compareLockedTx 比较两个 lockedTxKey，按 (nonce, sender) 字典序。
func compareLockedTx(a any, b any) int {
	tx1 := a.(lockedTxKey)
	tx2 := b.(lockedTxKey)
	if tx1.nonce != tx2.nonce {
		return int(tx1.nonce) - int(tx2.nonce)
	}
	return bytes.Compare(tx1.sender[:], tx2.sender[:])
}

// ============================================================================
// lockedStates — 锁状态存储核心数据结构
//
// 存储所有写锁（lockedStates）和读锁（rlockedStates）的全局状态。
// 提供按 txHash+callIndex 的反向索引，用于高效清理和 unlock。
//
// 注意：本结构体不是线程安全的，调用方需在更高层加锁。
// ============================================================================

// newLockedStates 创建一个空的锁状态集合。
func newLockedStates() *lockedStates {
	return &lockedStates{
		lockedStates:           make(map[api.LockKey]*lockedState),
		rlockedStates:          make(map[api.LockKey]*rlockedState),
		callIndex2lockedState:  make(map[common.Hash]map[string]map[api.LockKey]common.Hash),
		callIndex2rlockedState: make(map[common.Hash]map[string]map[api.LockKey]struct{}),
	}
}

// lockedStates 存储跨分片交易模拟过程中的所有锁状态。
// 包含四个映射：
//
//	lockedStates           — LockKey → *lockedState（写锁：谁锁了哪个状态）
//	rlockedStates          — LockKey → *rlockedState（读锁：谁在读哪个状态）
//	callIndex2lockedState  — txHash → callIndex → LockKey → value（写锁反向索引）
//	callIndex2rlockedState — txHash → callIndex → LockKey → struct{}（读锁反向索引）
//
// 正向映射用于冲突检测：给定一个 LockKey，快速判断是否已被其他交易锁住。
// 反向映射用于事务清理：给定一个 txHash，快速找出其所有锁并释放。
type lockedStates struct {
	rlockedStates          map[api.LockKey]*rlockedState
	lockedStates           map[api.LockKey]*lockedState                           // address + key → TxHash, 写锁主映射
	callIndex2lockedState  map[common.Hash]map[string]map[api.LockKey]common.Hash // TxHash → callIndex → lockKey → value, 写锁反向索引
	callIndex2rlockedState map[common.Hash]map[string]map[api.LockKey]struct{}    // TxHash → callIndex → lockKey → value, 读锁反向索引
}

func (s *lockedStates) RLockedStates() map[api.LockKey]*rlockedState {
	return s.rlockedStates
}

func (s *lockedStates) LockedStates() map[api.LockKey]*lockedState {
	return s.lockedStates
}

func (s *lockedStates) CallIndex2LockedStates() map[common.Hash]map[string]map[api.LockKey]common.Hash {
	return s.callIndex2lockedState
}

// length 返回写锁的数量。
func (s *lockedStates) length() int {
	if s == nil {
		return 0
	}
	return len(s.lockedStates)
}

// clear 清除所有未被占用的写锁（lockedBy 为零值的锁）。
// 在 handleLockCommit 的 Commit 阶段被调用，释放那些已提交的临时锁。
func (s *lockedStates) clear() {
	for key, state := range s.lockedStates {
		if bytes.Compare(state.lockedBy.Bytes(), common.Hash{}.Bytes()) == 0 {
			delete(s.lockedStates, key)
		}
	}
}

// init 为给定的 LockKey 创建一个初始状态（lockedBy = zero hash，表示空闲）。
func (s *lockedStates) init(key api.LockKey) *lockedState {
	s.lockedStates[key] = &lockedState{
		lockedBy: common.Hash{},
		version:  0,
	}
	return s.lockedStates[key]
}

// getCallIndex2LockedStates 返回给定 txHash 的写锁反向映射。
func (s *lockedStates) getCallIndex2LockedStates(txHash common.Hash) map[string]map[api.LockKey]common.Hash {
	return s.callIndex2lockedState[txHash]
}

// getCallIndex2RLockedStates 返回给定 txHash 的读锁反向映射。
func (s *lockedStates) getCallIndex2RLockedStates(txHash common.Hash) map[string]map[api.LockKey]struct{} {
	return s.callIndex2rlockedState[txHash]
}

// getLockedValue 返回给定 (txHash, callIndex, key) 的锁定值。
func (s *lockedStates) getLockedValue(txHash common.Hash, callIndexStr string, key api.LockKey) common.Hash {
	if s.callIndex2lockedState[txHash] == nil {
		return common.Hash{}
	}
	if s.callIndex2lockedState[txHash][callIndexStr] == nil {
		return common.Hash{}
	}
	return s.callIndex2lockedState[txHash][callIndexStr][key]
}

// setLockedValue 设置给定 (txHash, callIndex, key) 的锁定值（自动创建映射层级）。
func (s *lockedStates) setLockedValue(txHash common.Hash, callIndexStr string, key api.LockKey, value common.Hash) {
	if s.callIndex2lockedState[txHash] == nil {
		s.callIndex2lockedState[txHash] = make(map[string]map[api.LockKey]common.Hash)
	}
	if s.callIndex2lockedState[txHash][callIndexStr] == nil {
		s.callIndex2lockedState[txHash][callIndexStr] = make(map[api.LockKey]common.Hash)
	}
	s.callIndex2lockedState[txHash][callIndexStr][key] = value
}

// addLockedState 添加一个写锁条目到反向索引，同时设置 lockedStates[key]。
func (s *lockedStates) addLockedState(txHash common.Hash, callIndexStr string, key api.LockKey, value common.Hash, state *lockedState) {
	if s.callIndex2lockedState[txHash] == nil {
		s.callIndex2lockedState[txHash] = make(map[string]map[api.LockKey]common.Hash)
	}
	if s.callIndex2lockedState[txHash][callIndexStr] == nil {
		s.callIndex2lockedState[txHash][callIndexStr] = make(map[api.LockKey]common.Hash)
	}
	s.callIndex2lockedState[txHash][callIndexStr][key] = value
	s.lockedStates[key] = state
}

// addRLockedState 添加一个读锁条目到反向索引，同时设置 rlockedStates[key]。
func (s *lockedStates) addRLockedState(txHash common.Hash, callIndexStr string, key api.LockKey, state *rlockedState) {
	if s.callIndex2rlockedState[txHash] == nil {
		s.callIndex2rlockedState[txHash] = make(map[string]map[api.LockKey]struct{})
	}
	if s.callIndex2rlockedState[txHash][callIndexStr] == nil {
		s.callIndex2rlockedState[txHash][callIndexStr] = make(map[api.LockKey]struct{})
	}
	s.callIndex2rlockedState[txHash][callIndexStr][key] = struct{}{}
	s.rlockedStates[key] = state
}

// deleteLockedState 从反向索引和正向映射中删除一个写锁。
// 返回被删除的值。
func (s *lockedStates) deleteLockedState(txHash common.Hash, callIndexStr string, key api.LockKey) common.Hash {
	if s.lockedStates[key] == nil {
		return common.Hash{}
	}
	s.lockedStates[key].lockedBy = common.Hash{}
	delete(s.lockedStates, key)
	value := s.callIndex2lockedState[txHash][callIndexStr][key]
	delete(s.callIndex2lockedState[txHash][callIndexStr], key)
	if len(s.callIndex2lockedState[txHash][callIndexStr]) == 0 {
		delete(s.callIndex2lockedState[txHash], callIndexStr)
	}
	if len(s.callIndex2lockedState[txHash]) == 0 {
		delete(s.callIndex2lockedState, txHash)
	}
	return value
}

// deleteRLockedState 从反向索引和正向映射中删除一个读锁。
func (s *lockedStates) deleteRLockedState(txHash common.Hash, callIndexStr string, key api.LockKey) {
	if s.rlockedStates[key] == nil {
		return
	}
	delete(s.rlockedStates, key)
	delete(s.callIndex2rlockedState[txHash][callIndexStr], key)
	if len(s.callIndex2rlockedState[txHash][callIndexStr]) == 0 {
		delete(s.callIndex2rlockedState[txHash], callIndexStr)
	}
	if len(s.callIndex2rlockedState[txHash]) == 0 {
		delete(s.callIndex2rlockedState, txHash)
	}
}

// ============================================================================
// stateLockManager with Versioning Support
//
// stateLockManager 是全局锁状态的管理器，负责：
// 1. 保存所有已上链交易的锁状态（lockedStates）
// 2. 在每次 stateDB.Commit() 时创建锁状态快照（按 stateRoot 索引）
// 3. 为每个模拟交易提供与其 stateRoot 匹配的 stateLocker 实例
//
// 快照机制解决了 stateDB（版本化）和 locker（实时全局）之间的一致性问题：
// 模拟交易在 stateRoot A 上执行，就应该看到 stateRoot A 时的锁状态。
// 不能因为后续区块提交导致锁状态变化，而影响正在模拟中的交易判断。
// ============================================================================

// stateLockManager 是锁状态管理器的顶层结构。
// 改造后无锁化：去掉 mu，用 sync.Map + atomic 替代。
type stateLockManager struct {
	sscService   *sscService
	tempLockView *TempLockView

	// ── 无锁化替换 ──
	// 全局锁状态改用 sync.Map 支持并发无锁读写
	globalLockedStates   sync.Map // LockKey → *lockedState（写锁，含 version）
	globalRLockedStates  sync.Map // LockKey → *rlockedState（读锁）
	globalFinishedTxs    sync.Map // common.Hash → bool
	globalLockStartBlock sync.Map // LockKey → uint64

	// 版本化（atomic 递增，用于 MVCC 过滤）
	version atomic.Uint64 // 当前全局版本

	// 版本边界（替代快照 map）
	prevRoot    common.Hash // 上一个 stateRoot（Verify 用）
	prevVersion uint64      // 上一个版本号

	// 当前状态（atomic 读写）
	currentRoot atomic.Value // common.Hash

	// ── 保留旧字段（用于 pendingStates 的 Commit 合并，单线程访问） ──
	commitCnt       uint64
	currentBlockNum uint64
	lockedStates    *lockedStates        // 旧版全局 lockedStates（pendingStates 合并目标，同步写入）
	finishedTxs     map[common.Hash]bool // 旧版 finishedTxs
	lockStartBlock  map[api.LockKey]uint64
}

// newStateLockManager 创建一个新的锁管理器（无锁版本）。
func newStateLockManager(service *sscService) *stateLockManager {
	mgr := &stateLockManager{
		sscService:      service,
		commitCnt:       0,
		currentBlockNum: 0,
		lockedStates:    newLockedStates(),
		finishedTxs:     make(map[common.Hash]bool),
		lockStartBlock:  make(map[api.LockKey]uint64),
	}

	mgr.version.Store(0)
	mgr.currentRoot.Store(common.Hash{})
	mgr.tempLockView = NewTempLockView(mgr)

	return mgr
}

// ============================================================================
// lockedState & rlockedState — 锁状态描述符
// ============================================================================

// lockedState 描述一个写锁的状态。
// lockedBy 为 common.Hash{} 时表示锁空闲。
type lockedState struct {
	lockedBy common.Hash // 持有此写锁的交易哈希
	lockTime time.Time
	version  uint64 // 创建时的版本号（新增）
}

// lockable 检查此写锁能否被 txHash 获取：
// - 如果锁空闲（lockedBy == zero），可以获取
// - 如果锁已被同一个 txHash 持有，可以重入
// - 如果锁被其他交易持有，不可获取
func (ls *lockedState) lockable(txHash common.Hash) bool {
	if bytes.Compare(ls.lockedBy.Bytes(), common.Hash{}.Bytes()) == 0 {
		return true
	}
	if bytes.Compare(ls.lockedBy.Bytes(), txHash.Bytes()) == 0 {
		return true
	}
	return false
}

// rlockedState 描述一个读锁的状态。
// 读锁可以被多个交易同时持有（lockedBy 列表）。
type rlockedState struct {
	locked   bool
	lockedBy []common.Hash // 持有此读锁的所有交易哈希
	lockTime time.Time
}

// ============================================================================
// handleLockCommit — 版本更新（无锁化版本）
//
// 改前：深拷贝 + clear()，O(n)，需全局锁
// 改后：version++，O(1)，无锁
// ============================================================================

// handleLockCommit 版本更新。不做深拷贝，只递增版本号。
// 旧数据靠 version 过滤，不再删除。
func (s *stateLockManager) handleLockCommit(newRoot common.Hash) error {
	t0 := time.Now()

	newVersion := s.version.Add(1) // 版本递增

	// 记录上一个版本边界（Verify 用）
	s.prevVersion = newVersion - 1
	oldRoot := s.currentRoot.Swap(newRoot).(common.Hash)
	s.prevRoot = oldRoot

	// 旧字段同步（单线程，安全）
	s.commitCnt++
	s.lockedStates.clear()

	// 更新区块号
	if s.sscService != nil && s.sscService.bc != nil {
		s.currentBlockNum = s.sscService.bc.CurrentHeader().NumberU64()
	}

	// 锁状态统计（只读，单线程安全）
	lockedStatesCount := s.lockedStates.length()
	lockedTxCount := len(s.lockedStates.callIndex2lockedState)

	// 统计锁持有时间
	type lockInfo struct {
		txHash string
		key    string
		block  uint64
	}
	staleLocks := make([]lockInfo, 0)
	for key, state := range s.lockedStates.lockedStates {
		startBlock, hasStart := s.lockStartBlock[key]
		if !hasStart {
			s.lockStartBlock[key] = s.currentBlockNum
		} else {
			heldBlocks := s.currentBlockNum - startBlock
			if heldBlocks > 30 {
				staleLocks = append(staleLocks, lockInfo{
					txHash: state.lockedBy.Hex()[:16],
					key:    string(key),
					block:  heldBlocks,
				})
			}
		}
	}
	for key := range s.lockStartBlock {
		if _, stillLocked := s.lockedStates.lockedStates[key]; !stillLocked {
			delete(s.lockStartBlock, key)
		}
	}

	total := time.Since(t0)
	utils.SSCLogger().Info().
		Uint64("newVersion", newVersion).
		Uint64("prevVersion", s.prevVersion).
		Str("oldRoot", oldRoot.Hex()).
		Str("newRoot", newRoot.Hex()).
		Uint64("blockNum", s.currentBlockNum).
		Int("lockedStates", lockedStatesCount).
		Int("lockedTx", lockedTxCount).
		Int("staleLockCount", len(staleLocks)).
		Dur("total", total).
		Msg("locker version updated")

	for _, l := range staleLocks {
		utils.SSCLogger().Warn().
			Str("txHash", l.txHash).
			Str("lockKey", l.key).
			Uint64("heldBlocks", l.block).
			Msg("LOCK_STALE: lock held more than 10 blocks, possible leak")
	}

	return nil
}

// ============================================================================
// InitLockManager — 创世状态初始化
// ============================================================================

// InitLockManager 初始化锁管理器。
func (s *stateLockManager) InitLockManager(genesisRoot common.Hash) error {
	s.currentRoot.Store(genesisRoot)
	utils.SSCLogger().Info().
		Str("stateRoot", genesisRoot.Hex()).
		Msg("locker inited")
	return nil
}

// ============================================================================
// GetLockerAt — 版本化的 stateLocker 创建
//
// 核心入口：模拟交易在 stateRoot 上执行时，获取该 stateRoot 对应的锁上下文。
// 返回的 stateLocker 包含：
//   - baseVersion: 创建时的版本号（用于 MVCC 过滤）
//   - pendingStates: 空的隔离 pendingState，用于本次模拟的锁操作
//
// 不再维护历史快照 map，只保留当前版本 + 上一个版本。
// ============================================================================

// GetLockerAt 返回一个与指定 stateRoot 对应的 stateLocker 实例。
func (s *stateLockManager) GetLockerAt(stateRoot common.Hash) (api.StateLocker, error) {
	curRoot := s.currentRoot.Load().(common.Hash)
	curVer := s.version.Load()

	baseVersion := curVer

	// 请求当前版本或空 root → 用当前版本
	if stateRoot == curRoot || stateRoot == (common.Hash{}) {
		utils.SSCLogger().Info().
			Str("stateRoot", stateRoot.Hex()).
			Str("currentRoot", curRoot.Hex()).
			Bool("isCurrent", stateRoot == curRoot).
			Uint64("baseVersion", baseVersion).
			Msgf("GetLockerAt: current state locker (root=%s)", stateRoot.Hex()[:20])
	} else if stateRoot == s.prevRoot {
		// 请求上一个版本 → 用 prevVersion 过滤
		baseVersion = s.prevVersion
		utils.SSCLogger().Info().
			Str("stateRoot", stateRoot.Hex()).
			Str("currentRoot", curRoot.Hex()).
			Uint64("baseVersion", baseVersion).
			Msgf("GetLockerAt: returning previous version (root=%s)", stateRoot.Hex()[:20])
	} else {
		// 更旧的版本 → 降级为当前版本（warn 日志）
		s.sscService.stats.SnapshotMiss.Add(1)
		utils.SSCLogger().Warn().
			Str("stateRoot", stateRoot.Hex()).
			Str("currentRoot", curRoot.Hex()).
			Msgf("GetLockerAt: unknown root, using current (root=%s)", stateRoot.Hex()[:20])
		baseVersion = curVer
	}

	return &stateLocker{
		root:             stateRoot,
		baseVersion:      baseVersion,
		stateLockManager: s,

		txLocks: make(map[common.Hash]*sync.Mutex),

		pendingStates: &lockedStates{
			lockedStates:          make(map[api.LockKey]*lockedState),
			callIndex2lockedState: make(map[common.Hash]map[string]map[api.LockKey]common.Hash),
		},
		tmpFinishedTxs: make(map[common.Hash]bool),
		journal:        &lockJournal{entries: make([]journalEntry, 0)},
		validRevisions: make([]revision, 0),
		nextRevisionId: 0,
		pendingUnlocks: &pendingUnlockCache{
			unlockedTxs: make(map[common.Hash]bool),
		},
	}, nil
}

// GetLocker 返回当前状态的 locker 实例（向后兼容）。
func (s *stateLockManager) GetLocker() api.StateLocker {
	curRoot := s.currentRoot.Load().(common.Hash)
	locker, _ := s.GetLockerAt(curRoot)
	return locker
}

// ============================================================================
// ============================================================================
// 辅助方法
// ============================================================================

// GetTempLockView 返回与此管理器关联的 TempLockView（临时锁视图）。
func (s *stateLockManager) GetTempLockView() *TempLockView {
	return s.tempLockView
}

// GetRWLockStates 返回当前全局写锁和读锁的副本。
// 被 TempLockView.OnBlockCommitted 调用来同步已提交的锁状态。
// 改后：从 globalLockedStates（sync.Map）读取。
func (s *stateLockManager) GetRWLockStates() (map[api.LockKey]*lockedState, map[api.LockKey]*rlockedState) {
	if s == nil {
		panic("stateLockManager is nil")
	}

	writeLockedStates := make(map[api.LockKey]*lockedState)
	readLockedStates := make(map[api.LockKey]*rlockedState)

	s.globalLockedStates.Range(func(k, v interface{}) bool {
		writeLockedStates[k.(api.LockKey)] = v.(*lockedState)
		return true
	})
	s.globalRLockedStates.Range(func(k, v interface{}) bool {
		readLockedStates[k.(api.LockKey)] = v.(*rlockedState)
		return true
	})

	return writeLockedStates, readLockedStates
}

// GetPendingLockStates 返回一个 stateLocker 实例中待提交（pending）的锁状态。
// 用于调试和监控事务隔离。
func (s *stateLocker) GetPendingLockStates() (map[api.LockKey]*lockedState, map[api.LockKey]*rlockedState) {
	writeLockedStates := make(map[api.LockKey]*lockedState)
	for k, v := range s.pendingStates.lockedStates {
		writeLockedStates[k] = v
	}
	return writeLockedStates, nil
}

// GetFullLockStates 返回 base snapshot + pendingStates 的合并视图。
// 改后：基于 baseVersion 过滤的全局 lockedStates + pendingStates。
func (s *stateLocker) GetFullLockStates() (map[api.LockKey]*lockedState, map[api.LockKey]*rlockedState) {
	writeLockedStates := make(map[api.LockKey]*lockedState)

	// 从全局 lockedStates 读取版本 >= baseVersion 的锁
	s.globalLockedStates.Range(func(k, v interface{}) bool {
		st := v.(*lockedState)
		if st.version >= s.baseVersion {
			writeLockedStates[k.(api.LockKey)] = st
		}
		return true
	})

	// 用 pendingStates 覆盖
	for k, v := range s.pendingStates.lockedStates {
		writeLockedStates[k] = v
	}

	return writeLockedStates, nil
}

// printTxNums 打印锁管理器中的交易数量统计信息。
func (s *stateLockManager) printTxNums() {
	sn := 0
	lockedStateNum := 0
	s.globalLockedStates.Range(func(_, _ interface{}) bool {
		lockedStateNum++
		return true
	})

	utils.SSCLogger().Info().
		Int("commitCnt", int(s.commitCnt)).
		Int("lockedStateNum", lockedStateNum).
		Int("stateNum", sn).
		Msg("print waiting tx nums")
}
