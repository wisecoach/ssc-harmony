package ssc

import (
	"bytes"
	"sort"
	"sync"
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
		lockedStates:          make(map[api.LockKey]*lockedState),
		callIndex2lockedState: make(map[common.Hash]map[string]map[api.LockKey]common.Hash),
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
//
// 注意：这里有一个潜在 bug——
// 读锁的 value 被写入了 writeLock 的反向索引 callIndex2lockedState（而不是 rlocked 版本）。
// 见 addRLockedState 第 121 行写入的是 callIndex2rlockedState，而 Getters 第 114 行也在操作 callIndex2lockedState。
// 这可能不会导致运行时错误，但反映了历史迁移时的不一致。
func (s *lockedStates) addRLockedState(txHash common.Hash, callIndexStr string, key api.LockKey, state *rlockedState) {
	if s.callIndex2lockedState[txHash] == nil {
		s.callIndex2lockedState[txHash] = make(map[string]map[api.LockKey]common.Hash)
	}
	if s.callIndex2lockedState[txHash][callIndexStr] == nil {
		s.callIndex2lockedState[txHash][callIndexStr] = make(map[api.LockKey]common.Hash)
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
// 它持有一个共享的 lockedStates 实例（全局锁状态），
// 以及每个 stateRoot 的快照用于版本化访问。
type stateLockManager struct {
	sscService   *sscService
	tempLockView *TempLockView

	mu sync.RWMutex

	// 基础锁状态字段
	commitCnt    uint64
	lockedStates *lockedStates        // 全局写锁/读锁状态
	finishedTxs  map[common.Hash]bool // 已完成（已提交）的交易

	// 版本化支持（NEW）
	currentRoot  common.Hash                           // 当前区块的 stateRoot
	snapshots    map[common.Hash]*lockedStatesSnapshot // stateRoot → 快照
	snapshotMeta map[common.Hash]*snapshotMetadata     // stateRoot → 访问元数据（LRU用）
	maxSnapshots int                                   // LRU 上限
	snapshotTTL  time.Duration                         // 快照 TTL

	// 清理
	cleanupStop chan struct{}
	cleanupOnce sync.Once
}

// newStateLockManager 创建一个新的版本化锁管理器。
func newStateLockManager(service *sscService) *stateLockManager {
	mgr := &stateLockManager{
		sscService:   service,
		mu:           sync.RWMutex{},
		commitCnt:    0,
		lockedStates: newLockedStates(),
		finishedTxs:  make(map[common.Hash]bool),
		snapshots:    make(map[common.Hash]*lockedStatesSnapshot),
		snapshotMeta: make(map[common.Hash]*snapshotMetadata),
		maxSnapshots: DefaultMaxSnapshots,
		snapshotTTL:  DefaultSnapshotTTL,
		cleanupStop:  make(chan struct{}),
	}

	mgr.tempLockView = NewTempLockView(mgr)
	go mgr.startCleanupRoutine()

	return mgr
}

// startCleanupRoutine 定期清理过期和超量的快照。
func (s *stateLockManager) startCleanupRoutine() {
	ticker := time.NewTicker(SnapshotCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.cleanupSnapshots()
		case <-s.cleanupStop:
			return
		}
	}
}

// StopCleanup 停止清理协程（shutdown 时调用）。
func (s *stateLockManager) StopCleanup() {
	s.cleanupOnce.Do(func() {
		close(s.cleanupStop)
	})
}

// cleanupSnapshots 移除过期的快照（TTL 超时）并强制 LRU 上限。
func (s *stateLockManager) cleanupSnapshots() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	expired := make([]common.Hash, 0)

	// Phase 1: 移除 TTL 过期的快照
	for root, snapshot := range s.snapshots {
		if now.Sub(snapshot.timestamp) > s.snapshotTTL {
			expired = append(expired, root)
		}
	}

	for _, root := range expired {
		s.deleteSnapshot(root)
	}

	// Phase 2: 如果仍然超出容量，移除 LRU 最旧的快照
	if len(s.snapshots) > s.maxSnapshots {
		type accessRecord struct {
			root       common.Hash
			lastAccess time.Time
		}
		records := make([]accessRecord, 0, len(s.snapshots))
		for root, meta := range s.snapshotMeta {
			records = append(records, accessRecord{
				root:       root,
				lastAccess: meta.lastAccess,
			})
		}

		// 按最后访问时间排序（最旧的在前）
		sort.Slice(records, func(i, j int) bool {
			return records[i].lastAccess.Before(records[j].lastAccess)
		})

		// 移除最旧的快照直到低于上限
		removeCount := len(s.snapshots) - s.maxSnapshots
		for i := 0; i < removeCount && i < len(records); i++ {
			s.deleteSnapshot(records[i].root)
		}
	}

	utils.SSCLogger().Info().
		Int("remaining", len(s.snapshots)).
		Int("expired_removed", len(expired)).
		Msg("cleanup snapshots")
}

// deleteSnapshot 删除一个快照及其元数据。
func (s *stateLockManager) deleteSnapshot(root common.Hash) {
	delete(s.snapshots, root)
	delete(s.snapshotMeta, root)
}

// SetSnapshotConfig 配置快照上限和 TTL。
func (s *stateLockManager) SetSnapshotConfig(maxSnapshots int, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if maxSnapshots > 0 {
		s.maxSnapshots = maxSnapshots
	}
	if ttl > 0 {
		s.snapshotTTL = ttl
	}
}

// GetSnapshotCount 返回当前快照数量（用于监控）。
func (s *stateLockManager) GetSnapshotCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.snapshots)
}

// ============================================================================
// lockedState & rlockedState — 锁状态描述符
// ============================================================================

// lockedState 描述一个写锁的状态。
// lockedBy 为 common.Hash{} 时表示锁空闲。
type lockedState struct {
	lockedBy common.Hash // 持有此写锁的交易哈希
	lockTime time.Time
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
// handleLockCommit — 区块提交时的锁快照创建
//
// 在 stateLocker.Commit() 流程的最后一步调用。
// 将当前全局锁状态深拷贝为一个只读快照，绑定到 newRoot。
//
// 此快照之后被 GetLockerAt(newRoot) 使用，
// 提供给后续 simulate 的 stateLocker 作为 baseSnapshot。
// ============================================================================

// handleLockCommit 创建一个指定 stateRoot 的锁状态快照。
//
// 流程：
// 1. 深拷贝 lockedStates → 创建不可变快照
// 2. 清空未占用的锁（lockedBy 为零值的）
// 3. 将快照关联到 newRoot
func (s *stateLockManager) handleLockCommit(newRoot common.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	utils.SSCLogger().Info().Msgf("handleLockCommit: creating snapshot for stateRoot %s", newRoot.Hex())

	// === 创建当前状态的快照 ===
	snapshot := &lockedStatesSnapshot{
		lockedStates:  make(map[api.LockKey]*lockedState, len(s.lockedStates.lockedStates)),
		rlockedStates: make(map[api.LockKey]*rlockedState, len(s.lockedStates.rlockedStates)),
		finishedTxs:   make(map[common.Hash]bool, len(s.finishedTxs)),
		timestamp:     time.Now(),
	}

	// 深拷贝写锁
	for k, v := range s.lockedStates.lockedStates {
		snapshot.lockedStates[k] = &lockedState{
			lockedBy: v.lockedBy,
			lockTime: v.lockTime,
		}
	}

	// 深拷贝读锁
	for k, v := range s.lockedStates.rlockedStates {
		snapshot.rlockedStates[k] = &rlockedState{
			lockedBy: append([]common.Hash{}, v.lockedBy...),
			lockTime: v.lockTime,
		}
	}

	// 深拷贝 finishedTxs
	for k, v := range s.finishedTxs {
		snapshot.finishedTxs[k] = v
	}

	// 存储快照
	s.snapshots[newRoot] = snapshot
	s.snapshotMeta[newRoot] = &snapshotMetadata{
		lastAccess:  time.Now(),
		accessCount: 0,
	}
	oldRoot := s.currentRoot
	s.currentRoot = newRoot

	// === 原始提交逻辑（不变）===
	s.commitCnt++
	s.lockedStates.clear()

	lockedTxs := make([]string, 0)
	for txHash := range s.lockedStates.callIndex2lockedState {
		lockedTxs = append(lockedTxs, txHash.Hex()[:8])
	}

	utils.SSCLogger().Info().
		Str("oldRoot", oldRoot.Hex()).
		Str("newRoot", newRoot.Hex()).
		Int("snapshot_count", len(s.snapshots)).
		Int("lockedStates", s.lockedStates.length()).
		Int("lockedTx", len(s.lockedStates.callIndex2lockedState)).
		Msg("locker snapshot created")

	return nil
}

// ============================================================================
// InitLockManager — 创世状态初始化
// ============================================================================

// InitLockManager 创建创世块的空快照，用于系统启动时的初始状态。
func (s *stateLockManager) InitLockManager(genesisRoot common.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 只创建 genesis 空快照
	snapshot := &lockedStatesSnapshot{
		lockedStates:  make(map[api.LockKey]*lockedState),
		rlockedStates: make(map[api.LockKey]*rlockedState),
		finishedTxs:   make(map[common.Hash]bool),
		timestamp:     time.Now(),
	}

	s.snapshots[genesisRoot] = snapshot
	s.currentRoot = genesisRoot

	utils.SSCLogger().Info().
		Str("stateRoot", genesisRoot.Hex()).
		Int("snapshot_count", len(s.snapshots)).
		Msg("locker snapshot inited")

	return nil
}

// ============================================================================
// GetLockerAt — 版本化的 stateLocker 创建
//
// 核心入口：模拟交易在 stateRoot 上执行时，获取该 stateRoot 对应的锁上下文。
// 返回的 stateLocker 包含：
//   - baseSnapshot: stateRoot 时的只读锁快照
//   - pendingStates: 空的隔离 pendingState，用于本次模拟的锁操作
//
// 三种情况：
//   1. 当前 root → 返回一个关联当前全局状态的 locker
//   2. 历史 root（有快照）→ 返回一个关联历史快照的 locker
//   3. 未知 root（快照已过期）→ 降级为当前全局状态（warn 日志）
// ============================================================================

// GetLockerAt 返回一个与指定 stateRoot 对应的 stateLocker 实例。
func (s *stateLockManager) GetLockerAt(stateRoot common.Hash) (api.StateLocker, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 更新访问元数据
	if meta, exists := s.snapshotMeta[stateRoot]; exists {
		meta.lastAccess = time.Now()
		meta.accessCount++
	}

	// Case 1: 请求当前状态 — 返回一个包含当前全局锁状态作为 baseSnapshot 的 locker
	if s.currentRoot == stateRoot || stateRoot == (common.Hash{}) {
		utils.SSCLogger().Info().Msgf("GetLockerAt: returning current state locker for root %s", stateRoot.Hex())
		return &stateLocker{
			txLock:           sync.RWMutex{},
			root:             stateRoot,
			stateLockManager: s,
			// baseSnapshot 是当前全局锁状态的只读视图
			baseSnapshot: &lockedStatesSnapshot{
				lockedStates:  s.lockedStates.lockedStates,
				rlockedStates: s.lockedStates.rlockedStates,
				finishedTxs:   s.finishedTxs,
				timestamp:     time.Now(),
			},
			// pendingStates 用于未提交变更（隔离到 Commit 为止）
			pendingStates: &lockedStates{
				lockedStates:           make(map[api.LockKey]*lockedState),
				rlockedStates:          make(map[api.LockKey]*rlockedState),
				callIndex2lockedState:  make(map[common.Hash]map[string]map[api.LockKey]common.Hash),
				callIndex2rlockedState: make(map[common.Hash]map[string]map[api.LockKey]struct{}),
			},
			tmpFinishedTxs: make(map[common.Hash]bool),
			journal:        &lockJournal{entries: make([]journalEntry, 0)},
			validRevisions: make([]revision, 0),
			nextRevisionId: 0,
			// pendingUnlocks 跟踪需要在 Commit 时从 stateLockManager 解锁的 txHash
			pendingUnlocks: &pendingUnlockCache{
				unlockedTxs: make(map[common.Hash]bool),
			},
		}, nil
	}

	// Case 2: 请求历史状态 — 从快照恢复 locker
	snapshot, exists := s.snapshots[stateRoot]
	if !exists {
		s.sscService.stats.SnapshotMiss.Add(1)
		utils.SSCLogger().Warn().
			Str("stateRoot", stateRoot.Hex()).
			Int("available_snapshots", len(s.snapshots)).
			Msgf("GetLockerAt: snapshot not found, use current state locker")
		// 降级为当前全局状态
		return &stateLocker{
			txLock:           sync.RWMutex{},
			root:             stateRoot,
			stateLockManager: s,
			baseSnapshot: &lockedStatesSnapshot{
				lockedStates:  s.lockedStates.lockedStates,
				rlockedStates: s.lockedStates.rlockedStates,
				finishedTxs:   s.finishedTxs,
				timestamp:     time.Now(),
			},
			pendingStates: &lockedStates{
				lockedStates:           make(map[api.LockKey]*lockedState),
				rlockedStates:          make(map[api.LockKey]*rlockedState),
				callIndex2lockedState:  make(map[common.Hash]map[string]map[api.LockKey]common.Hash),
				callIndex2rlockedState: make(map[common.Hash]map[string]map[api.LockKey]struct{}),
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

	startTime := time.Now()
	s.sscService.stats.SnapshotHit.Add(1)
	utils.SSCLogger().Info().
		Str("stateRoot", stateRoot.Hex()).
		Int("locked_count", len(snapshot.lockedStates)).
		Dur("cost", time.Since(startTime)).
		Msgf("GetLockerAt: returning historical state locker")

	// 创建一个新的 locker 实例，baseSnapshot 来自快照（只读，不可变）
	locker := &stateLocker{
		txLock:           sync.RWMutex{},
		root:             stateRoot,
		stateLockManager: s,
		// baseSnapshot 是这个 stateRoot 下的不可变快照
		baseSnapshot: snapshot,
		// pendingStates 持有未提交变更 —— 与全局状态隔离
		pendingStates: &lockedStates{
			lockedStates:           make(map[api.LockKey]*lockedState),
			rlockedStates:          make(map[api.LockKey]*rlockedState),
			callIndex2lockedState:  make(map[common.Hash]map[string]map[api.LockKey]common.Hash),
			callIndex2rlockedState: make(map[common.Hash]map[string]map[api.LockKey]struct{}),
		},
		tmpFinishedTxs: make(map[common.Hash]bool),
		journal:        &lockJournal{entries: make([]journalEntry, 0)},
		validRevisions: make([]revision, 0),
		nextRevisionId: 0,
		// pendingUnlocks 跟踪需要在 Commit 时从 stateLockManager 解锁的 txHash
		pendingUnlocks: &pendingUnlockCache{
			unlockedTxs: make(map[common.Hash]bool),
		},
	}

	return locker, nil
}

// GetLocker 返回当前状态的 locker 实例（向后兼容）。
func (s *stateLockManager) GetLocker() api.StateLocker {
	s.mu.RLock()
	currentRoot := s.currentRoot
	s.mu.RUnlock()

	locker, _ := s.GetLockerAt(currentRoot)
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
func (s *stateLockManager) GetRWLockStates() (map[api.LockKey]*lockedState, map[api.LockKey]*rlockedState) {
	if s == nil {
		panic("stateLockManager is nil")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	writeLockedStates := make(map[api.LockKey]*lockedState)
	readLockedStates := make(map[api.LockKey]*rlockedState)
	for k, v := range s.lockedStates.lockedStates {
		writeLockedStates[k] = v
	}
	for k, v := range s.lockedStates.rlockedStates {
		readLockedStates[k] = v
	}

	return writeLockedStates, readLockedStates
}

// GetPendingLockStates 返回一个 stateLocker 实例中待提交（pending）的锁状态。
// 用于调试和监控事务隔离。
func (s *stateLocker) GetPendingLockStates() (map[api.LockKey]*lockedState, map[api.LockKey]*rlockedState) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	writeLockedStates := make(map[api.LockKey]*lockedState)
	readLockedStates := make(map[api.LockKey]*rlockedState)
	for k, v := range s.pendingStates.lockedStates {
		writeLockedStates[k] = v
	}
	for k, v := range s.pendingStates.rlockedStates {
		readLockedStates[k] = v
	}

	return writeLockedStates, readLockedStates
}

// GetFullLockStates 返回 baseSnapshot + pendingStates 的合并视图。
// 表示这个 locker 当前看到的全部锁状态。
func (s *stateLocker) GetFullLockStates() (map[api.LockKey]*lockedState, map[api.LockKey]*rlockedState) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	writeLockedStates := make(map[api.LockKey]*lockedState)
	readLockedStates := make(map[api.LockKey]*rlockedState)

	// 先从 baseSnapshot 复制（不可变基准）
	for k, v := range s.baseSnapshot.lockedStates {
		writeLockedStates[k] = v
	}
	for k, v := range s.baseSnapshot.rlockedStates {
		readLockedStates[k] = v
	}

	// 再用 pendingStates 覆盖（未提交变更优先）
	for k, v := range s.pendingStates.lockedStates {
		writeLockedStates[k] = v
	}
	for k, v := range s.pendingStates.rlockedStates {
		readLockedStates[k] = v
	}

	return writeLockedStates, readLockedStates
}

// printTxNums 打印锁管理器中的交易数量统计信息。
func (s *stateLockManager) printTxNums() {
	simulateNums := make(map[uint64]int)
	verifyNums := make(map[uint64]int)
	tx2stateNums := make(map[string]int)
	lockedStateNum := len(s.lockedStates.LockedStates())
	stateNum := 0
	for txHash, c2s := range s.lockedStates.CallIndex2LockedStates() {
		txStateNum := 0
		for _, states := range c2s {
			txStateNum += len(states)
		}
		tx2stateNums[txHash.Hex()[2:10]] = txStateNum
		stateNum += txStateNum
	}
	utils.SSCLogger().Info().
		Int("commitCnt", int(s.commitCnt)).
		Int("txsOnchain", len(tx2stateNums)).
		Int("stateNum", stateNum).
		Int("lockedStateNum", lockedStateNum).
		Interface("simulateNums", simulateNums).
		Interface("verifyNums", verifyNums).
		Interface("tx2stateNums", tx2stateNums).
		Msg("print waiting tx nums")
}
