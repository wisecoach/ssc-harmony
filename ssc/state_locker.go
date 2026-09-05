package ssc

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/harmony-one/harmony/ssc/perf"
	"github.com/pkg/errors"
)

// ============================================================================
// stateLocker — 锁状态管理器单实例
//
// 每个模拟交易在执行时获得一个 stateLocker 实例，用于管理该交易的锁操作。
// 锁操作只影响本实例的 pendingStates，提交时才会合并到全局 stateLockManager。
//
// 关键设计：
// - Lock/Unlock 只操作 pendingStates，不直接影响全局锁
// - 锁冲突检测同时检查全局 lockedStates（版本过滤） + pendingStates
// - CommitTx/RollbackTx 将事务锁释放记录到 pendingUnlocks
// - Commit(newRoot) 将 pendingStates 合并到 stateLockManager 全局状态
// ============================================================================

// stateLocker is a version-aware locker instance
type stateLocker struct {
	root    common.Hash
	stateDB api.StateDB
	*stateLockManager

	// ── MVCC 版本边界（替代 baseSnapshot）──
	baseVersion uint64 // 创建时的版本号，用于过滤全局 lockedStates

	// ── Per-tx 锁（替代全局 txLock）──
	txLocksMu sync.Mutex                  // 仅保护 txLocks map 的分配
	txLocks   map[common.Hash]*sync.Mutex // txHash → per-tx 锁

	// pendingStates holds uncommitted changes for this locker instance
	// These changes are isolated until Commit() is called
	pendingStates *lockedStates
	// pendingUnlocks tracks txHashes that need to be unlocked from stateLockManager on Commit
	pendingUnlocks *pendingUnlockCache
	tmpFinishedTxs map[common.Hash]bool
	journal        *lockJournal
	validRevisions []revision
	nextRevisionId int
}

// getTxLock 获取或创建一笔 tx 的私有锁。
func (s *stateLocker) getTxLock(txHash common.Hash) *sync.Mutex {
	s.txLocksMu.Lock()
	lk, ok := s.txLocks[txHash]
	if !ok {
		lk = &sync.Mutex{}
		s.txLocks[txHash] = lk
	}
	s.txLocksMu.Unlock()
	return lk
}

// BindStateDB binds a stateDB instance to this locker
func (s *stateLocker) BindStateDB(root common.Hash, stateDB api.StateDB) {
	s.stateDB = stateDB
	s.root = root
}

// ============================================================================
// Lock/Unlock Operations
//
// 锁操作的核心逻辑（改造后无锁化）：
//
// 1. 冲突检测（Lockable/RLockable）：
//    直接从全局 globalLockedStates（sync.Map）读取，用 baseVersion 过滤版本
//    加上 pendingStates（本实例 pending 状态）检查
//
// 2. 上锁（Lock/RLock）：
//    写入 pendingStates（隔离），追加 journal 条目
//    pendingStates 是本 locker 私有的，单线程访问，不需要锁
//
// 3. 释放锁（unlock/unlockRLock）：
//    从 pendingStates 删除，追加 unlock 类型的 journal 条目
// ============================================================================

// globalCheckLock 检查全局 lockedStates 中 key 是否有冲突（带版本过滤）。
func (s *stateLocker) globalCheckLock(key api.LockKey, txHash common.Hash, isRLock bool) error {
	if v, ok := s.globalLockedStates.Load(key); ok {
		st := v.(*lockedState)
		// 跳过已释放的锁
		if bytes.Compare(st.lockedBy.Bytes(), common.Hash{}.Bytes()) == 0 {
			return nil
		}
		// 跳过旧于 baseVersion 的锁（创建本 locker 之前的锁，已被快照）
		if st.version < s.baseVersion {
			return nil
		}
		// 写锁被其他交易持有 → 冲突
		if !st.lockable(txHash) {
			return errors.Wrap(api.ErrLockConflict_OnChain,
				fmt.Sprintf("[stateDB] locked by tx %s", st.lockedBy.Hex()))
		}
	}

	// 检查全局读锁
	if isRLock {
		if v, ok := s.globalRLockedStates.Load(key); ok {
			rls := v.(*rlockedState)
			if rls.locked {
				return errors.Wrap(api.ErrLockConflict_OnChain,
					fmt.Sprintf("[stateDB] rlocked by tx"))
			}
		}
	}

	return nil
}

// RLockable 检查读锁是否可以获取（不产生实际上锁）。
func (s *stateLocker) RLockable(key api.LockKey, txHash common.Hash) error {
	// 检查全局 lockedStates（sync.Map，无锁）
	if err := s.globalCheckLock(key, txHash, true); err != nil {
		s.sscService.stats.LockableFailRlock.Add(1)
		return err
	}

	// 检查 pending states
	if ls, exists := s.pendingStates.lockedStates[key]; exists {
		s.sscService.stats.LockableFailRlock.Add(1)
		return errors.Wrap(api.ErrLockConflict_OffChain,
			fmt.Sprintf("locked by tx %s in pending states", ls.lockedBy.Hex()))
	}

	return nil
}

// Lockable 检查写锁是否可以获取（不产生实际上锁）。
func (s *stateLocker) Lockable(key api.LockKey, txHash common.Hash) error {
	t0 := time.Now()
	defer func() {
		perf.RecordPkg("stateLock", "Lockable", "checkLock", time.Since(t0))
	}()
	// 检查全局 lockedStates（sync.Map，无锁）
	if err := s.globalCheckLock(key, txHash, false); err != nil {
		s.sscService.stats.LockableFailBase.Add(1)
		s.sscService.stats.RecordLockConflict(string(key))
		// 真实链上锁冲突：定位 global 写持有者并记 CMH waitEdge(leader)。
		if v, ok := s.globalLockedStates.Load(key); ok {
			if st := v.(*lockedState); st != nil && st.lockedBy != (common.Hash{}) {
				s.maybeRecordChainBlocked(txHash, st.lockedBy, key)
			}
		}
		return err
	}

	// 检查 pending states
	if ls, exists := s.pendingStates.lockedStates[key]; exists {
		if !ls.lockable(txHash) {
			s.sscService.stats.LockableFailPending.Add(1)
			s.sscService.stats.RecordLockConflict(string(key))
			// pending(同块)真实链上锁冲突：记 CMH waitEdge(leader)。
			s.maybeRecordChainBlocked(txHash, ls.lockedBy, key)
			return errors.Wrap(api.ErrLockConflict_OnChain,
				fmt.Sprintf("[TempLockView] locked by tx %s", ls.lockedBy.Hex()))
		}
	}

	// 检查 pending states 的读锁
	if h, ok := s.FindPendingLockHolder(key); ok {
		s.sscService.stats.LockableFailRlock.Add(1)
		s.maybeRecordChainBlocked(txHash, h, key)
		return errors.Wrap(api.ErrLockConflict_OnChain,
			fmt.Sprintf("[TempLockView] rlocked by tx %s", h.Hex()))
	}

	return nil
}

// maybeRecordChainBlocked 在链上锁判定(Lockable)判出真实链上锁冲突后调用：
// 把"请求方 req 想拿 key 却被 holder(global/pending) 占住"记成 CMH waitEdge。
// 仅分片 leader 有效（内部 onChainBlocked 会门控）；TLV 不作 waitEdge 来源。
func (s *stateLocker) maybeRecordChainBlocked(req, holder common.Hash, key api.LockKey) {
	if s == nil || holder == (common.Hash{}) || holder == req {
		return
	}
	if s.sscService == nil || s.sscService.retryScheduler == nil {
		return
	}
	holderPri, _ := s.GetTxPriority(holder)
	s.sscService.retryScheduler.onChainBlocked(req, holder, key, holderPri)
}

// Lock 获取一个写锁。写入 pendingStates（隔离），追加 journal 条目。
func (s *stateLocker) Lock(txHash common.Hash, callIndex api.CallIndex, key api.LockKey, value common.Hash) error {
	// pendingStates 是本 locker 私有的，单线程访问，不需锁
	ls, exists := s.pendingStates.lockedStates[key]
	if !exists {
		ls = &lockedState{}
	}
	ls.lockedBy = txHash
	ls.lockTime = time.Now()
	ls.version = s.version.Load() // 记录当前版本
	s.pendingStates.addLockedState(txHash, callIndex.ToString(), key, value, ls)
	s.journal.append(&lockEntry{
		txHash:    txHash,
		callIndex: callIndex,
		key:       key,
		value:     value,
	})
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Str("callIndex", callIndex.ToString()).
		Str("key", string(key)).
		Msgf("mu state, pending locked %d state, journal %d", s.pendingStates.length(), s.journal.length())
	return nil
}

// RLock 获取一个读锁。写入 pendingStates（隔离），追加 journal 条目。
func (s *stateLocker) RLock(txHash common.Hash, callIndex api.CallIndex, key api.LockKey) error {
	ls, exists := s.pendingStates.rlockedStates[key]
	if !exists {
		ls = &rlockedState{}
	}
	ls.lockedBy = append(ls.lockedBy, txHash)
	ls.lockTime = time.Now()
	s.pendingStates.addRLockedState(txHash, callIndex.ToString(), key, ls)
	s.journal.append(&rlockEntry{
		txHash:    txHash,
		callIndex: callIndex,
		key:       key,
	})
	return nil
}

// unlock 释放一个写锁。从 pendingStates 中删除，追加 unlockEntry。
func (s *stateLocker) unlock(txHash common.Hash, callIndexStr string, key api.LockKey, rollback bool, newValue common.Hash) error {
	if state, exists := s.pendingStates.lockedStates[key]; exists {
		value := s.pendingStates.deleteLockedState(txHash, callIndexStr, key)
		s.journal.append(&unlockEntry{
			state:        state,
			txHash:       txHash,
			callIndexStr: callIndexStr,
			key:          key,
			oldValue:     value,
			newValue:     newValue,
			rollback:     rollback,
		})
	}
	return nil
}

// unlockRLock 释放一个读锁。从 pendingStates 中删除，追加 unlockRLockEntry。
func (s *stateLocker) unlockRLock(txHash common.Hash, callIndexStr string, key api.LockKey, rollback bool, newValue common.Hash) error {
	if state, exists := s.pendingStates.rlockedStates[key]; exists {
		s.pendingStates.deleteRLockedState(txHash, callIndexStr, key)
		s.journal.append(&unlockRLockEntry{
			state:        state,
			txHash:       txHash,
			callIndexStr: callIndexStr,
			key:          key,
		})
	}
	return nil
}

// ============================================================================
// Snapshot and Revert (transaction-level)
// ============================================================================

// Snapshot 创建一个新的快照点，返回 revision ID。
func (s *stateLocker) Snapshot() int {
	id := s.nextRevisionId
	s.nextRevisionId++
	s.validRevisions = append(s.validRevisions, revision{id, s.journal.length()})
	return id
}

// RevertToSnapshot 回滚到指定的快照点。
func (s *stateLocker) RevertToSnapshot(targetId int) {
	idx := sort.Search(len(s.validRevisions), func(i int) bool {
		return s.validRevisions[i].id >= targetId
	})
	if idx >= len(s.validRevisions) || s.validRevisions[idx].id != targetId {
		panic("invalid snapshot id")
	}
	snapshot := s.validRevisions[idx].journalIndex
	s.journal.revert(s, snapshot)
	s.validRevisions = s.validRevisions[:idx]
}

// ============================================================================
// Transaction Commit/Rollback
// ============================================================================

// CommitTx 提交一个交易，释放其在 pendingStates 中的所有锁。
// 改后：使用 per-tx 锁（txLocks[txHash]），不同 txHash 不争抢。
func (s *stateLocker) CommitTx(txHash common.Hash) error {
	t0 := time.Now()
	defer func() {
		perf.RecordPkg("stateLock", "CommitTx", "commitTx", time.Since(t0))
	}()
	lk := s.getTxLock(txHash)
	tLockWait := time.Now()
	lk.Lock()
	tLockWaitDur := time.Since(tLockWait)
	defer lk.Unlock()

	if tLockWaitDur > 1*time.Millisecond {
		utils.SSCLogger().Warn().Str("txHash", txHash.Hex()).
			Dur("lockWait", tLockWaitDur).
			Msg("CommitTx: slow lock wait (per-tx)")
	}

	// 从 pendingStates 中解锁该交易的所有写锁
	c2ls := s.pendingStates.getCallIndex2LockedStates(txHash)
	if c2ls != nil {
		for callIndex, lockKeyMap := range c2ls {
			for lockKey := range lockKeyMap {
				if err := s.unlock(txHash, callIndex, lockKey, false, common.Hash{}); err != nil {
					return err
				}
			}
		}
	}
	// 从 pendingStates 中解锁该交易的所有读锁
	c2rls := s.pendingStates.getCallIndex2RLockedStates(txHash)
	if c2rls != nil {
		for callIndex, lockKeyMap := range c2rls {
			for lockKey := range lockKeyMap {
				if err := s.unlockRLock(txHash, callIndex, lockKey, false, common.Hash{}); err != nil {
					return err
				}
			}
		}
	}

	s.pendingUnlocks.addTx(txHash)
	s.tmpFinishedTxs[txHash] = true
	s.journal.append(&finishTxEntry{
		txHash:           txHash,
		commitOrRollback: true,
	})
	return nil
}

// RollbackTx 回滚一个交易，恢复 stateDB 中的值并释放锁。
func (s *stateLocker) RollbackTx(txHash common.Hash) error {
	t0 := time.Now()
	defer func() {
		perf.RecordPkg("stateLock", "RollbackTx", "rollbackTx", time.Since(t0))
	}()
	lk := s.getTxLock(txHash)
	tLockWait := time.Now()
	lk.Lock()
	tLockWaitDur := time.Since(tLockWait)
	defer lk.Unlock()

	if tLockWaitDur > 1*time.Millisecond {
		utils.SSCLogger().Warn().Str("txHash", txHash.Hex()).
			Dur("lockWait", tLockWaitDur).
			Msg("RollbackTx: slow lock wait (per-tx)")
	}

	c2ls := s.pendingStates.getCallIndex2LockedStates(txHash)
	if c2ls != nil {
		for callIndex, lockKeyMap := range c2ls {
			for lockKey, oldValue := range lockKeyMap {
				addr, key := lockKey.Value()
				newValue, err := s.stateDB.GetStateWithoutLock(addr, key)
				if err != nil {
					return err
				}
				if err = s.stateDB.SetStateWithoutLock(addr, key, oldValue); err != nil {
					return err
				}
				if err = s.unlock(txHash, callIndex, lockKey, true, newValue); err != nil {
					return err
				}
			}
		}
	}
	c2rls := s.pendingStates.getCallIndex2RLockedStates(txHash)
	if c2rls != nil {
		for callIndex, lockKeyMap := range c2rls {
			for lockKey := range lockKeyMap {
				if err := s.unlockRLock(txHash, callIndex, lockKey, true, common.Hash{}); err != nil {
					return err
				}
			}
		}
	}

	s.pendingUnlocks.addTx(txHash)
	s.tmpFinishedTxs[txHash] = false
	s.journal.append(&finishTxEntry{
		txHash:           txHash,
		commitOrRollback: false,
	})
	return nil
}

// FindPendingLockHolder 返回某 key 在 pendingStates（同块未提交）中的写锁或读锁持有者。
// DSN-52：冲突可能来自同块 pending 写锁，也可能来自 pending 读锁
// （写路径 `Lockable` 会把 pending 读锁也判成冲突），供 Wait-Die 优先级比较
// （dsn52ShouldYield）与链下忽略低优先级锁（lowerPriorityLock）定位持有者。
func (s *stateLocker) FindPendingLockHolder(key api.LockKey) (common.Hash, bool) {
	if s == nil || s.pendingStates == nil {
		return common.Hash{}, false
	}
	// 写锁优先
	if ls, ok := s.pendingStates.lockedStates[key]; ok && ls != nil {
		if ls.lockedBy != (common.Hash{}) {
			return ls.lockedBy, true
		}
	}
	// 读锁：可能多个持有者，返回第一个
	if rls, ok := s.pendingStates.rlockedStates[key]; ok && rls != nil {
		for _, h := range rls.lockedBy {
			if h != (common.Hash{}) {
				return h, true
			}
		}
	}
	return common.Hash{}, false
}

// ReleasePendingLocks 强制释放某持有者在 pendingStates（同块未提交锁）里的全部写锁与读锁。
// DSN-52：释放/回滚路径需同时清理 globalLockedStates 与同块 pending 锁，
// 避免同块冲突（[TempLockView] locked by tx）残留。
// 返回释放的写锁数量（用于监控）。
func (s *stateLocker) ReleasePendingLocks(txHash common.Hash) int {
	if s == nil || s.pendingStates == nil {
		return 0
	}
	if txHash == (common.Hash{}) {
		return 0
	}
	count := 0
	// 写锁：按 txHash 反向索引遍历删除
	if ci, ok := s.pendingStates.callIndex2lockedState[txHash]; ok {
		for callIndexStr, keyMap := range ci {
			for key := range keyMap {
				s.pendingStates.deleteLockedState(txHash, callIndexStr, key)
				count++
			}
		}
	}
	// 读锁
	if ci, ok := s.pendingStates.callIndex2rlockedState[txHash]; ok {
		for callIndexStr, keySet := range ci {
			for key := range keySet {
				s.pendingStates.deleteRLockedState(txHash, callIndexStr, key)
			}
		}
	}
	return count
}

// Commit commits the current locker state and creates a new version.
// 改后：不再取 manager.mu.Lock()，改为 sync.Map 原子写入。
func (s *stateLocker) Commit(newRoot common.Hash) error {
	currentVersion := s.version.Load()

	utils.SSCLogger().Debug().
		Int("pendingLockedStates", len(s.pendingStates.lockedStates)).
		Int("unlockedTxs", len(s.pendingUnlocks.unlockedTxs)).
		Uint64("version", currentVersion).
		Msgf("Commit state, pendingStates: %d, root: %s", s.pendingStates.length(), s.root.Hex())

	// === 将 pendingStates 合并到全局 stateLockManager（无锁，sync.Map） ===
	// 合并写锁到 globalLockedStates
	for key, pendingState := range s.pendingStates.lockedStates {
		st := &lockedState{
			lockedBy: pendingState.lockedBy,
			lockTime: pendingState.lockTime,
			version:  currentVersion,
		}
		s.globalLockedStates.Store(key, st)
		// 同时写入旧版 lockedStates（用于 pendingUnlocks 的 delete 兼容）
		s.lockedStates.lockedStates[key] = st
	}
	// 合并读锁
	for key, pendingState := range s.pendingStates.rlockedStates {
		rls := &rlockedState{
			lockedBy: append([]common.Hash{}, pendingState.lockedBy...),
			lockTime: pendingState.lockTime,
		}
		s.globalRLockedStates.Store(key, rls)
		s.lockedStates.rlockedStates[key] = rls
	}

	// 合并 callIndex 反向映射到旧版 lockedStates（用于 unlock 跟踪）
	for txHash, callIndexMap := range s.pendingStates.callIndex2lockedState {
		for callIndex, lockKeyMap := range callIndexMap {
			for lockKey, value := range lockKeyMap {
				s.lockedStates.setLockedValue(txHash, callIndex, lockKey, value)
			}
		}
	}
	for txHash, callIndexMap := range s.pendingStates.callIndex2rlockedState {
		for callIndex, lockKeyMap := range callIndexMap {
			for lockKey := range lockKeyMap {
				s.lockedStates.addRLockedState(txHash, callIndex, lockKey,
					s.lockedStates.rlockedStates[lockKey])
			}
		}
	}

	// === 将 pendingUnlocks 应用到全局 ===
	s.pendingUnlocks.applyTo(s)

	// 提交 tmpFinishedTxs
	for hash, b := range s.tmpFinishedTxs {
		s.globalFinishedTxs.Store(hash, b)
		s.finishedTxs[hash] = b
	}

	// 清空临时状态
	s.journal = &lockJournal{}
	s.validRevisions = make([]revision, 0)
	s.nextRevisionId = 0
	s.pendingStates = &lockedStates{
		lockedStates:           make(map[api.LockKey]*lockedState),
		rlockedStates:          make(map[api.LockKey]*rlockedState),
		callIndex2lockedState:  make(map[common.Hash]map[string]map[api.LockKey]common.Hash),
		callIndex2rlockedState: make(map[common.Hash]map[string]map[api.LockKey]struct{}),
	}
	s.pendingUnlocks = &pendingUnlockCache{
		unlockedTxs: make(map[common.Hash]bool),
	}

	// === 版本更新 ===
	return s.handleLockCommit(newRoot)
}

// ============================================================================
// Pending Unlock Cache
//
// 改后：applyTo 不再需要 mgr.mu.Lock()，改为操作 sync.Map
// ============================================================================

// pendingUnlockCache 记录需要在 Commit 时从 stateLockManager 解锁的 txHash。
type pendingUnlockCache struct {
	unlockedTxs map[common.Hash]bool // 需要从 stateLockManager 解锁的 txHash
}

func (p *pendingUnlockCache) addTx(txHash common.Hash) {
	p.unlockedTxs[txHash] = true
}

// applyTo 将 pending 解锁应用到全局 stateLockManager。
// 改后：无锁，操作 sync.Map + 旧版 map。
func (p *pendingUnlockCache) applyTo(locker *stateLocker) {
	mgr := locker.stateLockManager

	for txHash := range p.unlockedTxs {
		mgr.sscService.stats.PendingUnlockTotal.Add(1)
		mgr.sscService.stats.PendingUnlockBatch.Add(1)

		// 从 globalLockedStates（sync.Map）删除该 tx 的所有写锁
		c2ls := mgr.lockedStates.getCallIndex2LockedStates(txHash)
		if c2ls == nil {
			// BUG-12 修复: 跨块 rollback/commit 时反向写索引缺失。不能静默跳过，
			// 否则锁永久残留在 globalLockedStates（LOCK_STALE 级联泄漏）。
			// 改为按 lockedBy==txHash 从 globalLockedStates 扫描释放孤儿写锁。
			released := releaseOrphanWriteLocks(mgr, txHash)
			if released > 0 {
				utils.SSCLogger().Warn().Str("txHash", txHash.Hex()).
					Int("released", released).
					Int("globalWriteLocks", countGlobalWriteLocks(mgr, txHash)).
					Msg("[applyTo] reverse write-index nil, released orphan write locks by holder (BUG-12)")
			} else {
				utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
					Int("globalWriteLocks", countGlobalWriteLocks(mgr, txHash)).
					Msg("[applyTo] reverse write-index nil, no orphan write locks by holder")
			}
		} else {
			for callIndex, lockKeyMap := range c2ls {
				for lockKey := range lockKeyMap {
					if v, ok := mgr.globalLockedStates.Load(lockKey); ok {
						ls := v.(*lockedState)
						heldNs := time.Since(ls.lockTime).Nanoseconds()
						mgr.sscService.stats.RecordLockHeld(string(lockKey), heldNs)
					}
					// globalLockedStates 删除已释放的锁（避免 sync.Map 无限增长）
					mgr.globalLockedStates.Delete(lockKey)
					// 旧版 lockedStates 删除（兼容）
					delete(mgr.lockedStates.lockedStates, lockKey)
				}
				delete(c2ls, callIndex)
			}
		}

		// 从 globalRLockedStates（sync.Map）删除该 tx 的所有读锁
		c2rls := mgr.lockedStates.getCallIndex2RLockedStates(txHash)
		if c2rls == nil {
			// BUG-12 修复: 反向读索引缺失 → 按持有者扫描释放孤儿读锁
			releaseOrphanReadLocks(mgr, txHash)
		} else {
			for callIndex, lockKeyMap := range c2rls {
				for lockKey := range lockKeyMap {
					mgr.globalRLockedStates.Delete(lockKey)
					delete(mgr.lockedStates.rlockedStates, lockKey)
				}
				delete(c2rls, callIndex)
			}
		}

		// 清理旧版反向映射
		delete(mgr.lockedStates.callIndex2lockedState, txHash)
		delete(mgr.lockedStates.callIndex2rlockedState, txHash)
	}
}

// releaseOrphanWriteLocks 从 globalLockedStates 按 lockedBy==txHash 扫描并释放孤儿写锁。
// 用于 applyTo 反向写索引缺失（BUG-12）时的兜底清理，避免锁永久残留在全局锁表。
func releaseOrphanWriteLocks(mgr *stateLockManager, txHash common.Hash) int {
	var orphan []api.LockKey
	mgr.globalLockedStates.Range(func(k, v interface{}) bool {
		key := k.(api.LockKey)
		if ls, ok := v.(*lockedState); ok && bytes.Equal(ls.lockedBy.Bytes(), txHash.Bytes()) {
			orphan = append(orphan, key)
		}
		return true
	})
	for _, key := range orphan {
		mgr.globalLockedStates.Delete(key)
		delete(mgr.lockedStates.lockedStates, key)
	}
	return len(orphan)
}

// releaseOrphanReadLocks 从 globalRLockedStates 按持有者包含 txHash 扫描并释放孤儿读锁。
func releaseOrphanReadLocks(mgr *stateLockManager, txHash common.Hash) int {
	var orphan []api.LockKey
	mgr.globalRLockedStates.Range(func(k, v interface{}) bool {
		key := k.(api.LockKey)
		if rls, ok := v.(*rlockedState); ok {
			for _, holder := range rls.lockedBy {
				if bytes.Equal(holder.Bytes(), txHash.Bytes()) {
					orphan = append(orphan, key)
					break
				}
			}
		}
		return true
	})
	for _, key := range orphan {
		mgr.globalRLockedStates.Delete(key)
		delete(mgr.lockedStates.rlockedStates, key)
	}
	return len(orphan)
}

// BUG-12 诊断辅助: 统计 globalLockedStates 中由指定 txHash 持有 (lockedBy==txHash) 的写锁数量。
// 用于 applyTo 反向索引缺失时判断该交易是否仍残留在全局锁中。
func countGlobalWriteLocks(mgr *stateLockManager, txHash common.Hash) int {
	n := 0
	mgr.globalLockedStates.Range(func(_, v interface{}) bool {
		if ls, ok := v.(*lockedState); ok && ls.lockedBy == txHash {
			n++
		}
		return true
	})
	return n
}
