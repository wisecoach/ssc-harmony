package ssc

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
)

// ============================================================================
// Journal System (snapshot/revert for stateLocker)
//
// stateLocker 使用 journal + revision 实现事务级快照/回滚。
// 每个 Lock/Unlock 操作记录一个 journalEntry，通过 revert() 撤销。
// 类似以太坊 StateDB 的 Journal 机制，但专用于锁状态管理。
// ============================================================================

// revision 记录一个快照点，对应 journal 中的某个位置。
type revision struct {
	id           int // 快照 ID，由 nextRevisionId 递增分配
	journalIndex int // journal 中的位置索引，用于回滚到此位置
}

// journalEntry 是 journal 中一个条目的接口。
// 每个条目记录一次锁操作，可以 revert 撤销该操作。
type journalEntry interface {
	// revert 撤销此条目引入的变更。
	// 反向操作：release → lock, lock → release。
	revert(locker *stateLocker)
}

// ============================================================================
// Entry Types
//
// 5 种 entry 类型，对应 5 种锁操作：
//   lockEntry       → Lock() 操作
//   rlockEntry      → RLock() 操作
//   unlockEntry     → unlock() 操作（写锁释放）
//   unlockRLockEntry → unlockRLock() 操作（读锁释放）
//   finishTxEntry   → CommitTx() / RollbackTx() 完成标记
// ============================================================================

// lockEntry 记录一次写锁（Lock）操作。
type lockEntry struct {
	txHash    common.Hash
	callIndex api.CallIndex
	key       api.LockKey
	value     common.Hash
}

// revert 撤销 Lock：从 pendingStates 中删除该写锁记录。
func (l *lockEntry) revert(locker *stateLocker) {
	locker.mu.Lock()
	defer locker.mu.Unlock()

	// 从 pendingStates（隔离的事务状态）中还原
	locker.pendingStates.deleteLockedState(l.txHash, l.callIndex.ToString(), l.key)
}

// rlockEntry 记录一次读锁（RLock）操作。
type rlockEntry struct {
	txHash    common.Hash
	callIndex api.CallIndex
	key       api.LockKey
}

// revert 撤销 RLock：从 pendingStates 中删除该读锁记录。
func (l *rlockEntry) revert(locker *stateLocker) {
	locker.mu.Lock()
	defer locker.mu.Unlock()

	// 从 pendingStates（隔离的事务状态）中还原
	locker.pendingStates.deleteRLockedState(l.txHash, l.callIndex.ToString(), l.key)
}

// unlockEntry 记录一次写锁释放（unlock）操作。
// 在 CommitTx 或 RollbackTx 中调用，将 pendingStates 中的写锁释放。
// revert 时需要将锁状态加回 pendingStates。
//
// rollback 标志区分两种场景：
//   - true:  RollbackTx，需要将 stateDB 中的值恢复为旧值
//   - false: CommitTx，正常释放即可
type unlockEntry struct {
	txHash       common.Hash
	state        *lockedState // 被释放前的锁状态（含 lockedBy, lockTime）
	callIndexStr string
	key          api.LockKey
	newValue     common.Hash // 释放后的新值（RollbackTx 时是 stateDB 的当前值）
	oldValue     common.Hash // 释放前记录的值（用于 revert 恢复）
	rollback     bool        // 是否回滚（true=RollbackTx, false=CommitTx）
}

// revert 撤销 unlock：
// 1. 将 lockedState 加回 pendingStates（恢复锁）
// 2. 如果是回滚场景，将 stateDB 的值也还原
func (l *unlockEntry) revert(locker *stateLocker) {
	func() {
		locker.mu.Lock()
		defer locker.mu.Unlock()
		// 将锁状态加回 pendingStates（还原到 lockedState）
		locker.pendingStates.addLockedState(l.txHash, l.callIndexStr, l.key, l.oldValue, l.state)
	}()

	if l.rollback {
		// 如果是回滚，还需要将 stateDB 中的值还原
		locker.mu.Lock()
		locker.pendingStates.setLockedValue(l.txHash, l.callIndexStr, l.key, l.oldValue)
		locker.mu.Unlock()

		addr, key := l.key.Value()
		locker.stateDB.SetState(l.txHash, addr, key, l.newValue)
	}
}

// unlockRLockEntry 记录一次读锁释放（unlockRLock）操作。
type unlockRLockEntry struct {
	txHash       common.Hash
	state        *rlockedState // 被释放前的读锁状态
	callIndexStr string
	key          api.LockKey
}

// revert 撤销 unlockRLock：将 rlockedState 加回 pendingStates。
func (l *unlockRLockEntry) revert(locker *stateLocker) {
	locker.mu.Lock()
	defer locker.mu.Unlock()
	// 将读锁状态加回 pendingStates（还原到 rlockedState）
	locker.pendingStates.addRLockedState(l.txHash, l.callIndexStr, l.key, l.state)
}

// finishTxEntry 记录一次事务完成操作（CommitTx 或 RollbackTx）。
type finishTxEntry struct {
	txHash           common.Hash
	commitOrRollback bool // true=CommitTx, false=RollbackTx
}

// revert 撤销事务完成：重新将该 txHash 标记为未完成。
func (l *finishTxEntry) revert(locker *stateLocker) {
	delete(locker.tmpFinishedTxs, l.txHash)
}

// ============================================================================
// lockJournal — 有序的 journal 条目列表
// ============================================================================

// lockJournal 按顺序存储所有锁操作条目，支持快照级回滚。
type lockJournal struct {
	entries []journalEntry
}

// length 返回当前 journal 条目数。
func (j *lockJournal) length() int {
	return len(j.entries)
}

// append 追加一个 journal 条目。
func (j *lockJournal) append(entry journalEntry) {
	j.entries = append(j.entries, entry)
}

// revert 回滚到指定的快照位置。
// 从最新条目开始反向遍历，逐一 revert 每个条目，然后截断 journal。
func (j *lockJournal) revert(locker *stateLocker, snapshot int) {
	utils.SSCLogger().Debug().Msgf("revert mu journal to snapshot %d, current %d", snapshot, len(j.entries))
	for i := len(j.entries) - 1; i >= snapshot; i-- {
		j.entries[i].revert(locker)
	}
	j.entries = j.entries[:snapshot]
}
