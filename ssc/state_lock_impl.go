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

type lockedTxKey struct {
	nonce         uint64
	simulationNum int
	sender        common.Address
}

func compareLockedTx(a any, b any) int {
	tx1 := a.(lockedTxKey)
	tx2 := b.(lockedTxKey)
	if tx1.nonce != tx2.nonce {
		return int(tx1.nonce) - int(tx2.nonce)
	}
	return bytes.Compare(tx1.sender[:], tx2.sender[:])
}

func newLockedStates() *lockedStates {
	return &lockedStates{
		lockedStates:          make(map[api.LockKey]*lockedState),
		callIndex2lockedState: make(map[common.Hash]map[string]map[api.LockKey]common.Hash),
	}
}

// not thread safe
type lockedStates struct {
	rlockedStates          map[api.LockKey]*rlockedState
	lockedStates           map[api.LockKey]*lockedState                           // address + key -> TxHash, used for mu
	callIndex2lockedState  map[common.Hash]map[string]map[api.LockKey]common.Hash // TxHash -> callIndex -> lockKey -> value, used for unlock
	callIndex2rlockedState map[common.Hash]map[string]map[api.LockKey]struct{}    // TxHash -> callIndex -> lockKey -> value, used for unlock
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

func (s *lockedStates) length() int {
	return len(s.lockedStates)
}

func (s *lockedStates) clear() {
	for key, state := range s.lockedStates {
		if bytes.Compare(state.lockedBy.Bytes(), common.Hash{}.Bytes()) == 0 {
			delete(s.lockedStates, key)
		}
	}
}

func (s *lockedStates) init(key api.LockKey) *lockedState {
	s.lockedStates[key] = &lockedState{
		lockedBy: common.Hash{},
	}
	return s.lockedStates[key]
}

func (s *lockedStates) getCallIndex2LockedStates(txHash common.Hash) map[string]map[api.LockKey]common.Hash {
	return s.callIndex2lockedState[txHash]
}

func (s *lockedStates) getCallIndex2RLockedStates(txHash common.Hash) map[string]map[api.LockKey]struct{} {
	return s.callIndex2rlockedState[txHash]
}

func (s *lockedStates) getLockedValue(txHash common.Hash, callIndexStr string, key api.LockKey) common.Hash {
	if s.callIndex2lockedState[txHash] == nil {
		return common.Hash{}
	}
	if s.callIndex2lockedState[txHash][callIndexStr] == nil {
		return common.Hash{}
	}
	return s.callIndex2lockedState[txHash][callIndexStr][key]
}

func (s *lockedStates) setLockedValue(txHash common.Hash, callIndexStr string, key api.LockKey, value common.Hash) {
	if s.callIndex2lockedState[txHash] == nil {
		s.callIndex2lockedState[txHash] = make(map[string]map[api.LockKey]common.Hash)
	}
	if s.callIndex2lockedState[txHash][callIndexStr] == nil {
		s.callIndex2lockedState[txHash][callIndexStr] = make(map[api.LockKey]common.Hash)
	}
	s.callIndex2lockedState[txHash][callIndexStr][key] = value
}

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
// ============================================================================

// stateLockManager extends stateLockManager with snapshot capabilities
type stateLockManager struct {
	sscService   *sscService
	tempLockView *TempLockView

	mu sync.RWMutex

	// Original fields (unchanged)
	commitCnt    uint64
	lockedStates *lockedStates
	finishedTxs  map[common.Hash]bool

	// === NEW: Versioning support ===
	currentRoot  common.Hash                           // Current block's stateRoot
	snapshots    map[common.Hash]*lockedStatesSnapshot // stateRoot → snapshot
	snapshotMeta map[common.Hash]*snapshotMetadata     // stateRoot → access metadata
	maxSnapshots int                                   // LRU limit
	snapshotTTL  time.Duration                         // Time-to-live for snapshots

	// Cleanup
	cleanupStop chan struct{}
	cleanupOnce sync.Once
}

// newStateLockManager creates a new versioned lock manager
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
	go mgr.checkLockTime()
	go mgr.startCleanupRoutine()

	return mgr
}

// startCleanupRoutine periodically removes expired and excess snapshots
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

// StopCleanup stops the cleanup goroutine (call during shutdown)
func (s *stateLockManager) StopCleanup() {
	s.cleanupOnce.Do(func() {
		close(s.cleanupStop)
	})
}

// cleanupSnapshots removes expired snapshots and enforces LRU limit
func (s *stateLockManager) cleanupSnapshots() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	expired := make([]common.Hash, 0)

	// Phase 1: Remove expired snapshots (TTL-based)
	for root, snapshot := range s.snapshots {
		if now.Sub(snapshot.timestamp) > s.snapshotTTL {
			expired = append(expired, root)
		}
	}

	for _, root := range expired {
		s.deleteSnapshot(root)
	}

	// Phase 2: Enforce LRU limit if still over capacity
	if len(s.snapshots) > s.maxSnapshots {
		// Find least recently accessed snapshots
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

		// Sort by last access time (oldest first)
		sort.Slice(records, func(i, j int) bool {
			return records[i].lastAccess.Before(records[j].lastAccess)
		})

		// Remove oldest snapshots until under limit
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

// deleteSnapshot removes a snapshot and its metadata
func (s *stateLockManager) deleteSnapshot(root common.Hash) {
	delete(s.snapshots, root)
	delete(s.snapshotMeta, root)
}

// SetSnapshotConfig allows configuring snapshot limits
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

// GetSnapshotCount returns current number of snapshots (for monitoring)
func (s *stateLockManager) GetSnapshotCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.snapshots)
}

type lockedState struct {
	lockedBy   common.Hash // the transaction hash that locked this state
	lockTime   time.Time
	hasTimeout bool
}

func (ls *lockedState) lockable(txHash common.Hash) bool {
	if bytes.Compare(ls.lockedBy.Bytes(), common.Hash{}.Bytes()) == 0 {
		return true
	}
	if bytes.Compare(ls.lockedBy.Bytes(), txHash.Bytes()) == 0 {
		return true
	}
	return false
}

type rlockedState struct {
	locked     bool
	lockedBy   []common.Hash
	lockTime   time.Time
	hasTimeout bool
}

// handleLockCommit commits the locker state and creates a snapshot for the given stateRoot
func (s *stateLockManager) handleLockCommit(newRoot common.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	utils.SSCLogger().Info().Msgf("handleLockCommit: creating snapshot for stateRoot %s", newRoot.Hex())

	// === Create snapshot of current state ===
	snapshot := &lockedStatesSnapshot{
		lockedStates:  make(map[api.LockKey]*lockedState, len(s.lockedStates.lockedStates)),
		rlockedStates: make(map[api.LockKey]*rlockedState, len(s.lockedStates.rlockedStates)),
		finishedTxs:   make(map[common.Hash]bool, len(s.finishedTxs)),
		timestamp:     time.Now(),
	}

	// Deep copy lockedStates
	for k, v := range s.lockedStates.lockedStates {
		snapshot.lockedStates[k] = &lockedState{
			lockedBy:   v.lockedBy,
			lockTime:   v.lockTime,
			hasTimeout: v.hasTimeout,
		}
	}

	// Deep copy rlockedStates
	for k, v := range s.lockedStates.rlockedStates {
		snapshot.rlockedStates[k] = &rlockedState{
			lockedBy:   append([]common.Hash{}, v.lockedBy...),
			lockTime:   v.lockTime,
			hasTimeout: v.hasTimeout,
		}
	}

	// Deep copy finishedTxs
	for k, v := range s.finishedTxs {
		snapshot.finishedTxs[k] = v
	}

	// Store snapshot
	s.snapshots[newRoot] = snapshot
	s.snapshotMeta[newRoot] = &snapshotMetadata{
		lastAccess:  time.Now(),
		accessCount: 0,
	}
	oldRoot := s.currentRoot
	s.currentRoot = newRoot

	// === Original commit logic (unchanged) ===
	s.commitCnt++
	s.lockedStates.clear()

	lockedTxs := make([]string, 0)
	for txHash, _ := range s.lockedStates.callIndex2lockedState {
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
// Versioned Locker Retrieval (NEW)
// ============================================================================

// GetLockerAt returns a locker instance for the given stateRoot.
// This allows transaction simulation to use the correct locker state
// that matches the stateDB version.
func (s *stateLockManager) GetLockerAt(stateRoot common.Hash) (api.StateLocker, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Update access metadata
	if meta, exists := s.snapshotMeta[stateRoot]; exists {
		meta.lastAccess = time.Now()
		meta.accessCount++
	}

	// Case 1: Requesting current state - return a fresh locker
	if s.currentRoot == stateRoot || stateRoot == (common.Hash{}) {
		utils.SSCLogger().Info().Msgf("GetLockerAt: returning current state locker for root %s", stateRoot.Hex())
		return &stateLocker{
			txLock:           sync.RWMutex{},
			root:             stateRoot,
			stateLockManager: s,
			// baseSnapshot is a view of current global state (read-only)
			baseSnapshot: &lockedStatesSnapshot{
				lockedStates:  s.lockedStates.lockedStates,
				rlockedStates: s.lockedStates.rlockedStates,
				finishedTxs:   s.finishedTxs,
				timestamp:     time.Now(),
			},
			// pendingStates for uncommitted changes (isolated until Commit)
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
			// pendingUnlocks tracks txHashes to unlock from stateLockManager on Commit
			pendingUnlocks: &pendingUnlockCache{
				unlockedTxs: make(map[common.Hash]bool),
			},
		}, nil
	}

	// Case 2: Requesting historical state - return locker with snapshot
	snapshot, exists := s.snapshots[stateRoot]
	if !exists {
		utils.SSCLogger().Warn().
			Str("stateRoot", stateRoot.Hex()).
			Int("available_snapshots", len(s.snapshots)).
			Msgf("GetLockerAt: snapshot not found, use current state locker")
		return &stateLocker{
			txLock:           sync.RWMutex{},
			root:             stateRoot,
			stateLockManager: s,
			// baseSnapshot is a view of current global state (fallback when snapshot missing)
			baseSnapshot: &lockedStatesSnapshot{
				lockedStates:  s.lockedStates.lockedStates,
				rlockedStates: s.lockedStates.rlockedStates,
				finishedTxs:   s.finishedTxs,
				timestamp:     time.Now(),
			},
			// pendingStates for uncommitted changes (isolated until Commit)
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
			// pendingUnlocks tracks txHashes to unlock from stateLockManager on Commit
			pendingUnlocks: &pendingUnlockCache{
				unlockedTxs: make(map[common.Hash]bool),
			},
		}, nil
	}

	startTime := time.Now()
	utils.SSCLogger().Info().
		Str("stateRoot", stateRoot.Hex()).
		Int("locked_count", len(snapshot.lockedStates)).
		Dur("cost", time.Since(startTime)).
		Msgf("GetLockerAt: returning historical state locker")

	// Create a new locker instance with baseSnapshot from snapshot (read-only, immutable)
	locker := &stateLocker{
		txLock:           sync.RWMutex{},
		root:             stateRoot,
		stateLockManager: s,
		// baseSnapshot is the immutable snapshot at this stateRoot
		baseSnapshot: snapshot,
		// pendingStates holds uncommitted changes - isolated from global state
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
		// pendingUnlocks tracks txHashes to unlock from stateLockManager on Commit
		pendingUnlocks: &pendingUnlockCache{
			unlockedTxs: make(map[common.Hash]bool),
		},
	}

	return locker, nil
}

// GetLocker returns a new locker instance for the current state (backward compatible)
func (s *stateLockManager) GetLocker() api.StateLocker {
	s.mu.RLock()
	currentRoot := s.currentRoot
	s.mu.RUnlock()

	locker, _ := s.GetLockerAt(currentRoot)
	return locker
}

func (s *stateLockManager) checkLockTime() {
	for {
		time.Sleep(time.Second * 10)
		s.mu.Lock()
		now := time.Now()
		for lockKey, ls := range s.lockedStates.LockedStates() {
			if now.Sub(ls.lockTime) > time.Second*10 && !ls.hasTimeout {
				utils.SSCLogger().Warn().Str("lockKey", string(lockKey)).Str("lockedBy", ls.lockedBy.Hex()).
					Msgf("the state has been locked for more than 10 second, unlock it")
				ls.hasTimeout = true
			}
		}
		utils.SSCLogger().Info().Msgf("check mu time, locked states: %d", s.lockedStates.length())
		s.mu.Unlock()
	}
}

func (s *stateLockManager) GetTempLockView() *TempLockView {
	return s.tempLockView
}

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

// GetPendingLockStates returns pending (uncommitted) lock states for a locker
// This is useful for debugging and monitoring transaction isolation
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

// GetFullLockStates returns the combined view of baseSnapshot + pendingStates
// This represents what this locker sees as the current locked state
func (s *stateLocker) GetFullLockStates() (map[api.LockKey]*lockedState, map[api.LockKey]*rlockedState) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	writeLockedStates := make(map[api.LockKey]*lockedState)
	readLockedStates := make(map[api.LockKey]*rlockedState)

	// First, copy from baseSnapshot (immutable)
	for k, v := range s.baseSnapshot.lockedStates {
		writeLockedStates[k] = v
	}
	for k, v := range s.baseSnapshot.rlockedStates {
		readLockedStates[k] = v
	}

	// Then, overlay pendingStates (uncommitted changes take precedence)
	for k, v := range s.pendingStates.lockedStates {
		writeLockedStates[k] = v
	}
	for k, v := range s.pendingStates.rlockedStates {
		readLockedStates[k] = v
	}

	return writeLockedStates, readLockedStates
}

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
