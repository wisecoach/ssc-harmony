package ssc

import (
	"bytes"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/emirpasic/gods/trees/redblacktree"
	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/pkg/errors"
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

type lockedTx struct {
	TxHash            common.Hash
	Nonce             uint64
	Sender            common.Address
	States            map[api.LockKey]interface{}
	Num2wait          int
	NextSimulationNum int
	OriginShardId     uint32
	SimulateOrVerify  bool
	StartReSimuChan   chan struct{} `json:"-"`
	CommitNum         uint64
}

func (t *lockedTx) Key() lockedTxKey {
	return lockedTxKey{
		nonce:         t.Nonce,
		simulationNum: t.NextSimulationNum,
		sender:        t.Sender,
	}
}

type revision struct {
	id           int
	journalIndex int
}

type journalEntry interface {
	// revert undoes the changes introduced by this journal entry.
	revert(locker *stateLocker)
}

type lockEntry struct {
	txHash    common.Hash
	callIndex api.CallIndex
	key       api.LockKey
	value     common.Hash
}

func (l *lockEntry) revert(locker *stateLocker) {
	locker.lock.Lock()
	defer locker.lock.Unlock()

	locker.wfg.UnlockState(l.txHash, l.key)
	locker.lockedStates.deleteLockedState(l.txHash, l.callIndex.ToString(), l.key)
}

type unlockEntry struct {
	txHash       common.Hash
	state        *lockedState
	callIndexStr string
	key          api.LockKey
	lockType     api.LockType
	newValue     common.Hash
	oldValue     common.Hash
	rollback     bool
}

func (l *unlockEntry) revert(locker *stateLocker) {
	func() {
		locker.lock.Lock()
		defer locker.lock.Unlock()
		locker.wfg.GrantLock(l.txHash, l.key, l.lockType)
		locker.lockedStates.addLockedState(l.txHash, l.callIndexStr, l.key, l.oldValue, l.state)
		for txHash, _ := range l.state.waitingTxs {
			locker.waitingTxs[txHash].Num2wait++
		}
	}()

	if l.rollback {
		locker.lock.Lock()
		locker.lockedStates.setLockedValue(l.txHash, l.callIndexStr, l.key, l.oldValue)
		locker.lock.Unlock()

		addr, key := l.key.Value()
		locker.stateDB.SetState(addr, key, l.newValue)
	}
}

type finishTxEntry struct {
	txHash           common.Hash
	commitOrRollback bool
}

func (l *finishTxEntry) revert(locker *stateLocker) {
	delete(locker.tmpFinishedTxs, l.txHash)
}

type lockJournal struct {
	entries []journalEntry
}

func (j lockJournal) length() int {
	return len(j.entries)
}

func (j *lockJournal) append(entry journalEntry) {
	j.entries = append(j.entries, entry)
}

func (j *lockJournal) revert(locker *stateLocker, snapshot int) {
	utils.SSCLogger().Debug().Msgf("revert lock journal to snapshot %d, current %d", snapshot, len(j.entries))
	for i := len(j.entries) - 1; i >= snapshot; i-- {
		j.entries[i].revert(locker)
	}
	j.entries = j.entries[:snapshot]
}

func newLockedStates() *lockedStates {
	return &lockedStates{
		lockedStates:          make(map[api.LockKey]*lockedState),
		callIndex2lockedState: make(map[common.Hash]map[string]map[api.LockKey]common.Hash),
		lockedAddBalance:      make(map[common.Hash]map[string]map[common.Address]*big.Int),
		lockedSubBalance:      make(map[common.Hash]map[string]map[common.Address]*big.Int),
	}
}

// not thread safe
type lockedStates struct {
	lockedStates          map[api.LockKey]*lockedState                           // address + key -> TxHash, used for lock
	callIndex2lockedState map[common.Hash]map[string]map[api.LockKey]common.Hash // TxHash -> callIndex -> lockKey -> value, used for unlock
	lockedAddBalance      map[common.Hash]map[string]map[common.Address]*big.Int // TxHash -> callIndex -> address -> freezeAddBalance
	lockedSubBalance      map[common.Hash]map[string]map[common.Address]*big.Int // TxHash -> callIndex -> address -> freezeSubBalance
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

func (s *lockedStates) get(key api.LockKey) (ls *lockedState, exists bool) {
	ls, exists = s.lockedStates[key]
	return
}

func (s *lockedStates) clear() {
	for key, state := range s.lockedStates {
		if len(state.waitingTxs) == 0 && bytes.Compare(state.lockedBy.Bytes(), common.Hash{}.Bytes()) == 0 {
			delete(s.lockedStates, key)
		}
	}
}

func (s *lockedStates) init(key api.LockKey) *lockedState {
	s.lockedStates[key] = &lockedState{
		locked:     false,
		lockedBy:   common.Hash{},
		waitingTxs: make(map[common.Hash]struct{}),
	}
	return s.lockedStates[key]
}

func (s *lockedStates) getCallIndex2LockedStates(txHash common.Hash) map[string]map[api.LockKey]common.Hash {
	return s.callIndex2lockedState[txHash]
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

func (s *lockedStates) deleteLockedState(txHash common.Hash, callIndexStr string, key api.LockKey) common.Hash {
	if s.lockedStates[key] == nil {
		return common.Hash{}
	}
	if len(s.lockedStates[key].waitingTxs) == 0 {
		delete(s.lockedStates, key)
	} else {
		s.lockedStates[key].locked = false
		s.lockedStates[key].lockedBy = common.Hash{}
	}
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

type stateLocker struct {
	txLock  sync.RWMutex
	stateDB api.StateDB
	*stateLockManager

	tmpFinishedTxs map[common.Hash]bool
	journal        *lockJournal
	validRevisions []revision
	nextRevisionId int
}

func (s *stateLocker) BindStateDB(stateDB api.StateDB) {
	s.stateDB = stateDB
}

func (s *stateLocker) Locked(key api.LockKey) error {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if ls, exists := s.lockedStates.get(key); exists && ls.locked {
		return errors.Wrap(api.ErrLockedByOtherTx, fmt.Sprintf("locked by tx %s", ls.lockedBy.Hex()))
	}
	return nil
}

func (s *stateLocker) CheckLockable(key api.LockKey, txHash common.Hash, lockType api.LockType) error {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.wfg.CheckLock(txHash, key, api.ExclusiveLock)
}

func (s *stateLocker) Lock(txHash common.Hash, callIndex api.CallIndex, key api.LockKey, value common.Hash, lockType api.LockType) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.wfg.GrantLock(txHash, key, lockType)
	ls, exists := s.lockedStates.get(key)
	if !exists {
		ls = &lockedState{waitingTxs: make(map[common.Hash]struct{})}
	}
	ls.locked = true
	ls.lockedBy = txHash
	ls.lockTime = time.Now()
	s.lockedStates.addLockedState(txHash, callIndex.ToString(), key, value, ls)
	s.journal.append(&lockEntry{
		txHash:    txHash,
		callIndex: callIndex,
		key:       key,
		value:     value,
	})
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Str("callIndex", callIndex.ToString()).
		Str("key", string(key)).
		Msgf("lock state, locked %d state, journal %d", s.lockedStates.length(), s.journal.length())
	return nil
}

func (s *stateLocker) unlock(txHash common.Hash, callIndexStr string, key api.LockKey, rollback bool, newValue common.Hash) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if state, exists := s.lockedStates.get(key); exists {
		s.wfg.UnlockState(txHash, key)
		value := s.lockedStates.deleteLockedState(txHash, callIndexStr, key)
		for hash, _ := range state.waitingTxs {
			if tx, exists := s.waitingTxs[hash]; exists {
				tx.Num2wait--
			}
		}
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Msg("unlock state, waiting txs Num2wait")
		s.journal.append(&unlockEntry{
			state:        state,
			txHash:       txHash,
			callIndexStr: callIndexStr,
			key:          key,
			oldValue:     value,
			lockType:     s.wfg.heldLocks[txHash][key],
			newValue:     newValue,
			rollback:     rollback,
		})
	}
	return nil
}

func (s *stateLocker) Snapshot() int {
	id := s.nextRevisionId
	s.nextRevisionId++
	s.validRevisions = append(s.validRevisions, revision{id, s.journal.length()})
	return id
}

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

func (s *stateLocker) CommitTx(txHash common.Hash) error {
	s.txLock.Lock()
	defer s.txLock.Unlock()

	c2ls := s.lockedStates.getCallIndex2LockedStates(txHash)
	if c2ls != nil {
		for callIndex, lockKeyMap := range c2ls {
			for lockKey, _ := range lockKeyMap {
				err := s.unlock(txHash, callIndex, lockKey, false, common.Hash{})
				if err != nil {
					return err
				}
			}
		}
	}
	s.tmpFinishedTxs[txHash] = true
	s.journal.append(&finishTxEntry{
		txHash:           txHash,
		commitOrRollback: true,
	})
	return nil
}

func (s *stateLocker) RollbackTx(txHash common.Hash) error {
	s.txLock.Lock()
	defer s.txLock.Unlock()

	c2ls := s.lockedStates.getCallIndex2LockedStates(txHash)
	if c2ls != nil {
		for callIndex, lockKeyMap := range c2ls {
			for lockKey, oldValue := range lockKeyMap {
				addr, key := lockKey.Value()
				newValue, err := s.stateDB.GetStateWithoutLock(addr, key)
				if err != nil {
					return err
				}
				err = s.stateDB.SetStateWithoutLock(addr, key, oldValue)
				if err != nil {
					return err
				}
				err = s.unlock(txHash, callIndex, lockKey, true, newValue)
				if err != nil {
					return err
				}
			}
		}
	}

	s.tmpFinishedTxs[txHash] = false
	s.journal.append(&finishTxEntry{
		txHash:           txHash,
		commitOrRollback: false,
	})
	return nil
}

func (s *stateLocker) Commit() error {
	utils.SSCLogger().Debug().Msgf("Commit state lock, tmpFinishedTxs: %d",
		len(s.tmpFinishedTxs))
	s.txLock.Lock()
	// commit all temporary locked States to the committed States
	for hash, b := range s.tmpFinishedTxs {
		s.finishedTxs[hash] = b
	}
	// clear temporary States
	s.journal = &lockJournal{}
	s.validRevisions = make([]revision, 0)
	s.nextRevisionId = 0
	s.txLock.Unlock()

	return s.handleLockCommit()
}

type LockRequest struct {
	txHash   common.Hash
	tx       *lockedTx
	lockKey  api.LockKey
	lockType api.LockType
}

func newWaitForGraph() *waitForGraph {
	return &waitForGraph{
		edges:      make(map[common.Hash]map[common.Hash]struct{}),
		heldLocks:  make(map[common.Hash]map[api.LockKey]api.LockType),
		lockQueues: make(map[api.LockKey][]*LockRequest),
	}
}

// it's not thread safe, it will be called by SSCVM for LockExecution or LockWithRWSet, which is synchronized
type waitForGraph struct {
	edges      map[common.Hash]map[common.Hash]struct{}     // A -> B means A is waiting for B
	heldLocks  map[common.Hash]map[api.LockKey]api.LockType // TxHash -> lockKey
	lockQueues map[api.LockKey][]*LockRequest               // lockKey -> waiting txs
}

func (g *waitForGraph) CheckLock(txHash common.Hash, lockKey api.LockKey, lockType api.LockType) error {
	// check if has same or stronger cmLock
	if heldLock, exists := g.heldLocks[txHash][lockKey]; exists {
		if heldLock == api.ExclusiveLock || (heldLock == api.SharedLock && lockType == api.SharedLock) {
			return nil // 已经持有兼容的锁
		}
	}

	// check if state has conflicting locks
	conflictingTxs := make([]common.Hash, 0)
	for holderID, locks := range g.heldLocks {
		if bytes.Compare(holderID[:], txHash[:]) == 0 {
			continue
		}

		if heldLock, exists := locks[lockKey]; exists {
			// check lock compatibility
			if lockType == api.ExclusiveLock || heldLock == api.ExclusiveLock {
				conflictingTxs = append(conflictingTxs, holderID)
				utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msgf("state locked by other tx: %s, state:%s", holderID.Hex(), lockKey)
			}
		}
	}

	if len(conflictingTxs) == 0 {
		return nil
	}

	if g.detectCycle(txHash) {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
			Err(api.ErrDeadLockDetected).
			Str("wfg", g.PrintGraph()).
			Msg("deadlock detected")
		return api.ErrDeadLockDetected
	} else {
		_, held := g.heldLocks[txHash]
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
			Err(api.ErrLockedByOtherTx).
			Bool("hasHeldOtherState", held).
			Str("wfg", g.PrintGraph()).
			Str("key", string(lockKey)).
			Msgf("state locked by other tx")
		return api.ErrLockedByOtherTx
	}
}

// RequestLock 事务请求锁
func (g *waitForGraph) RequestLock(txHash common.Hash, lockKey api.LockKey, lockType api.LockType) error {
	// check if has same or stronger cmLock
	if heldLock, exists := g.heldLocks[txHash][lockKey]; exists {
		if heldLock == api.ExclusiveLock || (heldLock == api.SharedLock && lockType == api.SharedLock) {
			return nil // 已经持有兼容的锁
		}
	}

	// check if state has conflicting locks
	conflictingTxs := make([]common.Hash, 0)
	for holderID, locks := range g.heldLocks {
		if bytes.Compare(holderID[:], txHash[:]) == 0 {
			continue
		}

		if heldLock, exists := locks[lockKey]; exists {
			// check cmLock compatibility
			if lockType == api.ExclusiveLock || heldLock == api.ExclusiveLock {
				conflictingTxs = append(conflictingTxs, holderID)
			}
		}
	}

	// 如果没有冲突，直接授予锁
	if len(conflictingTxs) == 0 {
		g.GrantLock(txHash, lockKey, lockType)
		return nil
	}

	if g.detectCycle(txHash) {
		return api.ErrDeadLockDetected
	} else {
		return api.ErrLockedByOtherTx
	}
}

func (g *waitForGraph) removeLockRequest(lockKey api.LockKey, txHash common.Hash) {
	queue := g.lockQueues[lockKey]
	for i, req := range queue {
		if bytes.Compare(req.txHash[:], txHash[:]) == 0 {
			g.lockQueues[lockKey] = append(queue[:i], queue[i+1:]...)
			break
		}
	}
}

func (g *waitForGraph) UnlockState(txHash common.Hash, lockKey api.LockKey) {
	if locks, exists := g.heldLocks[txHash]; exists {
		delete(locks, lockKey)
		if len(locks) == 0 {
			delete(g.heldLocks, txHash)
		}
	}
}

func (g *waitForGraph) UnlockTx(txHash common.Hash) {
	delete(g.heldLocks, txHash)
	delete(g.edges, txHash)
}

// detectCycle 检测从给定事务开始的循环（死锁）
func (g *waitForGraph) detectCycle(startTx common.Hash) bool {
	visited := make(map[common.Hash]bool)
	recStack := make(map[common.Hash]bool)

	return g.detectCycleDFS(startTx, visited, recStack)
}

// detectCycleDFS 使用深度优先搜索检测循环
func (g *waitForGraph) detectCycleDFS(txHash common.Hash, visited, recStack map[common.Hash]bool) bool {
	if !visited[txHash] {
		visited[txHash] = true
		recStack[txHash] = true

		// 遍历所有邻居（这个事务等待的所有事务）
		for neighbor := range g.edges[txHash] {
			if !visited[neighbor] && g.detectCycleDFS(neighbor, visited, recStack) {
				return true
			} else if recStack[neighbor] {
				return true
			}
		}
	}

	recStack[txHash] = false
	return false
}

func (g *waitForGraph) GrantLock(txHash common.Hash, lockKey api.LockKey, lockType api.LockType) {
	if g.heldLocks[txHash] == nil {
		g.heldLocks[txHash] = make(map[api.LockKey]api.LockType)
	}
	g.heldLocks[txHash][lockKey] = lockType
}

func (g *waitForGraph) addEdge(waiter common.Hash, holder common.Hash) {
	if g.edges[waiter] == nil {
		g.edges[waiter] = make(map[common.Hash]struct{})
	}
	g.edges[waiter][holder] = struct{}{}
}

func (g *waitForGraph) removeEdge(waiter common.Hash, holder common.Hash) {
	if g.edges[waiter] != nil {
		delete(g.edges[waiter], holder)
		if len(g.edges[waiter]) == 0 {
			delete(g.edges, waiter)
		}
	}
}

// PrintGraph 打印当前等待图的状态（用于调试）
func (g *waitForGraph) PrintGraph() string {
	str := ""
	str += fmt.Sprintln("=== Wait-for Graph ===")
	for waiter, holders := range g.edges {
		for holder := range holders {
			str += fmt.Sprintf("Tx%s -> Tx%s\n", waiter.Hex()[:16], holder.Hex()[:16])
		}
	}
	str += fmt.Sprintln("=== Held Locks ===")
	for txID, locks := range g.heldLocks {
		for resource, lockType := range locks {
			lockTypeStr := "S"
			if lockType == api.ExclusiveLock {
				lockTypeStr = "X"
			}
			str += fmt.Sprintf("Tx%s holds %s lock on %s\n", txID.Hex()[:16], lockTypeStr, resource)
		}
	}
	str += fmt.Sprintln("==================")
	return str
}

func newStateLockManager(service *sscService) api.StateLockManager {
	mgr := &stateLockManager{
		sscService:            service,
		lock:                  sync.RWMutex{},
		commitCnt:             0,
		waitingTxs:            make(map[common.Hash]*lockedTx),
		commitNum2simulateTxs: make(map[uint64]map[common.Hash]*lockedTx),
		commitNum2verifyTxs:   make(map[uint64]map[common.Hash]*lockedTx),
		needSignalTxQueue:     redblacktree.NewWith(compareLockedTx),
		readyTxQueue:          redblacktree.NewWith(compareLockedTx),
		waitingTxQueue:        redblacktree.NewWith(compareLockedTx),
		uncommittedTxQueue:    redblacktree.NewWith(compareLockedTx),
		unsubscribedTxs:       make(map[common.Hash]struct{}),
		wfg:                   newWaitForGraph(),
		lockedStates:          newLockedStates(),
		finishedTxs:           make(map[common.Hash]bool), // TxHash -> commit or rollback
	}

	go mgr.checkLockTime()

	return mgr
}

type lockedState struct {
	locked     bool                     // if locked by statedb, if lockedBy is not empty but locked is false means that it has been subscribed by lockedBy
	lockedBy   common.Hash              // the transaction hash that locked this state
	waitingTxs map[common.Hash]struct{} // the txs waiting for the state
	lockTime   time.Time
	hasTimeout bool
}

type stateLockManager struct {
	sscService *sscService

	lock sync.RWMutex

	commitCnt             uint64
	waitingTxs            map[common.Hash]*lockedTx // TxHash -> locked States, used for conflict check
	commitNum2simulateTxs map[uint64]map[common.Hash]*lockedTx
	commitNum2verifyTxs   map[uint64]map[common.Hash]*lockedTx

	needSignalTxQueue  *redblacktree.Tree
	readyTxQueue       *redblacktree.Tree
	waitingTxQueue     *redblacktree.Tree
	uncommittedTxQueue *redblacktree.Tree
	unsubscribedTxs    map[common.Hash]struct{}
	wfg                *waitForGraph
	lockedStates       *lockedStates
	finishedTxs        map[common.Hash]bool // TxHash -> commit or rollback
}

func (s *stateLockManager) checkLockTime() {
	for {
		time.Sleep(time.Second * 10)
		s.lock.Lock()
		now := time.Now()
		for lockKey, ls := range s.lockedStates.LockedStates() {
			if ls.locked && now.Sub(ls.lockTime) > time.Second*10 && !ls.hasTimeout {
				utils.SSCLogger().Warn().Str("lockKey", string(lockKey)).Str("lockedBy", ls.lockedBy.Hex()).
					Msgf("the state has been locked for more than 10 second, unlock it")
				ls.hasTimeout = true
			}
		}
		utils.SSCLogger().Info().Msgf("check lock time, locked states: %d", s.lockedStates.length())
		s.lock.Unlock()
	}
}

func (s *stateLockManager) GetLocker() api.StateLocker {
	return &stateLocker{
		txLock:           sync.RWMutex{},
		stateDB:          nil,
		stateLockManager: s,
		tmpFinishedTxs:   make(map[common.Hash]bool),
		journal:          &lockJournal{entries: make([]journalEntry, 0)},
		validRevisions:   make([]revision, 0),
		nextRevisionId:   0,
	}
}

func (s *stateLockManager) Subscribe(txHash common.Hash, nonce uint64, sender common.Address, states map[api.LockKey]interface{}, originShardId uint32, nextSimulationNum int, simulateOrVerify bool) error {
	if !s.sscService.IsLeader(txHash) {
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Msg("not leader, skip subscribe")
		return nil
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	tx := &lockedTx{
		TxHash:            txHash,
		Nonce:             nonce,
		Sender:            sender,
		States:            states,
		Num2wait:          0,
		NextSimulationNum: nextSimulationNum,
		OriginShardId:     originShardId,
		SimulateOrVerify:  simulateOrVerify,
		CommitNum:         s.commitCnt,
		StartReSimuChan:   make(chan struct{}),
	}

	if oldTxHash, found := s.uncommittedTxQueue.Get(tx.Key()); found {
		utils.SSCLogger().Error().
			Interface("key_old_tx", s.waitingTxs[oldTxHash.(common.Hash)]).
			Interface("key_new_tx", tx).
			Msgf("conflict tx")
	}
	if s.waitingTxs[txHash] != nil {
		utils.SSCLogger().Error().
			Interface("old_tx", s.waitingTxs[txHash]).
			Interface("new_tx", tx).
			Msgf("duplicate tx")
		return nil
	}
	s.uncommittedTxQueue.Put(tx.Key(), tx.TxHash)
	s.addTx(txHash, tx)

	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
		Msgf("check lock conflict failed for simulation for %d, try to resimulate later, waiting: %d, uncommittedTxQueue: %d, readyTxQueue: %d, waitingTxQueue: %d", nextSimulationNum, len(s.waitingTxs), s.uncommittedTxQueue.Size(), s.waitingTxQueue.Size(), s.readyTxQueue.Size())

	return nil
}

func (s *stateLockManager) UnSubscribe(txHash common.Hash) {
	if !s.sscService.IsLeader(txHash) {
		return
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	tx := s.waitingTxs[txHash]
	if tx == nil {
		return
	}

	s.unsubscribedTxs[txHash] = struct{}{}

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Msg("unsubscribe tx, remove from uncommitted and waiting queue")
	return
}

func (s *stateLockManager) NotifyReSimulationStart(txHash common.Hash) {
	s.lock.Lock()
	defer s.lock.Unlock()

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Msg("notify resimulation start")

	tx, exists := s.waitingTxs[txHash]
	if !exists {
		return
	}
	tx.StartReSimuChan <- struct{}{}
}

func (s *stateLockManager) printTxNums() {
	simulateNums := make(map[uint64]int)
	verifyNums := make(map[uint64]int)
	for num, txs := range s.commitNum2simulateTxs {
		simulateNums[num] = len(txs)
	}
	for num, txs := range s.commitNum2verifyTxs {
		verifyNums[num] = len(txs)
	}
	tx2stateNums := make(map[string]int)
	lockedStateNum := 0
	for _, state := range s.lockedStates.LockedStates() {
		if state.locked {
			lockedStateNum++
		}
	}
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

func (s *stateLockManager) addTx(txHash common.Hash, tx *lockedTx) {
	s.waitingTxs[txHash] = tx
	if tx.SimulateOrVerify {
		if s.commitNum2simulateTxs[tx.CommitNum] == nil {
			s.commitNum2simulateTxs[tx.CommitNum] = make(map[common.Hash]*lockedTx)
		}
		s.commitNum2simulateTxs[tx.CommitNum][txHash] = tx
	} else {
		if s.commitNum2verifyTxs[tx.CommitNum] == nil {
			s.commitNum2verifyTxs[tx.CommitNum] = make(map[common.Hash]*lockedTx)
		}
		s.commitNum2verifyTxs[tx.CommitNum][txHash] = tx
	}
}

func (s *stateLockManager) removeTx(txHash common.Hash, tx *lockedTx) {
	delete(s.waitingTxs, txHash)
	if tx.SimulateOrVerify {
		delete(s.commitNum2simulateTxs[tx.CommitNum], txHash)
		if len(s.commitNum2simulateTxs[tx.CommitNum]) == 0 {
			delete(s.commitNum2simulateTxs, tx.CommitNum)
		}
	} else {
		delete(s.commitNum2verifyTxs[tx.CommitNum], txHash)
		if len(s.commitNum2verifyTxs[tx.CommitNum]) == 0 {
			delete(s.commitNum2verifyTxs, tx.CommitNum)
		}
	}
}

func (s *stateLockManager) handleLockCommit() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.commitCnt++
	// s.printTxNums()

	utils.SSCLogger().Info().Msgf("unsubscribe_num=%d, needSignal_num=%d, ready_num=%d, uncommitted_num=%d, waiting_num=%d, txs=%d, locked_states=%d",
		len(s.unsubscribedTxs), s.needSignalTxQueue.Size(), s.readyTxQueue.Size(), s.uncommittedTxQueue.Size(), s.waitingTxQueue.Size(), len(s.waitingTxs), s.lockedStates.length())

	// remove tx
	for txHash := range s.unsubscribedTxs {
		if tx, exists := s.waitingTxs[txHash]; exists {
			key := tx.Key()
			for lockKey, _ := range tx.States {
				ls, exist := s.lockedStates.get(lockKey)
				if exist {
					delete(ls.waitingTxs, txHash)
					if ls.lockedBy == txHash {
						ls.locked = false
						ls.lockedBy = common.Hash{}
					}
				}
			}
			s.uncommittedTxQueue.Remove(key)
			s.waitingTxQueue.Remove(key)
			s.readyTxQueue.Remove(key)
			s.needSignalTxQueue.Remove(key)
			s.removeTx(txHash, tx)
		}
	}
	s.unsubscribedTxs = make(map[common.Hash]struct{})

	// handle uncommitted waiting txs, and apply ready tx in uncommitted waiting txs
	iter := s.uncommittedTxQueue.Iterator()
	for iter.Next() {
		key := iter.Key().(lockedTxKey)
		txHash := iter.Value().(common.Hash)
		tx := s.waitingTxs[txHash]
		for lockKey, _ := range tx.States {
			ls, exists := s.lockedStates.get(lockKey)
			if exists {
				ls.waitingTxs[tx.TxHash] = struct{}{}
				if bytes.Compare(common.Hash{}.Bytes(), ls.lockedBy.Bytes()) != 0 || len(ls.waitingTxs) > 0 {
					tx.Num2wait++
				}
			}
		}
		s.waitingTxQueue.Put(key, tx.TxHash)
	}
	s.uncommittedTxQueue.Clear()

	// select ready tx queue
	iter = s.waitingTxQueue.Iterator()
	for iter.Next() {
		key := iter.Key().(lockedTxKey)
		txHash := iter.Value().(common.Hash)
		tx := s.waitingTxs[txHash]
		if tx == nil {
			utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
				Msg("tx not exist")
			continue
		}
		if tx.Num2wait == 0 {
			utils.SSCLogger().Debug().Str("txHash", tx.TxHash.Hex()).
				Msg("the States required by the transaction are all unlocked, move to ready queue")
			s.readyTxQueue.Put(key, tx.TxHash)
		}
	}

	// waitingTxMap := s.waitingTxMap()
	// utils.SSCLogger().Debug().Interface("waitingTxs", waitingTxMap).
	// 	Msgf("handle lock commit, waiting txs Num2wait, needSignal_num=%d, ready_num=%d, waiting_num=%d, txs=%d", s.needSignalTxQueue.Size(), s.readyTxQueue.Size(), s.waitingTxQueue.Size(), len(s.waitingTxs))

	// handle new ready tx
	s.handleReadyTxQueue()

	s.lockedStates.clear()

	return nil
}

func (s *stateLockManager) waitingNumMap() map[string]int {
	nums := make(map[string]int)
	for _, tx := range s.waitingTxs {
		key := fmt.Sprintf("%d:%s", tx.Nonce, tx.TxHash.Hex()[:16])
		nums[key] = tx.Num2wait
	}
	return nums
}

func (s *stateLockManager) txMap(tree *redblacktree.Tree) map[common.Hash]int {
	nums := make(map[common.Hash]int)
	iter := tree.Iterator()
	for iter.Next() {
		txHash := iter.Value().(common.Hash)
		tx := s.waitingTxs[txHash]
		nums[txHash] = tx.Num2wait
	}
	return nums
}

func (s *stateLockManager) handleReadyTxQueue() {
	utils.SSCLogger().Debug().Msgf("handle ready tx queue, num=%d", s.readyTxQueue.Size())
	iter := s.readyTxQueue.Iterator()
	for iter.Next() {
		key := iter.Key().(lockedTxKey)
		txHash := iter.Value().(common.Hash)
		tx := s.waitingTxs[txHash]
		hash := tx.TxHash

		if tx.Num2wait > 0 {
			continue
		}

		for lockKey, _ := range tx.States {
			// don't set locked, just set lockBy means just subscribe
			ls, exists := s.lockedStates.get(lockKey)
			if !exists {
				ls = s.lockedStates.init(lockKey)
			}
			ls.lockedBy = hash
			// remove waiting txs, and add other tx Num2wait for the state
			delete(ls.waitingTxs, hash)
			for otherTxHash, _ := range ls.waitingTxs {
				otherTx := s.waitingTxs[otherTxHash]
				if otherTx != nil {
					otherTx.Num2wait++
				}
			}
		}

		if s.sscService.IsLeader(hash) {
			s.sscService.stateLock.RLock()
			state, _ := s.sscService.getState(hash)
			s.sscService.stateLock.RUnlock()
			if state == nil {
				utils.SSCLogger().Debug().Str("txHash", hash.Hex()).Msg("tx has been closed, unsubscribe it")
				s.uncommittedTxQueue.Remove(tx.Key())
				s.waitingTxQueue.Remove(tx.Key())
				s.removeTx(hash, tx)
				continue
			}
			s.needSignalTxQueue.Put(key, tx.TxHash)
			s.waitingTxQueue.Remove(key)
			go func() {
				utils.SSCLogger().Debug().Str("txHash", hash.Hex()).Msgf("the resimulation's States are unlocked, try to recall simulation, simulatonNum: %d, send resimulation to %d", tx.NextSimulationNum, tx.OriginShardId)
				err := s.sscService.sendReSimulationSignal(&api.ReSimulationSignal{
					TxHash:           hash.Bytes(),
					ShardId:          s.sscService.SelfShard,
					OriginShardId:    tx.OriginShardId,
					SimulationNum:    tx.NextSimulationNum,
					Ready:            true,
					NeedResimulate:   true,
					SimulateOrVerify: tx.SimulateOrVerify,
				})

				if err != nil {
					s.lock.Lock()
					defer s.lock.Unlock()
					s.waitingTxQueue.Put(key, tx.TxHash)
					s.needSignalTxQueue.Remove(key)
					utils.SSCLogger().Error().Err(err).Str("txHash", hash.Hex()).
						Msg("failed to send resimulation signal")
					return
				}

				select {
				case <-tx.StartReSimuChan:
					utils.SSCLogger().Debug().Str("txHash", hash.Hex()).
						Msgf("start resimulation, remove from waiting queue")
					s.lock.Lock()
					defer s.lock.Unlock()
					s.needSignalTxQueue.Remove(key)
					s.removeTx(hash, tx)
				case <-time.After(time.Second * 10):
					utils.SSCLogger().Debug().Str("txHash", hash.Hex()).
						Msgf("waited for 10s, but no resimulation started, keep in waiting queue")
					s.lock.Lock()
					defer s.lock.Unlock()
					s.waitingTxQueue.Put(key, tx.TxHash)
					s.needSignalTxQueue.Remove(key)
					for lockKey, _ := range tx.States {
						ls, exists := s.lockedStates.get(lockKey)
						if !exists {
							ls = s.lockedStates.init(lockKey)
						}
						ls.lockedBy = common.Hash{}
						// remove waiting txs, and add other tx Num2wait for the state
						for otherTxHash, _ := range ls.waitingTxs {
							otherTx := s.waitingTxs[otherTxHash]
							if otherTx != nil {
								otherTx.Num2wait--
							}
						}
						ls.waitingTxs[hash] = struct{}{}
					}
				}
			}()
		}
	}

	s.readyTxQueue.Clear()
}
