package ssc

import (
	"bytes"
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

func newStateLockManager(service *sscService) *stateLockManager {
	mgr := &stateLockManager{
		sscService:   service,
		mu:           sync.RWMutex{},
		commitCnt:    0,
		lockedStates: newLockedStates(),
		finishedTxs:  make(map[common.Hash]bool), // TxHash -> commit or rollback
	}

	mgr.tempLockView = NewTempLockView(mgr)
	go mgr.checkLockTime()

	return mgr
}

type lockedState struct {
	lockedBy   common.Hash // the transaction hash that locked this state
	lockTime   time.Time
	hasTimeout bool
}

type rlockedState struct {
	locked     bool
	lockedBy   []common.Hash
	lockTime   time.Time
	hasTimeout bool
}

type stateLockManager struct {
	sscService   *sscService
	tempLockView *TempLockView

	mu           sync.RWMutex
	commitCnt    uint64
	lockedStates *lockedStates
	finishedTxs  map[common.Hash]bool // TxHash -> commit or rollback
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

func (s *stateLockManager) handleLockCommit() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.commitCnt++
	s.lockedStates.clear()

	return nil
}
