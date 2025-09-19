package ssc

import (
	"bytes"
	"fmt"
	"github.com/emirpasic/gods/trees/redblacktree"
	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"math/big"
	"sort"
	"sync"
	"time"
)

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
	locker.tmpLockedStates[l.key].locked = false
	locker.tmpLockedStates[l.key].lockedBy = common.Hash{}
	if locker.tmpCallIndex2lockedState[l.txHash] != nil {
		if locker.tmpCallIndex2lockedState[l.txHash][l.callIndex.ToString()] != nil {
			delete(locker.tmpCallIndex2lockedState[l.txHash][l.callIndex.ToString()], l.key)
		}
		if len(locker.tmpCallIndex2lockedState[l.txHash]) == 0 {
			delete(locker.tmpCallIndex2lockedState, l.txHash)
		}
	}
}

type unlockEntry struct {
	txHash       common.Hash
	state        *lockedState
	callIndexStr string
	key          api.LockKey
	newValue     common.Hash
	oldValue     common.Hash
	dirty        bool // if true, unlock the tmp state, or unlock the committed lock
	rollback     bool
}

func (l *unlockEntry) revert(locker *stateLocker) {
	if l.dirty {
		locker.tmpLockedStates[l.key] = l.state
		if locker.tmpCallIndex2lockedState[l.txHash] == nil {
			locker.tmpCallIndex2lockedState[l.txHash] = make(map[string]map[api.LockKey]common.Hash)
		}
		if locker.tmpCallIndex2lockedState[l.txHash][l.callIndexStr] == nil {
			locker.tmpCallIndex2lockedState[l.txHash][l.callIndexStr] = make(map[api.LockKey]common.Hash)
		}
		for txHash, _ := range l.state.waitingTxs {
			locker.waitingTxs[txHash].Num2wait++
		}
		if l.rollback {
			locker.tmpCallIndex2lockedState[l.txHash][l.callIndexStr][l.key] = l.newValue
			addr, key := l.key.Value()
			locker.stateDB.SetState(addr, key, l.newValue)
		}
	} else {
		locker.lockedStates[l.key] = l.state
		if locker.callIndex2lockedState[l.txHash] == nil {
			locker.callIndex2lockedState[l.txHash] = make(map[string]map[api.LockKey]common.Hash)
		}
		if locker.callIndex2lockedState[l.txHash][l.callIndexStr] == nil {
			locker.callIndex2lockedState[l.txHash][l.callIndexStr] = make(map[api.LockKey]common.Hash)
		}
		for txHash, _ := range l.state.waitingTxs {
			locker.waitingTxs[txHash].Num2wait++
		}
		if l.rollback {
			locker.tmpCallIndex2lockedState[l.txHash][l.callIndexStr][l.key] = l.newValue
			addr, key := l.key.Value()
			locker.stateDB.SetState(addr, key, l.newValue)
		}
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
	utils.SSCLogger().Info().Msgf("revert lock journal to snapshot %d, current %d", snapshot, len(j.entries))
	for i := len(j.entries) - 1; i >= snapshot; i-- {
		j.entries[i].revert(locker)
	}
	j.entries = j.entries[:snapshot]
}

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
}

func (t *lockedTx) Key() lockedTxKey {
	return lockedTxKey{
		nonce:         t.Nonce,
		simulationNum: t.NextSimulationNum,
		sender:        t.Sender,
	}
}

type waitingTxNode struct {
	txHash   common.Hash
	tx       *lockedTx
	outEdges map[common.Hash]struct{} // other txs that this tx is waiting for
	inEdges  map[common.Hash]struct{} // other txs that are waiting for this tx

	visited bool // temporary field for cycle detection
}

// it's not thread safe, it will be called by SSCVM for LockExecution or LockWithRWSet, which is synchronized
type waitForGraph struct {
	nodes map[common.Hash]*waitingTxNode
}

func (g *waitForGraph) addNode(tx *lockedTx) {
	if _, exists := g.nodes[tx.TxHash]; !exists {
		g.nodes[tx.TxHash] = &waitingTxNode{
			txHash:   tx.TxHash,
			tx:       tx,
			outEdges: make(map[common.Hash]struct{}),
			inEdges:  make(map[common.Hash]struct{}),
			visited:  false,
		}
	}
}

func (g *waitForGraph) removeNode(txHash common.Hash) {
	delete(g.nodes, txHash)
	for _, node := range g.nodes {
		delete(node.outEdges, txHash)
		delete(node.inEdges, txHash)
	}
}

func newStateLockManager(service *sscService) api.StateLockManager {
	mgr := &stateLockManager{
		sscService:            service,
		lock:                  sync.Mutex{},
		waitingTxs:            make(map[common.Hash]*lockedTx),
		needSignalTxQueue:     redblacktree.NewWith(compareLockedTx),
		readyTxQueue:          redblacktree.NewWith(compareLockedTx),
		waitingTxQueue:        redblacktree.NewWith(compareLockedTx),
		uncommittedTxQueue:    redblacktree.NewWith(compareLockedTx),
		wfg:                   &waitForGraph{nodes: make(map[common.Hash]*waitingTxNode)},
		finishedTxs:           make(map[common.Hash]bool), // TxHash -> commit or rollback
		lockedStates:          make(map[api.LockKey]*lockedState),
		callIndex2lockedState: make(map[common.Hash]map[string]map[api.LockKey]common.Hash),
		lockedAddBalance:      make(map[common.Hash]map[string]map[common.Address]*big.Int),
		lockedSubBalance:      make(map[common.Hash]map[string]map[common.Address]*big.Int),
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

	lock sync.Mutex

	waitingTxs         map[common.Hash]*lockedTx // TxHash -> locked States, used for conflict check
	needSignalTxQueue  *redblacktree.Tree
	readyTxQueue       *redblacktree.Tree
	waitingTxQueue     *redblacktree.Tree
	uncommittedTxQueue *redblacktree.Tree

	wfg                   *waitForGraph
	finishedTxs           map[common.Hash]bool                                   // TxHash -> commit or rollback
	lockedStates          map[api.LockKey]*lockedState                           // address + key -> TxHash, used for lock
	callIndex2lockedState map[common.Hash]map[string]map[api.LockKey]common.Hash // TxHash -> callIndex -> lockKey -> value, used for unlock
	lockedAddBalance      map[common.Hash]map[string]map[common.Address]*big.Int // TxHash -> callIndex -> address -> freezeAddBalance
	lockedSubBalance      map[common.Hash]map[string]map[common.Address]*big.Int // TxHash -> callIndex -> address -> freezeSubBalance
}

func (s *stateLockManager) checkLockTime() {
	for {
		time.Sleep(time.Second * 3)
		s.lock.Lock()
		now := time.Now()
		for lockKey, ls := range s.lockedStates {
			if ls.locked && now.Sub(ls.lockTime) > time.Second*10 && !ls.hasTimeout {
				utils.SSCLogger().Warn().Str("lockKey", string(lockKey)).Str("lockedBy", ls.lockedBy.Hex()).
					Msgf("the state has been locked for more than 10 second, unlock it")
				ls.hasTimeout = true
			}
		}
		s.lock.Unlock()
	}
}

func (s *stateLockManager) GetLocker() api.StateLocker {
	// stack := debug.Stack()
	// utils.SSCLogger().Info().Msgf("get state locker, stack: %s", stack)
	return &stateLocker{
		stateDB:                  nil,
		stateLockManager:         s,
		journal:                  &lockJournal{entries: make([]journalEntry, 0)},
		validRevisions:           make([]revision, 0),
		nextRevisionId:           0,
		tmpLockedStates:          make(map[api.LockKey]*lockedState),
		tmpCallIndex2lockedState: make(map[common.Hash]map[string]map[api.LockKey]common.Hash),
		tmpFinishedTxs:           make(map[common.Hash]bool),
	}
}

func (s *stateLockManager) Subscribe(txHash common.Hash, nonce uint64, sender common.Address, states map[api.LockKey]interface{}, originShardId uint32, nextSimulationNum int, simulateOrVerify bool) error {
	if !s.sscService.IsLeader(txHash) {
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
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
		StartReSimuChan:   make(chan struct{}),
	}
	// TODO 可能需要加一个队列不可过长的限制

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
	s.waitingTxs[txHash] = tx

	s.logNums()

	utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
		Msgf("check lock conflict failed for simulation, try to resimulate later, waiting: %d, uncommittedTxQueue: %d", len(s.waitingTxs), s.uncommittedTxQueue.Size())

	return nil
}

func (s *stateLockManager) UnSubscribe(txHash common.Hash) error {
	if !s.sscService.IsLeader(txHash) {
		return nil
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	tx := s.waitingTxs[txHash]
	if tx == nil {
		return nil
	}

	s.uncommittedTxQueue.Remove(tx.Key())
	s.waitingTxQueue.Remove(tx.Key())
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
		Msg("unsubscribe tx, remove from uncommitted and waiting queue")
	delete(s.waitingTxs, txHash)
	s.logNums()

	return nil
}

func (s *stateLockManager) NotifyReSimulationStart(txHash common.Hash) {
	s.lock.Lock()
	defer s.lock.Unlock()

	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
		Msg("notify resimulation start")

	tx, exists := s.waitingTxs[txHash]
	if !exists {
		return
	}
	tx.StartReSimuChan <- struct{}{}
}

func (s *stateLockManager) handleLockCommit() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	utils.SSCLogger().Info().Msgf("needSignal_num=%d, ready_num=%d, waiting_num=%d, txs=%d", s.needSignalTxQueue.Size(), s.readyTxQueue.Size(), s.waitingTxQueue.Size(), len(s.waitingTxs))
	// handle uncommitted waiting txs, and apply ready tx in uncommitted waiting txs
	iter := s.uncommittedTxQueue.Iterator()
	for iter.Next() {
		key := iter.Key().(lockedTxKey)
		txHash := iter.Value().(common.Hash)
		tx := s.waitingTxs[txHash]
		for lockKey, _ := range tx.States {
			ls := s.lockedStates[lockKey]
			if ls != nil && (bytes.Compare(common.Hash{}.Bytes(), ls.lockedBy.Bytes()) != 0 || len(ls.waitingTxs) > 0) {
				tx.Num2wait++
			}
			if ls == nil {
				s.lockedStates[lockKey] = &lockedState{
					waitingTxs: make(map[common.Hash]struct{}),
				}
			}
			s.lockedStates[lockKey].waitingTxs[tx.TxHash] = struct{}{}
		}
		s.waitingTxQueue.Put(key, tx.TxHash)
		s.logNums()
	}
	s.uncommittedTxQueue.Clear()
	s.logNums()

	// select ready tx queue
	iter = s.waitingTxQueue.Iterator()
	for iter.Next() {
		key := iter.Key().(lockedTxKey)
		txHash := iter.Value().(common.Hash)
		tx := s.waitingTxs[txHash]
		if tx.Num2wait == 0 {
			utils.SSCLogger().Info().Str("txHash", tx.TxHash.Hex()).
				Msg("the States required by the transaction are all unlocked, move to ready queue")
			s.readyTxQueue.Put(key, tx.TxHash)
			s.logNums()
		}
	}

	waitingTxMap := s.waitingTxMap()
	utils.SSCLogger().Info().Interface("waitingTxs", waitingTxMap).
		Msgf("handle lock commit, waiting txs Num2wait, needSignal_num=%d, ready_num=%d, waiting_num=%d, txs=%d", s.needSignalTxQueue.Size(), s.readyTxQueue.Size(), s.waitingTxQueue.Size(), len(s.waitingTxs))

	// handle new ready tx
	s.handleReadyTxQueue()

	return nil
}

func (s *stateLockManager) logNums() {
	// utils.SSCLogger().Info().Msgf("needSignal_num=%d, ready_num=%d, waiting_num=%d, uncommitted_num=%d, txs=%d", s.needSignalTxQueue.Size(), s.readyTxQueue.Size(), s.waitingTxQueue.Size(), s.uncommittedTxQueue.Size(), len(s.waitingTxs))
}

func (s *stateLockManager) waitingNumMap() map[string]int {
	nums := make(map[string]int)
	for _, tx := range s.waitingTxs {
		key := fmt.Sprintf("%d:%s", tx.Nonce, tx.TxHash.Hex()[:16])
		nums[key] = tx.Num2wait
	}
	return nums
}

func (s *stateLockManager) waitingTxMap() map[string]string {
	txMap := make(map[string]string)
	for _, tx := range s.waitingTxs {
		str := ""
		for key, _ := range tx.States {
			if state, exist := s.lockedStates[key]; exist && state.locked {
				str += string(key)[:16] + ":for(" + state.lockedBy.Hex()[:16] + "), "
			}
		}
		txMap[tx.TxHash.Hex()[:16]] = str
	}
	return txMap
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
	utils.SSCLogger().Info().Msgf("handle ready tx queue, num=%d", s.readyTxQueue.Size())
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
			if s.lockedStates[lockKey] == nil {
				s.lockedStates[lockKey] = &lockedState{
					waitingTxs: make(map[common.Hash]struct{}),
				}
			}
			state := s.lockedStates[lockKey]
			state.lockedBy = hash
			// remove waiting txs, and add other tx Num2wait for the state
			delete(state.waitingTxs, hash)
			for otherTxHash, _ := range state.waitingTxs {
				otherTx := s.waitingTxs[otherTxHash]
				otherTx.Num2wait++
			}
		}
		s.logNums()

		if s.sscService.IsLeader(hash) {
			s.needSignalTxQueue.Put(key, tx.TxHash)
			s.waitingTxQueue.Remove(key)
			s.logNums()
			go func() {
				utils.SSCLogger().Info().Str("txHash", hash.Hex()).Msgf("the resimulation's States are unlocked, try to recall simulation, simulatonNum: %d, send resimulation to %d", tx.NextSimulationNum, tx.OriginShardId)
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
					utils.SSCLogger().Info().Str("txHash", hash.Hex()).
						Msgf("start resimulation, remove from waiting queue")
					s.lock.Lock()
					defer s.lock.Unlock()
					s.needSignalTxQueue.Remove(key)
					delete(s.waitingTxs, hash)
					s.logNums()
				case <-time.After(time.Second * 10):
					utils.SSCLogger().Info().Str("txHash", hash.Hex()).
						Msgf("waited for 10s, but no resimulation started, keep in waiting queue")
					s.lock.Lock()
					defer s.lock.Unlock()
					s.waitingTxQueue.Put(key, tx.TxHash)
					s.needSignalTxQueue.Remove(key)
					for lockKey, _ := range tx.States {
						if s.lockedStates[lockKey] == nil {
							s.lockedStates[lockKey] = &lockedState{
								waitingTxs: make(map[common.Hash]struct{}),
							}
						}
						state := s.lockedStates[lockKey]
						state.lockedBy = common.Hash{}
						// remove waiting txs, and add other tx Num2wait for the state
						for otherTxHash, _ := range state.waitingTxs {
							otherTx := s.waitingTxs[otherTxHash]
							otherTx.Num2wait--
						}
						state.waitingTxs[hash] = struct{}{}
					}
					s.logNums()
				}
			}()
		}
	}

	s.readyTxQueue.Clear()
}

type stateLocker struct {
	stateDB api.StateDB
	*stateLockManager

	journal                  *lockJournal
	validRevisions           []revision
	nextRevisionId           int
	tmpLockedStates          map[api.LockKey]*lockedState
	tmpCallIndex2lockedState map[common.Hash]map[string]map[api.LockKey]common.Hash
	tmpFinishedTxs           map[common.Hash]bool
}

func (s *stateLocker) BindStateDB(stateDB api.StateDB) {
	s.stateDB = stateDB
}

func (s *stateLocker) CheckLock(key api.LockKey) error {
	if ls, exists := s.tmpLockedStates[key]; exists && ls.locked {
		return api.ErrLockedByOtherTx
	}
	if ls, exists := s.lockedStates[key]; exists && ls.locked {
		return api.ErrLockedByOtherTx
	}
	return nil
}

func (s *stateLocker) CheckReentrantLock(key api.LockKey, txHash common.Hash) error {
	if ls, exists := s.tmpLockedStates[key]; exists && ls.locked && bytes.Compare(ls.lockedBy.Bytes(), txHash.Bytes()) != 0 {
		return api.ErrLockedByOtherTx
	}
	if ls, exists := s.lockedStates[key]; exists && ls.locked && bytes.Compare(ls.lockedBy.Bytes(), txHash.Bytes()) != 0 {
		return api.ErrLockedByOtherTx
	}
	return nil
}

func (s *stateLocker) Lock(txHash common.Hash, callIndex api.CallIndex, key api.LockKey, value common.Hash) error {
	if s.tmpCallIndex2lockedState[txHash] == nil {
		s.tmpCallIndex2lockedState[txHash] = make(map[string]map[api.LockKey]common.Hash)
	}
	if s.tmpCallIndex2lockedState[txHash][callIndex.ToString()] == nil {
		s.tmpCallIndex2lockedState[txHash][callIndex.ToString()] = make(map[api.LockKey]common.Hash)
	}
	s.tmpCallIndex2lockedState[txHash][callIndex.ToString()][key] = value
	if s.tmpLockedStates[key] == nil {
		s.tmpLockedStates[key] = &lockedState{waitingTxs: make(map[common.Hash]struct{})}
	}
	s.tmpLockedStates[key].locked = true
	s.tmpLockedStates[key].lockedBy = txHash
	s.tmpLockedStates[key].lockTime = time.Now()
	s.journal.append(&lockEntry{
		txHash:    txHash,
		callIndex: callIndex,
		key:       key,
		value:     value,
	})
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
		Str("callIndex", callIndex.ToString()).
		Str("key", string(key)).
		Msgf("lock state, locked [%d/%d] state, journal %d", len(s.lockedStates), len(s.tmpLockedStates), s.journal.length())
	return nil
}

func (s *stateLocker) unlock(txHash common.Hash, callIndexStr string, key api.LockKey) error {
	if state, exists := s.tmpLockedStates[key]; exists {
		if len(s.tmpLockedStates[key].waitingTxs) == 0 {
			delete(s.tmpLockedStates, key)
		} else {
			s.tmpLockedStates[key].locked = false
			s.tmpLockedStates[key].lockedBy = common.Hash{}
		}
		delete(s.tmpCallIndex2lockedState[txHash][callIndexStr], key)
		for hash, _ := range state.waitingTxs {
			s.waitingTxs[hash].Num2wait--
		}
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
			Interface("waitingTxs", s.waitingNumMap()).
			Interface("readyTxQueue", s.readyTxQueue).
			Interface("waitingTxQueue", s.waitingTxQueue).
			Msg("unlock state, waiting txs Num2wait")
		s.journal.append(&unlockEntry{
			state:        state,
			txHash:       txHash,
			callIndexStr: callIndexStr,
			key:          key,
			dirty:        true,
			rollback:     false,
		})
	}
	if state, exists := s.lockedStates[key]; exists {
		if len(s.lockedStates[key].waitingTxs) == 0 {
			delete(s.lockedStates, key)
		} else {
			s.lockedStates[key].locked = false
			s.lockedStates[key].lockedBy = common.Hash{}
		}
		delete(s.callIndex2lockedState[txHash][callIndexStr], key)
		for hash, _ := range state.waitingTxs {
			s.waitingTxs[hash].Num2wait--
		}
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
			Interface("waitingTxs", s.waitingNumMap()).
			Msg("unlock state, waiting txs Num2wait")
		s.journal.append(&unlockEntry{
			state:        state,
			txHash:       txHash,
			callIndexStr: callIndexStr,
			key:          key,
			dirty:        false,
			rollback:     false,
		})
	}
	return nil
}

func (s *stateLocker) unlockForRollback(txHash common.Hash, callIndexStr string, key api.LockKey, newValue common.Hash) error {
	if state, exists := s.tmpLockedStates[key]; exists && state.locked {
		if len(s.tmpLockedStates[key].waitingTxs) == 0 {
			delete(s.tmpLockedStates, key)
		} else {
			s.tmpLockedStates[key].locked = false
			s.tmpLockedStates[key].lockedBy = common.Hash{}
		}
		value := s.tmpCallIndex2lockedState[txHash][callIndexStr][key]
		delete(s.tmpCallIndex2lockedState[txHash][callIndexStr], key)
		for hash, _ := range state.waitingTxs {
			s.waitingTxs[hash].Num2wait--
		}
		s.journal.append(&unlockEntry{
			state:        state,
			txHash:       txHash,
			callIndexStr: callIndexStr,
			key:          key,
			oldValue:     value,
			newValue:     newValue,
			dirty:        true,
			rollback:     true,
		})
	}
	if state, exists := s.lockedStates[key]; exists && state.locked {
		if len(s.lockedStates[key].waitingTxs) == 0 {
			delete(s.lockedStates, key)
		} else {
			s.lockedStates[key].locked = false
			s.lockedStates[key].lockedBy = common.Hash{}
		}
		value := s.callIndex2lockedState[txHash][callIndexStr][key]
		delete(s.callIndex2lockedState[txHash][callIndexStr], key)
		for hash, _ := range state.waitingTxs {
			s.waitingTxs[hash].Num2wait--
		}
		s.journal.append(&unlockEntry{
			state:        state,
			txHash:       txHash,
			callIndexStr: callIndexStr,
			key:          key,
			oldValue:     value,
			newValue:     newValue,
			dirty:        false,
			rollback:     true,
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
	if s.callIndex2lockedState[txHash] != nil {
		for callIndex, lockKeyMap := range s.callIndex2lockedState[txHash] {
			for lockKey, _ := range lockKeyMap {
				err := s.unlock(txHash, callIndex, lockKey)
				if err != nil {
					return err
				}
			}
		}
	}
	if s.tmpCallIndex2lockedState[txHash] != nil {
		for callIndex, lockKeyMap := range s.tmpCallIndex2lockedState[txHash] {
			for lockKey, _ := range lockKeyMap {
				err := s.unlock(txHash, callIndex, lockKey)
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
	if s.callIndex2lockedState[txHash] != nil {
		for callIndex, lockKeyMap := range s.callIndex2lockedState[txHash] {
			for lockKey, oldValue := range lockKeyMap {
				addr, key := lockKey.Value()
				newValue, err := s.stateDB.GetState(addr, key)
				if err != nil {
					return err
				}
				err = s.stateDB.SetState(addr, key, oldValue)
				if err != nil {
					return err
				}
				err = s.unlockForRollback(txHash, callIndex, lockKey, newValue)
				if err != nil {
					return err
				}
			}
		}
	}
	if s.tmpCallIndex2lockedState[txHash] != nil {
		for callIndex, lockKeyMap := range s.tmpCallIndex2lockedState[txHash] {
			for lockKey, oldValue := range lockKeyMap {
				addr, key := lockKey.Value()
				newValue, err := s.stateDB.GetState(addr, key)
				if err != nil {
					return err
				}
				err = s.stateDB.SetState(addr, key, oldValue)
				if err != nil {
					return err
				}
				err = s.unlockForRollback(txHash, callIndex, lockKey, newValue)
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
	utils.SSCLogger().Info().Msgf("Commit state lock, tmpFinishedTxs: %d, tmpLockedStates: %d, tmpCallIndex2lockedState: %d",
		len(s.tmpFinishedTxs), len(s.tmpLockedStates), len(s.tmpCallIndex2lockedState))
	// commit all temporary locked States to the committed States
	for hash, b := range s.tmpFinishedTxs {
		s.finishedTxs[hash] = b
	}
	for lockKey, ls := range s.tmpLockedStates {
		if s.lockedStates[lockKey] != nil {
			oldLs := s.lockedStates[lockKey]
			ls.waitingTxs = oldLs.waitingTxs
			for hash, _ := range ls.waitingTxs {
				s.waitingTxs[hash].Num2wait++
			}
		}
		s.lockedStates[lockKey] = ls
	}
	for txHash, callIndexMap := range s.tmpCallIndex2lockedState {
		if s.callIndex2lockedState[txHash] == nil {
			s.callIndex2lockedState[txHash] = make(map[string]map[api.LockKey]common.Hash)
		}
		for callIndex, lockKeyMap := range callIndexMap {
			if s.callIndex2lockedState[txHash][callIndex] == nil {
				s.callIndex2lockedState[txHash][callIndex] = make(map[api.LockKey]common.Hash)
			}
			for lockKey, value := range lockKeyMap {
				s.callIndex2lockedState[txHash][callIndex][lockKey] = value
			}
		}
	}
	// clear temporary States
	s.tmpLockedStates = make(map[api.LockKey]*lockedState)
	s.tmpCallIndex2lockedState = make(map[common.Hash]map[string]map[api.LockKey]common.Hash)
	s.journal = &lockJournal{}
	s.validRevisions = make([]revision, 0)
	s.nextRevisionId = 0

	// close transactions of sscService
	s.sscService.closeTransactions(s.tmpFinishedTxs)

	return s.handleLockCommit()
}
