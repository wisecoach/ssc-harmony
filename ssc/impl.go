package ssc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/event"
	"github.com/harmony-one/harmony/block"
	"github.com/harmony-one/harmony/core"
	corestate "github.com/harmony-one/harmony/core/state"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/core/vm"
	"github.com/harmony-one/harmony/hmy"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/harmony-one/harmony/ssc/lm"
)

type sscService struct {
	*CommitteeMechanism
	Comm         *Comm
	BLSSignerMgr api.BLSSignerMgr
	txSigner     api.TxSigner
	txSubmitter  api.TxSubmitter
	lockStateMgr api.StateLockManager
	timerMgr     *CXTTimerManager

	Config *api.Config
	bc     core.BlockChain

	stateLock               lm.RWMutex // cmLock for simulationState, executionVerifyContexts, finishedTxs
	simulationState         map[common.Hash]*api.CXTSimulationState
	executionVerifyContexts map[common.Hash]*api.ExecutionVerifyContext // TxHash -> ExecutionVerifyContext
	finishedTxs             map[common.Hash]bool

	commitLock   lm.RWMutex // cmLock for commitStates
	commitStates map[common.Hash]*api.CommitState

	syncLock            lm.Mutex // cmLock for callStatesInWaiting
	callStatesInWaiting map[common.Hash][]*api.SimulationCallState

	simuLock       lm.Mutex // cmLock for simuWaitingChs, simuResultCh
	simuWaitingChs map[common.Hash][]chan *api.CXTSimulationSSCResult
	simuResultCh   map[common.Hash]chan *api.CXTSimulationSSCResult // used to notify simulation finished

	// preSimuLock    lm.Mutex                                         // cmLock for cachedResultCh
	// cachedResultCh map[common.Hash]chan *api.CXTSimulationSSCResult // cached for consensus leader

	ctx          context.Context
	chainHeadCh  chan core.ChainHeadEvent
	chainHeadSub event.Subscription
}

func (s *sscService) GetShardID(address common.Address) uint32 {
	return s.CommitteeMechanism.GetShardID(address)
}

func NewService(ctx context.Context, config *api.Config, cm *CommitteeMechanism, sscConfig *api.ShardSimulateCommitteeConfig,
	signerMgr api.BLSSignerMgr, nodeAPI hmy.NodeAPI, bc core.BlockChain, txSigner api.TxSigner) api.Service {
	state, _ := bc.State()
	txSub := &txSubmitter{
		lock:      lm.NewMutex(),
		selfShard: cm.SelfShard,
		txSigner:  txSigner,
		nonce:     state.GetNonce(txSigner.Address()),
		nodeAPI:   nodeAPI,
		config:    config,
	}
	service := &sscService{
		CommitteeMechanism: cm,
		Comm:               NewComm(),
		BLSSignerMgr:       signerMgr,
		txSigner:           txSigner,
		Config:             config,
		txSubmitter:        txSub,
		bc:                 bc,
		stateLock:          lm.NewRWMutex(),
		commitLock:         lm.NewRWMutex(),
		simuLock:           lm.NewMutex(),
		// preSimuLock:             lm.NewMutex(),
		syncLock:                lm.NewMutex(),
		simulationState:         make(map[common.Hash]*api.CXTSimulationState),
		commitStates:            make(map[common.Hash]*api.CommitState),
		executionVerifyContexts: make(map[common.Hash]*api.ExecutionVerifyContext),
		callStatesInWaiting:     make(map[common.Hash][]*api.SimulationCallState),
		finishedTxs:             make(map[common.Hash]bool),
		simuWaitingChs:          make(map[common.Hash][]chan *api.CXTSimulationSSCResult),
		simuResultCh:            make(map[common.Hash]chan *api.CXTSimulationSSCResult),
		// cachedResultCh:          make(map[common.Hash]chan *api.CXTSimulationSSCResult),
		ctx:          ctx,
		chainHeadCh:  make(chan core.ChainHeadEvent, 10),
		chainHeadSub: nil,
	}
	service.lockStateMgr = newStateLockManager(service)
	service.timerMgr = NewTimerManager(sscConfig.Timeout, service)
	subscription := bc.SubscribeChainHeadEvent(service.chainHeadCh)
	service.chainHeadSub = subscription
	utils.SSCLogger().Info().Msg("ssc service start...")
	go service.loop()
	return service
}

func (s *sscService) BlockCommitted(block *types.Block) error {
	utils.SSCLogger().Info().Uint64("blockNum", block.NumberU64()).Msg("block committed")
	s.timerMgr.BlockCommitted(block.NumberU64())
	err := s.CommitteeMechanism.HandleBlockCommitted(block)
	if err != nil {
		return err
	}
	return nil
}

func (s *sscService) loop() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.chainHeadSub.Err():
			return
		case head := <-s.chainHeadCh:
			b := head.Block
			var callStatesInWaiting []*api.SimulationCallState
			func() {
				s.syncLock.Lock()
				defer s.syncLock.Unlock()
				callStatesInWaiting = s.callStatesInWaiting[b.Header().Hash()]
				delete(s.callStatesInWaiting, b.Header().Hash())
			}()
			for _, callState := range callStatesInWaiting {
				callState.SyncedCh <- struct{}{}
			}
		}
	}
}

func (s *sscService) startSimulation(req *api.CXTSimulationRequest) (*api.SimulationCallState, error) {
	txHash := common.BytesToHash(req.TxHash)
	_, err := s.startCXT(txHash, req.SimulationNum, s.SelfShard, make([]uint32, 0), req, nil)
	if err != nil {
		return nil, err
	}

	callState := &api.SimulationCallState{
		BlockHash:         common.BytesToHash(req.BlockHash),
		CallIndex:         api.CallIndex{},
		TopRequest:        req,
		DependentCXTCalls: make(map[string]*api.DependentCXTCall),
		RWSet:             newRWSet(),
		Result:            nil,
		CallSSCResult:     nil,
		Executed:          false,
		Lock:              sync.Mutex{},
	}

	header := s.bc.GetHeaderByHash(callState.BlockHash)
	if header == nil {
		s.waitForSync(callState)
		header = s.bc.GetHeaderByHash(callState.BlockHash)
		if header == nil {
			utils.SSCLogger().Error().Msg("failed to get block header")
			return nil, errors.New("failed to get block header")
		}
	}
	db, err := s.bc.StateAt(header.Root())
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to get state")
		return nil, err
	}
	callState.DB = db

	func() {
		s.stateLock.Lock()
		defer s.stateLock.Unlock()
		state, _ := s.getState(txHash)
		state.CurrentCallFrame = &api.CallFrame{
			CallIndex: api.CallIndex{},
			PC:        0,
		}
		state.SimulationCallStates[req.SimulationNum] = state.SimulationCallStates[req.SimulationNum].Add(callState)
	}()

	func() {
		s.commitLock.Lock()
		defer s.commitLock.Unlock()
		commitState := s.commitStates[txHash]
		if commitState == nil {
			s.commitStates[txHash] = &api.CommitState{
				CommitVotes:     make(map[int]map[uint32][]*api.CXTCommitVote),
				CommitSSCVotes:  make(map[int]map[uint32]*api.CXTCommitSSCVote),
				RollbackVotes:   make(map[uint32][]*api.CXTCommitVote),
				RollbackSSCVote: nil,
			}
		}
	}()

	return callState, nil
}

func (s *sscService) startCXT(txHash common.Hash, simulationNum int, originShardId uint32, relatedShards api.RelatedShards,
	topRequest *api.CXTSimulationRequest, callRequest *api.CXTCallSSCRequest) (bool, error) {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	state, _ := s.getState(txHash)

	// don't start if the simulationNum is not larger than current one
	if state != nil && state.SimulationNum >= simulationNum {
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Msgf("the context has been started, current simulationNum %d, new simulationNum %d", state.SimulationNum, simulationNum)
		return false, nil
	}

	// set cxt timeout ctx
	ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)

	newState := &api.CXTSimulationState{
		CurrentCallFrame: &api.CallFrame{
			CallIndex: api.CallIndex{},
			PC:        0,
		},
		CallStack:            api.NewCallStack(),
		SimulationRequest:    topRequest,
		SimulationResult:     nil,
		SimulationCallStates: make(map[int]api.SimulationCallStates),
		SimulationNum:        simulationNum,
		LockedCallIndex:      api.MINCallIndex,
		OriginShardId:        originShardId,
		RelatedShards:        relatedShards.Add(s.SelfShard),
		ReSimulationSignals:  make(map[int]map[uint32]*api.ReSimulationSignal),
		Ctx:                  ctx,
		CtxCancel:            cancel,
	}

	// start a new resimulation state
	if simulationNum > 0 {
		if state == nil {
			state = newState
		}
		// extend from originRequest
		if topRequest != nil {
			utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
				Interface("originRequest", state.SimulationRequest).
				Msgf("extend from origin request, simulationNum %d", topRequest.SimulationNum)
			originReq := state.SimulationRequest
			topRequest.Tx = originReq.Tx
			topRequest.From = originReq.From
			topRequest.Author = originReq.Author
			topRequest.GasPool = originReq.GasPool
		}
		newState.SimulationRequest = topRequest
		newState.SimulationCallStates = state.SimulationCallStates
	}

	if topRequest != nil {
		newState.Nonce = topRequest.Tx.Nonce()
		newState.TxSender, _ = topRequest.Tx.SenderAddress()
	}
	if callRequest != nil {
		newState.Nonce = callRequest.Nonce
		newState.TxSender = common.BytesToAddress(callRequest.TxSender)
	}

	newState.SimulationCallStates[simulationNum] = make(api.SimulationCallStates, 0)
	s.simulationState[txHash] = newState

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Msgf("startCXT for tx %s, simulationNum %d, OriginShardId %d, relatedShards %v, oldRelatedShards %v",
			txHash.Hex(), simulationNum, originShardId, newState.RelatedShards, relatedShards)

	return true, nil
}

func (s *sscService) handleTxSp1Timeout(txHash common.Hash) {
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("cxt has timeout for sp1, send rollback vote to ssc leader")
	payload := &api.CXTInvalidSimulationPayload{
		Type: api.CXTTimeout,
	}
	payloadBytes, _ := json.Marshal(payload)
	vote := &api.CXTCommitVote{
		BaseSSCMessage: api.BaseSSCMessage{},
		TxHash:         txHash.Bytes(),
		ShardId:        s.SelfShard,
		OriginShardId:  s.SelfShard,
		Type:           api.Rollback,
		Reason:         api.ReasonCxtTimeoutForSp1,
		Payload:        payloadBytes,
	}
	s.sendCXTCommitVote(s.SelfShard, vote)
}

// block committed will be called after apply the block, so we can close the transaction here after check if the tx's simulation is not onchain
func (s *sscService) handleTxPoolTimeout(txHash common.Hash, poolTimeout uint64) {
	s.stateLock.RLock()
	_, err := s.getState(txHash)
	_, existsOnChain := s.executionVerifyContexts[txHash]
	s.stateLock.RUnlock()
	if err != nil {
		return
	}
	if existsOnChain {
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Msgf("cxt has timeout for pool, but the tx is already onchain")
		return
	}
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
		Msgf("simulation is pool timeout, status=PoolTimeout")
	s.closeTransaction(txHash, false)
	// simulationCommit := &api.SimulationCommit{
	// 	SimulationNum:        state.SimulationNum,
	// 	TxHash:               txHash.Bytes(),
	// 	Nonce:                state.Nonce,
	// 	Sender:               state.TxSender.Bytes(),
	// 	RelatedShards:        state.RelatedShards,
	// 	Commit:               false,
	// 	Status:               api.PoolTimeout,
	// 	BaseBLSSignedMessage: api.BaseBLSSignedMessage{},
	// }
	// s.thresholdSignSimulationCommit(simulationCommit)
	// ctx, cancel := context.WithTimeout(state.Ctx, s.Config.CallTimeout)
	// defer cancel()
	// leaders := make([]*api.Member, 0)
	// for _, shardId := range simulationCommit.RelatedShards {
	// 	leaders = append(leaders, s.GetLeader(shardId, common.BytesToHash(simulationCommit.TxHash)))
	// }
	// err = s.Comm.Multicast(ctx, leaders, api.Method_CommitSimulation, simulationCommit)
	// if err != nil {
	// 	utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).Msg("failed to multicast simulation commit for pool timeout")
	// 	return
	// }
	// s.lockStateMgr.UnSubscribe(txHash)
}

func newStateSet() *api.StateSet {
	return &api.StateSet{
		Balance: make(map[common.Address]*big.Int),
		State:   make(map[common.Address]map[common.Hash]common.Hash),
	}
}

func newRWSet() *api.RWSet {
	return &api.RWSet{
		ReadState:    newStateSet(),
		WriteState:   newStateSet(),
		CurrentState: newStateSet(),
	}
}

func (s *sscService) SimulateCXTransaction(req *api.CXTSimulationRequest) {
	utils.SSCLogger().Info().Str("txHash", req.Tx.Hash().String()).Msg("simulate cx transaction, start")
	leader := s.GetLeader(s.SelfShard, req.Tx.Hash())

	ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)

	go func() {
		defer cancel()
		ret := new(api.CXTSimulationSSCResult)
		err := s.Comm.Call(ctx, ret, leader, api.Method_StartSimulateCXTransaction, req)
		if err != nil {
			utils.SSCLogger().Error().Str("txHash", req.Tx.Hash().Hex()).Err(err).Msgf("failed to call start simulate cx transaction")
			ret = &api.CXTSimulationSSCResult{Err: err.Error()}
		}
		utils.SSCLogger().Info().Str("txHash", req.Tx.Hash().String()).Msg("simulate cx transaction, end")
	}()
}

func (s *sscService) CallCXTContract(req *api.CXTCallRequest) *api.CXTCallSSCResult {
	txHash := common.BytesToHash(req.TxHash)
	committee := s.GetCommittee(s.SelfShard)
	targetShardId := s.GetShardID(req.Addr)
	var (
		cxtState      *api.CXTSimulationState
		dependentCall *api.DependentCXTCall
		ctx           context.Context
		cancel        context.CancelFunc
	)
	sscResult := func() *api.CXTCallSSCResult {
		s.stateLock.Lock()
		defer s.stateLock.Unlock()

		cxtState, _ = s.getState(txHash)
		if cxtState == nil {
			return &api.CXTCallSSCResult{Err: "no simulation state found"}
		}
		ctx, cancel = context.WithTimeout(cxtState.Ctx, s.Config.CallTimeout)

		req.Nonce = cxtState.Nonce
		req.TxSender = cxtState.TxSender.Bytes()
		req.SimulationNum = cxtState.SimulationNum
		req.OriginShardId = cxtState.OriginShardId
		req.FromShardId = s.SelfShard
		req.CallIndex = make(api.CallIndex, len(cxtState.CurrentCallFrame.CallIndex))
		copy(req.CallIndex, cxtState.CurrentCallFrame.CallIndex)
		req.CallIndex = append(req.CallIndex, cxtState.CurrentCallFrame.PC)
		req.RelatedShards = cxtState.RelatedShards
		cxtState.CurrentCallFrame.Next()

		// if it's recall and callState is locked
		if cxtState.SimulationNum > 0 && req.CallIndex.Compare(cxtState.LockedCallIndex) <= 0 {
			utils.SSCLogger().Debug().Str("txHash", txHash.String()).
				Str("callIndex", req.CallIndex.ToString()).
				Msg("recall contract has been locked, reuse it")
			callStates := cxtState.SimulationCallStates[req.SimulationNum]
			callerState := callStates.Get(req.CallIndex[:len(req.CallIndex)-1])
			lockedCallStates := cxtState.SimulationCallStates[cxtState.SimulationNum-1]
			dependentCall := lockedCallStates.Get(req.CallIndex).DependentCXTCalls[req.CallIndex.ToString()]
			callerState.DependentCXTCalls[req.CallIndex.ToString()] = dependentCall
			ret := callerState.DependentCXTCalls[req.CallIndex.ToString()].SSCResult
			return ret
		}
		callStates := cxtState.SimulationCallStates[req.SimulationNum]
		// get caller's call state
		callerState := callStates.Get(req.CallIndex[:len(req.CallIndex)-1])
		if callerState == nil {
			return &api.CXTCallSSCResult{Err: "no call state found"}
		}
		dependentCall = callerState.DependentCXTCalls[req.CallIndex.ToString()]
		if dependentCall != nil && callerState.Executed {
			return callerState.CallSSCResult
		}
		if dependentCall == nil {
			dependentCall = &api.DependentCXTCall{
				CallIndex:     req.CallIndex,
				Requests:      make([]*api.CXTCallRequest, 0, committee.Threshold),
				SignedRequest: nil,
				SSCResult:     nil,
				Executed:      false,
				WaitingChs:    make([]chan *api.CXTCallSSCResult, 0),
			}
			callerState.DependentCXTCalls[req.CallIndex.ToString()] = dependentCall
		}
		return nil
	}()

	if sscResult != nil {
		return sscResult
	}

	sign, err := s.signerMgr.GetSSCSigner().Sign(req)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.String()).
			Str("callIndex", req.CallIndex.ToString()).Err(err).Msg("failed to sign cxt call ssc request")
		return nil
	}
	req.BaseSSCMessage.SenderAddr = s.txSigner.Address()
	req.BaseSSCMessage.Signature = sign

	leader := s.GetLeader(s.SelfShard, txHash)
	utils.SSCLogger().Debug().
		Str("txHash", txHash.String()).
		Str("cxtStateCallIndex", cxtState.CurrentCallFrame.CallIndex.ToString()).
		Str("reqCallIndex", req.CallIndex.ToString()).
		Int("currentIndex", cxtState.CurrentCallFrame.PC).
		Str("leader", leader.Endpoint).
		Uint32("selfShard", s.SelfShard).
		Uint32("targetShard", targetShardId).
		Str("addr", req.Addr.Hex()).
		Str("callIndex", req.CallIndex.ToString()).Msg("call cxt contract, start")

	defer cancel()
	ret := new(api.CXTCallSSCResult)
	err = s.Comm.Call(ctx, ret, leader, api.Method_RequestCallCXT, req)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to call cxt contract")
		return &api.CXTCallSSCResult{Err: fmt.Sprintf("error from targetShard %d, err=%s", targetShardId, err.Error())}
	}
	if ret.Err != "" {
		utils.SSCLogger().Error().Str("txHash", txHash.String()).
			Str("callIndex", req.CallIndex.ToString()).Err(errors.New(ret.Err)).Msgf("failed to call cxt contract, targetShardId: %d", req.TargetShardId)
	} else {
		err = s.signerMgr.GetSSCSigner().Verify(ret)
		if err != nil {
			utils.SSCLogger().Error().Str("txHash", txHash.String()).
				Str("callIndex", req.CallIndex.ToString()).Err(err).Msg("failed to verify cxt call ssc result signature")
			return &api.CXTCallSSCResult{Err: fmt.Sprintf("error from targetShard %d, err=%s", targetShardId, err.Error())}
		}
	}
	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	dependentCall.SSCResult = ret
	dependentCall.Executed = true
	dependentCall.WaitingChs = nil
	cxtState.RelatedShards = ret.RelatedShards
	utils.SSCLogger().Debug().Str("txHash", txHash.String()).
		Str("callIndex", req.CallIndex.ToString()).
		Uint32("targetShard", targetShardId).
		Str("relatedShards", fmt.Sprintf("%v", cxtState.RelatedShards)).
		Interface("result", ret).
		Msg("call cxt contract, end")
	return ret
}

func (s *sscService) VerifySimulation(simulationBytes []byte, stateDB api.StateDB, header *block.Header) {
	// 1
	simulation := &api.CXTSimulation{}
	err := json.Unmarshal(simulationBytes, simulation)
	// 1 [x]
	if err != nil {
		txHash := common.BytesToHash(simulation.TxHash)
		utils.SSCLogger().Error().Err(err).Str("txHash", txHash.Hex()).
			Msgf("failed to unmarshal simulation, size: %d", len(simulationBytes))
		payload := &api.CXTInvalidSimulationPayload{
			Type: api.InvalidSerialization,
		}
		payloadBytes, _ := json.Marshal(payload)
		vote := &api.CXTCommitVote{
			TxHash:        txHash.Bytes(),
			Type:          api.Rollback,
			ShardId:       s.SelfShard,
			OriginShardId: simulation.OriginShardId,
			Reason:        api.ReasonInvalidSimulation,
			Payload:       payloadBytes,
		}
		s.sendCXTCommitVote(simulation.OriginShardId, vote)
		return
	}

	txHash := common.BytesToHash(simulation.TxHash)
	if s.SelfShard == simulation.OriginShardId {
		s.timerMgr.StartTimer(txHash, header.NumberU64(), s.SelfShard)
	}

	conflictLockCallIndexes := make([]api.CallIndex, 0)
	conflictLockKeys := make([]api.LockKey, 0)
	conflictCallStateIndex := -1
	var execErr error

	callStateMap := make(map[string]*api.CXTCallState)
	for _, callState := range simulation.CallStates {
		callStateMap[callState.CallIndex.ToString()] = callState
	}
	verifyContext := &api.ExecutionVerifyContext{Simulation: simulation, CallStateMap: callStateMap}
	s.stateLock.Lock()
	s.executionVerifyContexts[txHash] = verifyContext
	s.stateLock.Unlock()

CallStates:
	for i, callState := range simulation.CallStates {
		// check if states in write set are locked by other tx
		for address, stateMap := range callState.RWSet.WriteState.State {
			for key, _ := range stateMap {
				lockKey := api.FormKey(address, key)
				_, err := stateDB.GetState(address, key)
				if err != nil && errors.Is(err, api.ErrLockedByOtherTx) {
					utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
						Msgf("lock conflict for key: %s", lockKey)
					conflictLockKeys = append(conflictLockKeys, lockKey)
					conflictLockCallIndexes = append(conflictLockCallIndexes, callState.CallIndex)
				}
			}
		}
		// check if states in read set are locked by other tx or not consistent with current state
		for address, stateMap := range callState.RWSet.ReadState.State {
			for key, _ := range stateMap {
				lockKey := api.FormKey(address, key)
				onChainValue, err := stateDB.GetState(address, key)
				if err != nil && errors.Is(err, api.ErrLockedByOtherTx) {
					utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
						Msgf("lock conflict for key: %s", lockKey)
					conflictLockKeys = append(conflictLockKeys, lockKey)
					conflictLockCallIndexes = append(conflictLockCallIndexes, callState.CallIndex)
				}
				if err == nil && bytes.Compare(onChainValue.Bytes(), callState.RWSet.ReadState.State[address][key].Bytes()) != 0 {
					conflictCallStateIndex = i
					break CallStates
				}
			}
		}

		// execute the contract and check if the write set is consistent with the simulation's write set
		execErr = s.verifyExecuteForCallState(simulation, txHash, callState, stateDB.(*corestate.DB))
		if execErr != nil {
			break CallStates
		}
	}

	if execErr != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
			Err(execErr).Msg("failed to verify execution for call state")
		payload := &api.CXTInvalidSimulationPayload{
			Type: api.InvalidExecution,
		}
		payloadBytes, _ := json.Marshal(payload)
		vote := &api.CXTCommitVote{
			TxHash:        txHash.Bytes(),
			Type:          api.Rollback,
			ShardId:       s.SelfShard,
			OriginShardId: simulation.OriginShardId,
			Reason:        api.ReasonInvalidSimulation,
			Payload:       payloadBytes,
		}
		s.sendCXTCommitVote(simulation.OriginShardId, vote)
		return
	}

	if conflictCallStateIndex >= 0 {
		snapshot := stateDB.Snapshot()
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Str("callIndex", simulation.CallStates[conflictCallStateIndex].CallIndex.ToString()).
			Msgf("try to lock state with execution")
		err := s.lockStateWithExecution(simulation.CallStates[conflictCallStateIndex], stateDB.(*corestate.DB))
		if err != nil {
			stateDB.RevertToSnapshot(snapshot)
			utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
				Err(err).Msg("failed to lock state with execution")
			payload := &api.CXTInvalidSimulationPayload{
				Type: api.InvalidExecution,
			}
			payloadBytes, _ := json.Marshal(payload)
			vote := &api.CXTCommitVote{
				TxHash:        txHash.Bytes(),
				Type:          api.Rollback,
				ShardId:       s.SelfShard,
				OriginShardId: simulation.OriginShardId,
				Reason:        api.ReasonConflictRWSetFailedLock,
				Payload:       payloadBytes,
			}
			s.sendCXTCommitVote(simulation.OriginShardId, vote)
			return
		}
		payload := &api.CXTConflictRWSetPayload{
			ConflictCallIndex: simulation.CallStates[conflictCallStateIndex].CallIndex,
		}
		payloadBytes, _ := json.Marshal(payload)
		vote := &api.CXTCommitVote{
			TxHash:        txHash.Bytes(),
			ShardId:       s.SelfShard,
			Type:          api.Recall,
			OriginShardId: simulation.OriginShardId,
			Reason:        api.ReasonConflictRWSetRecall,
			Payload:       payloadBytes,
		}
		s.sendCXTCommitVote(s.SelfShard, vote)
		return
	}

	if len(conflictLockKeys) > 0 {
		// save conflict cmLock keys to re-simulation later, and the commit simulation tx won't be packaged into the block
		lockedStates := make(map[api.LockKey]interface{})
		for _, callState := range simulation.CallStates {
			for addr, account := range callState.RWSet.ReadState.State {
				for key, _ := range account {
					lockedStates[api.FormKey(addr, key)] = struct{}{}
				}
			}
			for addr, account := range callState.RWSet.WriteState.State {
				for key, _ := range account {
					lockedStates[api.FormKey(addr, key)] = struct{}{}
				}
			}
		}

		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Int("lockedStates", len(lockedStates)).
			Msg("subscribe lock States for re-simulation")

		err = s.lockStateMgr.Subscribe(txHash, simulation.Nonce, common.BytesToAddress(simulation.Sender), lockedStates,
			simulation.OriginShardId, simulation.SimulationNum+1, false)
		if err != nil {
			payload := &api.CXTConflictRWSetPayload{
				ConflictCallIndex: api.CallIndex{},
			}
			payloadBytes, _ := json.Marshal(payload)
			s.sendCXTCommitVote(simulation.OriginShardId, &api.CXTCommitVote{
				TxHash:        txHash.Bytes(),
				SimulationNum: simulation.SimulationNum,
				ShardId:       simulation.ShardId,
				OriginShardId: simulation.OriginShardId,
				Type:          api.Rollback,
				Reason:        api.ReasonConflictRWSetFailedLock,
				Payload:       payloadBytes,
			})
		}
	} else {
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Uint32("OriginShardId", simulation.OriginShardId).
			Int("size", len(simulationBytes)).
			Msgf("simulation is valid, lock the rwset and send commit vote")
		for _, callState := range simulation.CallStates {
			s.lockStateWithRWSet(txHash, callState, stateDB)
		}
		vote := &api.CXTCommitVote{
			TxHash:        txHash.Bytes(),
			ShardId:       s.SelfShard,
			OriginShardId: simulation.OriginShardId,
			Type:          api.Commit,
			Reason:        api.ReasonSuccess,
			Payload:       nil,
		}
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Uint32("OriginShardId", simulation.OriginShardId).
			Int("size", len(simulationBytes)).
			Msgf("send cxt commit vote")
		s.sendCXTCommitVote(s.SelfShard, vote)
	}
}

func (s *sscService) verifyExecuteForCallState(simulation *api.CXTSimulation, txHash common.Hash, callState *api.CXTCallState, stateDB *corestate.DB) error {
	chainConfig := s.bc.Config()
	vmConfig := s.bc.GetVMConfig()
	s.stateLock.RLock()
	verifyContext := s.executionVerifyContexts[txHash]
	_, finished := s.finishedTxs[txHash]
	s.stateLock.RUnlock()
	if finished {
		return api.ErrTxHasBeenClosed
	}
	if verifyContext == nil {
		return errors.New("execution verify context is nil")
	}

	var (
		origin    common.Address
		callIndex api.CallIndex
		gasPrice  *big.Int
		addr      common.Address
		input     []byte
		gas       uint64
		value     *big.Int
		header    *block.Header
		simuRet   []byte
	)

	if callState.CallIndex.Top() {
		req := callState.TopRequest
		header = s.bc.GetHeaderByHash(common.BytesToHash(req.BlockHash))
		origin = req.From
		callIndex = api.CallIndex{}
		gasPrice = req.Tx.GasPrice()
		addr = *req.Tx.To()
		input = req.Tx.Data()
		gas = req.Tx.GasLimit()
		value = req.Tx.Value()
		if callState.TopResult == nil {
			marshal, _ := json.Marshal(callState)
			return errors.New(fmt.Sprintf("top result is nil, callState: %s", string(marshal)))
		}
		simuRet = callState.TopResult.Result
	} else {
		req := callState.CallRequest
		blockHash := common.BytesToHash(req.BlockHash)
		header = s.bc.GetHeaderByHash(blockHash)
		origin = req.Caller
		callIndex = req.CallIndex
		gasPrice = req.GasPrice
		addr = req.Addr
		input = req.Input
		gas = req.Gas
		value = req.Value
		if callState.CallResult == nil {
			marshal, _ := json.Marshal(callState)
			return errors.New(fmt.Sprintf("top result is nil, callState: %s", string(marshal)))
		}
		simuRet = callState.CallResult.Result
	}

	verifyContext.CurrentState = newStateSet()
	verifyContext.DependentResults = callState.DependentResults
	verifyContext.CallFrame = &api.CallFrame{
		CallIndex: callIndex,
		PC:        0,
	}

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Str("callIndex", callIndex.ToString()).
		Int("results_num", len(verifyContext.DependentResults)).
		Str("simulation", simulation.String()).
		Msgf("verify call state %s for shard %d", callState.CallIndex.ToString(), s.SelfShard)

	// prepare execution verify context
	sender := vm.AccountRef(origin)
	vmCtx := core.NewSSCVMContext(origin, txHash, callIndex, gasPrice, header, s.bc, nil)
	sscvm := vm.NewSSCVM(vmCtx, stateDB, chainConfig, *vmConfig, s, vm.ExecutionVerify)

	// execute contract with ExecutionVerify
	ret, _, err := sscvm.Call(sender, addr, input, gas, value)
	if err != nil {
		return err
	}

	// compare the result and leftOverGas
	if bytes.Compare(ret, simuRet) != 0 {
		return errors.New(fmt.Sprintf("simulation result is not equal to execution result, expected: %v, got: %v", simuRet, ret))
	}
	if !verifyContext.CurrentState.Equal(callState.RWSet.WriteState) {
		return errors.New(fmt.Sprintf("simulation write set is not equal to execution write set, callFrame=%v, process: [%d/%d], expected: %v, got: %v",
			verifyContext.CallFrame, verifyContext.CallFrame.PC, len(verifyContext.DependentResults),
			callState.RWSet.WriteState, verifyContext.CurrentState))
	}
	return nil
}

func (s *sscService) sendCXTCommitVote(shardId uint32, vote *api.CXTCommitVote) {
	txHash := common.BytesToHash(vote.TxHash)

	if vote.Type == api.Recall {
		sign, err := s.signerMgr.GetSSCSigner().Sign(vote)
		if err != nil {
			utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
				Msg("failed to sign CXTCommitVote")
			return
		}
		vote.BaseSSCMessage = api.BaseSSCMessage{
			SenderAddr: s.txSigner.Address(),
			Signature:  sign,
		}
	} else {
		sign, err := s.signerMgr.GetValidatorSigner().Sign(vote)
		if err != nil {
			utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
				Msg("failed to sign CXTCommitVote")
			return
		}
		vote.BaseSSCMessage = api.BaseSSCMessage{
			SenderAddr: s.txSigner.Address(),
			Signature:  sign,
		}
	}

	ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
	defer cancel()

	targetLeader := s.GetLeader(shardId, txHash)

	utils.SSCLogger().Debug().Msgf("send CXTCommitVote to %s", targetLeader.Endpoint)
	s.Comm.Call(ctx, nil, targetLeader, api.Method_HandleCommitVote, vote)
}

func (s *sscService) checkLockConflict(simulation *api.CXTSimulation, stateDB api.StateDB) bool {
	txHash := common.BytesToHash(simulation.TxHash)
	conflictCallIndexes := make([]api.CallIndex, 0)
	conflictLockKeys := make([]api.LockKey, 0)

	for _, callState := range simulation.CallStates {
		// check if read set is conflict with current state
		for address, stateMap := range callState.RWSet.ReadState.State {
			for key, _ := range stateMap {
				lockKey := api.FormKey(address, key)
				// TODO 改成只是检查锁状态，不需要获取状态
				_, err := stateDB.GetState(address, key)
				if err != nil && errors.Is(err, api.ErrLockedByOtherTx) {
					utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
						Msgf("lock conflict for key: %s", lockKey)
					conflictLockKeys = append(conflictLockKeys, lockKey)
					conflictCallIndexes = append(conflictCallIndexes, callState.CallIndex)
				}
			}
		}
		for address, stateMap := range callState.RWSet.WriteState.State {
			for key, _ := range stateMap {
				lockKey := api.FormKey(address, key)
				_, err := stateDB.GetState(address, key)
				if err != nil && errors.Is(err, api.ErrLockedByOtherTx) {
					utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
						Msgf("lock conflict for key: %s", lockKey)
					conflictLockKeys = append(conflictLockKeys, lockKey)
					conflictCallIndexes = append(conflictCallIndexes, callState.CallIndex)
				}
			}
		}
	}

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Int("conflictLockKeys", len(conflictLockKeys)).
		Int("conflictCallIndexes", len(conflictCallIndexes)).
		Msg("check lock conflict for simulation")

	return len(conflictLockKeys) > 0
}

func (s *sscService) lockStateWithExecution(callState *api.CXTCallState, state *corestate.DB) error {
	var (
		topReq        *api.CXTSimulationRequest
		callReq       *api.CXTCallSSCRequest
		txHash        common.Hash
		caller        common.Address
		callIndex     api.CallIndex
		gasPrice      *big.Int
		addr          common.Address
		input         []byte
		gas           uint64
		value         *big.Int
		relatedShards []uint32
	)
	if callState.CallIndex.Top() {
		topReq = callState.TopRequest
		txHash = common.BytesToHash(topReq.TxHash)
		caller = topReq.From
		callIndex = api.CallIndex{}
		gasPrice = topReq.Tx.GasPrice()
		addr = *topReq.Tx.To()
		input = topReq.Tx.Data()
		gas = topReq.Tx.GasLimit()
		value = topReq.Tx.Value()
		relatedShards = callState.TopResult.RelatedShards
	} else {
		callReq = callState.CallRequest
		txHash = common.BytesToHash(callReq.TxHash)
		caller = callReq.Caller
		callIndex = callReq.CallIndex
		gasPrice = callReq.GasPrice
		addr = callReq.Addr
		input = callReq.Input
		gas = callReq.Gas
		value = callReq.Value
		relatedShards = callState.CallResult.RelatedShards
	}
	header := s.bc.CurrentHeader()
	vmCtx := core.NewSSCVMContext(caller, txHash, callIndex, gasPrice, header, s.bc, nil)
	sscvm := vm.NewSSCVM(vmCtx, state, s.bc.Config(), *s.bc.GetVMConfig(), s, vm.LockExecution)

	// execute contract with LockExecution to cmLock state and save recall simulation to handle recall request
	ret, leftOverGas, err := sscvm.Call(vm.AccountRef(caller), addr, input, gas, value)
	result := &api.CXTCallResult{
		CallIndex:      callIndex,
		RelatedShards:  relatedShards,
		Result:         ret,
		LeftOverGas:    leftOverGas,
		BlockHash:      header.Hash().Bytes(),
		BaseSSCMessage: api.BaseSSCMessage{},
	}
	if err != nil {
		result.Err = err.Error()
	}

	// cache the recall state for the next simulation recall state
	if s.IsMember() {
		s.stateLock.Lock()
		defer s.stateLock.Unlock()
		simulationState, err := s.getState(txHash)
		if err != nil {
			return err
		}
		if simulationState.SimulationCallStates[simulationState.SimulationNum+1] == nil {
			simulationState.SimulationCallStates[simulationState.SimulationNum+1] = make(api.SimulationCallStates, 0)
		}
		nextCallState := simulationState.SimulationCallStates[simulationState.SimulationNum+1].Get(callIndex)
		if nextCallState == nil {
			nextCallState = &api.SimulationCallState{
				CallIndex:         callIndex,
				TopRequest:        topReq,
				CallRequest:       callReq,
				DependentCXTCalls: nil,
				RWSet:             newRWSet(),
				Result:            result,
				CallSSCResult:     nil,
				Executed:          false,
				Lock:              sync.Mutex{},
			}
			simulationState.SimulationCallStates[simulationState.SimulationNum+1].Add(nextCallState)
		}
	}

	return err
}

func (s *sscService) lockStateWithRWSet(txHash common.Hash, callState *api.CXTCallState, stateDB api.StateDB) {
	for address, stateMap := range callState.RWSet.WriteState.State {
		for key, value := range stateMap {
			stateDB.SetAndLockState(txHash, callState.CallIndex, address, key, value)
		}
	}
}

func (s *sscService) CommitOrRollbackWithProof(commitProofBytes []byte, stateDB api.StateDB) error {
	commitProof := &api.CXTCommitProof{}
	err := json.Unmarshal(commitProofBytes, commitProof)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to unmarshal commit proof")
		return err
	}
	txHash := common.BytesToHash(commitProof.TxHash)
	s.stateLock.Lock()
	_, exists := s.finishedTxs[txHash]
	s.stateLock.Unlock()
	if exists {
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msg("cxt has committed or rollback")
		return nil
	}

	if commitProof.Type == api.Commit {
		utils.SSCLogger().Info().Str("txHash", txHash.String()).Msgf("commit with proof, origin: [%v,%d]", commitProof.OriginShard == s.SelfShard, commitProof.OriginShard)
		if commitProof.SimulationNum > 0 {
			utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msgf("commit with proof after %d resimulation, origin: [%v,%d]",
				commitProof.SimulationNum, commitProof.OriginShard == s.SelfShard, commitProof.OriginShard)
		}
		err := stateDB.CommitTx(txHash)
		if err != nil {
			utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(err).Msg("failed to commit tx with proof")
			return err
		}
		s.closeTransaction(txHash, true)
	} else if commitProof.Type == api.Rollback {
		utils.SSCLogger().Info().Str("txHash", txHash.String()).Msgf("rollback with proof, origin: [%v,%d], reason: %s", commitProof.OriginShard == s.SelfShard, commitProof.OriginShard, commitProof.Reason.String())
		if commitProof.SimulationNum > 0 {
			utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msgf("rollback with proof after %d resimulation, origin: [%v,%d]",
				commitProof.SimulationNum, commitProof.OriginShard == s.SelfShard, commitProof.OriginShard)
		}
		err := stateDB.RollbackTx(txHash)
		if err != nil {
			utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(err).Msg("failed to rollback tx with proof")
			return err
		}
		s.closeTransaction(txHash, false)
	}

	return nil
}

func (s *sscService) StartSimulateCXTransaction(req *api.CXTSimulationRequest) *api.CXTSimulationSSCResult {
	var waitingCh chan *api.CXTSimulationSSCResult
	txHash := req.Tx.Hash()

	utils.SSCLogger().Info().Int("simulationNum", req.SimulationNum).Str("txHash", txHash.String()).Msg("start simulate cx transaction, start")
	defer utils.SSCLogger().Info().Int("simulationNum", req.SimulationNum).Str("txHash", txHash.String()).Msg("start simulate cx transaction, end")

	s.simuLock.Lock()
	if s.simuWaitingChs[txHash] == nil {
		s.simuWaitingChs[txHash] = make([]chan *api.CXTSimulationSSCResult, 0)
	} else {
		waitingCh = make(chan *api.CXTSimulationSSCResult)
		s.simuWaitingChs[txHash] = append(s.simuWaitingChs[txHash], waitingCh)
	}
	if s.simuResultCh[txHash] == nil {
		s.simuResultCh[txHash] = make(chan *api.CXTSimulationSSCResult, 1)
	}
	s.simuLock.Unlock()

	if waitingCh != nil {
		return <-waitingCh
	}

	defer func() {
		s.simuLock.Lock()
		delete(s.simuWaitingChs, txHash)
		s.simuLock.Unlock()
	}()

	header := s.bc.CurrentHeader()
	req.BlockHash = header.Hash().Bytes()
	committee := s.GetCommittee(s.SelfShard)
	n := committee.Number
	t := committee.Threshold

	ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(n)
	lock := sync.Mutex{}
	results := make([]api.SSCMessage, 0, t)

	for i := 0; i < n; i++ {
		member := committee.Members[i] // 正确捕获
		go func() {
			defer wg.Done()
			ret := new(api.CXTSimulationResult)
			err := s.Comm.Call(ctx, ret, member, api.Method_HandleSimulateRequest, req)
			if err != nil {
				return
			}
			lock.Lock()
			if len(results) < t && ctx.Err() == nil {
				results = append(results, ret)
				if len(results) == t {
					cancel()
				}
			}
			lock.Unlock()
		}()
	}
	wg.Wait()
	sscResult, err := s.aggregateSimulationResults(results)
	if err != nil {
		sscResult = &api.CXTSimulationSSCResult{Err: err.Error(), RelatedShards: []uint32{s.SelfShard}}
		utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(err).Msg("failed to aggregate simulation results")
		return sscResult
	}

	simulationCommit := &api.SimulationCommit{
		SimulationNum:        req.SimulationNum,
		TxHash:               req.Tx.Hash().Bytes(),
		Nonce:                req.Tx.Nonce(),
		Sender:               req.From.Bytes(),
		RelatedShards:        sscResult.RelatedShards,
		BaseBLSSignedMessage: api.BaseBLSSignedMessage{},
	}

	if len(sscResult.Err) == 0 {
		simulationCommit.Commit = true
		simulationCommit.Status = api.OK
		utils.SSCLogger().Debug().Str("txHash", common.BytesToHash(simulationCommit.TxHash).Hex()).
			Msgf("simulation accomplished, send simulation commit, commit type: %v, simulationNum: %d, relatedShards: %v",
				simulationCommit.Commit, simulationCommit.SimulationNum, simulationCommit.RelatedShards)
	} else {
		if vm.IsLockedByOtherTxErr(sscResult.Err) {
			simulationCommit.Commit = false
			simulationCommit.Status = api.LockConflict
			// don't send commit simulation if state is locked by other tx
			return sscResult
		} else {
			simulationCommit.Commit = false
			simulationCommit.Status = api.ExecutionFailed
		}
		utils.SSCLogger().Error().Str("txHash", common.BytesToHash(simulationCommit.TxHash).Hex()).
			Err(errors.New(sscResult.Err)).Msgf("simulation accomplished, send simulation commit, commit type: %v, status: %v, simulationNum: %d, relatedShards: %v",
			simulationCommit.Commit, simulationCommit.Status.String(), simulationCommit.SimulationNum, simulationCommit.RelatedShards)
	}

	s.stateLock.Lock()
	state, err := s.getState(req.Tx.Hash())
	if err != nil {
		s.stateLock.Unlock()
		sscResult = &api.CXTSimulationSSCResult{Err: err.Error(), RelatedShards: []uint32{s.SelfShard}}
		utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(err).Msg("failed to get state")
		return sscResult
	}
	s.stateLock.Unlock()

	s.simuLock.Lock()
	state.SimulationCallStates[state.SimulationNum][0].TopSSCResult = sscResult
	state.SimulationResult = sscResult
	chs := s.simuWaitingChs[txHash]
	s.simuLock.Unlock()

	for _, ch := range chs {
		ch <- sscResult
	}

	s.thresholdSignSimulationCommit(simulationCommit)
	leaders := make([]*api.Member, 0)
	for _, shardId := range simulationCommit.RelatedShards {
		leaders = append(leaders, s.GetLeader(shardId, common.BytesToHash(simulationCommit.TxHash)))
	}
	s.Comm.Multicast(ctx, leaders, api.Method_CommitSimulation, simulationCommit)

	s.simuLock.Lock()
	if s.simuResultCh[txHash] != nil {
		s.simuResultCh[txHash] <- sscResult
		close(s.simuResultCh[txHash])
		delete(s.simuResultCh, txHash)
	}
	s.simuLock.Unlock()

	return sscResult
}

func (s *sscService) thresholdSignSimulationCommit(commit *api.SimulationCommit) {
	committee := s.GetCommittee(s.SelfShard)
	n := committee.Number
	t := committee.Threshold
	txHash := common.BytesToHash(commit.TxHash)
	s.stateLock.RLock()
	state, err := s.getState(txHash)
	s.stateLock.RUnlock()
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(err).Msg("failed to get state")
		return
	}

	ctx, cancel := context.WithTimeout(state.Ctx, s.Config.CallTimeout)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(n)
	lock := sync.Mutex{}
	baseMsgs := make([]api.SSCMessage, 0, t)

	for _, member := range committee.Members {
		go func() {
			defer wg.Done()
			signature := make([]byte, 0)
			err := s.Comm.Call(ctx, &signature, member, api.Method_SignSimulationCommit, commit)
			if err != nil {
				return
			}
			utils.SSCLogger().Debug().Msg("received signature " + common.Bytes2Hex(signature) + " from " + member.Address.String())
			lock.Lock()
			if len(baseMsgs) < t {
				baseMsgs = append(baseMsgs, api.BaseSSCMessage{
					Signature:  signature,
					SenderAddr: member.Address,
				})
				if len(baseMsgs) == t {
					cancel()
				}
			}
			lock.Unlock()
		}()
	}
	wg.Wait()
	aggregatedSig, bitMap, err := s.BLSSignerMgr.GetSSCSigner().Aggregate(baseMsgs)
	if err != nil {
		utils.SSCLogger().Error().Err(err)
		return
	}
	commit.Signatures = aggregatedSig
	commit.BLSBitMap = bitMap
	commit.ShardId = s.SelfShard
}

func (s *sscService) aggregateSimulationResults(results []api.SSCMessage) (*api.CXTSimulationSSCResult, error) {
	if len(results) == 0 {
		return nil, errors.New("no simulation results to aggregate")
	}
	result := results[0].(*api.CXTSimulationResult)
	aggregatedSig, bitMap, err := s.BLSSignerMgr.GetSSCSigner().Aggregate(results)
	if err != nil {
		return nil, err
	}
	sscResult := &api.CXTSimulationSSCResult{
		RelatedShards: result.RelatedShards,
		Result:        result.Result,
		Receipt:       result.Receipt,
		UsedGas:       result.UsedGas,
		Err:           result.Err,
		BaseBLSSignedMessage: api.BaseBLSSignedMessage{
			ShardId:    s.SelfShard,
			Signatures: aggregatedSig,
			BLSBitMap:  bitMap,
		},
	}
	return sscResult, nil
}

func (s *sscService) HandleSimulateRequest(ctx context.Context, req *api.CXTSimulationRequest) *api.CXTSimulationResult {
	txHash := common.BytesToHash(req.TxHash)
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Int("simulationNum", req.SimulationNum).Msgf("handle simulate request, start, simulationNum=%d", req.SimulationNum)
	defer utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msg("handle simulate request, end")
	callState, err := s.startSimulation(req)
	if err != nil {
		return &api.CXTSimulationResult{Err: err.Error()}
	}
	// callState.Lock.Lock()
	// defer callState.Lock.Unlock()

	s.stateLock.RLock()
	simuState, err := s.getState(txHash)
	if err != nil {
		s.stateLock.RUnlock()
		return &api.CXTSimulationResult{Err: err.Error()}
	}
	relatedShards := simuState.RelatedShards
	s.stateLock.RUnlock()

	header := s.bc.GetHeaderByHash(common.BytesToHash(req.BlockHash))
	state, _ := s.bc.StateAt(header.Root())
	chainConfig := s.bc.Config()
	vmConfig := s.bc.GetVMConfig()

	s.timerMgr.StartPoolTimer(txHash, header.NumberU64(), s.SelfShard)

	var (
		gp            *core.GasPool
		tx            *types.Transaction
		executionType vm.ExecutionType
	)

	if req.SimulationNum == 0 {
		tx = req.Tx
		gp = new(core.GasPool).AddGas(req.GasPool)
		executionType = vm.SimulationCall
		// init cxt state
	} else {
		tx = simuState.SimulationRequest.Tx
		gp = new(core.GasPool).AddGas(simuState.SimulationRequest.GasPool)
		executionType = vm.SimulationReCall
	}

	var signer types.Signer
	if tx.IsEthCompatible() {
		if !chainConfig.IsEthCompatible(header.Epoch()) {
			return &api.CXTSimulationResult{Err: "ethereum compatible transactions not supported at current epoch"}
		}
		signer = types.NewEIP155Signer(chainConfig.EthCompatibleChainID)
	} else {
		signer = types.MakeSigner(chainConfig, header.Epoch())
	}
	msg, err := tx.AsMessage(signer)

	if err != nil {
		return &api.CXTSimulationResult{Err: err.Error(), BaseSSCMessage: api.BaseSSCMessage{}}
	}

	vmCtx := core.NewSSCVMContext(msg.From(), tx.Hash(), api.CallIndex{}, tx.GasPrice(), header, s.bc, req.Author)
	vmCtx.TxType = types.CXTransaction
	sscvm := vm.NewSSCVM(vmCtx, state, chainConfig, *vmConfig, s, executionType)
	result, err := core.NewSSCStateTransition(sscvm, msg, gp).TransitionDb()
	if err != nil {
		return &api.CXTSimulationResult{Err: err.Error(), BaseSSCMessage: api.BaseSSCMessage{}}
	}
	ret := &api.CXTSimulationResult{
		RelatedShards:  relatedShards,
		Result:         result.ReturnData,
		Receipt:        nil,
		UsedGas:        result.UsedGas,
		Err:            "",
		BaseSSCMessage: api.BaseSSCMessage{},
	}
	if result.VMErr != nil {
		ret.Err = result.VMErr.Error()
	}
	if callState.LockedByOtherTx != nil {
		ret.Err = api.ErrLockedByOtherTx.Error()
		lockedStates := make(map[api.LockKey]interface{})
		for addr, account := range callState.RWSet.ReadState.State {
			for key, _ := range account {
				lockedStates[api.FormKey(addr, key)] = struct{}{}
			}
		}
		for addr, account := range callState.RWSet.ReadState.State {
			for key, _ := range account {
				lockedStates[api.FormKey(addr, key)] = struct{}{}
			}
		}
		err := s.lockStateMgr.Subscribe(txHash, req.Tx.Nonce(), req.From, lockedStates,
			s.SelfShard, req.SimulationNum+1, true)
		if err != nil {
			return nil
		}
	}

	sign, err := s.signerMgr.GetSSCSigner().Sign(ret)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Str("txHash", txHash.Hex()).Msg("failed to sign simulation result")
		return nil
	}
	ret.BaseSSCMessage.Signature = sign
	ret.BaseSSCMessage.SenderAddr = s.SelfAddr

	return ret
}

func (s *sscService) RequestCallCXT(req *api.CXTCallRequest) *api.CXTCallSSCResult {
	txHash := common.BytesToHash(req.TxHash)
	committee := s.GetCommittee(s.SelfShard)
	waitingCh := make(chan *api.CXTCallSSCResult, 1)
	var (
		cxtState      *api.CXTSimulationState
		dependentCall *api.DependentCXTCall
	)
	sscResult := func() *api.CXTCallSSCResult {
		s.stateLock.Lock()
		defer s.stateLock.Unlock()

		cxtState, _ = s.getState(txHash)
		if cxtState == nil {
			return &api.CXTCallSSCResult{Err: "no simulation state found"}
		}

		callStates := cxtState.SimulationCallStates[req.SimulationNum]
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Str("callIndex", req.CallIndex.ToString()).Msgf("request call cxt, addr: %s, callStates: %s", req.Addr.Hex(), callStates.ToString())
		// get caller's call state
		callerState := callStates.Get(req.CallIndex[:len(req.CallIndex)-1])
		if callerState == nil {
			return &api.CXTCallSSCResult{Err: "no call state found"}
		}
		dependentCall = callerState.DependentCXTCalls[req.CallIndex.ToString()]
		if dependentCall != nil && callerState.Executed {
			return callerState.CallSSCResult
		}

		if dependentCall == nil {
			utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(errors.New("dependent call not initialized")).
				Interface("req", req).
				Msg("dependent call not initialized")
			return &api.CXTCallSSCResult{Err: "dependent call not initialized"}
		}
		dependentCall.Requests = append(dependentCall.Requests, req)
		dependentCall.WaitingChs = append(dependentCall.WaitingChs, waitingCh)
		if len(dependentCall.Requests) < committee.Threshold {
			dependentCall.Requests = append(dependentCall.Requests, req)
		}
		return nil
	}()

	if sscResult != nil {
		return sscResult
	}

	utils.SSCLogger().Debug().Str("txHash", txHash.String()).Str("callIndex", req.CallIndex.ToString()).Msgf("request call cxt, waiting [%d/%d]", len(dependentCall.Requests), committee.Threshold)

	if len(dependentCall.Requests) == committee.Threshold {
		// aggregate signatures
		// send request to leader
		leader := s.GetLeader(req.TargetShardId, txHash)
		sscReq := s.aggregateSSCCallRequest(dependentCall.Requests)
		dependentCall.SignedRequest = sscReq

		utils.SSCLogger().Debug().
			Str("txHash", txHash.String()).
			Str("cxtStateCallIndex", cxtState.CurrentCallFrame.CallIndex.ToString()).
			Str("reqCallIndex", req.CallIndex.ToString()).
			Str("leader", leader.Endpoint).
			Uint32("sscTargetShardId", s.SelfShard).
			Uint32("reqTargetShardId", req.TargetShardId).
			Uint32("targetShardId", s.GetShardID(req.Addr)).
			Str("addr", req.Addr.Hex()).
			Str("sscAddr", sscReq.Addr.Hex()).
			Str("callIndex", req.CallIndex.ToString()).Msg("request call cxt, start")
		defer utils.SSCLogger().Debug().Str("txHash", txHash.String()).Msg("request call cxt, end")

		s.stateLock.Lock()
		cxtState, _ = s.getState(txHash)
		s.stateLock.Unlock()
		if cxtState == nil {
			return &api.CXTCallSSCResult{Err: "no simulation state found"}
		}
		ctx, cancel := context.WithTimeout(context.Background(), s.Config.CallTimeout)
		defer cancel()

		ret := new(api.CXTCallSSCResult)
		err := s.Comm.Call(ctx, ret, leader, api.Method_HandleCXTSSCCall, sscReq)
		if err != nil {
			utils.SSCLogger().Error().Err(err).Str("txHash", txHash.String()).Str("callIndex", req.CallIndex.ToString()).Msg("failed to call cxt ssc call")
			ret = &api.CXTCallSSCResult{Err: err.Error(), BaseBLSSignedMessage: api.BaseBLSSignedMessage{}}
		}
		if ret.Err != "" {
			utils.SSCLogger().Error().Err(errors.New(ret.Err)).Str("txHash", txHash.String()).Str("callIndex", req.CallIndex.ToString()).Msgf("failed to call cxt ssc call, targetShardId=%d", req.TargetShardId)
		}
		for _, ch := range dependentCall.WaitingChs {
			ch <- ret
		}
	}

	return <-waitingCh
}

func (s *sscService) aggregateSSCCallRequest(requests []*api.CXTCallRequest) *api.CXTCallSSCRequest {
	if len(requests) == 0 {
		utils.SSCLogger().Error().Msg("no cxt call requests to aggregate")
		return nil
	}
	msgs := make([]api.SSCMessage, 0, len(requests))
	for _, msg := range requests {
		msgs = append(msgs, msg)
	}
	request := requests[0]
	aggregatedSig, bitMap, err := s.BLSSignerMgr.GetSSCSigner().Aggregate(msgs)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Interface("requests", requests).Msg("failed to aggregate cxt call request signatures")
		return nil
	}
	sscRequest := &api.CXTCallSSCRequest{
		OriginShardId: request.OriginShardId,
		FromShardId:   request.FromShardId,
		TargetShardId: request.TargetShardId,
		SimulationNum: request.SimulationNum,
		RelatedShards: request.RelatedShards,
		TxHash:        request.TxHash,
		Nonce:         request.Nonce,
		TxSender:      request.TxSender,
		CallIndex:     request.CallIndex,
		Caller:        request.Caller,
		Addr:          request.Addr,
		Input:         request.Input,
		Gas:           request.Gas,
		GasPrice:      request.GasPrice,
		Value:         request.Value,
		BaseBLSSignedMessage: api.BaseBLSSignedMessage{
			ShardId:    s.SelfShard,
			Signatures: aggregatedSig,
			BLSBitMap:  bitMap,
		},
		BlockHash: nil,
	}
	return sscRequest
}

func (s *sscService) HandleCXTCall(req *api.CXTCallSSCRequest) *api.CXTCallResult {
	err := s.signerMgr.GetSSCSigner().Verify(req)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to verify cxt call request signature")
		return &api.CXTCallResult{Err: fmt.Sprintf("error from targetShard %d, err=%s", req.TargetShardId, err.Error())}
	}

	// begin cxt call, update simulationState and callIndex
	callState, err := s.startCall(req)
	if err != nil {
		return &api.CXTCallResult{Err: fmt.Sprintf("error from targetShard %d, err=%s", req.TargetShardId, err.Error())}
	}
	defer s.endCall(req)
	callState.Lock.Lock()
	defer callState.Lock.Unlock()
	txHash := common.BytesToHash(req.TxHash)
	s.stateLock.RLock()
	simuState, err := s.getState(txHash)
	if err != nil {
		s.stateLock.RUnlock()
		return &api.CXTCallResult{Err: "transaction has been closed"}
	}
	relatedShards := simuState.RelatedShards
	s.stateLock.RUnlock()

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Str("callIndex", req.CallIndex.ToString()).Str("addr", req.Addr.Hex()).Msg("handle cxt call, start")
	defer utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msg("handle cxt call, end")

	header := s.bc.GetHeaderByHash(common.BytesToHash(req.BlockHash))
	state, err := s.bc.StateAt(header.Root())
	chainConfig := s.bc.Config()
	vmConfig := s.bc.GetVMConfig()
	sender := vm.AccountRef(req.Caller)
	var executionType vm.ExecutionType
	if req.SimulationNum == 0 {
		executionType = vm.SimulationCall
	} else {
		executionType = vm.SimulationReCall
	}

	s.timerMgr.StartPoolTimer(txHash, header.NumberU64(), s.SelfShard)

	// create sscvm instance
	vmCtx := core.NewSSCVMContext(req.Caller, txHash, req.CallIndex, req.GasPrice, header, s.bc, nil)
	sscvm := vm.NewSSCVM(vmCtx, state, chainConfig, *vmConfig, s, executionType)

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("call contract with %s, callIndex=%s, simulationNum=%d",
		executionType.String(), req.CallIndex.ToString(), req.SimulationNum)
	ret, leftOverGas, err := sscvm.CallFromOtherShard(req.FromShardId, sender, req.Addr, req.Input, req.Gas, req.Value)
	result := &api.CXTCallResult{
		CallIndex:     req.CallIndex,
		RelatedShards: relatedShards,
		Result:        ret,
		LeftOverGas:   leftOverGas,
		BlockHash:     header.Hash().Bytes(),
	}
	if callState.LockedByOtherTx != nil {
		result.Err = api.ErrLockedByOtherTx.Error()
		lockedStates := make(map[api.LockKey]interface{})
		for addr, account := range callState.RWSet.ReadState.State {
			for key, _ := range account {
				lockedStates[api.FormKey(addr, key)] = struct{}{}
			}
		}
		for addr, account := range callState.RWSet.ReadState.State {
			for key, _ := range account {
				lockedStates[api.FormKey(addr, key)] = struct{}{}
			}
		}
		err := s.lockStateMgr.Subscribe(txHash, req.Nonce, common.BytesToAddress(req.TxSender), lockedStates,
			req.OriginShardId, req.SimulationNum+1, true)
		if err != nil {
			return nil
		}
	}
	if err != nil {
		result.Err = fmt.Sprintf("error from targetShard %d, err=%s", req.TargetShardId, err.Error())
		utils.SSCLogger().Error().Err(err).Msg("failed to call contract")
	}
	sign, err := s.signerMgr.GetSSCSigner().Sign(result)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to sign cxt call result")
		return nil
	}
	result.BaseSSCMessage = api.BaseSSCMessage{
		Signature:  sign,
		SenderAddr: s.SelfAddr,
	}
	utils.SSCLogger().Debug().
		Str("txHash", txHash.Hex()).
		Str("callIndex", req.CallIndex.ToString()).
		Interface("ret", ret).
		Msgf("cxt call result: %s, err: %s", common.Bytes2Hex(result.Result), result.Err)
	return result
}

func (s *sscService) startCall(req *api.CXTCallSSCRequest) (*api.SimulationCallState, error) {
	// try to start cxt if not exist
	txHash := common.BytesToHash(req.TxHash)
	_, err := s.startCXT(txHash, req.SimulationNum, req.OriginShardId, req.RelatedShards, nil, req)
	if err != nil {
		return nil, err
	}

	s.stateLock.Lock()
	state, err := s.getState(txHash)
	if err != nil {
		s.stateLock.Unlock()
		return nil, err
	}
	// check if callIndex is already in progress, don't start again
	if state.SimulationCallStates[state.SimulationNum].Get(req.CallIndex) != nil {
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("callIndex has been started, simulationNum=%d, callIndex=%s", state.SimulationNum, req.CallIndex.ToString())
		s.stateLock.Unlock()
		return state.SimulationCallStates[state.SimulationNum].Get(req.CallIndex), nil
	}
	// update new callIndex and clear current index
	if state.CurrentCallFrame != nil {
		state.CallStack.Push(state.CurrentCallFrame)
	}
	state.CurrentCallFrame = &api.CallFrame{
		CallIndex: req.CallIndex,
		PC:        0,
	}
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Str("callIndex", state.CurrentCallFrame.CallIndex.ToString()).Int("currentIndex", state.CurrentCallFrame.PC).
		Str("simulatedCallStates", state.SimulationCallStates[state.SimulationNum].ToString()).
		Msgf("start call, simulationNum=%d, callIndex=%s", state.SimulationNum, req.CallIndex.ToString())
	s.stateLock.Unlock()

	callState := state.SimulationCallStates[state.SimulationNum].Get(req.CallIndex)
	if callState == nil {
		callState = &api.SimulationCallState{
			BlockHash:         common.BytesToHash(req.BlockHash),
			CallIndex:         req.CallIndex,
			CallRequest:       req,
			DependentCXTCalls: make(map[string]*api.DependentCXTCall),
			RWSet:             newRWSet(),
			Result:            nil,
			CallSSCResult:     nil,
			Executed:          false,
			Lock:              sync.Mutex{},
		}
		header := s.bc.GetHeaderByHash(callState.BlockHash)
		// need to sync state
		if header == nil {
			utils.SSCLogger().Debug().Msgf("block header not found, waiting for sync")
			s.waitForSync(callState)
			utils.SSCLogger().Debug().Msgf("sync completed, block header founded")
		}
		s.bc.StateAt(header.Root())
		state.SimulationCallStates[state.SimulationNum] = state.SimulationCallStates[state.SimulationNum].Add(callState)
	}
	return callState, nil
}

func (s *sscService) endCall(req *api.CXTCallSSCRequest) {
	txHash := common.BytesToHash(req.TxHash)
	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	state, _ := s.getState(txHash)
	if state == nil {
		return
	}
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("end call")
	state.CurrentCallFrame = state.CallStack.Pop()
}

func (s *sscService) waitForSync(callState *api.SimulationCallState) {
	callState.SyncedCh = make(chan struct{})

	utils.SSCLogger().Debug().
		Str("txHash", common.Bytes2Hex(callState.CallRequest.TxHash)).
		Str("callIndex", callState.CallIndex.ToString()).
		Str("blockHash", callState.BlockHash.Hex()).Msg("waiting for sync")

	func() {
		s.syncLock.Lock()
		defer s.syncLock.Unlock()
		callStates, exists := s.callStatesInWaiting[callState.BlockHash]
		if !exists {
			callStates = make([]*api.SimulationCallState, 0)
		}
		s.callStatesInWaiting[callState.BlockHash] = append(callStates, callState)
	}()

	select {
	case <-callState.SyncedCh:
		utils.SSCLogger().Debug().Msgf("callState synced %s", callState.CallIndex.ToString())
	case <-s.ctx.Done():
		utils.SSCLogger().Debug().Msg("sscService context done")
	}
}

func (s *sscService) aggregateSSCRecallRequest(requests []*api.CXTRecallRequest) *api.CXTRecallSSCRequest {
	if len(requests) == 0 {
		utils.SSCLogger().Error().Msg("no ssc recall requests to aggregate")
		return nil
	}
	req := requests[0]
	msgs := make([]api.SSCMessage, 0, len(requests))
	for _, msg := range requests {
		msgs = append(msgs, msg)
	}
	sig, bitMap, err := s.signerMgr.GetSSCSigner().Aggregate(msgs)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to aggregate ssc recall request signatures")
		return nil
	}

	return &api.CXTRecallSSCRequest{
		SimulationNum: req.SimulationNum,
		OriginShardId: req.OriginShardId,
		FromShardId:   req.FromShardId,
		TargetShardId: req.TargetShardId,
		RelatedShards: req.RelatedShards,
		TxHash:        req.TxHash,
		CallIndex:     req.CallIndex,
		Caller:        req.Caller,
		Addr:          req.Addr,
		Input:         req.Input,
		Gas:           req.Gas,
		GasPrice:      req.GasPrice,
		Value:         req.Value,
		BaseBLSSignedMessage: api.BaseBLSSignedMessage{
			ShardId:    s.SelfShard,
			Signatures: sig,
			BLSBitMap:  bitMap,
		},
	}
}

func (s *sscService) SignSimulationCommit(commit *api.SimulationCommit) []byte {
	signature, err := s.BLSSignerMgr.GetSSCSigner().Sign(commit)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Str("txHash", common.BytesToHash(commit.TxHash).Hex()).Msgf("failed to sign simulation commit, txHash: %s", common.BytesToHash(commit.TxHash).Hex())
		return nil
	}
	return signature
}

func (s *sscService) SignCXTSimulation(simulation *api.CXTSimulation) []byte {
	signature, err := s.BLSSignerMgr.GetSSCSigner().Sign(simulation)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Str("txHash", common.BytesToHash(simulation.TxHash).Hex()).Msgf("failed to sign simulation commit, txHash: %s", common.BytesToHash(simulation.TxHash).Hex())
		return nil
	}
	return signature
}

// HandleCommitVote
//
//	@Description: handle the vote from self or other shard's ssc member
//	1. only accept the vote from other shard if vote is rollback for invalidSimulation
//	2. waiting for threshold votes, threshold is half of the committee members if vote is recall, otherwise threshold is half of all member in shard
//	3. if threshold votes are received, send the vote to origin-shard's ssc leader
func (s *sscService) HandleCommitVote(vote *api.CXTCommitVote) {

	// 1
	var threshold int
	sscOrValidator := true
	if vote.Type == api.Recall {
		threshold = int(math.Ceil(float64(len(s.GetCommittee(vote.ShardId).Members)) / 2))
		sscOrValidator = true
	} else {
		threshold = int(math.Ceil(float64(len(s.GetValidators(vote.ShardId))) / 2))
		sscOrValidator = false
	}
	if vote.ShardId != s.SelfShard && vote.Type == api.Rollback && vote.Reason == api.ReasonInvalidSimulation {
		utils.SSCLogger().Warn().Str("txHash", common.BytesToHash(vote.TxHash).Hex()).
			Msgf("received invalid simulation rollback vote from shard %d, ignore", vote.OriginShardId)
		return
	}
	// 2
	reachThreshold := false
	var sscVote *api.CXTCommitSSCVote
	txHash := common.BytesToHash(vote.TxHash)

	func() {
		s.commitLock.Lock()
		defer s.commitLock.Unlock()
		if s.commitStates[txHash] == nil {
			s.commitStates[txHash] = &api.CommitState{
				CommitVotes:     make(map[int]map[uint32][]*api.CXTCommitVote),
				CommitSSCVotes:  make(map[int]map[uint32]*api.CXTCommitSSCVote),
				RollbackVotes:   make(map[uint32][]*api.CXTCommitVote),
				RollbackSSCVote: nil,
			}
		}
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("handle commit vote, type=%s", vote.Type)
		if vote.Type == api.Commit {
			if s.commitStates[txHash].CommitVotes[vote.SimulationNum] == nil {
				s.commitStates[txHash].CommitVotes[vote.SimulationNum] = make(map[uint32][]*api.CXTCommitVote)
			}
			if s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId] == nil {
				s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId] = make([]*api.CXTCommitVote, 0)
			}
			if len(s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId]) >= threshold {
				utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("already received enough votes from shard %d, ignore", vote.ShardId)
				return
			}
			s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId] = append(s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId], vote)
			utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("received %s vote from shard %d, [%d/%d]",
				vote.Type.String(), vote.OriginShardId, len(s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId]), threshold)
			reachThreshold = len(s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId]) == threshold
			if reachThreshold {
				utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("reach threshold for commit votes from shard %d, begin to aggregate ssc commit vote", vote.ShardId)
				sscVote = s.aggregateSSCCommitVote(s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId], sscOrValidator)
			}
		}
		if vote.Type == api.Rollback {
			utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("handle rollback vote")
			if s.commitStates[txHash].RollbackVotes[vote.ShardId] == nil {
				s.commitStates[txHash].RollbackVotes[vote.ShardId] = make([]*api.CXTCommitVote, 0)
			}
			if len(s.commitStates[txHash].RollbackVotes[vote.ShardId]) >= threshold {
				utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("already received enough rollback votes from shard %d, ignore", vote.ShardId)
				return
			}
			s.commitStates[txHash].RollbackVotes[vote.ShardId] = append(s.commitStates[txHash].RollbackVotes[vote.ShardId], vote)
			utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("received %s vote from shard %d, [%d/%d]",
				vote.Type.String(), vote.OriginShardId, len(s.commitStates[txHash].RollbackVotes[vote.ShardId]), threshold)
			reachThreshold = len(s.commitStates[txHash].RollbackVotes[vote.ShardId]) == threshold
			if reachThreshold {
				sscVote = s.aggregateSSCCommitVote(s.commitStates[txHash].RollbackVotes[vote.ShardId], sscOrValidator)
			}
		}
	}()

	if reachThreshold {
		// 3
		leader := s.GetLeader(vote.OriginShardId, txHash)

		ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
		defer cancel()

		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("send SSC commit vote to leader: %s, shard=%d, originShard=%d, type=%s", leader.Endpoint, vote.ShardId, vote.OriginShardId, vote.Type.String())
		err := s.Comm.Call(ctx, nil, leader, api.Method_HandleCXTCommitSSCVote, sscVote)
		if err != nil {
			utils.SSCLogger().Error().Err(err).Msg("failed to send commit vote")
			return
		}
	}
}

func (s *sscService) aggregateSSCCommitVote(votes []*api.CXTCommitVote, sscOrValidator bool) *api.CXTCommitSSCVote {
	if len(votes) == 0 {
		utils.SSCLogger().Error().Msg("no ssc commit votes to aggregate")
		return nil
	}
	msgs := make([]api.SSCMessage, 0, len(votes))
	for _, vote := range votes {
		msgs = append(msgs, vote)
	}
	result := votes[0]
	var (
		aggregatedSig []byte
		bitMap        []byte
		err           error
	)
	if sscOrValidator {
		aggregatedSig, bitMap, err = s.BLSSignerMgr.GetSSCSigner().Aggregate(msgs)
	} else {
		aggregatedSig, bitMap, err = s.BLSSignerMgr.GetValidatorSigner().Aggregate(msgs)
	}
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to aggregate ssc commit vote signatures")
		return nil
	}
	sscResult := &api.CXTCommitSSCVote{
		TxHash:        result.TxHash,
		SimulationNum: result.SimulationNum,
		ShardId:       result.ShardId,
		OriginShardId: result.OriginShardId,
		Type:          result.Type,
		Reason:        result.Reason,
		Payload:       result.Payload,
		BaseBLSSignedMessage: api.BaseBLSSignedMessage{
			ShardId:    s.SelfShard,
			Signatures: aggregatedSig,
			BLSBitMap:  bitMap,
		},
	}
	return sscResult
}

// HandleCXTRecallProof
//
//	@Description: handle the recall proof from other shard's ssc member
//	1. start new simulation number
//	2. get the smallest callIndex as lockedCallIndex
func (s *sscService) HandleCXTRecallProof(proof *api.CXTCommitProof) {
	// 1
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	txHash := common.BytesToHash(proof.TxHash)
	simuState, err := s.getState(txHash)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", common.BytesToHash(proof.TxHash).Hex()).Err(err).Msg("handle cxt recall proof failed")
		return
	}
	simuState.SimulationNum = proof.SimulationNum + 1
	if simuState.SimulationCallStates[simuState.SimulationNum] == nil {
		simuState.SimulationCallStates[simuState.SimulationNum] = make(api.SimulationCallStates, 0)
	}

	// 2
	conflictCallIndexes := make([]api.CallIndex, 0)
	for _, vote := range proof.Votes {
		if vote.Type == api.Recall {
			payload := &api.CXTConflictRWSetPayload{}
			_ = json.Unmarshal(vote.Payload, payload)
			conflictCallIndexes = append(conflictCallIndexes, payload.ConflictCallIndex)
		}
	}
	sort.Slice(conflictCallIndexes, func(i, j int) bool {
		return conflictCallIndexes[i].Compare(conflictCallIndexes[j]) < 0
	})
	simuState.LockedCallIndex = conflictCallIndexes[0]
}

func (s *sscService) HandleCXTSSCCall(req *api.CXTCallSSCRequest) *api.CXTCallSSCResult {
	err := s.BLSSignerMgr.GetSSCSigner().Verify(req)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", common.BytesToHash(req.TxHash).String()).Err(err).Msg("invalid cxt ssc call signature")
		return &api.CXTCallSSCResult{Err: err.Error(), BaseBLSSignedMessage: api.BaseBLSSignedMessage{}}
	}

	// select the consistent state
	header := s.bc.CurrentHeader()
	req.BlockHash = header.Hash().Bytes()
	utils.SSCLogger().Debug().Str("txHash", common.BytesToHash(req.TxHash).String()).Msg("handle cxt ssc call, start")
	defer utils.SSCLogger().Debug().Str("txHash", common.BytesToHash(req.TxHash).String()).Msg("handle cxt ssc call, end")
	committee := s.GetCommittee(s.SelfShard)
	t := committee.Threshold
	n := committee.Number
	txHash := common.BytesToHash(req.TxHash)
	_, err = s.startCall(req)
	if err != nil {
		return &api.CXTCallSSCResult{Err: fmt.Sprintf("error from targetShard %d, err=%s", req.TargetShardId, err.Error())}
	}

	s.stateLock.Lock()
	state, err := s.getState(txHash)
	s.stateLock.Unlock()
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).Msg("failed to get state")
		return nil
	}

	ctx, cancel := context.WithTimeout(state.Ctx, s.Config.CallTimeout)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(n)
	lock := sync.Mutex{}
	results := make([]api.SSCMessage, 0, t)

	for _, member := range committee.Members {
		go func() {
			defer wg.Done()
			ret := new(api.CXTCallResult)
			err := s.Comm.Call(ctx, ret, member, api.Method_HandleCXTCall, req)
			if err != nil {
				utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).Msg("failed to call cxt call")
				return
			}
			lock.Lock()
			if len(results) < t {
				results = append(results, ret)
				if len(results) == t {
					cancel()
				}
			}
			lock.Unlock()
		}()
	}
	wg.Wait()
	sscResult, err := s.aggregateCXSSCCallResult(results)
	if err != nil {
		utils.SSCLogger().Error().
			Str("txHash", txHash.String()).Err(err).Msg("failed to aggregate cxt call results")
		return &api.CXTCallSSCResult{
			Err:                  fmt.Sprintf("error from targetShard %d, err=%s", req.TargetShardId, err.Error()),
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{},
		}
	}

	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	state, err = s.getState(txHash)
	if err != nil {
		return &api.CXTCallSSCResult{
			Err:                  fmt.Sprintf("error from targetShard %d, err=%s", req.TargetShardId, err.Error()),
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{},
		}
	}
	if state.SimulationCallStates[req.SimulationNum] == nil {
		return &api.CXTCallSSCResult{
			Err:                  fmt.Sprintf("no related simulation num"),
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{},
		}
	}
	callStates := state.SimulationCallStates[req.SimulationNum]
	if callStates.Get(req.CallIndex) == nil {
		return &api.CXTCallSSCResult{
			Err:                  fmt.Sprintf("no related call index"),
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{},
		}
	}
	callState := callStates.Get(req.CallIndex)
	if callState == nil {
		return &api.CXTCallSSCResult{
			Err:                  fmt.Sprintf("no related call index"),
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{},
		}
	}
	callState.CallSSCResult = sscResult
	return sscResult
}

func (s *sscService) aggregateCXSSCCallResult(results []api.SSCMessage) (*api.CXTCallSSCResult, error) {
	if len(results) == 0 {
		utils.SSCLogger().Error().Msg("no cxt call results to aggregate")
		return nil, errors.New("no cxt call results to aggregate")
	}
	result := results[0].(*api.CXTCallResult)
	aggregatedSig, bitMap, err := s.BLSSignerMgr.GetSSCSigner().Aggregate(results)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to aggregate cxt call results signatures")
		return nil, err
	}
	sscResult := &api.CXTCallSSCResult{
		CallIndex:     result.CallIndex,
		RelatedShards: result.RelatedShards,
		Result:        result.Result,
		LeftOverGas:   result.LeftOverGas,
		BlockHash:     result.BlockHash,
		Err:           result.Err,
		BaseBLSSignedMessage: api.BaseBLSSignedMessage{
			ShardId:    s.SelfShard,
			Signatures: aggregatedSig,
			BLSBitMap:  bitMap,
		},
	}
	return sscResult, nil
}

func (s *sscService) CommitSimulation(commit *api.SimulationCommit) {
	txHash := common.BytesToHash(commit.TxHash)
	switch commit.Status {
	case api.OK:
		utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msgf("simulation committed, status=%s", commit.Status.String())
	case api.LockConflict:
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msgf("simulation failed, status=%s", commit.Status.String())
		return
	case api.ExecutionFailed:
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msgf("simulation failed, status=%s", commit.Status.String())
		s.closeTransaction(txHash, false)
		return
	case api.PoolTimeout: // TODO deprecated, just close transaction if pool timeout
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msgf("simulation is pool timeout, status=%s", commit.Status.String())
		s.closeTransaction(txHash, false)
		return
	}

	s.stateLock.Lock()
	simuState, err := s.getState(txHash)
	if err != nil {
		s.stateLock.Unlock()
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).Msg("commit simulation failed")
		return
	}
	// update related shards
	simuState.RelatedShards = commit.RelatedShards
	simulationCallStates := simuState.SimulationCallStates[commit.SimulationNum]
	s.stateLock.Unlock()

	// build commit simulation
	callStates := make([]*api.CXTCallState, 0)
	for i, simulationCallState := range simulationCallStates {
		depentResults := make([]*api.CXTCallSSCResult, 0)
		for _, dc := range simulationCallState.DependentCXTCalls {
			depentResults = append(depentResults, dc.SSCResult)
		}
		sort.Slice(depentResults, func(i, j int) bool {
			return depentResults[i].CallIndex.Compare(depentResults[j].CallIndex) < 0
		})
		callState := &api.CXTCallState{
			CallIndex:        simulationCallState.CallIndex,
			TopRequest:       simulationCallState.TopRequest,
			CallRequest:      simulationCallState.CallRequest,
			RWSet:            simulationCallState.RWSet,
			DependentResults: depentResults,
			CallResult:       simulationCallState.CallSSCResult,
			TopResult:        simulationCallState.TopSSCResult,
		}
		callStates = append(callStates, callState)
		if callState.TopResult == nil && callState.CallResult == nil {
			utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msgf("callIndex %s has no result", simulationCallState.CallIndex.ToString())
		}
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("build commit simulation [%d/%d], callIndex: %s, rwset: %v", i+1, len(simulationCallStates), simulationCallState.CallIndex.ToString(), simulationCallState.RWSet.WriteState.State)

	}

	simulation := &api.CXTSimulation{
		SimulationNum:        commit.SimulationNum,
		TxHash:               commit.TxHash,
		Nonce:                commit.Nonce,
		Sender:               commit.Sender,
		ShardId:              commit.ShardId,
		OriginShardId:        simuState.OriginShardId,
		RelatedShards:        commit.RelatedShards,
		CallStates:           callStates,
		BaseBLSSignedMessage: api.BaseBLSSignedMessage{},
	}

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("build commit simulation completed")

	s.buildSignaturesForSimulation(simuState, simulation)
	err = s.txSubmitter.SubmitSimulationTx(simulation)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to submit simulation tx")
		return
	}
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("simulation committed")
}

func (s *sscService) buildSignaturesForSimulation(state *api.CXTSimulationState, simulation *api.CXTSimulation) {
	txHash := common.BytesToHash(simulation.TxHash)
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("begin to build signatures for simulation")
	committee := s.GetCommittee(s.SelfShard)
	n := committee.Number
	t := committee.Threshold

	ctx, cancel := context.WithTimeout(state.Ctx, s.Config.CallTimeout)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(n)
	baseMsgs := make([]api.SSCMessage, 0, t)
	lock := sync.Mutex{}

	for _, member := range committee.Members {
		go func() {
			defer wg.Done()
			signature := make([]byte, 0)
			err := s.Comm.Call(ctx, &signature, member, api.Method_SignCXTSimulation, simulation)
			if err != nil {
				return
			}
			lock.Lock()
			if len(baseMsgs) < t {
				baseMsgs = append(baseMsgs, api.BaseSSCMessage{
					Signature:  signature,
					SenderAddr: member.Address,
				})
				if len(baseMsgs) == t {
					cancel()
				}
			}
			lock.Unlock()
		}()
	}

	wg.Wait()
	aggregatedSig, bitMap, err := s.BLSSignerMgr.GetSSCSigner().Aggregate(baseMsgs)
	if err != nil {
		utils.SSCLogger().Error().Err(err)
		return
	}
	simulation.Signatures = aggregatedSig
	simulation.BLSBitMap = bitMap
}

// HandleCXTCommitSSCVote
// @Description: handle the vote from other shard's ssc leader
// 1. collect all the related shards for cross-shard transaction
// 2. if one vote is rollback, send rollback commit proof to all shards
// 3. if one vote is recall, broadcast recall commit proof and begin to recall the simulation
// 4. if all votes are commit, send commit proof to all shards
func (s *sscService) HandleCXTCommitSSCVote(vote *api.CXTCommitSSCVote) {
	txHash := common.BytesToHash(vote.TxHash)
	s.stateLock.RLock()
	state, err := s.getState(txHash)
	if err != nil {
		s.stateLock.RUnlock()
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).Msgf("handle commit ssc vote failed")
		return
	}
	relatedShards := state.RelatedShards
	s.stateLock.RUnlock()

	if !relatedShards.Contains(vote.ShardId) {
		utils.SSCLogger().Error().Msgf("shard %d is not related to tx %s", vote.ShardId, txHash.String())
		return
	}
	var sscVotes map[uint32]*api.CXTCommitSSCVote

	s.commitLock.RLock()
	commitState := s.commitStates[txHash]
	if vote.Type == api.Commit {
		if commitState.CommitSSCVotes[vote.SimulationNum] == nil {
			commitState.CommitSSCVotes[vote.SimulationNum] = make(map[uint32]*api.CXTCommitSSCVote)
		}
		commitState.CommitSSCVotes[vote.SimulationNum][vote.ShardId] = vote
		sscVotes = commitState.CommitSSCVotes[vote.SimulationNum]
	}
	if vote.Type == api.Rollback {
		commitState.RollbackSSCVote = vote
		sscVotes = map[uint32]*api.CXTCommitSSCVote{
			vote.ShardId: vote,
		}
	}
	s.commitLock.RUnlock()

	s.SignalReSimulation(&api.ReSimulationSignal{
		TxHash:           vote.TxHash,
		ShardId:          vote.ShardId,
		OriginShardId:    vote.OriginShardId,
		SimulationNum:    vote.SimulationNum + 1,
		Ready:            true,
		NeedResimulate:   false,
		SimulateOrVerify: false,
	})

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("received %s ssc vote [%d/%d], %d in %v, simulationNum=%d",
		vote.Type, len(sscVotes), len(relatedShards), vote.ShardId, relatedShards, vote.SimulationNum)

	if vote.Type == api.Rollback || len(sscVotes) == len(relatedShards) {
		commitType := api.Commit
		commitReason := api.ReasonSuccess
		sscVoteList := make([]*api.CXTCommitSSCVote, 0)
		recallShards := make([]uint32, 0)
		for _, v := range sscVotes {
			if v.Type == api.Rollback {
				commitType = api.Rollback
				commitReason = v.Reason
				sscVoteList = append(make([]*api.CXTCommitSSCVote, 0), v)
				break
			}
			if v.Type == api.Recall && commitType != api.Rollback {
				commitType = api.Recall
				commitReason = v.Reason
				recallShards = append(recallShards, v.ShardId)
			}
			sscVoteList = append(sscVoteList, v)
		}
		sort.Slice(sscVoteList, func(i, j int) bool {
			return sscVoteList[i].ShardId < sscVoteList[j].ShardId
		})

		ctx, cancel := context.WithTimeout(state.Ctx, s.Config.CallTimeout)
		defer cancel()
		members := make([]*api.Member, 0, len(relatedShards))
		for _, shardId := range relatedShards {
			members = append(members, s.GetLeader(shardId, txHash))
		}

		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("receive all related ssc votes, commit type: %v, reason: %v, simulationNum: %d, relatedShards: %v",
			commitType, commitReason, vote.SimulationNum, relatedShards)

		switch commitType {
		case api.Commit:
			proof := &api.CXTCommitProof{
				TxHash:        txHash.Bytes(),
				SimulationNum: vote.SimulationNum,
				Type:          commitType,
				Reason:        commitReason,
				Votes:         sscVoteList,
				OriginShard:   vote.OriginShardId,
				RelatedShards: relatedShards,
			}
			_ = s.Comm.Multicast(ctx, members, api.Method_HandleCXTCommitProof, proof)
		case api.Rollback:
			proof := &api.CXTCommitProof{
				TxHash:        txHash.Bytes(),
				SimulationNum: vote.SimulationNum,
				Type:          commitType,
				Reason:        commitReason,
				Votes:         sscVoteList,
				OriginShard:   vote.OriginShardId,
				RelatedShards: relatedShards,
			}
			_ = s.Comm.Multicast(ctx, members, api.Method_HandleCXTCommitProof, proof)
		case api.Recall:
			proof := &api.CXTCommitProof{
				TxHash:        txHash.Bytes(),
				SimulationNum: vote.SimulationNum,
				Type:          commitType,
				Reason:        commitReason,
				Votes:         sscVoteList,
				OriginShard:   vote.OriginShardId,
				RelatedShards: relatedShards,
			}
			_ = s.Comm.Multicast(ctx, members, api.Method_HandleCXTCommitProof, proof)
			go func() {
				s.startReSimulation(proof.TxHash, proof.SimulationNum+1)
			}()
		default:
			utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msgf("unsupportted vote type: %s", vote.Type)
		}
	}
}

func (s *sscService) sendReSimulationSignal(signal *api.ReSimulationSignal) error {
	leader := s.GetLeader(signal.OriginShardId, common.BytesToHash(signal.TxHash))

	s.stateLock.RLock()
	state, err := s.getState(common.BytesToHash(signal.TxHash))
	s.stateLock.RUnlock()
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", common.BytesToHash(signal.TxHash).Hex()).Msg("transaction has been closed")
		return err
	}

	ctx, cancel := context.WithTimeout(state.Ctx, s.Config.CallTimeout)
	defer cancel()

	err = s.Comm.Call(ctx, nil, leader, api.Method_SignalReSimulation, signal)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", common.BytesToHash(signal.TxHash).Hex()).Msg("send signal resimulation failed")
		return err
	}
	return nil
}

func (s *sscService) SignalReSimulation(signal *api.ReSimulationSignal) {
	txHash := common.BytesToHash(signal.TxHash)
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("handle signal resimulation, shardId: %d, OriginShardId: %d, simulationNum: %d, SimulateOrVerify: %v, needResimulate: %v",
		signal.ShardId, signal.OriginShardId, signal.SimulationNum, signal.SimulateOrVerify, signal.NeedResimulate)

	s.stateLock.RLock()
	defer s.stateLock.RUnlock()
	state, err := s.getState(txHash)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msg("transaction has been closed")
		return
	}
	if signal.SimulateOrVerify {
		// if it's a simulate resimulation signal, just call resimulation
		go func() {
			utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("start recall simulation directly for simulate, send notify to shard %d", signal.OriginShardId)
			leader := s.GetLeader(signal.ShardId, txHash)

			ctx, cancel := context.WithTimeout(state.Ctx, s.Config.CallTimeout)
			defer cancel()

			err := s.Comm.Call(ctx, nil, leader, api.Method_NotifyReSimulationStart, txHash)
			if err != nil {
				utils.SSCLogger().Error().Err(err).Msg("failed to notify re-simulation start")
				return
			}
			s.startReSimulation(signal.TxHash, signal.SimulationNum)
		}()
	} else {
		// if it's a verify resimulation signal, we need collect all related shard's signals or sscVotes
		if state.ReSimulationSignals[signal.SimulationNum] == nil {
			state.ReSimulationSignals[signal.SimulationNum] = make(map[uint32]*api.ReSimulationSignal)
		}
		state.ReSimulationSignals[signal.SimulationNum][signal.ShardId] = signal
		if signal.NeedResimulate {
			utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("try to resimulation for verify conflict, collected %d/%d: %d in %v resimulation signals, simulationNum %d",
				len(state.ReSimulationSignals[signal.SimulationNum]), len(state.RelatedShards), signal.ShardId, state.RelatedShards, signal.SimulationNum)
		}
		if len(state.ReSimulationSignals[signal.SimulationNum]) == len(state.RelatedShards) {
			needResimulate := false
			for _, s := range state.ReSimulationSignals[signal.SimulationNum] {
				if s.NeedResimulate {
					needResimulate = true
					break
				}
			}
			if needResimulate {
				go func() {
					leaders := make([]*api.Member, 0)
					shards := make([]uint32, 0)
					for shardId, simulationSignal := range state.ReSimulationSignals[signal.SimulationNum] {
						if simulationSignal.NeedResimulate {
							leaders = append(leaders, s.GetLeader(shardId, txHash))
							shards = append(shards, shardId)
						}
					}
					utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("start re-simulation for verify, send notify to %v", shards)

					ctx, cancel := context.WithTimeout(state.Ctx, s.Config.CallTimeout)
					defer cancel()

					err := s.Comm.Multicast(ctx, leaders, api.Method_NotifyReSimulationStart, txHash)
					if err != nil {
						utils.SSCLogger().Error().Err(err).Msg("failed to notify re-simulation start")
						return
					}

					s.startReSimulation(signal.TxHash, signal.SimulationNum)
				}()
			}
		}
	}
}

func (s *sscService) startReSimulation(txHash []byte, simulationNum int) {
	s.stateLock.Lock()
	state, err := s.getState(common.BytesToHash(txHash))
	s.stateLock.Unlock()
	if err != nil {
		utils.SSCLogger().Error().Err(err).Str("txHash", common.BytesToHash(txHash).Hex()).Msg("resimulation failed")
		return
	}

	ctx, cancel := context.WithTimeout(state.Ctx, s.Config.CallTimeout)
	defer cancel()

	header := s.bc.CurrentHeader()
	// request recall origin contract
	lastReq := state.SimulationRequest
	if lastReq == nil {
		utils.SSCLogger().Error().Str("txHash", common.BytesToHash(txHash).Hex()).Msg("failed to get last simulation request")
		return
	}
	req := &api.CXTSimulationRequest{
		TxHash:        txHash,
		SimulationNum: simulationNum,
		Author:        lastReq.Author,
		BlockHash:     header.Hash().Bytes(),
		Tx:            lastReq.Tx,
		From:          lastReq.From,
		GasPool:       0,
	}

	utils.SSCLogger().Debug().Str("txHash", common.BytesToHash(txHash).Hex()).Msgf("start recall simulation, simulationNum: %d", simulationNum)

	committee := s.GetCommittee(s.SelfShard)
	n := committee.Number
	t := committee.Threshold
	wg := sync.WaitGroup{}
	wg.Add(n)
	lock := sync.Mutex{}
	results := make([]api.SSCMessage, 0, t)

	for _, member := range committee.Members {
		go func() {
			defer wg.Done()
			ret := new(api.CXTSimulationResult)
			err := s.Comm.Call(ctx, ret, member, api.Method_HandleSimulateRequest, req)
			if err != nil {
				utils.SSCLogger().Error().Str("txHash", common.BytesToHash(txHash).String()).Err(err).Msg("failed to call simulate request")
				return
			}
			lock.Lock()
			if len(results) < t {
				results = append(results, ret)
				if len(results) == t {
					cancel()
				}
			}
			lock.Unlock()
		}()
	}
	wg.Wait()
	sscResult, err := s.aggregateSimulationResults(results)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", common.BytesToHash(txHash).String()).Err(err).Msg("failed to aggregate simulation results")
		sscResult = &api.CXTSimulationSSCResult{Err: err.Error(), RelatedShards: []uint32{s.SelfShard}}
	} else {
		s.stateLock.Lock()
		state.SimulationResult = sscResult
		state.SimulationCallStates[simulationNum][0].TopSSCResult = sscResult
		s.stateLock.Unlock()
	}

	// build and send simulation commit
	simulationCommit := &api.SimulationCommit{
		SimulationNum:        req.SimulationNum,
		TxHash:               txHash,
		Nonce:                req.Tx.Nonce(),
		Sender:               req.From.Bytes(),
		RelatedShards:        sscResult.RelatedShards,
		BaseBLSSignedMessage: api.BaseBLSSignedMessage{},
	}

	if len(sscResult.Err) == 0 {
		simulationCommit.Commit = true
		simulationCommit.Status = api.OK
		utils.SSCLogger().Debug().Str("txHash", common.BytesToHash(simulationCommit.TxHash).Hex()).
			Msgf("resimulation accomplished, send simulation commit, commit type: %v, simulationNum: %d, relatedShards: %v",
				simulationCommit.Commit, simulationCommit.SimulationNum, simulationCommit.RelatedShards)
	} else {
		if vm.IsLockedByOtherTxErr(sscResult.Err) {
			utils.SSCLogger().Debug().Str("txHash", common.BytesToHash(txHash).Hex()).Msgf("resimulation failed err=%s, it has subscribe to resimulate again, nextSimulationNum=%d", sscResult.Err, req.SimulationNum+1)
			simulationCommit.Commit = false
			simulationCommit.Status = api.LockConflict
			return
		} else {
			simulationCommit.Commit = false
			simulationCommit.Status = api.ExecutionFailed
		}
		utils.SSCLogger().Error().Str("txHash", common.BytesToHash(simulationCommit.TxHash).Hex()).
			Err(errors.New(sscResult.Err)).Msgf("resimulation accomplished, send simulation commit, commit type: %v, status: %v, simulationNum: %d, relatedShards: %v",
			simulationCommit.Commit, simulationCommit.Status.String(), simulationCommit.SimulationNum, simulationCommit.RelatedShards)
	}

	s.thresholdSignSimulationCommit(simulationCommit)
	members := make([]*api.Member, 0, len(committee.Members))
	for _, shardId := range simulationCommit.RelatedShards {
		members = append(members, s.GetLeader(shardId, common.BytesToHash(simulationCommit.TxHash)))
	}
	_ = s.Comm.Multicast(ctx, members, api.Method_CommitSimulation, simulationCommit)
}

func (s *sscService) NotifyReSimulationStart(txHash common.Hash) {
	s.lockStateMgr.NotifyReSimulationStart(txHash)
}

func (s *sscService) HandleCXTCommitProof(proof *api.CXTCommitProof) {
	utils.SSCLogger().Debug().Str("txHash", common.BytesToHash(proof.TxHash).Hex()).Msgf("handle commit or rollback proof, type: %v, simulationNum: %d", proof.Type, proof.SimulationNum)
	// if proof is commit or rollback, send commit proof transaction
	if proof.Type == api.Commit || proof.Type == api.Rollback {
		err := s.txSubmitter.SubmitCommitOrRollbackTx(proof)
		if err != nil {
			utils.SSCLogger().Error().Err(err).Msg("failed to submit commit or rollback tx")
			return
		}
		if proof.TxHash[0] == 0 {
			s.txSubmitter.SubmitCommitOrRollbackTx(proof)
		}
	}

	if proof.Type == api.Recall {
		committee := s.GetCommittee(s.SelfShard)
		_ = s.Comm.Multicast(s.ctx, committee.Members, api.Method_HandleCXTRecallProof, proof)
	}
}

func (s *sscService) getState(txHash common.Hash) (*api.CXTSimulationState, error) {
	if state, exists := s.simulationState[txHash]; exists {
		return state, nil
	}
	if _, exists := s.finishedTxs[txHash]; exists {
		return nil, api.ErrTxHasBeenClosed
	}
	return nil, api.ErrTxNotExist
}

func (s *sscService) closeTransaction(txHash common.Hash, commitOrRollback bool) {
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msgf("close transaction, commit: %v", commitOrRollback)
	s.stateLock.Lock()
	state, _ := s.getState(txHash)
	if state != nil {
		state.CtxCancel()
	}
	delete(s.simulationState, txHash)
	delete(s.executionVerifyContexts, txHash)
	s.finishedTxs[txHash] = commitOrRollback
	s.stateLock.Unlock()
	s.commitLock.Lock()
	delete(s.commitStates, txHash)
	s.commitLock.Unlock()
	s.lockStateMgr.UnSubscribe(txHash)
}

func (s *sscService) closeTransactions(txs map[common.Hash]bool) {
	utils.SSCLogger().Info().Interface("txs", txs).Msgf("close transactions, num=%d", len(txs))
	s.stateLock.Lock()
	for txHash, commitOrRollback := range txs {
		state, _ := s.getState(txHash)
		if state != nil {
			state.CtxCancel()
		}
		delete(s.executionVerifyContexts, txHash)
		delete(s.simulationState, txHash)
		s.finishedTxs[txHash] = commitOrRollback
	}
	s.stateLock.Unlock()
	s.commitLock.Lock()
	for txHash, _ := range txs {
		delete(s.commitStates, txHash)
	}
	s.commitLock.Unlock()
	for txHash, _ := range txs {
		s.lockStateMgr.UnSubscribe(txHash)
	}
}

func (s *sscService) StateLockManager() api.StateLockManager {
	return s.lockStateMgr
}
