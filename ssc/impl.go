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

type pendingCXTRequest struct {
	req       *api.CXTCallRequest
	waitingCh chan *api.CXTCallSSCResult
}

type sscService struct {
	*CommitteeMechanism
	Comm           *Comm
	BLSSignerMgr   api.BLSSignerMgr
	txSigner       api.TxSigner
	txSubmitter    api.TxSubmitter
	lockStateMgr   api.StateLockManager
	tempLockView   *TempLockView
	timerMgr       *CXTTimerManager
	retryScheduler *retryScheduler

	Config *api.Config
	bc     core.BlockChain

	stateLock       lm.RWMutex // cmLock for simulationState, executionVerifyContexts, finishedTxs
	simulationState map[common.Hash]*api.CXTSimulationState
	finishedTxs     map[common.Hash]bool

	verifyCtxLock           lm.RWMutex
	executionVerifyContexts map[common.Hash]*api.ExecutionVerifyContext // TxHash -> ExecutionVerifyContext

	commitLock   lm.RWMutex // cmLock for commitStates
	commitStates map[common.Hash]*api.CommitState

	syncLock            lm.Mutex // cmLock for callStatesInWaiting
	callStatesInWaiting map[common.Hash][]*api.SimulationCallState

	simuLock       lm.Mutex // cmLock for simuWaitingChs, simuResultCh
	simuWaitingChs map[common.Hash][]chan *api.CXTSimulationSSCResult
	simuResultCh   map[common.Hash]chan *api.CXTSimulationSSCResult // used to notify simulation finished

	pendingLock     sync.Mutex
	pendingRequests map[common.Hash][]*pendingCXTRequest // use to store the pending requests

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
	signerMgr api.BLSSignerMgr, nodeAPI hmy.NodeAPI, bc core.BlockChain, txSigner api.TxSigner, comm *Comm, txSub api.TxSubmitter) api.Service {
	service := &sscService{
		CommitteeMechanism:      cm,
		Comm:                    comm,
		BLSSignerMgr:            signerMgr,
		txSigner:                txSigner,
		Config:                  config,
		txSubmitter:             txSub,
		bc:                      bc,
		stateLock:               lm.NewRWMutex(),
		verifyCtxLock:           lm.NewRWMutex(),
		commitLock:              lm.NewRWMutex(),
		simuLock:                lm.NewMutex(),
		syncLock:                lm.NewMutex(),
		simulationState:         make(map[common.Hash]*api.CXTSimulationState),
		commitStates:            make(map[common.Hash]*api.CommitState),
		executionVerifyContexts: make(map[common.Hash]*api.ExecutionVerifyContext),
		callStatesInWaiting:     make(map[common.Hash][]*api.SimulationCallState),
		finishedTxs:             make(map[common.Hash]bool),
		simuWaitingChs:          make(map[common.Hash][]chan *api.CXTSimulationSSCResult),
		simuResultCh:            make(map[common.Hash]chan *api.CXTSimulationSSCResult),
		pendingRequests:         make(map[common.Hash][]*pendingCXTRequest),
		pendingLock:             sync.Mutex{},
		ctx:                     ctx,
		chainHeadCh:             make(chan core.ChainHeadEvent, 10),
		chainHeadSub:            nil,
	}
	lockStateMgr := newStateLockManager(service)
	service.lockStateMgr = lockStateMgr
	service.tempLockView = lockStateMgr.GetTempLockView()
	service.timerMgr = NewTimerManager(sscConfig.Timeout, service)
	service.retryScheduler = NewRetryScheduler(ctx, service, comm, service.SelfShard, service.tempLockView)
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
		utils.SSCLogger().Error().Err(err).Msg("failed to handle block committed")
		return err
	}
	s.retryScheduler.OnBlockCommitted(block)
	return nil
}

func (s *sscService) loop() {
	for {
		select {
		case <-s.ctx.Done():
			utils.SSCLogger().Info().Msg("ssc service stop...")
			return
		case <-s.chainHeadSub.Err():
			utils.SSCLogger().Error().Msg("chain head subscription error")
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
	txHash := req.TxHash
	_, err := s.startCXT(txHash, req.SimulationNum, s.SelfShard, make([]uint32, 0), req, nil)
	if err != nil {
		return nil, err
	}

	utils.SSCLogger().Debug().
		Str("txHash", txHash.Hex()).
		Msg("start simulation 1")

	callState := &api.SimulationCallState{
		BlockHash:         req.BlockHash,
		CallIndex:         api.CallIndex{},
		TopRequest:        req,
		DependentCXTCalls: make(map[string]*api.DependentCXTCall),
		RWSet:             newRWSet(),
		Result:            nil,
		CallSSCResult:     nil,
		Executed:          false,
		Lock:              sync.Mutex{},
	}

	utils.SSCLogger().Debug().
		Str("txHash", txHash.Hex()).
		Msg("start simulation 1.1")

	header := s.bc.GetHeaderByHash(callState.BlockHash)
	if header == nil {
		utils.SSCLogger().Debug().
			Str("txHash", txHash.Hex()).
			Msg("start simulation 1.2")
		s.waitForSync(callState)
		utils.SSCLogger().Debug().
			Str("txHash", txHash.Hex()).
			Msg("start simulation 1.2.1")
		header = s.bc.GetHeaderByHash(callState.BlockHash)
		utils.SSCLogger().Debug().
			Str("txHash", txHash.Hex()).
			Msg("start simulation 1.3")
		if header == nil {

			utils.SSCLogger().Debug().
				Str("txHash", txHash.Hex()).
				Msg("start simulation 1.4")
			utils.SSCLogger().Error().Msg("failed to get block header")
			return nil, errors.New("failed to get block header")
		}
	}

	utils.SSCLogger().Debug().
		Str("txHash", txHash.Hex()).
		Msg("start simulation 3")

	db, err := s.bc.StateAt(header.Root())
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to get state")
		return nil, err
	}
	callState.DB = db

	utils.SSCLogger().Debug().
		Str("txHash", txHash.Hex()).
		Msg("start simulation 2")

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

	func() {
		s.stateLock.Lock()
		defer s.stateLock.Unlock()
		state, _ := s.getState(txHash)
		state.CurrentCallFrame = &api.CallFrame{
			CallIndex: api.CallIndex{},
			PC:        0,
		}
		state.SimulationCallStates[req.SimulationNum] = state.SimulationCallStates[req.SimulationNum].Add(callState)
		s.pendingLock.Lock()
		pendingList := s.pendingRequests[txHash]
		s.pendingRequests[txHash] = make([]*pendingCXTRequest, 0)
		if len(pendingList) > 0 {
			// 异步处理，避免阻塞当前 goroutine 和锁
			for _, p := range pendingList {
				utils.SSCLogger().Info().
					Str("txHash", txHash.Hex()).
					Str("callIndex", p.req.CallIndex.ToString()).
					Msg("found pending CXT request")
				if p.req.CallIndex[:len(p.req.CallIndex)-1].ToString() == callState.CallIndex.ToString() {
					utils.SSCLogger().Info().
						Str("txHash", txHash.Hex()).
						Str("callIndex", p.req.CallIndex.ToString()).
						Msg("processing pending CXT request")
					go func(req *api.CXTCallRequest, ch chan *api.CXTCallSSCResult) {
						result := s.RequestCallCXT(req) // 递归调用，此时状态已存在
						utils.SSCLogger().Info().
							Str("txHash", txHash.Hex()).
							Str("callIndex", req.CallIndex.ToString()).
							Msg("finished processing pending CXT request")
						ch <- result
					}(p.req, p.waitingCh)
				} else {
					s.pendingRequests[txHash] = append(s.pendingRequests[txHash], p)
				}
			}
		}
		s.pendingLock.Unlock()
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
		CallForest:           api.NewCallForest(),
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
		newState.RelatedShards = newState.RelatedShards.Merge(state.RelatedShards)
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
		Msgf("startCXT for tx %s, simulationNum %d, FromShard %d, relatedShards %v, oldRelatedShards %v",
			txHash.Hex(), simulationNum, originShardId, newState.RelatedShards, relatedShards)

	return true, nil
}

func (s *sscService) handleTxSp1Timeout(info txInfo) {
	utils.SSCLogger().Debug().Str("txHash", info.txHash.Hex()).Msgf("cxt has timeout for sp1, send rollback vote to ssc leader")
	payload := &api.CXTInvalidSimulationPayload{
		Type: api.CXTTimeout,
	}
	payloadBytes, _ := json.Marshal(payload)
	vote := &api.CXTCommitVote{
		BaseSSCMessage: api.BaseSSCMessage{
			Epoch: info.epoch,
		},
		TxHash:        info.txHash,
		ShardId:       s.SelfShard,
		OriginShardId: s.SelfShard,
		Type:          api.Rollback,
		Reason:        api.ReasonCxtTimeoutForSp1,
		Payload:       payloadBytes,
	}
	s.sendCXTCommitVote(s.SelfShard, vote)
}

// block committed will be called after apply the block, so we can close the transaction here after check if the tx's simulation is not onchain
func (s *sscService) handleTxPoolTimeout(info txInfo) {
	txHash := info.txHash
	s.verifyCtxLock.RLock()
	_, existsOnChain := s.executionVerifyContexts[txHash]
	s.verifyCtxLock.RUnlock()
	s.stateLock.RLock()
	_, err := s.getState(txHash)
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
		Msgf("simulation is pool timeout, status=PoolTimeout, timeout for [%d, %d], current=%d, epoch=%d", info.blockNum, info.poolTimeout, info.poolTimeout, info.blockNum)
	s.closeTransaction(txHash, false)
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
	req.Epoch = s.CurrentEpoch
	leader := s.GetLeader(req.Epoch, s.SelfShard)

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
	txHash := req.TxHash
	committee := s.GetCommittee(req.Epoch, s.SelfShard)
	targetShardId := req.TargetShardId
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

		req.Nonce = cxtState.Nonce
		req.TxSender = cxtState.TxSender.Bytes()
		req.SimulationNum = cxtState.SimulationNum
		req.OriginShardId = cxtState.OriginShardId
		req.FromShardId = s.SelfShard
		req.CallIndex = make(api.CallIndex, len(cxtState.CurrentCallFrame.CallIndex))
		copy(req.CallIndex, cxtState.CurrentCallFrame.CallIndex)
		req.CallIndex = append(req.CallIndex, cxtState.CurrentCallFrame.PC)
		req.RelatedShards = cxtState.RelatedShards.Add(req.TargetShardId)
		cxtState.CurrentCallFrame.Next()

		// if it's recall and callState is locked
		if cxtState.SimulationNum > 0 && req.CallIndex.Compare(cxtState.LockedCallIndex) <= 0 {
			utils.SSCLogger().Debug().Str("txHash", txHash.String()).
				Str("callIndex", req.CallIndex.ToString()).
				Msg("recall contract has been locked, reuse it")
			callStates := cxtState.SimulationCallStates[req.SimulationNum]
			callerState := callStates.Get(req.CallIndex[:len(req.CallIndex)-1])
			lockedCallStates := cxtState.SimulationCallStates[cxtState.SimulationNum-1]
			dependentCall = lockedCallStates.Get(req.CallIndex).DependentCXTCalls[req.CallIndex.ToString()]
			callerState.DependentCXTCalls[req.CallIndex.ToString()] = dependentCall
			ret := callerState.DependentCXTCalls[req.CallIndex.ToString()].SSCResult
			return ret
		}
		callStates := cxtState.SimulationCallStates[req.SimulationNum]
		// get caller's call state
		callerState := callStates.Get(req.CallIndex[:len(req.CallIndex)-1])
		if callerState == nil {
			utils.SSCLogger().Debug().Str("txHash", txHash.String()).
				Interface("cxtState", cxtState).
				Interface("callStates", callStates).
				Str("callIndex", req.CallIndex.ToString()).
				Interface("cxtState.callerState", cxtState.SimulationCallStates[req.SimulationNum].Get(req.CallIndex[:len(req.CallIndex)-1])).
				Msg("no caller's call state found")
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
		if cxtState != nil {
			subNode := new(api.CallNode)
			subNode.FromData(sscResult.TreeNode)
			cxtState.CallForest.Insert(subNode)
		}
		return sscResult
	}

	sign, err := s.signerMgr.GetSSCSigner().Sign(req)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.String()).
			Str("callIndex", req.CallIndex.ToString()).Err(err).Msg("failed to sign cxt call ssc request")
		return nil
	}
	req.BaseSSCMessage.SenderAddr = s.SelfAddr
	req.BaseSSCMessage.Signature = sign

	leader := s.GetLeader(req.Epoch, s.SelfShard)
	utils.SSCLogger().Debug().
		Str("txHash", txHash.String()).
		Interface("CurrentCallFrame", cxtState.CurrentCallFrame).
		Str("reqCallIndex", req.CallIndex.ToString()).
		Interface("relatedShards", req.RelatedShards).
		Uint32("selfShard", s.SelfShard).
		Uint32("targetShard", targetShardId).
		Str("addr", req.Addr.Hex()).
		Str("callIndex", req.CallIndex.ToString()).Msg("call cxt contract, start")

	ctx, cancel := context.WithTimeout(cxtState.Ctx, s.Config.CallTimeout)
	defer cancel()
	ret := new(api.CXTCallSSCResult)
	err = s.Comm.Call(ctx, ret, leader, api.Method_RequestCallCXT, req)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to call cxt contract")
		utils.SSCLogger().Debug().Str("txHash", txHash.String()).
			Str("callIndex", req.CallIndex.ToString()).
			Uint32("targetShard", targetShardId).
			Str("relatedShards", fmt.Sprintf("%v", cxtState.RelatedShards)).
			Interface("result", ret).
			Msg("call cxt contract, end")
		ret = &api.CXTCallSSCResult{Err: fmt.Sprintf("error from targetShard %d, err=%s", targetShardId, err.Error())}
		dependentCall.SSCResult = ret
	} else {
		if ret.Err != "" {
			utils.SSCLogger().Error().Str("txHash", txHash.String()).
				Str("callIndex", req.CallIndex.ToString()).Err(errors.New(ret.Err)).Msgf("failed to call cxt contract, targetShardId: %d", req.TargetShardId)
		} else {
			err = s.signerMgr.GetSSCSigner().Verify(ret)
			if err != nil {
				utils.SSCLogger().Error().Str("txHash", txHash.String()).
					Str("callIndex", req.CallIndex.ToString()).Err(err).Msg("failed to verify cxt call ssc result signature")
				utils.SSCLogger().Debug().Str("txHash", txHash.String()).
					Str("callIndex", req.CallIndex.ToString()).
					Uint32("targetShard", targetShardId).
					Str("relatedShards", fmt.Sprintf("%v", cxtState.RelatedShards)).
					Interface("result", ret).
					Msg("call cxt contract, end")
				ret = &api.CXTCallSSCResult{Err: fmt.Sprintf("error from targetShard %d, err=%s", targetShardId, err.Error())}
				dependentCall.SSCResult = ret
				subNode := new(api.CallNode)
				subNode.FromData(ret.TreeNode)
				cxtState.CallForest.Insert(subNode)
				return ret
			}
		}
		s.stateLock.Lock()
		dependentCall.SSCResult = ret
		dependentCall.Executed = true
		dependentCall.WaitingChs = nil
		oldRelatedShards := cxtState.RelatedShards
		cxtState.RelatedShards = cxtState.RelatedShards.Merge(ret.RelatedShards)
		utils.SSCLogger().Debug().Str("txHash", txHash.String()).
			Str("result", fmt.Sprintf("%v", cxtState.RelatedShards)).
			Str("old", fmt.Sprintf("%v", oldRelatedShards)).
			Str("new", fmt.Sprintf("%v", ret.RelatedShards)).
			Msg("update related shards")
		utils.SSCLogger().Debug().Str("txHash", txHash.String()).
			Str("callIndex", req.CallIndex.ToString()).
			Uint32("targetShard", targetShardId).
			Str("relatedShards", fmt.Sprintf("%v", cxtState.RelatedShards)).
			Interface("result", ret).
			Msg("call cxt contract, end")
		s.stateLock.Unlock()
	}
	subNode := new(api.CallNode)
	subNode.FromData(ret.TreeNode)
	cxtState.CallForest.Insert(subNode)
	// wait for sub execution finished
	subNode.Wait(s.SelfShard)
	return ret
}

func (s *sscService) VerifySimulation(simulationBytes []byte, stateDB api.StateDB, header *block.Header) {
	// 1
	simulation := &api.CXTSimulation{}
	err := json.Unmarshal(simulationBytes, simulation)
	// 1 [x]
	if err != nil {
		txHash := simulation.TxHash
		utils.SSCLogger().Error().Err(err).Str("txHash", txHash.Hex()).
			Msgf("failed to unmarshal simulation, size: %d", len(simulationBytes))
		payload := &api.CXTInvalidSimulationPayload{
			Type: api.InvalidSerialization,
		}
		payloadBytes, _ := json.Marshal(payload)
		vote := &api.CXTCommitVote{
			TxHash:        txHash,
			Type:          api.Rollback,
			ShardId:       s.SelfShard,
			OriginShardId: simulation.OriginShardId,
			Reason:        api.ReasonInvalidSimulation,
			Payload:       payloadBytes,
			BaseSSCMessage: api.BaseSSCMessage{
				Epoch: simulation.Epoch,
			},
		}
		s.sendCXTCommitVote(simulation.OriginShardId, vote)
		return
	}

	txHash := simulation.TxHash
	if s.SelfShard == simulation.OriginShardId {
		s.timerMgr.StartTimer(txHash, simulation.Epoch, header.NumberU64(), s.SelfShard)
	}

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		// Interface("simulation", simulation).
		Int("simulationNum", simulation.SimulationNum).
		Uint64("epoch", uint64(simulation.Epoch)).
		Msg("begin to verify simulation")

	conflictLockCallIndexes := make([]api.CallIndex, 0)
	conflictLockKeys := make([]api.LockKey, 0)
	conflictCallStateIndex := -1
	var execErr error

	callStateMap := make(map[string]*api.CXTCallState)
	for _, callState := range simulation.CallStates {
		callStateMap[callState.CallIndex.ToString()] = callState
	}
	verifyContext := &api.ExecutionVerifyContext{Simulation: simulation, CallStateMap: callStateMap}
	// s.verifyCtxLock.Lock()
	s.executionVerifyContexts[txHash] = verifyContext
	// s.verifyCtxLock.Unlock()

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Msg("verify simulation 1")

CallStates:
	for i, callState := range simulation.CallStates {
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Str("callIndex", callState.CallIndex.ToString()).
			Msgf("verify call state %d", i)
		// check if states in write set are locked by other tx
		for address, stateMap := range callState.RWSet.WriteState.State {
			for key, _ := range stateMap {
				lockKey := api.FormKey(address, key)
				_, stateErr := stateDB.GetState(txHash, address, key)
				utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
					Str("callIndex", callState.CallIndex.ToString()).
					Str("lockKey", string(lockKey)).
					Err(stateErr).
					Msg("check state")
				if stateErr != nil {
					if errors.Is(stateErr, api.ErrLockedByOtherTx) {
						utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
							Msgf("mu conflict for key: %s", lockKey)
						conflictLockKeys = append(conflictLockKeys, lockKey)
						conflictLockCallIndexes = append(conflictLockCallIndexes, callState.CallIndex)
					} else {
						utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
							Str("callIndex", callState.CallIndex.ToString()).
							Err(stateErr).Msgf("failed to get state for key: %s", lockKey)
						return
					}
				}
			}
		}
		// check if states in read set are locked by other tx or not consistent with current state
		for address, stateMap := range callState.RWSet.ReadState.State {
			for key, _ := range stateMap {
				lockKey := api.FormKey(address, key)
				onChainValue, stateErr := stateDB.GetState(txHash, address, key)
				if stateErr != nil {
					if errors.Is(stateErr, api.ErrLockedByOtherTx) {
						utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
							Msgf("mu conflict for key: %s", lockKey)
						conflictLockKeys = append(conflictLockKeys, lockKey)
						conflictLockCallIndexes = append(conflictLockCallIndexes, callState.CallIndex)
					} else {
						utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
							Str("callIndex", callState.CallIndex.ToString()).
							Err(stateErr).Msgf("failed to get state for key: %s", lockKey)
						return
					}
				}
				if stateErr == nil && bytes.Compare(onChainValue.Bytes(), callState.RWSet.ReadState.State[address][key].Bytes()) != 0 {
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
			TxHash:        txHash,
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
			Msgf("try to mu state with execution")
		err := s.lockStateWithExecution(simulation.Epoch, simulation.CallStates[conflictCallStateIndex], stateDB.(*corestate.DB))
		if err != nil {
			stateDB.RevertToSnapshot(snapshot)
			utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
				Err(err).Msg("failed to mu state with execution")
			payload := &api.CXTInvalidSimulationPayload{
				Type: api.InvalidExecution,
			}
			payloadBytes, _ := json.Marshal(payload)
			vote := &api.CXTCommitVote{
				TxHash:        txHash,
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
			TxHash:        txHash,
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
		ls := make(map[api.LockKey]interface{})
		for _, callState := range simulation.CallStates {
			for addr, account := range callState.RWSet.ReadState.State {
				for key, _ := range account {
					ls[api.FormKey(addr, key)] = struct{}{}
				}
			}
			for addr, account := range callState.RWSet.WriteState.State {
				for key, _ := range account {
					ls[api.FormKey(addr, key)] = struct{}{}
				}
			}
		}
		if s.IsLeader() {
			retryTx := &api.RetryTx{
				TxHash:        txHash,
				Epoch:         simulation.Epoch,
				Sender:        simulation.Sender,
				Nonce:         simulation.Nonce,
				OriginShardID: simulation.OriginShardId,
				RelatedShards: simulation.RelatedShards,
				SimulationNum: simulation.SimulationNum + 1,
				Condition:     api.Verify,
			}

			utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
				Int("simulationNum", retryTx.SimulationNum).
				Int("ls", len(ls)).
				Msg("subscribe mu States for re-simulation")

			s.retryScheduler.CallForRetry(retryTx)
		}
	} else {
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Uint32("FromShard", simulation.OriginShardId).
			Int("size", len(simulationBytes)).
			Msgf("simulation is valid, mu the rwset and send commit vote")
		for _, callState := range simulation.CallStates {
			s.lockStateWithRWSet(txHash, callState, stateDB)
		}
		vote := &api.CXTCommitVote{
			TxHash:        txHash,
			ShardId:       s.SelfShard,
			OriginShardId: simulation.OriginShardId,
			Type:          api.Commit,
			Reason:        api.ReasonSuccess,
			Payload:       nil,
		}
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Uint32("FromShard", simulation.OriginShardId).
			Int("size", len(simulationBytes)).
			Msgf("send cxt commit vote")
		s.sendCXTCommitVote(s.SelfShard, vote)
	}
}

func (s *sscService) verifyExecuteForCallState(simulation *api.CXTSimulation, txHash common.Hash, callState *api.CXTCallState, stateDB *corestate.DB) error {
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Uint32("FromShard", simulation.OriginShardId).
		Msgf("verify execution for call state")
	chainConfig := s.bc.Config()
	vmConfig := s.bc.GetVMConfig()
	s.verifyCtxLock.RLock()
	verifyContext := s.executionVerifyContexts[txHash]
	s.verifyCtxLock.RUnlock()
	s.stateLock.RLock()
	_, finished := s.finishedTxs[txHash]
	s.stateLock.RUnlock()
	if finished {
		return api.ErrTxHasBeenClosed
	}
	if verifyContext == nil {
		return errors.New("execution verify context is nil")
	}
	defer func() {
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Uint32("FromShard", simulation.OriginShardId).
			Msgf("verify execution for call state done")
	}()

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
		header = s.bc.GetHeaderByHash(req.BlockHash)
		if header == nil {
			utils.SSCLogger().Error().Str("blockHash", req.BlockHash.Hex()).
				Msg("failed to get header")
			return errors.New("header is nil")
		}
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
		blockHash := req.BlockHash
		header = s.bc.GetHeaderByHash(blockHash)
		if header == nil {
			utils.SSCLogger().Error().Str("blockHash", blockHash.Hex()).
				Msg("failed to get header")
			return errors.New("header is nil")
		}
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
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Str("callIndex", callIndex.ToString()).
		Msgf("verify call state %s for shard %d", callState.CallIndex.ToString(), s.SelfShard)
	verifyContext.DependentResults = callState.DependentResults
	verifyContext.CallFrame = &api.CallFrame{
		CallIndex: callIndex,
		PC:        0,
	}

	// prepare execution verify context
	sender := vm.AccountRef(origin)
	vmCtx := core.NewSSCVMContext(origin, txHash, callIndex, gasPrice, header, s.bc, nil)
	sscvm := vm.NewSSCVM(vmCtx, stateDB, chainConfig, *vmConfig, s, vm.ExecutionVerify)

	// execute contract with ExecutionVerify
	ret, _, err := sscvm.Call(sender, addr, input, gas, value)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Str("txHash", txHash.Hex()).Msg("verify callState failed")
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
	txHash := vote.TxHash

	var signer api.BLSSigner

	if vote.Type == api.Recall {
		signer = s.signerMgr.GetSSCSigner()
	} else {
		signer = s.signerMgr.GetValidatorSigner()
	}

	sign, err := signer.Sign(vote)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
			Msg("failed to sign CXTCommitVote")
		return
	}
	vote.BaseSSCMessage.SenderAddr = signer.Address()
	vote.BaseSSCMessage.Signature = sign

	ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
	defer cancel()

	targetLeader := s.GetLeader(vote.Epoch, shardId)
	if targetLeader == nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).
			Msg("failed to get leader")
		return
	}

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("send CXTCommitVote to %s", targetLeader.Endpoint)
	s.Comm.Call(ctx, nil, targetLeader, api.Method_HandleCommitVote, vote)
}

func (s *sscService) checkLockConflict(simulation *api.CXTSimulation, stateDB api.StateDB) bool {
	txHash := simulation.TxHash
	conflictCallIndexes := make([]api.CallIndex, 0)
	conflictLockKeys := make([]api.LockKey, 0)

	for _, callState := range simulation.CallStates {
		// check if read set is conflict with current state
		for address, stateMap := range callState.RWSet.ReadState.State {
			for key, _ := range stateMap {
				lockKey := api.FormKey(address, key)
				// TODO 改成只是检查锁状态，不需要获取状态
				_, err := stateDB.GetState(txHash, address, key)
				if err != nil && errors.Is(err, api.ErrLockedByOtherTx) {
					utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
						Msgf("mu conflict for key: %s", lockKey)
					conflictLockKeys = append(conflictLockKeys, lockKey)
					conflictCallIndexes = append(conflictCallIndexes, callState.CallIndex)
				}
			}
		}
		for address, stateMap := range callState.RWSet.WriteState.State {
			for key, _ := range stateMap {
				lockKey := api.FormKey(address, key)
				_, err := stateDB.GetState(txHash, address, key)
				if err != nil && errors.Is(err, api.ErrLockedByOtherTx) {
					utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
						Msgf("mu conflict for key: %s", lockKey)
					conflictLockKeys = append(conflictLockKeys, lockKey)
					conflictCallIndexes = append(conflictCallIndexes, callState.CallIndex)
				}
			}
		}
	}

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Int("conflictLockKeys", len(conflictLockKeys)).
		Int("conflictCallIndexes", len(conflictCallIndexes)).
		Msg("check mu conflict for simulation")

	return len(conflictLockKeys) > 0
}

func (s *sscService) lockStateWithExecution(epoch api.Epoch, callState *api.CXTCallState, state *corestate.DB) error {
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
		txHash = topReq.TxHash
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
		txHash = callReq.TxHash
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
		TxHash:        txHash,
		CallIndex:     callIndex,
		RelatedShards: relatedShards,
		Result:        ret,
		LeftOverGas:   leftOverGas,
		BlockHash:     header.Hash(),
		BaseSSCMessage: api.BaseSSCMessage{
			Epoch: epoch,
		},
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
	txHash := commitProof.TxHash
	s.stateLock.Lock()
	_, exists := s.finishedTxs[txHash]
	s.stateLock.Unlock()
	if exists {
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msg("cxt has committed or rollback")
		return nil
	}

	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msg("commit or rollback with proof")

	if commitProof.Type == api.Commit {
		utils.SSCLogger().Info().Str("txHash", txHash.String()).Msgf("commit with proof, origin: [%v,%d], epoch=%d", commitProof.OriginShard == s.SelfShard, commitProof.OriginShard, commitProof.Epoch)
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
		utils.SSCLogger().Info().Str("txHash", txHash.String()).Msgf("rollback with proof, origin: [%v,%d], epoch=%d, reason: %s", commitProof.OriginShard == s.SelfShard, commitProof.OriginShard, commitProof.Epoch, commitProof.Reason.String())
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

func (s *sscService) NewEpoch(newEpochBytes []byte, sscvm api.VM, stateDB api.StateDB, blockNum uint64) error {
	newEpoch := &api.NewEpoch{}
	err := json.Unmarshal(newEpochBytes, newEpoch)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to unmarshal new epoch")
		return err
	}
	utils.SSCLogger().Info().Uint32("selfShardId", s.SelfShard).Interface("newEpoch", newEpoch).Msg("new epoch")
	if s.SelfShard == newEpoch.Committee.ShardID {
		utils.SSCLogger().Info().Msgf("local call new epoch, epoch=%d, need to cross call other %d shards", newEpoch.Committee.Epoch, len(s.LatestCommittees)-1)
		for i := 0; i < len(s.LatestCommittees); i++ {
			shardId := uint32(i)
			if shardId != s.SelfShard {
				utils.SSCLogger().Info().Msgf("cross call new epoch, epoch=%d, shardId=%d", newEpoch.Committee.Epoch, shardId)
				_, _, err := sscvm.CrossCall(shardId, vm.NewEpochAddr, vm.NewEpochAddr, newEpochBytes, 10000000, new(big.Int))
				if err != nil {
					utils.SSCLogger().Error().Err(err).Msg("failed to cross call new epoch")
					return err
				}
			}
		}
	}
	return s.CommitteeMechanism.HandleNewEpoch(newEpoch, stateDB, blockNum)
}

func (s *sscService) StartSimulateCXTransaction(req *api.CXTSimulationRequest) *api.CXTSimulationSSCResult {
	var waitingCh chan *api.CXTSimulationSSCResult
	txHash := req.Tx.Hash()

	utils.SSCLogger().Info().Int("simulationNum", req.SimulationNum).Str("txHash", txHash.String()).Msg("start simulate cx transaction, start")
	defer utils.SSCLogger().Info().Int("simulationNum", req.SimulationNum).Str("txHash", txHash.String()).Msg("start simulate cx transaction, end")

	func() {
		s.simuLock.Lock()
		defer s.simuLock.Unlock()
		if s.simuWaitingChs[txHash] == nil {
			s.simuWaitingChs[txHash] = make([]chan *api.CXTSimulationSSCResult, 0)
		} else {
			waitingCh = make(chan *api.CXTSimulationSSCResult)
			s.simuWaitingChs[txHash] = append(s.simuWaitingChs[txHash], waitingCh)
		}
		if s.simuResultCh[txHash] == nil {
			s.simuResultCh[txHash] = make(chan *api.CXTSimulationSSCResult, 1)
		}
	}()

	if waitingCh != nil {
		return <-waitingCh
	}

	defer func() {
		s.simuLock.Lock()
		delete(s.simuWaitingChs, txHash)
		s.simuLock.Unlock()
	}()

	header := s.bc.CurrentHeader()
	req.BlockHash = header.Hash()
	committee := s.GetCommittee(req.Epoch, s.SelfShard)
	n := committee.Number
	t := committee.Threshold

	ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(n)
	var (
		results []api.SSCMessage
		hasSelf bool
		lock    sync.Mutex
	)

	// 预分配容量，但长度动态控制
	results = make([]api.SSCMessage, 0, t)

	for i := 0; i < n; i++ {
		member := committee.Members[i]
		go func(m *api.Member) {
			defer wg.Done()
			ret := new(api.CXTSimulationResult)
			err := s.Comm.Call(ctx, ret, m, api.Method_HandleSimulateRequest, req)
			if err != nil {
				utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(err).Msg("failed to call")
				return
			}

			lock.Lock()
			defer lock.Unlock()

			// 如果上下文已取消，直接退出
			if ctx.Err() != nil {
				utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(ctx.Err()).Msg("context cancelled")
				return
			}

			if bytes.Compare(m.Address.Bytes(), s.SelfAddr.Bytes()) == 0 {
				hasSelf = true
				if len(results) < t {
					results = append([]api.SSCMessage{ret}, results...)
				} else {
					results[0] = ret // 替换最后一个
				}
			} else {
				if len(results) < t {
					results = append(results, ret)
				}
			}

			// 检查是否满足终止条件
			if hasSelf && len(results) == t {
				utils.SSCLogger().Info().Str("txHash", txHash.String()).Msg("simulation result received, including self")
				cancel()
			}
		}(member)
	}
	wg.Wait()

	var (
		sscResult *api.CXTSimulationSSCResult
		err       error
	)
	if len(results) == t {
		leaderRet := results[0].(*api.CXTSimulationResult)
		if leaderRet.Err != "" {
			sscResult = &api.CXTSimulationSSCResult{Err: leaderRet.Err, RelatedShards: []uint32{s.SelfShard}}
			utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(errors.New(leaderRet.Err)).Msg("failed to simulate transaction")
		} else {
			sscResult, err = s.aggregateSimulationResults(results)
			if err != nil {
				sscResult = &api.CXTSimulationSSCResult{Err: err.Error(), RelatedShards: []uint32{s.SelfShard}}
				utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(err).Msg("failed to aggregate simulation results")
				return sscResult
			}
		}
	} else {
		if len(results) == 0 {
			sscResult = &api.CXTSimulationSSCResult{Err: "no response from other shards", RelatedShards: []uint32{s.SelfShard}}
		} else {
			errRet := results[len(results)-1].(*api.CXTSimulationResult)
			sscResult = &api.CXTSimulationSSCResult{Err: errRet.Err, RelatedShards: []uint32{s.SelfShard}}
		}
	}

	simulationCommit := &api.SimulationCommit{
		SimulationNum: req.SimulationNum,
		TxHash:        req.Tx.Hash(),
		Nonce:         req.Tx.Nonce(),
		Sender:        req.From,
		RelatedShards: sscResult.RelatedShards,
		BaseBLSSignedMessage: api.BaseBLSSignedMessage{
			Epoch:   req.Epoch,
			ShardId: s.SelfShard,
		},
	}

	if len(sscResult.Err) == 0 {
		simulationCommit.Commit = true
		simulationCommit.Status = api.OK
		utils.SSCLogger().Debug().Str("txHash", simulationCommit.TxHash.Hex()).
			Msgf("simulation accomplished, send simulation commit, commit type: %v, simulationNum: %d, relatedShards: %v",
				simulationCommit.Commit, simulationCommit.SimulationNum, simulationCommit.RelatedShards)
	} else {
		if vm.IsLockedByOtherTxErr(sscResult.Err) {
			utils.SSCLogger().Debug().Str("txHash", simulationCommit.TxHash.Hex()).
				Msgf("simulation's state is locked by other tx, try to retry if self is leader: %v", s.IsLeader())
			simulationCommit.Commit = false
			simulationCommit.Status = api.LockConflict
			// don't send commit simulation if state is locked by other tx
			if s.IsLeader() {
				s.retryScheduler.CallForRetry(&api.RetryTx{
					TxHash:        txHash,
					Epoch:         req.Epoch,
					RelatedShards: sscResult.RelatedShards,
					SimulationNum: req.SimulationNum + 1,
					Condition:     api.Simulate,
				})
			}
			return sscResult
		} else {
			simulationCommit.Commit = false
			simulationCommit.Status = api.ExecutionFailed
		}
		utils.SSCLogger().Error().Str("txHash", simulationCommit.TxHash.Hex()).
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

	var chs []chan *api.CXTSimulationSSCResult

	func() {
		s.simuLock.Lock()
		defer s.simuLock.Unlock()
		if len(state.SimulationCallStates[state.SimulationNum]) == 0 {
			utils.SSCLogger().Error().Interface("state", state).Msgf("simulation call states is empty")
		}
		state.SimulationCallStates[state.SimulationNum][0].TopSSCResult = sscResult
		state.SimulationResult = sscResult
		chs = s.simuWaitingChs[txHash]
	}()

	for _, ch := range chs {
		ch <- sscResult
	}

	s.thresholdSignSimulationCommit(simulationCommit)
	leaders := make([]*api.Member, 0)
	for _, shardId := range simulationCommit.RelatedShards {
		leaders = append(leaders, s.GetLeader(req.Epoch, shardId))
	}
	utils.SSCLogger().Debug().Str("txHash", txHash.String()).Interface("commit", simulationCommit).Interface("leaders", leaders).Msgf("send simulation commit to %v", simulationCommit.RelatedShards)
	ctxCS, cancelCS := context.WithTimeout(s.ctx, s.Config.CallTimeout)
	defer cancelCS()

	s.Comm.Multicast(ctxCS, leaders, api.Method_CommitSimulation, simulationCommit)

	var resultCh chan *api.CXTSimulationSSCResult
	func() {
		s.simuLock.Lock()
		defer s.simuLock.Unlock()
		if s.simuResultCh[txHash] != nil {
			resultCh = s.simuResultCh[txHash]
			close(s.simuResultCh[txHash])
			delete(s.simuResultCh, txHash)
		}
	}()
	if resultCh != nil {
		<-resultCh
	}

	return sscResult
}

func (s *sscService) thresholdSignSimulationCommit(commit *api.SimulationCommit) {
	committee := s.GetCommittee(commit.Epoch, s.SelfShard)
	n := committee.Number
	t := committee.Threshold
	txHash := commit.TxHash
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
	var (
		results []api.SSCMessage
		hasSelf bool
		lock    sync.Mutex
	)
	// 预分配容量，但长度动态控制
	results = make([]api.SSCMessage, 0, t)

	for i := 0; i < n; i++ {
		member := committee.Members[i]
		go func(m *api.Member) {
			defer wg.Done()
			signature := make([]byte, 0)
			signErr := s.Comm.Call(ctx, &signature, member, api.Method_SignSimulationCommit, commit)
			if signErr != nil {
				return
			}
			ret := api.BaseSSCMessage{
				Epoch:      commit.Epoch,
				Signature:  signature,
				SenderAddr: member.Address,
			}

			utils.SSCLogger().Debug().
				Str("txHash", txHash.String()).
				Str("from", m.Address.String()).
				Msg("received simulation result")

			lock.Lock()
			defer lock.Unlock()

			// 如果上下文已取消，直接退出
			if ctx.Err() != nil {
				return
			}

			if bytes.Compare(m.Address.Bytes(), s.SelfAddr.Bytes()) == 0 {
				hasSelf = true
				if len(results) < t {
					results = append([]api.SSCMessage{ret}, results...)
				} else {
					results[0] = ret // 替换最后一个
				}
			} else {
				if len(results) < t {
					results = append(results, ret)
				}
			}

			// 检查是否满足终止条件
			if hasSelf && len(results) == t {
				cancel()
			}
		}(member)
	}
	wg.Wait()

	aggregatedSig, bitMap, err := s.BLSSignerMgr.GetSSCSigner().Aggregate(results)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msgf("failed to aggregate signatures")
		return
	}
	utils.SSCLogger().Debug().Msgf("aggregated signature: %v, bitMap: %v", common.Bytes2Hex(aggregatedSig), common.Bytes2Hex(bitMap))
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
		TreeNode:      result.TreeNode,
		BaseBLSSignedMessage: api.BaseBLSSignedMessage{
			ShardId:    s.SelfShard,
			Signatures: aggregatedSig,
			BLSBitMap:  bitMap,
			Epoch:      result.Epoch,
		},
	}
	return sscResult, nil
}

func (s *sscService) HandleSimulateRequest(ctx context.Context, req *api.CXTSimulationRequest) *api.CXTSimulationResult {
	txHash := req.TxHash
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Int("simulationNum", req.SimulationNum).Msgf("handle simulate request, start, simulationNum=%d", req.SimulationNum)
	defer utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msg("handle simulate request, end")
	callState, err := s.startSimulation(req)
	if err != nil {
		return &api.CXTSimulationResult{Err: err.Error()}
	}

	s.stateLock.RLock()
	simuState, stErr := s.getState(txHash)
	if stErr != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(stErr).Msg("failed to get state")
		s.stateLock.RUnlock()
		return &api.CXTSimulationResult{Err: stErr.Error()}
	}
	relatedShards := simuState.RelatedShards
	simuState.SimulateCh = make(chan struct{})
	s.stateLock.RUnlock()

	defer func() {
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msg("simulation completed")
		close(simuState.SimulateCh)
	}()

	header := s.bc.GetHeaderByHash(req.BlockHash)
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
		return &api.CXTSimulationResult{Err: err.Error(), BaseSSCMessage: api.BaseSSCMessage{Epoch: req.Epoch}}
	}

	utils.SSCLogger().Debug().
		Str("txHash", txHash.String()).
		Str("from", msg.From().String()).
		Msg("simulate contract with sscvm")

	var ret *api.CXTSimulationResult
	vmCtx := core.NewSSCVMContext(msg.From(), tx.Hash(), api.CallIndex{}, tx.GasPrice(), header, s.bc, req.Author)
	vmCtx.TxType = types.CXTransaction
	sscvm := vm.NewSSCVM(vmCtx, state, chainConfig, *vmConfig, s, executionType)
	result, err := core.NewSSCStateTransition(sscvm, msg, gp).TransitionDb()
	if err != nil {
		ret = &api.CXTSimulationResult{Err: err.Error(), BaseSSCMessage: api.BaseSSCMessage{Epoch: req.Epoch}}
	} else {
		s.stateLock.RLock()
		simuState, err = s.getState(txHash)
		if err != nil {
			s.stateLock.RUnlock()
			return &api.CXTSimulationResult{Err: err.Error()}
		}
		relatedShards = simuState.RelatedShards
		s.stateLock.RUnlock()

		ret = &api.CXTSimulationResult{
			RelatedShards: relatedShards,
			Result:        result.ReturnData,
			Receipt:       nil,
			UsedGas:       result.UsedGas,
			Err:           "",
			BaseSSCMessage: api.BaseSSCMessage{
				Epoch: req.Epoch,
			},
		}
		if result.VMErr != nil {
			ret.Err = result.VMErr.Error()
		}
		if callState.LockedByOtherTx != nil {
			ret.Err = api.ErrLockedByOtherTx.Error()
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
	txHash := req.TxHash
	committee := s.GetCommittee(req.Epoch, s.SelfShard)
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Int("simulationNum", req.SimulationNum).Msgf("request call cxt, start, simulationNum=%d", req.SimulationNum)

	s.stateLock.Lock()
	cxtState, _ := s.getState(txHash)
	if cxtState == nil {
		s.stateLock.Unlock()
		return &api.CXTCallSSCResult{Err: "no simulation state found"}
	}
	callStates := cxtState.SimulationCallStates[req.SimulationNum]
	// get caller's call state
	callerState := callStates.Get(req.CallIndex[:len(req.CallIndex)-1])

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Str("signer", req.SenderAddr.Hex()).Str("callIndex", req.CallIndex.ToString()).Msgf("request call cxt, addr: %s, callStates: %s", req.Addr.Hex(), callStates.ToString())
	if callerState == nil {
		pendingWaitingCh := make(chan *api.CXTCallSSCResult, 1)
		s.pendingLock.Lock()
		if s.pendingRequests == nil {
			s.pendingRequests = make(map[common.Hash][]*pendingCXTRequest)
		}
		s.pendingRequests[txHash] = append(s.pendingRequests[txHash], &pendingCXTRequest{
			req:       req,
			waitingCh: pendingWaitingCh,
		})
		s.pendingLock.Unlock()
		s.stateLock.Unlock()
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Str("signer", req.SenderAddr.Hex()).Str("callIndex", req.CallIndex.ToString()).Msgf("request call cxt, no caller state found, waiting for pending request")
		return <-pendingWaitingCh
	}

	s.stateLock.Unlock()

	waitingCh := make(chan *api.CXTCallSSCResult, 1)
	var (
		dependentCall  *api.DependentCXTCall
		reachThreshold bool
	)
	sscResult := func() *api.CXTCallSSCResult {
		s.stateLock.Lock()
		defer s.stateLock.Unlock()

		dependentCall = callerState.DependentCXTCalls[req.CallIndex.ToString()]
		if dependentCall != nil && dependentCall.SSCResult != nil {
			utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Str("callIndex", req.CallIndex.ToString()).Msgf("request call cxt, dependent call result found")
			return dependentCall.SSCResult
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

		dependentCall.WaitingChs = append(dependentCall.WaitingChs, waitingCh)
		if len(dependentCall.Requests) < committee.Threshold {
			dependentCall.Requests = append(dependentCall.Requests, req)
			if len(dependentCall.Requests) == committee.Threshold {
				reachThreshold = true
			}
		}
		utils.SSCLogger().Debug().Str("txHash", txHash.String()).Str("callIndex", req.CallIndex.ToString()).Msgf("request call cxt, waiting [%d/%d], chs=%d", len(dependentCall.Requests), committee.Threshold, len(dependentCall.WaitingChs))
		return nil
	}()

	if sscResult != nil {
		return sscResult
	}

	if reachThreshold {
		// aggregate signatures
		// send request to leader
		leader := s.GetLeader(req.Epoch, req.TargetShardId)
		sscReq := s.aggregateSSCCallRequest(dependentCall.Requests)
		dependentCall.SignedRequest = sscReq

		utils.SSCLogger().Debug().
			Str("txHash", txHash.String()).
			Str("cxtStateCallIndex", cxtState.CurrentCallFrame.CallIndex.ToString()).
			Str("reqCallIndex", req.CallIndex.ToString()).
			Uint32("sscTargetShardId", s.SelfShard).
			Uint32("reqTargetShardId", req.TargetShardId).
			Uint32("targetShardId", s.GetShardID(req.Addr)).
			Str("addr", req.Addr.Hex()).
			Str("callIndex", req.CallIndex.ToString()).Msg("request call cxt, start")
		defer utils.SSCLogger().Debug().Str("txHash", txHash.String()).Msg("request call cxt, end")

		ret := new(api.CXTCallSSCResult)
		var err error

		s.stateLock.Lock()
		cxtState, err = s.getState(txHash)
		s.stateLock.Unlock()
		if cxtState != nil {
			dependentCall = cxtState.SimulationCallStates[req.SimulationNum].Get(req.CallIndex[:len(req.CallIndex)-1]).DependentCXTCalls[req.CallIndex.ToString()]
			ctx, cancel := context.WithTimeout(context.Background(), s.Config.CallTimeout)
			defer cancel()
			err = s.Comm.Call(ctx, ret, leader, api.Method_HandleCXTSSCCall, sscReq)
			if err != nil {
				utils.SSCLogger().Error().Err(err).Interface("req", sscReq).Str("txHash", txHash.String()).Str("callIndex", req.CallIndex.ToString()).Msg("failed to call cxt ssc call")
				ret = &api.CXTCallSSCResult{Err: err.Error(), BaseBLSSignedMessage: api.BaseBLSSignedMessage{Epoch: req.Epoch}}
			}
			if ret.Err != "" {
				utils.SSCLogger().Error().Err(errors.New(ret.Err)).Str("txHash", txHash.String()).Str("callIndex", req.CallIndex.ToString()).Msgf("failed to call cxt ssc call, targetShardId=%d", req.TargetShardId)
			}
		} else {
			ret = &api.CXTCallSSCResult{Err: err.Error()}
		}

		utils.SSCLogger().Debug().Str("txHash", txHash.String()).Str("callIndex", req.CallIndex.ToString()).Interface("ret", ret).Msgf("request call cxt, return, chs=%d", len(dependentCall.WaitingChs))
		s.stateLock.Lock()
		dependentCall.SSCResult = ret
		chs := dependentCall.WaitingChs
		s.stateLock.Unlock()
		for _, ch := range chs {
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
	utils.SSCLogger().Debug().Str("bitMap", common.Bytes2Hex(bitMap)).Msgf("aggregated cxt call request signatures, msgs=%d", len(msgs))
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
			Epoch:      request.Epoch,
		},
		BlockHash: common.Hash{},
	}
	return sscRequest
}

func (s *sscService) HandleCXTCall(req *api.CXTCallSSCRequest) *api.CXTCallResult {
	err := s.signerMgr.GetSSCSigner().Verify(req)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to verify cxt call request signature")
		return &api.CXTCallResult{Err: fmt.Sprintf("error from targetShard %d, err=%s", req.TargetShardId, err.Error())}
	}

	txHash := req.TxHash
	// begin cxt call, update simulationState and callIndex
	callState, err := s.startCall(req)
	if err != nil {
		return &api.CXTCallResult{Err: fmt.Sprintf("error from targetShard %d, err=%s", req.TargetShardId, err.Error())}
	}
	s.stateLock.RLock()
	simuState, stateErr := s.getState(txHash)
	if stateErr != nil {
		s.stateLock.RUnlock()
		return &api.CXTCallResult{Err: "transaction has been closed"}
	}
	s.stateLock.RUnlock()

	defer s.endCall(req)
	callState.Lock.Lock()
	defer callState.Lock.Unlock()

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Str("callIndex", req.CallIndex.ToString()).Str("addr", req.Addr.Hex()).Msg("handle cxt call, start")
	defer utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msg("handle cxt call, end")

	header := s.bc.GetHeaderByHash(req.BlockHash)
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

	callNode := api.NewCallNode(req.TxHash, req.CallIndex, s.SelfShard)
	simuState.CallForest.Insert(callNode)
	// create sscvm instance
	vmCtx := core.NewSSCVMContext(req.Caller, txHash, req.CallIndex, req.GasPrice, header, s.bc, nil)
	sscvm := vm.NewSSCVM(vmCtx, state, chainConfig, *vmConfig, s, executionType)

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("call contract with %s, callIndex=%s, simulationNum=%d",
		executionType.String(), req.CallIndex.ToString(), req.SimulationNum)
	ret, leftOverGas, err := sscvm.CallFromOtherShard(req.FromShardId, sender, req.Addr, req.Input, req.Gas, req.Value)

	if ret == nil && err == nil {
		utils.SSCLogger().Error().Err(err).Str("txHash", txHash.Hex()).Str("callIndex", req.CallIndex.ToString()).Msgf("successfully return nil, addr=%s, leftOverGas=%d, ret=%s", req.Addr, leftOverGas, ret)
	}

	s.stateLock.RLock()
	simuState, stateErr = s.getState(txHash)
	if stateErr != nil {
		s.stateLock.RUnlock()
		return &api.CXTCallResult{Err: "transaction has been closed"}
	}
	relatedShards := simuState.RelatedShards.Merge(req.RelatedShards).Add(req.FromShardId)
	s.stateLock.RUnlock()
	result := &api.CXTCallResult{
		TxHash:        txHash,
		CallIndex:     req.CallIndex,
		RelatedShards: relatedShards,
		Result:        ret,
		LeftOverGas:   leftOverGas,
		BlockHash:     header.Hash(),
		TreeNode:      callNode.ToData(),
	}
	if callState.LockedByOtherTx != nil {
		result.Err = api.ErrLockedByOtherTx.Error()
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Str("callIndex", req.CallIndex.ToString()).Msgf("return locked by other tx")
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
		Epoch:      req.Epoch,
	}
	utils.SSCLogger().Debug().
		Str("txHash", txHash.Hex()).
		Str("callIndex", req.CallIndex.ToString()).
		Interface("ret", ret).
		Msgf("cxt call result: %s, err: %s", common.Bytes2Hex(result.Result), result.Err)
	callNode.Done()
	return result
}

func (s *sscService) startCall(req *api.CXTCallSSCRequest) (*api.SimulationCallState, error) {
	// try to start cxt if not exist
	txHash := req.TxHash
	_, err := s.startCXT(txHash, req.SimulationNum, req.OriginShardId, req.RelatedShards, nil, req)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to start cxt")
		return nil, err
	}

	s.stateLock.Lock()
	state, err := s.getState(txHash)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Msg("failed to get state")
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
			BlockHash:         req.BlockHash,
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
		s.stateLock.Lock()
		state.SimulationCallStates[state.SimulationNum] = state.SimulationCallStates[state.SimulationNum].Add(callState)
		s.pendingLock.Lock()
		pendingList := s.pendingRequests[txHash]
		s.pendingRequests[txHash] = make([]*pendingCXTRequest, 0)
		if len(pendingList) > 0 {
			// 异步处理，避免阻塞当前 goroutine 和锁
			for _, p := range pendingList {
				if p.req.CallIndex[:len(p.req.CallIndex)-1].ToString() == callState.CallIndex.ToString() {
					utils.SSCLogger().Info().
						Str("txHash", txHash.Hex()).
						Str("callIndex", req.CallIndex.ToString()).
						Msg("processing pending CXT request")
					go func(req *api.CXTCallRequest, ch chan *api.CXTCallSSCResult) {
						result := s.RequestCallCXT(req) // 递归调用，此时状态已存在
						utils.SSCLogger().Info().
							Str("txHash", txHash.Hex()).
							Str("callIndex", req.CallIndex.ToString()).
							Msg("finished processing pending CXT request")
						ch <- result
					}(p.req, p.waitingCh)
				} else {
					s.pendingRequests[txHash] = append(s.pendingRequests[txHash], p)
				}
			}
		}
		s.pendingLock.Unlock()
		s.stateLock.Unlock()
	}
	return callState, nil
}

func (s *sscService) endCall(req *api.CXTCallSSCRequest) {
	txHash := req.TxHash
	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	state, _ := s.getState(txHash)
	if state == nil {
		return
	}
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Str("callIndex", req.CallIndex.ToString()).Msgf("end call")
	state.CurrentCallFrame = state.CallStack.Pop()
}

func (s *sscService) waitForSync(callState *api.SimulationCallState) {
	callState.SyncedCh = make(chan struct{})

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

func (s *sscService) SignSimulationCommit(commit *api.SimulationCommit) []byte {
	signature, err := s.BLSSignerMgr.GetSSCSigner().Sign(commit)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Str("txHash", commit.TxHash.Hex()).Msgf("failed to sign simulation commit, txHash: %s", commit.TxHash.Hex())
		return nil
	}
	return signature
}

func (s *sscService) SignCXTSimulation(simulation *api.CXTSimulation) []byte {
	signature, err := s.BLSSignerMgr.GetSSCSigner().Sign(simulation)
	if err != nil {
		utils.SSCLogger().Error().Err(err).Str("txHash", simulation.TxHash.Hex()).Msgf("failed to sign simulation commit, txHash: %s", simulation.TxHash.Hex())
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
		threshold = int(math.Ceil(float64(len(s.GetCommittee(vote.Epoch, vote.ShardId).Members)) / 2))
		sscOrValidator = true
	} else {
		threshold = int(math.Ceil(float64(len(s.GetValidators(vote.Epoch, vote.ShardId))) / 2))
		sscOrValidator = false
	}
	if vote.ShardId != s.SelfShard && vote.Type == api.Rollback && vote.Reason == api.ReasonInvalidSimulation {
		utils.SSCLogger().Warn().Str("txHash", vote.TxHash.Hex()).
			Msgf("received invalid simulation rollback vote from shard %d, ignore", vote.OriginShardId)
		return
	}
	// 2
	reachThreshold := false
	var sscVote *api.CXTCommitSSCVote
	txHash := vote.TxHash

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
		leader := s.GetLeader(vote.Epoch, vote.OriginShardId)

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
			Epoch:      result.Epoch,
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

	txHash := proof.TxHash
	simuState, err := s.getState(txHash)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", proof.TxHash.Hex()).Err(err).Msg("handle cxt recall proof failed")
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
	if req == nil {
		utils.SSCLogger().Error().Msg("nil request")
		return &api.CXTCallSSCResult{Err: "nil request"}
	}
	err := s.BLSSignerMgr.GetSSCSigner().Verify(req)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", req.TxHash.String()).Err(err).Interface("req", req).Msg("invalid cxt ssc call signature")
		return &api.CXTCallSSCResult{Err: err.Error(), BaseBLSSignedMessage: api.BaseBLSSignedMessage{Epoch: req.Epoch}}
	}

	// select the consistent state
	header := s.bc.CurrentHeader()
	req.BlockHash = header.Hash()
	utils.SSCLogger().Debug().Str("txHash", req.TxHash.String()).Str("callIndex", req.CallIndex.ToString()).Msg("handle cxt ssc call, start")
	committee := s.GetCommittee(req.Epoch, s.SelfShard)
	t := committee.Threshold
	n := committee.Number
	txHash := req.TxHash
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
	var (
		results []api.SSCMessage
		hasSelf bool
		lock    sync.Mutex
	)
	// 预分配容量，但长度动态控制
	results = make([]api.SSCMessage, 0, t)

	for i := 0; i < n; i++ {
		member := committee.Members[i]
		go func(m *api.Member) {
			defer wg.Done()
			ret := new(api.CXTCallResult)
			callErr := s.Comm.Call(ctx, ret, member, api.Method_HandleCXTCall, req)
			if callErr != nil {
				return
			}

			lock.Lock()
			defer lock.Unlock()

			// 如果上下文已取消，直接退出
			if ctx.Err() != nil {
				return
			}

			if bytes.Compare(m.Address.Bytes(), s.SelfAddr.Bytes()) == 0 {
				hasSelf = true
				if len(results) < t {
					results = append([]api.SSCMessage{ret}, results...)
				} else {
					results[0] = ret // 替换最后一个
				}
			} else {
				if len(results) < t {
					results = append(results, ret)
				}
			}

			// 检查是否满足终止条件
			if hasSelf && len(results) == t {
				cancel()
			}
		}(member)
	}
	wg.Wait()

	if len(results) < t {
		if len(results) == 0 {
			utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).Msgf("handle cxt ssc call failed, error from targetShard %d", req.TargetShardId)
			return &api.CXTCallSSCResult{
				TxHash:               txHash,
				Err:                  fmt.Sprintf("handle ssc call failed, error from targetShard %d", req.TargetShardId),
				BaseBLSSignedMessage: api.BaseBLSSignedMessage{Epoch: req.Epoch, ShardId: req.ShardId},
			}
		}
		errRet := results[len(results)-1].(*api.CXTCallResult)
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Err(err).Msgf("handle cxt ssc call failed, error from targetShard %d, err=%s", req.TargetShardId, errRet.Err)
		return &api.CXTCallSSCResult{
			TxHash:               txHash,
			Err:                  fmt.Sprintf("handle ssc call failed, error from targetShard %d, err=%s", req.TargetShardId, errRet.Err),
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{Epoch: req.Epoch, ShardId: req.ShardId},
		}
	}

	var sscResult *api.CXTCallSSCResult
	leaderRet := results[0].(*api.CXTCallResult)
	if leaderRet.Err != "" {
		sscResult = &api.CXTCallSSCResult{Err: leaderRet.Err, TxHash: txHash, BaseBLSSignedMessage: api.BaseBLSSignedMessage{Epoch: req.Epoch, ShardId: req.ShardId}}
		utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(errors.New(leaderRet.Err)).Msg("failed to simulate transaction")
	} else {
		sscResult, err = s.aggregateCXSSCCallResult(results)
		if err != nil {
			utils.SSCLogger().Error().
				Str("txHash", txHash.String()).Err(err).Msg("failed to aggregate cxt call results")
			return &api.CXTCallSSCResult{
				TxHash:               txHash,
				Err:                  fmt.Sprintf("error from targetShard %d, err=%s", req.TargetShardId, err.Error()),
				BaseBLSSignedMessage: api.BaseBLSSignedMessage{Epoch: req.Epoch, ShardId: req.ShardId},
			}
		}
	}

	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	state, err = s.getState(txHash)
	if err != nil {
		return &api.CXTCallSSCResult{
			TxHash:               txHash,
			Err:                  fmt.Sprintf("error from targetShard %d, err=%s", req.TargetShardId, err.Error()),
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{Epoch: req.Epoch, ShardId: req.ShardId},
		}
	}
	if state.SimulationCallStates[req.SimulationNum] == nil {
		return &api.CXTCallSSCResult{
			TxHash:               txHash,
			Err:                  fmt.Sprintf("no related simulation num"),
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{Epoch: req.Epoch, ShardId: req.ShardId},
		}
	}
	callStates := state.SimulationCallStates[req.SimulationNum]
	if callStates.Get(req.CallIndex) == nil {
		return &api.CXTCallSSCResult{
			TxHash:               txHash,
			Err:                  fmt.Sprintf("no related call index"),
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{Epoch: req.Epoch, ShardId: req.ShardId},
		}
	}
	callState := callStates.Get(req.CallIndex)
	if callState == nil {
		return &api.CXTCallSSCResult{
			TxHash:               txHash,
			Err:                  fmt.Sprintf("no related call index"),
			BaseBLSSignedMessage: api.BaseBLSSignedMessage{Epoch: req.Epoch, ShardId: req.ShardId},
		}
	}
	callState.CallSSCResult = sscResult
	utils.SSCLogger().Debug().Str("txHash", req.TxHash.String()).Interface("sscResult", sscResult).Msg("handle cxt ssc call, end")
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
		TxHash:        result.TxHash,
		CallIndex:     result.CallIndex,
		RelatedShards: result.RelatedShards,
		Result:        result.Result,
		LeftOverGas:   result.LeftOverGas,
		BlockHash:     result.BlockHash,
		Err:           result.Err,
		TreeNode:      result.TreeNode,
		BaseBLSSignedMessage: api.BaseBLSSignedMessage{
			ShardId:    s.SelfShard,
			Signatures: aggregatedSig,
			BLSBitMap:  bitMap,
			Epoch:      result.Epoch,
		},
	}
	return sscResult, nil
}

func (s *sscService) CommitSimulation(commit *api.SimulationCommit) {
	txHash := commit.TxHash
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
	oldRelatedShards := simuState.RelatedShards
	simuState.RelatedShards = simuState.RelatedShards.Merge(commit.RelatedShards)
	utils.SSCLogger().Debug().Str("txHash", txHash.String()).
		Str("result", fmt.Sprintf("%v", simuState.RelatedShards)).
		Str("old", fmt.Sprintf("%v", oldRelatedShards)).
		Str("new", fmt.Sprintf("%v", commit.RelatedShards)).
		Msg("update related shards")
	simulationCallStates := simuState.SimulationCallStates[commit.SimulationNum]
	s.stateLock.Unlock()

	// wait for simulation finished
	// <-simuState.SimulateCh

	// build commit simulation
	callStates := make([]*api.CXTCallState, 0)
	for i, simulationCallState := range simulationCallStates {
		depentResults := make([]*api.CXTCallSSCResult, 0)
		for _, dc := range simulationCallState.DependentCXTCalls {
			depentResults = append(depentResults, dc.SSCResult)
			if dc.SSCResult == nil {
				utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Interface("dc", dc).Msgf("dependent callIndex %s has no result", dc.CallIndex.ToString())
			}
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
		SimulationNum: commit.SimulationNum,
		TxHash:        commit.TxHash,
		Nonce:         commit.Nonce,
		Sender:        commit.Sender,
		ShardId:       commit.ShardId,
		OriginShardId: simuState.OriginShardId,
		RelatedShards: commit.RelatedShards,
		CallStates:    callStates,
		BaseBLSSignedMessage: api.BaseBLSSignedMessage{
			Epoch: commit.Epoch,
		},
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
	txHash := simulation.TxHash
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("begin to build signatures for simulation")
	committee := s.GetCommittee(simulation.Epoch, s.SelfShard)
	n := committee.Number
	t := committee.Threshold

	ctx, cancel := context.WithTimeout(state.Ctx, s.Config.CallTimeout)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(n)
	var (
		results []api.SSCMessage
		hasSelf bool
		lock    sync.Mutex
	)
	// 预分配容量，但长度动态控制
	results = make([]api.SSCMessage, 0, t)

	for i := 0; i < n; i++ {
		member := committee.Members[i]
		go func(m *api.Member) {
			defer wg.Done()
			signature := make([]byte, 0)
			err := s.Comm.Call(ctx, &signature, member, api.Method_SignCXTSimulation, simulation)
			if err != nil {
				return
			}
			ret := api.BaseSSCMessage{
				Signature:  signature,
				SenderAddr: member.Address,
				Epoch:      simulation.Epoch,
			}

			lock.Lock()
			defer lock.Unlock()

			// 如果上下文已取消，直接退出
			if ctx.Err() != nil {
				return
			}

			if bytes.Compare(m.Address.Bytes(), s.SelfAddr.Bytes()) == 0 {
				hasSelf = true
				if len(results) < t {
					results = append([]api.SSCMessage{ret}, results...)
				} else {
					results[0] = ret // 替换最后一个
				}
			} else {
				if len(results) < t {
					results = append(results, ret)
				}
			}

			// 检查是否满足终止条件
			if hasSelf && len(results) == t {
				cancel()
			}
		}(member)
	}
	wg.Wait()

	aggregatedSig, bitMap, err := s.BLSSignerMgr.GetSSCSigner().Aggregate(results)
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
	txHash := vote.TxHash
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
			members = append(members, s.GetLeader(vote.Epoch, shardId))
		}

		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("receive all related ssc votes, commit type: %v, reason: %v, simulationNum: %d, relatedShards: %v",
			commitType, commitReason, vote.SimulationNum, relatedShards)

		switch commitType {
		case api.Commit:
			proof := &api.CXTCommitProof{
				TxHash:        txHash,
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
				TxHash:        txHash,
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
				TxHash:        txHash,
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

func (s *sscService) SignalReSimulation(signal *api.ReSimulationSignals) {
	s.retryScheduler.HandleReSimulationSignal(signal)
}

func (s *sscService) startReSimulation(txHash common.Hash, simulationNum int) {
	s.stateLock.Lock()
	state, err := s.getState(txHash)
	s.stateLock.Unlock()
	if err != nil {
		utils.SSCLogger().Error().Err(err).Str("txHash", txHash.Hex()).Msg("resimulation failed")
		return
	}

	ctx, cancel := context.WithTimeout(state.Ctx, s.Config.CallTimeout)
	defer func() {
		cancel()
	}()

	header := s.bc.CurrentHeader()
	// request recall origin contract
	lastReq := state.SimulationRequest
	if lastReq == nil {
		utils.SSCLogger().Error().Str("txHash", txHash.Hex()).Msg("failed to get last simulation request")
		return
	}
	req := &api.CXTSimulationRequest{
		TxHash:        txHash,
		Epoch:         lastReq.Epoch,
		SimulationNum: simulationNum,
		Author:        lastReq.Author,
		BlockHash:     header.Hash(),
		Tx:            lastReq.Tx,
		From:          lastReq.From,
		GasPool:       0,
	}

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("start recall simulation, simulationNum: %d", simulationNum)

	committee := s.GetCommittee(req.Epoch, s.SelfShard)
	n := committee.Number
	t := committee.Threshold

	wg := sync.WaitGroup{}
	wg.Add(n)
	var (
		results []api.SSCMessage
		hasSelf bool
		lock    sync.Mutex
	)
	// 预分配容量，但长度动态控制
	results = make([]api.SSCMessage, 0, t)

	for i := 0; i < n; i++ {
		member := committee.Members[i]
		go func(m *api.Member) {
			defer wg.Done()
			ret := new(api.CXTSimulationResult)
			callErr := s.Comm.Call(ctx, ret, member, api.Method_HandleSimulateRequest, req)
			if callErr != nil {
				utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(err).Msg("failed to call simulate request")
				return
			}

			lock.Lock()
			defer lock.Unlock()

			// 如果上下文已取消，直接退出
			if ctx.Err() != nil {
				return
			}

			if bytes.Compare(m.Address.Bytes(), s.SelfAddr.Bytes()) == 0 {
				hasSelf = true
				if len(results) < t {
					results = append([]api.SSCMessage{ret}, results...)
				} else {
					results[0] = ret // 替换最后一个
				}
			} else {
				if len(results) < t {
					results = append(results, ret)
				}
			}

			// 检查是否满足终止条件
			if hasSelf && len(results) == t {
				cancel()
			}
		}(member)
	}
	wg.Wait()

	var (
		sscResult *api.CXTSimulationSSCResult
	)

	if len(results) == t {
		leaderRet := results[0].(*api.CXTSimulationResult)
		if leaderRet.Err != "" {
			sscResult = &api.CXTSimulationSSCResult{Err: leaderRet.Err, RelatedShards: []uint32{s.SelfShard}}
			utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(errors.New(leaderRet.Err)).Msg("failed to simulate transaction")
		} else {
			sscResult, err = s.aggregateSimulationResults(results)
			if err != nil {
				sscResult = &api.CXTSimulationSSCResult{Err: err.Error(), RelatedShards: []uint32{s.SelfShard}}
				utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(err).Msg("failed to aggregate simulation results")
			}
		}
	} else {
		if len(results) == 0 {
			sscResult = &api.CXTSimulationSSCResult{Err: "no response from other shards", RelatedShards: []uint32{s.SelfShard}}
		} else {
			errRet := results[len(results)-1].(*api.CXTSimulationResult)
			sscResult = &api.CXTSimulationSSCResult{Err: errRet.Err, RelatedShards: []uint32{s.SelfShard}}
		}
	}
	sscResult, err = s.aggregateSimulationResults(results)
	if err != nil {
		utils.SSCLogger().Error().Str("txHash", txHash.String()).Err(err).Msg("failed to aggregate simulation results")
		sscResult = &api.CXTSimulationSSCResult{Err: err.Error(), RelatedShards: []uint32{s.SelfShard}}
	} else {
		s.stateLock.Lock()
		state.SimulationResult = sscResult
		if len(state.SimulationCallStates[simulationNum]) == 0 {
			sscResult = &api.CXTSimulationSSCResult{Err: "callStates size is 0", RelatedShards: []uint32{s.SelfShard}}
		} else {
			state.SimulationCallStates[simulationNum][0].TopSSCResult = sscResult
		}
		s.stateLock.Unlock()
	}

	// build and send simulation commit
	simulationCommit := &api.SimulationCommit{
		SimulationNum: req.SimulationNum,
		TxHash:        txHash,
		Nonce:         req.Tx.Nonce(),
		Sender:        req.From,
		RelatedShards: sscResult.RelatedShards,
		BaseBLSSignedMessage: api.BaseBLSSignedMessage{
			Epoch: req.Epoch,
		},
	}

	if len(sscResult.Err) == 0 {
		simulationCommit.Commit = true
		simulationCommit.Status = api.OK
		utils.SSCLogger().Debug().Str("txHash", simulationCommit.TxHash.Hex()).
			Msgf("resimulation accomplished, send simulation commit, commit type: %v, simulationNum: %d, relatedShards: %v",
				simulationCommit.Commit, simulationCommit.SimulationNum, simulationCommit.RelatedShards)
	} else {
		if vm.IsLockedByOtherTxErr(sscResult.Err) {
			utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msgf("resimulation failed err=%s, it has subscribe to resimulate again, nextSimulationNum=%d", sscResult.Err, req.SimulationNum+1)
			simulationCommit.Commit = false
			simulationCommit.Status = api.LockConflict
			if s.IsLeader() {
				s.retryScheduler.CallForRetry(&api.RetryTx{
					TxHash:        txHash,
					Epoch:         req.Epoch,
					RelatedShards: sscResult.RelatedShards,
					SimulationNum: req.SimulationNum + 1,
					Condition:     api.Simulate,
				})
			}
			return
		} else {
			simulationCommit.Commit = false
			simulationCommit.Status = api.ExecutionFailed
		}
		utils.SSCLogger().Error().Str("txHash", simulationCommit.TxHash.Hex()).
			Err(errors.New(sscResult.Err)).Msgf("resimulation accomplished, send simulation commit, commit type: %v, status: %v, simulationNum: %d, relatedShards: %v",
			simulationCommit.Commit, simulationCommit.Status.String(), simulationCommit.SimulationNum, simulationCommit.RelatedShards)
	}

	s.thresholdSignSimulationCommit(simulationCommit)
	members := make([]*api.Member, 0, len(committee.Members))
	for _, shardId := range simulationCommit.RelatedShards {
		members = append(members, s.GetLeader(simulationCommit.Epoch, shardId))
	}
	_ = s.Comm.Multicast(ctx, members, api.Method_CommitSimulation, simulationCommit)
}

func (s *sscService) HandleCXTCommitProof(proof *api.CXTCommitProof) {
	utils.SSCLogger().Debug().Str("txHash", proof.TxHash.Hex()).Msgf("handle commit or rollback proof, type: %v, simulationNum: %d", proof.Type, proof.SimulationNum)
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
		committee := s.GetCommittee(proof.Epoch, s.SelfShard)
		ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
		defer cancel()
		_ = s.Comm.Multicast(ctx, committee.Members, api.Method_HandleCXTRecallProof, proof)
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
	s.finishedTxs[txHash] = commitOrRollback
	s.stateLock.Unlock()
	s.commitLock.Lock()
	delete(s.commitStates, txHash)
	s.commitLock.Unlock()
	s.verifyCtxLock.Lock()
	delete(s.executionVerifyContexts, txHash)
	s.verifyCtxLock.Unlock()
	s.retryScheduler.StaleTx(txHash)
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
		s.retryScheduler.StaleTx(txHash)
	}
}

func (s *sscService) StateLockManager() api.StateLockManager {
	return s.lockStateMgr
}

func (s *sscService) AddRetryTx(tx *api.RetryTx) {
	if !s.IsLeader() {
		return
	}
	retryTx := &api.RetryTx{
		TxHash:        tx.TxHash,
		Epoch:         tx.Epoch,
		Sender:        tx.Sender,
		Nonce:         tx.Nonce,
		GasPrice:      tx.GasPrice,
		OriginShardID: tx.OriginShardID,
		ReadSet:       nil,
		WriteSet:      nil,
		RelatedShards: tx.RelatedShards,
		SimulationNum: tx.SimulationNum,
		Condition:     tx.Condition,
	}
	readSet := make([]api.LockKey, 0)
	writeSet := make([]api.LockKey, 0)

	s.stateLock.Lock()
	state, err := s.getState(tx.TxHash)
	if err != nil {
		s.stateLock.Unlock()
		utils.SSCLogger().Error().Str("txHash", tx.TxHash.Hex()).Err(err).Msg("failed to get state")
		return
	}
	for _, callState := range state.SimulationCallStates[tx.SimulationNum] {
		for addr, account := range callState.RWSet.ReadState.State {
			for key, _ := range account {
				readSet = append(readSet, api.FormKey(addr, key))
			}
		}
		for addr, account := range callState.RWSet.WriteState.State {
			for key, _ := range account {
				writeSet = append(writeSet, api.FormKey(addr, key))
			}
		}
	}
	retryTx.ReadSet = readSet
	retryTx.WriteSet = writeSet
	s.stateLock.Unlock()

	s.retryScheduler.AddToRetry(retryTx)
}

func (s *sscService) RetryCommit(txHash common.Hash) *api.RetryCommitResp {
	locked := s.retryScheduler.RetryCommit(txHash)
	utils.SSCLogger().Info().Str("txHash", txHash.Hex()).Msgf("retry commit, locked: %v", locked)
	return &api.RetryCommitResp{
		Locked: locked,
		TxHash: txHash,
	}
}

func (s *sscService) RetryCancel(txHash common.Hash) {
	s.retryScheduler.RetryCancel(txHash)
}
