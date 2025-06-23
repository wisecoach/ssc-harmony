package ssc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/event"
	"github.com/harmony-one/harmony/block"
	"github.com/harmony-one/harmony/core"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/core/vm"
	"github.com/harmony-one/harmony/hmy"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/rs/zerolog"
	"math"
	"math/big"
	"sort"
	"sync"
)

type sscService struct {
	*CommitteeMechanism
	Comm        *Comm
	BLSSigner   api.BLSSigner
	txSigner    api.TxSigner
	txSubmitter api.TxSubmitter

	Config *api.Config
	bc     core.BlockChain

	logger *zerolog.Logger

	simulationState     map[common.Hash]*api.CXTSimulationState
	stateLock           sync.RWMutex
	commitStates        map[common.Hash]*api.CommitState
	pendingSimulation   []*api.CXTSimulation
	pendingCommitProofs []*api.CXTCommitProof
	commitLock          sync.RWMutex

	executionVerifyContexts map[common.Hash]*api.ExecutionVerifyContext // txHash -> ExecutionVerifyContext
	callStatesInWaiting     map[common.Hash][]*api.SimulationCallState
	simuLock                sync.Mutex
	simuWaitingChs          map[common.Hash][]chan *api.CXTSimulationSSCResult
	simuResultCh            map[common.Hash]chan *api.CXTSimulationSSCResult

	ctx          context.Context
	chainHeadCh  chan core.ChainHeadEvent
	chainHeadSub event.Subscription
}

func NewService(ctx context.Context, config *api.Config, cm *CommitteeMechanism,
	signer api.BLSSigner, nodeAPI hmy.NodeAPI, bc core.BlockChain, txSigner api.TxSigner) api.Service {
	logger := utils.Logger()
	state, _ := bc.State()
	txSub := &txSubmitter{
		lock:      sync.Mutex{},
		selfShard: cm.SelfShard,
		txSigner:  txSigner,
		nonce:     state.GetNonce(txSigner.Address()),
		nodeAPI:   nodeAPI,
		config:    config,
	}
	service := &sscService{
		CommitteeMechanism:      cm,
		Comm:                    NewComm(),
		BLSSigner:               signer,
		txSigner:                txSigner,
		Config:                  config,
		txSubmitter:             txSub,
		bc:                      bc,
		logger:                  logger,
		simulationState:         make(map[common.Hash]*api.CXTSimulationState),
		commitStates:            make(map[common.Hash]*api.CommitState),
		executionVerifyContexts: make(map[common.Hash]*api.ExecutionVerifyContext),
		stateLock:               sync.RWMutex{},
		callStatesInWaiting:     make(map[common.Hash][]*api.SimulationCallState),
		simuLock:                sync.Mutex{},
		simuWaitingChs:          make(map[common.Hash][]chan *api.CXTSimulationSSCResult),
		simuResultCh:            make(map[common.Hash]chan *api.CXTSimulationSSCResult),
		commitLock:              sync.RWMutex{},
		ctx:                     ctx,
		chainHeadCh:             make(chan core.ChainHeadEvent, 10),
		chainHeadSub:            nil,
	}
	subscription := bc.SubscribeChainHeadEvent(service.chainHeadCh)
	service.chainHeadSub = subscription
	go service.loop()
	return service
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
			s.lock.Lock()
			callStatesInWaiting := s.callStatesInWaiting[b.Header().Hash()]
			delete(s.callStatesInWaiting, b.Header().Hash())
			s.lock.Unlock()
			for _, callState := range callStatesInWaiting {
				callState.SyncedCh <- struct{}{}
			}
		}
	}
}

func (s *sscService) startSimulation(req *api.CXTSimulationRequest) *api.SimulationCallState {
	txHash := common.BytesToHash(req.TxHash)
	s.startCXT(txHash, req.SimulationNum, api.CallIndex{}, s.SelfShard, make([]uint32, 0), req, nil)

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
			s.logger.Error().Msg("failed to get block header")
			return nil
		}
	}
	db, err := s.bc.StateAt(header.Root())
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to get state")
		return nil
	}
	callState.DB = db

	s.lock.Lock()
	s.simulationState[txHash].SimulationCallStates[req.SimulationNum] = s.simulationState[txHash].SimulationCallStates[req.SimulationNum].Add(callState)
	s.lock.Unlock()

	return callState
}

func (s *sscService) startCXT(txHash common.Hash, simulationNum int, callIndex api.CallIndex, originShardId uint32, relatedShards []uint32,
	topRequest *api.CXTSimulationRequest, callRequest *api.CXTCallSSCRequest) bool {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	state := s.simulationState[txHash]

	if state != nil {
		return false
	}

	// add self shard to related shards
	hasCalled := false
	for _, shardID := range relatedShards {
		if shardID == s.SelfShard {
			hasCalled = true
			break
		}
	}
	if !hasCalled {
		relatedShards = append(relatedShards, s.SelfShard)
	}
	relatedShardMap := make(map[uint32]struct{})
	for _, shardId := range relatedShards {
		relatedShardMap[shardId] = struct{}{}
	}

	// set cxt timeout ctx
	timeoutCtx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)

	simuState := &api.CXTSimulationState{
		CallIndex:            callIndex,
		CurrentIndex:         simulationNum,
		SimulationRequest:    topRequest,
		SimulationCallStates: make(map[int]api.SimulationCallStates),
		SimulationNum:        0,
		LockedCallIndex:      nil,
		OriginShardId:        originShardId,
		RelatedShards:        relatedShards,
		RelatedShardMap:      relatedShardMap,
		TimeoutCtx:           timeoutCtx,
		TimeoutCancel:        cancel,
	}

	simuState.SimulationCallStates[simulationNum] = make(api.SimulationCallStates, 0)

	go func() {
		<-timeoutCtx.Done()
		if errors.Is(timeoutCtx.Err(), context.DeadlineExceeded) {
			// TODO handle cxt timeout
			cancel()
		}
		// if timeoutCtx is canceled, it means the simulation has finished
	}()

	s.simulationState[txHash] = simuState
	return true
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

func (s *sscService) SimulationResult(txHash common.Hash) (*api.CXTSimulationSSCResult, error) {
	s.simuLock.Lock()
	defer s.simuLock.Unlock()
	ch := s.simuResultCh[txHash]
	if ch == nil {
		return nil, errors.New("it haven't started simulation")
	}
	return <-ch, nil
}

func (s *sscService) SimulateCXTransaction(req *api.CXTSimulationRequest) {
	s.logger.Info().Str("txHash", req.Tx.Hash().String()).Msg("simulate cx transaction, start")
	leader := s.GetLeader(s.SelfShard, req.Tx.Hash())

	ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
	ch := make(chan *api.CXTSimulationSSCResult)
	s.simuLock.Lock()
	s.simuResultCh[req.Tx.Hash()] = ch
	s.simuLock.Unlock()

	go func() {
		defer cancel()
		ret := new(api.CXTSimulationSSCResult)
		err := s.Comm.Call(ctx, ret, leader, api.Method_StartSimulateCXTransaction, req)
		if err != nil {
			ret = &api.CXTSimulationSSCResult{Err: err.Error()}
		}
		ch <- ret
		s.logger.Info().Str("txHash", req.Tx.Hash().String()).Msg("simulate cx transaction, end")
	}()
}

func (s *sscService) CallCXTContract(req *api.CXTCallRequest) *api.CXTCallSSCResult {
	txHash := common.BytesToHash(req.TxHash)

	s.stateLock.RLock()
	cxtState := s.simulationState[txHash]
	if cxtState == nil {
		s.stateLock.RUnlock()
		return &api.CXTCallSSCResult{Err: "no simulation state found"}
	}
	s.stateLock.RUnlock()

	req.SimulationNum = cxtState.SimulationNum
	req.OriginShardId = cxtState.OriginShardId
	req.FromShardId = s.SelfShard
	req.CallIndex = make(api.CallIndex, len(cxtState.CallIndex))
	copy(req.CallIndex, cxtState.CallIndex)
	req.CallIndex = append(req.CallIndex, cxtState.CurrentIndex)
	cxtState.CurrentIndex++

	leader := s.GetLeader(s.SelfShard, txHash)

	s.logger.Info().
		Str("txHash", txHash.String()).
		Str("cxtStateCallIndex", cxtState.CallIndex.ToString()).
		Str("reqCallIndex", req.CallIndex.ToString()).
		Str("leader", leader.Endpoint).
		Str("callIndex", req.CallIndex.ToString()).Msg("call cxt contract, start")
	defer s.logger.Info().Str("txHash", txHash.String()).Msg("call cxt contract, end")

	ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
	defer cancel()

	ret := new(api.CXTCallSSCResult)
	err := s.Comm.Call(ctx, ret, leader, api.Method_RequestCallCXT, req)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to call cxt contract")
		return &api.CXTCallSSCResult{Err: err.Error()}
	}
	if ret.Err != "" {
		s.logger.Error().Str("txHash", txHash.String()).Str("err", ret.Err).Msg("failed to call cxt contract")
	}
	return ret
}

// VerifySimulation
//  1. unmarshal simulation and verify the signature of the simulation
//     1 [x] return: it means ssc is malicious, all nodes should send a rollback vote to origin-shard's ssc leader directly
//  2. execute the called contract with VerifyExecution and check if the write set is consistent with the simulation's write set
//     2 [x] return: it means ssc is malicious, it make a invalid simulation, all nodes should send a rollback vote to origin-shard's ssc leader directly
//  3. check each cross-call if the state is consistent with the simulation's read set
//     3 [y] if all cross-call is consistent, return send a commit vote to self-shard's ssc leader
//     3 [x] if there is a inconsistent cross-call, re-execute the first inconsistent cross-call and lock the state with LockExecution
//     3 [x|x] return: if re-execute failed, all nodes should send a rollback vote to ssc leader and then send signed vote to origin-shard's ssc leader
//     3 [x|y] return: if the first inconsistent cross-call is re-executed and locks state successfully, send a recall vote with conflict callIndex to ssc leader
//
// Note: thread unsafe
func (s *sscService) VerifySimulation(simulationBytes []byte) {
	// 1
	simulation := &api.CXTSimulation{}
	err := json.Unmarshal(simulationBytes, simulation)
	// 1 [x]
	if err != nil {
		payload := &api.CXTInvalidSimulationPayload{
			Type: api.InvalidSerialization,
		}
		payloadBytes, _ := json.Marshal(payload)
		vote := &api.CXTCommitVote{
			TxHash:        simulation.TxHash,
			Type:          api.Rollback,
			ShardId:       s.SelfShard,
			OriginShardId: simulation.OriginShardId,
			Reason:        api.Reason_InvalidSimulation,
			Payload:       payloadBytes,
		}
		s.sendCXTCommitVote(simulation.OriginShardId, vote)
		return
	}
	// 1 TODO verify the signature
	if err != nil {
		payload := &api.CXTInvalidSimulationPayload{
			Type: api.InvalidSignature,
		}
		payloadBytes, _ := json.Marshal(payload)
		vote := &api.CXTCommitVote{
			TxHash:        simulation.TxHash,
			Type:          api.Rollback,
			OriginShardId: simulation.OriginShardId,
			Reason:        api.Reason_InvalidSimulation,
			Payload:       payloadBytes,
		}
		s.sendCXTCommitVote(simulation.OriginShardId, vote)
		return
	}

	// 2
	if !s.verifyExecutionState(simulation) {
		return
	}

	// 3
	valid := s.checkStateAndReadSet(simulation)

	// 3 [y]
	if valid {
		// tempDelete s.logger.Info().Msgf("simulation is valid, txHash: %s", simulation.TxHash)
		vote := &api.CXTCommitVote{
			TxHash:        simulation.TxHash,
			ShardId:       s.SelfShard,
			OriginShardId: simulation.OriginShardId,
			Type:          api.Commit,
			Reason:        api.Reason_SUCCESS,
			Payload:       nil,
		}
		s.sendCXTCommitVote(simulation.OriginShardId, vote)
	}
}

func (s *sscService) sendCXTCommitVote(shardId uint32, vote *api.CXTCommitVote) {
	ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
	defer cancel()

	targetLeader := s.GetLeader(shardId, common.BytesToHash(vote.TxHash))

	s.logger.Info().Msgf("send CXTCommitVote to %s", targetLeader.Address.String())
	s.Comm.Call(ctx, nil, targetLeader, api.Method_HandleCommitVote, vote)
}

func (s *sscService) verifyExecutionState(simulation *api.CXTSimulation) bool {
	valid := true
	txHash := common.BytesToHash(simulation.TxHash)
	callStateMap := make(map[string]*api.CXTCallState)
	for _, callState := range simulation.CallStates {
		callStateMap[callState.CallIndex.ToString()] = callState
	}
	verifyContext := &api.ExecutionVerifyContext{Simulation: simulation, CallStateMap: callStateMap}
	s.executionVerifyContexts[txHash] = verifyContext

	state, _ := s.bc.State()
	chainConfig := s.bc.Config()
	vmConfig := s.bc.GetVMConfig()
	for _, callState := range simulation.CallStates {
		var (
			origin      common.Address
			callIndex   api.CallIndex
			gasPrice    *big.Int
			fromShardId uint32
			addr        common.Address
			input       []byte
			gas         uint64
			value       *big.Int
			header      *block.Header
			simuRet     []byte
		)

		if callState.TopRequest != nil {
			req := callState.TopRequest
			header = s.bc.GetHeaderByHash(common.BytesToHash(req.BlockHash))
			tx := req.Tx
			var signer types.Signer
			if tx.IsEthCompatible() {
				if !chainConfig.IsEthCompatible(header.Epoch()) {
					return false
				}
				signer = types.NewEIP155Signer(chainConfig.EthCompatibleChainID)
			} else {
				signer = types.MakeSigner(chainConfig, header.Epoch())
			}
			msg, err := tx.AsMessage(signer)
			if err != nil {
				return false
			}
			origin = msg.From()
			callIndex = api.CallIndex{}
			gasPrice = req.Tx.GasPrice()
			fromShardId = s.SelfShard
			addr = *req.Tx.To()
			input = req.Tx.Data()
			gas = req.Tx.GasLimit()
			value = req.Tx.Value()
			simuRet = callState.TopResult.Result
		} else {
			req := callState.CallRequest
			blockHash := common.BytesToHash(req.BlockHash)
			header = s.bc.GetHeaderByHash(blockHash)
			origin = req.Caller
			callIndex = req.CallIndex
			gasPrice = req.GasPrice
			fromShardId = req.FromShardId
			addr = req.Addr
			input = req.Input
			gas = req.Gas
			value = req.Value
			simuRet = callState.CallResult.Result
		}

		verifyContext.CurrentState = newStateSet()
		verifyContext.DependentResults = callState.DependentResults
		verifyContext.CallIndex = callIndex
		verifyContext.CurrentIndex = 0

		// prepare execution verify context
		sender := vm.AccountRef(origin)
		vmCtx := core.NewSSCVMContext(origin, txHash, callIndex, gasPrice, header, s.bc, nil)
		sscvm := vm.NewSSCVM(vmCtx, state, chainConfig, *vmConfig, s, vm.ExecutionVerify)

		// execute contract with ExecutionVerify
		ret, _, err := sscvm.CallFromOtherShard(fromShardId, sender, addr, input, gas, value)
		if err != nil {
			valid = false
			break
		}

		// compare the result and leftOverGas
		if bytes.Compare(ret, simuRet) != 0 {
			valid = false
			break
		}
		if !verifyContext.CurrentState.Equal(callState.RWSet.WriteState) {
			valid = false
			break
		}
	}

	// 2 [x]
	if !valid {
		payload := &api.CXTInvalidSimulationPayload{
			Type: api.InvalidExecution,
		}
		payloadBytes, _ := json.Marshal(payload)
		vote := &api.CXTCommitVote{
			TxHash:        simulation.TxHash,
			Type:          api.Rollback,
			ShardId:       s.SelfShard,
			OriginShardId: simulation.OriginShardId,
			Reason:        api.Reason_InvalidSimulation,
			Payload:       payloadBytes,
		}
		s.sendCXTCommitVote(simulation.OriginShardId, vote)
	}

	return valid
}

func (s *sscService) checkStateAndReadSet(simulation *api.CXTSimulation) bool {
	txHash := common.BytesToHash(simulation.TxHash)
	conflictCallIndexes := make([]api.CallIndex, 0)
	state, _ := s.bc.State()
Outer:
	for _, callState := range simulation.CallStates {
		callConflict := false
		// check if read set is conflict with current state
	ReadSetCheck:
		for address, stateMap := range callState.RWSet.ReadState.State {
			for key, value := range stateMap {
				s, _ := state.GetState(address, key)
				if bytes.Compare(value.Bytes(), s.Bytes()) != 0 {
					callConflict = true
					break ReadSetCheck
				}
			}
		}
		if callConflict {
			err := s.lockStateWithExecution(callState)
			// 3 [x|x]
			if err != nil {
				vote := &api.CXTCommitVote{
					TxHash:        simulation.TxHash,
					ShardId:       s.SelfShard,
					OriginShardId: simulation.OriginShardId,
					Type:          api.Rollback,
					Reason:        api.Reason_ConflictRWSet_FailedLock,
					Payload:       nil,
				}
				s.sendCXTCommitVote(s.SelfShard, vote)
				return false
			}
			conflictCallIndexes = append(conflictCallIndexes, callState.CallIndex)
			break Outer
		} else {
			s.lockStateWithRWSet(txHash, callState)
		}
	}

	// 3 [x|y]
	if len(conflictCallIndexes) > 0 {
		payload := &api.CXTConflictRWSetPayload{
			ConflictCallIndex: conflictCallIndexes[0],
		}
		payloadBytes, _ := json.Marshal(payload)
		vote := &api.CXTCommitVote{
			TxHash:        simulation.TxHash,
			ShardId:       s.SelfShard,
			Type:          api.Recall,
			OriginShardId: simulation.OriginShardId,
			Reason:        api.Reason_ConflictRWSet_Recall,
			Payload:       payloadBytes,
		}
		s.sendCXTCommitVote(s.SelfShard, vote)
		return false
	}

	// 3 [y]
	return true
}

func (s *sscService) lockStateWithExecution(callState *api.CXTCallState) error {
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
		caller = topReq.SenderAddr
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
	state, _ := s.bc.State()
	header := s.bc.CurrentHeader()
	vmCtx := core.NewSSCVMContext(caller, txHash, callIndex, gasPrice, header, s.bc, nil)
	sscvm := vm.NewSSCVM(vmCtx, state, s.bc.Config(), *s.bc.GetVMConfig(), s, vm.LockExecution)

	// execute contract with LockExecution to lock state and save recall simulation to handle recall request
	ret, leftOverGas, err := sscvm.Call(vm.AccountRef(caller), addr, input, gas, value)
	result := &api.CXTCallResult{
		CallIndex:      callIndex,
		RelatedShards:  relatedShards,
		Result:         ret,
		LeftOverGas:    leftOverGas,
		BlockHash:      header.Hash().Bytes(),
		BaseSSCMessage: nil,
	}
	if err != nil {
		result.Err = err.Error()
	}

	simulationState := s.getState(txHash)
	// cache the recall state for the next simulation recall state
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

	return err
}

func (s *sscService) lockStateWithRWSet(txHash common.Hash, callState *api.CXTCallState) {
	state, _ := s.bc.LockableState()
	for address, stateMap := range callState.RWSet.WriteState.State {
		for key, value := range stateMap {
			_ = state.SetStateWithLock(txHash, callState.CallIndex, address, key, value)
		}
	}
}

func (s *sscService) CommitOrRollbackWithProof(unlock bool, commitProofBytes []byte) {
	commitProof := &api.CXTCommitProof{}
	err := json.Unmarshal(commitProofBytes, commitProof)
	// TODO verify
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to unmarshal commit proof")
		return
	}
	lockableState, _ := s.bc.LockableState()
	txHash := common.BytesToHash(commitProof.TxHash)
	if commitProof.Type == api.Commit {
		if unlock {
			s.logger.Info().Str("txHash", txHash.String()).Msg("commit with proof")
		}
		lockableState.Commit(unlock, txHash)
	} else if commitProof.Type == api.Rollback {
		if unlock {
			s.logger.Info().Str("txHash", txHash.String()).Msg("rollback with proof")
		}
		lockableState.Rollback(unlock, txHash)
	}

	if unlock {
		// clear the context
		s.closeTransaction(txHash)
	}
}

func (s *sscService) StartSimulateCXTransaction(req *api.CXTSimulationRequest) *api.CXTSimulationSSCResult {
	var waitingCh chan *api.CXTSimulationSSCResult
	txHash := req.Tx.Hash()

	s.logger.Info().Str("txHash", txHash.String()).Msg("start simulate cx transaction, start")
	defer s.logger.Info().Str("txHash", txHash.String()).Msg("start simulate cx transaction, end")

	s.simuLock.Lock()
	if s.simuWaitingChs[txHash] == nil {
		s.simuWaitingChs[txHash] = make([]chan *api.CXTSimulationSSCResult, 0)
	} else {
		waitingCh = make(chan *api.CXTSimulationSSCResult)
		s.simuWaitingChs[txHash] = append(s.simuWaitingChs[txHash], waitingCh)
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

	req.BlockHash = s.bc.CurrentHeader().Hash().Bytes()
	committee := s.GetCommittee(s.SelfShard)
	t := committee.Threshold

	ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(t)
	lock := sync.Mutex{}
	results := make([]api.SSCMessage, 0, t)

	for _, member := range committee.Members {
		go func() {
			ret := new(api.CXTSimulationResult)
			err := s.Comm.Call(ctx, ret, member, api.Method_HandleSimulateRequest, req)
			if err != nil {
				s.logger.Error().Err(err).Msg("failed to call simulate request")
				return
			}
			lock.Lock()
			if len(results) < t {
				results = append(results, ret)
			}
			lock.Unlock()
			wg.Done()
		}()
	}
	wg.Wait()
	sscResult, err := s.aggregateSimulationResults(results)
	if err != nil {
		sscResult = &api.CXTSimulationSSCResult{Err: err.Error(), RelatedShards: []uint32{s.SelfShard}}
	}

	state := s.getState(req.Tx.Hash())
	state.SimulationCallStates[state.SimulationNum][0].TopSSCResult = sscResult

	var simulationCommit *api.SimulationCommit
	if len(sscResult.Err) != 0 {
		simulationCommit = &api.SimulationCommit{
			TxHash:               req.Tx.Hash().Bytes(),
			RelatedShards:        sscResult.RelatedShards,
			Commit:               false,
			Status:               api.ExecutionFailed,
			BaseBLSSignedMessage: &api.BaseBLSSignedMessage{},
		}
	} else {
		simulationCommit = &api.SimulationCommit{
			TxHash:               req.Tx.Hash().Bytes(),
			RelatedShards:        sscResult.RelatedShards,
			Commit:               true,
			Status:               api.OK,
			BaseBLSSignedMessage: &api.BaseBLSSignedMessage{},
		}
	}
	s.thresholdSignSimulationCommit(simulationCommit)
	leaders := make([]*api.Member, 0, len(committee.Members))
	for _, shardId := range simulationCommit.RelatedShards {
		leaders = append(leaders, s.GetLeader(shardId, common.BytesToHash(simulationCommit.TxHash)))
	}
	s.Comm.Multicast(ctx, leaders, api.Method_CommitSimulation, simulationCommit)

	s.simuLock.Lock()
	chs := s.simuWaitingChs[txHash]
	s.simuLock.Unlock()

	for _, ch := range chs {
		ch <- sscResult
	}

	return sscResult
}

func (s *sscService) thresholdSignSimulationCommit(commit *api.SimulationCommit) {
	committee := s.GetCommittee(s.SelfShard)
	t := committee.Threshold

	ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(t)
	lock := sync.Mutex{}
	baseMsgs := make([]api.SSCMessage, 0)

	for _, member := range committee.Members {
		go func() {
			signature := make([]byte, 0)
			err := s.Comm.Call(ctx, signature, member, api.Method_SignSimulationCommit, commit)
			if err != nil {
				return
			}
			lock.Lock()
			if len(baseMsgs) < t {
				baseMsgs = append(baseMsgs, &api.BaseSSCMessage{
					Signature:  signature,
					SenderAddr: member.Address,
				})
			}
			lock.Unlock()
			wg.Done()
		}()
	}
	wg.Wait()
	aggregatedSig, bitMap, err := s.BLSSigner.Aggregate(baseMsgs)
	if err != nil {
		s.logger.Error().Err(err)
		return
	}
	commit.Signatures = aggregatedSig
	commit.BLSBitMap = bitMap
}

func (s *sscService) aggregateSimulationResults(results []api.SSCMessage) (*api.CXTSimulationSSCResult, error) {
	result := results[0].(*api.CXTSimulationResult)
	aggregatedSig, bitMap, err := s.BLSSigner.Aggregate(results)
	if err != nil {
		return nil, err
	}
	sscResult := &api.CXTSimulationSSCResult{
		RelatedShards: result.RelatedShards,
		Result:        result.Result,
		Receipt:       result.Receipt,
		UsedGas:       result.UsedGas,
		Err:           result.Err,
		BaseBLSSignedMessage: &api.BaseBLSSignedMessage{
			ShardId:    s.SelfShard,
			Signatures: aggregatedSig,
			BLSBitMap:  bitMap,
		},
	}
	return sscResult, nil
}

func (s *sscService) HandleSimulateRequest(req *api.CXTSimulationRequest) *api.CXTSimulationResult {
	s.logger.Info().Str("txHash", req.Tx.Hash().String()).Msg("handle simulate request, start")
	defer s.logger.Info().Str("txHash", req.Tx.Hash().String()).Msg("handle simulate request, end")
	callState := s.startSimulation(req)
	if callState == nil {
		return &api.CXTSimulationResult{Err: "failed to start simulation"}
	}
	callState.Lock.Lock()
	defer callState.Lock.Unlock()

	header := s.bc.GetHeaderByHash(common.BytesToHash(req.BlockHash))
	state, _ := s.bc.StateAt(header.Root())
	chainConfig := s.bc.Config()
	vmConfig := s.bc.GetVMConfig()

	var (
		gp            *core.GasPool
		tx            *types.Transaction
		txHash        common.Hash
		executionType vm.ExecutionType
	)
	txHash = common.BytesToHash(req.TxHash)

	if req.SimulationNum == 0 {
		tx = req.Tx
		gp = new(core.GasPool).AddGas(req.GasPool)
		executionType = vm.SimulationCall
		// init cxt state
	} else {
		simuState := s.simulationState[txHash]
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
		return &api.CXTSimulationResult{Err: err.Error()}
	}

	ctx := core.NewSSCVMContext(msg.From(), tx.Hash(), api.CallIndex{}, tx.GasPrice(), header, s.bc, req.Author)
	ctx.TxType = types.CXTransaction
	sscvm := vm.NewSSCVM(ctx, state, chainConfig, *vmConfig, s, executionType)
	result, err := core.NewSSCStateTransition(sscvm, msg, gp).TransitionDb()
	if err != nil {
		return &api.CXTSimulationResult{Err: err.Error()}
	}

	simuState := s.getState(txHash)
	return &api.CXTSimulationResult{
		RelatedShards: simuState.RelatedShards,
		Result:        result.ReturnData,
		Receipt:       nil,
		UsedGas:       result.UsedGas,
		Err:           "",
	}
}

func (s *sscService) RequestCallCXT(req *api.CXTCallRequest) *api.CXTCallSSCResult {
	txHash := common.BytesToHash(req.TxHash)
	committee := s.GetCommittee(s.SelfShard)
	s.stateLock.Lock()
	cxtState := s.simulationState[txHash]
	if cxtState == nil {
		s.stateLock.Unlock()
		return &api.CXTCallSSCResult{Err: "no simulation state found"}
	}
	callStates := cxtState.SimulationCallStates[req.SimulationNum]
	s.logger.Info().Hex("txHash", req.TxHash).Str("callIndex", req.CallIndex.ToString()).Msgf("request call cxt, callStates: %s", callStates.ToString())
	// get caller's call state
	callerState := callStates.Get(req.CallIndex[:len(req.CallIndex)-1])
	if callerState == nil {
		s.stateLock.Unlock()
		return &api.CXTCallSSCResult{Err: "no call state found"}
	}
	dependentCall := callerState.DependentCXTCalls[req.CallIndex.ToString()]
	if dependentCall != nil && callerState.Executed {
		s.stateLock.Unlock()
		return callerState.CallSSCResult
	}
	waitingCh := make(chan *api.CXTCallSSCResult, 1)
	if dependentCall == nil {
		dependentCall = &api.DependentCXTCall{
			CallIndex:     req.CallIndex,
			Requests:      make([]*api.CXTCallRequest, 0, committee.Threshold),
			SignedRequest: nil,
			SSCResult:     nil,
			Executed:      false,
			WaitingChs:    make([]chan *api.CXTCallSSCResult, 0),
		}
		dependentCall.Requests = append(dependentCall.Requests, req)
		callerState.DependentCXTCalls[req.CallIndex.ToString()] = dependentCall
	}
	dependentCall.WaitingChs = append(dependentCall.WaitingChs, waitingCh)
	if len(dependentCall.Requests) < committee.Threshold {
		dependentCall.Requests = append(dependentCall.Requests, req)
	}
	s.stateLock.Unlock()

	s.logger.Info().Str("txHash", txHash.String()).Str("callIndex", req.CallIndex.ToString()).Msgf("request call cxt, waiting [%d/%d]", len(dependentCall.Requests), committee.Threshold)

	if len(dependentCall.Requests) == committee.Threshold {
		// aggregate signatures
		// send request to leader
		leader := s.GetLeader(req.TargetShardId, txHash)
		sscReq := s.aggregateSSCCallRequest(dependentCall.Requests)
		dependentCall.SignedRequest = sscReq

		s.logger.Info().Str("txHash", txHash.String()).Str("leader", leader.Endpoint).Str("callIndex", req.CallIndex.ToString()).Msg("request call cxt, start")
		defer s.logger.Info().Str("txHash", txHash.String()).Msg("request call cxt, end")

		ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
		defer cancel()
		ret := new(api.CXTCallSSCResult)
		err := s.Comm.Call(ctx, ret, leader, api.Method_HandleCXTSSCCall, sscReq)
		if err != nil {
			ret = &api.CXTCallSSCResult{Err: err.Error()}
		}
		s.stateLock.Lock()
		dependentCall.SSCResult = ret
		dependentCall.Executed = true
		s.stateLock.Unlock()
		for _, ch := range dependentCall.WaitingChs {
			ch <- ret
		}
	}

	return <-waitingCh
}

func (s *sscService) aggregateSSCCallRequest(results []*api.CXTCallRequest) *api.CXTCallSSCRequest {
	msgs := make([]api.SSCMessage, 0, len(results))
	for _, msg := range results {
		msgs = append(msgs, msg)
	}
	result := results[0]
	aggregatedSig, bitMap, _ := s.BLSSigner.Aggregate(msgs)
	sscResult := &api.CXTCallSSCRequest{
		OriginShardId: result.OriginShardId,
		FromShardId:   result.FromShardId,
		TargetShardId: result.TargetShardId,
		SimulationNum: result.SimulationNum,
		RelatedShards: result.RelatedShards,
		TxHash:        result.TxHash,
		CallIndex:     result.CallIndex,
		Caller:        result.Caller,
		Addr:          result.Addr,
		Input:         result.Input,
		Gas:           result.Gas,
		GasPrice:      result.GasPrice,
		Value:         result.Value,
		BaseBLSSignedMessage: &api.BaseBLSSignedMessage{
			ShardId:    s.SelfShard,
			Signatures: aggregatedSig,
			BLSBitMap:  bitMap,
		},
	}
	return sscResult
}

func (s *sscService) HandleCXTCall(req *api.CXTCallSSCRequest) *api.CXTCallResult {
	// begin cxt call, update simulationState and callIndex
	callState := s.startCall(req)
	defer s.endCall(req)
	callState.Lock.Lock()
	defer callState.Lock.Unlock()

	s.logger.Info().Hex("txHash", req.TxHash).Str("callIndex", req.CallIndex.ToString()).Msg("handle cxt call, start")
	defer s.logger.Info().Hex("txHash", req.TxHash).Msg("handle cxt call, end")

	txHash := common.BytesToHash(req.TxHash)
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

	// create sscvm instance
	vmCtx := core.NewSSCVMContext(req.Caller, txHash, req.CallIndex, req.GasPrice, header, s.bc, nil)
	sscvm := vm.NewSSCVM(vmCtx, state, chainConfig, *vmConfig, s, executionType)

	// call contract with SimulationCall
	ret, leftOverGas, err := sscvm.CallFromOtherShard(req.FromShardId, sender, req.Addr, req.Input, req.Gas, req.Value)
	simuState := s.getState(txHash)
	result := &api.CXTCallResult{
		CallIndex:     req.CallIndex,
		RelatedShards: simuState.RelatedShards,
		Result:        ret,
		LeftOverGas:   leftOverGas,
		BlockHash:     header.Hash().Bytes(),
	}
	if err != nil {
		result.Err = err.Error()
	}
	return result
}

func (s *sscService) startCall(req *api.CXTCallSSCRequest) *api.SimulationCallState {
	// try to start cxt if not exist
	txHash := common.BytesToHash(req.TxHash)
	s.startCXT(txHash, req.SimulationNum, req.CallIndex, req.OriginShardId, req.RelatedShards, nil, req)

	s.lock.RLock()
	state := s.simulationState[txHash]
	s.lock.RUnlock()
	// update new callIndex and clear current index
	state.OldCallIndex = state.CallIndex
	state.OldCurrentIndex = state.CurrentIndex
	state.CallIndex = req.CallIndex
	state.CurrentIndex = 0

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
			s.logger.Info().Msgf("block header not found, waiting for sync")
			s.waitForSync(callState)
		}
		s.bc.StateAt(header.Root())
		state.SimulationCallStates[state.SimulationNum] = state.SimulationCallStates[state.SimulationNum].Add(callState)
	}
	return callState
}

func (s *sscService) endCall(req *api.CXTCallSSCRequest) {
	txHash := common.BytesToHash(req.TxHash)
	s.lock.RLock()
	state := s.simulationState[txHash]
	state.CurrentIndex = state.OldCurrentIndex
	state.CallIndex = state.OldCallIndex
	s.lock.RUnlock()
}

func (s *sscService) waitForSync(callState *api.SimulationCallState) {
	callState.SyncedCh = make(chan struct{})

	s.lock.Lock()
	callStates, exists := s.callStatesInWaiting[callState.BlockHash]
	if !exists {
		callStates = make([]*api.SimulationCallState, 0)
	}
	s.callStatesInWaiting[callState.BlockHash] = append(callStates, callState)
	s.lock.Unlock()

	select {
	case <-callState.SyncedCh:
		s.logger.Debug().Msgf("callState %s synced", callState.CallIndex.ToString())
	case <-s.ctx.Done():
		// tempDelete s.logger.Info().Msg("sscService context done")
	}
}

func (s *sscService) aggregateSSCRecallRequest(requests []*api.CXTRecallRequest) *api.CXTRecallSSCRequest {
	req := requests[0]
	// TODO implement it with BLS
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
	}
}

func (s *sscService) SignSimulationCommit(commit *api.SimulationCommit) []byte {
	signature, err := s.BLSSigner.Sign(commit)
	if err != nil {
		return nil
	}
	return signature
}

func (s *sscService) SignCXTSimulation(simulation *api.CXTSimulation) []byte {
	signature, err := s.BLSSigner.Sign(simulation)
	if err != nil {
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
	if vote.Type == api.Recall {
		threshold = int(math.Ceil(float64(len(s.GetCommittee(vote.ShardId).Members)) / 2))
	} else {
		threshold = int(math.Ceil(float64(len(s.GetValidators(vote.ShardId))) / 2))
	}
	if vote.ShardId != s.SelfShard && vote.Type == api.Rollback && vote.Reason == api.Reason_InvalidSimulation {
		return
	}
	// 2
	s.commitLock.Lock()
	txHash := common.BytesToHash(vote.TxHash)
	if s.commitStates[txHash] == nil {
		s.commitStates[txHash] = &api.CommitState{
			CommitVotes:    make(map[int]map[uint32][]*api.CXTCommitVote),
			CommitSSCVotes: make(map[int]map[uint32]*api.CXTCommitSSCVote),
		}
	}
	if s.commitStates[txHash].CommitVotes[vote.SimulationNum] == nil {
		s.commitStates[txHash].CommitVotes[vote.SimulationNum] = make(map[uint32][]*api.CXTCommitVote)
	}
	if s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId] == nil {
		s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId] = make([]*api.CXTCommitVote, 0)
	}
	if len(s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId]) >= threshold {
		s.commitLock.Unlock()
		return
	}
	s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId] = append(s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId], vote)
	s.logger.Info().Str("txHash", txHash.Hex()).Msgf("received commit vote [%d/%d]", len(s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId]), threshold)
	reachThreshold := len(s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId]) == threshold
	s.commitLock.Unlock()
	if reachThreshold {
		// 3
		leader := s.GetLeader(vote.ShardId, txHash)
		ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
		defer cancel()

		sscVote := s.aggregateSSCCommitVote(s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId])
		s.logger.Info().Str("txHash", txHash.Hex()).Msgf("send SSC commit vote to leader %s", leader.Address)
		err := s.Comm.Call(ctx, nil, leader, api.Method_HandleCXTCommitSSCVote, sscVote)
		if err != nil {
			s.logger.Error().Err(err).Msg("failed to send commit vote")
			return
		}
	}
}

func (s *sscService) aggregateSSCCommitVote(votes []*api.CXTCommitVote) *api.CXTCommitSSCVote {
	msgs := make([]api.SSCMessage, 0, len(votes))
	result := votes[0]
	aggregatedSig, bitMap, err := s.BLSSigner.Aggregate(msgs)
	if err != nil {
		s.logger.Error().Err(err)
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
		BaseBLSSignedMessage: &api.BaseBLSSignedMessage{
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
	simuState := s.getState(txHash)
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
	s.logger.Info().Str("txHash", common.BytesToHash(req.TxHash).String()).Msg("handle cxt ssc call, start")
	defer s.logger.Info().Str("txHash", common.BytesToHash(req.TxHash).String()).Msg("handle cxt ssc call, end")
	committee := s.GetCommittee(s.SelfShard)
	t := committee.Threshold

	// select the consistent state
	header := s.bc.CurrentHeader()
	req.BlockHash = header.Hash().Bytes()

	ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(t)
	lock := sync.Mutex{}
	results := make([]api.SSCMessage, 0, t)

	for _, member := range committee.Members {
		go func() {
			ret := new(api.CXTCallResult)
			err := s.Comm.Call(ctx, ret, member, api.Method_HandleCXTCall, req)
			if err != nil {
				return
			}
			lock.Lock()
			if len(results) < t {
				results = append(results, ret)
			}
			lock.Unlock()
			wg.Done()
		}()
	}
	wg.Wait()
	sscResult, err := s.aggregateCXSSCCallResult(results)
	if err != nil {
		s.logger.Error().Err(err)
	}
	state := s.getState(common.BytesToHash(req.TxHash))
	callState := state.SimulationCallStates[req.SimulationNum].Get(req.CallIndex)
	callState.CallSSCResult = sscResult
	return sscResult
}

func (s *sscService) aggregateCXSSCCallResult(results []api.SSCMessage) (*api.CXTCallSSCResult, error) {
	result := results[0].(*api.CXTCallResult)
	aggregatedSig, bitMap, err := s.BLSSigner.Aggregate(results)
	if err != nil {
		return nil, err
	}
	sscResult := &api.CXTCallSSCResult{
		CallIndex:     result.CallIndex,
		RelatedShards: result.RelatedShards,
		Result:        result.Result,
		LeftOverGas:   result.LeftOverGas,
		BlockHash:     result.BlockHash,
		Err:           result.Err,
		BaseBLSSignedMessage: &api.BaseBLSSignedMessage{
			ShardId:    s.SelfShard,
			Signatures: aggregatedSig,
			BLSBitMap:  bitMap,
		},
	}
	return sscResult, nil
}

func (s *sscService) CommitSimulation(commit *api.SimulationCommit) {
	txHash := common.BytesToHash(commit.TxHash)
	if !commit.Commit {
		s.logger.Error().Msgf("simulation %s failed, status=%d", txHash.String(), commit.Status)
		s.closeTransaction(txHash)
		return
	}

	simuState := s.getState(txHash)
	// TODO verify the simulation commit
	var (
		err error
	)
	simulationCallStates := simuState.SimulationCallStates[commit.SimulationNum]
	callStates := make([]*api.CXTCallState, 0)
	for _, simulationCallState := range simulationCallStates {
		depentResults := make([]*api.CXTCallSSCResult, 0)
		for _, dc := range simulationCallState.DependentCXTCalls {
			depentResults = append(depentResults, dc.SSCResult)
		}
		sort.Slice(depentResults, func(i, j int) bool {
			return depentResults[i].CallIndex.ToString() < depentResults[j].CallIndex.ToString()
		})
		callStates = append(callStates, &api.CXTCallState{
			CallIndex:        simulationCallState.CallIndex,
			TopRequest:       simulationCallState.TopRequest,
			CallRequest:      simulationCallState.CallRequest,
			RWSet:            simulationCallState.RWSet,
			DependentResults: depentResults,
			CallResult:       simulationCallState.CallSSCResult,
			TopResult:        simulationCallState.TopSSCResult,
		})
	}
	simulation := &api.CXTSimulation{
		TxHash:               commit.TxHash,
		RelatedShards:        simuState.RelatedShards,
		CallStates:           callStates,
		BaseBLSSignedMessage: &api.BaseBLSSignedMessage{},
	}

	s.buildSignaturesForSimulation(simulation)
	err = s.txSubmitter.SubmitSimulationTx(simulation)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to submit simulation tx")
		return
	}
	s.logger.Info().Str("txHash", txHash.Hex()).Msgf("simulation committed")
}

func (s *sscService) buildSignaturesForSimulation(simulation *api.CXTSimulation) {
	committee := s.GetCommittee(s.SelfShard)
	t := committee.Threshold

	ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(t)
	baseMsgs := make([]api.SSCMessage, 0, t)
	lock := sync.Mutex{}

	for _, member := range committee.Members {
		go func() {
			signature := make([]byte, 0)
			err := s.Comm.Call(ctx, signature, member, api.Method_SignCXTSimulation, simulation)
			if err != nil {
				return
			}
			lock.Lock()
			if len(baseMsgs) < t {
				baseMsgs = append(baseMsgs, &api.BaseSSCMessage{
					Signature:  signature,
					SenderAddr: member.Address,
				})
			}
			lock.Unlock()
			wg.Done()
		}()
	}

	wg.Wait()
	aggregatedSig, bitMap, err := s.BLSSigner.Aggregate(baseMsgs)
	if err != nil {
		s.logger.Error().Err(err)
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
	relatedShards := s.simulationState[txHash].RelatedShards
	relatedShardMap := s.simulationState[txHash].RelatedShardMap
	s.stateLock.RUnlock()
	if _, exists := relatedShardMap[vote.ShardId]; !exists {
		s.logger.Error().Msgf("shard %d is not related to tx %s", vote.ShardId, txHash.String())
		return
	}

	s.commitLock.RLock()
	commitState := s.commitStates[txHash]
	if commitState.CommitSSCVotes[vote.SimulationNum] == nil {
		commitState.CommitSSCVotes[vote.SimulationNum] = make(map[uint32]*api.CXTCommitSSCVote)
	}
	commitState.CommitSSCVotes[vote.SimulationNum][vote.ShardId] = vote
	sscVotes := commitState.CommitSSCVotes[vote.SimulationNum]
	s.commitLock.RUnlock()

	s.logger.Info().Msgf("received commit ssc vote [%d/%d]", len(sscVotes), len(relatedShardMap))
	if len(sscVotes) == len(relatedShardMap) {
		commitType := api.Commit
		commitReason := api.Reason_SUCCESS
		sscVoteList := make([]*api.CXTCommitSSCVote, 0)
		recallShards := make([]uint32, 0)
		for _, v := range sscVotes {
			if v.Type == api.Rollback {
				commitType = api.Rollback
				commitReason = v.Reason
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

		ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
		defer cancel()
		members := make([]*api.Member, 0, len(relatedShards))
		for _, shardId := range relatedShards {
			members = append(members, s.GetLeader(shardId, txHash))
		}

		switch commitType {
		case api.Commit:
			proof := &api.CXTCommitProof{
				TxHash:        txHash.Bytes(),
				SimulationNum: vote.SimulationNum,
				Type:          commitType,
				Reason:        commitReason,
				Votes:         sscVoteList,
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
				RelatedShards: relatedShards,
			}
			_ = s.Comm.Multicast(ctx, members, api.Method_HandleCXTCommitProof, proof)
			go func() {
				s.startRecallSimulation(proof)
			}()
		}
	}
}

func (s *sscService) startRecallSimulation(proof *api.CXTCommitProof) {
	ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
	defer cancel()

	// request recall origin contract
	req := &api.CXTSimulationRequest{
		SimulationNum: proof.SimulationNum + 1,
		TxHash:        proof.TxHash,
	}

	committee := s.GetCommittee(s.SelfShard)
	t := committee.Threshold
	wg := sync.WaitGroup{}
	wg.Add(t)
	lock := sync.Mutex{}
	results := make([]api.SSCMessage, 0, t)

	for _, member := range committee.Members {
		go func() {
			ret := new(api.CXTSimulationResult)
			err := s.Comm.Call(ctx, ret, member, api.Method_HandleSimulateRequest, req)
			if err != nil {
				return
			}
			lock.Lock()
			if len(results) < t {
				results = append(results, ret)
			}
			lock.Unlock()
			wg.Done()
		}()
	}
	wg.Wait()
	sscResult, err := s.aggregateSimulationResults(results)
	if err != nil {
		sscResult = &api.CXTSimulationSSCResult{Err: err.Error(), RelatedShards: []uint32{s.SelfShard}}
	}

	// build and send simulation commit
	var simulationCommit *api.SimulationCommit
	if len(sscResult.Err) > 0 {
		simulationCommit = &api.SimulationCommit{
			SimulationNum: req.SimulationNum,
			TxHash:        proof.TxHash,
			RelatedShards: sscResult.RelatedShards,
			Commit:        false,
			Status:        api.ExecutionFailed,
		}
	} else {
		simulationCommit = &api.SimulationCommit{
			SimulationNum: req.SimulationNum,
			TxHash:        proof.TxHash,
			RelatedShards: sscResult.RelatedShards,
			Commit:        true,
			Status:        api.OK,
		}
	}
	s.thresholdSignSimulationCommit(simulationCommit)
	members := make([]*api.Member, 0, len(committee.Members))
	for _, shardId := range simulationCommit.RelatedShards {
		members = append(members, s.GetLeader(shardId, common.BytesToHash(simulationCommit.TxHash)))
	}
	_ = s.Comm.Multicast(ctx, members, api.Method_CommitSimulation, simulationCommit)
}

func (s *sscService) HandleCXTCommitProof(proof *api.CXTCommitProof) {
	// if proof is commit or rollback, send commit proof transaction
	if proof.Type == api.Commit || proof.Type == api.Rollback {
		err := s.txSubmitter.SubmitCommitOrRollbackTx(proof)
		if err != nil {
			s.logger.Error().Err(err).Msg("failed to submit commit or rollback tx")
			return
		}
		if proof.TxHash[0] == 0 {
			s.txSubmitter.SubmitCommitOrRollbackTx(proof)
		}
		// // 模拟harmony的额外调用过程
		// for i := 0; i < 1; i++ {
		// 	err := s.txSubmitter.SubmitCommitOrRollbackTx(proof)
		// 	if err != nil {
		// 		s.logger.Error().Err(err).Msg("failed to submit empty tx")
		// 		return
		// 	}
		// }
	}

	if proof.Type == api.Recall {
		committee := s.GetCommittee(s.SelfShard)
		_ = s.Comm.Multicast(s.ctx, committee.Members, api.Method_HandleCXTRecallProof, proof)
	}
}

func (s *sscService) getState(txHash common.Hash) *api.CXTSimulationState {
	s.stateLock.RLock()
	defer s.stateLock.RUnlock()
	return s.simulationState[txHash]
}

func (s *sscService) closeTransaction(txHash common.Hash) {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	s.commitLock.Lock()
	defer s.commitLock.Unlock()
	delete(s.simulationState, txHash)
	delete(s.commitStates, txHash)
	delete(s.executionVerifyContexts, txHash)
}
