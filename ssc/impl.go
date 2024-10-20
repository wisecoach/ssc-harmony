package ssc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/accounts"
	"github.com/harmony-one/harmony/accounts/keystore"
	"github.com/harmony-one/harmony/core"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/core/vm"
	"github.com/harmony-one/harmony/hmy"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/rs/zerolog"
	"math"
	"math/big"
	"sort"
	"sync"
)

type sscService struct {
	CommitteeMechanism
	Comm      Comm
	BLSSigner BLSSigner
	Config    api.Config

	keyStore keystore.KeyStore
	hmy      *hmy.Harmony
	bc       core.BlockChain
	db       vm.StateDB

	logger *zerolog.Logger

	simulationState         map[common.Hash]*api.CXTSimulationState
	executionVerifyState    map[common.Hash]*api.ExecutionVerifyState // Deprecated
	commitStates            map[common.Hash]*api.CommitState
	executionVerifyContexts map[common.Hash]*api.ExecutionVerifyContext // txHash -> ExecutionVerifyContext
	stateLock               sync.RWMutex

	ctx context.Context
}

func (s *sscService) StartCXT(txHash common.Hash, callIndex api.CallIndex, originShardId uint32, relatedShards []uint32) bool {
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
		CallIndex:              callIndex,
		CurrentIndex:           0,
		SimulationCallStates:   make(map[int]api.SimulationCallStates),
		SimulationRecallStates: make(map[int]api.SimulationRecallStates),
		OriginShardId:          originShardId,
		RelatedShards:          relatedShards,
		TimeoutCtx:             timeoutCtx,
		TimeoutCancel:          cancel,
	}

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

func (s *sscService) SimulateCXTransaction(req *api.CXTSimulationRequest) *api.CXTSimulationSSCResult {
	leader := s.GetLeader(s.SelfShard, req.Tx.Hash())

	ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
	defer cancel()

	ret, err := s.Comm.Call(ctx, leader, api.Method_StartSimulateCXTransaction, req)
	if err != nil {
		return nil
	}
	return ret.(*api.CXTSimulationSSCResult)
}

func (s *sscService) CallCXContract(req *api.CXTCallRequest) *api.CXTCallSSCResult {
	txHash := common.BytesToHash(req.TxHash)

	s.stateLock.RLock()
	cxtState := s.simulationState[txHash]
	if cxtState == nil {
		s.stateLock.RUnlock()
		return &api.CXTCallSSCResult{Err: errors.New("no simulation state found")}
	}
	s.stateLock.RUnlock()

	req.SimulationNum = cxtState.SimulationNum
	req.OriginShardId = cxtState.OriginShardId
	req.FromShardId = s.SelfShard
	copy(req.CallIndex, cxtState.CallIndex)
	req.CallIndex = append(req.CallIndex, cxtState.CurrentIndex)
	cxtState.CurrentIndex++

	leader := s.GetLeader(s.SelfShard, txHash)
	ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
	defer cancel()

	ret, err := s.Comm.Call(ctx, leader, api.Method_RequestCallCX, req)
	if err != nil {
		return &api.CXTCallSSCResult{Err: err}
	}
	return ret.(*api.CXTCallSSCResult)
}

func (s *sscService) RecallCXContract(req *api.CXTRecallRequest) *api.CXTRecallSSCResult {
	txHash := common.BytesToHash(req.TxHash)

	s.stateLock.RLock()
	cxtState := s.simulationState[txHash]
	if cxtState == nil {
		s.stateLock.RUnlock()
		return &api.CXTRecallSSCResult{Err: errors.New("no simulation state found")}
	}
	s.stateLock.RUnlock()

	req.SimulationNum = cxtState.SimulationNum
	req.FromShardId = s.SelfShard
	req.OriginShardId = cxtState.OriginShardId
	copy(req.CallIndex, cxtState.CallIndex)
	req.CallIndex = append(req.CallIndex, cxtState.CurrentIndex)
	cxtState.CurrentIndex++

	leader := s.GetLeader(s.SelfShard, txHash)
	ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
	defer cancel()

	ret, err := s.Comm.Call(ctx, leader, api.Method_RequestRecallCX, req)
	if err != nil {
		return &api.CXTRecallSSCResult{Err: err}
	}
	return ret.(*api.CXTRecallSSCResult)
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

	// TODO sign data
	targetLeader := s.GetLeader(shardId, common.BytesToHash(vote.TxHash))
	s.Comm.Call(ctx, targetLeader, api.Method_HandleCommitVote, vote)
}

func (s *sscService) verifyExecutionState(simulation *api.CXTSimulation) bool {
	valid := true
	txHash := common.BytesToHash(simulation.TxHash)
	// executionVerifyState := &api.ExecutionVerifyState{
	// 	Calls: make(map[string]*api.ExecutionVerifyCall),
	// }
	callStateMap := make(map[string]*api.CXTCallState)
	for _, callState := range simulation.CallStates {
		callStateMap[callState.CallIndex.ToString()] = callState
	}
	verifyContext := &api.ExecutionVerifyContext{Simulation: simulation, CallStateMap: callStateMap}
	s.executionVerifyContexts[txHash] = verifyContext
	// s.InitSimulationContext(simulation)
	// s.executionVerifyState[txHash] = executionVerifyState

	state, _ := s.bc.State()
	chainConfig := s.bc.Config()
	vmConfig := s.bc.GetVMConfig()
	for _, callState := range simulation.CallStates {
		// callIndex := callState.Request.CallIndex
		req := callState.Request
		simuRet := callState.Result

		verifyContext.CurrentState = newStateSet()
		verifyContext.DependentResults = callState.DependentResults
		verifyContext.CallIndex = req.CallIndex

		// add new execution verify state
		// executionVerifyCall := &api.ExecutionVerifyCall{
		// 	CallIndex:        req.CallIndex,
		// 	DependentResults: callState.DependentResults,
		// 	CurrentState:     newStateSet(),
		// }
		// executionVerifyState.Calls[callState.CallIndex.ToString()] = executionVerifyCall

		// prepare execution verify context
		header := s.bc.GetHeaderByHash(common.BytesToHash(simuRet.BlockHash))
		sender := vm.AccountRef(req.Caller)
		vmCtx := core.NewSSCVMContext(req.Caller, txHash, req.CallIndex, req.GasPrice, header, s.bc, nil)
		sscvm := vm.NewSSCVM(vmCtx, state, chainConfig, *vmConfig, s, vm.ExecutionVerify)

		// execute contract with ExecutionVerify
		ret, leftOverGas, err := sscvm.CallFromOtherShard(req.FromShardId, sender, req.Addr, req.Input, req.Gas, req.Value)
		if err != nil {
			valid = false
			break
		}
		// executionVerifyCall.Return = ret
		// executionVerifyCall.LeftOverGas = leftOverGas

		// compare the result and leftOverGas
		if bytes.Compare(ret, callState.Result.Result) != 0 {
			valid = false
			break
		}
		if leftOverGas != callState.Result.LeftOverGas {
			valid = false
			break
		}
		if verifyContext.CurrentState.Equal(callState.RWSet.WriteState) {
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
			conflictCallIndexes = append(conflictCallIndexes, callState.Request.CallIndex)
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
	req := callState.Request
	txHash := common.BytesToHash(req.TxHash)
	state, _ := s.bc.State()
	header := s.bc.CurrentHeader()
	sender := vm.AccountRef(req.Caller)
	vmCtx := core.NewSSCVMContext(req.Caller, txHash, req.CallIndex, req.GasPrice, header, s.bc, nil)
	sscvm := vm.NewSSCVM(vmCtx, state, s.bc.Config(), *s.bc.GetVMConfig(), s, vm.LockExecution)

	// execute contract with LockExecution to lock state and save recall simulation to handle recall request
	ret, leftOverGas, err := sscvm.CallFromOtherShard(req.FromShardId, sender, req.Addr, req.Input, req.Gas, req.Value)
	result := &api.CXTRecallResult{
		RelatedShards: callState.Result.RelatedShards,
		Locked:        true,
		Result:        ret,
		LeftOverGas:   leftOverGas,
		BlockHash:     header.Hash().Bytes(),
		Err:           err,
	}

	simulationState := s.getState(txHash)
	// cache the recall state for the next simulation recall state
	if simulationState.SimulationRecallStates[simulationState.SimulationNum+1] == nil {
		simulationState.SimulationRecallStates[simulationState.SimulationNum+1] = make(api.SimulationRecallStates, 0)
	}
	recallState := simulationState.SimulationRecallStates[simulationState.SimulationNum+1].Get(callState.Request.CallIndex)
	if recallState == nil {
		recallState = &api.SimulationRecallState{
			CallIndex:  callState.Request.CallIndex,
			Locked:     true,
			Requests:   nil,
			SSCRequest: nil,
			RWSet:      nil,
			Result:     result,
		}
		simulationState.SimulationRecallStates[simulationState.SimulationNum+1].Add(recallState)
	}

	return err
}

func (s *sscService) lockStateWithRWSet(txHash common.Hash, callState *api.CXTCallState) {
	state, _ := s.bc.LockableState()
	for address, stateMap := range callState.RWSet.WriteState.State {
		for key, value := range stateMap {
			_ = state.SetStateWithLock(txHash, callState.Request.CallIndex, address, key, value)
		}
	}
}

func (s *sscService) VerifyReSimulation(reSimulationBytes []byte) {
	// TODO implement
	panic("implement me")
}

func (s *sscService) StartSimulateCXTransaction(req *api.CXTSimulationRequest) *api.CXTSimulationSSCResult {
	committee := s.GetCommittee(s.SelfShard)
	t := committee.Threshold

	ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(t)
	lock := sync.Mutex{}
	results := make([]*api.CXTSimulationResult, 0, t)

	for _, member := range committee.Members {
		go func() {
			result, err := s.Comm.Call(ctx, member, api.Method_HandleSimulateRequest, req)
			if err != nil {
				return
			}
			lock.Lock()
			if len(results) < t {
				results = append(results, result.(*api.CXTSimulationResult))
			}
			lock.Unlock()
			wg.Done()
		}()
	}
	wg.Wait()
	sscResult, err := aggregateSimulationResults(results)
	if err != nil {
		sscResult = &api.CXTSimulationSSCResult{Err: err, RelatedShards: []uint32{s.SelfShard}}
	}
	var simulationCommit *api.SimulationCommit
	if sscResult.Err != nil {
		simulationCommit = &api.SimulationCommit{
			TxHash:        req.Tx.Hash().Bytes(),
			RelatedShards: sscResult.RelatedShards,
			Commit:        false,
			Status:        api.ExecutionFailed,
			Signatures:    make([]byte, 0),
		}
	} else {
		simulationCommit = &api.SimulationCommit{
			TxHash:        req.Tx.Hash().Bytes(),
			RelatedShards: sscResult.RelatedShards,
			Commit:        true,
			Status:        api.OK,
			Signatures:    make([]byte, 0),
		}
	}
	s.thresholdSignSimulationCommit(simulationCommit)
	members := make([]*api.Member, 0, len(committee.Members))
	for _, shardId := range simulationCommit.RelatedShards {
		members = append(members, s.GetLeader(shardId, common.BytesToHash(simulationCommit.TxHash)))
	}
	s.Comm.Multicast(ctx, members, api.Method_CommitSimulation, simulationCommit)

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
	signatures := make([][]byte, 0, t)

	for _, member := range committee.Members {
		go func() {
			result, err := s.Comm.Call(ctx, member, api.Method_SignSimulationCommit, commit)
			if err != nil {
				return
			}
			lock.Lock()
			if len(signatures) < t {
				signatures = append(signatures, result.([]byte))
			}
			lock.Unlock()
			wg.Done()
		}()
	}
	wg.Wait()
	// TODO aggregate signatures to a bls signature
	blsSignature := make([]byte, 0)
	commit.Signatures = blsSignature
}

func aggregateSimulationResults(results []*api.CXTSimulationResult) (*api.CXTSimulationSSCResult, error) {
	panic("implement me")
}

func (s *sscService) HandleSimulateRequest(req *api.CXTSimulationRequest) *api.CXTSimulationResult {
	state, err := s.bc.State()
	chainConfig := s.bc.Config()
	vmConfig := s.bc.GetVMConfig()
	header := req.Header
	tx := req.Tx
	gp := new(core.GasPool).AddGas(req.GasPool)

	txHash := tx.Hash()
	// init cxt state
	_ = s.StartCXT(txHash, api.CallIndex{}, s.SelfShard, make([]uint32, 0))

	var signer types.Signer
	if tx.IsEthCompatible() {
		if !chainConfig.IsEthCompatible(header.Epoch()) {
			return &api.CXTSimulationResult{Err: errors.New("ethereum compatible transactions not supported at current epoch")}
		}
		signer = types.NewEIP155Signer(chainConfig.EthCompatibleChainID)
	} else {
		signer = types.MakeSigner(chainConfig, header.Epoch())
	}
	msg, err := tx.AsMessage(signer)

	if err != nil {
		return &api.CXTSimulationResult{Err: err}
	}

	ctx := core.NewSSCVMContext(msg.From(), tx.Hash(), api.CallIndex{}, tx.GasPrice(), header, s.bc, req.Author)
	ctx.TxType = types.CXTransaction
	sscvm := vm.NewSSCVM(ctx, state, chainConfig, *vmConfig, s, vm.SimulationCall)
	result, err := core.NewSSCStateTransition(sscvm, msg, gp).TransitionDb()
	if err != nil {
		return &api.CXTSimulationResult{Err: err}
	}
	root := state.IntermediateRoot(chainConfig.IsS3(header.Epoch())).Bytes()

	receipt := types.NewReceipt(root, err != nil, result.UsedGas)
	receipt.TxHash = tx.Hash()
	receipt.GasUsed = result.UsedGas
	simuState := s.getState(txHash)
	return &api.CXTSimulationResult{
		RelatedShards: simuState.RelatedShards,
		Result:        result.ReturnData,
		Receipt:       receipt,
		UsedGas:       result.UsedGas,
		Err:           nil,
	}
}

func (s *sscService) HandleReSimulateRequest(req *api.CXTReSimulationRequest) *api.CXTReSimulationResult {
	txHash := common.BytesToHash(req.TxHash)

	s.stateLock.RLock()
	cxtState := s.simulationState[txHash]
	if cxtState == nil {
		s.stateLock.RUnlock()
		return &api.CXTReSimulationResult{Err: errors.New("no simulation state found")}
	}
	s.stateLock.RUnlock()

	simuReq := cxtState.SimulationRequest
	tx := simuReq.Tx
	header := s.bc.CurrentHeader()
	chainConfig := s.bc.Config()
	author := simuReq.Author
	gp := new(core.GasPool).AddGas(simuReq.GasPool)
	vmConfig := s.bc.GetVMConfig()
	state, _ := s.bc.State()

	var signer types.Signer
	if tx.IsEthCompatible() {
		if !chainConfig.IsEthCompatible(header.Epoch()) {
			return &api.CXTReSimulationResult{Err: errors.New("ethereum compatible transactions not supported at current epoch")}
		}
		signer = types.NewEIP155Signer(chainConfig.EthCompatibleChainID)
	} else {
		signer = types.MakeSigner(chainConfig, header.Epoch())
	}
	msg, err := tx.AsMessage(signer)

	if err != nil {
		return &api.CXTReSimulationResult{Err: err}
	}

	ctx := core.NewSSCVMContext(msg.From(), tx.Hash(), api.CallIndex{}, tx.GasPrice(), header, s.bc, author)
	ctx.TxType = types.CXTransaction
	sscvm := vm.NewSSCVM(ctx, state, chainConfig, *vmConfig, s, vm.SimulationCall)
	result, err := core.NewSSCStateTransition(sscvm, msg, gp).TransitionDb()
	if err != nil {
		return &api.CXTReSimulationResult{Err: err}
	}
	root := state.IntermediateRoot(chainConfig.IsS3(header.Epoch())).Bytes()

	receipt := types.NewReceipt(root, err != nil, result.UsedGas)
	receipt.TxHash = tx.Hash()
	receipt.GasUsed = result.UsedGas
	simuState := s.getState(txHash)
	return &api.CXTReSimulationResult{
		RelatedShards: simuState.RelatedShards,
		Result:        result.ReturnData,
		Receipt:       receipt,
		UsedGas:       result.UsedGas,
		Err:           nil,
	}
}

func (s *sscService) RequestCallCXT(req *api.CXTCallRequest) *api.CXTCallSSCResult {
	txHash := common.BytesToHash(req.TxHash)
	committee := s.GetCommittee(s.SelfShard)
	s.stateLock.Lock()
	cxtState := s.simulationState[txHash]
	if cxtState == nil {
		s.stateLock.Unlock()
		return &api.CXTCallSSCResult{Err: errors.New("no simulation state found")}
	}
	// if Call has executed
	callStates := cxtState.SimulationCallStates[req.SimulationNum]
	callState := callStates.Get(req.CallIndex)
	if callState != nil && callState.Executed {
		s.stateLock.Unlock()
		return callState.Result
	}
	waitingCh := make(chan *api.CXTCallSSCResult)
	if callState == nil {
		callState = &api.SimulationCallState{Requests: make([]*api.CXTCallRequest, 0)}
		callState.Requests = append(callState.Requests, req)
		callState.WaitingChs = make([]chan *api.CXTCallSSCResult, 0)
		callStates.Add(callState)
	}
	callState.WaitingChs = append(callState.WaitingChs, waitingCh)
	if len(callState.Requests) < committee.Threshold {
		callState.Requests = append(callState.Requests, req)
	}
	s.stateLock.Unlock()

	if len(callState.Requests) == committee.Threshold {
		// aggregate signatures
		// send request to leader
		leader := s.GetLeader(req.TargetShardId, txHash)
		sscReq := aggregateSSCCallRequest(callState.Requests)
		ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
		defer cancel()
		ret, err := s.Comm.Call(ctx, leader, api.Method_HandleCXTCall, sscReq)
		if err != nil {
			ret = &api.CXTCallSSCResult{Err: err}
		}
		result := ret.(*api.CXTCallSSCResult)
		s.stateLock.Lock()
		callState.Result = result
		callState.Executed = true
		s.stateLock.Unlock()
		for _, ch := range callState.WaitingChs {
			ch <- result
		}
	}

	return <-waitingCh
}

func aggregateSSCCallRequest([]*api.CXTCallRequest) *api.CXTCallSSCRequest {
	panic("implement me")
}

func (s *sscService) HandleCXTCall(req *api.CXTCallSSCRequest) *api.CXTCallResult {
	// begin cxt call, update simulationState and callIndex
	s.beginCXCall(req)
	txHash := common.BytesToHash(req.TxHash)
	state, err := s.bc.State()
	chainConfig := s.bc.Config()
	vmConfig := s.bc.GetVMConfig()
	header := s.bc.CurrentHeader()
	sender := vm.AccountRef(req.Caller)

	// create sscvm instance
	vmCtx := core.NewSSCVMContext(req.Caller, txHash, req.CallIndex, req.GasPrice, header, s.bc, nil)
	sscvm := vm.NewSSCVM(vmCtx, state, chainConfig, *vmConfig, s, vm.SimulationCall)

	// call contract with SimulationCall
	ret, leftOverGas, err := sscvm.CallFromOtherShard(req.FromShardId, sender, req.Addr, req.Input, req.Gas, req.Value)
	simuState := s.getState(txHash)
	return &api.CXTCallResult{
		RelatedShards: simuState.RelatedShards,
		Result:        ret,
		LeftOverGas:   leftOverGas,
		BlockHash:     header.Hash().Bytes(),
		Err:           err,
	}
}

func (s *sscService) beginCXCall(req *api.CXTCallSSCRequest) {
	// try to start cxt if not exist
	txHash := common.BytesToHash(req.TxHash)
	s.StartCXT(txHash, req.CallIndex, req.OriginShardId, req.RelatedShards)

	s.lock.RLock()
	state := s.simulationState[txHash]
	s.lock.RUnlock()
	// update new callIndex and clear current index
	state.CallIndex = req.CallIndex
	state.CurrentIndex = 0
}

func (s *sscService) RequestRecallCXT(req *api.CXTRecallRequest) *api.CXTRecallSSCResult {
	txHash := common.BytesToHash(req.TxHash)
	committee := s.GetCommittee(s.SelfShard)
	s.stateLock.Lock()
	cxtState := s.simulationState[txHash]
	if cxtState == nil {
		s.stateLock.Unlock()
		return &api.CXTRecallSSCResult{Err: errors.New("no simulation state found")}
	}
	// if Call has executed
	callState := cxtState.SimulationRecallStates[req.SimulationNum].Get(req.CallIndex)
	if callState != nil && callState.Executed {
		s.stateLock.Unlock()
		return callState.SSCResult
	}
	waitingCh := make(chan *api.CXTRecallSSCResult)
	if callState == nil {
		callState = &api.SimulationRecallState{Requests: make([]*api.CXTRecallRequest, 0)}
		callState.Requests = append(callState.Requests, req)
		callState.WaitingChs = make([]chan *api.CXTRecallSSCResult, 0)
		cxtState.SimulationRecallStates[req.SimulationNum].Add(callState)
	}
	callState.WaitingChs = append(callState.WaitingChs, waitingCh)
	if len(callState.Requests) < committee.Threshold {
		callState.Requests = append(callState.Requests, req)
	}
	s.stateLock.Unlock()

	if len(callState.Requests) == committee.Threshold {
		// aggregate signatures
		// send request to leader
		leader := s.GetLeader(req.TargetShardId, txHash)
		sscReq := s.aggregateSSCRecallRequest(callState.Requests)
		ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
		defer cancel()
		ret, err := s.Comm.Call(ctx, leader, api.Method_HandleCXTCall, sscReq)
		if err != nil {
			ret = &api.CXTCallSSCResult{Err: err}
		}
		result := ret.(*api.CXTRecallSSCResult)
		s.stateLock.Lock()
		callState.SSCResult = result
		callState.Executed = true
		s.stateLock.Unlock()
		for _, ch := range callState.WaitingChs {
			ch <- result
		}
	}

	return <-waitingCh
}

func (s *sscService) aggregateSSCRecallRequest(requests []*api.CXTRecallRequest) *api.CXTRecallSSCRequest {
	panic("implement me")

}

func (s *sscService) HandleCXTRecall(req *api.CXTRecallSSCRequest) *api.CXTRecallResult {
	// begin cxt call, update simulationState and callIndex
	s.beginCXRecall(req)
	txHash := common.BytesToHash(req.TxHash)
	state, err := s.bc.State()
	chainConfig := s.bc.Config()
	vmConfig := s.bc.GetVMConfig()
	header := s.bc.CurrentHeader()
	sender := vm.AccountRef(req.Caller)

	// create sscvm instance
	vmCtx := core.NewSSCVMContext(req.Caller, txHash, req.CallIndex, req.GasPrice, header, s.bc, nil)
	sscvm := vm.NewSSCVM(vmCtx, state, chainConfig, *vmConfig, s, vm.SimulationReCall)

	// call contract with SimulationCall
	ret, leftOverGas, err := sscvm.CallFromOtherShard(req.FromShardId, sender, req.Addr, req.Input, req.Gas, req.Value)
	simuState := s.getState(txHash)
	return &api.CXTRecallResult{
		RelatedShards: simuState.RelatedShards,
		Result:        ret,
		LeftOverGas:   leftOverGas,
		BlockHash:     header.Hash().Bytes(),
		Err:           err,
	}
}

func (s *sscService) beginCXRecall(req *api.CXTRecallSSCRequest) {
	// try to start cxt if not exist
	txHash := common.BytesToHash(req.TxHash)
	s.StartCXT(txHash, req.CallIndex, req.OriginShardId, req.RelatedShards)

	s.lock.RLock()
	state := s.simulationState[txHash]
	s.lock.RUnlock()
	// update new callIndex and clear current index
	state.CallIndex = req.CallIndex
	state.CurrentIndex = 0
}

// HandleCXTRecallProof
//
//	@Description: handle the recall proof from other shard's ssc member
//	1. start new simulation number
//	2. get the smallest callIndex as lockedCallIndex
func (s *sscService) HandleCXTRecallProof(proof *api.CXTRecallProof) {
	// 1
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	txHash := common.BytesToHash(proof.TxHash)
	simuState := s.getState(txHash)
	simuState.SimulationNum = proof.SimulationNum + 1
	if simuState.SimulationRecallStates[simuState.SimulationNum] == nil {
		simuState.SimulationRecallStates[simuState.SimulationNum] = make(api.SimulationRecallStates, 0)
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

func (s *sscService) SignSimulationCommit(commit *api.SimulationCommit) []byte {
	data, err := json.Marshal(commit)
	if err != nil {
		return nil
	}
	signature, err := s.BLSSigner.Sign(data)
	if err != nil {
		return nil
	}
	return signature
}

func (s *sscService) SignCXTSimulation(simulation *api.CXTSimulation) []byte {
	data, err := json.Marshal(simulation)
	if err != nil {
		return nil
	}
	signature, err := s.BLSSigner.Sign(data)
	if err != nil {
		return nil
	}
	return signature
}

func (s *sscService) SignCXTReSimulation(simulation *api.CXTReSimulation) []byte {
	data, err := json.Marshal(simulation)
	if err != nil {
		return nil
	}
	signature, err := s.BLSSigner.Sign(data)
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
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

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
		return
	}
	s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId] = append(s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId], vote)
	if len(s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId]) == threshold {
		// 3
		leader := s.GetLeader(vote.ShardId, txHash)
		ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
		defer cancel()

		sscVote := s.aggregateSSCCommitVote(s.commitStates[txHash].CommitVotes[vote.SimulationNum][vote.ShardId])
		_, err := s.Comm.Call(ctx, leader, api.Method_HandleCXTCommitSSCVote, sscVote)
		if err != nil {
			s.logger.Error().Err(err).Msg("failed to send commit vote")
			return
		}
	}
}

func (s *sscService) aggregateSSCCommitVote(votes []*api.CXTCommitVote) *api.CXTCommitSSCVote {
	panic("implement me")
}

func (s *sscService) HandleCXTSSCCall(req *api.CXTCallSSCRequest) *api.CXTCallSSCResult {
	committee := s.GetCommittee(s.SelfShard)
	t := committee.Threshold

	ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(t)
	lock := sync.Mutex{}
	results := make([]*api.CXTCallResult, 0, t)

	for _, member := range committee.Members {
		go func() {
			result, err := s.Comm.Call(ctx, member, api.Method_HandleCXTCall, req)
			if err != nil {
				return
			}
			lock.Lock()
			if len(results) < t {
				results = append(results, result.(*api.CXTCallResult))
			}
			lock.Unlock()
			wg.Done()
		}()
	}
	wg.Wait()
	sscResult := aggregateCXSSCCallResult(results)
	return sscResult
}

func aggregateCXSSCCallResult(results []*api.CXTCallResult) *api.CXTCallSSCResult {
	// TODO implement me
	panic("implement me")
}

func (s *sscService) HandleCXTSSCRecall(req *api.CXTRecallSSCRequest) *api.CXTRecallSSCResult {
	// TODO implement me
	panic("implement")
}

func (s *sscService) CommitSimulation(commit *api.SimulationCommit) {
	simuState := s.getState(common.BytesToHash(commit.TxHash))
	// TODO verify the simulation commit
	var (
		simulationTx *types.Transaction
		err          error
	)

	simulationCallStates := simuState.SimulationCallStates[commit.SimulationNum]
	callStates := make([]*api.CXTCallState, 0)
	for _, simulationCallState := range simulationCallStates {
		callStates = append(callStates, &api.CXTCallState{
			CallIndex:        simulationCallState.CallIndex,
			Request:          simulationCallState.SignedRequest,
			RWSet:            simulationCallState.RWSet,
			DependentResults: simulationCallState.DependentResults,
			Result:           simulationCallState.Result,
		})
	}
	simulation := &api.CXTSimulation{
		TxHash:        commit.TxHash,
		RelatedShards: simuState.RelatedShards,
		CallStates:    callStates,
	}

	s.buildSignaturesForSimulation(simulation)
	simulationTx, err = s.buildSimulationTx(simulation)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to build simulation tx")
		return
	}

	ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
	defer cancel()

	err = s.hmy.SendTx(ctx, simulationTx)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to send simulation tx")
		return
	}
}

func (s *sscService) buildSignaturesForSimulation(simulation *api.CXTSimulation) {
	committee := s.GetCommittee(s.SelfShard)
	t := committee.Threshold

	ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(t)
	signatures := make([][]byte, 0, t)
	lock := sync.Mutex{}

	for _, member := range committee.Members {
		go func() {
			result, err := s.Comm.Call(ctx, member, api.Method_SignCXTSimulation, simulation)
			if err != nil {
				return
			}
			lock.Lock()
			if len(signatures) < t {
				signatures = append(signatures, result.([]byte))
			}
			lock.Unlock()
			wg.Done()
		}()
	}

	wg.Wait()
	// TODO aggregate signatures to a bls signature
	blsSignature := make([]byte, 0)
	simulation.Signatures = blsSignature
}

func (s *sscService) buildSimulationTx(simulation *api.CXTSimulation) (*types.Transaction, error) {
	input, _ := json.Marshal(simulation)
	state, err := s.bc.State()
	if err != nil {
		return nil, err
	}
	nonce := state.GetNonce(s.SelfAddr)
	tx := types.NewTransaction(nonce, vm.SimulationCommitAddr, s.SelfShard, big.NewInt(0), s.Config.SimulationCommitGasLimit, s.Config.SimulationCommitGasPrice, input)
	signedTx, err := s.keyStore.SignTx(accounts.Account{Address: s.SelfAddr}, tx, big.NewInt(int64(s.SelfShard)))
	if err != nil {
		return nil, err
	}
	return signedTx, nil
}

// func (s *sscService) buildSignaturesForReSimulation(reSimulation *api.CXTReSimulation) {
// 	committee := s.GetCommittee(s.SelfShard)
// 	t := committee.Threshold
//
// 	ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
// 	defer cancel()
//
// 	wg := sync.WaitGroup{}
// 	wg.Add(t)
// 	signatures := make([][]byte, 0, t)
// 	lock := sync.Mutex{}
//
// 	for _, member := range committee.Members {
// 		go func() {
// 			result, err := s.Comm.Call(ctx, member, api.Method_SignCXTReSimulation, reSimulation)
// 			if err != nil {
// 				return
// 			}
// 			lock.Lock()
// 			if len(signatures) < t {
// 				signatures = append(signatures, result.([]byte))
// 			}
// 			lock.Unlock()
// 			wg.Done()
// 		}()
// 	}
//
// 	wg.Wait()
// 	// TODO aggregate signatures to a bls signature
// 	blsSignature := make([]byte, 0)
// 	reSimulation.Signatures = blsSignature
// }

func (s *sscService) buildReSimulationTx(reSimulation *api.CXTReSimulation) (*types.Transaction, error) {
	input, _ := json.Marshal(reSimulation)
	state, err := s.bc.State()
	if err != nil {
		return nil, err
	}
	nonce := state.GetNonce(s.SelfAddr)
	tx := types.NewTransaction(nonce, vm.ReSimulationCommitAddr, s.SelfShard, big.NewInt(0), s.Config.SimulationCommitGasLimit, s.Config.SimulationCommitGasPrice, input)
	signedTx, err := s.keyStore.SignTx(accounts.Account{Address: s.SelfAddr}, tx, big.NewInt(int64(s.SelfShard)))
	if err != nil {
		return nil, err
	}
	return signedTx, nil
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
		return
	}

	s.stateLock.Lock()
	commitState := s.commitStates[txHash]
	if commitState.CommitSSCVotes[vote.SimulationNum] == nil {
		commitState.CommitSSCVotes[vote.SimulationNum] = make(map[uint32]*api.CXTCommitSSCVote)
	}
	commitState.CommitSSCVotes[vote.SimulationNum][vote.ShardId] = vote
	sscVotes := commitState.CommitSSCVotes[vote.SimulationNum]
	s.stateLock.Unlock()

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
		members := make([]*api.Member, 0, len(recallShards))
		for _, shardId := range recallShards {
			members = append(members, s.GetLeader(shardId, txHash))
		}

		switch commitType {
		case api.Commit:
			proof := &api.CXTCommitProof{
				TxHash:        txHash.Bytes(),
				Type:          commitType,
				Reason:        commitReason,
				Votes:         sscVoteList,
				RelatedShards: relatedShards,
			}
			_ = s.Comm.Multicast(ctx, members, api.Method_HandleCXTCommitProof, proof)
		case api.Rollback:
			proof := &api.CXTCommitProof{
				TxHash:        txHash.Bytes(),
				Type:          commitType,
				Reason:        commitReason,
				Votes:         sscVoteList,
				RelatedShards: relatedShards,
			}
			_ = s.Comm.Multicast(ctx, members, api.Method_HandleCXTCommitProof, proof)
		case api.Recall:
			proof := &api.CXTRecallProof{
				TxHash:        txHash.Bytes(),
				SimulationNum: vote.SimulationNum,
				RelatedShards: relatedShards,
				Votes:         sscVoteList,
				RecallShards:  recallShards,
			}
			_ = s.Comm.Multicast(ctx, members, api.Method_HandleCXTRecallProof, proof)
			go func() {
				s.startRecallSimulation(proof)
			}()
		}
	}
}

func (s *sscService) startRecallSimulation(proof *api.CXTRecallProof) {
	ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
	defer cancel()

	// request recall origin contract
	req := &api.CXTReSimulationRequest{
		SimulationNum: proof.SimulationNum + 1,
		TxHash:        proof.TxHash,
	}

	committee := s.GetCommittee(s.SelfShard)
	t := committee.Threshold
	wg := sync.WaitGroup{}
	wg.Add(t)
	lock := sync.Mutex{}
	results := make([]*api.CXTReSimulationResult, 0, t)

	for _, member := range committee.Members {
		go func() {
			result, err := s.Comm.Call(ctx, member, api.Method_HandleReSimulateRequest, req)
			if err != nil {
				return
			}
			lock.Lock()
			if len(results) < t {
				results = append(results, result.(*api.CXTReSimulationResult))
			}
			lock.Unlock()
			wg.Done()
		}()
	}
	wg.Wait()
	sscResult := aggregateReSimulationSSCResult(results)

	// build and send simulation commit
	var simulationCommit *api.SimulationCommit
	if sscResult.Err != nil {
		simulationCommit = &api.SimulationCommit{
			SimulationNum: req.SimulationNum,
			TxHash:        proof.TxHash,
			RelatedShards: sscResult.RelatedShards,
			Commit:        false,
			Status:        api.ExecutionFailed,
			Signatures:    make([]byte, 0),
		}
	} else {
		simulationCommit = &api.SimulationCommit{
			SimulationNum: req.SimulationNum,
			TxHash:        proof.TxHash,
			RelatedShards: sscResult.RelatedShards,
			Commit:        true,
			Status:        api.OK,
			Signatures:    make([]byte, 0),
		}
	}
	s.thresholdSignSimulationCommit(simulationCommit)
	members := make([]*api.Member, 0, len(committee.Members))
	for _, shardId := range simulationCommit.RelatedShards {
		members = append(members, s.GetLeader(shardId, common.BytesToHash(simulationCommit.TxHash)))
	}
	_ = s.Comm.Multicast(ctx, members, api.Method_CommitSimulation, simulationCommit)
}

func aggregateReSimulationSSCResult(results []*api.CXTReSimulationResult) *api.CXTReSimulationResult {
	// TODO implement me
	panic("implement")
}

func (s *sscService) HandleCXTCommitProof(proof *api.CXTCommitProof) {
	tx, err := s.buildCommitTx(proof)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to build simulation tx")
		return
	}
	ctx, cancel := context.WithTimeout(s.ctx, s.Config.CallTimeout)
	defer cancel()

	err = s.hmy.SendTx(ctx, tx)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to send simulation tx")
		return
	}
}

func (s *sscService) buildCommitTx(proof *api.CXTCommitProof) (*types.Transaction, error) {
	input, _ := json.Marshal(proof)
	state, err := s.bc.State()
	if err != nil {
		return nil, err
	}
	nonce := state.GetNonce(s.SelfAddr)
	tx := types.NewTransaction(nonce, vm.SimulationCommitAddr, s.SelfShard, big.NewInt(0), s.Config.SimulationCommitGasLimit, s.Config.SimulationCommitGasPrice, input)
	signedTx, err := s.keyStore.SignTx(accounts.Account{Address: s.SelfAddr}, tx, big.NewInt(int64(s.SelfShard)))
	if err != nil {
		return nil, err
	}
	return signedTx, nil
}

func (s *sscService) BroadcastCXTRecallProof(proof *api.CXTRecallProof) {
	committee := s.GetCommittee(s.SelfShard)
	_ = s.Comm.Multicast(s.ctx, committee.Members, api.Method_HandleCXTRecallProof, proof)
}

func (s *sscService) getState(txHash common.Hash) *api.CXTSimulationState {
	s.stateLock.RLock()
	defer s.stateLock.RUnlock()
	return s.simulationState[txHash]
}
