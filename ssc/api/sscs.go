package api

import (
	"github.com/ethereum/go-ethereum/common"
	"math/big"
)

const (
	Method_SimulateCXTransaction      = "ssc_simulateCXTransaction"
	Method_CallCXContract             = "ssc_callCXContract"
	Method_RecallCXContract           = "ssc_recallCXContract"
	Method_VerifySimulation           = "ssc_verifySimulation"
	Method_StartSimulateCXTransaction = "ssc_startSimulateCXTransaction"
	Method_HandleSimulateRequest      = "ssc_handleSimulateRequest"
	Method_HandleReSimulateRequest    = "ssc_handleReSimulateRequest"
	Method_RequestCallCX              = "ssc_requestCallCX"
	Method_HandleCXTCall              = "ssc_handleCXTCall"
	Method_RequestRecallCX            = "ssc_requestRecallCX"
	Method_HandleCXRecall             = "ssc_handleCXRecall"
	Method_HandleCXTRecallProof       = "ssc_handleCXTRecallProof"
	Method_SignSimulationCommit       = "ssc_signSimulationCommit"
	Method_SignCXTSimulation          = "ssc_signCXTSimulation"
	Method_SignCXTReSimulation        = "ssc_signCXTReSimulation"
	Method_HandleCommitVote           = "ssc_handleCommitVote"
	Method_HandleCXSSCCall            = "ssc_handleCXSSCCall"
	Method_CommitSimulation           = "ssc_commitSimulation"
	Method_HandleCXTCommitSSCVote     = "ssc_handleCXTCommitSSCVote"
	Method_HandleCXTCommitProof       = "ssc_handleCXTCommitProof"
	Method_BroadcastCXTRecallProof    = "ssc_broadcastCXTRecallProof"
)

// CXTStateSimulationDB will save the state of the cross-shard transaction simulation
type CXTStateSimulationDB interface {
	// StartCXT
	//  @Description: start a cross-shard transaction simulation, and begin to save the state
	//  @return new if start a new CTX simulation, false if already started
	StartCXT(txHash common.Hash, callIndex CallIndex, originShardId uint32, relatedShards []uint32) (new bool)
	// GetRWSet get the read-write set of the cross-shard transaction simulation
	GetRWSet(txHash common.Hash) *RWSet
	// EndCTX end a cross-shard transaction simulation
	EndCTX(txHash common.Hash)

	CreateAccount(txHash common.Hash, address common.Address)
	SubBalance(txHash common.Hash, address common.Address, balance *big.Int)
	AddBalance(txHash common.Hash, address common.Address, balance *big.Int)
	GetBalance(txHash common.Hash, address common.Address) *big.Int
	GetState(txHash common.Hash, address common.Address, key common.Hash) (common.Hash, error)
	SetState(txHash common.Hash, address common.Address, key common.Hash, value common.Hash) error

	// --------------------------- functions for execution verify --------------------------------

	SubSimuBalance(txHash common.Hash, address common.Address, balance *big.Int) error
	AddSimuBalance(txHash common.Hash, address common.Address, balance *big.Int) error
	GetSimuBalance(txHash common.Hash, address common.Address) (*big.Int, error)
	GetSimuState(txHash common.Hash, address common.Address, key common.Hash) (common.Hash, error)
	SetSimuState(txHash common.Hash, address common.Address, key common.Hash, value common.Hash) error
	GetResult(txHash common.Hash) (result []byte, leftOverGas uint64, err error)
}

// InternalService
//
//	@Description: It provides functions for internal modules
type InternalService interface {
	CXTStateSimulationDB

	// SimulateCXTransaction
	//
	//	@Description: simulate cross-shard transaction called by proposer, send request to leader of CXTransaction, and wait
	//	for the simulation result
	SimulateCXTransaction(req *CXTSimulationRequest) *CXTSimulationSSCResult

	// CallCXContract
	//
	//	@Description: call for cross-shard contract, send request to leader of CXTransaction, and wait for the simulation
	//	result
	CallCXContract(req *CXTCallRequest) *CXTCallSSCResult

	// RecallCXContract
	//
	//	@Description: recall for cross-shard contract, send request to leader of CXTransaction, and wait for the simulation
	//	result
	RecallCXContract(req *CXTRecallRequest) *CXTRecallSSCResult

	// VerifySimulation
	//	@Description: verify the simulation and vote for commit or rollback
	VerifySimulation(simulationBytes []byte)

	// VerifyReSimulation
	//	@Description: verify the resimulation and vote for commit or rollback
	VerifyReSimulation(reSimulationBytes []byte)
}

// ShardService
// @Description: It provides functions for the operation in the self's shard
type ShardService interface {
	// StartSimulateCXTransaction
	//  @Description: handle request from proposer, process the request as following:
	//			1. broadcast request to all members
	//			2. wait for k result of simulation
	// 			3. aggregate the result and signatures and return to proposer
	// 			4. notify all associated shards to commit simulation
	StartSimulateCXTransaction(req *CXTSimulationRequest) *CXTSimulationSSCResult

	// HandleSimulateRequest
	//  @Description: handle request from ssc's leader, process the request as following:
	//	1. simulate the contract execution, and save the read-write set to build the simulation result
	//	2. once need to call cross-shard contract, then send request to leader of target shard's ssc
	//  3. after simulation completed, send the result to leader of ssc
	HandleSimulateRequest(req *CXTSimulationRequest) *CXTSimulationResult

	// HandleReSimulateRequest
	//  @Description: handle request from ssc's leader
	// HandleReSimulateRequest(req *CXTReSimulationRequest) *CXTReSimulationResult

	// RequestCallCXT
	//
	//	 @Description: handle request from ssc's leader, aggregate signatures of request after reaching threshold, then send
	//		signed request to leader of target shard's ssc
	RequestCallCXT(req *CXTCallRequest) *CXTCallSSCResult

	// HandleCXTCall
	//
	//	@Description: handle cross-shard call request from leader, simulate the contract execution and return the result
	HandleCXTCall(req *CXTCallSSCRequest) *CXTCallResult

	// RequestRecallCXT
	//
	//	 @Description: handle request from ssc's memeber, aggregate signatures of request after reaching threshold, then send
	//		signed request to leader of target shard's ssc
	RequestRecallCXT(req *CXTRecallRequest) *CXTRecallSSCResult

	// HandleCXTRecall
	//
	//	@Description: handle the signed recall request from caller shard ssc, broadcast to members to simulate contract
	//	it's used to recall for the conflict of simulation and real state.
	HandleCXTRecall(req *CXTRecallSSCRequest) *CXTRecallResult

	// HandleCXTRecallProof
	//
	//	@Description: handle the cross-shard transaction submit proof to recall for the reason
	// 	1. broadcast to ssc members
	HandleCXTRecallProof(proof *CXTRecallProof)

	// SignSimulationCommit
	//  @Description: sign the simulation commit from ssc's leader
	SignSimulationCommit(commit *SimulationCommit) []byte

	// SignCXTSimulation
	//  @Description: sign the cross-shard tx simulation from ssc's leader
	SignCXTSimulation(simulation *CXTSimulation) []byte

	// SignCXTReSimulation
	//  @Description: sign the cross-shard tx simulation from ssc's leader
	SignCXTReSimulation(simulation *CXTReSimulation) []byte

	// HandleCommitVote
	//  @Description: handle the commit vote from ssc's member, aggregate the votes after reaching threshold, then send
	HandleCommitVote(vote *CXTCommitVote)
}

// CrossService
// @Description: It provides functions for the operation between shards
type CrossService interface {
	// HandleCXTSSCCall
	//  @Description: handle the signed call request from caller shard ssc, broadcast to members to simulate contract
	//	execution and wait for the result and signatures
	HandleCXTSSCCall(req *CXTCallSSCRequest) *CXTCallSSCResult

	// HandleCXTSSCRecall
	//  @Description: handle the signed call request from caller shard ssc, broadcast to members to simulate contract
	//	execution and wait for the result and signatures
	HandleCXTSSCRecall(req *CXTRecallSSCRequest) *CXTRecallSSCResult

	// CommitSimulation
	//  @Description: handle the commit request from original shard's ssc, used to commit the simulation result as a
	// 	special transaction to call precompiled contract
	CommitSimulation(commit *SimulationCommit)

	// HandleCXTCommitSSCVote
	// @Description: handle the commit vote from other shard, to build a CXTransactionCommitProof or just try to recall
	HandleCXTCommitSSCVote(vote *CXTCommitSSCVote)

	// HandleCXTCommitProof
	//
	//	@Description: handle the cross-shard transaction submit proof to commit or rollback for the reason
	//					commit: 	1. all shards of cross-shard contract are successfully executed
	//					rollback:	1. execution or simulation failed; 2. transaction timeout; 3. ssc's malicious behavior
	HandleCXTCommitProof(proof *CXTCommitProof)

	// BroadcastCXTRecallProof
	//  @Description: broadcast the cross-shard transaction submit proof to recall for the reason
	BroadcastCXTRecallProof(proof *CXTRecallProof)
}

type Service interface {
	CXTStateSimulationDB
	InternalService
	ShardService
	CrossService
}
