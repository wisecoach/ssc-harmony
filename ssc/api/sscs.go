package api

import (
	"context"
	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/numeric"
	staking "github.com/harmony-one/harmony/staking/types"
	"math/big"
)

const (
	Method_StartSimulateCXTransaction = "ssc_startSimulateCXTransaction"
	Method_HandleSimulateRequest      = "ssc_handleSimulateRequest"
	Method_RequestCallCXT             = "ssc_requestCallCXT"
	Method_HandleCXTCall              = "ssc_handleCXTCall"
	Method_HandleCXTRecallProof       = "ssc_handleCXTRecallProof"
	Method_SignSimulationCommit       = "ssc_signSimulationCommit"
	Method_SignCXTSimulation          = "ssc_signCXTSimulation"
	Method_HandleCommitVote           = "ssc_handleCommitVote"
	Method_HandleCXTSSCCall           = "ssc_handleCXTSSCCall"
	Method_CommitSimulation           = "ssc_commitSimulation"
	Method_HandleCXTCommitSSCVote     = "ssc_handleCXTCommitSSCVote"
	Method_HandleCXTCommitProof       = "ssc_handleCXTCommitProof"
	Method_BroadcastCXTRecallProof    = "ssc_broadcastCXTRecallProof"
	Method_RequestSimulationResult    = "ssc_requestSimulationResult"
	Method_SignalReSimulation         = "ssc_signalReSimulation"
	Method_NotifyReSimulationStart    = "ssc_notifyReSimulationStart"
)

type ShardLocator interface {
	// GetShardID returns the shard ID of the given address
	GetShardID(address common.Address) uint32
	// ShardNum returns the number of shards
	ShardNum() uint32
}

type BLSSigner interface {
	Sign(msg MessageToSign) ([]byte, error)
	// Aggregate aggregate the signature of messages, and return aggregated signature, bitmap and error
	Aggregate(msgs []SSCMessage) (signatures []byte, bitmap []byte, err error)
	Verify(msg BLSSignedMessage) error
}

type TxSigner interface {
	Sign(tx *types.Transaction) (*types.Transaction, error)
	Address() common.Address
}

type TxSubmitter interface {
	SubmitSimulationTx(simulation *CXTSimulation) error
	SubmitCommitOrRollbackTx(proof *CXTCommitProof) error
	SubmitEmptyTx() error
}

type StateDB interface {
	CreateAccount(common.Address)

	SubBalance(common.Address, *big.Int)
	AddBalance(common.Address, *big.Int)
	GetBalance(common.Address) *big.Int

	GetNonce(common.Address) uint64
	SetNonce(common.Address, uint64)

	GetValidatorFirstElectionEpoch(addr common.Address) *big.Int
	AddReward(*staking.ValidatorWrapper, *big.Int, map[common.Address]numeric.Dec) error

	GetSSCConfig() *ShardSimulateCommitteeConfig
	SetSSCConfig(config *ShardSimulateCommitteeConfig)

	AddRefund(uint64)
	SubRefund(uint64)
	GetRefund() uint64

	GetCommittedState(common.Address, common.Hash) common.Hash
	GetState(common.Address, common.Hash) (common.Hash, error)
	SetState(common.Address, common.Hash, common.Hash) error
	GetStateWithLock(txHash common.Hash, callIndex CallIndex, address common.Address, key common.Hash) (common.Hash, error)
	SetStateWithLock(txHash common.Hash, callIndex CallIndex, address common.Address, key common.Hash, value common.Hash) error

	Exist(common.Address) bool
	Empty(common.Address) bool

	RevertToSnapshot(int)
	Snapshot() int

	CommitTx(txHash common.Hash) error
	RollbackTx(txHash common.Hash) error
	Commit(deleteEmptyObjects bool) (common.Hash, error)
}

// CXTStateSimulationDB will save the state of the cross-shard transaction simulation
type CXTStateSimulationDB interface {
	// GetRWSet get the read-write set of the cross-shard transaction simulation
	GetRWSet(txHash common.Hash) *RWSet
	// EndCTX end a cross-shard transaction simulation
	EndCTX(txHash common.Hash)

	CreateAccount(txHash common.Hash, address common.Address)
	SubBalance(db StateDB, txHash common.Hash, address common.Address, balance *big.Int)
	AddBalance(db StateDB, txHash common.Hash, address common.Address, balance *big.Int)
	GetBalance(db StateDB, txHash common.Hash, address common.Address) *big.Int
	GetState(db StateDB, txHash common.Hash, address common.Address, key common.Hash) (common.Hash, error)
	SetState(db StateDB, txHash common.Hash, address common.Address, key common.Hash, value common.Hash) error

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
	SimulateCXTransaction(req *CXTSimulationRequest)

	SimulationResult(txHash common.Hash) (*CXTSimulationSSCResult, error)

	// CallCXTContract
	//
	//	@Description: call for cross-shard contract, send request to leader of CXTransaction, and wait for the simulation
	//	result
	CallCXTContract(req *CXTCallRequest) *CXTCallSSCResult

	// RecallCXContract
	//
	//	@Description: recall for cross-shard contract, send request to leader of CXTransaction, and wait for the simulation
	//	result
	// RecallCXContract(req *CXTRecallRequest) *CXTRecallSSCResult

	// VerifySimulation
	//	@Description: verify the simulation and vote for commit or rollback
	VerifySimulation(simulationBytes []byte, stateDB StateDB)

	CommitOrRollbackWithProof(commitProofBytes []byte, stateDB StateDB) error

	StateLockManager() StateLockManager
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
	HandleSimulateRequest(ctx context.Context, req *CXTSimulationRequest) *CXTSimulationResult

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

	// SignSimulationCommit
	//  @Description: sign the simulation commit from ssc's leader
	SignSimulationCommit(commit *SimulationCommit) []byte

	// SignCXTSimulation
	//  @Description: sign the cross-shard tx simulation from ssc's leader
	SignCXTSimulation(simulation *CXTSimulation) []byte

	// HandleCommitVote
	//  @Description: handle the commit vote from ssc's member, aggregate the votes after reaching threshold, then send
	HandleCommitVote(vote *CXTCommitVote)

	RequestSimulationResult(req *SimulationResultRequest) (*CXTSimulationSSCResult, error)
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
	// HandleCXTSSCRecall(req *CXTRecallSSCRequest) *CXTRecallSSCResult

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

	// SignalReSimulation
	//  @Description: handle the signal to re-simulate the cross-shard transaction
	//  @param signal
	//
	SignalReSimulation(signal *ReSimulationSignal)

	NotifyReSimulationStart(txHash common.Hash)
}

type Service interface {
	ShardLocator
	CXTStateSimulationDB
	InternalService
	ShardService
	CrossService
}
