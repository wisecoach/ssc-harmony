package api

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/block"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/crypto/bls"
	"github.com/harmony-one/harmony/numeric"
	staking "github.com/harmony-one/harmony/staking/types"
)

const (
	Method_StartSimulateCXTransaction = "ssc_startSimulateCXTransaction"
	Method_HandleSimulateRequest      = "ssc_handleSimulateRequest"
	Method_RequestCallCXT             = "ssc_requestCallCXT"
	Method_HandleCXTCall              = "ssc_handleCXTCall"
	Method_SignSimulationCommit       = "ssc_signSimulationCommit"
	Method_SignCXTSimulation          = "ssc_signCXTSimulation"
	Method_HandleCommitVote           = "ssc_handleCommitVote"
	Method_HandleCXTSSCCall           = "ssc_handleCXTSSCCall"
	Method_CommitSimulation           = "ssc_commitSimulation"
	// Method_ReserveLegCommit — 2PC (H1)：首建 SimTx 前，origin 让各相关分片 leader 先“预留”
	//（推导本分片 RWSet 并占 TLV，但不建/不提交 SimTx），全部预留成功后才允许 CommitSimulation 建单。
	Method_ReserveLegCommit       = "ssc_reserveLegCommit"
	Method_HandleCXTCommitSSCVote = "ssc_handleCXTCommitSSCVote"
	Method_HandleCXTCommitProof   = "ssc_handleCXTCommitProof"
	Method_SignalReSimulation     = "ssc_signalReSimulation"
	Method_AddRetryTx             = "ssc_addRetryTx"
	Method_RetryCommit            = "ssc_retryCommit"
	Method_RetryCommitDAG         = "ssc_retryCommitDAG"
	Method_RetryCancel            = "ssc_retryCancel"
	Method_HandleRetrySignal      = "ssc_handleRetrySignal"
	Method_SLTest                 = "ssc_sLTest"
	Method_HandleNewEpoch         = "ssc_handleNewEpoch"
	Method_AddToPassivePool       = "ssc_addToPassivePool"
	Method_SignDeadlockProbe      = "ssc_signDeadlockProbe"
	Method_DetectDeadlockProbe    = "ssc_detectDeadlockProbe"
	Method_StoreSimDAGPatch       = "ssc_storeSimDAGPatch"
)

type ShardLocator interface {
	// GetShardID returns the shard ID of the given address
	GetShardID(address common.Address) uint32
	// ShardNum returns the number of shards
	ShardNum() uint32
}

type BLSSignerMgr interface {
	GetSSCSigner() BLSSigner
	GetValidatorSigner() BLSSigner
	UpdateSSCPubKeys(shardID uint32, epoch Epoch, changed bool, addr2Index map[common.Address]int, pubKeys []bls.PublicKeyWrapper, threshold int)
	UpdateValidatorPubKeys(shardID uint32, epoch Epoch, changed bool, addr2Index map[common.Address]int, pubKeys []bls.PublicKeyWrapper, threshold int)
}

type BLSSigner interface {
	Address() common.Address
	Sign(msg MessageToSign) ([]byte, error)
	// Aggregate aggregate the signature of messages, and return aggregated signature, bitmap and error
	Aggregate(msgs []SSCMessage) (signatures []byte, bitmap []byte, err error)
	Verify(msg BLSSignedMessage) error
	UpdatePubKeys(shardID uint32, epoch Epoch, changed bool, addr2Index map[common.Address]int, pubKeys []bls.PublicKeyWrapper, threshold int)
}

type TxSigner interface {
	Sign(tx *types.Transaction) (*types.Transaction, error)
	Address() common.Address
}

type TxSubmitter interface {
	SubmitSimulationTx(simulation *CXTSimulation) error
	SubmitSimulationTxWithSigner(simulation *CXTSimulation, signerType string) error
	SubmitCommitOrRollbackTx(proof *CXTCommitProof) error
	SubmitVictimTx(victimTx *VictimTx) error
	SubmitEmptyTx() error
	SubmitNewEpoch(newEpoch *NewEpoch) error
	SubmitUploadOpinions(uploadOpinions *SelfOpinions) error
	OnBlockCommitted(block *types.Block)
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
	GetState(common.Hash, common.Address, common.Hash) (common.Hash, error)
	SetState(common.Hash, common.Address, common.Hash, common.Hash) error
	GetStateWithoutLock(common.Address, common.Hash) (common.Hash, error)
	SetStateWithoutLock(common.Address, common.Hash, common.Hash) error
	GetAndLockState(txHash common.Hash, callIndex CallIndex, address common.Address, key common.Hash) (common.Hash, error)
	SetAndLockState(txHash common.Hash, callIndex CallIndex, address common.Address, key common.Hash, value common.Hash) error
	CheckLock(key LockKey, txHash common.Hash) error

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

	SubSimuBalance(txHash common.Hash, callIndex CallIndex, address common.Address, balance *big.Int) error
	AddSimuBalance(txHash common.Hash, callIndex CallIndex, address common.Address, balance *big.Int) error
	GetSimuBalance(txHash common.Hash, callIndex CallIndex, address common.Address) (*big.Int, error)
	GetSimuState(txHash common.Hash, callIndex CallIndex, address common.Address, key common.Hash) (common.Hash, error)
	SetSimuState(txHash common.Hash, callIndex CallIndex, address common.Address, key common.Hash, value common.Hash) error
	GetResult(txHash common.Hash, callIndex CallIndex) (result []byte, leftOverGas uint64, err error)
}

type VM interface {
	CrossCall(targetShardId uint32, caller common.Address, addr common.Address, input []byte, gas uint64, value *big.Int) (ret []byte, leftOverGas uint64, err error)
}

// InternalService
//
//	@Description: It provides functions for internal modules
type InternalService interface {
	CXTStateSimulationDB

	// SimulateCXTransaction
	//
	//	@Description: simulate cross-shard transaction called by proposer, send request to leader of CXTransaction
	SimulateCXTransaction(req *CXTSimulationRequest)

	// CallCXTContract
	//
	//	@Description: call for cross-shard contract, send request to leader of CXTransaction, and wait for the simulation
	//	result
	CallCXTContract(req *CXTCallRequest) *CXTCallSSCResult

	// VerifySimulation
	//	@Description: verify the simulation and vote for commit or rollback
	VerifySimulation(simulationBytes []byte, stateDB StateDB, header *block.Header)

	CommitOrRollbackWithProof(commitProofBytes []byte, stateDB StateDB, blockNum uint64) error

	// HandleVictimTx 处理块内 VictimTx（CMH 判环后打进本分片的控制交易）：
	// 本节点作为本分片 validator，向本分片 leader 投一张 rollback 票（VictimTx 方案）。
	HandleVictimTx(victimTxBytes []byte) error

	NewEpoch(newEpochBytes []byte, vm VM, stateDB StateDB, blockNum uint64) error

	UploadSLOpinion(opinionsBytes []byte, stateDB StateDB) error

	StateLockManager() StateLockManager

	BlockCommitted(block *types.Block) error
}

// ShardService
// @Description: It provides functions for the operation in the self'S shard
type ShardService interface {
	// StartSimulateCXTransaction
	//  @Description: handle request from proposer, process the request as following:
	//			1. broadcast request to all members
	//			2. wait for k result of simulation
	// 			3. aggregate the result and signatures and return to proposer
	// 			4. notify all associated shards to commit simulation
	StartSimulateCXTransaction(req *CXTSimulationRequest) *CXTSimulationSSCResult

	// HandleSimulateRequest
	//  @Description: handle request from ssc'S leader, process the request as following:
	//	1. simulate the contract execution, and save the read-write set to build the simulation result
	//	2. Once need to call cross-shard contract, then send request to leader of target shard'S ssc
	//  3. after simulation completed, send the result to leader of ssc
	HandleSimulateRequest(ctx context.Context, req *CXTSimulationRequest) *CXTSimulationResult

	// HandleReSimulateRequest
	//  @Description: handle request from ssc'S leader
	// HandleReSimulateRequest(req *CXTReSimulationRequest) *CXTReSimulationResult

	// RequestCallCXT
	//
	//	 @Description: handle request from ssc'S leader, aggregate signatures of request after reaching threshold, then send
	//		signed request to leader of target shard'S ssc
	RequestCallCXT(req *CXTCallRequest) *CXTCallSSCResult

	// HandleCXTCall
	//
	//	@Description: handle cross-shard call request from leader, simulate the contract execution and return the result
	HandleCXTCall(req *CXTCallSSCRequest) *CXTCallResult

	// SignSimulationCommit
	//  @Description: sign the simulation commit from ssc'S leader
	SignSimulationCommit(commit *SimulationCommit) []byte

	// SignCXTSimulation
	//  @Description: sign the cross-shard tx simulation from ssc'S leader
	SignCXTSimulation(simulation *CXTSimulation) []byte

	// SignDeadlockProbe
	//  @Description: sign a deadlock probe for BLS committee aggregation
	SignDeadlockProbe(probe *DeadlockProbe) []byte

	// HandleCommitVote
	//  @Description: handle the commit vote from ssc'S member, aggregate the votes after reaching threshold, then send
	HandleCommitVote(vote *CXTCommitVote)

	SLTest(req *SLTestRequest) *SLTestResult

	AddRetryTx(tx *RetryTx)

	RetryCommit(txHash common.Hash) *RetryCommitResp

	// RetryCommitDAG — DSN-55 rev2：DAG 救援 attempt 的 RetryCommit。
	// 与 RetryCommit 唯一区别：取得 TLV 锁后提权保锁(不被 wound)。
	RetryCommitDAG(txHash common.Hash) *RetryCommitResp

	// ReserveLegCommit — 2PC (H1) Phase A：首建 SimTx 前 origin 调用各相关分片 leader，
	// 让它推导本分片 RWSet 并占 TLV（预留），但【不建/不提交 SimTx】。
	// 返回 Locked=true 表示本分片已持有 TLV、等 Phase B CommitSimulation 复用该锁建单；
	// 返回 Locked=false 表示本分片此刻拿不到 TLV（origin 应整单放弃并 RetryCancel 已预留分片）。
	ReserveLegCommit(commit *SimulationCommit) *RetryCommitResp

	RetryCancel(txHash common.Hash)

	AddToPassivePool(txHash common.Hash)

	HandleNewEpoch(newEpoch *NewEpoch, blockNum uint64) error

	// StoreSimDAGPatch — DSN-54：leader 在 RetryCommit 消费上游后，向本分片成员广播
	// 该交易本轮模拟所需的链下 DAG patch 子图；成员据此写入本地 simDAGPatches。
	StoreSimDAGPatch(req *StoreSimDAGPatchRequest)
}

// CrossService
// @Description: It provides functions for the operation between shards
type CrossService interface {
	// HandleCXTSSCCall
	//  @Description: handle the signed call request from caller shard ssc, broadcast to members to simulate contract
	//	execution and wait for the result and signatures
	HandleCXTSSCCall(req *CXTCallSSCRequest) *CXTCallSSCResult

	// CommitSimulation
	//  @Description: handle the commit request from original shard'S ssc, used to commit the simulation result as a
	// 	special transaction to call precompiled contract
	CommitSimulation(commit *SimulationCommit)

	// HandleCXTCommitSSCVote
	// @Description: handle the commit vote from other shard, to build a CXTransactionCommitProof or just try to recall
	HandleCXTCommitSSCVote(vote *CXTCommitSSCVote)

	// HandleCXTCommitProof
	//
	//	@Description: handle the cross-shard transaction submit proof to commit or rollback for the reason
	//					commit: 	1. all shards of cross-shard contract are successfully executed
	//					rollback:	1. execution or simulation failed; 2. transaction timeout; 3. ssc'S malicious behavior
	HandleCXTCommitProof(proof *CXTCommitProof)

	// SignalReSimulation
	//  @Description: handle the signal to re-simulate the cross-shard transaction
	//  @param signal
	//
	SignalReSimulation(signal *RetrySignals)

	// HandleRetrySignal
	//  @Description: handle the retry signal from another shard's leader.
	//  When a SimTx is submitted, the retry scheduler sends this signal
	//  to the origin shard with its UpstreamTxList so the downstream retry simulation
	//  can read the upstream's produced state values directly.
	HandleRetrySignal(signal *RetrySignal)

	// DetectDeadlockProbe
	//  @Description: handle a priority-aware reverse CMH deadlock probe from another shard.
	DetectDeadlockProbe(probe *DeadlockProbe) *DeadlockProbeAck
}

type Service interface {
	ShardLocator
	CXTStateSimulationDB
	InternalService
	ShardService
	CrossService

	// ExtractSSCTransactions 从内部池按类型顺序提取一批内部交易（DSN-48）。
	// 返回二维（第一维=InternalTxType 桶），供 worker 写入区块。
	ExtractSSCTransactions(maxTotal int) [][]*types.SSCInternalTx

	// DiscardSSCInternalTx 从内部池删除一条内部交易（按 hash）。
	// 用于执行失败、应停止重试的内部交易（DSN-48：避免每块无限重试）。
	DiscardSSCInternalTx(tx *types.SSCInternalTx)

	// GetModuleStatus 返回 SSC 各模块当前维护的数据量快照。
	// 供实验结束后通过监控 gRPC 服务（SSCMonitorService）调用，
	// 用于分析各模块是否正确释放资源。
	GetModuleStatus() *ModuleStatus
}
