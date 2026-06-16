package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/harmony-one/harmony/core/types"
)

const (
	committeesByteSizeStr = "SSC/committeesByteSize"
	committeesOffsetStr   = "SSC/committeesOffset"
)

var (
	ErrInvalidExecution = errors.New("invalid execution")
)

var (
	SSCPrecompileContractAddr = common.BytesToAddress([]byte{248})
	CommitteesByteSize        = crypto.Keccak256Hash([]byte(committeesByteSizeStr))
	CommitteeOffset           = crypto.Keccak256Hash([]byte(committeesOffsetStr))
)

type SimulationCommitStatus int
type CXTCommitType int
type CXTCommitReason int
type CXTStatus int
type MaliciousStrategy int

const (
	OK SimulationCommitStatus = iota
	ExecutionFailed
	LockConflict
	PoolTimeout
)

func (s SimulationCommitStatus) String() string {
	switch s {
	case OK:
		return "OK"
	case ExecutionFailed:
		return "ExecutionFailed"
	case LockConflict:
		return "LockConflict"
	case PoolTimeout:
		return "PoolTimeout"
	default:
		return "Unknown"
	}
}

const (
	Commit CXTCommitType = iota
	Recall
	Rollback
)

func (c CXTCommitType) String() string {
	switch c {
	case Commit:
		return "Commit"
	case Recall:
		return "Recall"
	case Rollback:
		return "Rollback"
	default:
		return "Unknown"
	}
}

const (
	ReasonSuccess CXTCommitReason = iota
	ReasonExecutionFailed
	ReasonInvalidSimulation
	ReasonConflictRWSetFailedLock
	ReasonConflictRWSetRecall
	ReasonCxtTimeoutForSp1
	ReasonMaxOnChainRetriesExceeded
)

func (c CXTCommitReason) String() string {
	switch c {
	case ReasonSuccess:
		return "SUCCESS"
	case ReasonExecutionFailed:
		return "ExecutionFailed"
	case ReasonInvalidSimulation:
		return "InvalidSimulation"
	case ReasonConflictRWSetFailedLock:
		return "ConflictRWSet_FailedLock"
	case ReasonConflictRWSetRecall:
		return "ConflictRWSet_Recall"
	case ReasonCxtTimeoutForSp1:
		return "ReasonCxtTimeoutForSp1"
	case ReasonMaxOnChainRetriesExceeded:
		return "MaxOnChainRetriesExceeded"
	default:
		return "Unknown"
	}
}

const (
	WAITING_FOR_SIMULATING             = iota // init status -> startSimulation or startCall
	SIMULATING                                // first simulating -> SubmitSimulationTx
	RESIMULATING                              // second simulating -> SubmitSimulationTx
	WAITING_FOR_RESIMULATING_ON_CHAIN         // VerifySimulation conflict -> startSimulation or startCall
	WAITING_FOR_RESIMULATION_OFF_CHAIN        // HandleSimulateRequest conflict -> startSimulation or startCall
	SIMULATION_COMMMITTING                    // sendSimulationCommit -> VerifySimulation
	VERIFYING_SIMULATION                      // VerifySimulation -> SubmitCommitOrRollbackTx
	BUILDING_COMMIT_PROOF                     // HandleCommitVote
	CXT_COMMITTING                            // SubmitCommitOrRollbackTx -> CommitOrRollbackTx
	CXT_ROLLBACKING                           // SubmitCommitOrRollbackTx -> CommitOrRollbackTx
	CXT_COMMITTED                             // CommitOrRollbackTx -> closeTransaction
	CXT_ROLLBACKED                            // CommitOrRollbackTx -> closeTransaction
)

func (s CXTStatus) String() string {
	switch s {
	case WAITING_FOR_SIMULATING:
		return "WAITING_FOR_SIMULATING"
	case WAITING_FOR_RESIMULATING_ON_CHAIN:
		return "WAITING_FOR_RESIMULATING_ON_CHAIN"
	case WAITING_FOR_RESIMULATION_OFF_CHAIN:
		return "WAITING_FOR_RESIMULATION_OFF_CHAIN"
	case SIMULATING:
		return "SIMULATING"
	case RESIMULATING:
		return "RESIMULATING"
	case SIMULATION_COMMMITTING:
		return "SIMULATION_COMMMITTING"
	case BUILDING_COMMIT_PROOF:
		return "BUILDING_COMMIT_PROOF"
	case VERIFYING_SIMULATION:
		return "VERIFYING_SIMULATION"
	case CXT_COMMITTING:
		return "CXT_COMMITTING"
	case CXT_ROLLBACKING:
		return "CXT_ROLLBACKING"
	case CXT_COMMITTED:
		return "CXT_COMMITTED"
	case CXT_ROLLBACKED:
		return "CXT_ROLLBACKED"
	default:
		return "Unknown"
	}
}

const (
	MaliciousNone = iota
	MaliciousDelay
	MaliciousDenied
)

func (m MaliciousStrategy) String() string {
	switch m {
	case MaliciousNone:
		return "MaliciousNone"
	case MaliciousDelay:
		return "MaliciousDelay"
	case MaliciousDenied:
		return "MaliciousDenied"
	default:
		return "Unknown"
	}
}

type Epoch uint64

type RelatedShards []uint32

func (r RelatedShards) Contains(shard uint32) bool {
	for _, s := range r {
		if s == shard {
			return true
		}
	}
	return false
}

func (r RelatedShards) ToMap() map[uint32]struct{} {
	m := make(map[uint32]struct{})
	for _, s := range r {
		m[s] = struct{}{}
	}
	return m
}

func (r RelatedShards) Add(shard uint32) RelatedShards {
	for _, s := range r {
		if s == shard {
			return r
		}
	}
	return append(r, shard)
}

func (r RelatedShards) Merge(shards RelatedShards) RelatedShards {
	newRS := r
	for _, s := range shards {
		newRS = newRS.Add(s)
	}
	return newRS
}

// CallIndex is the index of the cross-shard call
type CallIndex []int

var MINCallIndex = CallIndex{-1}

// Top is the call is the top call
func (c CallIndex) Top() bool {
	return len(c) == 0
}

func (c CallIndex) ToString() string {
	return strings.Join(strings.Fields(fmt.Sprint(c)), ":")
}

func FromString(callIndexStr string) CallIndex {
	indexStrs := strings.Split(callIndexStr, ":")
	indexes := make([]int, len(indexStrs))
	for i, indexStr := range indexStrs {
		indexes[i] = int(indexStr[0])
	}
	return indexes
}

// Compare compare each element, if prefix is the same, the longer one is smaller
func (c CallIndex) Compare(other CallIndex) int {
	i := 0
	for i < len(c) && i < len(other) {
		if c[i] != other[i] {
			return c[i] - other[i]
		}
		i++
	}
	return len(other) - len(c)
}

type CallNodeData struct {
	CallIndex CallIndex       `json:"call_index"`
	ShardId   uint32          `json:"shard_id"`
	Children  []*CallNodeData `json:"children,omitempty"`
}

type Config struct {
	SimulationLimit          int
	CallTimeout              time.Duration
	CXTTimeout               time.Duration
	SimulationCommitGasLimit uint64
	SimulationCommitGasPrice *big.Int
	LockExecutionOnce        bool
	MischiefConfig           *MischiefConfig
}

// Candidate is the candidate of the committee
type Candidate struct {
	Address    common.Address
	Stake      *big.Int `json:"stake" gencodec:"required"`
	Reputation uint64
}

// Member is the member of the committee
type Member struct {
	Address   common.Address
	PubKey    string
	Endpoint  string
	BLSPubKey string
}

type ShardSimulateCommitteeConfig struct {
	Committees []*ShardSimulateCommittee `json:"committees" yaml:"committees"`
	Timeout    *TimeoutConfig            `json:"timeout" yaml:"timeout"`
	Reputation *ReputationConfig         `json:"reputation" yaml:"reputation"`
}

// ShardSimulateCommittee (SSC) is the committee of the shard simulation
type ShardSimulateCommittee struct {
	ShardID            uint32
	Epoch              Epoch
	Members            []*Member
	Threshold          int
	Validators         []*Member
	ValidatorThreshold int
	Number             int
	MemberIndex        map[common.Address]int `json:"-"`
	ValidatorIndex     map[common.Address]int `json:"-"`
}

type TimeoutConfig struct {
	Sp1               uint64 `json:"sp1" yaml:"sp1"`                                   // source phase 1, used to notify origin shard to rollback cxt for timeout
	PoolTimeout       uint64 `json:"pool_timeout" yaml:"pool_timeout"`                 // the timeout to remove cxt from pool
	MaxOnChainRetries uint64 `json:"max_on_chain_retries" yaml:"max_on_chain_retries"` // max retries for on-chain verification
}

type ReputationConfig struct {
	SLFileSize    int           `json:"sl_file_size" yaml:"sl_file_size"`       // the file size of the subjective logic test
	SLDifficulty  int           `json:"sl_difficulty" yaml:"sl_difficulty"`     // the difficulty of the subjective logic test
	SLTimeout     time.Duration `json:"sl_timeout" yaml:"sl_timeout"`           // the timeout to subjective logic test
	SLPeriod      time.Duration `json:"sl_period" yaml:"sl_period"`             // the period to subjective logic test
	A             float64       `json:"a" yaml:"a"`                             // base rate of Omega for SL
	C             float64       `json:"c" yaml:"c"`                             // the uncertainty constant
	W1            float64       `json:"w1" yaml:"w1"`                           // the weight of A
	W2            float64       `json:"w2" yaml:"w2"`                           // the weight of S
	W3            float64       `json:"w3" yaml:"w3"`                           // the weight of P
	RewardPrice   *big.Int      `json:"reward_price" yaml:"reward_price"`       // reward gas price
	RB            float64       `json:"rb" yaml:"rb"`                           // reward base coefficient
	RS            float64       `json:"rs" yaml:"rs"`                           // reward simulation coefficient
	BlockPerEpoch uint64        `json:"block_per_epoch" yaml:"block_per_epoch"` // blocks per epoch
	Theta         float64       `json:"theta" yaml:"theta"`                     // coefficient of cost
	T             uint64        `json:"t" yaml:"t"`                             // epochs for reward to be redeemable
}

type MessageToSign interface {
	// Bytes return the bytes to calculate hash
	Bytes() []byte
}

type SSCMessage interface {
	MessageToSign
	GetSenderAddr() common.Address
	GetSignature() []byte
	GetEpochs() []Epoch
}

type BaseSSCMessage struct {
	Signature  []byte
	SenderAddr common.Address
	Epochs     []Epoch
}

func (s BaseSSCMessage) GetSenderAddr() common.Address {
	return s.SenderAddr
}

func (s BaseSSCMessage) GetSignature() []byte {
	return s.Signature
}

func (s BaseSSCMessage) GetEpochs() []Epoch {
	return s.Epochs
}

func (s BaseSSCMessage) Bytes() []byte {
	return make([]byte, 0)
}

type BLSSignedMessage interface {
	MessageToSign
	GetShardId() uint32
	GetSignatures() []byte
	GetBLSBitMap() []byte
	GetEpochs() []Epoch
}

type BaseBLSSignedMessage struct {
	ShardId    uint32
	Signatures []byte
	BLSBitMap  []byte
	Epochs     []Epoch
}

func (m BaseBLSSignedMessage) GetShardId() uint32 {
	return m.ShardId
}

func (m BaseBLSSignedMessage) GetSignatures() []byte {
	return m.Signatures
}

func (m BaseBLSSignedMessage) GetBLSBitMap() []byte {
	return m.BLSBitMap
}

func (m BaseBLSSignedMessage) GetEpochs() []Epoch {
	return m.Epochs
}

// CXTSimulationRequest is the request of the cross-shard transaction simulation
type CXTSimulationRequest struct {
	BlockNum      uint64
	Epochs        []Epoch
	TxHash        common.Hash
	SimulationNum int
	Author        *common.Address
	BlockHash     common.Hash
	Tx            *types.Transaction
	From          common.Address
	GasPool       uint64
}

// CXTSimulationResult is the result of the cross-shard transaction simulation
type CXTSimulationResult struct {
	BaseSSCMessage
	RelatedShards RelatedShards
	Result        []byte
	Receipt       *types.Receipt
	UsedGas       uint64
	Err           string
	TreeNode      *CallNodeData
}

func (m *CXTSimulationResult) Bytes() []byte {
	type CXTSimulationResultWithoutSignature struct {
		RelatedShards RelatedShards
		Result        []byte
		Receipt       *types.Receipt
		UsedGas       uint64
		Err           string
		TreeNode      *CallNodeData
	}
	msg := CXTSimulationResultWithoutSignature{
		RelatedShards: m.RelatedShards,
		Result:        m.Result,
		Receipt:       m.Receipt,
		UsedGas:       m.UsedGas,
		Err:           m.Err,
		TreeNode:      m.TreeNode,
	}
	bytes, err := json.Marshal(msg)
	if err != nil {
		return nil
	}
	return bytes
}

// CXTSimulationSSCResult is CXTSimulationResult with the signature of SSC
type CXTSimulationSSCResult struct {
	RelatedShards RelatedShards
	Result        []byte
	Receipt       *types.Receipt
	UsedGas       uint64
	Err           string
	TreeNode      *CallNodeData
	BaseBLSSignedMessage
}

func (m *CXTSimulationSSCResult) Bytes() []byte {
	type CXTSimulationSSCResultWithoutSignature struct {
		RelatedShards RelatedShards
		Result        []byte
		Receipt       *types.Receipt
		UsedGas       uint64
		Err           string
		TreeNode      *CallNodeData
	}
	msg := CXTSimulationSSCResultWithoutSignature{
		RelatedShards: m.RelatedShards,
		Result:        m.Result,
		Receipt:       m.Receipt,
		UsedGas:       m.UsedGas,
		Err:           m.Err,
		TreeNode:      m.TreeNode,
	}
	bytes, err := json.Marshal(msg)
	if err != nil {
		return nil
	}
	return bytes
}

// CXTReSimulationRequest is the request of the cross-shard transaction simulation
type CXTReSimulationRequest struct {
	BaseSSCMessage
	SimulationNum int
	TxHash        common.Hash
}

func (m *CXTReSimulationRequest) Bytes() []byte {
	type CXTReSimulationRequestWithoutSignature struct {
		SimulationNum int
		TxHash        common.Hash
	}
	msg := CXTReSimulationRequestWithoutSignature{
		SimulationNum: m.SimulationNum,
		TxHash:        m.TxHash,
	}
	bytes, err := json.Marshal(msg)
	if err != nil {
		return nil
	}
	return bytes
}

// CXTReSimulationSSCResult is CXTSimulationResult with the signature of SSC
type CXTReSimulationSSCResult struct {
	SimulationNum int
	RelatedShards RelatedShards
	Result        []byte
	Receipt       *types.Receipt
	UsedGas       uint64
	Err           error
	TreeNode      *CallNodeData
	BaseBLSSignedMessage
}

func (m *CXTReSimulationSSCResult) Bytes() []byte {
	type CXTReSimulationSSCResultWithoutSignature struct {
		SimulationNum int
		RelatedShards RelatedShards
		Result        []byte
		Receipt       *types.Receipt
		UsedGas       uint64
		Err           error
		TreeNode      *CallNodeData
	}
	msg := CXTReSimulationSSCResultWithoutSignature{
		SimulationNum: m.SimulationNum,
		RelatedShards: m.RelatedShards,
		Result:        m.Result,
		Receipt:       m.Receipt,
		UsedGas:       m.UsedGas,
		Err:           m.Err,
		TreeNode:      m.TreeNode,
	}
	bytes, err := json.Marshal(msg)
	if err != nil {
		return nil
	}
	return bytes
}

// CXTSimulation is the simulation of the cross-shard transaction
type CXTSimulation struct {
	SimulationNum int
	TxHash        common.Hash
	Nonce         uint64
	Sender        common.Address
	ShardId       uint32
	OriginShardId uint32
	RelatedShards RelatedShards
	CallStates    []*CXTCallState // all cross-shard call of cxt related to this shard
	BaseBLSSignedMessage
}

func (m *CXTSimulation) Bytes() []byte {
	type CXTSimulationWithoutSignature struct {
		SimulationNum int
		TxHash        common.Hash
		Nonce         uint64
		Sender        common.Address
		ShardId       uint32
		OriginShardId uint32
		RelatedShards RelatedShards
		CallStates    []*CXTCallState
	}
	msg := CXTSimulationWithoutSignature{
		SimulationNum: m.SimulationNum,
		TxHash:        m.TxHash,
		Nonce:         m.Nonce,
		Sender:        m.Sender,
		ShardId:       m.ShardId,
		OriginShardId: m.OriginShardId,
		RelatedShards: m.RelatedShards,
		CallStates:    m.CallStates,
	}
	bytes, err := json.Marshal(msg)
	if err != nil {
		return nil
	}
	return bytes
}

func (m *CXTSimulation) String() string {
	callStatesStr := ""
	return fmt.Sprintf("simulation_%s_%d: {%S}", m.TxHash, m.SimulationNum, callStatesStr)
}

// CXTReSimulation is the simulation of the cross-shard transaction
type CXTReSimulation struct {
	SimulationNum int
	TxHash        common.Hash
	ShardId       uint32
	OriginShardId uint32
	RelatedShards RelatedShards
	RecallStates  []*CXTRecallState // all cross-shard call of cxt related to this shard
	BaseBLSSignedMessage
}

func (m *CXTReSimulation) Bytes() []byte {
	type CXTReSimulationWithoutSignature struct {
		SimulationNum int
		TxHash        common.Hash
		ShardId       uint32
		OriginShardId uint32
		RelatedShards RelatedShards
		RecallStates  []*CXTRecallState
	}
	msg := CXTReSimulationWithoutSignature{
		SimulationNum: m.SimulationNum,
		TxHash:        m.TxHash,
		ShardId:       m.ShardId,
		OriginShardId: m.OriginShardId,
		RelatedShards: m.RelatedShards,
		RecallStates:  m.RecallStates,
	}
	bytes, err := json.Marshal(msg)
	if err != nil {
		return nil
	}
	return bytes
}

// CXTCallState is the state of cross-shard call
type CXTCallState struct {
	CallIndex        CallIndex
	TopRequest       *CXTSimulationRequest // the start request of the simulation, nil if it'S not the top request
	CallRequest      *CXTCallSSCRequest    // the request of the cross-shard call, nil if it'S the top request
	RWSet            *RWSet                // Read and write set generated by the simulation
	DependentResults []*CXTCallSSCResult
	CallResult       *CXTCallSSCResult
	TopResult        *CXTSimulationSSCResult
}

// CXTRecallState is the state of cross-shard Recall
type CXTRecallState struct {
	SimulationNum    int
	CallIndex        CallIndex
	Request          *CXTRecallSSCRequest
	RWSet            *RWSet // Read and write set generated by the simulation
	DependentResults []*CXTRecallSSCResult
	Result           *CXTRecallSSCResult
}

// CXTCallRequest is the request of cross-shard call
type CXTCallRequest struct {
	BaseSSCMessage
	OriginShardId uint32
	FromShardId   uint32
	TargetShardId uint32
	SimulationNum int
	RelatedShards RelatedShards
	TxHash        common.Hash
	Nonce         uint64
	TxSender      []byte
	CallIndex     CallIndex
	Caller        common.Address
	Addr          common.Address
	Input         []byte
	Gas           uint64
	GasPrice      *big.Int `json:"gas_price" gencodec:"required"`
	Value         *big.Int `json:"value" gencodec:"required"`
	BlockHash     []byte
}

func (m *CXTCallRequest) Bytes() []byte {
	type CXTCallRequestWithoutSignature struct {
		OriginShardId uint32
		FromShardId   uint32
		TargetShardId uint32
		SimulationNum int
		RelatedShards RelatedShards
		TxHash        common.Hash
		Nonce         uint64
		TxSender      []byte
		CallIndex     CallIndex
		Caller        common.Address
		Addr          common.Address
		Input         []byte
		Gas           uint64
		GasPrice      *big.Int `json:"gas_price" gencodec:"required"`
		Value         *big.Int `json:"value" gencodec:"required"`
	}
	msg := CXTCallRequestWithoutSignature{
		OriginShardId: m.OriginShardId,
		FromShardId:   m.FromShardId,
		TargetShardId: m.TargetShardId,
		SimulationNum: m.SimulationNum,
		RelatedShards: m.RelatedShards,
		TxHash:        m.TxHash,
		Nonce:         m.Nonce,
		TxSender:      m.TxSender,
		CallIndex:     m.CallIndex,
		Caller:        m.Caller,
		Addr:          m.Addr,
		Input:         m.Input,
		Gas:           m.Gas,
		GasPrice:      m.GasPrice,
		Value:         m.Value,
	}
	bytes, err := json.Marshal(msg)
	if err != nil {
		return nil
	}
	return bytes
}

// CXTCallSSCRequest is the request of cross-shard call with the signature of SSC
type CXTCallSSCRequest struct {
	OriginShardId uint32
	FromShardId   uint32
	TargetShardId uint32
	SimulationNum int
	RelatedShards RelatedShards
	TxHash        common.Hash
	Nonce         uint64
	TxSender      []byte
	CallIndex     CallIndex
	Caller        common.Address
	Addr          common.Address
	Input         []byte
	Gas           uint64
	GasPrice      *big.Int `json:"gas_price" gencodec:"required"`
	Value         *big.Int `json:"value" gencodec:"required"`
	BaseBLSSignedMessage

	// the SSC set the block hash of the simulation, used to sync the state, note: it'S not included in the signature
	BlockHash common.Hash
	BlockNum  uint64
}

func (m *CXTCallSSCRequest) Bytes() []byte {
	type CXTCallSSCRequestWithoutSignature struct {
		OriginShardId uint32
		FromShardId   uint32
		TargetShardId uint32
		SimulationNum int
		RelatedShards RelatedShards
		TxHash        common.Hash
		Nonce         uint64
		TxSender      []byte
		CallIndex     CallIndex
		Caller        common.Address
		Addr          common.Address
		Input         []byte
		Gas           uint64
		GasPrice      *big.Int `json:"gas_price" gencodec:"required"`
		Value         *big.Int `json:"value" gencodec:"required"`
	}
	msg := CXTCallSSCRequestWithoutSignature{
		OriginShardId: m.OriginShardId,
		FromShardId:   m.FromShardId,
		TargetShardId: m.TargetShardId,
		SimulationNum: m.SimulationNum,
		RelatedShards: m.RelatedShards,
		TxHash:        m.TxHash,
		Nonce:         m.Nonce,
		TxSender:      m.TxSender,
		CallIndex:     m.CallIndex,
		Caller:        m.Caller,
		Addr:          m.Addr,
		Input:         m.Input,
		Gas:           m.Gas,
		GasPrice:      m.GasPrice,
		Value:         m.Value,
	}
	bytes, err := json.Marshal(msg)
	if err != nil {
		return nil
	}
	return bytes
}

// CXTCallResult is the result of cross-shard call
type CXTCallResult struct {
	TxHash        common.Hash
	CallIndex     CallIndex
	RelatedShards RelatedShards
	Result        []byte
	LeftOverGas   uint64
	BlockHash     common.Hash // the state of block hash of simulation
	Err           string
	TreeNode      *CallNodeData
	BaseSSCMessage
}

func (m *CXTCallResult) Bytes() []byte {
	type CXTCallResultWithoutSignature struct {
		TxHash        common.Hash
		CallIndex     CallIndex
		RelatedShards RelatedShards
		Result        []byte
		LeftOverGas   uint64
		BlockHash     common.Hash
		Err           string
		TreeNode      *CallNodeData
	}
	msg := CXTCallResultWithoutSignature{
		TxHash:        m.TxHash,
		CallIndex:     m.CallIndex,
		RelatedShards: m.RelatedShards,
		Result:        m.Result,
		LeftOverGas:   m.LeftOverGas,
		BlockHash:     m.BlockHash,
		Err:           m.Err,
		TreeNode:      m.TreeNode,
	}
	bytes, err := json.Marshal(msg)
	if err != nil {
		return nil
	}
	return bytes
}

// CXTCallSSCResult is the result of cross-shard call with the signature of SSC
type CXTCallSSCResult struct {
	TxHash        common.Hash
	CallIndex     CallIndex
	RelatedShards RelatedShards
	Result        []byte
	LeftOverGas   uint64
	BlockHash     common.Hash // the state of block hash of simulation
	Err           string
	TreeNode      *CallNodeData
	BaseBLSSignedMessage
}

func (m *CXTCallSSCResult) Bytes() []byte {
	type CXTCallSSCResultWithoutSignature struct {
		TxHash        common.Hash
		CallIndex     CallIndex
		RelatedShards RelatedShards
		Result        []byte
		LeftOverGas   uint64
		BlockHash     common.Hash
		Err           string
		TreeNode      *CallNodeData
	}
	msg := CXTCallSSCResultWithoutSignature{
		TxHash:        m.TxHash,
		CallIndex:     m.CallIndex,
		RelatedShards: m.RelatedShards,
		Result:        m.Result,
		LeftOverGas:   m.LeftOverGas,
		BlockHash:     m.BlockHash,
		Err:           m.Err,
		TreeNode:      m.TreeNode,
	}
	bytes, err := json.Marshal(msg)
	if err != nil {
		return nil
	}
	return bytes
}

// CXTRecallRequest is the request of cross-shard Recall
type CXTRecallRequest struct {
	BaseSSCMessage
	SimulationNum int
	FromShardId   uint32
	OriginShardId uint32
	TargetShardId uint32
	RelatedShards RelatedShards
	TxHash        common.Hash
	CallIndex     CallIndex
	Caller        common.Address
	Addr          common.Address
	Input         []byte
	Gas           uint64
	GasPrice      *big.Int `json:"gas_price" gencodec:"required"`
	Value         *big.Int `json:"value" gencodec:"required"`
}

func (m *CXTRecallRequest) Bytes() []byte {
	type CXTRecallRequestWithoutSignature struct {
		SimulationNum int
		FromShardId   uint32
		OriginShardId uint32
		TargetShardId uint32
		RelatedShards RelatedShards
		TxHash        common.Hash
		CallIndex     CallIndex
		Caller        common.Address
		Addr          common.Address
		Input         []byte
		Gas           uint64
		GasPrice      *big.Int `json:"gas_price" gencodec:"required"`
		Value         *big.Int `json:"value" gencodec:"required"`
	}
	msg := CXTRecallRequestWithoutSignature{
		SimulationNum: m.SimulationNum,
		FromShardId:   m.FromShardId,
		OriginShardId: m.OriginShardId,
		TargetShardId: m.TargetShardId,
		RelatedShards: m.RelatedShards,
		TxHash:        m.TxHash,
		CallIndex:     m.CallIndex,
		Caller:        m.Caller,
		Addr:          m.Addr,
		Input:         m.Input,
		Gas:           m.Gas,
		GasPrice:      m.GasPrice,
		Value:         m.Value,
	}
	bytes, err := json.Marshal(msg)
	if err != nil {
		return nil
	}
	return bytes
}

// CXTRecallSSCRequest is the request of cross-shard Recall with the signature of SSC
type CXTRecallSSCRequest struct {
	SimulationNum int
	OriginShardId uint32
	FromShardId   uint32
	TargetShardId uint32
	RelatedShards RelatedShards
	TxHash        common.Hash
	CallIndex     CallIndex
	Caller        common.Address
	Addr          common.Address
	Input         []byte
	Gas           uint64
	GasPrice      *big.Int `json:"gas_price" gencodec:"required"`
	Value         *big.Int `json:"value" gencodec:"required"`
	BaseBLSSignedMessage
}

func (m *CXTRecallSSCRequest) Bytes() []byte {
	type CXTRecallSSCRequestWithoutSignature struct {
		SimulationNum int
		OriginShardId uint32
		FromShardId   uint32
		TargetShardId uint32
		RelatedShards RelatedShards
		TxHash        common.Hash
		CallIndex     CallIndex
		Caller        common.Address
		Addr          common.Address
		Input         []byte
		Gas           uint64
		GasPrice      *big.Int `json:"gas_price" gencodec:"required"`
		Value         *big.Int `json:"value" gencodec:"required"`
	}
	msg := CXTRecallSSCRequestWithoutSignature{
		SimulationNum: m.SimulationNum,
		OriginShardId: m.OriginShardId,
		FromShardId:   m.FromShardId,
		TargetShardId: m.TargetShardId,
		RelatedShards: m.RelatedShards,
		TxHash:        m.TxHash,
		CallIndex:     m.CallIndex,
		Caller:        m.Caller,
		Addr:          m.Addr,
		Input:         m.Input,
		Gas:           m.Gas,
		GasPrice:      m.GasPrice,
		Value:         m.Value,
	}
	bytes, err := json.Marshal(msg)
	if err != nil {
		return nil
	}
	return bytes
}

// CXTRecallResult is the result of cross-shard recall
type CXTRecallResult struct {
	BaseSSCMessage
	SimulationNum int
	RelatedShards RelatedShards
	Locked        bool // if the recall lock the state on-chain or re-simulate with rwset
	Result        []byte
	LeftOverGas   uint64
	BlockHash     []byte // block hash of simulation
	Err           error
	TreeNode      *CallNodeData
}

func (m *CXTRecallResult) Bytes() []byte {
	type CXTRecallResultWithoutSignature struct {
		SimulationNum int
		RelatedShards RelatedShards
		Locked        bool
		Result        []byte
		LeftOverGas   uint64
		BlockHash     []byte
		Err           error
		TreeNode      *CallNodeData
	}
	msg := CXTRecallResultWithoutSignature{
		SimulationNum: m.SimulationNum,
		RelatedShards: m.RelatedShards,
		Locked:        m.Locked,
		Result:        m.Result,
		LeftOverGas:   m.LeftOverGas,
		BlockHash:     m.BlockHash,
		Err:           m.Err,
		TreeNode:      m.TreeNode,
	}
	bytes, err := json.Marshal(msg)
	if err != nil {
		return nil
	}
	return bytes
}

// CXTRecallSSCResult is the result of cross-shard recall with the signature of SSC
type CXTRecallSSCResult struct {
	SimulationNum int
	RelatedShards RelatedShards
	Locked        bool // if the recall lock the state on-chain or re-simulate with rwset
	Result        []byte
	LeftOverGas   uint64
	BlockHash     []byte // block hash of simulation
	Err           error
	TreeNode      *CallNodeData
	BaseBLSSignedMessage
}

func (m *CXTRecallSSCResult) Bytes() []byte {
	type CXTRecallSSCResultWithoutSignature struct {
		SimulationNum int
		RelatedShards RelatedShards
		Locked        bool
		Result        []byte
		LeftOverGas   uint64
		BlockHash     []byte
		Err           error
		TreeNode      *CallNodeData
	}
	msg := CXTRecallSSCResultWithoutSignature{
		SimulationNum: m.SimulationNum,
		RelatedShards: m.RelatedShards,
		Locked:        m.Locked,
		Result:        m.Result,
		LeftOverGas:   m.LeftOverGas,
		BlockHash:     m.BlockHash,
		Err:           m.Err,
		TreeNode:      m.TreeNode,
	}
	bytes, err := json.Marshal(msg)
	if err != nil {
		return nil
	}
	return bytes
}

type SimulationResultRequest struct {
	SimulationNum int
	TxHash        common.Hash
}

func (m *SimulationResultRequest) Bytes() []byte {
	bytes, err := json.Marshal(m)
	if err != nil {
		return nil
	}
	return bytes
}

// SimulationCommit is the used to notify SSC to commit the simulation
type SimulationCommit struct {
	SimulationNum int // the number of the simulation
	TxHash        common.Hash
	Nonce         uint64
	Sender        common.Address
	RelatedShards RelatedShards
	Commit        bool
	Status        SimulationCommitStatus
	Reason        string
	BaseBLSSignedMessage
	// UseCRSigner tells the handler to submit the resulting SimTx using the
	// CR signer's nonce chain (crSigner, crNonce) instead of the normal
	// simulation signer. This ensures CR(crN) → SimTx(crN+1) ordering.
	UseCRSigner bool `json:"usecrsigner"`
}

func (m *SimulationCommit) Bytes() []byte {
	type SimulationCommitWithoutSignature struct {
		SimulationNum int
		TxHash        common.Hash
		Nonce         uint64
		Sender        common.Address
		RelatedShards RelatedShards
		Commit        bool
		Status        SimulationCommitStatus
		UseCRSigner   bool
	}
	msg := SimulationCommitWithoutSignature{
		SimulationNum: m.SimulationNum,
		TxHash:        m.TxHash,
		Nonce:         m.Nonce,
		Sender:        m.Sender,
		RelatedShards: m.RelatedShards,
		Commit:        m.Commit,
		Status:        m.Status,
		UseCRSigner:   m.UseCRSigner,
	}
	bytes, err := json.Marshal(msg)
	if err != nil {
		return nil
	}
	return bytes
}

// CXTCommitVote is the vote of the cross-shard transaction commit
type CXTCommitVote struct {
	BaseSSCMessage
	TxHash        common.Hash
	SimulationNum int // the number of the simulation
	ShardId       uint32
	OriginShardId uint32
	Type          CXTCommitType
	Reason        CXTCommitReason
	Payload       []byte
}

func (m *CXTCommitVote) Bytes() []byte {
	type CXTCommitVoteWithoutSignature struct {
		TxHash        common.Hash
		SimulationNum int
		ShardId       uint32
		OriginShardId uint32
		Type          CXTCommitType
		Reason        CXTCommitReason
		Payload       []byte
	}
	msg := CXTCommitVoteWithoutSignature{
		TxHash:        m.TxHash,
		SimulationNum: m.SimulationNum,
		ShardId:       m.ShardId,
		OriginShardId: m.OriginShardId,
		Type:          m.Type,
		Reason:        m.Reason,
		Payload:       m.Payload,
	}
	bytes, err := json.Marshal(msg)
	if err != nil {
		return nil
	}
	return bytes
}

type InvalidSimulationType int

const (
	InvalidSerialization InvalidSimulationType = iota
	InvalidSignature
	InvalidExecution
	CXTTimeout
	MaxRetryExceeded
)

type CXTInvalidSimulationPayload struct {
	Type InvalidSimulationType
}

type CXTConflictRWSetPayload struct {
	ConflictCallIndex CallIndex
}

type CXTExecutionFailedPayload struct {
	Err error
}

// CXTCommitSSCVote is the vote of the cross-shard transaction commit with the signatures of SSC or all nodes
type CXTCommitSSCVote struct {
	TxHash        common.Hash
	SimulationNum int // the number of the simulation
	ShardId       uint32
	OriginShardId uint32
	Type          CXTCommitType
	Reason        CXTCommitReason
	Payload       []byte
	BaseBLSSignedMessage
}

func (m *CXTCommitSSCVote) Bytes() []byte {
	type CXTCommitSSCVoteWithoutSignature struct {
		TxHash        common.Hash
		SimulationNum int
		ShardId       uint32
		OriginShardId uint32
		Type          CXTCommitType
		Reason        CXTCommitReason
		Payload       []byte
	}
	msg := CXTCommitSSCVoteWithoutSignature{
		TxHash:        m.TxHash,
		SimulationNum: m.SimulationNum,
		ShardId:       m.ShardId,
		OriginShardId: m.OriginShardId,
		Type:          m.Type,
		Reason:        m.Reason,
		Payload:       m.Payload,
	}
	bytes, err := json.Marshal(msg)
	if err != nil {
		return nil
	}
	return bytes
}

type CXTCommitProof struct {
	BaseSSCMessage
	TxHash        common.Hash
	SimulationNum int
	Type          CXTCommitType
	Reason        CXTCommitReason
	OriginShard   uint32
	RelatedShards RelatedShards
	Votes         []*CXTCommitSSCVote
}

func (m *CXTCommitProof) Bytes() []byte {
	type CXTCommitProofWithoutSignature struct {
		TxHash        common.Hash
		SimulationNum int
		Type          CXTCommitType
		Reason        CXTCommitReason
		OriginShard   uint32
		RelatedShards RelatedShards
		Votes         []*CXTCommitSSCVote
	}
	msg := CXTCommitProofWithoutSignature{
		TxHash:        m.TxHash,
		SimulationNum: m.SimulationNum,
		Type:          m.Type,
		Reason:        m.Reason,
		OriginShard:   m.OriginShard,
		RelatedShards: m.RelatedShards,
		Votes:         m.Votes,
	}
	bytes, err := json.Marshal(msg)
	if err != nil {
		return nil
	}
	return bytes
}

type CXTRecallProof struct {
	BaseSSCMessage
	TxHash        common.Hash
	SimulationNum int // the number of the simulation
	RelatedShards RelatedShards
	Votes         []*CXTCommitSSCVote
	RecallShards  []uint32
}

func (m *CXTRecallProof) Bytes() []byte {
	type CXTRecallProofWithoutSignature struct {
		TxHash        common.Hash
		SimulationNum int
		RelatedShards RelatedShards
		Votes         []*CXTCommitSSCVote
		RecallShards  []uint32
	}
	msg := CXTRecallProofWithoutSignature{
		TxHash:        m.TxHash,
		SimulationNum: m.SimulationNum,
		RelatedShards: m.RelatedShards,
		Votes:         m.Votes,
		RecallShards:  m.RecallShards,
	}
	bytes, err := json.Marshal(msg)
	if err != nil {
		return nil
	}
	return bytes
}

type ConflictCondition int

const (
	Simulate ConflictCondition = iota
	Verify
)

type ReSimulationSignals struct {
	OriginShard uint32
	FromShard   uint32
	Epoch       Epoch // epoch of originShard
	Signals     []*ReSimulationSignal
}

type ReSimulationSignal struct {
	TxHash        common.Hash
	FromShard     uint32
	Epoch         Epoch
	SimulationNum int
	Condition     ConflictCondition
	Ready         bool
	// CRHotWritePatch carries the CR WriteSet for hot keys that were unlocked.
	// When set, the origin shard stores this in CXTSimulationState.CRHotWritePatch
	// so the retry simulation reads the CR's updated values.
	CRHotWritePatch *RWSet `json:"cr_hot_write_patch,omitempty"`
}

func NewCallStack(txHash common.Hash, simulationNum int) *CallStack {
	return &CallStack{
		TxHash:        txHash,
		SimulationNum: simulationNum,
		CallFrames:    make([]*CallFrame, 0),
		PopNum:        0,
		PushNum:       0,
	}
}

type CallFrame struct {
	CallIndex CallIndex // the call index of the call
	PC        int       // the program counter of the call
}

func (c *CallFrame) Next() {
	c.PC++
}

func (c *CallFrame) Reset() {
	c.PC = 0
}

func (c *CallFrame) String() string {
	return c.CallIndex.ToString() + ":" + strconv.Itoa(c.PC)
}

type CallStack struct {
	TxHash        common.Hash
	SimulationNum int
	CallFrames    []*CallFrame // the call frames of the call stack
	PopNum        int
	PushNum       int
}

func (c *CallStack) Top() *CallFrame {
	if len(c.CallFrames) == 0 {
		return nil
	}
	return c.CallFrames[len(c.CallFrames)-1]
}

func (c *CallStack) Push(frame *CallFrame) {
	c.PushNum++
	c.CallFrames = append(c.CallFrames, frame)
}

func (c *CallStack) Pop() *CallFrame {
	c.PopNum++
	if len(c.CallFrames) == 0 {
		// utils.SSCLogger().Info().Str("txHash", c.TxHash.Hex()).Int("simulationNum", c.SimulationNum).Str("callStack", c.String()).Msgf("pop call nil frame, pop=%d, push=%d, length=%d", c.PopNum, c.PushNum, len(c.CallFrames))
		return nil
	}
	frame := c.CallFrames[len(c.CallFrames)-1]
	c.CallFrames = c.CallFrames[:len(c.CallFrames)-1]
	return frame
}

func (c *CallStack) String() string {
	framesStr := make([]string, 0)
	for _, frame := range c.CallFrames {
		framesStr = append(framesStr, frame.String())
	}
	return fmt.Sprintf("CallStack_%s_%d: [%s]", c.TxHash.Hex(), c.SimulationNum, strings.Join(framesStr, "->"))
}

type CXTSimulationState struct {
	Nonce                      uint64
	TxSender                   common.Address
	Epochs                     []Epoch
	CurrentCallFrame           *CallFrame
	CallStack                  *CallStack
	Status                     CXTStatus
	SimulationRequest          *CXTSimulationRequest // the simulation request, only origin member has this
	SimulationResult           *CXTSimulationSSCResult
	SimulationCallStates       map[int]SimulationCallStates
	SimulationNum              int       // the number of the simulation used to identify the recall
	OnChainLockedSimulationNum int       // the simulationNum when the tx was first locked on-chain (0 = not yet locked)
	LockedCallIndex            CallIndex // the locked call index, only the recall after this call index need to be executed
	OriginShardId              uint32
	RelatedShards              RelatedShards
	ReSimulationSignals        map[int]map[uint32]*ReSimulationSignal
	CallForest                 *CallForest
	// CRHotWritePatch stores state values from a CR transaction's WriteSet
	// for hot keys. When set, GetState() checks this patch FIRST before
	// querying stateDB, allowing retry simulations to read post-CR values.
	CRHotWritePatch       *RWSet             `json:"cr_hot_write_patch,omitempty"`
	SimulateCh            chan struct{}      `json:"-"`
	SimulationReentryLock sync.Mutex         `json:"-"`
	Ctx                   context.Context    `json:"-"`
	CtxCancel             context.CancelFunc `json:"-"`
}

type SimulationCallStates []*SimulationCallState

func (s SimulationCallStates) Get(index CallIndex) *SimulationCallState {
	for _, state := range s {
		if state.CallIndex.ToString() == index.ToString() {
			return state
		}
	}
	return nil
}

func (s SimulationCallStates) Add(state *SimulationCallState) SimulationCallStates {
	for i := len(s) - 1; i >= 0; i-- {
		if state.Compare(s[i]) >= 0 {
			s = append(s, nil)
			copy(s[i+1:], s[i:])
			s[i] = state
			return s
		}
	}
	s = append(s, state)
	return s
}

func (s SimulationCallStates) ToString() string {
	str := ""
	for _, state := range s {
		str += state.CallIndex.ToString() + " "
	}
	return str
}

type SimulationCallState struct {
	SimulationNum     int
	BlockNum          uint64
	BlockHash         common.Hash
	CallIndex         CallIndex
	TopRequest        *CXTSimulationRequest
	CallRequest       *CXTCallSSCRequest
	DependentCXTCalls map[string]*DependentCXTCall // callIndex -> dependentCXTCall
	RWSet             *RWSet
	Result            *CXTCallResult
	CallSSCResult     *CXTCallSSCResult
	TopSSCResult      *CXTSimulationSSCResult
	LockedByOtherTx   error
	CallForest        *CallForest
	DB                StateDB       `json:"-"` // the state db of the simulation
	SyncedCh          chan struct{} `json:"-"` // used to notify the state is synced
	Lock              sync.Mutex    `json:"-"`
	StateLock         sync.Mutex    `json:"-"`
	Executed          bool
}

func (s *SimulationCallState) Compare(other *SimulationCallState) int {
	return s.CallIndex.Compare(other.CallIndex)
}

type DependentCXTCall struct {
	CallIndex     CallIndex
	Requests      []*CXTCallRequest
	SignedRequest *CXTCallSSCRequest
	SSCResult     *CXTCallSSCResult
	WaitCh        chan struct{} // 等待 channel，用于通知所有等待的请求
	CloseOnce     sync.Once
}

type ExecutionVerifyState struct {
	Calls map[string]*ExecutionVerifyCall // callIndex -> call
}

type ExecutionVerifyCall struct {
	CallIndex        CallIndex
	Return           []byte
	LeftOverGas      uint64
	CurrentState     *StateSet
	DependentResults []*CXTCallSSCResult
}

type ExecutionVerifyContext struct {
	Simulation       *CXTSimulation
	CallStateMap     map[string]*CXTCallState
	CallFrame        *CallFrame
	CurrentState     *StateSet
	DependentResults []*CXTCallSSCResult
}

type RWSet struct {
	ReadState    *StateSet
	WriteState   *StateSet
	CurrentState *StateSet
}

func NewStateSet() *StateSet {
	return &StateSet{State: make(map[common.Address]map[common.Hash]common.Hash), Balance: make(map[common.Address]*big.Int)}
}

type StateSet struct {
	Balance map[common.Address]*big.Int
	State   map[common.Address]map[common.Hash]common.Hash
}

func (s *StateSet) Equal(other *StateSet) bool {
	if len(s.Balance) != len(other.Balance) {
		return false
	}
	for addr, balance := range s.Balance {
		if otherBalance, ok := other.Balance[addr]; !ok || balance.Cmp(otherBalance) != 0 {
			return false
		}
	}
	if len(s.State) != len(other.State) {
		return false
	}
	for addr, state := range s.State {
		if otherState, ok := other.State[addr]; !ok || len(state) != len(otherState) {
			return false
		}
		for key, value := range state {
			if otherValue, ok := other.State[addr][key]; !ok || value != otherValue {
				return false
			}
		}
	}
	return true
}

type CommitState struct {
	CommitVotes     map[int]map[uint32][]*CXTCommitVote  // simulationNum -> shardId -> votes
	CommitSSCVotes  map[int]map[uint32]*CXTCommitSSCVote // simulationNum -> shardId -> sscVote
	RollbackVotes   map[uint32][]*CXTCommitVote          // shardId -> votes
	RollbackSSCVote *CXTCommitSSCVote
}

type NewEpoch struct {
	Committee *ShardSimulateCommittee `json:"Committee"`
}

type SLTestRequest struct {
	TestFile   []byte `json:"TestFile"`
	Difficulty int    `json:"Difficulty"`
}

type SLTestResult struct {
	Result uint64 `json:"Result"`
	Error  string `json:"Error"`
}

type SelfOpinions struct {
	From     common.Address `json:"From"`
	Opinions []*SLOpinion   `json:"Opinions"`
}

type SLOpinion struct {
	From  common.Address `json:"From"`
	To    common.Address `json:"To"`
	S     int            `json:"S"`
	F     int            `json:"F"`
	Beta  float64        `json:"Beta"`
	Delta float64        `json:"Delta"`
	Omega float64        `json:"Omega"`
}

type RetryTx struct {
	TxHash        common.Hash
	Epochs        []Epoch
	Sender        common.Address
	Nonce         uint64
	GasPrice      uint64
	OriginShardID uint32
	ReadSet       []LockKey
	WriteSet      []LockKey
	RelatedShards RelatedShards
	SimulationNum int
	Condition     ConflictCondition
}

type RetryCommitResp struct {
	TxHash common.Hash
	Locked bool
}
