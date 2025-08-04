package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/harmony-one/harmony/core/types"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"
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

const (
	OK SimulationCommitStatus = iota
	ExecutionFailed
)

func (s SimulationCommitStatus) String() string {
	switch s {
	case OK:
		return "OK"
	case ExecutionFailed:
		return "ExecutionFailed"
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
	Reason_SUCCESS CXTCommitReason = iota
	Reason_ExecutionFailed
	Reason_InvalidSimulation
	Reason_ConflictRWSet_FailedLock
	Reason_ConflictRWSet_Recall
)

func (c CXTCommitReason) String() string {
	switch c {
	case Reason_SUCCESS:
		return "SUCCESS"
	case Reason_ExecutionFailed:
		return "ExecutionFailed"
	case Reason_InvalidSimulation:
		return "InvalidSimulation"
	case Reason_ConflictRWSet_FailedLock:
		return "ConflictRWSet_FailedLock"
	case Reason_ConflictRWSet_Recall:
		return "ConflictRWSet_Recall"
	default:
		return "Unknown"
	}
}

type Epoch uint64

// CallIndex is the index of the cross-shard call
type CallIndex []int

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

type Config struct {
	CallTimeout              time.Duration
	CXTTimeout               time.Duration
	SimulationCommitGasLimit uint64
	SimulationCommitGasPrice *big.Int
	LockExecutionOnce        bool
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
	Stake     *big.Int `json:"stake" gencodec:"required"`
	Endpoint  string
	BLSPubKey []byte
}

type Validator struct {
	Address common.Address
	PubKey  []byte
}

type ShardSimulateCommitteeConfig struct {
	Committees []*ShardSimulateCommittee
}

// ShardSimulateCommittee (SSC) is the committee of the shard simulation
type ShardSimulateCommittee struct {
	ShardID   uint32
	Epoch     Epoch
	Members   []*Member
	Number    int
	Threshold int
}

type MessageToSign interface {
	// Bytes return the bytes to calculate hash
	Bytes() []byte
}

type SSCMessage interface {
	MessageToSign
	GetSenderAddr() common.Address
	GetSignature() []byte
}

type BaseSSCMessage struct {
	Signature  []byte
	SenderAddr common.Address
}

func (s *BaseSSCMessage) GetSenderAddr() common.Address {
	return s.SenderAddr
}

func (s *BaseSSCMessage) GetSignature() []byte {
	return s.Signature
}

func (s *BaseSSCMessage) Bytes() []byte {
	return make([]byte, 0)
}

type BLSSignedMessage interface {
	MessageToSign
	GetShardId() uint32
	GetSignatures() []byte
	GetBLSBitMap() []byte
}

type BaseBLSSignedMessage struct {
	ShardId    uint32
	Signatures []byte
	BLSBitMap  []byte
}

func (m *BaseBLSSignedMessage) GetShardId() uint32 {
	return m.ShardId
}

func (m *BaseBLSSignedMessage) GetSignatures() []byte {
	return m.Signatures
}

func (m *BaseBLSSignedMessage) GetBLSBitMap() []byte {
	return m.BLSBitMap
}

// CXTSimulationRequest is the request of the cross-shard transaction simulation
type CXTSimulationRequest struct {
	TxHash        []byte
	SimulationNum int
	Author        *common.Address
	BlockHash     []byte
	Tx            *types.Transaction
	From          common.Address
	GasPool       uint64
}

// CXTSimulationResult is the result of the cross-shard transaction simulation
type CXTSimulationResult struct {
	*BaseSSCMessage
	RelatedShards []uint32
	Result        []byte
	Receipt       *types.Receipt
	UsedGas       uint64
	Err           string
}

func (m *CXTSimulationResult) Bytes() []byte {
	temp := m.BaseSSCMessage
	m.BaseSSCMessage = nil
	bytes, err := json.Marshal(m)
	m.BaseSSCMessage = temp
	if err != nil {
		return nil
	}
	return bytes
}

// CXTSimulationSSCResult is CXTSimulationResult with the signature of SSC
type CXTSimulationSSCResult struct {
	RelatedShards []uint32
	Result        []byte
	Receipt       *types.Receipt
	UsedGas       uint64
	Err           string
	*BaseBLSSignedMessage
}

func (m *CXTSimulationSSCResult) Bytes() []byte {
	temp := m.BaseBLSSignedMessage
	m.BaseBLSSignedMessage = nil
	bytes, err := json.Marshal(m)
	m.BaseBLSSignedMessage = temp
	if err != nil {
		return nil
	}
	return bytes
}

// CXTReSimulationRequest is the request of the cross-shard transaction simulation
type CXTReSimulationRequest struct {
	*BaseSSCMessage
	SimulationNum int
	TxHash        []byte
}

func (m *CXTReSimulationRequest) Bytes() []byte {
	temp := m.BaseSSCMessage
	m.BaseSSCMessage = nil
	bytes, err := json.Marshal(m)
	m.BaseSSCMessage = temp
	if err != nil {
		return nil
	}
	return bytes
}

// CXTReSimulationSSCResult is CXTSimulationResult with the signature of SSC
type CXTReSimulationSSCResult struct {
	SimulationNum int
	RelatedShards []uint32
	Result        []byte
	Receipt       *types.Receipt
	UsedGas       uint64
	Err           error
	*BaseBLSSignedMessage
}

func (m *CXTReSimulationSSCResult) Bytes() []byte {
	temp := m.BaseBLSSignedMessage
	m.BaseBLSSignedMessage = nil
	bytes, err := json.Marshal(m)
	m.BaseBLSSignedMessage = temp
	if err != nil {
		return nil
	}
	return bytes
}

// CXTSimulation is the simulation of the cross-shard transaction
type CXTSimulation struct {
	SimulationNum int
	TxHash        []byte
	ShardId       uint32
	OriginShardId uint32
	RelatedShards []uint32
	CallStates    []*CXTCallState // all cross-shard call of cxt related to this shard
	*BaseBLSSignedMessage
}

func (m *CXTSimulation) Bytes() []byte {
	temp := m.BaseBLSSignedMessage
	m.BaseBLSSignedMessage = nil
	bytes, err := json.Marshal(m)
	m.BaseBLSSignedMessage = temp
	if err != nil {
		return nil
	}
	return bytes
}

func (m *CXTSimulation) String() string {
	callStatesStr := ""
	for _, callState := range m.CallStates {
		dependentResultsStr := ""
		for _, result := range callState.DependentResults {
			dependentResultsStr += fmt.Sprintf("(%s:%v) ", result.CallIndex.ToString(), result.Result[len(result.Result)-1])
		}
		callStatesStr += fmt.Sprintf("%s: [%s]", callState.CallIndex.ToString(), dependentResultsStr)
		dependentResultsStr += "|"
	}
	return fmt.Sprintf("simulation_%s_%d: {%s}", common.Bytes2Hex(m.TxHash), m.SimulationNum, callStatesStr)
}

// CXTReSimulation is the simulation of the cross-shard transaction
type CXTReSimulation struct {
	SimulationNum int
	TxHash        []byte
	ShardId       uint32
	OriginShardId uint32
	RelatedShards []uint32
	RecallStates  []*CXTRecallState // all cross-shard call of cxt related to this shard
	*BaseBLSSignedMessage
}

func (m *CXTReSimulation) Bytes() []byte {
	temp := m.BaseBLSSignedMessage
	m.BaseBLSSignedMessage = nil
	bytes, err := json.Marshal(m)
	m.BaseBLSSignedMessage = temp
	if err != nil {
		return nil
	}
	return bytes
}

// CXTCallState is the state of cross-shard call
type CXTCallState struct {
	CallIndex        CallIndex
	TopRequest       *CXTSimulationRequest // the start request of the simulation, nil if it's not the top request
	CallRequest      *CXTCallSSCRequest    // the request of the cross-shard call, nil if it's the top request
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
	*BaseSSCMessage
	OriginShardId uint32
	FromShardId   uint32
	TargetShardId uint32
	SimulationNum int
	RelatedShards []uint32
	TxHash        []byte
	CallIndex     CallIndex
	Caller        common.Address
	Addr          common.Address
	Input         []byte
	Gas           uint64
	GasPrice      *big.Int `json:"gas_price" gencodec:"required"`
	Value         *big.Int `json:"value" gencodec:"required"`
}

func (m *CXTCallRequest) Bytes() []byte {
	temp := m.BaseSSCMessage
	m.BaseSSCMessage = nil
	bytes, err := json.Marshal(m)
	m.BaseSSCMessage = temp
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
	RelatedShards []uint32
	TxHash        []byte
	CallIndex     CallIndex
	Caller        common.Address
	Addr          common.Address
	Input         []byte
	Gas           uint64
	GasPrice      *big.Int `json:"gas_price" gencodec:"required"`
	Value         *big.Int `json:"value" gencodec:"required"`
	*BaseBLSSignedMessage

	// the SSC set the block hash of the simulation, used to sync the state, note: it's not included in the signature
	BlockHash []byte
}

func (m *CXTCallSSCRequest) Bytes() []byte {
	temp := m.BaseBLSSignedMessage
	tempBlockHash := m.BlockHash
	m.BaseBLSSignedMessage = nil
	m.BlockHash = nil
	bytes, err := json.Marshal(m)
	m.BaseBLSSignedMessage = temp
	m.BlockHash = tempBlockHash
	if err != nil {
		return nil
	}
	return bytes
}

// CXTCallResult is the result of cross-shard call
type CXTCallResult struct {
	CallIndex     CallIndex
	RelatedShards []uint32
	Result        []byte
	LeftOverGas   uint64
	BlockHash     []byte // the state of block hash of simulation
	Err           string
	*BaseSSCMessage
}

func (m *CXTCallResult) Bytes() []byte {
	temp := m.BaseSSCMessage
	m.BaseSSCMessage = nil
	bytes, err := json.Marshal(m)
	m.BaseSSCMessage = temp
	if err != nil {
		return nil
	}
	return bytes
}

// CXTCallSSCResult is the result of cross-shard call with the signature of SSC
type CXTCallSSCResult struct {
	CallIndex     CallIndex
	RelatedShards []uint32
	Result        []byte
	LeftOverGas   uint64
	BlockHash     []byte // the state of block hash of simulation
	Err           string
	*BaseBLSSignedMessage
}

func (m *CXTCallSSCResult) Bytes() []byte {
	temp := m.BaseBLSSignedMessage
	m.BaseBLSSignedMessage = nil
	bytes, err := json.Marshal(m)
	m.BaseBLSSignedMessage = temp
	if err != nil {
		return nil
	}
	return bytes
}

// CXTRecallRequest is the request of cross-shard Recall
type CXTRecallRequest struct {
	*BaseSSCMessage
	SimulationNum int
	FromShardId   uint32
	OriginShardId uint32
	TargetShardId uint32
	RelatedShards []uint32
	TxHash        []byte
	CallIndex     CallIndex
	Caller        common.Address
	Addr          common.Address
	Input         []byte
	Gas           uint64
	GasPrice      *big.Int `json:"gas_price" gencodec:"required"`
	Value         *big.Int `json:"value" gencodec:"required"`
}

func (m *CXTRecallRequest) Bytes() []byte {
	temp := m.BaseSSCMessage
	m.BaseSSCMessage = nil
	bytes, err := json.Marshal(m)
	m.BaseSSCMessage = temp
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
	RelatedShards []uint32
	TxHash        []byte
	CallIndex     CallIndex
	Caller        common.Address
	Addr          common.Address
	Input         []byte
	Gas           uint64
	GasPrice      *big.Int `json:"gas_price" gencodec:"required"`
	Value         *big.Int `json:"value" gencodec:"required"`
	*BaseBLSSignedMessage
}

func (m *CXTRecallSSCRequest) Bytes() []byte {
	temp := m.BaseBLSSignedMessage
	m.BaseBLSSignedMessage = nil
	bytes, err := json.Marshal(m)
	m.BaseBLSSignedMessage = temp
	if err != nil {
		return nil
	}
	return bytes
}

// CXTRecallResult is the result of cross-shard recall
type CXTRecallResult struct {
	*BaseSSCMessage
	SimulationNum int
	RelatedShards []uint32
	Locked        bool // if the recall lock the state on-chain or re-simulate with rwset
	Result        []byte
	LeftOverGas   uint64
	BlockHash     []byte // block hash of simulation
	Err           error
}

func (m *CXTRecallResult) Bytes() []byte {
	temp := m.BaseSSCMessage
	m.BaseSSCMessage = nil
	bytes, err := json.Marshal(m)
	m.BaseSSCMessage = temp
	if err != nil {
		return nil
	}
	return bytes
}

// CXTRecallSSCResult is the result of cross-shard recall with the signature of SSC
type CXTRecallSSCResult struct {
	SimulationNum int
	RelatedShards []uint32
	Locked        bool // if the recall lock the state on-chain or re-simulate with rwset
	Result        []byte
	LeftOverGas   uint64
	BlockHash     []byte // block hash of simulation
	Err           error
	*BaseBLSSignedMessage
}

func (m *CXTRecallSSCResult) Bytes() []byte {
	temp := m.BaseBLSSignedMessage
	m.BaseBLSSignedMessage = nil
	bytes, err := json.Marshal(m)
	m.BaseBLSSignedMessage = temp
	if err != nil {
		return nil
	}
	return bytes
}

type SimulationResultRequest struct {
	SimulationNum int
	TxHash        []byte
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
	TxHash        []byte
	RelatedShards []uint32
	Commit        bool
	Status        SimulationCommitStatus
	*BaseBLSSignedMessage
}

func (m *SimulationCommit) Bytes() []byte {
	temp := m.BaseBLSSignedMessage
	m.BaseBLSSignedMessage = nil
	bytes, err := json.Marshal(m)
	m.BaseBLSSignedMessage = temp
	if err != nil {
		return nil
	}
	return bytes
}

// CXTCommitVote is the vote of the cross-shard transaction commit
type CXTCommitVote struct {
	*BaseSSCMessage
	TxHash        []byte
	SimulationNum int // the number of the simulation
	ShardId       uint32
	OriginShardId uint32
	Type          CXTCommitType
	Reason        CXTCommitReason
	Payload       []byte
}

func (m *CXTCommitVote) Bytes() []byte {
	temp := m.BaseSSCMessage
	m.BaseSSCMessage = nil
	bytes, err := json.Marshal(m)
	m.BaseSSCMessage = temp
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
	TxHash        []byte
	SimulationNum int // the number of the simulation
	ShardId       uint32
	OriginShardId uint32
	Type          CXTCommitType
	Reason        CXTCommitReason
	Payload       []byte
	*BaseBLSSignedMessage
}

func (m *CXTCommitSSCVote) Bytes() []byte {
	temp := m.BaseBLSSignedMessage
	m.BaseBLSSignedMessage = nil
	bytes, err := json.Marshal(m)
	m.BaseBLSSignedMessage = temp
	if err != nil {
		return nil
	}
	return bytes
}

type CXTCommitProof struct {
	*BaseSSCMessage
	TxHash        []byte
	SimulationNum int
	Type          CXTCommitType
	Reason        CXTCommitReason
	OriginShard   uint32
	RelatedShards []uint32
	Votes         []*CXTCommitSSCVote
}

func (m *CXTCommitProof) Bytes() []byte {
	temp := m.BaseSSCMessage
	m.BaseSSCMessage = nil
	bytes, err := json.Marshal(m)
	m.BaseSSCMessage = temp
	if err != nil {
		return nil
	}
	return bytes
}

type CXTRecallProof struct {
	*BaseSSCMessage
	TxHash        []byte
	SimulationNum int // the number of the simulation
	RelatedShards []uint32
	Votes         []*CXTCommitSSCVote
	RecallShards  []uint32
}

func (m *CXTRecallProof) Bytes() []byte {
	temp := m.BaseSSCMessage
	m.BaseSSCMessage = nil
	bytes, err := json.Marshal(m)
	m.BaseSSCMessage = temp
	if err != nil {
		return nil
	}
	return bytes
}

func NewCallStack() *CallStack {
	return &CallStack{
		CallFrames: make([]*CallFrame, 0),
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
	CallFrames []*CallFrame // the call frames of the call stack
}

func (c *CallStack) Push(frame *CallFrame) {
	c.CallFrames = append(c.CallFrames, frame)
}

func (c *CallStack) Pop() *CallFrame {
	if len(c.CallFrames) == 0 {
		return nil
	}
	frame := c.CallFrames[len(c.CallFrames)-1]
	c.CallFrames = c.CallFrames[:len(c.CallFrames)-1]
	return frame
}

type CXTSimulationState struct {
	CurrentCallFrame     *CallFrame
	CallStack            *CallStack
	SimulationRequest    *CXTSimulationRequest // the simulation request, only origin member has this
	SimulationResult     *CXTSimulationSSCResult
	SimulationCallStates map[int]SimulationCallStates
	SimulationNum        int       // the number of the simulation used to identify the recall
	LockedCallIndex      CallIndex // the locked call index, only the recall after this call index need to be executed
	OriginShardId        uint32
	RelatedShards        []uint32
	RelatedShardMap      map[uint32]struct{}
	TimeoutCtx           context.Context
	TimeoutCancel        context.CancelFunc
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
	BlockHash         common.Hash
	CallIndex         CallIndex
	TopRequest        *CXTSimulationRequest
	CallRequest       *CXTCallSSCRequest
	DependentCXTCalls map[string]*DependentCXTCall // callIndex -> dependentCXTCall
	RWSet             *RWSet
	Result            *CXTCallResult
	CallSSCResult     *CXTCallSSCResult
	TopSSCResult      *CXTSimulationSSCResult

	DB       StateDB       // the state db of the simulation
	SyncedCh chan struct{} // used to notify the state is synced
	Lock     sync.Mutex
	Executed bool
}

func (s *SimulationCallState) Compare(other *SimulationCallState) int {
	return s.CallIndex.Compare(other.CallIndex)
}

type DependentCXTCall struct {
	CallIndex     CallIndex
	Requests      []*CXTCallRequest
	SignedRequest *CXTCallSSCRequest
	Executed      bool
	SSCResult     *CXTCallSSCResult
	WaitingChs    []chan *CXTCallSSCResult
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

type SimulationRecallStates []*SimulationRecallState

func (s SimulationRecallStates) Get(index CallIndex) *SimulationRecallState {
	for _, state := range s {
		if state.CallIndex.ToString() == index.ToString() {
			return state
		}
	}
	return nil
}

func (s SimulationRecallStates) Add(state *SimulationRecallState) {
	for i := len(s) - 1; i >= 0; i-- {
		if state.CallIndex.Compare(s[i].CallIndex) >= 0 {
			s = append(s, nil)
			copy(s[i+1:], s[i:])
			s[i] = state
			return
		}
	}
}

type SimulationRecallState struct {
	CallIndex        CallIndex
	Locked           bool // if the recall lock the state on-chain or re-simulate with rwset
	Requests         []*CXTRecallRequest
	SSCRequest       *CXTRecallSSCRequest
	DependentResults []*CXTRecallSSCResult
	RWSet            *RWSet
	Result           *CXTRecallResult    // the result of simulation self performed
	SSCResult        *CXTRecallSSCResult // the result of simulation from SSC
	Executed         bool
	WaitingChs       []chan *CXTRecallSSCResult
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
	CommitVotes    map[int]map[uint32][]*CXTCommitVote  // simulationNum -> shardId -> votes
	CommitSSCVotes map[int]map[uint32]*CXTCommitSSCVote // simulationNum -> shardId -> sscVote
}
