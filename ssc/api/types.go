package api

import (
	"bytes"
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
	Rollback
)

func (c CXTCommitType) String() string {
	switch c {
	case Commit:
		return "Commit"
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
	Address     common.Address `json:"address"`
	PubKey      string         `json:"pubkey"`
	Endpoint    string         `json:"endpoint"`    // JSON-RPC endpoint for external API
	SSCEndpoint string         `json:"sscendpoint"` // gRPC endpoint for SSC internal RPC
	BLSPubKey   string         `json:"blspubkey"`
}

type ShardSimulateCommitteeConfig struct {
	Committees []*ShardSimulateCommittee `json:"committees" yaml:"committees"`
	Timeout    *TimeoutConfig            `json:"timeout" yaml:"timeout"`
	Chain      *ChainConfig              `json:"chain" yaml:"chain"`
	Reputation *ReputationConfig         `json:"reputation" yaml:"reputation"`
}

// ChainConfig controls the simulation dependency chaining (DAG chaining).
// When enabled, submitting a SimTx triggers chainNextSim which searches the
// retry pool for dependent transactions and sends them DAG upstream-index signals so
// they can simulate in the same block without waiting for block confirmation.
type ChainConfig struct {
	Enabled     bool `json:"enabled" yaml:"enabled"`             // master switch
	MaxPerBlock int  `json:"max_per_block" yaml:"max_per_block"` // max chained SimTx per block
	MaxDepth    int  `json:"max_depth" yaml:"max_depth"`         // max chain depth (0 = unlimited)
}

// ShardSimulateCommittee (SSC) is the committee of the shard simulation
type ShardSimulateCommittee struct {
	ShardID            uint32                 `json:"shardid" yaml:"shardid"`
	Epoch              Epoch                  `json:"epoch" yaml:"epoch"`
	Members            []*Member              `json:"members" yaml:"members"`
	Threshold          int                    `json:"threshold" yaml:"threshold"`
	Validators         []*Member              `json:"validators" yaml:"validators"`
	ValidatorThreshold int                    `json:"validatorthreshold" yaml:"validatorthreshold"`
	Number             int                    `json:"number" yaml:"number"`
	MemberIndex        map[common.Address]int `json:"-" yaml:"-"`
	ValidatorIndex     map[common.Address]int `json:"-" yaml:"-"`
}

type TimeoutConfig struct {
	Sp1                  uint64 `json:"sp1" yaml:"sp1"`                                         // source phase 1, used to notify origin shard to rollback cxt for timeout
	PoolTimeout          uint64 `json:"pool_timeout" yaml:"pool_timeout"`                       // the timeout to remove cxt from pool
	MaxOnChainRetries    uint64 `json:"max_on_chain_retries" yaml:"max_on_chain_retries"`       // max retries for on-chain verification
	MaxRetriesTotal      uint64 `json:"max_retries_total" yaml:"max_retries_total"`             // max total retries before giving up
	ForceSimulation      bool   `json:"force_simulation" yaml:"force_simulation"`               // continue execution on lock conflict, get full RWSet
	EnableLockOnConflict bool   `json:"enable_lock_on_conflict" yaml:"enable_lock_on_conflict"` // lock conflict callState via lockStateWithExecution before CallForRetry
	MaxChainDepth        int    `json:"max_chain_depth" yaml:"max_chain_depth"`                 // DSN-50: 链下 DAG 链深上限（<=0 时使用默认值 5）
	// EnableDAG 控制整个链下 off-chain DAG / PatchPool 子系统是否启用。
	// 默认 false（消融/调试用）：禁用后 AddNode/MarkReady/consume/release 全部空转，
	// isPatchFinalized 恒为 true（所有锁都视为不可 wound），conflict 不再被 patch 救援。
	// 置 true 时恢复原有 DAG 链式救援行为。实验/消融开关。
	EnableDAG bool `json:"enable_dag" yaml:"enable_dag"`
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
	// UpstreamTxList — DSN-54：leader 已分配的上游引用，成员据此从 simDAGPatches 反查上游写集。
	UpstreamTxList []TxSimKey
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
	SimulationNum  int
	TxHash         common.Hash
	Nonce          uint64
	Sender         common.Address
	ShardId        uint32
	OriginShardId  uint32
	RelatedShards  RelatedShards
	CallStates     []*CXTCallState // all cross-shard call of cxt related to this shard
	UpstreamTxList []TxSimKey      `json:"upstream_tx_list,omitempty"` // all upstream SimTxs (DAG)
	BaseBLSSignedMessage
}

func (m *CXTSimulation) Bytes() []byte {
	type CXTSimulationWithoutSignature struct {
		SimulationNum  int
		TxHash         common.Hash
		Nonce          uint64
		Sender         common.Address
		ShardId        uint32
		OriginShardId  uint32
		RelatedShards  RelatedShards
		CallStates     []*CXTCallState
		UpstreamTxList []TxSimKey
	}
	msg := CXTSimulationWithoutSignature{
		SimulationNum:  m.SimulationNum,
		TxHash:         m.TxHash,
		Nonce:          m.Nonce,
		Sender:         m.Sender,
		ShardId:        m.ShardId,
		OriginShardId:  m.OriginShardId,
		RelatedShards:  m.RelatedShards,
		CallStates:     m.CallStates,
		UpstreamTxList: m.UpstreamTxList,
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
	}
	msg := SimulationCommitWithoutSignature{
		SimulationNum: m.SimulationNum,
		TxHash:        m.TxHash,
		Nonce:         m.Nonce,
		Sender:        m.Sender,
		RelatedShards: m.RelatedShards,
		Commit:        m.Commit,
		Status:        m.Status,
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
	ReleaseOnly   bool // DSN-52: 只释放锁、不 close 交易（冲突让位后回重试池）
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
		ReleaseOnly   bool
	}
	msg := CXTCommitProofWithoutSignature{
		TxHash:        m.TxHash,
		SimulationNum: m.SimulationNum,
		Type:          m.Type,
		Reason:        m.Reason,
		OriginShard:   m.OriginShard,
		RelatedShards: m.RelatedShards,
		Votes:         m.Votes,
		ReleaseOnly:   m.ReleaseOnly,
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

type RetrySignals struct {
	OriginShard uint32
	FromShard   uint32
	Epoch       Epoch // epoch of originShard
	Signals     []*RetrySignal
}

type RetrySignal struct {
	TxHash        common.Hash
	FromShard     uint32
	Epoch         Epoch
	SimulationNum int
	Condition     ConflictCondition
	Ready         bool
	// UpstreamTxList is the DAG upstream edge set (the source of truth). When a
	// tx is rescued as a DAG chain member, this must travel with the retry signal
	// so the origin shard persists the upstream node into its offChainDAG and the
	// later CommitSimulation can build a SimTx that carries a non-empty
	// UpstreamTxList → Verify judges chain → skips lock conflict.
	// (DSN-56：不再携带扁平 merged ChainPatch；上游写集从 offChainDAG/simDAGPatches 反查。)
	UpstreamTxList []TxSimKey `json:"upstream_tx_list,omitempty"`
}

// ChainNode represents a node in the simulation dependency chain.
// Each node stores its own WriteSet patch, along with references to all upstream nodes (DAG).
type ChainNode struct {
	TxHash         common.Hash
	SimulationNum  int
	Patch          *RWSet     // 自己的 WriteSet
	UpstreamTxList []TxSimKey // 所有上游 SimTx（空=根节点，DAG 支持多上游）
}

// TxSimKey identifies a specific simulation of a transaction.
type TxSimKey struct {
	TxHash        common.Hash
	SimulationNum int
}

// SimPatchNode — DSN-54 模拟期链下 DAG patch 子图的一个节点。
// Writes 用 RWSet 承载本节点写集（只看 WriteState）；Upstream 为上游边（供传递依赖反查）。
type SimPatchNode struct {
	TxSim    TxSimKey
	Upstream []TxSimKey
	Writes   *RWSet
}

// SimPatchSubgraph — leader 从 offChainDAG 为该交易某一轮模拟构建的上游 patch 子图，
// 广播给成员后，成员按 (txHash, simulationNum) 存入 simDAGPatches。
type SimPatchSubgraph struct {
	TxHash        common.Hash
	SimulationNum int
	Nodes         []*SimPatchNode
}

// StoreSimDAGPatchRequest — 广播 RPC 的载荷。
type StoreSimDAGPatchRequest struct {
	Subgraph *SimPatchSubgraph
}

// Priority defines a global ordering for retryTx lock contention.
// Lower value = higher priority (earlier nonce / lower shardId / smaller txHash wins).
type Priority struct {
	Nonce         uint64      `json:"nonce"`
	OriginShardID uint32      `json:"origin_shard_id"`
	TxHash        common.Hash `json:"tx_hash"` // 最终 tiebreaker，确保所有交易有确定全序
}

// Less returns true if p has higher priority than other.
// Priority order: Nonce (smaller = higher priority) > OriginShardID > TxHash
func (p Priority) Less(other Priority) bool {
	if p.Nonce != other.Nonce {
		return p.Nonce < other.Nonce
	}
	if p.OriginShardID != other.OriginShardID {
		return p.OriginShardID < other.OriginShardID
	}
	// Final tiebreaker: compare TxHash as big-endian bytes
	return bytes.Compare(p.TxHash.Bytes(), other.TxHash.Bytes()) < 0
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

// TxState 交易公共状态，由 sscService 持有。
// 所有模块（Verifier/Committer/CommitVote/Retry）通过 sscService 的方法访问，
// 不直接依赖 Simulator。
type TxState struct {
	Mu            sync.Mutex // per-tx 锁，替代全局 stateLock
	Closed        bool       // 替代 finishedTxs[txHash]
	TxHash        common.Hash
	Nonce         uint64
	TxSender      common.Address
	Epochs        []Epoch
	Status        CXTStatus
	SimulationNum int
	OriginShardId uint32
	RelatedShards RelatedShards
	// RetrySignals maps simulationNum -> shardId -> signal
	RetrySignals map[int]map[uint32]*RetrySignal
	Ctx          context.Context    `json:"-"`
	CtxCancel    context.CancelFunc `json:"-"`
}

// SimulationState 模拟专属状态，由 Simulator 持有。
// 只有 Simulator 读写，其他模块不直接访问。
type SimulationState struct {
	CurrentCallFrame      *CallFrame
	CallStack             *CallStack
	SimulationRequest     *CXTSimulationRequest
	SimulationResult      *CXTSimulationSSCResult
	SimulationCallStates  map[int]SimulationCallStates
	LockedCallIndex       CallIndex
	CallForest            *CallForest
	SimulateCh            chan struct{} `json:"-"`
	SimulationReentryLock sync.Mutex    `json:"-"`
}

type CXTSimulationState struct {
	Nonce                 uint64
	TxSender              common.Address
	Epochs                []Epoch
	CurrentCallFrame      *CallFrame
	CallStack             *CallStack
	Status                CXTStatus
	SimulationRequest     *CXTSimulationRequest // the simulation request, only origin member has this
	SimulationResult      *CXTSimulationSSCResult
	SimulationCallStates  map[int]SimulationCallStates
	SimulationNum         int       // the number of the simulation used to identify the recall
	LockedCallIndex       CallIndex // the locked call index, only the recall after this call index need to be executed
	OriginShardId         uint32
	RelatedShards         RelatedShards
	RetrySignals          map[int]map[uint32]*RetrySignal
	CallForest            *CallForest
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
	LockedKeys        []LockKey `json:"-"` // keys that had lock conflicts, used by ForceSimulation
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
	Simulation   *CXTSimulation
	CallStateMap map[string]*CXTCallState
	// SubCtx 内嵌子验证上下文映射，key: CallIndex → *ssc.callVerifyContext
	// 并行验证时每 CallState 独立，Cleanup 时随根自动释放
	SubCtx           sync.Map
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

// RetryStatus represents the lifecycle state of a retryTx in the retry pool.
type RetryStatus int

const (
	RetryActive   RetryStatus = iota // In subscriber index, eligible for matching
	RetryConsumed                    // Patch consumed, waiting for re-sim result
	RetryPassive                     // Skipped by active scanning, waiting for origin signal
	RetryWounded                     // TLV lock stolen, will be restored on next block
)

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
	Status        RetryStatus // 重试交易的生命周期状态
}

type RetryCommitResp struct {
	TxHash              common.Hash
	Locked              bool
	OnChainLockConflict bool // 锁定失败的原因是链上锁冲突（用于 v2 被动池决策）
}

// LockLayer 标明一条等待边来自哪一层锁（DSN-53 §3.3）。
type LockLayer uint8

const (
	LockLayerOnChain LockLayer = iota // 链上 SLM（主目标，持久）
	LockLayerTLV                      // 链下 TLV（仅当持有者已 Finalized/将上链才构成持久边）
)

func (l LockLayer) String() string {
	switch l {
	case LockLayerOnChain:
		return "onchain"
	case LockLayerTLV:
		return "tlv"
	default:
		return "unknown"
	}
}

// DeadlockProbe 是跨分片优先级感知反向 CMH 探针。
// 每个转发 hop 由发送分片 SSC 委员会阈值聚合 BLS 签名背书。
// Bytes() 对除 BaseBLSSignedMessage 外的全部字段做确定性序列化，供 BLS 签名/验证。
type DeadlockProbe struct {
	Init     common.Hash   // 发起者（探针回到 Init 即成环）
	Sender   common.Hash   // 当前发送者（上一跳被卡的交易）
	Current  common.Hash   // 当前接收者（要检查它是否在等别人）
	Path     []common.Hash // 已访问路径（用于选 victim）
	Layer    LockLayer     // 上一跳 waitEdge 来自哪一层
	WaitKey  LockKey       // 上一跳 waitEdge 的冲突 key（可选）
	Epoch    uint64        // 当前所在 epoch（防跨 epoch 重放）
	BlockNum uint64        // 触发时的块号（防 stale）
	Nonce    uint64        // 发起者唯一 nonce（去重/防重放）
	Shard    uint32        // 当前所在分片
	// Proof 是 A 方案（VictimTx 证明）：探针在转发过程中累积的、沿途各跳已
	// 门限签名的探针消息链。用于最后一跳判环后，作为“这笔确实被 CMH 判成链上
	// 死锁”的证据带进 VictimTx。
	// 注意：Proof 不进入 Bytes() 的签名范围（各跳自身仍各自签名），因此不影响
	// 既有探针验签；它只是随探针携带的只读证据累积。存储的每个 hop 均不带各自
	// 的 Proof（扁平化，避免 O(n^2) 递归膨胀）。
	Proof []*DeadlockProbe `json:"proof,omitempty"`
	BaseBLSSignedMessage
}

func (m *DeadlockProbe) Bytes() []byte {
	type DeadlockProbeWithoutSignature struct {
		Init     common.Hash
		Sender   common.Hash
		Current  common.Hash
		Path     []common.Hash
		Layer    LockLayer
		WaitKey  LockKey
		Epoch    uint64
		BlockNum uint64
		Nonce    uint64
		Shard    uint32
	}
	msg := DeadlockProbeWithoutSignature{
		Init:     m.Init,
		Sender:   m.Sender,
		Current:  m.Current,
		Path:     m.Path,
		Layer:    m.Layer,
		WaitKey:  m.WaitKey,
		Epoch:    m.Epoch,
		BlockNum: m.BlockNum,
		Nonce:    m.Nonce,
		Shard:    m.Shard,
	}
	b, err := json.Marshal(msg)
	if err != nil {
		return nil
	}
	return b
}

// DeadlockProbeAck 是探针的受理回执（可选，用于监控/确认）。
type DeadlockProbeAck struct {
	Init     common.Hash
	Current  common.Hash
	Shard    uint32
	Accepted bool
}

func (m *DeadlockProbeAck) Bytes() []byte {
	b, err := json.Marshal(struct {
		Init     common.Hash
		Current  common.Hash
		Shard    uint32
		Accepted bool
	}{
		Init:     m.Init,
		Current:  m.Current,
		Shard:    m.Shard,
		Accepted: m.Accepted,
	})
	if err != nil {
		return nil
	}
	return b
}

// VictimTx 是 CMH 判环后、由“最后一跳发现环的 shard”的 leader 构造并打进本分片
// 的内部控制交易（DSN-53 VictimTx 方案）。它不碰状态、不与任何 SimTx 抢锁，
// 只是让本分片每个 validator 在块内看到后，各自向 leader 投一张 Rollback vote，
// 从而凑够委员会门限、沿既有 rollback 路径终局回滚 victim。
//
// Proof 携带“判环这一路上各跳的签名探测消息链”（A 方案），是 validator 验证
// “victim 确实被 CMH 判成链上死锁”的证明。
type VictimTx struct {
	TxHash        common.Hash      // victim 交易 hash
	Nonce         uint64           // victim nonce（用于同桶内按 priority 排序）
	OriginShardId uint32           // victim 的 origin shard
	RelatedShards []uint32         // victim 的 related shards
	Epochs        []Epoch          // victim 各分片 epoch（对齐 CXTCommitVote 语义）
	SimulationNum int              // victim 当前 simulation 代数
	Proof         []*DeadlockProbe // 跨分片探测证据链（沿途各跳签名探针）
}
