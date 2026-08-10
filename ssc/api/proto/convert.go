package sscpb

import (
	"encoding/json"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/harmony-one/harmony/core/types"
	api "github.com/harmony-one/harmony/ssc/api"
)

// ============================================================================
// Scalar helpers (exported for use by rpc/ssc_grpc.go)
// ============================================================================

func HashFromProto(p *Hash) common.Hash {
	return hashFromProto(p)
}

func AddrFromProto(p *Address) common.Address {
	return addrFromProto(p)
}

func hashFromProto(p *Hash) common.Hash {
	if p == nil {
		return common.Hash{}
	}
	return common.BytesToHash(p.GetVal())
}

func hashToProto(h common.Hash) *Hash {
	return &Hash{Val: h.Bytes()}
}

func addrFromProto(p *Address) common.Address {
	if p == nil {
		return common.Address{}
	}
	return common.BytesToAddress(p.GetVal())
}

func addrToProto(a common.Address) *Address {
	return &Address{Val: a.Bytes()}
}

func addrPtrFromProto(p []byte) *common.Address {
	if len(p) == 0 {
		return nil
	}
	addr := common.BytesToAddress(p)
	return &addr
}

func addrPtrToProto(a *common.Address) []byte {
	if a == nil {
		return nil
	}
	return a.Bytes()
}

func bigIntFromProto(p *BigInt) *big.Int {
	if p == nil || len(p.GetVal()) == 0 {
		return new(big.Int)
	}
	return new(big.Int).SetBytes(p.GetVal())
}

func bigIntToProto(b *big.Int) *BigInt {
	if b == nil {
		return &BigInt{}
	}
	return &BigInt{Val: b.Bytes()}
}

func callIndexFromProto(p []int32) api.CallIndex {
	if p == nil {
		return nil
	}
	out := make(api.CallIndex, len(p))
	for i, v := range p {
		out[i] = int(v)
	}
	return out
}

func callIndexToProto(c api.CallIndex) []int32 {
	if c == nil {
		return nil
	}
	out := make([]int32, len(c))
	for i, v := range c {
		out[i] = int32(v)
	}
	return out
}

// ============================================================================
// Lock / State types
// ============================================================================

func lockKeyFromProto(p *LockKey) api.LockKey {
	if p == nil {
		return ""
	}
	return api.FormKey(common.BytesToAddress(p.GetAddress()), common.BytesToHash(p.GetKey()))
}

func lockKeyToProto(k api.LockKey) *LockKey {
	addr, key := k.Value()
	return &LockKey{
		Address: addr.Bytes(),
		Key:     key.Bytes(),
	}
}

func lockKeysFromProto(p []*LockKey) []api.LockKey {
	if p == nil {
		return nil
	}
	out := make([]api.LockKey, len(p))
	for i, v := range p {
		out[i] = lockKeyFromProto(v)
	}
	return out
}

func lockKeysToProto(ks []api.LockKey) []*LockKey {
	if ks == nil {
		return nil
	}
	out := make([]*LockKey, len(ks))
	for i, k := range ks {
		out[i] = lockKeyToProto(k)
	}
	return out
}

func stateSetFromProto(p *StateSet) *api.StateSet {
	if p == nil {
		return nil
	}
	s := api.NewStateSet()
	for _, entry := range p.GetEntries() {
		addr := common.BytesToAddress(entry.GetAddress())
		if entry.GetBalance() != nil {
			s.Balance[addr] = new(big.Int).SetBytes(entry.GetBalance())
		}
		m := make(map[common.Hash]common.Hash)
		for _, kv := range entry.GetState() {
			k := common.BytesToHash(kv.GetKey())
			v := common.BytesToHash(kv.GetValue())
			m[k] = v
		}
		if len(m) > 0 {
			s.State[addr] = m
		}
	}
	return s
}

func stateSetToProto(s *api.StateSet) *StateSet {
	if s == nil {
		return nil
	}
	entries := make([]*StateEntry, 0, len(s.State)+1)
	// Balance entries
	for addr, balance := range s.Balance {
		entries = append(entries, &StateEntry{
			Address: addr.Bytes(),
			Balance: balance.Bytes(),
		})
	}
	// State entries (merge with balance if same address)
	merged := make(map[common.Address]int)
	for i, e := range entries {
		merged[common.BytesToAddress(e.GetAddress())] = i
	}
	for addr, state := range s.State {
		if idx, ok := merged[addr]; ok {
			e := entries[idx]
			for k, v := range state {
				e.State = append(e.State, &StateKVPair{
					Key:   k.Bytes(),
					Value: v.Bytes(),
				})
			}
		} else {
			e := &StateEntry{Address: addr.Bytes()}
			for k, v := range state {
				e.State = append(e.State, &StateKVPair{
					Key:   k.Bytes(),
					Value: v.Bytes(),
				})
			}
			entries = append(entries, e)
		}
	}
	return &StateSet{Entries: entries}
}

func rwSetFromProto(p *RWSet) *api.RWSet {
	if p == nil {
		return nil
	}
	return &api.RWSet{
		ReadState:    stateSetFromProto(p.GetReadState()),
		WriteState:   stateSetFromProto(p.GetWriteState()),
		CurrentState: stateSetFromProto(p.GetCurrentState()),
	}
}

func rwSetToProto(r *api.RWSet) *RWSet {
	if r == nil {
		return nil
	}
	return &RWSet{
		ReadState:    stateSetToProto(r.ReadState),
		WriteState:   stateSetToProto(r.WriteState),
		CurrentState: stateSetToProto(r.CurrentState),
	}
}

// ============================================================================
// Signature base messages
// ============================================================================

func baseSSCFromProto(p *BaseSSCMessage) api.BaseSSCMessage {
	if p == nil {
		return api.BaseSSCMessage{}
	}
	return api.BaseSSCMessage{
		Signature:  p.GetSignature(),
		SenderAddr: addrFromProto(p.GetSenderAddr()),
		Epochs:     epochsFromProto(p.GetEpochs()),
	}
}

func baseSSCToProto(m api.BaseSSCMessage) *BaseSSCMessage {
	return &BaseSSCMessage{
		Signature:  m.Signature,
		SenderAddr: addrToProto(m.SenderAddr),
		Epochs:     epochsToProto(m.Epochs),
	}
}

func baseBLSFromProto(p *BaseBLSSignedMessage) api.BaseBLSSignedMessage {
	if p == nil {
		return api.BaseBLSSignedMessage{}
	}
	return api.BaseBLSSignedMessage{
		ShardId:    p.GetShardId(),
		Signatures: p.GetSignatures(),
		BLSBitMap:  p.GetBlsBitmap(),
		Epochs:     epochsFromProto(p.GetEpochs()),
	}
}

func baseBLSToProto(m api.BaseBLSSignedMessage) *BaseBLSSignedMessage {
	return &BaseBLSSignedMessage{
		ShardId:    m.ShardId,
		Signatures: m.Signatures,
		BlsBitmap:  m.BLSBitMap,
		Epochs:     epochsToProto(m.Epochs),
	}
}

func epochsFromProto(p []uint64) []api.Epoch {
	if p == nil {
		return nil
	}
	out := make([]api.Epoch, len(p))
	for i, v := range p {
		out[i] = api.Epoch(v)
	}
	return out
}

func epochsToProto(e []api.Epoch) []uint64 {
	if e == nil {
		return nil
	}
	out := make([]uint64, len(e))
	for i, v := range e {
		out[i] = uint64(v)
	}
	return out
}

// ============================================================================
// CallNodeData
// ============================================================================

func callNodeDataFromProto(p *CallNodeData) *api.CallNodeData {
	if p == nil {
		return nil
	}
	children := make([]*api.CallNodeData, len(p.GetChildren()))
	for i, c := range p.GetChildren() {
		children[i] = callNodeDataFromProto(c)
	}
	return &api.CallNodeData{
		CallIndex: callIndexFromProto(p.GetCallIndex()),
		ShardId:   p.GetShardId(),
		Children:  children,
	}
}

func callNodeDataToProto(n *api.CallNodeData) *CallNodeData {
	if n == nil {
		return nil
	}
	children := make([]*CallNodeData, len(n.Children))
	for i, c := range n.Children {
		children[i] = callNodeDataToProto(c)
	}
	return &CallNodeData{
		CallIndex: callIndexToProto(n.CallIndex),
		ShardId:   n.ShardId,
		Children:  children,
	}
}

// ============================================================================
// CXTSimulationRequest
// ============================================================================

func CXTSimulationRequestFromProto(p *CXTSimulationRequest) *api.CXTSimulationRequest {
	if p == nil {
		return nil
	}
	var tx *types.Transaction
	if len(p.GetTx().GetData()) > 0 {
		tx = new(types.Transaction)
		rlp.DecodeBytes(p.GetTx().GetData(), tx)
	}
	return &api.CXTSimulationRequest{
		BlockNum:      p.GetBlockNum(),
		Epochs:        epochsFromProto(p.GetEpochs()),
		TxHash:        hashFromProto(p.GetTxHash()),
		SimulationNum: int(p.GetSimulationNum()),
		Author:        addrPtrFromProto(p.GetAuthor()),
		BlockHash:     hashFromProto(p.GetBlockHash()),
		Tx:            tx,
		From:          addrFromProto(p.GetFrom()),
		GasPool:       p.GetGasPool(),
	}
}

func CXTSimulationRequestToProto(a *api.CXTSimulationRequest) *CXTSimulationRequest {
	if a == nil {
		return nil
	}
	var txData []byte
	if a.Tx != nil {
		txData, _ = rlp.EncodeToBytes(a.Tx)
	}
	return &CXTSimulationRequest{
		BlockNum:      a.BlockNum,
		Epochs:        epochsToProto(a.Epochs),
		TxHash:        hashToProto(a.TxHash),
		SimulationNum: int32(a.SimulationNum),
		Author:        addrPtrToProto(a.Author),
		BlockHash:     hashToProto(a.BlockHash),
		Tx:            &RLPBytes{Data: txData},
		From:          addrToProto(a.From),
		GasPool:       a.GasPool,
	}
}

// ============================================================================
// CXTSimulationResult
// ============================================================================

func CXTSimulationResultFromProto(p *CXTSimulationResult) *api.CXTSimulationResult {
	if p == nil {
		return nil
	}
	var receipt *types.Receipt
	if len(p.GetReceipt().GetData()) > 0 {
		receipt = new(types.Receipt)
		rlp.DecodeBytes(p.GetReceipt().GetData(), (*types.ReceiptForStorage)(receipt))
	}
	return &api.CXTSimulationResult{
		BaseSSCMessage: baseSSCFromProto(p.GetBase()),
		RelatedShards:  relatedShardsFromProto(p.GetRelatedShards()),
		Result:         p.GetResult(),
		Receipt:        receipt,
		UsedGas:        p.GetUsedGas(),
		Err:            p.GetErr(),
		TreeNode:       callNodeDataFromProto(p.GetTreeNode()),
		ConflictKeys:   lockKeysFromProto(p.GetConflictKeys()),
	}
}

func CXTSimulationResultToProto(a *api.CXTSimulationResult) *CXTSimulationResult {
	if a == nil {
		return nil
	}
	var receiptData []byte
	if a.Receipt != nil {
		receiptData, _ = json.Marshal(a.Receipt)
	}
	return &CXTSimulationResult{
		Base:          baseSSCToProto(a.BaseSSCMessage),
		RelatedShards: relatedShardsToProto(a.RelatedShards),
		Result:        a.Result,
		Receipt:       &RLPBytes{Data: receiptData},
		UsedGas:       a.UsedGas,
		Err:           a.Err,
		TreeNode:      callNodeDataToProto(a.TreeNode),
		ConflictKeys:  lockKeysToProto(a.ConflictKeys),
	}
}

// ============================================================================
// CXTSimulationSSCResult
// ============================================================================

func CXTSimulationSSCResultFromProto(p *CXTSimulationSSCResult) *api.CXTSimulationSSCResult {
	if p == nil {
		return nil
	}
	var receipt *types.Receipt
	if len(p.GetReceipt().GetData()) > 0 {
		receipt = new(types.Receipt)
		json.Unmarshal(p.GetReceipt().GetData(), receipt)
	}
	return &api.CXTSimulationSSCResult{
		RelatedShards:  relatedShardsFromProto(p.GetRelatedShards()),
		Result:         p.GetResult(),
		Receipt:        receipt,
		UsedGas:        p.GetUsedGas(),
		Err:            p.GetErr(),
		TreeNode:       callNodeDataFromProto(p.GetTreeNode()),
		ConflictKeys:   lockKeysFromProto(p.GetConflictKeys()),
		BaseBLSSignedMessage: baseBLSFromProto(p.GetBase()),
	}
}

func CXTSimulationSSCResultToProto(a *api.CXTSimulationSSCResult) *CXTSimulationSSCResult {
	if a == nil {
		return nil
	}
	var receiptData []byte
	if a.Receipt != nil {
		receiptData, _ = json.Marshal(a.Receipt)
	}
	return &CXTSimulationSSCResult{
		RelatedShards: relatedShardsToProto(a.RelatedShards),
		Result:        a.Result,
		Receipt:       &RLPBytes{Data: receiptData},
		UsedGas:       a.UsedGas,
		Err:           a.Err,
		TreeNode:      callNodeDataToProto(a.TreeNode),
		ConflictKeys:  lockKeysToProto(a.ConflictKeys),
		Base:          baseBLSToProto(a.BaseBLSSignedMessage),
	}
}

// ============================================================================
// CXTSimulation
// ============================================================================

func CXTSimulationFromProto(p *CXTSimulation) *api.CXTSimulation {
	if p == nil {
		return nil
	}
	return &api.CXTSimulation{
		SimulationNum:  int(p.GetSimulationNum()),
		TxHash:         hashFromProto(p.GetTxHash()),
		Nonce:          p.GetNonce(),
		Sender:         addrFromProto(p.GetSender()),
		ShardId:        p.GetShardId(),
		OriginShardId:  p.GetOriginShardId(),
		RelatedShards:  relatedShardsFromProto(p.GetRelatedShards()),
		CallStates:     callStatesFromProto(p.GetCallStates()),
		ChainPatch:     rwSetFromProto(p.GetChainPatch()),
		UpstreamTxList: txSimKeysFromProto(p.GetUpstreamTxList()),
		BaseBLSSignedMessage: baseBLSFromProto(p.GetBase()),
	}
}

func CXTSimulationToProto(a *api.CXTSimulation) *CXTSimulation {
	if a == nil {
		return nil
	}
	return &CXTSimulation{
		SimulationNum:  int32(a.SimulationNum),
		TxHash:         hashToProto(a.TxHash),
		Nonce:          a.Nonce,
		Sender:         addrToProto(a.Sender),
		ShardId:        a.ShardId,
		OriginShardId:  a.OriginShardId,
		RelatedShards:  relatedShardsToProto(a.RelatedShards),
		CallStates:     callStatesToProto(a.CallStates),
		ChainPatch:     rwSetToProto(a.ChainPatch),
		UpstreamTxList: txSimKeysToProto(a.UpstreamTxList),
		Base:           baseBLSToProto(a.BaseBLSSignedMessage),
	}
}

// ============================================================================
// CXTCallState
// ============================================================================

func callStateFromProto(p *CXTCallState) *api.CXTCallState {
	if p == nil {
		return nil
	}
	return &api.CXTCallState{
		CallIndex:        callIndexFromProto(p.GetCallIndex()),
		TopRequest:       CXTSimulationRequestFromProto(p.GetTopRequest()),
		CallRequest:      CXTCallSSCRequestFromProto(p.GetCallRequest()),
		RWSet:            rwSetFromProto(p.GetRwSet()),
		DependentResults: cxtCallSSCResultsFromProto(p.GetDependentResults()),
		CallResult:       CXTCallSSCResultFromProto(p.GetCallResult()),
		TopResult:        CXTSimulationSSCResultFromProto(p.GetTopResult()),
	}
}

func callStateToProto(s *api.CXTCallState) *CXTCallState {
	if s == nil {
		return nil
	}
	return &CXTCallState{
		CallIndex:        callIndexToProto(s.CallIndex),
		TopRequest:       CXTSimulationRequestToProto(s.TopRequest),
		CallRequest:      CXTCallSSCRequestToProto(s.CallRequest),
		RwSet:            rwSetToProto(s.RWSet),
		DependentResults: cxtCallSSCResultsToProto(s.DependentResults),
		CallResult:       CXTCallSSCResultToProto(s.CallResult),
		TopResult:        CXTSimulationSSCResultToProto(s.TopResult),
	}
}

func callStatesFromProto(p []*CXTCallState) []*api.CXTCallState {
	if p == nil {
		return nil
	}
	out := make([]*api.CXTCallState, len(p))
	for i, v := range p {
		out[i] = callStateFromProto(v)
	}
	return out
}

func callStatesToProto(s []*api.CXTCallState) []*CXTCallState {
	if s == nil {
		return nil
	}
	out := make([]*CXTCallState, len(s))
	for i, v := range s {
		out[i] = callStateToProto(v)
	}
	return out
}

// ============================================================================
// CXTCallRequest
// ============================================================================

func CXTCallRequestFromProto(p *CXTCallRequest) *api.CXTCallRequest {
	if p == nil {
		return nil
	}
	return &api.CXTCallRequest{
		BaseSSCMessage: baseSSCFromProto(p.GetBase()),
		OriginShardId:  p.GetOriginShardId(),
		FromShardId:    p.GetFromShardId(),
		TargetShardId:  p.GetTargetShardId(),
		SimulationNum:  int(p.GetSimulationNum()),
		RelatedShards:  relatedShardsFromProto(p.GetRelatedShards()),
		TxHash:         hashFromProto(p.GetTxHash()),
		Nonce:          p.GetNonce(),
		TxSender:       p.GetTxSender(),
		CallIndex:      callIndexFromProto(p.GetCallIndex()),
		Caller:         addrFromProto(p.GetCaller()),
		Addr:           addrFromProto(p.GetAddr()),
		Input:          p.GetInput(),
		Gas:            p.GetGas(),
		GasPrice:       bigIntFromProto(p.GetGasPrice()),
		Value:          bigIntFromProto(p.GetValue()),
		BlockHash:      p.GetBlockHash(),
	}
}

func CXTCallRequestToProto(a *api.CXTCallRequest) *CXTCallRequest {
	if a == nil {
		return nil
	}
	return &CXTCallRequest{
		Base:          baseSSCToProto(a.BaseSSCMessage),
		OriginShardId: a.OriginShardId,
		FromShardId:   a.FromShardId,
		TargetShardId: a.TargetShardId,
		SimulationNum: int32(a.SimulationNum),
		RelatedShards: relatedShardsToProto(a.RelatedShards),
		TxHash:        hashToProto(a.TxHash),
		Nonce:         a.Nonce,
		TxSender:      a.TxSender,
		CallIndex:     callIndexToProto(a.CallIndex),
		Caller:        addrToProto(a.Caller),
		Addr:          addrToProto(a.Addr),
		Input:         a.Input,
		Gas:           a.Gas,
		GasPrice:      bigIntToProto(a.GasPrice),
		Value:         bigIntToProto(a.Value),
		BlockHash:     a.BlockHash,
	}
}

// ============================================================================
// CXTCallSSCRequest
// ============================================================================

func CXTCallSSCRequestFromProto(p *CXTCallSSCRequest) *api.CXTCallSSCRequest {
	if p == nil {
		return nil
	}
	return &api.CXTCallSSCRequest{
		OriginShardId:  p.GetOriginShardId(),
		FromShardId:    p.GetFromShardId(),
		TargetShardId:  p.GetTargetShardId(),
		SimulationNum:  int(p.GetSimulationNum()),
		RelatedShards:  relatedShardsFromProto(p.GetRelatedShards()),
		TxHash:         hashFromProto(p.GetTxHash()),
		Nonce:          p.GetNonce(),
		TxSender:       p.GetTxSender(),
		CallIndex:      callIndexFromProto(p.GetCallIndex()),
		Caller:         addrFromProto(p.GetCaller()),
		Addr:           addrFromProto(p.GetAddr()),
		Input:          p.GetInput(),
		Gas:            p.GetGas(),
		GasPrice:       bigIntFromProto(p.GetGasPrice()),
		Value:          bigIntFromProto(p.GetValue()),
		BaseBLSSignedMessage: baseBLSFromProto(p.GetBase()),
		BlockHash:      hashFromProto(p.GetBlockHash()),
		BlockNum:       p.GetBlockNum(),
	}
}

func CXTCallSSCRequestToProto(a *api.CXTCallSSCRequest) *CXTCallSSCRequest {
	if a == nil {
		return nil
	}
	return &CXTCallSSCRequest{
		OriginShardId: a.OriginShardId,
		FromShardId:   a.FromShardId,
		TargetShardId: a.TargetShardId,
		SimulationNum: int32(a.SimulationNum),
		RelatedShards: relatedShardsToProto(a.RelatedShards),
		TxHash:        hashToProto(a.TxHash),
		Nonce:         a.Nonce,
		TxSender:      a.TxSender,
		CallIndex:     callIndexToProto(a.CallIndex),
		Caller:        addrToProto(a.Caller),
		Addr:          addrToProto(a.Addr),
		Input:         a.Input,
		Gas:           a.Gas,
		GasPrice:      bigIntToProto(a.GasPrice),
		Value:         bigIntToProto(a.Value),
		Base:          baseBLSToProto(a.BaseBLSSignedMessage),
		BlockHash:     hashToProto(a.BlockHash),
		BlockNum:      a.BlockNum,
	}
}

// ============================================================================
// CXTCallResult / CXTCallSSCResult
// ============================================================================

func CXTCallResultFromProto(p *CXTCallResult) *api.CXTCallResult {
	if p == nil {
		return nil
	}
	return &api.CXTCallResult{
		TxHash:        hashFromProto(p.GetTxHash()),
		CallIndex:     callIndexFromProto(p.GetCallIndex()),
		RelatedShards: relatedShardsFromProto(p.GetRelatedShards()),
		Result:        p.GetResult(),
		LeftOverGas:   p.GetLeftOverGas(),
		BlockHash:     hashFromProto(p.GetBlockHash()),
		Err:           p.GetErr(),
		TreeNode:      callNodeDataFromProto(p.GetTreeNode()),
		BaseSSCMessage: baseSSCFromProto(p.GetBase()),
	}
}

func CXTCallResultToProto(a *api.CXTCallResult) *CXTCallResult {
	if a == nil {
		return nil
	}
	return &CXTCallResult{
		TxHash:        hashToProto(a.TxHash),
		CallIndex:     callIndexToProto(a.CallIndex),
		RelatedShards: relatedShardsToProto(a.RelatedShards),
		Result:        a.Result,
		LeftOverGas:   a.LeftOverGas,
		BlockHash:     hashToProto(a.BlockHash),
		Err:           a.Err,
		TreeNode:      callNodeDataToProto(a.TreeNode),
		Base:          baseSSCToProto(a.BaseSSCMessage),
	}
}

func CXTCallSSCResultFromProto(p *CXTCallSSCResult) *api.CXTCallSSCResult {
	if p == nil {
		return nil
	}
	return &api.CXTCallSSCResult{
		TxHash:        hashFromProto(p.GetTxHash()),
		CallIndex:     callIndexFromProto(p.GetCallIndex()),
		RelatedShards: relatedShardsFromProto(p.GetRelatedShards()),
		Result:        p.GetResult(),
		LeftOverGas:   p.GetLeftOverGas(),
		BlockHash:     hashFromProto(p.GetBlockHash()),
		Err:           p.GetErr(),
		TreeNode:      callNodeDataFromProto(p.GetTreeNode()),
		BaseBLSSignedMessage: baseBLSFromProto(p.GetBase()),
	}
}

func CXTCallSSCResultToProto(a *api.CXTCallSSCResult) *CXTCallSSCResult {
	if a == nil {
		return nil
	}
	return &CXTCallSSCResult{
		TxHash:        hashToProto(a.TxHash),
		CallIndex:     callIndexToProto(a.CallIndex),
		RelatedShards: relatedShardsToProto(a.RelatedShards),
		Result:        a.Result,
		LeftOverGas:   a.LeftOverGas,
		BlockHash:     hashToProto(a.BlockHash),
		Err:           a.Err,
		TreeNode:      callNodeDataToProto(a.TreeNode),
		Base:          baseBLSToProto(a.BaseBLSSignedMessage),
	}
}

func cxtCallSSCResultsFromProto(p []*CXTCallSSCResult) []*api.CXTCallSSCResult {
	if p == nil {
		return nil
	}
	out := make([]*api.CXTCallSSCResult, len(p))
	for i, v := range p {
		out[i] = CXTCallSSCResultFromProto(v)
	}
	return out
}

func cxtCallSSCResultsToProto(s []*api.CXTCallSSCResult) []*CXTCallSSCResult {
	if s == nil {
		return nil
	}
	out := make([]*CXTCallSSCResult, len(s))
	for i, v := range s {
		out[i] = CXTCallSSCResultToProto(v)
	}
	return out
}

// ============================================================================
// SimulationCommit
// ============================================================================

func SimulationCommitFromProto(p *SimulationCommit) *api.SimulationCommit {
	if p == nil {
		return nil
	}
	return &api.SimulationCommit{
		SimulationNum:  int(p.GetSimulationNum()),
		TxHash:         hashFromProto(p.GetTxHash()),
		Nonce:          p.GetNonce(),
		Sender:         addrFromProto(p.GetSender()),
		RelatedShards:  relatedShardsFromProto(p.GetRelatedShards()),
		Commit:         p.GetCommit(),
		Status:         simulationCommitStatusFromProto(p.GetStatus()),
		Reason:         p.GetReason(),
		BaseBLSSignedMessage: baseBLSFromProto(p.GetBase()),
	}
}

func SimulationCommitToProto(a *api.SimulationCommit) *SimulationCommit {
	if a == nil {
		return nil
	}
	return &SimulationCommit{
		SimulationNum:  int32(a.SimulationNum),
		TxHash:         hashToProto(a.TxHash),
		Nonce:          a.Nonce,
		Sender:         addrToProto(a.Sender),
		RelatedShards:  relatedShardsToProto(a.RelatedShards),
		Commit:         a.Commit,
		Status:         simulationCommitStatusToProto(a.Status),
		Reason:         a.Reason,
		Base:           baseBLSToProto(a.BaseBLSSignedMessage),
	}
}

func simulationCommitStatusFromProto(p SimulationCommitStatus) api.SimulationCommitStatus {
	switch p {
	case SimulationCommitStatus_SIM_COMMIT_OK:
		return api.OK
	case SimulationCommitStatus_EXECUTION_FAILED:
		return api.ExecutionFailed
	case SimulationCommitStatus_LOCK_CONFLICT:
		return api.LockConflict
	case SimulationCommitStatus_POOL_TIMEOUT:
		return api.PoolTimeout
	default:
		return api.OK
	}
}

func simulationCommitStatusToProto(s api.SimulationCommitStatus) SimulationCommitStatus {
	switch s {
	case api.OK:
		return SimulationCommitStatus_SIM_COMMIT_OK
	case api.ExecutionFailed:
		return SimulationCommitStatus_EXECUTION_FAILED
	case api.LockConflict:
		return SimulationCommitStatus_LOCK_CONFLICT
	case api.PoolTimeout:
		return SimulationCommitStatus_POOL_TIMEOUT
	default:
		return SimulationCommitStatus_SIM_COMMIT_OK
	}
}

// ============================================================================
// CXTCommitVote
// ============================================================================

func CXTCommitVoteFromProto(p *CXTCommitVote) *api.CXTCommitVote {
	if p == nil {
		return nil
	}
	return &api.CXTCommitVote{
		BaseSSCMessage: baseSSCFromProto(p.GetBase()),
		TxHash:         hashFromProto(p.GetTxHash()),
		SimulationNum:  int(p.GetSimulationNum()),
		ShardId:        p.GetShardId(),
		OriginShardId:  p.GetOriginShardId(),
		Type:           cxtCommitTypeFromProto(p.GetType()),
		Reason:         cxtCommitReasonFromProto(p.GetReason()),
		Payload:        p.GetPayload(),
	}
}

func CXTCommitVoteToProto(a *api.CXTCommitVote) *CXTCommitVote {
	if a == nil {
		return nil
	}
	return &CXTCommitVote{
		Base:          baseSSCToProto(a.BaseSSCMessage),
		TxHash:        hashToProto(a.TxHash),
		SimulationNum: int32(a.SimulationNum),
		ShardId:       a.ShardId,
		OriginShardId: a.OriginShardId,
		Type:          cxtCommitTypeToProto(a.Type),
		Reason:        cxtCommitReasonToProto(a.Reason),
		Payload:       a.Payload,
	}
}

// ============================================================================
// CXTCommitSSCVote
// ============================================================================

func CXTCommitSSCVoteFromProto(p *CXTCommitSSCVote) *api.CXTCommitSSCVote {
	if p == nil {
		return nil
	}
	return &api.CXTCommitSSCVote{
		TxHash:        hashFromProto(p.GetTxHash()),
		SimulationNum: int(p.GetSimulationNum()),
		ShardId:       p.GetShardId(),
		OriginShardId: p.GetOriginShardId(),
		Type:          cxtCommitTypeFromProto(p.GetType()),
		Reason:        cxtCommitReasonFromProto(p.GetReason()),
		Payload:       p.GetPayload(),
		BaseBLSSignedMessage: baseBLSFromProto(p.GetBase()),
	}
}

func CXTCommitSSCVoteToProto(a *api.CXTCommitSSCVote) *CXTCommitSSCVote {
	if a == nil {
		return nil
	}
	return &CXTCommitSSCVote{
		TxHash:        hashToProto(a.TxHash),
		SimulationNum: int32(a.SimulationNum),
		ShardId:       a.ShardId,
		OriginShardId: a.OriginShardId,
		Type:          cxtCommitTypeToProto(a.Type),
		Reason:        cxtCommitReasonToProto(a.Reason),
		Payload:       a.Payload,
		Base:          baseBLSToProto(a.BaseBLSSignedMessage),
	}
}

// ============================================================================
// CXTCommitProof
// ============================================================================

func CXTCommitProofFromProto(p *CXTCommitProof) *api.CXTCommitProof {
	if p == nil {
		return nil
	}
	votes := make([]*api.CXTCommitSSCVote, len(p.GetVotes()))
	for i, v := range p.GetVotes() {
		votes[i] = CXTCommitSSCVoteFromProto(v)
	}
	return &api.CXTCommitProof{
		BaseSSCMessage: baseSSCFromProto(p.GetBase()),
		TxHash:         hashFromProto(p.GetTxHash()),
		SimulationNum:  int(p.GetSimulationNum()),
		Type:           cxtCommitTypeFromProto(p.GetType()),
		Reason:         cxtCommitReasonFromProto(p.GetReason()),
		OriginShard:    p.GetOriginShard(),
		RelatedShards:  relatedShardsFromProto(p.GetRelatedShards()),
		Votes:          votes,
	}
}

func CXTCommitProofToProto(a *api.CXTCommitProof) *CXTCommitProof {
	if a == nil {
		return nil
	}
	votes := make([]*CXTCommitSSCVote, len(a.Votes))
	for i, v := range a.Votes {
		votes[i] = CXTCommitSSCVoteToProto(v)
	}
	return &CXTCommitProof{
		Base:          baseSSCToProto(a.BaseSSCMessage),
		TxHash:        hashToProto(a.TxHash),
		SimulationNum: int32(a.SimulationNum),
		Type:          cxtCommitTypeToProto(a.Type),
		Reason:        cxtCommitReasonToProto(a.Reason),
		OriginShard:   a.OriginShard,
		RelatedShards: relatedShardsToProto(a.RelatedShards),
		Votes:         votes,
	}
}

// ============================================================================
// Retry types
// ============================================================================

func TxSimKeyFromProto(p *TxSimKey) api.TxSimKey {
	return api.TxSimKey{
		TxHash:        hashFromProto(p.GetTxHash()),
		SimulationNum: int(p.GetSimulationNum()),
	}
}

func TxSimKeyToProto(k api.TxSimKey) *TxSimKey {
	return &TxSimKey{
		TxHash:        hashToProto(k.TxHash),
		SimulationNum: int32(k.SimulationNum),
	}
}

func txSimKeysFromProto(p []*TxSimKey) []api.TxSimKey {
	if p == nil {
		return nil
	}
	out := make([]api.TxSimKey, len(p))
	for i, v := range p {
		out[i] = TxSimKeyFromProto(v)
	}
	return out
}

func txSimKeysToProto(ks []api.TxSimKey) []*TxSimKey {
	if ks == nil {
		return nil
	}
	out := make([]*TxSimKey, len(ks))
	for i, k := range ks {
		out[i] = TxSimKeyToProto(k)
	}
	return out
}

func RetrySignalFromProto(p *RetrySignal) *api.RetrySignal {
	if p == nil {
		return nil
	}
	return &api.RetrySignal{
		TxHash:        hashFromProto(p.GetTxHash()),
		FromShard:     p.GetFromShard(),
		Epoch:         api.Epoch(p.GetEpoch()),
		SimulationNum: int(p.GetSimulationNum()),
		Condition:     conflictConditionFromProto(p.GetCondition()),
		Ready:         p.GetReady(),
		ChainPatch:    rwSetFromProto(p.GetChainPatch()),
	}
}

func RetrySignalToProto(a *api.RetrySignal) *RetrySignal {
	if a == nil {
		return nil
	}
	return &RetrySignal{
		TxHash:        hashToProto(a.TxHash),
		FromShard:     a.FromShard,
		Epoch:         uint64(a.Epoch),
		SimulationNum: int32(a.SimulationNum),
		Condition:     conflictConditionToProto(a.Condition),
		Ready:         a.Ready,
		ChainPatch:    rwSetToProto(a.ChainPatch),
	}
}

func RetrySignalsFromProto(p *RetrySignals) *api.RetrySignals {
	if p == nil {
		return nil
	}
	signals := make([]*api.RetrySignal, len(p.GetSignals()))
	for i, v := range p.GetSignals() {
		signals[i] = RetrySignalFromProto(v)
	}
	return &api.RetrySignals{
		OriginShard: p.GetOriginShard(),
		FromShard:   p.GetFromShard(),
		Epoch:       api.Epoch(p.GetEpoch()),
		Signals:     signals,
	}
}

func RetrySignalsToProto(a *api.RetrySignals) *RetrySignals {
	if a == nil {
		return nil
	}
	signals := make([]*RetrySignal, len(a.Signals))
	for i, v := range a.Signals {
		signals[i] = RetrySignalToProto(v)
	}
	return &RetrySignals{
		OriginShard: a.OriginShard,
		FromShard:   a.FromShard,
		Epoch:       uint64(a.Epoch),
		Signals:     signals,
	}
}

func RetryTxFromProto(p *RetryTx) *api.RetryTx {
	if p == nil {
		return nil
	}
	return &api.RetryTx{
		TxHash:        hashFromProto(p.GetTxHash()),
		Epochs:        epochsFromProto(p.GetEpochs()),
		Sender:        addrFromProto(p.GetSender()),
		Nonce:         p.GetNonce(),
		GasPrice:      p.GetGasPrice(),
		OriginShardID: p.GetOriginShardId(),
		ReadSet:       lockKeysFromProto(p.GetReadSet()),
		WriteSet:      lockKeysFromProto(p.GetWriteSet()),
		RelatedShards: relatedShardsFromProto(p.GetRelatedShards()),
		SimulationNum: int(p.GetSimulationNum()),
		Condition:     conflictConditionFromProto(p.GetCondition()),
		Status:        retryStatusFromProto(p.GetStatus()),
	}
}

func RetryTxToProto(a *api.RetryTx) *RetryTx {
	if a == nil {
		return nil
	}
	return &RetryTx{
		TxHash:        hashToProto(a.TxHash),
		Epochs:        epochsToProto(a.Epochs),
		Sender:        addrToProto(a.Sender),
		Nonce:         a.Nonce,
		GasPrice:      a.GasPrice,
		OriginShardId: a.OriginShardID,
		ReadSet:       lockKeysToProto(a.ReadSet),
		WriteSet:      lockKeysToProto(a.WriteSet),
		RelatedShards: relatedShardsToProto(a.RelatedShards),
		SimulationNum: int32(a.SimulationNum),
		Condition:     conflictConditionToProto(a.Condition),
		Status:        retryStatusToProto(a.Status),
	}
}

func RetryCommitRespFromProto(p *RetryCommitResp) *api.RetryCommitResp {
	if p == nil {
		return nil
	}
	return &api.RetryCommitResp{
		TxHash:              hashFromProto(p.GetTxHash()),
		Locked:              p.GetLocked(),
		OnChainLockConflict: p.GetOnChainLockConflict(),
	}
}

func RetryCommitRespToProto(a *api.RetryCommitResp) *RetryCommitResp {
	if a == nil {
		return nil
	}
	return &RetryCommitResp{
		TxHash:              hashToProto(a.TxHash),
		Locked:              a.Locked,
		OnChainLockConflict: a.OnChainLockConflict,
	}
}

// ============================================================================
// Other types
// ============================================================================

func NewEpochFromProto(p *NewEpoch) *api.NewEpoch {
	if p == nil {
		return nil
	}
	return &api.NewEpoch{
		Committee: nil, // JSON serialized; caller must unmarshal if needed
	}
}

func NewEpochToProto(a *api.NewEpoch) *NewEpoch {
	if a == nil {
		return nil
	}
	return &NewEpoch{
		Committee: nil, // JSON serialized; handled by caller
	}
}

func SLTestRequestFromProto(p *SLTestRequest) *api.SLTestRequest {
	if p == nil {
		return nil
	}
	return &api.SLTestRequest{
		TestFile:   p.GetTestFile(),
		Difficulty: int(p.GetDifficulty()),
	}
}

func SLTestRequestToProto(a *api.SLTestRequest) *SLTestRequest {
	if a == nil {
		return nil
	}
	return &SLTestRequest{
		TestFile:   a.TestFile,
		Difficulty: int32(a.Difficulty),
	}
}

func SLTestResultFromProto(p *SLTestResult) *api.SLTestResult {
	if p == nil {
		return nil
	}
	return &api.SLTestResult{
		Result: p.GetResult(),
		Error:  p.GetError(),
	}
}

func SLTestResultToProto(a *api.SLTestResult) *SLTestResult {
	if a == nil {
		return nil
	}
	return &SLTestResult{
		Result: a.Result,
		Error:  a.Error,
	}
}

// ============================================================================
// Shared helpers
// ============================================================================

func relatedShardsFromProto(p []uint32) api.RelatedShards {
	return api.RelatedShards(p)
}

func relatedShardsToProto(r api.RelatedShards) []uint32 {
	return []uint32(r)
}

func cxtCommitTypeFromProto(p CXTCommitType) api.CXTCommitType {
	switch p {
	case CXTCommitType_TYPE_COMMIT:
		return api.Commit
	case CXTCommitType_TYPE_ROLLBACK:
		return api.Rollback
	default:
		return api.Commit
	}
}

func cxtCommitTypeToProto(a api.CXTCommitType) CXTCommitType {
	switch a {
	case api.Commit:
		return CXTCommitType_TYPE_COMMIT
	case api.Rollback:
		return CXTCommitType_TYPE_ROLLBACK
	default:
		return CXTCommitType_TYPE_COMMIT
	}
}

func cxtCommitReasonFromProto(p CXTCommitReason) api.CXTCommitReason {
	switch p {
	case CXTCommitReason_REASON_SUCCESS:
		return api.ReasonSuccess
	case CXTCommitReason_REASON_EXECUTION_FAILED:
		return api.ReasonExecutionFailed
	case CXTCommitReason_REASON_INVALID_SIMULATION:
		return api.ReasonInvalidSimulation
	case CXTCommitReason_REASON_CONFLICT_RWSET_FAILED_LOCK:
		return api.ReasonConflictRWSetFailedLock
	case CXTCommitReason_REASON_CONFLICT_RWSET_RECALL:
		return api.ReasonConflictRWSetRecall
	case CXTCommitReason_REASON_CXT_TIMEOUT_FOR_SP1:
		return api.ReasonCxtTimeoutForSp1
	case CXTCommitReason_REASON_MAX_ON_CHAIN_RETRIES_EXCEEDED:
		return api.ReasonMaxOnChainRetriesExceeded
	default:
		return api.ReasonSuccess
	}
}

func cxtCommitReasonToProto(a api.CXTCommitReason) CXTCommitReason {
	switch a {
	case api.ReasonSuccess:
		return CXTCommitReason_REASON_SUCCESS
	case api.ReasonExecutionFailed:
		return CXTCommitReason_REASON_EXECUTION_FAILED
	case api.ReasonInvalidSimulation:
		return CXTCommitReason_REASON_INVALID_SIMULATION
	case api.ReasonConflictRWSetFailedLock:
		return CXTCommitReason_REASON_CONFLICT_RWSET_FAILED_LOCK
	case api.ReasonConflictRWSetRecall:
		return CXTCommitReason_REASON_CONFLICT_RWSET_RECALL
	case api.ReasonCxtTimeoutForSp1:
		return CXTCommitReason_REASON_CXT_TIMEOUT_FOR_SP1
	case api.ReasonMaxOnChainRetriesExceeded:
		return CXTCommitReason_REASON_MAX_ON_CHAIN_RETRIES_EXCEEDED
	default:
		return CXTCommitReason_REASON_SUCCESS
	}
}

func conflictConditionFromProto(p ConflictCondition) api.ConflictCondition {
	switch p {
	case ConflictCondition_CONDITION_SIMULATE:
		return api.Simulate
	case ConflictCondition_CONDITION_VERIFY:
		return api.Verify
	default:
		return api.Simulate
	}
}

func conflictConditionToProto(a api.ConflictCondition) ConflictCondition {
	switch a {
	case api.Simulate:
		return ConflictCondition_CONDITION_SIMULATE
	case api.Verify:
		return ConflictCondition_CONDITION_VERIFY
	default:
		return ConflictCondition_CONDITION_SIMULATE
	}
}

func retryStatusFromProto(p RetryStatus) api.RetryStatus {
	switch p {
	case RetryStatus_RETRY_ACTIVE:
		return api.RetryActive
	case RetryStatus_RETRY_CONSUMED:
		return api.RetryConsumed
	case RetryStatus_RETRY_PASSIVE:
		return api.RetryPassive
	case RetryStatus_RETRY_WOUNDED:
		return api.RetryWounded
	default:
		return api.RetryActive
	}
}

func retryStatusToProto(a api.RetryStatus) RetryStatus {
	switch a {
	case api.RetryActive:
		return RetryStatus_RETRY_ACTIVE
	case api.RetryConsumed:
		return RetryStatus_RETRY_CONSUMED
	case api.RetryPassive:
		return RetryStatus_RETRY_PASSIVE
	case api.RetryWounded:
		return RetryStatus_RETRY_WOUNDED
	default:
		return RetryStatus_RETRY_ACTIVE
	}
}
