package api

import (
	"github.com/ethereum/go-ethereum/common"
	"math/big"
	"reflect"
	"testing"
)

func TestSerialize(t *testing.T) {
	m := &CXTCommitProof{
		BaseSSCMessage: &BaseSSCMessage{},
		TxHash:         nil,
		Type:           0,
		Reason:         0,
		RelatedShards:  nil,
		Votes:          nil,
	}
	println(string(m.Bytes()))
}

func TestBaseBLSSignedMessage_GetBLSBitMap(t *testing.T) {
	type fields struct {
		ShardId    uint32
		Signatures []byte
		BLSBitMap  []byte
	}
	tests := []struct {
		name   string
		fields fields
		want   []byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &BaseBLSSignedMessage{
				ShardId:    tt.fields.ShardId,
				Signatures: tt.fields.Signatures,
				BLSBitMap:  tt.fields.BLSBitMap,
			}
			if got := m.GetBLSBitMap(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetBLSBitMap() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBaseBLSSignedMessage_GetShardId(t *testing.T) {
	type fields struct {
		ShardId    uint32
		Signatures []byte
		BLSBitMap  []byte
	}
	tests := []struct {
		name   string
		fields fields
		want   uint32
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &BaseBLSSignedMessage{
				ShardId:    tt.fields.ShardId,
				Signatures: tt.fields.Signatures,
				BLSBitMap:  tt.fields.BLSBitMap,
			}
			if got := m.GetShardId(); got != tt.want {
				t.Errorf("GetShardId() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBaseBLSSignedMessage_GetSignatures(t *testing.T) {
	type fields struct {
		ShardId    uint32
		Signatures []byte
		BLSBitMap  []byte
	}
	tests := []struct {
		name   string
		fields fields
		want   []byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &BaseBLSSignedMessage{
				ShardId:    tt.fields.ShardId,
				Signatures: tt.fields.Signatures,
				BLSBitMap:  tt.fields.BLSBitMap,
			}
			if got := m.GetSignatures(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetSignatures() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBaseSSCMessage_GetSenderAddr(t *testing.T) {
	type fields struct {
		Signature  []byte
		SenderAddr common.Address
	}
	tests := []struct {
		name   string
		fields fields
		want   common.Address
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &BaseSSCMessage{
				Signature:  tt.fields.Signature,
				SenderAddr: tt.fields.SenderAddr,
			}
			if got := s.GetSenderAddr(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetSenderAddr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBaseSSCMessage_GetSignature(t *testing.T) {
	type fields struct {
		Signature  []byte
		SenderAddr common.Address
	}
	tests := []struct {
		name   string
		fields fields
		want   []byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &BaseSSCMessage{
				Signature:  tt.fields.Signature,
				SenderAddr: tt.fields.SenderAddr,
			}
			if got := s.GetSignature(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetSignature() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCallIndex_Compare(t *testing.T) {
	type args struct {
		other CallIndex
	}
	tests := []struct {
		name string
		c    CallIndex
		args args
		want int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.c.Compare(tt.args.other); got != tt.want {
				t.Errorf("Compare() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCallIndex_ToString(t *testing.T) {
	tests := []struct {
		name string
		c    CallIndex
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.c.ToString(); got != tt.want {
				t.Errorf("ToString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFromString(t *testing.T) {
	type args struct {
		callIndexStr string
	}
	tests := []struct {
		name string
		args args
		want CallIndex
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FromString(tt.args.callIndexStr); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FromString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSimulationCallState_Compare(t *testing.T) {
	type fields struct {
		CallIndex        CallIndex
		Requests         []*CXTCallRequest
		SignedRequest    *CXTCallSSCRequest
		DependentResults []*CXTCallSSCResult
		RWSet            *RWSet
		Result           *CXTCallSSCResult
		Executed         bool
		WaitingChs       []chan *CXTCallSSCResult
	}
	type args struct {
		other *SimulationCallState
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &SimulationCallState{
				CallIndex:        tt.fields.CallIndex,
				Requests:         tt.fields.Requests,
				SignedRequest:    tt.fields.SignedRequest,
				DependentResults: tt.fields.DependentResults,
				RWSet:            tt.fields.RWSet,
				CallSSCResult:    tt.fields.Result,
				Executed:         tt.fields.Executed,
				WaitingChs:       tt.fields.WaitingChs,
			}
			if got := s.Compare(tt.args.other); got != tt.want {
				t.Errorf("Compare() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSimulationCallStates_Add(t *testing.T) {
	type args struct {
		state *SimulationCallState
	}
	tests := []struct {
		name string
		s    SimulationCallStates
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.s.Add(tt.args.state)
		})
	}
}

func TestSimulationCallStates_Get(t *testing.T) {
	type args struct {
		index CallIndex
	}
	tests := []struct {
		name string
		s    SimulationCallStates
		args args
		want *SimulationCallState
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.Get(tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSimulationRecallStates_Add(t *testing.T) {
	type args struct {
		state *SimulationRecallState
	}
	tests := []struct {
		name string
		s    SimulationRecallStates
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.s.Add(tt.args.state)
		})
	}
}

func TestSimulationRecallStates_Get(t *testing.T) {
	type args struct {
		index CallIndex
	}
	tests := []struct {
		name string
		s    SimulationRecallStates
		args args
		want *SimulationRecallState
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.Get(tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStateSet_Equal(t *testing.T) {
	type fields struct {
		Balance map[common.Address]*big.Int
		State   map[common.Address]map[common.Hash]common.Hash
	}
	type args struct {
		other *StateSet
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &StateSet{
				Balance: tt.fields.Balance,
				State:   tt.fields.State,
			}
			if got := s.Equal(tt.args.other); got != tt.want {
				t.Errorf("Equal() = %v, want %v", got, tt.want)
			}
		})
	}
}
