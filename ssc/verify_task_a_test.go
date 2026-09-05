package ssc

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/ssc/api"
)

// TestClassifyChainReadWrite — Task A (DSN-60) 判据纯逻辑单测：
// classifyChainReadWrite 应正确区分“只读上游产出 / 读-写续接 / 纯续写 / 无依赖”。
func TestClassifyChainReadWrite(t *testing.T) {
	addr := common.HexToAddress("0x1111")
	keyA := common.HexToHash("0xaaa")
	keyB := common.HexToHash("0xbbb")

	// producedByUpstream: 只有 keyA 是上游产出的。
	produced := func(a common.Address, k common.Hash) bool {
		return a == addr && k == keyA
	}

	csWith := func(reads, writes []common.Hash) *api.CXTCallState {
		readSet := api.NewStateSet()
		for _, k := range reads {
			if readSet.State[addr] == nil {
				readSet.State[addr] = make(map[common.Hash]common.Hash)
			}
			readSet.State[addr][k] = common.HexToHash("0x1")
		}
		writeSet := api.NewStateSet()
		for _, k := range writes {
			if writeSet.State[addr] == nil {
				writeSet.State[addr] = make(map[common.Hash]common.Hash)
			}
			writeSet.State[addr][k] = common.HexToHash("0x2")
		}
		return &api.CXTCallState{
			RWSet: &api.RWSet{
				ReadState:  readSet,
				WriteState: writeSet,
			},
		}
	}

	sim := func(states []*api.CXTCallState) *api.CXTSimulation {
		return &api.CXTSimulation{
			UpstreamTxList: []api.TxSimKey{{TxHash: common.HexToHash("0xup")}},
			CallStates:     states,
		}
	}

	// 1) 只读上游产出 keyA，写的是无关 keyB → readDep=true, writeCont=false
	{
		rd, wc := classifyChainReadWrite(sim([]*api.CXTCallState{csWith([]common.Hash{keyA}, []common.Hash{keyB})}), produced)
		if !rd || wc {
			t.Fatalf("read-only case: expected readDep=true writeCont=false, got rd=%v wc=%v", rd, wc)
		}
	}

	// 2) 读 keyA 且续写 keyA → readDep=true, writeCont=true (read-write 续写)
	{
		rd, wc := classifyChainReadWrite(sim([]*api.CXTCallState{csWith([]common.Hash{keyA}, []common.Hash{keyA})}), produced)
		if !rd || !wc {
			t.Fatalf("read-write case: expected rd=true wc=true, got rd=%v wc=%v", rd, wc)
		}
	}

	// 3) 不读上游产出、但续写 keyA（纯续写/撞槽） → readDep=false, writeCont=true
	{
		rd, wc := classifyChainReadWrite(sim([]*api.CXTCallState{csWith([]common.Hash{keyB}, []common.Hash{keyA})}), produced)
		if rd || !wc {
			t.Fatalf("write-cont case: expected rd=false wc=true, got rd=%v wc=%v", rd, wc)
		}
	}

	// 4) 读写都是无关 keyB → 无依赖
	{
		rd, wc := classifyChainReadWrite(sim([]*api.CXTCallState{csWith([]common.Hash{keyB}, []common.Hash{keyB})}), produced)
		if rd || wc {
			t.Fatalf("no-dep case: expected rd=false wc=false, got rd=%v wc=%v", rd, wc)
		}
	}

	// 5) nil/空上游集合 → 无依赖
	{
		s := sim([]*api.CXTCallState{csWith([]common.Hash{keyA}, []common.Hash{keyA})})
		s.UpstreamTxList = nil
		rd, wc := classifyChainReadWrite(s, produced)
		if rd || wc {
			t.Fatalf("empty-upstream case: expected rd=false wc=false, got rd=%v wc=%v", rd, wc)
		}
		if rd, wc := classifyChainReadWrite(nil, produced); rd || wc {
			t.Fatalf("nil-sim case: expected false/false")
		}
	}
}
