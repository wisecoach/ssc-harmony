package ssc

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/ssc/api"
	sscpb "github.com/harmony-one/harmony/ssc/api/proto"
	"google.golang.org/protobuf/proto"
)

// makeSimTx 构造一个携带给定上游(UpstreamTxList)的 SimTx 内部交易。
func makeSimTx(upstreams []common.Hash) *types.SSCInternalTx {
	apiSim := &api.CXTSimulation{
		TxHash:         common.HexToHash("0xdead"),
		UpstreamTxList: make([]api.TxSimKey, 0, len(upstreams)),
	}
	for _, u := range upstreams {
		apiSim.UpstreamTxList = append(apiSim.UpstreamTxList, api.TxSimKey{TxHash: u})
	}
	payload, err := proto.Marshal(sscpb.CXTSimulationToProto(apiSim))
	if err != nil {
		panic(err)
	}
	return &types.SSCInternalTx{Type: types.InternalTxTypeSimTx, Payload: payload}
}

// TestSimTxLocalDepsReady — DSN-60 Task B：simTxLocalDepsReady 判定
// “本地上游同批”或“上游已 on-chain”为就绪；否则 not-ready。
func TestSimTxLocalDepsReady(t *testing.T) {
	up := common.HexToHash("0xup")
	down := makeSimTx([]common.Hash{up})

	// 1) 上游既不在同批、也未 on-chain → not-ready
	p := NewSSCInternalPool()
	if p.simTxLocalDepsReady(down, map[common.Hash]struct{}{}) {
		t.Fatalf("expected not-ready when upstream absent & not on-chain")
	}

	// 2) 上游在本批(inRow) → ready（DAG 序保证 U 在 D 前）
	if !p.simTxLocalDepsReady(down, map[common.Hash]struct{}{down.Hash(): {}, up: {}}) {
		t.Fatalf("expected ready when upstream in same batch")
	}

	// 3) 上游不在本批但已 on-chain → ready
	p2 := NewSSCInternalPool()
	p2.simUpstreamOnChain = func(h common.Hash) bool { return h == up }
	if !p2.simTxLocalDepsReady(down, nil) {
		t.Fatalf("expected ready when upstream on-chain")
	}

	// 4) 无上游(根 SimTx) → ready
	root := makeSimTx(nil)
	if !p.simTxLocalDepsReady(root, nil) {
		t.Fatalf("expected root (no upstream) ready")
	}
}

// TestFilterReadySimTxsBoundedHold — DSN-60 Task B：filterReadySimTxs 应扣住 not-ready 下游
// （不放本块），并在达到 simNotReadyHoldBlocks 后强制放行一次（避免饿死）。
func TestFilterReadySimTxsBoundedHold(t *testing.T) {
	up := common.HexToHash("0xup")
	down := makeSimTx([]common.Hash{up})

	// 上游永远不 on-chain，也不在同批 → down 一直 not-ready
	p := NewSSCInternalPool()
	p.simUpstreamOnChain = func(common.Hash) bool { return false }
	row := []*types.SSCInternalTx{down}

	// 前 simNotReadyHoldBlocks-1 次：扣住（不放行）
	for i := 0; i < simNotReadyHoldBlocks-1; i++ {
		if got := p.filterReadySimTxs(row); len(got) != 0 {
			t.Fatalf("iter %d: expected held (empty), got %d", i, len(got))
		}
	}
	// 第 simNotReadyHoldBlocks 次：达阈值，强制放行
	if got := p.filterReadySimTxs(row); len(got) != 1 {
		t.Fatalf("expected release after threshold, got %d", len(got))
	}
}

// TestFilterReadySimTxsReadyKeptOrdered — DSN-60 Task B：就绪的 SimTx（含同批依赖）应被保留，
// not-ready 的不占位；filterReadySimTxs 是纯筛选、保持输入顺序（DAG 序由先前的 orderSimTxsByDAG 负责）。
func TestFilterReadySimTxsReadyKeptOrdered(t *testing.T) {
	upTx := makeSimTx(nil) // 上游(根) SimTx
	down := makeSimTx([]common.Hash{upTx.Hash()})

	// down 引用 upTx；upTx 同批(inRow) → down 就绪；upTx 根 → 就绪。二者都应放行。
	p := NewSSCInternalPool()
	p.simUpstreamOnChain = func(common.Hash) bool { return false }
	row := []*types.SSCInternalTx{down, upTx} // 乱序入参（filter 只筛选，不排序）
	got := p.filterReadySimTxs(row)
	if len(got) != 2 {
		t.Fatalf("expected both ready returned, got %d", len(got))
	}
}

// TestOrderSimTxsByDAGThenFilter — DSN-60 Task B：模拟 Extract 的组合（orderSimTxsByDAG 后
// filterReadySimTxs）在“上游同批”场景下应产出 上游→下游 的顺序。
func TestOrderSimTxsByDAGThenFilter(t *testing.T) {
	upTx := makeSimTx(nil)
	down := makeSimTx([]common.Hash{upTx.Hash()})
	p := NewSSCInternalPool()
	row := []*types.SSCInternalTx{down, upTx} // 乱序入参
	ordered := p.orderSimTxsByDAG(row)
	if len(ordered) != 2 {
		t.Fatalf("expected 2 after DAG order, got %d", len(ordered))
	}
	// 未接谓词时 filterReadySimTxs 原样返回
	got := p.filterReadySimTxs(ordered)
	if len(got) != 2 {
		t.Fatalf("expected both kept (no predicate), got %d", len(got))
	}
	if got[0].Hash() != upTx.Hash() || got[1].Hash() != down.Hash() {
		t.Fatalf("expected upstream-first [upTx, down], got [%s, %s]",
			got[0].Hash().Hex(), got[1].Hash().Hex())
	}
}
