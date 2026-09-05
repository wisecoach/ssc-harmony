package ssc

import (
	"math/big"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/ssc/api"
)

// makeTestRWSet 构造一个 WriteSet，包含指定的 (address, key) 写入。
func makeTestRWSet(addr common.Address, keys ...common.Hash) *api.RWSet {
	ws := &api.RWSet{WriteState: api.NewStateSet()}
	if ws.WriteState.State[addr] == nil {
		ws.WriteState.State[addr] = make(map[common.Hash]common.Hash)
	}
	for _, k := range keys {
		ws.WriteState.State[addr][k] = k
	}
	return ws
}

// 简化：直接构造一个确定的 hash
func testHash(i byte) common.Hash {
	return common.BigToHash(new(big.Int).SetInt64(int64(i)))
}

// newTestDAG 构造一个**已启用**的 offChainDAG。
// patchpool.go 重构后 offChainDAG 默认关闭（enabled=false），所有方法被 isOn() 短路成 no-op；
// 单测必须显式开启，否则 AddNode/MarkReady/findCoveringSet 等全部空转导致测试全挂。
func newTestDAG() *offChainDAG {
	d := &offChainDAG{}
	d.SetEnabled(true)
	return d
}

func TestOffChainDAGFindCoveringSet(t *testing.T) {
	dag := newTestDAG()
	addr := common.HexToAddress("0x1111")
	k1 := testHash(1)
	k2 := testHash(2)
	k3 := testHash(3)
	k4 := testHash(4)

	// A 写 key1,key2,key4；B 写 key2,key3；C 写 key1（更高 simNum）
	a := testHash(0xa)
	b := testHash(0xb)
	c := testHash(0xc)

	dag.AddNode(a, 1, makeTestRWSet(addr, k1, k2, k4), nil)
	dag.MarkReady(a, nil)
	dag.AddNode(b, 1, makeTestRWSet(addr, k2, k3), nil)
	dag.MarkReady(b, nil)
	dag.AddNode(c, 2, makeTestRWSet(addr, k1), nil)
	dag.MarkReady(c, nil)

	// 1) 覆盖全部 key：应能选出 A+B（或 C+B），且数量合理
	set := dag.findCoveringSet([]api.LockKey{api.FormKey(addr, k1), api.FormKey(addr, k2), api.FormKey(addr, k3)}, common.Hash{}, 10)
	if len(set) == 0 {
		t.Fatalf("expected a covering set for all keys, got none")
	}
	covered := map[common.Hash]bool{}
	for _, n := range set {
		covered[n.TxHash] = true
	}
	if !covered[a] || !covered[b] {
		t.Fatalf("expected set to cover key1..3 via A+B, got %v", covered)
	}

	// 2) 排除自己：仅 A 写 key4，排除 A → 应无覆盖
	only := dag.findCoveringSet([]api.LockKey{api.FormKey(addr, k4)}, a, 10)
	if len(only) != 0 {
		t.Fatalf("expected no covering patch when excluding self (A), got %d", len(only))
	}

	// 3) 排除自己后可被其他节点覆盖（key1 由 A 与 C 写，排除 A 后剩 C）
	withC := dag.findCoveringSet([]api.LockKey{api.FormKey(addr, k1)}, a, 10)
	if len(withC) != 1 || withC[0].TxHash != c {
		t.Fatalf("expected C to cover key1 when A excluded, got %+v", withC)
	}

	// 4) 严格更早：maxSimNum=2 只允许 simNum<2 的节点 → C(simNum=2) 不可选，只剩 A
	strict := dag.findCoveringSet([]api.LockKey{api.FormKey(addr, k1)}, common.Hash{}, 2)
	if len(strict) != 1 || strict[0].TxHash != a {
		t.Fatalf("expected A to cover key1 under strict maxSimNum=2, got %+v", strict)
	}
	for _, n := range strict {
		if n.SimulationNum >= 2 {
			t.Fatalf("strict ordering violated: node simNum=%d >= maxSimNum=2", n.SimulationNum)
		}
	}
	// 同轮次（== maxSimNum）不允许：阻断同一轮内无限 chaining（链深有界）
	same := dag.findCoveringSet([]api.LockKey{api.FormKey(addr, k1)}, common.Hash{}, 2)
	for _, n := range same {
		if n.TxHash == c {
			t.Fatalf("same-round node C(simNum=2) must NOT be eligible when maxSimNum=2")
		}
	}
}
func TestOffChainDAGRemoveCleansAll(t *testing.T) {
	dag := newTestDAG()
	addr := common.HexToAddress("0x2222")
	k1 := testHash(1)
	k2 := testHash(2)
	tx := testHash(0x10)
	retryTx := testHash(0x20)

	dag.AddNode(tx, 1, makeTestRWSet(addr, k1, k2), nil)
	dag.MarkReady(tx, nil)

	// 该节点同时订阅了 key（作为 retryTx 场景）
	dag.subscribeRetryTx(retryTx, []api.LockKey{api.FormKey(addr, k1)}, []api.LockKey{api.FormKey(addr, k2)})

	if dag.patchCount() != 1 {
		t.Fatalf("expected 1 node before remove")
	}
	if dag.keyCount() == 0 {
		t.Fatalf("expected keyIndex populated after MarkReady")
	}
	if dag.subscriberCount() == 0 || dag.txSubKeyCount() == 0 {
		t.Fatalf("expected subscriber/txSubKeys populated")
	}

	dag.Remove(tx)

	if dag.patchCount() != 0 {
		t.Fatalf("expected nodes empty after Remove, got %d", dag.patchCount())
	}
	if dag.keyCount() != 0 {
		t.Fatalf("expected keyIndex empty after Remove, got %d", dag.keyCount())
	}

	// Remove 应同时清理该 retryTx 的订阅
	dag.Remove(retryTx)
	if dag.subscriberCount() != 0 {
		t.Fatalf("expected subscriber empty after Remove, got %d", dag.subscriberCount())
	}
	if dag.txSubKeyCount() != 0 {
		t.Fatalf("expected txSubKeys empty after Remove, got %d", dag.txSubKeyCount())
	}
}

func TestOffChainDAGMultiUpstreamConsume(t *testing.T) {
	dag := newTestDAG()
	addr := common.HexToAddress("0x3333")
	k1 := testHash(1)
	k2 := testHash(2)
	a := testHash(0xa1)
	b := testHash(0xa2)
	retry := testHash(0xb1)

	dag.AddNode(a, 1, makeTestRWSet(addr, k1), nil)
	dag.MarkReady(a, nil)
	dag.AddNode(b, 1, makeTestRWSet(addr, k2), nil)
	dag.MarkReady(b, nil)

	patches := dag.findCoveringSet([]api.LockKey{api.FormKey(addr, k1), api.FormKey(addr, k2)}, retry, 10)
	if len(patches) != 2 {
		t.Fatalf("expected 2 covering patches, got %d", len(patches))
	}

	prio := api.Priority{Nonce: 1, TxHash: retry}
	var consumed []common.Hash
	var merged *api.RWSet
	for _, n := range patches {
		p := dag.tryConsumePatch(n.TxHash, retry, prio)
		if p == nil {
			t.Fatalf("tryConsumePatch failed for %s", n.TxHash.Hex())
		}
		consumed = append(consumed, n.TxHash)
		merged = mergeRWSet(p, merged)
	}
	if len(merged.WriteState.State[addr]) != 2 {
		t.Fatalf("expected merged WriteSet to cover k1,k2, got %d keys", len(merged.WriteState.State[addr]))
	}
	for _, n := range patches {
		if dag.isPatchFinalized(n.TxHash) || dag.isConsumed(n.TxHash) == false {
			t.Fatalf("expected patch %s to be Consumed", n.TxHash.Hex())
		}
	}
	// release 后恢复 Free
	for _, txh := range consumed {
		dag.releasePatch(txh)
	}
	for _, n := range patches {
		if dag.isConsumed(n.TxHash) {
			t.Fatalf("expected patch %s to be Free after release", n.TxHash.Hex())
		}
	}
}

func TestOffChainDAGMarkReadyTriggersScan(t *testing.T) {
	dag := newTestDAG()
	addr := common.HexToAddress("0x4444")
	k1 := testHash(1)
	tx := testHash(0x55)

	// MarkReady 必须触发 onReady 回调（对应 impl.go 里对 scanPatchSubscribers 的触发），
	// 否则 SimTx 提交后不会增量扫描 subscriber，DAG chaining 失效 → retryTx 无法被及时救援 → 超时增多。
	var got *api.RWSet
	dag.AddNode(tx, 1, makeTestRWSet(addr, k1), nil)
	dag.MarkReady(tx, func(ws *api.RWSet) { got = ws })
	if got == nil {
		t.Fatal("expected MarkReady to invoke onReady callback (DAG chaining scan trigger)")
	}
	if _, ok := got.WriteState.State[addr][k1]; !ok {
		t.Fatalf("onReady received wrong writeSet: missing k1")
	}
}

// ── 辅助计数（测试用） ───────────────────────────────────────────────

func (dag *offChainDAG) patchCount() int {
	n := 0
	dag.nodes.Range(func(_, _ interface{}) bool { n++; return true })
	return n
}

func (dag *offChainDAG) keyCount() int {
	n := 0
	dag.keyIndex.Range(func(_, _ interface{}) bool { n++; return true })
	return n
}

func (dag *offChainDAG) subscriberCount() int {
	n := 0
	dag.subscriber.Range(func(_, _ interface{}) bool { n++; return true })
	return n
}

func (dag *offChainDAG) txSubKeyCount() int {
	n := 0
	dag.txSubKeys.Range(func(_, _ interface{}) bool { n++; return true })
	return n
}

func (dag *offChainDAG) isConsumed(txHash common.Hash) bool {
	nodeVal, ok := dag.nodes.Load(txHash)
	if !ok {
		return false
	}
	return nodeVal.(*OffChainPatchNode).Status() == PatchConsumed
}

// ─── DSN-50: 覆盖完整性 + 链深上限 ─────────────────────────────────

func TestOffChainDAGDepthComputation(t *testing.T) {
	dag := newTestDAG()
	addr := common.HexToAddress("0x5555")
	k1 := testHash(1)
	a := testHash(0xa1)
	b := testHash(0xa2)
	c := testHash(0xa3)

	dag.AddNode(a, 1, makeTestRWSet(addr, k1), nil) // 根
	if d := nodeDepth(dag, a); d != 1 {
		t.Fatalf("expected root depth 1, got %d", d)
	}
	dag.AddNode(b, 2, makeTestRWSet(addr, k1), []api.TxSimKey{{TxHash: a, SimulationNum: 1}}) // 依赖 a → depth 2
	if d := nodeDepth(dag, b); d != 2 {
		t.Fatalf("expected depth 2, got %d", d)
	}
	// c 依赖 a(depth1) 和 b(depth2) → depth 3
	dag.AddNode(c, 3, makeTestRWSet(addr, k1), []api.TxSimKey{{TxHash: a, SimulationNum: 1}, {TxHash: b, SimulationNum: 2}})
	if d := nodeDepth(dag, c); d != 3 {
		t.Fatalf("expected depth 3, got %d", d)
	}
}

func TestOffChainDAGIsFullyCovered(t *testing.T) {
	dag := newTestDAG()
	addr := common.HexToAddress("0x6666")
	k1 := testHash(1)
	k2 := testHash(2)
	p1 := testHash(0xb1) // 写 k1
	p2 := testHash(0xb2) // 写 k2

	patch1 := &OffChainPatchNode{TxHash: p1, Patch: makeTestRWSet(addr, k1), Depth: 1}
	patch2 := &OffChainPatchNode{TxHash: p2, Patch: makeTestRWSet(addr, k2), Depth: 1}

	// 完整覆盖
	if !dag.isFullyCovered([]api.LockKey{api.FormKey(addr, k1), api.FormKey(addr, k2)}, []*OffChainPatchNode{patch1, patch2}) {
		t.Fatal("expected fully covered with p1+p2")
	}
	// 只覆盖一个 → 不完整
	if dag.isFullyCovered([]api.LockKey{api.FormKey(addr, k1), api.FormKey(addr, k2)}, []*OffChainPatchNode{patch1}) {
		t.Fatal("expected NOT fully covered with only p1")
	}
	// 空 needKeys → true（无需求）
	if !dag.isFullyCovered(nil, []*OffChainPatchNode{patch1}) {
		t.Fatal("expected fully covered for empty needKeys")
	}
}

func TestOffChainDAGChainDepthOf(t *testing.T) {
	if d := chainDepthOf(nil); d != 1 {
		t.Fatalf("expected chainDepthOf(nil)==1, got %d", d)
	}
	p1 := &OffChainPatchNode{Depth: 1}
	p3 := &OffChainPatchNode{Depth: 3}
	if d := chainDepthOf([]*OffChainPatchNode{p1}); d != 2 {
		t.Fatalf("expected chainDepthOf([depth1])==2, got %d", d)
	}
	if d := chainDepthOf([]*OffChainPatchNode{p1, p3}); d != 4 {
		t.Fatalf("expected chainDepthOf([1,3])==4, got %d", d)
	}
}

func TestDefaultMaxChainDepth(t *testing.T) {
	if d := defaultMaxChainDepth(0); d != defaultMaxChainDepthValue {
		t.Fatalf("expected default depth %d for 0, got %d", defaultMaxChainDepthValue, d)
	}
	if d := defaultMaxChainDepth(8); d != 8 {
		t.Fatalf("expected configured depth 8, got %d", d)
	}
}

// nodeDepth 读取节点 Depth（测试用）。
func nodeDepth(dag *offChainDAG, txHash common.Hash) int {
	nv, ok := dag.nodes.Load(txHash)
	if !ok {
		return -1
	}
	return nv.(*OffChainPatchNode).Depth
}

// TestOffChainDAGBuildSimPatchSubgraph — DSN-54：验证从 offChainDAG 构建
// “被消费上游 + 传递闭包” 子图，且成员侧 ReadSimDAGPatch 能反查到任意上游写集。
func TestOffChainDAGBuildSimPatchSubgraph(t *testing.T) {
	dag := newTestDAG()
	addr := common.HexToAddress("0xab01")
	k1, k2, k3 := testHash(11), testHash(12), testHash(13)
	a, b, c := testHash(0xa), testHash(0xb), testHash(0xc)

	// A 写 k1（根）；B 写 k2，上游 A；C 写 k3，上游 B。
	dag.AddNode(a, 1, makeTestRWSet(addr, k1), nil)
	dag.AddNode(b, 1, makeTestRWSet(addr, k2), []api.TxSimKey{{TxHash: a, SimulationNum: 1}})
	dag.AddNode(c, 1, makeTestRWSet(addr, k3), []api.TxSimKey{{TxHash: b, SimulationNum: 1}})

	// target=C 直接消费上游 B → 子图应含 C、B、A（传递闭包）
	target := testHash(0xcc)
	roots := []api.TxSimKey{{TxHash: c, SimulationNum: 1}}
	sub := dag.buildSimPatchSubgraph(target, 2, roots)
	if sub == nil {
		t.Fatalf("expected subgraph, got nil")
	}
	if sub.TxHash != target || sub.SimulationNum != 2 {
		t.Fatalf("subgraph identity wrong: %+v", sub)
	}
	got := map[api.TxSimKey]bool{}
	for _, n := range sub.Nodes {
		got[n.TxSim] = true
	}
	if !got[api.TxSimKey{TxHash: c, SimulationNum: 1}] ||
		!got[api.TxSimKey{TxHash: b, SimulationNum: 1}] ||
		!got[api.TxSimKey{TxHash: a, SimulationNum: 1}] {
		t.Fatalf("expected transitive closure C,B,A, got %v", got)
	}
	if len(sub.Nodes) != 3 {
		t.Fatalf("expected exactly 3 nodes (closure), got %d", len(sub.Nodes))
	}

	// 成员侧反查：能查到传递上游 A 写的 k1（成员 rs 与 leader dag 解耦，只存子图）
	rs := &retryScheduler{offChainDAG: offChainDAG{}, simDAGPatches: sync.Map{}}
	rs.offChainDAG.SetEnabled(true)
	rs.StoreSimDAGPatch(sub)
	if val, found := rs.ReadSimDAGPatch(target, 2, addr, k1); !found || val != k1 {
		t.Fatalf("expected k1 from transitive upstream A, found=%v val=%s", found, val.Hex())
	}
	if val, found := rs.ReadSimDAGPatch(target, 2, addr, k3); !found || val != k3 {
		t.Fatalf("expected k3 from direct consumed C, found=%v", found)
	}
	if _, found := rs.ReadSimDAGPatch(target, 2, addr, testHash(99)); found {
		t.Fatalf("expected miss for unwritten key")
	}
	// 错误的 (tx,simNum) 不应命中
	if _, found := rs.ReadSimDAGPatch(target, 3, addr, k1); found {
		t.Fatalf("expected miss for different simulationNum")
	}
}

// TestSimDAGPatchOverwriteRoundtrip — DSN-54：StoreSimDAGPatch 按 (tx,simNum) 幂等覆盖，
// RemoveSimDAGPatches 整组清。
func TestSimDAGPatchOverwriteRoundtrip(t *testing.T) {
	rs := &retryScheduler{offChainDAG: offChainDAG{}, simDAGPatches: sync.Map{}}
	rs.offChainDAG.SetEnabled(true)
	addr := common.HexToAddress("0xbeef")
	k1 := testHash(21)
	tx := testHash(0x55)

	sub1 := &api.SimPatchSubgraph{TxHash: tx, SimulationNum: 1,
		Nodes: []*api.SimPatchNode{{TxSim: api.TxSimKey{TxHash: testHash(0x1), SimulationNum: 1},
			Writes: makeTestRWSet(addr, k1)}}}
	rs.StoreSimDAGPatch(sub1)
	if val, found := rs.ReadSimDAGPatch(tx, 1, addr, k1); !found || val != k1 {
		t.Fatalf("expected round 1 value, found=%v", found)
	}

	// 更高 simNum：新增条目，不影响旧轮读取（无跨轮脏读）
	k2 := testHash(22)
	sub2 := &api.SimPatchSubgraph{TxHash: tx, SimulationNum: 2,
		Nodes: []*api.SimPatchNode{{TxSim: api.TxSimKey{TxHash: testHash(0x2), SimulationNum: 1},
			Writes: makeTestRWSet(addr, k2)}}}
	rs.StoreSimDAGPatch(sub2)
	if _, found := rs.ReadSimDAGPatch(tx, 1, addr, k2); found {
		t.Fatalf("round 1 must not see round 2 writes")
	}
	if _, found := rs.ReadSimDAGPatch(tx, 2, addr, k1); found {
		t.Fatalf("round 2 must not see round 1 writes")
	}

	// RemoveSimDAGPatches 整组清
	rs.RemoveSimDAGPatches(tx)
	if _, found := rs.ReadSimDAGPatch(tx, 1, addr, k1); found {
		t.Fatalf("expected round 1 cleared")
	}
	if _, found := rs.ReadSimDAGPatch(tx, 2, addr, k2); found {
		t.Fatalf("expected round 2 cleared")
	}
}
