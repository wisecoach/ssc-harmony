package ssc

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/ssc/api"
)

// 构造一个不依赖 BLS / 网络 / 真实锁状态的 detector，用于纯逻辑单测。
// 通过测试缝（verifyOverride / routeOverride / edgeValidOverride / rollbackVictim）注入行为。
func newTestDetector() *deadlockDetector {
	d := newDeadlockDetector(
		1,             // selfShard
		nil, nil, nil, // state / comm / tempLockView
		&offChainDAG{}, // offChainDAG (enabled=false by default)
		nil,            // stateLockManager
		nil,            // signerMgr
	)
	d.verifyOverride = func(*api.DeadlockProbe) error { return nil }
	d.edgeValidOverride = func(e waitEdge) bool { return true }
	return d
}

// TestDetector_LowPriWaitAlsoRecordsEdge 验证核心语义改动：
//  1. 高被低（A→B）与低被高（B→A）都会记边（补全等待图，让环能闭合）；
//  2. shouldTriggerProbe 只对"高被低"为 true（低被高不探测）。
func TestDetector_LowPriWaitAlsoRecordsEdge(t *testing.T) {
	d := newTestDetector()
	priA := api.Priority{Nonce: 1, OriginShardID: 1, TxHash: hashOf("A")} // 高
	priB := api.Priority{Nonce: 2, OriginShardID: 1, TxHash: hashOf("B")} // 低
	if !priA.Less(priB) {
		t.Fatal("test setup: A should be higher priority than B")
	}

	// 高 A 被低 B 卡（在 shard1）
	if err := d.OnBlockedAt(hashOf("A"), hashOf("B"), 1, "k1", api.LockLayerOnChain,
		priA, priB, 0, 1); err != nil {
		t.Fatalf("OnBlockedAt A->B err: %v", err)
	}
	// 低 B 被高 A 卡（在 shard1）——回程边，也要记
	if err := d.OnBlockedAt(hashOf("B"), hashOf("A"), 1, "k2", api.LockLayerOnChain,
		priB, priA, 0, 1); err != nil {
		t.Fatalf("OnBlockedAt B->A err: %v", err)
	}

	edgesA := d.getEdges(hashOf("A"))
	if len(edgesA) != 1 || edgesA[0].holder != hashOf("B") {
		t.Fatalf("expected A->B edge recorded, got %+v", edgesA)
	}
	edgesB := d.getEdges(hashOf("B"))
	if len(edgesB) != 1 || edgesB[0].holder != hashOf("A") {
		t.Fatalf("expected B->A (low-pri wait) edge recorded too, got %+v", edgesB)
	}

	// 探测门：仅高被低为 true
	if !d.shouldTriggerProbe(edgesA[0]) {
		t.Errorf("A(high)->B(low) should probe")
	}
	if d.shouldTriggerProbe(edgesB[0]) {
		t.Errorf("B(low)->A(high) should NOT probe (only record edge)")
	}
}

// TestDetector_TwoRingClosureAndVictim 验证 2-环闭环：
// 双向边都记录后，探针 A→B→A 回到 Init 即判环，victim 应为环内低优先者 B。
func TestDetector_TwoRingClosureAndVictim(t *testing.T) {
	d := newTestDetector()
	priA := api.Priority{Nonce: 1, OriginShardID: 1, TxHash: hashOf("A")} // 高
	priB := api.Priority{Nonce: 2, OriginShardID: 1, TxHash: hashOf("B")} // 低

	// 记录双向边（A->B 会触发一次自动 dispatch，先忽略）
	if err := d.OnBlockedAt(hashOf("A"), hashOf("B"), 1, "k1", api.LockLayerOnChain,
		priA, priB, 0, 1); err != nil {
		t.Fatal(err)
	}
	if err := d.OnBlockedAt(hashOf("B"), hashOf("A"), 1, "k2", api.LockLayerOnChain,
		priB, priA, 0, 1); err != nil {
		t.Fatal(err)
	}

	// 捕获被 routeOverride 转发出去的探针
	var captured []*api.DeadlockProbe
	d.routeOverride = func(p *api.DeadlockProbe) { captured = append(captured, p) }
	// 记录 victim 回滚
	var victim common.Hash
	d.rollbackVictim = func(tx common.Hash) { victim = tx }

	// 驱动：探针到达 B（Sender=Init=A，跳过首跳重验证），应沿 B->A 转发回 A
	ack1 := d.HandleProbe(&api.DeadlockProbe{
		Init: hashOf("A"), Sender: hashOf("A"), Current: hashOf("B"),
		Path: []common.Hash{hashOf("A"), hashOf("B")}, Layer: api.LockLayerOnChain,
		WaitKey: "k1", Nonce: 7, Shard: 1,
	})
	if !ack1.Accepted {
		t.Fatalf("probe at B should be accepted, ack=%+v", ack1)
	}
	if len(captured) != 1 || captured[0].Current != hashOf("A") {
		t.Fatalf("expected forward B->A (Current=A), captured=%+v", captured)
	}

	// 该转发回到 Init（Current==Init）→ 成环
	ack2 := d.HandleProbe(captured[0])
	if !ack2.Accepted {
		t.Fatalf("returned probe should be accepted (ring), ack=%+v", ack2)
	}

	if got := d.ringDetected.Load(); got != 1 {
		t.Fatalf("expected ringDetected=1, got %d", got)
	}
	if got := d.victimDied.Load(); got != 1 {
		t.Fatalf("expected victimDied=1, got %d", got)
	}
	if victim != hashOf("B") {
		t.Fatalf("expected victim B (lowest priority), got %s", victim.Hex())
	}
}

func hashOf(s string) common.Hash {
	return common.BytesToHash([]byte(s))
}
