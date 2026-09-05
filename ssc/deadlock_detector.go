package ssc

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
)

// LockLayer type is in api package.

// waitEdge 是 DSN-53 的“确认被卡”等待图边：waiter 在 shard 被 holder 卡。
// 记边不区分优先级方向（完整 WFG，含低被高卡的“回程”边）；是否发起探针找 victim
// 由 shouldTriggerProbe 单独决定（仅高优先被低优先链上/已 Finalized 锁阻塞才探测）。
type waitEdge struct {
	waiter    common.Hash
	holder    common.Hash
	shard     uint32
	key       api.LockKey
	layer     api.LockLayer
	waiterPri api.Priority
	holderPri api.Priority
	expireAt  uint64 // 可选防 stale，默认 0（关闭）
}

type probeSeenKey struct {
	init  common.Hash
	nonce uint64
}

// deadlockDetector 是独立的反向 CMH 探针组件。
// waitEdges 只服务于探针，不参与上锁/调度。
type deadlockDetector struct {
	waitEdges   sync.Map // waiter common.Hash -> []waitEdge（持久边，真实链上锁）
	waitEdgesMu sync.Mutex

	seenProbes sync.Map // probeSeenKey -> struct{}

	selfShard        uint32
	state            *RetrySchedulerStateAccessor
	comm             *Comm
	tempLockView     *TempLockView
	offChainDAG      *offChainDAG
	stateLockManager *stateLockManager
	// pendingHolder 提供同块未提交(pendingStates)链上锁持有者查询，供 edgeValid 重验证用。
	// 由 retryScheduler 构造时注入（指向缓存 stateDB 的 FindPendingLockHolder）；可空。
	pendingHolder  func(key api.LockKey) (common.Hash, bool)
	signerMgr      api.BLSSignerMgr
	rollbackVictim func(txHash common.Hash)
	// victimTxSubmit 是 VictimTx 方案的回调：给定 victim 与判环的完整签名证据链，
	// 由最后一跳 shard 构造 VictimTx 打进本分片。优先于 rollbackVictim 使用。
	victimTxSubmit func(victim common.Hash, proof []*api.DeadlockProbe)

	currentBlock atomic.Uint64
	currentEpoch atomic.Uint64
	nonceCounter atomic.Uint64

	probeSent    atomic.Int64
	probeDropped atomic.Int64
	probeInvalid atomic.Int64
	ringDetected atomic.Int64
	victimDied   atomic.Int64

	// 单测缝（避免在单元测试里搭真实 BLS 验签 / 锁状态 / 跨 shard RPC）。
	// 非 nil 时优先于 signerMgr / stateLockManager / tempLockView / routeProbe 使用。
	verifyOverride    func(p *api.DeadlockProbe) error
	routeOverride     func(p *api.DeadlockProbe)
	edgeValidOverride func(e waitEdge) bool
}

// DeadlockDetectorStatus 是监控计数快照。
type DeadlockDetectorStatus struct {
	WaitEdges       int
	SeenProbes      int
	ProbeSent       int64
	ProbeDropped    int64
	ProbeInvalidSig int64
	RingDetected    int64
	VictimDied      int64
}

func newDeadlockDetector(
	selfShard uint32,
	state *RetrySchedulerStateAccessor,
	comm *Comm,
	tempLockView *TempLockView,
	dag *offChainDAG,
	slm *stateLockManager,
	signerMgr api.BLSSignerMgr,
) *deadlockDetector {
	return &deadlockDetector{
		selfShard:        selfShard,
		state:            state,
		comm:             comm,
		tempLockView:     tempLockView,
		offChainDAG:      dag,
		stateLockManager: slm,
		signerMgr:        signerMgr,
	}
}

// OnBlocked 记录一条等待边并触发探针（默认使用当前 block/epoch）。
func (d *deadlockDetector) OnBlocked(
	waiter, holder common.Hash,
	shard uint32,
	key api.LockKey,
	layer api.LockLayer,
	waiterPri, holderPri api.Priority,
) error {
	return d.OnBlockedAt(waiter, holder, shard, key, layer, waiterPri, holderPri,
		api.Epoch(d.currentEpoch.Load()), d.currentBlock.Load())
}

// OnBlockedAt 与 OnBlocked 相同，但允许调用方显式传入 epoch/block（用于防重放）。
func (d *deadlockDetector) OnBlockedAt(
	waiter, holder common.Hash,
	shard uint32,
	key api.LockKey,
	layer api.LockLayer,
	waiterPri, holderPri api.Priority,
	epoch api.Epoch,
	blockNum uint64,
) error {
	// 记边 = 描述"waiter 在 shard 被 holder 卡住"这一事实，不区分优先级方向。
	// 只有非空、非自环、且 key 未被 DAG patch 覆盖的才记（覆盖不算阻塞边）。
	if waiter == (common.Hash{}) || holder == (common.Hash{}) || waiter == holder {
		return nil
	}
	if d.isKeyPatchCovered(key, waiter) {
		return nil
	}
	edge := waitEdge{
		waiter:    waiter,
		holder:    holder,
		shard:     shard,
		key:       key,
		layer:     layer,
		waiterPri: waiterPri,
		holderPri: holderPri,
	}
	if !d.addWaitEdge(edge) {
		return nil
	}
	if l := utils.SSCLogger(); l != nil {
		l.Debug().
			Str("waiter", waiter.Hex()).
			Str("holder", holder.Hex()).
			Uint32("shard", shard).
			Str("key", string(key)).
			Str("layer", layer.String()).
			Msg("[deadlockDetector] waitEdge recorded")
	}
	// 探测（找 victim）= 只有"高优先被低优先卡"才需要。低被高卡只补图、不探测。
	if !d.shouldTriggerProbe(edge) {
		return nil
	}
	return d.triggerProbe(waiter, waiter, holder, shard, key, layer, epoch, blockNum)
}

// shouldTriggerProbe 只决定"是否发起探针找 victim"，不再决定是否记边。
// 记边由 OnBlockedAt 无条件（去掉 patch 覆盖/方向）完成；这里仅在
// "高优先 A 被更低优先 B 的不可 wound 锁（链上 / TLV 已 Finalized）卡住"时返回 true。
// 低优先被高优先卡、或优先级未知(holderPri 为零) 的边，只用于补全 WFG，不探测。
func (d *deadlockDetector) shouldTriggerProbe(e waitEdge) bool {
	if e.waiter == e.holder || e.holderPri == (api.Priority{}) {
		return false
	}
	// waiter 必须严格高于 holder（waiterPri.Less(holderPri)）
	if !e.waiterPri.Less(e.holderPri) {
		return false
	}
	switch e.layer {
	case api.LockLayerOnChain:
		return true
	case api.LockLayerTLV:
		return d.offChainDAG != nil && d.offChainDAG.isPatchFinalized(e.holder)
	default:
		return false
	}
}

// isKeyPatchCovered 判断该 key 是否已被 DAG patch 覆盖（覆盖则不算阻塞边）。
func (d *deadlockDetector) isKeyPatchCovered(key api.LockKey, exclude common.Hash) bool {
	if d.offChainDAG == nil {
		return false
	}
	patches := d.offChainDAG.findCoveringSet([]api.LockKey{key}, exclude, 0)
	return len(patches) > 0 && d.offChainDAG.isFullyCovered([]api.LockKey{key}, patches)
}

// addWaitEdge 去重后记录一条等待边。
func (d *deadlockDetector) addWaitEdge(edge waitEdge) bool {
	d.waitEdgesMu.Lock()
	defer d.waitEdgesMu.Unlock()

	val, _ := d.waitEdges.Load(edge.waiter)
	edges, _ := val.([]waitEdge)
	for _, e := range edges {
		if e.holder == edge.holder && e.shard == edge.shard && e.key == edge.key && e.layer == edge.layer {
			return false
		}
	}
	edges = append(edges, edge)
	d.waitEdges.Store(edge.waiter, edges)
	return true
}

// getEdges 返回某 waiter 的等待边快照。
func (d *deadlockDetector) getEdges(waiter common.Hash) []waitEdge {
	val, ok := d.waitEdges.Load(waiter)
	if !ok {
		return nil
	}
	edges, _ := val.([]waitEdge)
	out := make([]waitEdge, len(edges))
	copy(out, edges)
	return out
}

func (d *deadlockDetector) hasLocalWaitEdge(waiter, holder common.Hash, key api.LockKey, layer api.LockLayer) bool {
	for _, e := range d.getEdges(waiter) {
		if e.holder == holder && e.key == key && e.layer == layer {
			return true
		}
	}
	return false
}

// CleanupHolder 清理所有“holder==txHash”的边（以及 txHash 作为 waiter 的边）。
// 在 holder 的 CRTx commit/die 上链时由各分片调用。
func (d *deadlockDetector) CleanupHolder(txHash common.Hash) {
	if txHash == (common.Hash{}) {
		return
	}
	d.waitEdgesMu.Lock()
	defer d.waitEdgesMu.Unlock()

	var toDelete []common.Hash
	d.waitEdges.Range(func(key, val interface{}) bool {
		waiter := key.(common.Hash)
		edges, _ := val.([]waitEdge)
		if waiter == txHash {
			toDelete = append(toDelete, waiter)
			return true
		}
		kept := edges[:0]
		for _, e := range edges {
			if e.holder != txHash {
				kept = append(kept, e)
			}
		}
		if len(kept) == 0 {
			toDelete = append(toDelete, waiter)
		} else {
			d.waitEdges.Store(waiter, kept)
		}
		return true
	})
	for _, waiter := range toDelete {
		d.waitEdges.Delete(waiter)
	}
	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Msg("[deadlockDetector] CleanupHolder done")
}

// OnBlockCommitted 挂在区块提交：见 CRTx 即 CleanupHolder，实现跨分片清理。
func (d *deadlockDetector) OnBlockCommitted(block *types.Block) {
	if block == nil {
		return
	}
	d.currentBlock.Store(block.NumberU64())
	ssc := block.SSCTransactions()
	if len(ssc) <= int(types.InternalTxTypeCRTx) {
		return
	}
	for _, tx := range ssc[types.InternalTxTypeCRTx] {
		if tx == nil {
			continue
		}
		txHash, ok := d.tempLockView.crTxOriginalHash(tx)
		if ok && txHash != (common.Hash{}) {
			d.CleanupHolder(txHash)
		}
	}
}

// HandleProbe 处理收到的反向 CMH 探针。
func (d *deadlockDetector) HandleProbe(p *api.DeadlockProbe) *api.DeadlockProbeAck {
	if p == nil {
		return &api.DeadlockProbeAck{Accepted: false}
	}
	// 1) BLS 验证（单测可用 verifyOverride 代替真实验签）
	if d.verifyOverride != nil {
		if err := d.verifyOverride(p); err != nil {
			d.probeInvalid.Add(1)
			d.probeDropped.Add(1)
			utils.SSCLogger().Warn().Err(err).
				Str("init", p.Init.Hex()).
				Str("current", p.Current.Hex()).
				Msg("[deadlockDetector] probe invalid (override) signature, dropped")
			return &api.DeadlockProbeAck{Init: p.Init, Current: p.Current, Shard: p.Shard, Accepted: false}
		}
	} else if d.signerMgr == nil || d.signerMgr.GetSSCSigner() == nil {
		d.probeDropped.Add(1)
		return &api.DeadlockProbeAck{Init: p.Init, Current: p.Current, Shard: p.Shard, Accepted: false}
	} else if err := d.signerMgr.GetSSCSigner().Verify(p); err != nil {
		d.probeInvalid.Add(1)
		d.probeDropped.Add(1)
		utils.SSCLogger().Warn().Err(err).
			Str("init", p.Init.Hex()).
			Str("current", p.Current.Hex()).
			Msg("[deadlockDetector] probe invalid BLS signature, dropped")
		return &api.DeadlockProbeAck{Init: p.Init, Current: p.Current, Shard: p.Shard, Accepted: false}
	}
	// 3) 成环：Current == Init
	if p.Current == p.Init {
		d.ringDetected.Add(1)
		if l := utils.SSCLogger(); l != nil {
			l.Info().
				Str("init", p.Init.Hex()).
				Int("pathLen", len(p.Path)).
				Msg("[deadlockDetector] deadlock ring detected")
		}
		d.resolveRing(p)
		return &api.DeadlockProbeAck{Init: p.Init, Current: p.Current, Shard: p.Shard, Accepted: true}
	}

	// 2) 防重放/去重
	key := probeSeenKey{init: p.Init, nonce: p.Nonce}
	if _, loaded := d.seenProbes.LoadOrStore(key, struct{}{}); loaded {
		d.probeDropped.Add(1)
		return &api.DeadlockProbeAck{Init: p.Init, Current: p.Current, Shard: p.Shard, Accepted: false}
	}

	// 3) 重验证上一跳（Sender→Current）仍成立：只有本地记录过该边的分片才验证
	//（多播到 holder 的其余 RelatedShards 时，该分片不一定有这条 A→B 的局部边）。
	// 首跳（Sender==Init）不重验证：该边由本分片刚在 OnBlockedAt 里记下（同一调用栈），
	// 立刻用仅-global 的 edgeValid 重验会误判 stale（pending/TLV holder 不在 global）→ 自掐。
	// 从第二跳起（Sender!=Init），边是历史记录，才做当前锁事实重验证。
	if p.WaitKey != "" && p.Sender != p.Init && d.hasLocalWaitEdge(p.Sender, p.Current, p.WaitKey, p.Layer) {
		prev := waitEdge{
			waiter: p.Sender,
			holder: p.Current,
			shard:  p.Shard,
			key:    p.WaitKey,
			layer:  p.Layer,
		}
		if !d.edgeValid(prev) {
			d.probeDropped.Add(1)
			utils.SSCLogger().Debug().
				Str("init", p.Init.Hex()).
				Str("sender", p.Sender.Hex()).
				Str("current", p.Current.Hex()).
				Msg("[deadlockDetector] previous waitEdge stale, dropped")
			return &api.DeadlockProbeAck{Init: p.Init, Current: p.Current, Shard: p.Shard, Accepted: false}
		}
	}
	// 4) 查 Current 的出边
	edges := d.getEdges(p.Current)
	forwarded := false
	for _, edge := range edges {
		if !d.edgeValid(edge) {
			continue
		}
		// 注意：探针遍历完整 WFG（含"低被高"回程边）以回到 Init 成环。
		// "高优先被低优先"的限制只属于【触发/发起探测】（shouldTriggerProbe），
		// 不应阻止探针沿回程边走回发起者，否则 2-环永远闭合不了。
		next := &api.DeadlockProbe{
			Init:     p.Init,
			Sender:   p.Current,
			Current:  edge.holder,
			Path:     append(append([]common.Hash{}, p.Path...), edge.holder),
			Layer:    edge.layer,
			WaitKey:  edge.key,
			Epoch:    p.Epoch,
			BlockNum: p.BlockNum,
			Nonce:    p.Nonce,
			Shard:    d.selfShard,
			// Proof：把“本跳已收到并验签通过的探针 p”追加进累积证据链，
			// 这样最后一跳判环时手里握有沿途每一跳的签名探针。
			Proof: accumulateProbeProof(p),
		}
		if err := d.signProbe(next); err != nil {
			utils.SSCLogger().Error().Err(err).
				Str("init", p.Init.Hex()).
				Str("current", next.Current.Hex()).
				Msg("[deadlockDetector] forward sign failed, dropped")
			continue
		}
		d.probeSent.Add(1)
		d.dispatch(next)
		forwarded = true
	}
	if !forwarded {
		d.probeDropped.Add(1)
		utils.SSCLogger().Debug().
			Str("init", p.Init.Hex()).
			Str("current", p.Current.Hex()).
			Msg("[deadlockDetector] no valid outgoing waitEdge, dropped")
	}
	return &api.DeadlockProbeAck{Init: p.Init, Current: p.Current, Shard: p.Shard, Accepted: true}
}

// edgeValid 用当前 SLM/TLV/DAG 事实重验证一条边仍成立。
func (d *deadlockDetector) edgeValid(e waitEdge) bool {
	if d.edgeValidOverride != nil {
		return d.edgeValidOverride(e)
	}
	if e.waiter == (common.Hash{}) || e.holder == (common.Hash{}) {
		return false
	}
	if d.isKeyPatchCovered(e.key, e.waiter) {
		return false
	}
	if d.tempLockView != nil && d.tempLockView.IsWounded(e.waiter) {
		return false
	}
	switch e.layer {
	case api.LockLayerOnChain:
		if d.stateLockManager == nil && d.pendingHolder == nil {
			return false
		}
		if d.stateLockManager != nil {
			if meta, ok := d.stateLockManager.GetLockHolderMeta(e.key); ok && meta.TxHash == e.holder {
				return true
			}
			if h, ok := d.stateLockManager.GetRLockHolder(e.key); ok && h == e.holder {
				return true
			}
		}
		// pendingStates 与 global 等价(都是链上锁)：global 查不到时回退 pending。
		if d.pendingHolder != nil {
			if h, ok := d.pendingHolder(e.key); ok && h == e.holder {
				return true
			}
		}
		return false
	case api.LockLayerTLV:
		if d.tempLockView == nil || d.offChainDAG == nil {
			return false
		}
		h, _, ok := d.tempLockView.GetTempLockHolder(e.key)
		return ok && h == e.holder && d.offChainDAG.isPatchFinalized(e.holder)
	default:
		return false
	}
}

// stripProbeProof 返回 p 的一份浅拷贝且 Proof=nil。用于把某一跳探针“扁平化”地
// 存进证据链：既避免递归嵌套（每个 hop 不再带自己的 Proof），也避免与 p 共享可变切片。
func stripProbeProof(p *api.DeadlockProbe) *api.DeadlockProbe {
	if p == nil {
		return nil
	}
	c := *p
	c.Proof = nil
	return &c
}

// accumulateProbeProof 返回一条扁平化证据链 = 已累积的 hop（p.Proof）+ 本跳 p。
// 分配新切片，不改动 p.Proof；其中每个 hop 均不带各自 Proof。
func accumulateProbeProof(p *api.DeadlockProbe) []*api.DeadlockProbe {
	if p == nil {
		return nil
	}
	chain := make([]*api.DeadlockProbe, 0, len(p.Proof)+1)
	chain = append(chain, p.Proof...)
	chain = append(chain, stripProbeProof(p))
	return chain
}

// fullProbeProof 在判环收口（p.Current==Init）时返回完整证据链 = 沿途每一跳 +
// 最后一跳闭环的签名探针。作为 VictimTx 的“跨分片探测证明”。
func fullProbeProof(p *api.DeadlockProbe) []*api.DeadlockProbe {
	return accumulateProbeProof(p)
}

// resolveRing 从 Path 选环内最低优先级 victim，并触发终局 Rollback。
func (d *deadlockDetector) resolveRing(p *api.DeadlockProbe) {
	members := d.uniquePath(p.Path, p.Init)
	if len(members) == 0 {
		return
	}
	victim := d.lowestPriority(members)
	if victim == (common.Hash{}) {
		utils.SSCLogger().Warn().
			Str("init", p.Init.Hex()).
			Msg("[deadlockDetector] ring found but no victim priority available")
		return
	}
	d.victimDied.Add(1)
	if l := utils.SSCLogger(); l != nil {
		l.Warn().
			Str("victim", victim.Hex()).
			Str("init", p.Init.Hex()).
			Int("pathLen", len(p.Path)).
			Int("proofLen", len(fullProbeProof(p))).
			Msg("[deadlockDetector] ring victim chosen, submitting VictimTx")
	}
	// VictimTx 方案：由最后一跳 leader 构造 VictimTx 打进本分片（携带完整签名证据链），
	// 让本分片每个 validator 各自投 rollback 票。若未接上 VictimTx 回调（如纯单测），
	// 回退到旧的 rollbackVictim 直投路径以保持测试语义。
	if d.victimTxSubmit != nil {
		d.victimTxSubmit(victim, fullProbeProof(p))
		return
	}
	if d.rollbackVictim != nil {
		d.rollbackVictim(victim)
	}
}

func (d *deadlockDetector) uniquePath(path []common.Hash, init common.Hash) []common.Hash {
	seen := make(map[common.Hash]struct{})
	out := make([]common.Hash, 0, len(path)+1)
	for _, h := range path {
		if h == (common.Hash{}) {
			continue
		}
		if _, ok := seen[h]; ok {
			continue
		}
		seen[h] = struct{}{}
		out = append(out, h)
	}
	if init != (common.Hash{}) {
		if _, ok := seen[init]; !ok {
			out = append(out, init)
		}
	}
	return out
}

// lowestPriority 返回全局全序最低者（即优先级最低/数值最大的交易）。
func (d *deadlockDetector) lowestPriority(members []common.Hash) common.Hash {
	var best common.Hash
	for _, m := range members {
		pri, ok := d.priorityOf(m)
		if !ok {
			continue
		}
		if best == (common.Hash{}) {
			best = m
			continue
		}
		bestPri, _ := d.priorityOf(best)
		// best 更高（bestPri.Less(pri)）→ m 更低，选 m
		if bestPri.Less(pri) {
			best = m
		}
	}
	return best
}

func (d *deadlockDetector) priorityOf(txHash common.Hash) (api.Priority, bool) {
	if d.stateLockManager != nil {
		if p, ok := d.stateLockManager.GetTxPriority(txHash); ok {
			return p, true
		}
	}
	// 从本地 waitEdge 兜底
	for _, e := range d.getEdges(txHash) {
		if e.waiter == txHash {
			return e.waiterPri, true
		}
		if e.holder == txHash {
			return e.holderPri, true
		}
	}
	if d.tempLockView != nil {
		// TLV 中没有全局 tx→priority 索引，这里不再扫描（保持低开销）
	}
	return api.Priority{}, false
}

// triggerProbe 创建探针、收集本分片 BLS 背书并路由。
func (d *deadlockDetector) triggerProbe(
	init, sender, current common.Hash,
	shard uint32,
	key api.LockKey,
	layer api.LockLayer,
	epoch api.Epoch,
	blockNum uint64,
) error {
	if d.comm == nil || d.state == nil || d.signerMgr == nil {
		return nil
	}
	nonce := d.nonceCounter.Add(1)
	// Path 记录探针经过的节点：init + 首个 holder(current)，之后每跳由 HandleProbe
	// 追加下一跳 holder，这样环内每个到过的节点都在 Path 里（否则中间节点丢失，
	// resolveRing 无法正确取环成员/选 victim）。
	path := []common.Hash{init}
	if current != init && current != (common.Hash{}) {
		path = append(path, current)
	}
	probe := &api.DeadlockProbe{
		Init:     init,
		Sender:   sender,
		Current:  current,
		Path:     path,
		Layer:    layer,
		WaitKey:  key,
		Epoch:    uint64(epoch),
		BlockNum: blockNum,
		Nonce:    nonce,
		Shard:    shard,
	}
	if err := d.signProbe(probe); err != nil {
		utils.SSCLogger().Error().Err(err).
			Str("init", init.Hex()).
			Msg("[deadlockDetector] initial probe sign failed")
		return err
	}
	d.probeSent.Add(1)
	d.dispatch(probe)
	return nil
}

// signProbe 请求本分片委员会成员对 probe.Bytes() 签名并聚合阈值 BLS。
func (d *deadlockDetector) signProbe(probe *api.DeadlockProbe) error {
	if d.state == nil || d.state.GetCommittee == nil || d.signerMgr == nil {
		return nil
	}
	epoch := api.Epoch(probe.Epoch)
	committee := d.state.GetCommittee(epoch, d.selfShard)
	if committee == nil || len(committee.Members) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	shardNum := int(d.state.ShardNum())
	if shardNum <= 0 {
		shardNum = int(d.selfShard) + 1
		if shardNum < 1 {
			shardNum = 1
		}
	}
	epochs := make([]api.Epoch, shardNum)
	for i := range epochs {
		epochs[i] = 0
	}
	// Epochs 是按分片索引的数组：只填本分片当前 epoch，其余置 0（与现有签名基础设施一致）。
	if int(d.selfShard) < len(epochs) {
		epochs[d.selfShard] = epoch
	} else if len(epochs) > 0 {
		epochs[len(epochs)-1] = epoch
	}

	var (
		mu      sync.Mutex
		results []api.SSCMessage
		wg      sync.WaitGroup
	)
	threshold := committee.Threshold
	if threshold <= 0 {
		threshold = (len(committee.Members) + 1) / 2
	}
	selfAddr := d.signerMgr.GetSSCSigner().Address()
	for _, m := range committee.Members {
		wg.Add(1)
		go func(m *api.Member) {
			defer wg.Done()
			sig := make([]byte, 0)
			if err := d.comm.Call(ctx, &sig, m, api.Method_SignDeadlockProbe, probe); err != nil {
				return
			}
			ret := api.BaseSSCMessage{
				Signature:  sig,
				SenderAddr: m.Address,
				Epochs:     epochs,
			}
			mu.Lock()
			defer mu.Unlock()
			if ctx.Err() != nil {
				return
			}
			if m.Address == selfAddr {
				results = append([]api.SSCMessage{ret}, results...)
			} else {
				results = append(results, ret)
			}
		}(m)
	}
	wg.Wait()
	if len(results) < threshold {
		return nil
	}
	sig, bitmap, err := d.signerMgr.GetSSCSigner().Aggregate(results)
	if err != nil {
		return err
	}
	probe.ShardId = d.selfShard
	probe.Signatures = sig
	probe.BLSBitMap = bitmap
	probe.Epochs = epochs
	return nil
}

// dispatch 发送探针：单测可用 routeOverride 捕获转发，否则走真实 routeProbe。
func (d *deadlockDetector) dispatch(p *api.DeadlockProbe) {
	if p == nil {
		return
	}
	if d.routeOverride != nil {
		d.routeOverride(p)
		return
	}
	d.routeProbe(p)
}

// routeProbe 多播到 probe.Current 的 RelatedShards（只有持有出边 waitEdge 的分片继续）。
func (d *deadlockDetector) routeProbe(probe *api.DeadlockProbe) {
	if probe == nil || d.comm == nil || d.state == nil {
		return
	}
	related := d.relatedShardsOf(probe.Current)
	if len(related) == 0 {
		for shard := uint32(0); shard < d.state.ShardNum(); shard++ {
			related = append(related, shard)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, shard := range related {
		if shard == d.selfShard {
			d.HandleProbe(probe)
			continue
		}
		leader := d.state.GetLeader(api.Epoch(probe.Epoch), shard)
		if leader == nil {
			continue
		}
		var ack api.DeadlockProbeAck
		if err := d.comm.Call(ctx, &ack, leader, api.Method_DetectDeadlockProbe, probe); err != nil {
			utils.SSCLogger().Debug().Err(err).
				Str("init", probe.Init.Hex()).
				Str("current", probe.Current.Hex()).
				Uint32("shard", shard).
				Msg("[deadlockDetector] forward probe failed")
		}
	}
}

// relatedShardsOf 从 SLM txMeta 取交易相关分片。
func (d *deadlockDetector) relatedShardsOf(txHash common.Hash) []uint32 {
	if d.stateLockManager == nil {
		return nil
	}
	if tm, ok := d.stateLockManager.GetTxMeta(txHash); ok && len(tm.RelatedShards) > 0 {
		out := make([]uint32, len(tm.RelatedShards))
		copy(out, tm.RelatedShards)
		return out
	}
	return nil
}

// Stats 返回监控计数快照。
func (d *deadlockDetector) Stats() DeadlockDetectorStatus {
	waitEdges := 0
	d.waitEdges.Range(func(_, _ interface{}) bool { waitEdges++; return true })
	seen := 0
	d.seenProbes.Range(func(_, _ interface{}) bool { seen++; return true })
	return DeadlockDetectorStatus{
		WaitEdges:       waitEdges,
		SeenProbes:      seen,
		ProbeSent:       d.probeSent.Load(),
		ProbeDropped:    d.probeDropped.Load(),
		ProbeInvalidSig: d.probeInvalid.Load(),
		RingDetected:    d.ringDetected.Load(),
		VictimDied:      d.victimDied.Load(),
	}
}
