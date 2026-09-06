package ssc

import (
	"bytes"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
)

// ─── PatchConsumeStatus ───────────────────────────────────────────────

// PatchConsumeStatus represents the lifecycle state of an OffChainPatchNode.
type PatchConsumeStatus int32

const (
	PatchFree      PatchConsumeStatus = iota // Not yet taken by any retryTx
	PatchConsumed                            // Taken by a retryTx, can still be Wounded
	PatchFinalized                           // Locked by CommitSimulation, cannot be Wounded

)

const defaultMaxChainDepthValue = 5

// defaultMaxChainDepthValue DSN-50: 链下 DAG 链深上限的默认值。

// ─── OffChainPatchNode ───────────────────────────────────────────────

// OffChainPatchNode is a node in the single off-chain DAG (offChainDAG).
// It merges the scheduling state of the old ChainPatchNode (PatchPool) with the
// read-construction state of api.ChainNode (UpstreamTxList), so that the whole
// off-chain Patch pool is a single source of truth.
type OffChainPatchNode struct {
	TxHash         common.Hash    // txHash of the SimTx (本节点)
	SimulationNum  int            // simulationNum of the SimTx
	Patch          *api.RWSet     // WriteSet of the SimTx (本节点)
	UpstreamTxList []api.TxSimKey // 上游依赖（DAG 边）—— 原 api.ChainNode 字段
	Consumer       common.Hash    // retryTx that consumed this Patch (zero if Free)
	Priority       api.Priority   // priority of the consumer
	CreatedAt      time.Time      // time when added to pool
	Depth          int            // DSN-50: DAG 深度（根=1；非根=max(上游 Depth)+1），用于链深上限
	status         atomic.Int32   // PatchConsumeStatus, lock-free
}

func (n *OffChainPatchNode) Status() PatchConsumeStatus {
	return PatchConsumeStatus(n.status.Load())
}

func (n *OffChainPatchNode) SetStatus(s PatchConsumeStatus) {
	n.status.Store(int32(s))
}

// TryAcquire atomically transitions from Free→Consumed.
// Returns true if successful, false if already consumed/finalized.
func (n *OffChainPatchNode) TryAcquire() bool {
	return n.status.CompareAndSwap(int32(PatchFree), int32(PatchConsumed))
}

// ─── offChainDAG ─────────────────────────────────────────────────────

// offChainDAG is the single leader-side off-chain Patch DAG storage.
// It replaces the two parallel structures localPatches + patches (and their
// keyIndex / subscriber / txSubKeys indexes).
type offChainDAG struct {
	// nodes — 链下 DAG 节点，key: txHash, value: *OffChainPatchNode
	nodes sync.Map
	// keyIndex — key → {txHash set}，哪些 SimTx 写了这个 key（调度匹配反查）
	keyIndex sync.Map // key: api.LockKey, val: *sync.Map
	// subscriber — key → {retryTxHash set}，哪些 retryTx 等这个 key
	subscriber sync.Map // key: api.LockKey, val: *sync.Map
	// txSubKeys — 反向索引，retryTxHash → []LockKey
	txSubKeys sync.Map // key: common.Hash, val: []api.LockKey
	// enabled — 整个 off-chain DAG / PatchPool 子系统开关。
	// 零值 false = 默认禁用（消融/调试）。禁用时所有方法空转、isPatchFinalized 恒 true。
	enabled bool
}

// isOn 返回 DAG 子系统是否启用。
func (dag *offChainDAG) isOn() bool { return dag != nil && dag.enabled }

// SetEnabled 允许外部（配置/实验）打开或关闭 DAG 子系统。
func (dag *offChainDAG) SetEnabled(on bool) {
	if dag != nil {
		dag.enabled = on
	}
}

// AddNode 在链下 DAG 中创建（或更新）一个节点。
// 状态 = PatchFree；仅记录本节点 WriteSet + 上游依赖，尚未建立 keyIndex（不可被调度匹配）。
// 之后需调用 MarkReady 建立 keyIndex 反查并触发 subscriber 扫描。
func (dag *offChainDAG) AddNode(txHash common.Hash, simNum int, patch *api.RWSet, upstreamTxList []api.TxSimKey) {
	if !dag.isOn() {
		return
	}
	now := time.Now()
	nodeVal, _ := dag.nodes.LoadOrStore(txHash, &OffChainPatchNode{
		TxHash:         txHash,
		SimulationNum:  simNum,
		Patch:          patch,
		UpstreamTxList: upstreamTxList,
		CreatedAt:      now,
	})
	node := nodeVal.(*OffChainPatchNode)
	// 同 txHash 重复写入（如 sendChainSignal 复用节点）时更新字段，保持单一节点语义。
	node.SimulationNum = simNum
	node.Patch = patch
	node.UpstreamTxList = upstreamTxList
	node.Depth = chainDepthOfUpstreams(dag, upstreamTxList)
	node.SetStatus(PatchFree)
	// chainDepthDist：按节点真实 DAG 深度(node.Depth) 累计（不是 simulationNum）。
	recordDagNodeDepth(node.Depth)
	if l := utils.SSCLogger(); l != nil {
		l.Debug().Str("txHash", txHash.Hex()).
			Int("simNum", simNum).
			Int("upstreamCount", len(upstreamTxList)).
			Int("depth", node.Depth).
			Msg("offChainDAG.AddNode")
	}
}

// MarkReady 将节点置为“可被匹配”状态：
// 建立 keyIndex 反查，并通过 onReady 回调触发 scanPatchSubscribers 增量扫描
// （替代原 addPatch + OnPatchPoolUpdated 两步；scan 逻辑依赖 retryScheduler 状态，故以回调注入）。
func (dag *offChainDAG) MarkReady(txHash common.Hash, onReady func(*api.RWSet)) {
	if !dag.isOn() {
		return
	}
	nodeVal, ok := dag.nodes.Load(txHash)
	if !ok {
		return
	}
	node := nodeVal.(*OffChainPatchNode)
	if node.Patch == nil || len(node.Patch.WriteState.State) == 0 {
		return
	}
	for addr, state := range node.Patch.WriteState.State {
		for key := range state {
			lockKey := api.FormKey(addr, key)
			innerVal, _ := dag.keyIndex.LoadOrStore(lockKey, &sync.Map{})
			inner := innerVal.(*sync.Map)
			inner.Store(txHash, struct{}{})
		}
	}
	if l := utils.SSCLogger(); l != nil {
		l.Debug().Str("txHash", txHash.Hex()).
			Int("simNum", node.SimulationNum).
			Msg("offChainDAG.MarkReady")
	}
	// 触发增量扫描：让已订阅该 WriteSet 中 key 的 retryTx 尝试匹配（DAG chaining）
	if onReady != nil {
		onReady(node.Patch)
	}
}

// Remove 从链下 DAG 原子删除一个节点及其关联索引（keyIndex + subscriber + txSubKeys）。
// 这是 closeTransaction 中唯一的链下清理入口（替代 removePatch + patches.Delete 两处）。
func (dag *offChainDAG) Remove(txHash common.Hash) {
	if !dag.isOn() {
		return
	}
	t0 := time.Now()
	nodeVal, ok := dag.nodes.Load(txHash)
	if ok {
		node := nodeVal.(*OffChainPatchNode)

		// 清理 keyIndex（本节点作为 SimTx 写入了哪些 key）
		if node.Patch != nil {
			for addr, state := range node.Patch.WriteState.State {
				for key := range state {
					lockKey := api.FormKey(addr, key)
					if innerVal, ok := dag.keyIndex.Load(lockKey); ok {
						inner := innerVal.(*sync.Map)
						inner.Delete(txHash)
						empty := true
						inner.Range(func(_, _ interface{}) bool {
							empty = false
							return false
						})
						if empty {
							dag.keyIndex.Delete(lockKey)
						}
					}
				}
			}
		}
		dag.nodes.Delete(txHash)
	}

	// 清理 subscriber / txSubKeys：无论 txHash 是否在 nodes 中，
	// 只要它作为 retryTx 订阅过 key，都应一并清理（原子删除 node + keyIndex + subscriber + txSubKeys）。
	dag.unsubscribeRetryTx(txHash)

	if l := utils.SSCLogger(); l != nil {
		l.Debug().Str("txHash", txHash.Hex()).
			Str("duration", time.Since(t0).String()).
			Msg("offChainDAG.Remove timing")
	}
}

// patchStats returns snapshot of patch pool size metrics.
func (dag *offChainDAG) patchStats() (patchCount, keyCount int) {
	if !dag.isOn() {
		return 0, 0
	}
	dag.nodes.Range(func(_, _ interface{}) bool {
		patchCount++
		return true
	})
	dag.keyIndex.Range(func(_, _ interface{}) bool {
		keyCount++
		return true
	})
	return
}

// ─── Consume Operations ────────────────────────────────────────────

// tryConsumePatch attempts to take the Patch for the given txHash.
func (dag *offChainDAG) tryConsumePatch(txHash common.Hash, consumer common.Hash, priority api.Priority) *api.RWSet {
	if !dag.isOn() {
		return nil
	}
	nodeVal, ok := dag.nodes.Load(txHash)
	if !ok {
		return nil
	}
	node := nodeVal.(*OffChainPatchNode)
	if !node.TryAcquire() {
		return nil
	}
	node.Consumer = consumer
	node.Priority = priority
	return node.Patch
}

// releasePatch releases a consumed Patch (on retry failure), allowing other retryTxs to take it.
func (dag *offChainDAG) releasePatch(txHash common.Hash) {
	if !dag.isOn() {
		return
	}
	nodeVal, ok := dag.nodes.Load(txHash)
	if !ok {
		return
	}
	node := nodeVal.(*OffChainPatchNode)
	if node.Status() != PatchConsumed {
		return
	}
	node.SetStatus(PatchFree)
	node.Consumer = common.Hash{}
	node.Priority = api.Priority{}
}

// finalizePatch marks a Patch as Finalized — cannot be Wounded anymore.
// ⚠️ 仅允许 Consumed→Finalized：只有“已被某下游消费的上游”在其自身构建 SimTx 提交时才锁死。
// 不能对 Free 节点 finalize——Free 节点仍需保持可被后续下游消费（TryAcquire 只允许 Free→Consumed），
// 若一构建 SimTx 就 Finalize 会使节点不可再被消费、整条 DAG 链式救援失效（DSN-62 (A) 初版回归，已回退）。
func (dag *offChainDAG) finalizePatch(txHash common.Hash) {
	if !dag.isOn() {
		return
	}
	nodeVal, ok := dag.nodes.Load(txHash)
	if !ok {
		return
	}
	node := nodeVal.(*OffChainPatchNode)
	if node.Status() != PatchConsumed {
		return
	}
	node.SetStatus(PatchFinalized)
}

// isPatchFinalized returns true if the given txHash's Patch is Finalized.
func (dag *offChainDAG) isPatchFinalized(txHash common.Hash) bool {
	// DAG 禁用时并不代表"恒 finalized"。isPatchFinalized 是 wound 闸门：
	// DAG 关闭(无 patch 节点)时应返回 false → 恢复 TLV 正常 wound 语义，不禁 wound。
	if !dag.isOn() {
		return false
	}
	nodeVal, ok := dag.nodes.Load(txHash)
	if !ok {
		return false
	}
	return nodeVal.(*OffChainPatchNode).Status() == PatchFinalized
}

// ─── Subscriber ─────────────────────────────────────────────────────

// subscribeRetryTx registers a retryTx's key interests (both ReadSet and WriteSet).
func (dag *offChainDAG) subscribeRetryTx(txHash common.Hash, reads, writes []api.LockKey) {
	if !dag.isOn() {
		return
	}
	allKeys := make([]api.LockKey, 0, len(reads)+len(writes))
	allKeys = append(allKeys, reads...)
	allKeys = append(allKeys, writes...)

	for _, key := range allKeys {
		innerVal, _ := dag.subscriber.LoadOrStore(key, &sync.Map{})
		inner := innerVal.(*sync.Map)
		inner.Store(txHash, struct{}{})
	}
	dag.txSubKeys.Store(txHash, allKeys)
}

// unsubscribeRetryTx removes a retryTx from the subscriber index.
func (dag *offChainDAG) unsubscribeRetryTx(txHash common.Hash) {
	if !dag.isOn() {
		return
	}
	keysVal, ok := dag.txSubKeys.Load(txHash)
	if !ok {
		return
	}
	allKeys := keysVal.([]api.LockKey)
	for _, key := range allKeys {
		if innerVal, ok := dag.subscriber.Load(key); ok {
			inner := innerVal.(*sync.Map)
			inner.Delete(txHash)
			empty := true
			inner.Range(func(_, _ interface{}) bool {
				empty = false
				return false
			})
			if empty {
				dag.subscriber.Delete(key)
			}
		}
	}
	dag.txSubKeys.Delete(txHash)
}

// resubscribeRetryTx removes and re-registers a retryTx's key interests.
func (dag *offChainDAG) resubscribeRetryTx(txHash common.Hash, reads, writes []api.LockKey) {
	if !dag.isOn() {
		return
	}
	dag.unsubscribeRetryTx(txHash)
	dag.subscribeRetryTx(txHash, reads, writes)
}

// querySubscribers returns all retryTx hashes subscribed to any of the given keys (deduplicated).
func (dag *offChainDAG) querySubscribers(keys []api.LockKey) []common.Hash {
	if !dag.isOn() {
		return nil
	}
	seen := make(map[common.Hash]struct{})
	for _, key := range keys {
		if innerVal, ok := dag.subscriber.Load(key); ok {
			inner := innerVal.(*sync.Map)
			inner.Range(func(txHashVal, _ interface{}) bool {
				seen[txHashVal.(common.Hash)] = struct{}{}
				return true
			})
		}
	}
	result := make([]common.Hash, 0, len(seen))
	for txHash := range seen {
		result = append(result, txHash)
	}
	return result
}

// ─── Query ──────────────────────────────────────────────────────────

// patchesHaveConflict checks if a retryTx depends on any SimTx in the off-chain DAG.
func (dag *offChainDAG) patchesHaveConflict(retryTx *api.RetryTx) (bool, *OffChainPatchNode) {
	if !dag.isOn() {
		return false, nil
	}
	candidates := make(map[common.Hash]int)

	for _, key := range retryTx.ReadSet {
		if innerVal, ok := dag.keyIndex.Load(key); ok {
			inner := innerVal.(*sync.Map)
			inner.Range(func(txHashVal, _ interface{}) bool {
				candidates[txHashVal.(common.Hash)]++
				return true
			})
		}
	}
	for _, key := range retryTx.WriteSet {
		if innerVal, ok := dag.keyIndex.Load(key); ok {
			inner := innerVal.(*sync.Map)
			inner.Range(func(txHashVal, _ interface{}) bool {
				candidates[txHashVal.(common.Hash)]++
				return true
			})
		}
	}

	var best common.Hash
	var bestCnt int
	for txHash, cnt := range candidates {
		nodeVal, exists := dag.nodes.Load(txHash)
		if !exists {
			continue
		}
		node := nodeVal.(*OffChainPatchNode)
		status := node.Status()
		if cnt > bestCnt && status != PatchFinalized && status != PatchConsumed {
			bestCnt = cnt
			best = txHash
		}
	}
	if bestCnt > 0 {
		nodeVal, _ := dag.nodes.Load(best)
		return true, nodeVal.(*OffChainPatchNode)
	}
	return false, nil
}

// findCoveringPatch checks if a single Patch covers all given conflictKeys.
func (dag *offChainDAG) findCoveringPatch(conflictKeys []api.LockKey, exclude common.Hash) (bool, *OffChainPatchNode) {
	if !dag.isOn() {
		return false, nil
	}
	candidates := make(map[common.Hash]int)
	for _, key := range conflictKeys {
		innerVal, ok := dag.keyIndex.Load(key)
		if !ok {
			return false, nil
		}
		inner := innerVal.(*sync.Map)
		inner.Range(func(txHashVal, _ interface{}) bool {
			candidates[txHashVal.(common.Hash)]++
			return true
		})
	}

	var best common.Hash
	for txHash, cnt := range candidates {
		if cnt != len(conflictKeys) {
			continue
		}
		if txHash == exclude {
			continue
		}
		nodeVal, exists := dag.nodes.Load(txHash)
		if !exists {
			continue
		}
		node := nodeVal.(*OffChainPatchNode)
		status := node.Status()
		if status != PatchFinalized && status != PatchConsumed {
			if best == (common.Hash{}) {
				best = txHash
			}
		}
	}
	if best != (common.Hash{}) {
		nodeVal, _ := dag.nodes.Load(best)
		return true, nodeVal.(*OffChainPatchNode)
	}
	return false, nil
}

// findCoveringSet 返回一组 Free Patch，联合覆盖全部 conflictKeys（DAG 多 Patch 覆盖）。
// 贪心近似：每轮选覆盖最多「未覆盖 key」的 Free Patch。
// exclude：排除自己（防自环）；maxSimNum：仅选严格更早（SimulationNum < maxSimNum）的节点，
// 保证依赖图按轮次严格递增、链深有界（防止同轮无限 chaining）。
func (dag *offChainDAG) findCoveringSet(conflictKeys []api.LockKey, exclude common.Hash, maxSimNum int) []*OffChainPatchNode {
	if !dag.isOn() {
		return nil
	}
	type patchEntry struct {
		node *OffChainPatchNode
		keys map[int]struct{}
	}
	var available []patchEntry
	for i, key := range conflictKeys {
		innerVal, ok := dag.keyIndex.Load(key)
		if !ok {
			return nil
		}
		inner := innerVal.(*sync.Map)
		inner.Range(func(txHashVal, _ interface{}) bool {
			txHash := txHashVal.(common.Hash)
			if txHash == exclude {
				return true
			}
			nodeVal, exists := dag.nodes.Load(txHash)
			if !exists {
				return true
			}
			node := nodeVal.(*OffChainPatchNode)
			if node.Status() != PatchFree {
				return true
			}
			// 只选严格更早（SimulationNum < maxSimNum）的节点：依赖边必须指向更早轮次，
			// 从构造上保证 DAG 无环且链深有界（阻断同轮次无限 chaining）。
			if maxSimNum > 0 && node.SimulationNum >= maxSimNum {
				return true
			}
			found := false
			for j := range available {
				if available[j].node.TxHash == txHash {
					available[j].keys[i] = struct{}{}
					found = true
					break
				}
			}
			if !found {
				available = append(available, patchEntry{
					node: node,
					keys: map[int]struct{}{i: {}},
				})
			}
			return true
		})
	}

	covered := make(map[int]bool)
	var result []*OffChainPatchNode
	for len(covered) < len(conflictKeys) {
		bestIdx := -1
		bestCnt := 0
		for idx, entry := range available {
			cnt := 0
			for k := range entry.keys {
				if !covered[k] {
					cnt++
				}
			}
			if cnt > bestCnt {
				bestCnt = cnt
				bestIdx = idx
			}
		}
		if bestIdx == -1 || bestCnt == 0 {
			return nil
		}
		for k := range available[bestIdx].keys {
			covered[k] = true
		}
		result = append(result, available[bestIdx].node)
		available = append(available[:bestIdx], available[bestIdx+1:]...)
	}
	return result
}

// collectBlockedKeys 返回 retryTx 读写集中**当前被其它交易占用**的 key 集合
// （= 真正需要被 patch 覆盖的 key）：
//   - SLM/链上锁（含 pending）：stateDB.CheckLock 失败（非自己持有）；
//   - TLV/链下锁：tempLockView.HasConflict（已排除自己）。
//
// 未被任何锁占用的 key 在重跑模拟时可直接读写 stateDB，**不需要**上游 patch 覆盖，
// 不应纳入覆盖需求（此前用完整 Read∪Write 集做覆盖判定是历史 bug：只被抢 1-2 个
// 热 key 的交易永远凑不齐“覆盖全部 key”的 patch 组而饥饿）。
// stateDB 传 nil 时只统计 TLV 层（用于 Phase1b 等尚未取 stateDB 的链下场景）。
func (rs *retryScheduler) collectBlockedKeys(retryTx *api.RetryTx, stateDB api.StateDB) []api.LockKey {
	if retryTx == nil {
		return nil
	}
	seen := make(map[api.LockKey]struct{})
	var blocked []api.LockKey
	add := func(keys []api.LockKey) {
		for _, k := range keys {
			if _, ok := seen[k]; ok {
				continue
			}
			isBlocked := false
			if stateDB != nil {
				if err := stateDB.CheckLock(k, retryTx.TxHash); err != nil {
					isBlocked = true
				}
			}
			if !isBlocked && rs.tempLockView != nil && rs.tempLockView.HasConflict(retryTx.TxHash, k) {
				isBlocked = true
			}
			if isBlocked {
				seen[k] = struct{}{}
				blocked = append(blocked, k)
			}
		}
	}
	add(retryTx.WriteSet)
	add(retryTx.ReadSet)
	return blocked
}

// scanPatchSubscribers 在新增 Patch 后增量扫描 subscriber 中 key 重叠的 retryTx。
// 按优先级 reservation 避免多 tx 争锁。
func (rs *retryScheduler) scanPatchSubscribers(writeSet *api.RWSet) {
	if rs == nil || !rs.offChainDAG.isOn() {
		return
	}
	keys := extractWriteKeys(writeSet)
	if len(keys) == 0 {
		return
	}

	candidates := rs.offChainDAG.querySubscribers(keys)
	if len(candidates) == 0 {
		return
	}

	// 收集有效候 tx（排除非 active）
	type prioTx struct {
		txHash   common.Hash
		tx       *api.RetryTx
		needKeys []api.LockKey
		patches  []*OffChainPatchNode
	}
	var sorted []prioTx
	stateDB, _ := rs.getCachedStateDB()
	for _, txHash := range candidates {
		retryTxVal, exists := rs.retryPool.Load(txHash)
		if !exists {
			continue
		}
		retryTx := retryTxVal.(*api.RetryTx)
		if retryTx.Status == api.RetryPassive || retryTx.Status == api.RetryWounded || retryTx.Status == api.RetryConsumed {
			continue
		}
		// 覆盖需求只取“当前被其它交易占用”的 key：未被上锁的 key 重跑可直接读写 stateDB，
		// 纳入覆盖需求反而让热 key 交易永远凑不齐“全 key 覆盖集”而饥饿（历史 bug 修复）。
		needKeys := rs.collectBlockedKeys(retryTx, stateDB)
		if len(needKeys) == 0 {
			// 无被占用的 key → 不需要链式 patch 救援（走正常 OnBlockCommitted promote）。
			continue
		}
		patches := rs.offChainDAG.findCoveringSet(needKeys, txHash, retryTx.SimulationNum)
		if len(patches) == 0 {
			continue
		}
		// DSN-50 闸门：覆盖完整性 + 链深上限。不满足则不链式救援，退回等锁。
		if !rs.offChainDAG.isFullyCovered(needKeys, patches) {
			continue
		}
		if chainDepthOf(patches) > rs.maxChainDepth {
			chainRetryStats.SigChainDepthCapped.Add(1)
			continue
		}
		sorted = append(sorted, prioTx{txHash: txHash, tx: retryTx, needKeys: needKeys, patches: patches})
	}
	if len(sorted) == 0 {
		return
	}
	// 按 nonce 升序（高优先级在前），同一 nonce 按 txHash
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].tx.Nonce != sorted[j].tx.Nonce {
			return sorted[i].tx.Nonce < sorted[j].tx.Nonce
		}
		return bytes.Compare(sorted[i].txHash.Bytes(), sorted[j].txHash.Bytes()) < 0
	})

	// Reservation: 选中一笔后预留其 key，跳过冲突 tx
	reservedKeySet := make(map[api.LockKey]struct{})
	matchedCount := 0
	for _, pt := range sorted {
		// 检查是否与预留 key 冲突
		conflict := false
		for _, key := range pt.tx.ReadSet {
			if _, exists := reservedKeySet[key]; exists {
				conflict = true
				break
			}
		}
		if !conflict {
			for _, key := range pt.tx.WriteSet {
				if _, exists := reservedKeySet[key]; exists {
					conflict = true
					break
				}
			}
		}
		if conflict {
			continue
		}
		// 预留 key
		for _, key := range pt.tx.ReadSet {
			reservedKeySet[key] = struct{}{}
		}
		for _, key := range pt.tx.WriteSet {
			reservedKeySet[key] = struct{}{}
		}

		priority := api.Priority{
			Nonce:         pt.tx.Nonce,
			OriginShardID: pt.tx.OriginShardID,
			TxHash:        pt.txHash,
		}

		var consumedUpstreams []common.Hash
		var upstreamTxList []api.TxSimKey
		allOk := true
		for _, pn := range pt.patches {
			// DSN-56：消费上游不再把上游写集 merged 成扁平 patch；只建立“消费”关系与 DAG 上游边。
			if patch := rs.offChainDAG.tryConsumePatch(pn.TxHash, pt.txHash, priority); patch == nil {
				allOk = false
				break
			}
			consumedUpstreams = append(consumedUpstreams, pn.TxHash)
			upstreamTxList = append(upstreamTxList, api.TxSimKey{
				TxHash:        pn.TxHash,
				SimulationNum: pn.SimulationNum,
			})
		}
		if allOk {
			matchedCount++
			pt.tx.Status = api.RetryConsumed
			rs.offChainDAG.unsubscribeRetryTx(pt.txHash)
			rs.consumedPatches.Store(pt.txHash, consumedUpstreams)
			rs.sendChainSignal(pt.txHash, pt.tx, upstreamTxList)
		} else {
			for _, txh := range consumedUpstreams {
				rs.offChainDAG.releasePatch(txh)
			}
		}
	}
	if matchedCount > 0 {
		utils.SSCLogger().Debug().
			Int("candidates", len(sorted)).
			Int("matchedCount", matchedCount).
			Int("reservedKeys", len(reservedKeySet)).
			Msg("scanPatchSubscribers: reservation completed")
	}
}

// ─── Helpers ─────────────────────────────────────────────────────────

// extractWriteKeys 从 Patch 的 writeSet 中提取所有 LockKey。
func extractWriteKeys(writeSet *api.RWSet) []api.LockKey {
	if writeSet == nil || len(writeSet.WriteState.State) == 0 {
		return nil
	}
	var keys []api.LockKey
	for addr, state := range writeSet.WriteState.State {
		for key := range state {
			keys = append(keys, api.FormKey(addr, key))
		}
	}
	return keys
}

// defaultMaxChainDepth 返回链下 DAG 链深上限；配置 <=0 时使用默认值。
func defaultMaxChainDepth(configured int) int {
	if configured > 0 {
		return configured
	}
	return defaultMaxChainDepthValue
}

// chainDepthOfUpstreams 计算以给定上游列表为新节点上游时的 DAG 深度。
// 根节点（无上游）= 1；否则 = max(上游 Depth) + 1。
// 用于 AddNode 写入节点时记录显式 Depth（DSN-50）。
func chainDepthOfUpstreams(dag *offChainDAG, upstreamTxList []api.TxSimKey) int {
	depth := 1
	for _, up := range upstreamTxList {
		if nv, ok := dag.nodes.Load(up.TxHash); ok {
			if d := nv.(*OffChainPatchNode).Depth + 1; d > depth {
				depth = d
			}
		}
	}
	return depth
}

// chainDepthOf 计算以一组 Patch 为上游时，新节点将会得到的 DAG 深度（DSN-50）。
func chainDepthOf(patches []*OffChainPatchNode) int {
	depth := 1
	for _, p := range patches {
		if p == nil {
			continue
		}
		if d := p.Depth + 1; d > depth {
			depth = d
		}
	}
	return depth
}

// isFullyCovered 检查一组 Patch 是否联合覆盖 needKeys 的全部 key（DSN-50 覆盖完整性校验）。
// 只有每个 key 都被至少一个选中 Patch 写过，才算完整覆盖。
func (dag *offChainDAG) isFullyCovered(needKeys []api.LockKey, patches []*OffChainPatchNode) bool {
	if !dag.isOn() {
		return false
	}
	covered := make(map[api.LockKey]bool)
	for _, p := range patches {
		if p == nil || p.Patch == nil {
			continue
		}
		for _, k := range extractWriteKeys(p.Patch) {
			covered[k] = true
		}
	}
	for _, k := range needKeys {
		if !covered[k] {
			return false
		}
	}
	return true
}

// mergeRWSet 合并两个 RWSet 的 WriteState（new 覆盖相同 key）。
func mergeRWSet(new, old *api.RWSet) *api.RWSet {
	if old == nil {
		return new
	}
	if new == nil {
		return old
	}
	merged := &api.RWSet{
		ReadState:    api.NewStateSet(),
		WriteState:   api.NewStateSet(),
		CurrentState: api.NewStateSet(),
	}
	for addr, state := range old.WriteState.State {
		if merged.WriteState.State[addr] == nil {
			merged.WriteState.State[addr] = make(map[common.Hash]common.Hash)
		}
		for k, v := range state {
			merged.WriteState.State[addr][k] = v
		}
	}
	for addr, state := range new.WriteState.State {
		if merged.WriteState.State[addr] == nil {
			merged.WriteState.State[addr] = make(map[common.Hash]common.Hash)
		}
		for k, v := range state {
			merged.WriteState.State[addr][k] = v
		}
	}
	return merged
}

// buildSimPatchSubgraph — DSN-54：从 offChainDAG 为 (targetTx, targetSimNum) 的某轮模拟
// 构建 "被消费上游 + 传递闭包" 的模拟期 patch 子图。
// rootKeys 是 targetTx 本轮直接消费的上游节点（TxSimKey，含 simNum）；
// 递归收集每个 root 及其全部传递上游（沿 UpstreamTxList 边），visited 防环。
// 节点收集顺序确定（BFS 由 root 列表顺序 + 各自 UpstreamTxList 顺序决定，成员侧同构可复现），
// 保证同一 leader 广播给所有成员后各成员反查结果一致（门限签名一致性敏感）。
func (dag *offChainDAG) buildSimPatchSubgraph(targetTx common.Hash, targetSimNum int, rootKeys []api.TxSimKey) *api.SimPatchSubgraph {
	if !dag.isOn() {
		return nil
	}
	sub := &api.SimPatchSubgraph{
		TxHash:        targetTx,
		SimulationNum: targetSimNum,
	}
	seen := make(map[api.TxSimKey]struct{})
	var collect func(k api.TxSimKey)
	collect = func(k api.TxSimKey) {
		if _, ok := seen[k]; ok {
			return // DAG 环 / 已收集
		}
		seen[k] = struct{}{}

		nodeVal, ok := dag.nodes.Load(k.TxHash)
		if !ok {
			return
		}
		node := nodeVal.(*OffChainPatchNode)
		if node.SimulationNum != k.SimulationNum {
			return
		}

		sub.Nodes = append(sub.Nodes, &api.SimPatchNode{
			TxSim:    k,
			Upstream: node.UpstreamTxList,
			Writes:   node.Patch,
		})

		// 递归收集传递上游
		for _, up := range node.UpstreamTxList {
			collect(up)
		}
	}
	for _, r := range rootKeys {
		collect(r)
	}
	if len(sub.Nodes) == 0 {
		return nil
	}
	return sub
}
