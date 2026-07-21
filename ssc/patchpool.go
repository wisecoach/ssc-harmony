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

// PatchConsumeStatus represents the lifecycle state of a ChainPatchNode.
type PatchConsumeStatus int32

const (
	PatchFree      PatchConsumeStatus = iota // Not yet taken by any retryTx
	PatchConsumed                            // Taken by a retryTx, can still be Wounded
	PatchFinalized                           // Locked by CommitSimulation, cannot be Wounded
)

// ─── ChainPatchNode ───────────────────────────────────────────────────

// ChainPatchNode is a node in the PatchPool.
// Each submitted SimTx corresponds to one node.
type ChainPatchNode struct {
	TxHash        common.Hash  // txHash of the SimTx
	SimulationNum int          // simulationNum of the SimTx
	Patch         *api.RWSet   // WriteSet of the SimTx
	Consumer      common.Hash  // retryTx that consumed this Patch (zero if Free)
	Priority      api.Priority // priority of the consumer
	CreatedAt     time.Time    // time when added to pool
	status        atomic.Int32 // PatchConsumeStatus, lock-free
}

func (n *ChainPatchNode) Status() PatchConsumeStatus {
	return PatchConsumeStatus(n.status.Load())
}

func (n *ChainPatchNode) SetStatus(s PatchConsumeStatus) {
	n.status.Store(int32(s))
}

// TryAcquire atomically transitions from Free→Consumed.
// Returns true if successful, false if already consumed/finalized.
func (n *ChainPatchNode) TryAcquire() bool {
	return n.status.CompareAndSwap(int32(PatchFree), int32(PatchConsumed))
}

// ─── Core Operations ────────────────────────────────────────────────

// addPatch adds a submitted SimTx's WriteSet to the local patch pool.
// Also updates the keyIndex inverted index.
func (rs *retryScheduler) addPatch(txHash common.Hash, simNum int, writeSet *api.RWSet) {
	node := &ChainPatchNode{
		TxHash:        txHash,
		SimulationNum: simNum,
		Patch:         writeSet,
		CreatedAt:     time.Now(),
	}
	node.SetStatus(PatchFree)
	rs.localPatches.Store(txHash, node)

	for addr, state := range writeSet.WriteState.State {
		for key := range state {
			lockKey := api.FormKey(addr, key)
			innerVal, _ := rs.keyIndex.LoadOrStore(lockKey, &sync.Map{})
			inner := innerVal.(*sync.Map)
			inner.Store(txHash, struct{}{})
		}
	}
}

// removePatch removes a Patch from the pool and cleans up keyIndex.
func (rs *retryScheduler) removePatch(txHash common.Hash) {
	t0 := time.Now()
	nodeVal, ok := rs.localPatches.Load(txHash)
	if !ok {
		return
	}
	node := nodeVal.(*ChainPatchNode)

	for addr, state := range node.Patch.WriteState.State {
		for key := range state {
			lockKey := api.FormKey(addr, key)
			if innerVal, ok := rs.keyIndex.Load(lockKey); ok {
				inner := innerVal.(*sync.Map)
				inner.Delete(txHash)
				empty := true
				inner.Range(func(_, _ interface{}) bool {
					empty = false
					return false
				})
				if empty {
					rs.keyIndex.Delete(lockKey)
				}
			}
		}
	}
	rs.localPatches.Delete(txHash)

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Str("duration", time.Since(t0).String()).
		Msg("PatchPool.Remove timing")
}

// patchStats returns snapshot of patch pool size metrics.
func (rs *retryScheduler) patchStats() (patchCount, keyCount int) {
	rs.localPatches.Range(func(_, _ interface{}) bool {
		patchCount++
		return true
	})
	rs.keyIndex.Range(func(_, _ interface{}) bool {
		keyCount++
		return true
	})
	return
}

// ─── Consume Operations ────────────────────────────────────────────

// tryConsumePatch attempts to take the Patch for the given txHash.
func (rs *retryScheduler) tryConsumePatch(txHash common.Hash, consumer common.Hash, priority api.Priority) *api.RWSet {
	nodeVal, ok := rs.localPatches.Load(txHash)
	if !ok {
		return nil
	}
	node := nodeVal.(*ChainPatchNode)
	if !node.TryAcquire() {
		return nil
	}
	node.Consumer = consumer
	node.Priority = priority
	return node.Patch
}

// releasePatch releases a consumed Patch (on retry failure), allowing other retryTxs to take it.
func (rs *retryScheduler) releasePatch(txHash common.Hash) {
	nodeVal, ok := rs.localPatches.Load(txHash)
	if !ok {
		return
	}
	node := nodeVal.(*ChainPatchNode)
	if node.Status() != PatchConsumed {
		return
	}
	node.SetStatus(PatchFree)
	node.Consumer = common.Hash{}
	node.Priority = api.Priority{}
}

// finalizePatch marks a Patch as Finalized — cannot be Wounded anymore.
func (rs *retryScheduler) finalizePatch(txHash common.Hash) {
	nodeVal, ok := rs.localPatches.Load(txHash)
	if !ok {
		return
	}
	node := nodeVal.(*ChainPatchNode)
	if node.Status() != PatchConsumed {
		return
	}
	node.SetStatus(PatchFinalized)
}

// isPatchFinalized returns true if the given txHash's Patch is Finalized.
func (rs *retryScheduler) isPatchFinalized(txHash common.Hash) bool {
	nodeVal, ok := rs.localPatches.Load(txHash)
	if !ok {
		return false
	}
	return nodeVal.(*ChainPatchNode).Status() == PatchFinalized
}

// ─── Subscriber ─────────────────────────────────────────────────────

// subscribeRetryTx registers a retryTx's key interests (both ReadSet and WriteSet).
func (rs *retryScheduler) subscribeRetryTx(txHash common.Hash, reads, writes []api.LockKey) {
	allKeys := make([]api.LockKey, 0, len(reads)+len(writes))
	allKeys = append(allKeys, reads...)
	allKeys = append(allKeys, writes...)

	for _, key := range allKeys {
		innerVal, _ := rs.subscriber.LoadOrStore(key, &sync.Map{})
		inner := innerVal.(*sync.Map)
		inner.Store(txHash, struct{}{})
	}
	rs.txSubKeys.Store(txHash, allKeys)
}

// unsubscribeRetryTx removes a retryTx from the subscriber index.
func (rs *retryScheduler) unsubscribeRetryTx(txHash common.Hash) {
	keysVal, ok := rs.txSubKeys.Load(txHash)
	if !ok {
		return
	}
	allKeys := keysVal.([]api.LockKey)
	for _, key := range allKeys {
		if innerVal, ok := rs.subscriber.Load(key); ok {
			inner := innerVal.(*sync.Map)
			inner.Delete(txHash)
			empty := true
			inner.Range(func(_, _ interface{}) bool {
				empty = false
				return false
			})
			if empty {
				rs.subscriber.Delete(key)
			}
		}
	}
	rs.txSubKeys.Delete(txHash)
}

// resubscribeRetryTx removes and re-registers a retryTx's key interests.
func (rs *retryScheduler) resubscribeRetryTx(txHash common.Hash, reads, writes []api.LockKey) {
	rs.unsubscribeRetryTx(txHash)
	rs.subscribeRetryTx(txHash, reads, writes)
}

// querySubscribers returns all retryTx hashes subscribed to any of the given keys (deduplicated).
func (rs *retryScheduler) querySubscribers(keys []api.LockKey) []common.Hash {
	seen := make(map[common.Hash]struct{})
	for _, key := range keys {
		if innerVal, ok := rs.subscriber.Load(key); ok {
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

// patchesHaveConflict checks if a retryTx depends on any SimTx in the local patch pool.
func (rs *retryScheduler) patchesHaveConflict(retryTx *api.RetryTx) (bool, *ChainPatchNode) {
	candidates := make(map[common.Hash]int)

	for _, key := range retryTx.ReadSet {
		if innerVal, ok := rs.keyIndex.Load(key); ok {
			inner := innerVal.(*sync.Map)
			inner.Range(func(txHashVal, _ interface{}) bool {
				candidates[txHashVal.(common.Hash)]++
				return true
			})
		}
	}
	for _, key := range retryTx.WriteSet {
		if innerVal, ok := rs.keyIndex.Load(key); ok {
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
		nodeVal, exists := rs.localPatches.Load(txHash)
		if !exists {
			continue
		}
		node := nodeVal.(*ChainPatchNode)
		status := node.Status()
		if cnt > bestCnt && status != PatchFinalized && status != PatchConsumed {
			bestCnt = cnt
			best = txHash
		}
	}
	if bestCnt > 0 {
		nodeVal, _ := rs.localPatches.Load(best)
		return true, nodeVal.(*ChainPatchNode)
	}
	return false, nil
}

// findCoveringPatch checks if a single Patch covers all given conflictKeys.
func (rs *retryScheduler) findCoveringPatch(conflictKeys []api.LockKey) (bool, *ChainPatchNode) {
	candidates := make(map[common.Hash]int)
	for _, key := range conflictKeys {
		innerVal, ok := rs.keyIndex.Load(key)
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
		nodeVal, exists := rs.localPatches.Load(txHash)
		if !exists {
			continue
		}
		node := nodeVal.(*ChainPatchNode)
		status := node.Status()
		if status != PatchFinalized && status != PatchConsumed {
			if best == (common.Hash{}) {
				best = txHash
			}
		}
	}
	if best != (common.Hash{}) {
		nodeVal, _ := rs.localPatches.Load(best)
		return true, nodeVal.(*ChainPatchNode)
	}
	return false, nil
}

// findCoveringSet 返回一组 Free Patch，联合覆盖全部 conflictKeys（DAG 多 Patch 覆盖）。
// 贪心近似：每轮选覆盖最多「未覆盖 key」的 Free Patch。
func (rs *retryScheduler) findCoveringSet(conflictKeys []api.LockKey) []*ChainPatchNode {
	type patchEntry struct {
		node *ChainPatchNode
		keys map[int]struct{}
	}
	var available []patchEntry
	for i, key := range conflictKeys {
		innerVal, ok := rs.keyIndex.Load(key)
		if !ok {
			return nil
		}
		inner := innerVal.(*sync.Map)
		inner.Range(func(txHashVal, _ interface{}) bool {
			txHash := txHashVal.(common.Hash)
			nodeVal, exists := rs.localPatches.Load(txHash)
			if !exists {
				return true
			}
			node := nodeVal.(*ChainPatchNode)
			if node.Status() != PatchFree {
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
	var result []*ChainPatchNode
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

// scanPatchSubscribers 在新增 Patch 后增量扫描 subscriber 中 key 重叠的 retryTx。
// 按优先级 reservation 避免多 tx 争锁。
func (rs *retryScheduler) scanPatchSubscribers(writeSet *api.RWSet) {
	keys := extractWriteKeys(writeSet)
	if len(keys) == 0 {
		return
	}

	candidates := rs.querySubscribers(keys)
	if len(candidates) == 0 {
		return
	}

	// 收集有效候 tx（排除非 active）
	type prioTx struct {
		txHash  common.Hash
		tx      *api.RetryTx
		allKeys []api.LockKey
		patches []*ChainPatchNode
	}
	var sorted []prioTx
	for _, txHash := range candidates {
		retryTxVal, exists := rs.retryPool.Load(txHash)
		if !exists {
			continue
		}
		retryTx := retryTxVal.(*api.RetryTx)
		if retryTx.Status == api.RetryPassive || retryTx.Status == api.RetryWounded || retryTx.Status == api.RetryConsumed {
			continue
		}
		allKeys := append([]api.LockKey{}, retryTx.ReadSet...)
		allKeys = append(allKeys, retryTx.WriteSet...)
		patches := rs.findCoveringSet(allKeys)
		if len(patches) == 0 {
			continue
		}
		sorted = append(sorted, prioTx{txHash: txHash, tx: retryTx, allKeys: allKeys, patches: patches})
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
		var merged *api.RWSet
		var upstreamTxList []api.TxSimKey
		allOk := true
		for _, pn := range pt.patches {
			// DAG 防环：自指 patch 跳过（patch 是自己的之前提交，不构成上游依赖）
			if pn.TxHash == pt.txHash {
				chainRetryStats.SigSelfPatchSkip.Add(1)
				continue
			}

			patch := rs.tryConsumePatch(pn.TxHash, pt.txHash, priority)
			if patch == nil {
				allOk = false
				break
			}
			consumedUpstreams = append(consumedUpstreams, pn.TxHash)
			merged = mergeRWSet(patch, merged)
			upstreamTxList = append(upstreamTxList, api.TxSimKey{
				TxHash:        pn.TxHash,
				SimulationNum: pn.SimulationNum,
			})
		}
		if allOk {
			matchedCount++
			pt.tx.Status = api.RetryConsumed
			rs.unsubscribeRetryTx(pt.txHash)
			rs.consumedPatches.Store(pt.txHash, consumedUpstreams)
			rs.sendChainSignal(pt.txHash, pt.tx, merged, upstreamTxList)
		} else {
			for _, txh := range consumedUpstreams {
				rs.releasePatch(txh)
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
