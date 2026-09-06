package ssc

import (
	"bytes"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	sscpb "github.com/harmony-one/harmony/ssc/api/proto"
	"google.golang.org/protobuf/proto"
)

// tempLockEntry records the lock holder and its priority for Wound-Wait.
type tempLockEntry struct {
	Holder   common.Hash  // retryTx holding this lock
	Priority api.Priority // holder's priority (used for wounding decisions)
}

// RWKeySet — 交易的读写集
type RWKeySet struct {
	Reads  []api.LockKey
	Writes []api.LockKey
}

// readHolderMap is the inner sync.Map for read-lock holders of a single key.
// key: txHash common.Hash, value: struct{}{}
type readHolderMap struct {
	sync.Map
}

// TempLockView — 临时锁视图（委员会 leader 维护）
//
// 全部使用 sync.Map 替代传统 map + Mutex，不同 tx / 不同 key 之间完全并行。
//   - tempWriteLocks: LoadOrStore 实现单 key 原子获取，失败回滚已拿 key
//   - tempReadLocks:  两层 sync.Map，O(1) 查找持有者
//   - txReadWriteSets: per-tx 数据，不同 tx 互不冲突
//   - onChainKeys:     SimTx 上链后该交易在链上持有的 key（txHash → *RWKeySet）。
//     SimTx 提交时从 txReadWriteSets 迁移过来，供后续 CRTx 提交时
//     产生 releasedKeys 唤醒等待这些 key 的重试交易。
//   - woundedTxs:      per-tx 标记，不同 tx 互不冲突
type TempLockView struct {
	tempWriteLocks   sync.Map // key: api.LockKey, value: tempLockEntry
	tempReadLocks    sync.Map // key: api.LockKey, value: *readHolderMap
	txReadWriteSets  sync.Map // key: common.Hash, value: *RWKeySet
	onChainKeys      sync.Map // key: common.Hash, value: *RWKeySet
	woundedTxs       sync.Map // key: common.Hash, value: struct{}{}
	stateLockManager *stateLockManager
}

func NewTempLockView(manager *stateLockManager) *TempLockView {
	return &TempLockView{
		stateLockManager: manager,
	}
}

// TryLock 尝试为交易获取临时读/写锁。
// 无优先级判断，等价于 TryLockWithPriority 但总是返回 wounded=false。
func (v *TempLockView) TryLock(txHash common.Hash, reads []api.LockKey, writes []api.LockKey) bool {
	locked, _ := v.TryLockWithPriority(txHash, api.Priority{}, reads, writes)
	return locked
}

// TryLockWithPriority 尝试为交易获取临时读/写锁，支持 Wound-Wait 优先级抢占。
//
// 返回：
//
//	locked:  是否成功获得所有锁
//	wounded: 是否被更高优先级的交易踢掉（仅当 locked=false 时有意义）
//
// 每把写锁通过 LoadOrStore 原子获取，不存在跨 key 全局锁。
// 部分 key 获取失败后回滚已拿的 key，后续 stateDB CheckLock / ForceSimulation 兜底。
func (v *TempLockView) TryLockWithPriority(txHash common.Hash, priority api.Priority, reads []api.LockKey, writes []api.LockKey) (locked bool, wounded bool) {
	tEntry := time.Now()
	defer func() {
		tTotal := time.Since(tEntry)
		if tTotal > 100*time.Millisecond {
			utils.SSCLogger().Warn().Str("txHash", txHash.Hex()).
				Dur("total", tTotal).
				Int("writes", len(writes)).
				Int("reads", len(reads)).
				Msg("TryLockWithPriority: slow")
		}
	}()

	v.stateLockManager.sscService.stats.TempLockTryTotal.Add(1)

	rwSet := &RWKeySet{
		Reads:  append([]api.LockKey(nil), reads...),
		Writes: append([]api.LockKey(nil), writes...),
	}

	// Step 1: 逐个原子获取写锁（LoadOrStore）— 失败则回滚
	var acquiredWrites []api.LockKey
	for _, key := range rwSet.Writes {
		entry := tempLockEntry{Holder: txHash, Priority: priority}
		existing, loaded := v.tempWriteLocks.LoadOrStore(key, entry)
		if !loaded {
			// key 原来为空，成功获取
			acquiredWrites = append(acquiredWrites, key)
			continue
		}
		// key 已被占
		existingEntry := existing.(tempLockEntry)
		if bytes.Equal(existingEntry.Holder.Bytes(), txHash.Bytes()) {
			// 自己占着，不算冲突
			continue
		}
		if v.canWound(existingEntry, priority, txHash) {
			// 可以 Wound → 替换
			v.woundedTxs.Store(existingEntry.Holder, struct{}{})
			// DSN-62 (B)：若被 wound 的是某下游已消费的上游，把它无效化并解绑该下游，
			// 避免下游链在一个 doomed 上游上。
			v.notifyUpstreamWounded(existingEntry.Holder)
			v.tempWriteLocks.Store(key, entry)
			acquiredWrites = append(acquiredWrites, key)
			utils.SSCLogger().Debug().Str("wounded", existingEntry.Holder.Hex()).
				Str("wounder", txHash.Hex()).
				Str("key", string(key)).
				Msg("Wound: high priority tx took lock from low priority")
			continue
		}
		// 被高优先级占着（或 Finalized）→ 失败，回滚已拿的写锁
		v.stateLockManager.sscService.stats.TempLockTryFail.Add(1)
		utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
			Str("holder", existingEntry.Holder.Hex()).
			Str("key", string(key)).
			Msg("TryLockWithPriority: tempWriteLock conflict, cannot wound")
		// 回滚
		for _, k := range acquiredWrites {
			if val, ok := v.tempWriteLocks.Load(k); ok {
				entry := val.(tempLockEntry)
				if bytes.Equal(entry.Holder.Bytes(), txHash.Bytes()) {
					v.tempWriteLocks.Delete(k)
				}
			}
		}
		return false, false
	}

	// Step 2: 检查+注册读锁
	for _, key := range rwSet.Reads {
		// 检查该 key 是否被别的 tx 写锁着
		if existing, loaded := v.tempWriteLocks.Load(key); loaded {
			existingEntry := existing.(tempLockEntry)
			if !bytes.Equal(existingEntry.Holder.Bytes(), txHash.Bytes()) {
				if !v.canWound(existingEntry, priority, txHash) {
					// 读锁被高优写锁占着 → 失败，回滚已拿的写锁
					v.stateLockManager.sscService.stats.TempLockTryFail.Add(1)
					utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
						Str("holder", existingEntry.Holder.Hex()).
						Str("key", string(key)).
						Msg("TryLockWithPriority: read-tempWriteLock conflict, cannot wound")
					for _, k := range acquiredWrites {
						if val, ok := v.tempWriteLocks.Load(k); ok {
							entry := val.(tempLockEntry)
							if bytes.Equal(entry.Holder.Bytes(), txHash.Bytes()) {
								v.tempWriteLocks.Delete(k)
							}
						}
					}
					return false, false
				}
				// 可以 wound 写锁持有者 → 升级为写锁
				v.woundedTxs.Store(existingEntry.Holder, struct{}{})
				// DSN-62 (B)：同写锁 wound，把被 wound 的上游无效化并解绑其下游。
				v.notifyUpstreamWounded(existingEntry.Holder)
				v.tempWriteLocks.Store(key, tempLockEntry{Holder: txHash, Priority: priority})
				acquiredWrites = append(acquiredWrites, key)
				utils.SSCLogger().Debug().Str("wounded", existingEntry.Holder.Hex()).
					Str("wounder", txHash.Hex()).
					Str("key", string(key)).
					Msg("Wound (readset): high priority tx took read lock from low priority")
				continue
			}
		}
		// 注册读锁
		v.addReadLock(key, txHash)
	}

	// Step 3: 注册交易读写集（sync.Map，无锁）
	v.txReadWriteSets.Store(txHash, rwSet)
	return true, false
}

// addReadLock 为 tx 注册对 key 的读锁。
// 内层 readHolderMap 以 txHash 为 key，不同 tx 的 Store 不冲突。
func (v *TempLockView) addReadLock(key api.LockKey, txHash common.Hash) {
	holdersVal, _ := v.tempReadLocks.LoadOrStore(key, &readHolderMap{})
	holders := holdersVal.(*readHolderMap)
	holders.Store(txHash, struct{}{})
}

// canWound 检查是否可以踢掉当前锁持有者。
// 返回 false（不可被 wound）的条件：
//   - 持有者优先级更高/相等；
//   - 持有者的 Patch 已 Finalized；
//   - 持有者已被标记为被动链保护（DSN-62 (D) chainProtected）——不可被 wound，但仍可被消费。
func (v *TempLockView) canWound(entry tempLockEntry, requesterPri api.Priority, requester common.Hash) bool {
	// 检查 PatchPool 中该持有者的 Patch 是否已 Finalized
	if v.stateLockManager.sscService.retryScheduler.offChainDAG.isPatchFinalized(entry.Holder) {
		return false
	}
	// DSN-62 (D)：被动链保护标记 → 不可被 wound（与 Finalized 解耦，不影响可消费性）
	if v.stateLockManager.sscService.retryScheduler.offChainDAG.isChainProtected(entry.Holder) {
		return false
	}
	// 如果持有者优先级更低（数值更大），可以踢
	return requesterPri.Less(entry.Priority)
}

// protectHeldLocks — DSN-55 rev2：DAG 救援交易得锁后，把其已持有的每把 TLV 写锁的
// 优先级提到最高（Nonce=0, Origin=0, TxHash=0）。这样后续任何 requester 的
// requesterPri.Less(holderPri) 恒为 false → canWound 不放行 → 该锁不被后来者 wound 抢走，
// 直到它走上链(变真链上锁)或失败被 RetryCancel/GC 释放（锁删除后优先级随之作废）。
// 注意：只在“得锁成功后”调用；抢锁阶段(TryLockWithPriority 内)仍按普通全局 Priority，公平竞争。
func (v *TempLockView) protectHeldLocks(txHash common.Hash) {
	if v == nil {
		return
	}
	// 最高优先哨兵：任何真实交易都无法 Less 于它 → 不可被 wound
	maxPri := api.Priority{Nonce: 0, OriginShardID: 0, TxHash: common.Hash{}}
	v.tempWriteLocks.Range(func(k, val interface{}) bool {
		e := val.(tempLockEntry)
		if bytes.Equal(e.Holder.Bytes(), txHash.Bytes()) {
			e.Priority = maxPri
			v.tempWriteLocks.Store(k, e)
		}
		return true
	})
}

// IsHeldProtected 判断某 tx 是否已通过 protectHeldLocks 提升（任一把其持有的写锁达到最高优先）。
// 供打点/诊断。
func (v *TempLockView) IsHeldProtected(txHash common.Hash) bool {
	if v == nil {
		return false
	}
	protected := false
	v.tempWriteLocks.Range(func(_, val interface{}) bool {
		e := val.(tempLockEntry)
		if bytes.Equal(e.Holder.Bytes(), txHash.Bytes()) && e.Priority.Nonce == 0 {
			protected = true
			return false
		}
		return true
	})
	return protected
}

// IsWounded 检查当前交易是否已被 Wound（被更高优先级的交易踢掉）。
func (v *TempLockView) IsWounded(txHash common.Hash) bool {
	_, exists := v.woundedTxs.Load(txHash)
	return exists
}

// ClearWounded 清理指定交易的 wounded 标记（在交易重试开始时调用）。
func (v *TempLockView) ClearWounded(txHash common.Hash) {
	v.woundedTxs.Delete(txHash)
}

// notifyUpstreamWounded — DSN-62 (B)：被 Wound 的 tx 若正被某下游当作“已消费上游”，则通知
// retryScheduler 把它无效化并解绑该下游（见 retryScheduler.notifyWoundedUpstream）。
// 放在 TryLockWithPriority 的 wound 之后；nil 保护，绝不阻塞/影响 wound 主流程。
func (v *TempLockView) notifyUpstreamWounded(holder common.Hash) {
	if v == nil || v.stateLockManager == nil || v.stateLockManager.sscService == nil {
		return
	}
	rs := v.stateLockManager.sscService.retryScheduler
	if rs == nil {
		return
	}
	rs.notifyWoundedUpstream(holder)
}

// IsTempLockedBySelf 检查当前交易是否持有该 key 的 TempLock（写锁或读锁）。
// 用于 GetState/SetState 的锁仲裁：如果自己持有 TempLock，可以跳过 SLM 检查。
func (v *TempLockView) IsTempLockedBySelf(txHash common.Hash, lockKey api.LockKey) bool {
	// 检查写锁
	if val, loaded := v.tempWriteLocks.Load(lockKey); loaded {
		entry := val.(tempLockEntry)
		return bytes.Equal(entry.Holder.Bytes(), txHash.Bytes())
	}

	// 检查读锁 holder 列表
	if holdersVal, loaded := v.tempReadLocks.Load(lockKey); loaded {
		holders := holdersVal.(*readHolderMap)
		_, exists := holders.Load(txHash)
		return exists
	}

	return false
}

// HasConflict 检查指定 key 是否被**其他**交易在 TempLockView 中预约。
// 用于 GetState/SetState 的锁仲裁：若当前 tx 自己持有，不算冲突。
// lockKey 应为 api.FormKey(address, key) 的结果。
// GetTempLockHolder 返回某写锁 key 在 TLV 中的持有者及其优先级（用于链下模拟忽略低优先锁，Part A）。
func (v *TempLockView) GetTempLockHolder(key api.LockKey) (common.Hash, api.Priority, bool) {
	if val, ok := v.tempWriteLocks.Load(key); ok {
		entry := val.(tempLockEntry)
		if entry.Holder != (common.Hash{}) {
			return entry.Holder, entry.Priority, true
		}
	}
	return common.Hash{}, api.Priority{}, false
}

func (v *TempLockView) HasConflict(txHash common.Hash, lockKey api.LockKey) bool {
	// 检查写锁
	if val, loaded := v.tempWriteLocks.Load(lockKey); loaded {
		entry := val.(tempLockEntry)
		if !bytes.Equal(entry.Holder.Bytes(), txHash.Bytes()) {
			return true // 其他交易持有写锁
		}
		return false // 自己持有，不算冲突
	}

	// 检查读锁
	if holdersVal, loaded := v.tempReadLocks.Load(lockKey); loaded {
		holders := holdersVal.(*readHolderMap)
		hasOther := false
		holders.Range(func(k, _ interface{}) bool {
			if !bytes.Equal(k.(common.Hash).Bytes(), txHash.Bytes()) {
				hasOther = true
				return false
			}
			return true
		})
		return hasOther
	}

	return false
}

// CanLock 是 TryLock 的只读版本，不产生实际锁操作。
func (v *TempLockView) CanLock(txHash common.Hash, reads []api.LockKey, writes []api.LockKey) bool {
	for _, key := range writes {
		if val, loaded := v.tempWriteLocks.Load(key); loaded {
			entry := val.(tempLockEntry)
			if !bytes.Equal(entry.Holder.Bytes(), txHash.Bytes()) {
				return false
			}
		}
	}

	for _, key := range reads {
		if val, loaded := v.tempWriteLocks.Load(key); loaded {
			entry := val.(tempLockEntry)
			if !bytes.Equal(entry.Holder.Bytes(), txHash.Bytes()) {
				return false
			}
		}
	}
	return true
}

// OnBlockCommitted 清理区块中内部交易（SimTx/CRTx）对应的 TLV 临时锁，并返回被释放的 key 集合。
//
// DSN-47 §9：跨分片交易上链产物已从普通交易（block.Transactions()）迁移到 SSC 内部交易
// （block.SSCTransactions()）。普通交易从不登记 TLV 锁（txReadWriteSets 只被 RetryCommit 写入），
// 因此这里只处理内部交易：
//   - SimTx 上链：链上锁获得，TLV 临时锁作废。把 TLV 记录迁移到 onChainKeys（供 CRTx 唤醒），
//     并释放 TLV 锁、上报 releasedKeys（等待者会被 retryScheduler 的 CheckLock 过滤；
//     若该 SimTx 验证失败已回滚，则这些 key 立即可用）。
//   - CRTx 上链：链上锁释放。消费 onChainKeys（或 txReadWriteSets）中该交易的 key，
//     释放 TLV 锁并上报 releasedKeys → 唤醒等待这些 key 的重试交易。
func (v *TempLockView) OnBlockCommitted(block *types.Block) []api.LockKey {
	t0 := time.Now()

	// 统计日志（大致计数）
	writeLockCount := 0
	v.tempWriteLocks.Range(func(_, _ interface{}) bool {
		writeLockCount++
		return true
	})
	readLockCount := 0
	v.tempReadLocks.Range(func(_, _ interface{}) bool {
		readLockCount++
		return true
	})

	var releasedKeys []api.LockKey
	processedTx := 0

	ssc := block.SSCTransactions()

	// SimTx 桶（InternalTxTypeSimTx）
	if len(ssc) > int(types.InternalTxTypeSimTx) {
		for _, tx := range ssc[types.InternalTxTypeSimTx] {
			if tx == nil {
				continue
			}
			txHash, ok := v.simTxOriginalHash(tx)
			if !ok {
				continue
			}
			processedTx++
			rwSetVal, exists := v.txReadWriteSets.Load(txHash)
			if !exists {
				v.woundedTxs.Delete(txHash)
				continue
			}
			rwSet := rwSetVal.(*RWKeySet)
			v.txReadWriteSets.Delete(txHash)
			v.woundedTxs.Delete(txHash)
			// 该交易现在在链上持有这些 key → 迁移到 onChainKeys，供后续 CRTx 消费
			v.onChainKeys.Store(txHash, rwSet)
			v.releaseTLVLock(txHash, rwSet)
			// 上报全部 key：等待者会被 CheckLock 过滤（若 SimTx 验证失败已回滚则立即可用）
			releasedKeys = append(releasedKeys, rwSet.Writes...)
			releasedKeys = append(releasedKeys, rwSet.Reads...)
		}
	}

	// CRTx 桶（InternalTxTypeCRTx）
	if len(ssc) > int(types.InternalTxTypeCRTx) {
		for _, tx := range ssc[types.InternalTxTypeCRTx] {
			if tx == nil {
				continue
			}
			txHash, ok := v.crTxOriginalHash(tx)
			if !ok {
				continue
			}
			processedTx++
			var rwSet *RWKeySet
			if rwSetVal, exists := v.onChainKeys.Load(txHash); exists {
				rwSet = rwSetVal.(*RWKeySet)
				v.onChainKeys.Delete(txHash)
			} else if rwSetVal, exists := v.txReadWriteSets.Load(txHash); exists {
				rwSet = rwSetVal.(*RWKeySet)
				v.txReadWriteSets.Delete(txHash)
			}
			if rwSet == nil {
				v.woundedTxs.Delete(txHash)
				continue
			}
			v.woundedTxs.Delete(txHash)
			v.releaseTLVLock(txHash, rwSet)
			// 链上锁已释放 → 上报全部 key，唤醒等待者
			releasedKeys = append(releasedKeys, rwSet.Writes...)
			releasedKeys = append(releasedKeys, rwSet.Reads...)
		}
	}

	tCleanupLoop := time.Since(t0)
	utils.SSCLogger().Info().
		Uint64("blockNum", block.NumberU64()).
		Int("txCount", processedTx).
		Int("releasedKeys", len(releasedKeys)).
		Int("keys", writeLockCount+readLockCount).
		Dur("cleanupLoop", tCleanupLoop).
		Dur("total", time.Since(t0)).
		Msg("[TempLockView] OnBlockCommitted timing breakdown")

	// Deduplicate released keys
	seen := make(map[api.LockKey]struct{})
	unique := releasedKeys[:0]
	for _, key := range releasedKeys {
		if _, exists := seen[key]; !exists {
			seen[key] = struct{}{}
			unique = append(unique, key)
		}
	}
	return unique
}

// releaseTLVLock 释放一笔交易在 TLV 中占用的读写锁。
// 只做 TLV 清理，不返回 key；releasedKeys 由调用方按 rwSet 全量上报。
func (v *TempLockView) releaseTLVLock(txHash common.Hash, rwSet *RWKeySet) {
	for _, key := range rwSet.Writes {
		if val, ok := v.tempWriteLocks.Load(key); ok {
			entry := val.(tempLockEntry)
			if bytes.Equal(entry.Holder.Bytes(), txHash.Bytes()) {
				v.tempWriteLocks.Delete(key)
			}
		}
	}
	for _, key := range rwSet.Reads {
		if holdersVal, ok := v.tempReadLocks.Load(key); ok {
			holders := holdersVal.(*readHolderMap)
			holders.Delete(txHash)
		}
	}
}

// simTxOriginalHash 从 SimTx payload 解出原始跨分片交易 hash。
// 注意：不能直接用 tx.Hash()（那是内部交易自身的 hash，不是原始跨分片交易 hash）。
func (v *TempLockView) simTxOriginalHash(tx *types.SSCInternalTx) (common.Hash, bool) {
	sim := &sscpb.CXTSimulation{}
	if err := proto.Unmarshal(tx.Payload, sim); err != nil {
		utils.SSCLogger().Debug().Err(err).Msg("[TempLockView] SimTx unmarshal failed")
		return common.Hash{}, false
	}
	apiSim := sscpb.CXTSimulationFromProto(sim)
	if apiSim == nil {
		return common.Hash{}, false
	}
	return apiSim.TxHash, true
}

// crTxOriginalHash 从 CRTx payload 解出原始跨分片交易 hash。
func (v *TempLockView) crTxOriginalHash(tx *types.SSCInternalTx) (common.Hash, bool) {
	proof := &sscpb.CXTCommitProof{}
	if err := proto.Unmarshal(tx.Payload, proof); err != nil {
		utils.SSCLogger().Debug().Err(err).Msg("[TempLockView] CRTx unmarshal failed")
		return common.Hash{}, false
	}
	apiProof := sscpb.CXTCommitProofFromProto(proof)
	if apiProof == nil {
		return common.Hash{}, false
	}
	return apiProof.TxHash, true
}

// GarbageCollect 清理 stale 交易（如 nonce 过期）。
func (v *TempLockView) GarbageCollect(staleTxHash common.Hash) {
	// 清理 onChainKeys（若该交易已 SimTx 上链但尚未 CRTx）
	v.onChainKeys.Delete(staleTxHash)

	// 取出 tx 的读写集
	rwSetVal, exists := v.txReadWriteSets.Load(staleTxHash)
	if !exists {
		v.woundedTxs.Delete(staleTxHash)
		return
	}
	rwSet := rwSetVal.(*RWKeySet)

	// 清理写锁
	for _, key := range rwSet.Writes {
		if val, ok := v.tempWriteLocks.Load(key); ok {
			entry := val.(tempLockEntry)
			if bytes.Equal(entry.Holder.Bytes(), staleTxHash.Bytes()) {
				v.tempWriteLocks.Delete(key)
			}
		}
	}
	// 清理读锁
	for _, key := range rwSet.Reads {
		if holdersVal, ok := v.tempReadLocks.Load(key); ok {
			holders := holdersVal.(*readHolderMap)
			holders.Delete(staleTxHash)
		}
	}
	v.txReadWriteSets.Delete(staleTxHash)
	v.woundedTxs.Delete(staleTxHash)
}

// Stats 返回 TLV 各 sync.Map 的条目数
func (v *TempLockView) Stats() (writeLocks, readLocks, txSets, wounded int) {
	v.tempWriteLocks.Range(func(_, _ any) bool { writeLocks++; return true })
	v.tempReadLocks.Range(func(_, _ any) bool { readLocks++; return true })
	v.txReadWriteSets.Range(func(_, _ any) bool { txSets++; return true })
	v.onChainKeys.Range(func(_, _ any) bool { txSets++; return true })
	v.woundedTxs.Range(func(_, _ any) bool { wounded++; return true })
	return
}
