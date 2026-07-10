# [DSN-26] PatchPool DAG 扩展至 TLV Phase 1 — 用 Patch 覆盖 TLV 锁冲突

> **版本**：v1（2026-07-09）
> **状态**：设计阶段
> **关联文档**：`DSN-23-patchpool-dag-design.md`（PatchPool DAG）、`DSN-24-unified-lock-check.md`（三层锁仲裁）、`DSN-22-retrycommit-lock-order-fix.md`（三阶段 RetryCommit）

---

## 1. Problem Statement

### 1.1 漏斗数据

2026-07-09 实验（4 shards, RATE=100, delay=10）：

```
RetryCommit total:                    80,606 (100%)
  ├─ Phase 1 (TLV) fail:             48,483 (60.1%)  ← 在这就死了
  ├─ Phase 2a (stateDB) pass → OK:   30,095 (37.3%)
  └─ Phase 2b (DAG) entry:            1,158 (1.4%)
       ├─ DAG hit:      403 (34.8%)
       └─ DAG miss:     755 (65.2%)
```

**关键矛盾：**

- Phase 1 TLV 失败了 **48,483 次**——占了所有 RetryCommit 尝试的 60%
- Phase 2b 的 DAG 机会池只有 **1,158 次**——因为 Phase 2a 检查之前 TLV 已经过滤了绝大部分
- DAG 在 Phase 2b 的命中率 **34.8%** 其实不低，但受限于机会池太小，绝对命中数仅 **403 次**
- Phase 1 TLV 失败中，如果 DAG 也能以类似比例覆盖，**额外命中 ~16,872 次**，是当前 403 次的 **42×**

### 1.2 为什么不直接扩展

TLV 和 stateDB 的锁语义不同：

| 锁系统 | 持有者 | 锁含义 | 能否被 Patch 覆盖 |
|:-------|:------|:-------|:------------------|
| **stateDB lock**（Phase 2a 检查） | 链上 SimTx | "此 key 已被提交，等我 CR 完成" | ✅ **可以**——SimTx 已提交，它的 RWSet 作为 Patch 在 Pool 里 |
| **TLV lock**（Phase 1 检查） | 重试中 tx | "我预约了要重试这个 key，别抢" | ❓ **待验证**——如果持有者正在模拟并即将提交 SimTx，它的 Patch 也在 Pool 里 |

**关键条件：TLV 锁持有者的 Patch 必须在 PatchPool 中才有效。** TLV 锁的持有者可能正处于 `RetryCommit` → `StartReSimulation` → `CommitSimulation` 的流程中，还没提交 SimTx → 它的 Patch 还没入 Pool → 无法覆盖。

---

## 2. 设计方案

### 2.1 核心思路

在 `RetryCommit Phase 1`（TLV TryLock 失败后），不直接返回 `Locked: false`，而是先查 PatchPool 是否有 patch 可以覆盖 TLV 冲突 key。

```
当前：
  Phase 1 TLV TryLock → fail → return Locked: false

DSN-26：
  Phase 1 TLV TryLock → fail
    → 找出 TLV 冲突 key 集
    → FindCoveringSet(conflictKeys)  ← 复用 DAG 算法
        ├─ 命中 → TryConsume + mergeRWSet → return Locked: true ✅
        └─ 没命中 → return Locked: false
```

### 2.2 数据流

```go
// RetryCommit (retry_scheduler.go)
func (rs *retryScheduler) RetryCommit(txHash common.Hash, ...) {

    // ── Phase 1: TLV TryLock ──
    locked, wounded := rs.tempLockView.TryLockWithPriority(txHash, priority, reads, writes)
    if locked {
        goto Phase2
    }

    // ── Phase 1b: TLV fail → 查 PatchPool DAG 补救 ──
    tlvConflictKeys := rs.tempLockView.GetConflictKeys(txHash, retryTx.ReadSet, retryTx.WriteSet)
    patches := rs.patchPool.FindCoveringSet(tlvConflictKeys)
    if len(patches) > 0 {
        // Patch 覆盖全部 TLV 冲突 key → 跳过 TLV 锁
        var consumed []common.Hash
        var merged *api.RWSet
        allConsumed := true
        for _, node := range patches {
            patch := rs.patchPool.TryConsume(node.TxHash, txHash, priority)
            if patch == nil {
                allConsumed = false
                break
            }
            consumed = append(consumed, node.TxHash)
            merged = mergeRWSet(patch, merged)
        }
        if allConsumed {
            rs.state.SetChainPatch(txHash, merged)
            rs.consumedPatches.Store(txHash, consumed)
            rs.tempLockView.ClearWounded(txHash)
            chainRetryStats.SigRetryCommitPatchHit.Add(1)
            return RetryCommitResp{Locked: true, TxHash: txHash}
        }
        // 部分失败 → Release
        for _, txh := range consumed {
            rs.patchPool.Release(txh)
        }
    }

    // ── 真冲突，无 patch 可救 → 原逻辑 ──
    rs.tempLockView.GarbageCollect(txHash)
    return RetryCommitResp{Locked: false, TxHash: txHash}

Phase2:
    // ── Phase 2: stateDB CheckLock（不变）──
    ...
}
```

### 2.3 新增接口

**① `tempLockView.GetConflictKeys`** — 返回 TLV 视角下某笔 tx 的冲突 key 集

```go
// temp_lock_view.go
func (v *TempLockView) GetConflictKeys(txHash common.Hash, reads, writes []api.LockKey) []api.LockKey {
    v.mu.RLock()
    defer v.mu.RUnlock()
    var conflicts []api.LockKey
    
    for _, key := range writes {
        if entry, exists := v.tempWriteLocks[key]; exists && entry.txHash != txHash {
            conflicts = append(conflicts, key)
        }
    }
    for _, key := range reads {
        if entry, exists := v.tempReadLocks[key]; exists && entry.txHash != txHash {
            conflicts = append(conflicts, key)
        }
    }
    return conflicts
}
```

**② 日志新增** — `retryCommit failed: try lock failed, DAG rescued`

当 Phase 1b DAG 挽救成功时，打印 Info 日志以便统计。

### 2.4 预期效果

| 指标 | 当前 | 预期（估算） |
|:-----|:----:|:-----------:|
| Phase 1 fail → DAG 机会池 | 0（没查） | ~48,483 |
| DAG 命中率（Phase 1b） | — | ~34.8%（对齐 Phase 2b 的 FindCoveringSet 效率） |
| **额外 RetryOK（Phase 1b DAG hit）** | — | **~16,872** |
| 总 RetryOK | 30,095 | **~46,967（+56%）** |
| DAG 占总 RetryOK 比例 | 1.3% | **~36%** |

---

## 3. 风险与注意事项

### 3.1 TLV 锁持有者的 Patch 可能尚未入 Pool

TLV 锁的持有者（`txA`）可能还在执行中——RetryCommit 成功了但还没提交 SimTx，它的 RWSet 还不在 PatchPool 里。

**缓解**：这是一个**时序问题**。`txA` 的 Locked: true 到它提交 SimTx、Patch 入 Pool 之间有间隔。如果 `txB` 在这个间隔内查 TLV → 发现冲突 → 查 PatchPool → 没命中 → 返回 fail。等 `txA` 的 Patch 入 Pool 后，`txB` 会在下一轮 OnBlockCommitted 中重新被调度。**不会丢 tx**，只是延迟一轮。

### 3.2 已有 Wound 机制冲突

如果 TLV 锁是通过 Wound 机制被抢占的（owner B 被 Wound，释放了锁），那 owner B 的 Patch 可能已经被 `Release` 了。此时查 PatchPool 不会命中 owner B 的 patch。

**缓解**：被 Wound 的 tx 其 Patch 已经 Release → 自然不会被 FindCoveringSet 找到。这是正确的行为。

### 3.3 多 Patch TLV 释放

Phase 1b 成功后，如果后续 Phase 2a 也发现了 stateDB 冲突且 DAG 再次命中，两套 consumed patches 可能重叠重叠。

**修复**：用 `rs.consumedPatches` 统一管理，`closeTransaction` / `GarbageCollect` 时统一 Release。

---

## 4. 验证标准

| # | 验证项 | 指标 |
|:-:|:-------|:-----|
| 1 | Phase 1b DAG 命中数 | > 10,000（占 Phase 1 fail 的 >20%） |
| 2 | 总 RetryOK 提升 | 从 30,095 提升至 > 40,000 |
| 3 | LockConflict → CallForRetry 减少 | OnChainLockConflict 比例下降 |
| 4 | DAG 命中日志 | `retryCommit: found DAG patches in PatchPool, rescued from TLV lock` |
