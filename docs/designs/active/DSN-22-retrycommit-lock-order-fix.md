# [DSN-22] RetryCommit 锁检查顺序修正 — PatchPool 仅覆盖 stateDB 冲突

> **版本**：v1（2026-07-07）
> **状态**：设计阶段
> **关联文档**：`designs/active/DSN-09-active-passive-retry-pool.md`（被动池）、`designs/active/DSN-05-hotkey-retry-design.md`（PatchPool 原始设计）

---

## 1. Problem Statement

### 1.1 问题：PatchPool 无条件跳锁导致模拟死循环

当前 `RetryCommit` 的锁检查顺序为：

```
1. PatchPool.HasConflict(retryTx)?
     └─ 匹配 → TryConsume → SetChainPatch → 跳过所有锁 → Locked=true
     └─ 不匹配 → TryLockWithPriority → stateDB.CheckLock → 正常路径
```

`HasConflict` 只要 retryTx 的 **任意 1 个 key** 与 Patch 的 WriteSet 重合就算命中。选覆盖率最高的 Patch，但不检查 retryTx 的全部 key 是否都被 Patch 覆盖。

这导致以下死循环：

```
retryTx 的 key 集合 = {K1, K2, K3}
上游 SimTx 的 Patch WriteSet = {K1}

RetryCommit:
  HasConflict(K1 匹配) → 命中 ✅
  TryConsume → SetChainPatch({K1}) → Locked=true ✅
  ← stateDB 完全没查

O 端 tryToReSimulation:
  所有 shard Locked=true → TriggerReSimulation

StartReSimulation:
  GetState(K1) → ChainPatch 命中 → 返回上游值 ✅
  GetState(K2) → ChainPatch 不命中 → stateDB.GetState → 被其他 tx 锁着 ❌
  "resimulation failed: state is locked by other tx on chain"
  → CallForRetry → 重新入池
  → 下一轮 Patch 又命中 K1 → 又循环
```

**核心矛盾**：PatchPool 的设计意图是「已知上游 SimTx 会释放这些 key 的锁，先允许下游交易按依赖链使用」。但当前实现中 PatchPool **只要求至少 1 个 key 重合**，而 retryTx 的**未覆盖 key** 在 stateDB 中确实被锁着，导致模拟阶段真实锁冲突。

### 1.2 目标

修正 `RetryCommit` 的锁检查顺序，使得：
1. TempLockView 层面先做并发控制（Wound-Wait）
2. stateDB 做真实锁检查
3. PatchPool **仅在 stateDB 冲突后**介入，只覆盖真正冲突的 key

消除「PatchPool 跳锁 → 模拟时 stateDB 真实锁冲突」的死循环。

---

## 2. 设计方案

### 2.1 核心思路：两段式 RetryCommit

新的 `RetryCommit` 逻辑拆为两段：

```
Phase 1: 锁竞争
  TryLockWithPriority (tempLockView)
    → 冲突/被踢 → Locked=false
    → 通过 → 继续

  stateDB.CheckLock (遍历所有 key)
    → 全通过 → Locked=true ✅
    → 有冲突 → 记下冲突 key 集合 → 进 Phase 2

Phase 2: PatchPool 补救
  用冲突 key 集合查 PatchPool
    → 有单笔 Patch 覆盖全部冲突 key → TryConsume → Locked=true ✅
    → 没有 Patch 能覆盖 → OnChainLockConflict=true
```

### 2.2 三阶段详细流程

```
RetryCommit(txHash):

  [前置检查]
  1. 被动池 → 唤醒
  2. IsWounded → 被踢就直接失败
  3. retryPool 存在检查 → 不存在则失败
  4. IsLeader 检查 → 不是 leader 则失败

  ──────────────────────────────────────────────
  Phase 1: TempLockView 锁竞争
  ──────────────────────────────────────────────
  5. TryLockWithPriority(tempLockView, reads, writes)
       ├─ Locked=false, Wounded=true → 被高优先级踢了
       └─ Locked=false, Wounded=false → 锁被别的交易占着且 wound 不掉
            → return Locked=false, OnChainLockConflict=false
       └─ Locked=true → 持有 tempLockView 锁 → 继续

  ──────────────────────────────────────────────
  Phase 2: stateDB 真实锁检查
  ──────────────────────────────────────────────
  6. getStateDB() → bc.State()
       ├─ 失败 → return Locked=false (tryLockFail)
       └─ 成功 → stateDB

  7. 遍历 retryTx.WriteSet 每个 key → stateDB.CheckLock
     + 遍历 retryTx.ReadSet 每个 key → stateDB.CheckLock
       ├─ 全部通过 → 跳到 Phase 3（Locked=true）
       └─ 有冲突 key → 记录冲突集合 conflictKeys → 进 Phase 2b

  ──────────────────────────────────────────────
  Phase 2b: PatchPool 补救（仅覆盖冲突 key）
  ──────────────────────────────────────────────
  8. 用 conflictKeys 查 PatchPool
     在 PatchPool.KeyIndex 中找能覆盖全部 conflictKeys 的单笔 Patch
       ├─ 找到 → TryConsume → SetChainPatch → Locked=true ✅
       │        （Patch 只覆盖冲突 key，不涉及 tempLockView 锁）
       └─ 没找到 → 释放已持有的 tempLockView 锁
               → return Locked=false, OnChainLockConflict=true

  ──────────────────────────────────────────────
  Phase 3: 成功路径
  ──────────────────────────────────────────────
  9. ClearWounded
  10. return Locked=true
```

### 2.3 关键设计点

**① Phase 1 与 Phase 2 的锁关系**

TempLockView 和 stateDB 是两套正交的锁（见 2.4 分析）。Phase 1 成功后交易的 tempLockView 锁持有着，但 stateDB 检查仍可能冲突。这是因为另一个交易已经在 stateDB 层面锁住了该 key（它的 SimTx 进了链但还没 CommitTx 释放锁）。

Phase 2b 失败时需要**显式释放已持有的 tempLockView 锁**（调用 `GarbageCollect`），否则下轮 `TryLockWithPriority` 时自己的 tempLock 锁还会占着，但 stateDB 锁已经被释放了——这就回避了正确的锁竞争。

**② PatchPool 只覆盖 conflictKeys，不是 retryTx 的全部 key**

当前代码 `HasConflict` 返回覆盖率最高的 Patch（匹配 key 最多的那一个），然后无条件跳锁。新逻辑改为：**先收集 stateDB 冲突了哪些 key，只要求 Patch 覆盖这些 key**。retryTx 里没冲突的 key 不需要 Patch 覆盖——它们已经在 stateDB 层面通过了检查。

**③ PatchPool 的 `HasConflict` 改为按冲突 key 匹配**

当前 `HasConflict` 的签名是：
```go
func (pp *PatchPool) HasConflict(retryTx *RetryTx) (bool, *ChainPatchNode)
```

改为按给定 key 集合匹配：
```go
func (pp *PatchPool) FindCovering(conflictKeys []api.LockKey) (bool, *ChainPatchNode)
```

检查是否存在单笔 Patch 的 KeyIndex 覆盖了全部 `conflictKeys` 中的 key（非 Finalized、非 Consumed）。

**④ 单上游约束**

找到覆盖全部冲突 key 的 Patch 后，调用 `TryConsume` 将其标记为 Consumed。之后本分片内其他 retryTx 不能再消耗这笔 Patch（Patch 只能被一个下游消耗）。这符合 Patch 设计中的依赖链约束：一笔上游 SimTx 只能提供一个下游使用。

### 2.4 TempLockView vs stateDB 正交分析

| | TempLockView | stateDB Lock |
|:----|:-------------|:-------------|
| **作用域** | leader 节点内存 | 区块链全局 stateDB |
| **生命周期** | 模拟开始 → OnBlockCommitted | SimTx 提交进块 → CommitTx/RollbackTx |
| **解锁时机** | 每块 `OnBlockCommitted` 清理已提交交易 | `CommitTx`（合并到 baseSnapshot）或 `RollbackTx` |
| **本质** | leader 端并发控制：「同一交易别同时在多个分片上模拟」 | 链上状态一致性：「key 被 A 写了，B 要等 A 提交」 |

两者正交意味着：**即使 TempLockView 锁通过了（没有别的交易在 leader 端竞争同一交易），stateDB 层面也可能有链上锁冲突（之前提交的同 key SimTx 还没 CommitTx）**。这也正是 Phase 1 成功但 Phase 2 失败的根本原因。

### 2.5 对现有流程的影响

| 场景 | 当前行为 | 新逻辑行为 |
|:----|:---------|:----------|
| retryTx 所有 key 都无 stateDB 冲突 | PatchPool 先跳锁 | 直接 Phase 2 通过，不进 PatchPool |
| retryTx 的 key 全部被单笔 Patch 覆盖 | 和现在一样 | 一致（但通过 Phase 2 确认后进 Phase 2b） |
| retryTx 的 key 部分被 Patch 覆盖 | PatchPool 跳锁 → 模拟时撞锁 | Phase 2 发现冲突 → 冲突 key 不被 Patch 覆盖 → OnChainLockConflict=true |
| no Patch 匹配 | 走 TryLock/stateDB 正常路径 | 和现在一样 |

**关键变化**：之前 PatchPool 跳锁的交易中，部分 key 不被覆盖导致模拟死循环的那些，现在会被正确拦在 `OnChainLockConflict` 上。O 端收到后，可以走被动池逻辑或正常的 retry 等 stateDB 解锁。

---

## 3. 代码变更清单

### 3.1 `ssc/api/types.go`

**新增 `FindCovering` 方法**（替代当前 `HasConflict` 的语义）：

```go
// FindCovering checks if a single Patch in the Pool covers all given conflictKeys.
// Returns the matching node if found (non-Finalized, non-Consumed).
func (pp *PatchPool) FindCovering(conflictKeys []api.LockKey) (bool, *ChainPatchNode)
```

### 3.2 `ssc/retry_scheduler.go`

**修改 `RetryCommit`**（~line 1024-1141）：

```go
func (rs *retryScheduler) RetryCommit(txHash common.Hash) *api.RetryCommitResp {
    // [前置检查：被动池、IsWounded、retryPool、IsLeader]
    // ...保持不变...

    // Phase 1: TempLockView 锁竞争
    locked, wounded := rs.tempLockView.TryLockWithPriority(txHash, priority, ...)
    if wounded || !locked {
        return &api.RetryCommitResp{Locked: false, ...}
    }

    // Phase 2: stateDB 真实锁检查
    stateDB, err := rs.getStateDB(txHash)
    if err != nil {
        rs.tempLockView.GarbageCollect(txHash)
        return &api.RetryCommitResp{Locked: false, ...}
    }

    conflictKeys := make([]api.LockKey, 0)
    for _, key := range retryTx.WriteSet {
        if err := stateDB.CheckLock(key, txHash); err != nil {
            conflictKeys = append(conflictKeys, key)
        }
    }
    for _, key := range retryTx.ReadSet {
        if err := stateDB.CheckLock(key, txHash); err != nil {
            conflictKeys = append(conflictKeys, key)
        }
    }
    if len(conflictKeys) == 0 {
        // 全部通过 → Locked=true
        rs.tempLockView.ClearWounded(txHash)
        return &api.RetryCommitResp{Locked: true, TxHash: txHash}
    }

    // Phase 2b: PatchPool 补救 — 仅覆盖冲突 key
    if matched, node := rs.patchPool.FindCovering(conflictKeys); matched {
        if patch := rs.patchPool.TryConsume(node.TxHash, txHash, priority); patch != nil {
            rs.state.SetChainPatch(txHash, patch)
            rs.mu.Lock()
            rs.consumedPatches[txHash] = node.TxHash
            rs.mu.Unlock()
            rs.tempLockView.ClearWounded(txHash)
            return &api.RetryCommitResp{Locked: true, TxHash: txHash}
        }
    }

    // 无 Patch 覆盖 → 释放 tempLockView 锁 + OnChainLockConflict
    rs.tempLockView.GarbageCollect(txHash)
    return &api.RetryCommitResp{
        Locked:              false,
        TxHash:              txHash,
        OnChainLockConflict: true,
    }
}
```

**删除**：
- 当前代码中 Phase 1（查被动池之后、TryLockWithPriority 之前）的 PatchPool 逻辑（~line 1060-1077）

### 3.3 `ssc/temp_lock_view.go`

无改动。`GarbageCollect` 和 `ClearWounded` 已存在且签名正确。

---

## 4. 验证方法

### 4.1 实验验证

**前置条件**：同步代码 → 远程编译 → `test_single` 跑一轮

**检查点**：
1. `retryCommit failed: stateDB lock conflict` 次数
   - 预期：和之前实验相当（8,213 次左右，stateDB 冲突量不变）
2. `retryCommit: found local patch in PatchPool, skipping lock conflict` 次数
   - 预期：**大幅下降**。只有 stateDB 冲突且 Patch 能覆盖全部冲突 key 的才走这条路径
3. `retry commit success` → `StartReSimulation timing` 转化率
   - 预期保持在 99%+（和一前一样）
4. `resimulation failed` 次数
   - 预期：**大幅下降**。Patch 跳锁后模拟时不再撞 stateDB 锁

### 4.2 边界情况

- **单 key 交易**：1 个 key，stateDB 冲突，Patch 覆盖 → 和之前行为一致
- **多 key 全部被一个 Patch 覆盖**：正常跳锁
- **多 key 需要多个 Patch 才能覆盖全部冲突 key**：当前实现不考虑「多 Patch 组合」，因为违反单上游约束。这种情况下返回 `OnChainLockConflict=true`
- **Patch 已 Finalized**：`FindCovering` 跳过 Finalized 节点，和当前 `HasConflict` 行为一致
