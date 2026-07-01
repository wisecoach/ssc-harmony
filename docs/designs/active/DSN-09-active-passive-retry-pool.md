# [A09] 被动 Retry Pool — 跨分片锁冲突的 Event-driven 等待机制

> **版本**：v2（2026-06-30）
> **状态**：设计阶段（v1 实现方向有误，此为修正版）
> **关联文档**：`docs/A05-hotkey-retry-design.md`（PatchPool）、`docs/A06-lock-priority-coordination.md`（Wound-Wait）

---

## 1. Problem Statement

### 1.1 问题：每块盲扫

当前 retry 机制：当 `tryToReSimulation` 发现所有 related shard 的 `RetryCommit` 都返回 `Locked=true` 时，才触发 `TriggerReSimulation`。

但 `RetryCommit` 失败的原因之一是**其他分片的链上锁冲突**（`ErrLockConflict_OnChain`）。此时即使 retry tx 在本分片（shard A）上所有锁都可用，在远端分片（shard B）上被链上锁卡住。每块 `OnBlockCommitted` 都会盲扫 retryPool，导致：

```
每块:
  OnBlockCommitted:
    CanLock(txA, [K1,K2]) → true ✅
    → 发 RetrySignal → O → 广播 RetryCommit → A, B
       A: TryLock OK → Locked=true ✅
       B: TryLock OK → stateDB.CheckLock → 链上锁冲突 ❌
    → tryToReSimulation 失败 → tx 留在 retryPool
  ↑ 下块再来一次 ↑
```

毒性循环不在这里——这里的问题是**知道 B 的锁没释放，但每块还是浪费一次完整的 RetryCommit 轮次**。

### 1.2 目标

当 `RetryCommit` 因远端链上锁失败时，**停止每块重试**，等远端锁释放后通过 RetrySignal 路径重新唤醒，再尝试一次。中间的盲扫全部跳过。

---

## 2. 设计方案

### 2.1 核心交互流程

```
分片:
  O = Origin Shard（发起 retry 的分片）
  A = 发现锁解锁、发送 RetrySignal 的分片
  B = 链上锁冲突的分片

流程:
  1. A 发现 tx 的状态都解锁了 → RetrySignal → O
  2. O 广播 RetryCommit → A, B
  3. A: TryLock OK → Locked=true
  4. B: TryLock OK → stateDB.CheckLock → 链上锁冲突 → Locked=false, reason=OnChain
  5. O 收到 B 失败 → O 通知 A: 进被动池
  6. A 把 tx 移入 passivePool → 停止每块重试
  7. B 的链上锁释放（SimTx CR 完成 / chainNextSim 触发）
  8. B 发现该 key 已解锁 → RetrySignal → O
  9. O 广播 RetryCommit → A, B
  10. A 收到 RetryCommit → 出 passivePool → Locked=true
  11. B: 这次链上锁 OK → Locked=true
  12. 全部 true → TriggerReSimulation
```

### 2.2 关键设计点

**被动池在 shard A，不在 origin，不在 shard B。**

- Shard A 是**发现锁解锁并发出 RetrySignal** 的分片
- A 的被动池只是一个标记："这笔 tx 被远端锁卡住了，别再每块浪费 RetryCommit 了"
- B 不需要被动池——B 只需要在锁释放时通过正常的 `chainNextSim` → `RetrySignal` 路径通知 O

**唤醒不靠 chainNextSim 扫被动池。**

- B 的锁释放后，B 的 `chainNextSim` 照常发 RetrySignal 给 O（和普通 retry 走同一路径）
- O 收到 RetrySignal 后照常广播 RetryCommit 给所有 related shard
- A 收到 RetryCommit 时，检查这笔 tx 是否在 passivePool 中
  - 是 → 移出 passivePool，尝试锁竞争
  - 否 → 正常锁竞争

**被动池只影响 OnBlockCommitted 的 poll。**

- `OnBlockCommitted` 扫描 retryPool 时跳过 passivePool 中的 tx
- 不触发 CanLock，不消耗 TempLock 资源
- 但 `RetryCommit`（来自 O 的 RPC 调用）不受被动池影响——O 主动推的 RetryCommit 能绕过被动池直接触发锁竞争

### 2.3 数据结构

```go
// retryScheduler — 每个 shard 独立维护
type retryScheduler struct {
    retryPool    map[common.Hash]*api.RetryTx    // 主动池：每块 poll
    passivePool  map[common.Hash]struct{}         // 被动池：跳过 poll，仅标记
}
```

被动池不需要存 `WaitingKeys`/`EnterBlock`——它只是一个标记。唤醒不靠轮询，靠 O 推过来的 RetryCommit。

### 2.4 接口

```go
// 进入被动池：由 tryToReSimulation 在收到 B 失败后调用
func (rs *retryScheduler) AddToPassivePool(txHash common.Hash)

// 移出被动池：由 OnRetryCommit 在收到新的 RetryCommit 时调用
func (rs *retryScheduler) RemoveFromPassivePool(txHash common.Hash)

// 是否在被动池中
func (rs *retryScheduler) IsInPassivePool(txHash common.Hash) bool
```

---

## 3. 流程变更

### 3.1 OnBlockCommitted 跳过被动池

```go
func (rs *retryScheduler) OnBlockCommitted(block *types.Block) {
    // 只扫描 retryPool（主动池）
    for txHash, tx := range rs.retryPool {
        if _, inPassive := rs.passivePool[txHash]; inPassive {
            continue  // 跳过被动池中的 tx
        }
        // 正常 CanLock / 发 signal 逻辑
        ...
    }
}
```

### 3.2 RetryCommit 收到时的特殊处理

当 shard A 收到 O 广播的 `RetryCommit` 时，如果这笔 tx 在 `passivePool` 中：

```go
func (rs *retryScheduler) RetryCommit(txHash common.Hash) *api.RetryCommitResp {
    // 如果在被动池中，先移出
    if rs.IsInPassivePool(txHash) {
        rs.RemoveFromPassivePool(txHash)
        utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
            Msg("retryCommit: woken from passive pool, attempting lock")
    }
    // ... 正常锁竞争逻辑 ...
}
```

### 3.3 RetryCommit 链上锁失败的通知路径

当前 `RetryCommit` 已经区分 `ErrLockConflict_OnChain`。当 B 检测到时：

```
B.RetryCommit → stateDB.CheckLock → OnChain
  → B 返回 Locked=false, reason=OnChain
  → O 的 tryToReSimulation 收到
  → O 调用 A: AddToPassivePool(txHash)
  → A 把 tx 标记为被动
```

但这里需要新增一个 RPC 或回调：O → A 的「进被动池」通知。

### 3.4 B 锁释放后的唤醒

B 的锁释放后（SimTx CR 完成），B 的 `chainNextSim` 或 `OnBlockCommitted` 扫描 B 的 retryPool：

```go
// chainNextSim 中扫描 B 的 retryPool（现有逻辑）
for txHash, retryTx := range rs.retryPool {
    if dependsOn(retryTx, writeSet) {
        rs.sendChainSignal(txHash, retryTx, ...)
        // 这条信号会到 O → O 广播 RetryCommit → A, B
    }
}
```

不需要额外代码——B 的锁释放自然触发 `chainNextSim` → 找到依赖 tx → 发 RetrySignal → O → RetryCommit → A 被唤醒。

### 3.5 超时兜底

`PoolTimeout` 仍然对被动池有效。`OnBlockCommitted` 中检查：

```go
for txHash := range rs.passivePool {
    if rs.bc.CurrentHeader().NumberU64() >= rs.passivePoolEnterBlock[txHash] + rs.poolTimeout {
        // 超时 → closeTransaction
        rs.state.CloseTransaction(txHash, false, "PoolTimeout")
    }
}
```

但需要记录进入被动池时的 blockNum。所以除了 `passivePool` 集合外，还需要一个 `passivePoolEnterBlock map[common.Hash]uint64`。

---

## 4. 与 v1（已实现版本）的区别

| 维度 | v1（已实现，错的） | v2（正确的） |
|:-----|:-----------------|:------------|
| 被动池位置 | 每个 shard 的 retryScheduler | shard A（发 RetrySignal 的那个） |
| 进入时机 | RetryCommit 检测到 OnChain 失败→直接进 | O 收到 B 失败后→通知 A 进 |
| 唤醒机制 | chainNextSim 扫描 passivePool | B 锁释放→chainNextSim→RetrySignal→O→RetryCommit→A |
| chainNextSim 改动 | 需新增被动池扫描逻辑 | 不需要改，走现有 RetrySignal 路径 |
| 复杂度 | 修改多，侵入性强 | 简单标记 + 现有路径复用 |
| PoolTimeout | OnBlockCommitted 统一检查 | 类似，多一个 enterBlock 记录 |

---

## 5. 实现步骤

| Step | 内容 | 文件 |
|:-----|:-----|:-----|
| 1 | 数据结构：`passivePool` + `passivePoolEnterBlock` | `ssc/retry_scheduler.go` |
| 2 | 接口：`AddToPassivePool` / `RemoveFromPassivePool` / `IsInPassivePool` | `ssc/retry_scheduler.go` |
| 3 | `OnBlockCommitted` 跳过被动池中的 tx | `ssc/retry_scheduler.go` |
| 4 | `RetryCommit` 收到时检查被动池，移出 | `ssc/retry_scheduler.go` |
| 5 | `tryToReSimulation` 收到 B 的 OnChain 失败后，通知 A 进被动池 | `ssc/retry_scheduler.go` |
| 6 | `OnBlockCommitted` 检查被动池超时 | `ssc/retry_scheduler.go` |
| 7 | `closeTransaction` 清理被动池 | `ssc/impl.go` |
| 8 | 删除 v1 的 chainNextSim 被动池扫描代码 | `ssc/retry_scheduler.go` |
| 9 | 编译 + 实验验证 | — |

---

## 6. 未解决的问题

- **O → A 的「进被动池」通知**：当前没有 RPC 路径让 O 调用 A 的 `AddToPassivePool`。需要新增 `Method_AddToPassivePool` RPC，或者复用 `RetrySignal` 的响应路径。
- **多个 B 的情况**：如果一笔 tx 在 3 个远端 shard 上都有链上锁，需要等所有 B_i 都释放锁→发信号→O→RetryCommit。被动池只在 shard A 上，不受多个 B 的影响。
- **`tryToReSimulation` 的修改**：当前 `tryToReSimulation` 在收到任意 shard 的 `Locked=false` 时就返回失败。需要改为：如果失败原因是 OnChain，调用 O→A 的进被动池通知。
