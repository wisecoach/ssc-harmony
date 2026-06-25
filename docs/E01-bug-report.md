# Bug 反馈报告：A06 / A07 设计文档 vs 代码实现偏差

> **评估日期**：2026-06-20
> **评估范围**：`docs/A06-lock-priority-coordination.md` v5 和 `docs/A07-force-simulation.md` v1
> **评估方法**：逐项对比设计文档与当前代码实现（`ssc/api/types.go`, `ssc/temp_lock_view.go`, `ssc/retry_scheduler.go`, `ssc/simulator_leader.go`, `ssc/simulator_member.go`, `ssc/impl.go`, `core/vm/sscis_simulation_call.go`, `core/vm/sscis_simulation_recall.go`, `core/vm/sscvm.go`, `core/ssc_state_transition.go`, `ssc/state_locker.go`）

---

## 目录

1. **[Critical] Bug #1：ReadSet Wound-Wait 逻辑缺失**
2. **[Medium] Bug #2：CommitSimulation 被 Wound 后无 clean closure**
3. **[Medium] Bug #3：ConflictKeys 为无效 placeholder**
4. **[Minor] 偏差 #4：OnBlockCommitted 仍用 CanLock**
5. **[Minor] 偏差 #5：Priority 文档字段顺序与比较顺序不一致**

---

## [Critical] Bug #1：ReadSet Wound-Wait 逻辑缺失

### 位置

`ssc/temp_lock_view.go` 第 112-119 行

### 设计文档要求

`docs/A06-lock-priority-coordination.md §3.4.1`（第 216-226 行）明确要求读集上也要走 Wound-Wait 逻辑：

```go
for _, key := range readSet {
    if entry, exists := v.tempWriteLocks[key]; exists {
        if entry.Holder == txHash {
            continue
        }
        if !canWound(entry.Priority, priority) {
            return false, false
        }
        v.woundLock(key, txHash, priority)
    }
}
```

### 当前代码

```go
// Step 2: 检查读集
for _, key := range rwSet.Reads {
    if entry, writtenInTemp := v.tempWriteLocks[key]; writtenInTemp {
        if bytes.Compare(entry.Holder.Bytes(), txHash.Bytes()) != 0 {
            v.stateLockManager.sscService.stats.TempLockTryFail.Add(1)
            return false, false  // ← 直接失败，没有 Wound！
        }
    }
}
```

### 根因

实现时漏写了读集上的 Wound 分支，只做了简单的不等检查就返回失败。

### 后果

读集路径上 Wound-Wait 完全不起作用。场景：
- retryTx_A 持有 K0 的写锁（低优先级）
- retryTx_B（高优先级，需要读 K0）→ 直接失败，不会踢掉 retryTx_A
- **A06 的核心机制在 ReadSet 路径上退化到 Wound-Wait 前的行为**

### 修复方案

将读集循环改为与写集相同的 Wound 逻辑：

```go
// Step 2: 检查读集 — 支持 Wound-Wait
for _, key := range rwSet.Reads {
    if entry, writtenInTemp := v.tempWriteLocks[key]; writtenInTemp {
        if bytes.Compare(entry.Holder.Bytes(), txHash.Bytes()) != 0 {
            // 检查是否可以 Wound
            if v.canWound(entry, priority, txHash) {
                v.woundedTxs[entry.Holder] = struct{}{}
                v.tempWriteLocks[key] = tempLockEntry{
                    Holder:   txHash,
                    Priority: priority,
                }
                utils.SSCLogger().Info().Str("wounded", entry.Holder.Hex()).
                    Str("wounder", txHash.Hex()).
                    Str("key", string(key)).
                    Msg("Wound (readset): high priority tx took lock from low priority")
            } else {
                v.stateLockManager.sscService.stats.TempLockTryFail.Add(1)
                return false, false
            }
        }
    }
}
```

---

## [Medium] Bug #2：CommitSimulation 被 Wound 后无 clean closure

### 位置

`ssc/impl.go` 第 778-788 行

### 设计文档要求

`docs/A06-lock-priority-coordination.md §3.4.3`（第 291-295 行）：

```go
// 锁死 Patch：标记为 Finalized，不可再被 Wound
s.retryScheduler.patchPool.Finalize(commit.TxHash)

// 验证锁仍在
if s.retryScheduler.IsWounded(commit.TxHash) {
    // 被踢了 → 放弃提交
    s.closeTransaction(commit.TxHash, false, "WoundedByHigherPriority")
    return
}
```

`docs/A06-lock-priority-coordination.md §4.3`（第 480 行）：要求添加 `WoundedByHigherPriority` 关闭原因。

### 当前代码

```go
// v5 Wound-Wait: 二次验证锁仍然持有
if s.retryScheduler.tempLockView.IsWounded(txHash) {
    utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
        Msg("commit simulation: tx was wounded, aborting submission")
    // Finalize 了也要 Remove，确保其他 retryTx 能拿到
    s.retryScheduler.patchPool.Release(txHash)
    return  // ← 只是 return，没有 closeTransaction！
}
```

### 根因

被 Wound 后只 `Release` 了 PatchPool 条目就 return 了，没有调用 `closeTransaction` 做完整清理。

### 后果

1. **retryPool 残留**：被踢的交易仍留在 `retryPool` 和 `signals` 中。下次 `OnBlockCommitted` 可能再次尝试，然后又可能被踢，形成循环浪费。
2. **跨 shard 临时锁未清理**：之前 RetryCommit 在相关 shard 上打的 TempLockView 锁没被释放（`RetryCancel`）。
3. **可能解释实验数据**：`TriggerReSimulation` 永远为 0 的原因可能是：虽然 `RetryCommit` 返回了 `locked=true`，但 `CommitSimulation` 时被 Wound 后 silent return，上游 `aggregateSimulationResults` 没有收到明确的 fail 信号，使交易卡在中间状态。

### 修复方案

```go
if s.retryScheduler.tempLockView.IsWounded(txHash) {
    utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
        Msg("commit simulation: tx was wounded, aborting submission")
    s.retryScheduler.patchPool.Release(txHash)
    s.closeTransaction(txHash, false, "WoundedByHigherPriority")
    return
}
```

同时需要在 `simulator_leader.go` 中，如果 aggregate 到 `ForceSimulation` 且 ConflictKeys 非空的 case 也要做 clean closure（设计文档 §3.8 描述为不走 CommitSimulation 走 CallForRetry）。

---

## [Medium] Bug #3：ConflictKeys 为无效 placeholder

### 位置

`ssc/simulator_member.go` 第 1075 行

### 设计文档要求

`docs/A07-force-simulation.md §3.7`（第 240 行）：

```go
ret.ConflictKeys = extractConflictKeys(callState)  // 提取真实冲突 key
```

### 当前代码

```go
if callState.LockedByOtherTx != nil {
    ret.ConflictKeys = []api.LockKey{api.FormKey(common.Address{}, common.Hash{})} // placeholder
}
```

### 根因

这是实现 ForceSimulation 时特意留的 TODO placeholder，没有从 `callState` 中提取真实的冲突 key。

### 后果

1. 永远有 `len(ConflictKeys) > 0` → 日志 `ForceSimulation: conflict keys=N` 总会触发，但 N 始终是 1，且 key 值无意义
2. 后续需要具体冲突 key 做优化（如冲突 key 去重、优先级排序）时完全不可用
3. 日志分析时无法区分"真的有一个 key 冲突"和"placeholder 占位"

### 修复方案

从 `callState.LockedByOtherTx` 提取真正的冲突 key 列表。如果需要遍历所有访问过的 key：

```go
if callState.LockedByOtherTx != nil && callState.RWSet != nil {
    var conflictKeys []api.LockKey
    // 从 WriteSet 检测
    for addr, state := range callState.RWSet.WriteState.State {
        for key := range state {
            lockKey := api.FormKey(addr, key)
            if sim.stateDB.isLockedByOtherTx(lockKey) { // 需要此 API
                conflictKeys = append(conflictKeys, lockKey)
            }
        }
    }
    // 从 ReadSet 检测
    for addr, state := range callState.RWSet.ReadState.State {
        for key := range state {
            lockKey := api.FormKey(addr, key)
            if sim.stateDB.isLockedByOtherTx(lockKey) {
                conflictKeys = append(conflictKeys, lockKey)
            }
        }
    }
    ret.ConflictKeys = conflictKeys
}
```

如果不想额外做 stateDB 查询，至少应该把 LockedByOtherTx 的具体值传过来，而不是用零值 key。

---

## [Minor] 偏差 #4：OnBlockCommitted 仍用 CanLock

### 位置

`ssc/retry_scheduler.go` 第 238 行

### 当前代码

```go
if rs.tempLockView.CanLock(txHash, tx.ReadSet, tx.WriteSet) {
    signal.Ready = true
```

### 说明

`OnBlockCommitted` 中的 `CanLock` 不支持 Wound-Wait 语义。但此路径实际上被第 260 行的注释屏蔽：
```go
// 由 HotKey chain 接管 retry 信号的触发
for _, epoch2Signals := range shard2Epoch2RetrySignals {
    // ... 不发送 signal ...
}
```

所以当前这不影响主流程。但长期来看应该将这里的 `CanLock` 替换为 `TryLockWithPriority` + `IsWounded` 检查，以保持与设计文档一致。

---

## [Minor] 偏差 #5：Priority 文档字段顺序与比较顺序不一致

### 位置

`docs/A06-lock-priority-coordination.md`

### 当前文档

**§3.2（第 134 行）**——比较顺序：
```
(SimulationNum, Nonce, OriginShardID)
```

**§4.3（第 327 行）**——结构体字段声明：
```go
type Priority struct {
    Nonce         uint64
    OriginShardID uint32
    SimulationNum int
}
```

### 说明

比较顺序是 `(SimulationNum, Nonce, OriginShardID)`（SimulationNum 最优先），但结构体字段声明顺序是 `(Nonce, OriginShardID, SimulationNum)`。虽然代码中 `Less` 方法的实现是正确的（先比 SimulationNum），但文档中两处的字段顺序不一致，容易让人误解。

建议将 **§4.3** 的结构体定义改为：
```go
type Priority struct {
    SimulationNum int    // 重试次数多的优先（最高优先级）
    Nonce         uint64 // Nonce 小的优先
    OriginShardID uint32 // ShardID 小的优先
}
```

---

## 总结

| 严重程度 | Bug ID | 文件 | 影响 |
|:--------:|:------:|------|------|
| 🔴 Critical | #1 | `ssc/temp_lock_view.go` | ReadSet 路径上 Wound-Wait 完全失效 |
| 🟡 Medium | #2 | `ssc/impl.go` | 被 Wound 的交易成为僵尸，TriggerReSimulation=0 的可能根因 |
| 🟡 Medium | #3 | `ssc/simulator_member.go` | ConflictKeys 永远为无效的零值 key |
| 🟢 Minor | #4 | `ssc/retry_scheduler.go` | CanLock 不兼容 Wound-Wait，但路径已阻塞 |
| 🟢 Minor | #5 | `docs/A06-lock-priority-coordination.md` | 文档字段顺序歧义 |

---

# E02：onChainPatches 实现问题与链式重试失效

> **评估日期**：2026-06-21
> **评估范围**：`docs/A05-hotkey-retry-design.md` v6 onChainPatches 设计 vs 代码实现
> **评估方法**：实验日志分析 + 代码审查

## 问题 #6：[Critical] sendChainSignal 未设置 ChainNode.UpstreamTxHash

### 位置

`ssc/retry_scheduler.go` 第 822-827 行（`sendChainSignal`）

### 描述

`sendChainSignal` 创建的 `ChainNode` 中 `UpstreamTxHash` 和 `UpstreamSimNum` 始终为零值。注释说"将在 HandleRetrySignal 中设置"，但 `HandleRetrySignal` 中从未设置这两个字段：

```go
chainNode := &api.ChainNode{
    TxHash:        txHash,
    SimulationNum: retryTx.SimulationNum,
    Patch:         writeSet,
    // UpstreamTxHash 和 UpstreamSimNum 将在 HandleRetrySignal 中设置
    // ← 从未设置，永远为零值！
}
```

### 后果

- `patches[downstreamTxHash][simNum].UpstreamTxHash` 始终为零值
- `GetUpstreamTxRef(txHash)` 永远返回 `(common.Hash{}, 0)`
- `CommitSimulation` 中 `UpstreamTxHash` 永远为空
- SimTx 的 `hasUpstream` 永远为 false
- `ReadOnChainPatch` 无法被调用（因为 `isChainTx && UpstreamTxHash != zero` 条件不满足）
- 链式交易的依赖链断裂，所有 SimTx 都被当作根节点

### 实验证据

4540 次 `chain tx detected` 中 `hasUpstream=false` 占比 100%，`ReadOnChainPatch` 日志为 0。

### 修复

1. `chainNextSim` 加 `upstreamSimNum` 参数
2. `sendChainSignal` 加 `upstreamTxHash`/`upstreamSimNum` 参数，写入 ChainNode
3. `CommitSimulation` 使用 `GetUpstreamTxRef`（新增方法）替代 `GetChainPatchRef` 获取上游信息

### 状态

✅ 已修复（2026-06-21 编码完成，待实验验证）

---

## 问题 #7：[Medium] GetChainPatchRef 被误用于获取上游信息

### 位置

`ssc/impl.go` 第 783 行

### 描述

`CommitSimulation` 中原代码：

```go
if chRef := s.retryScheduler.GetChainPatchRef(txHash); chRef != nil {
    upstreamTxHash = chRef.TxHash  // ← 这是当前 tx 自己的 hash！
    upstreamSimNum = chRef.SimulationNum
}
```

`GetChainPatchRef` 返回 `node.TxHash`（当前交易自己的 hash），不是 `node.UpstreamTxHash`（上游 hash）。

### 修复

新增 `GetUpstreamTxRef(txHash)` 方法，返回真实的 `(UpstreamTxHash, UpstreamSimNum)`。

### 状态

✅ 已修复

---

## 问题 #8：[Medium] ForceSimulation 导致 VerifySimulation 结果不一致

### 位置

全局 — `ForceSimulation=true` 时

### 描述

`ForceSimulation` 在锁冲突时从 stateDB 读值继续模拟。但该值可能已被 Wound-Wait 中的更高优先级交易修改，导致模拟结果与链上执行结果不同，出现 `simulation result is not equal to execution result` 错误（527 次）。

### 当前状态

**已暂关**（`ForceSimulation=false`），等待 onChainPatches 完善后重新评估。

---

## 问题 #9：[Medium] unfinished 交易原因不明

### 描述

每轮实验有 300-400 笔 `unfinished` 交易（既不 commit 也不 rollback）。理论上锁最终会被释放，交易应终结。

### 分析方向

需在新 session 中排查：
1. 是否有交易在 `closeTransaction` 前死锁
2. `PoolTimeout` 是否正确触发
3. `TimerMgr` 的清理是否完整
4. 是否 Wound 后交易进入僵尸状态（与 Bug #2 相关）

### 状态

待排查

---

## 总结

| 严重程度 | Bug ID | 文件 | 影响 | 状态 |
|:--------:|:------:|------|------|:----:|
| 🔴 Critical | #6 | `ssc/retry_scheduler.go` | ChainNode.UpstreamTxHash 恒为零，链式重试全部断裂 | ✅ 已修 |
| 🟡 Medium | #7 | `ssc/impl.go` | GetChainPatchRef 返回自己 hash 而非上游 hash | ✅ 已修 |
| 🟡 Medium | #8 | 全局 | ForceSimulation 导致链上验证不一致 | ⏸️ 暂关 |
| 🟡 Medium | #9 | 待排查 | unfinished 交易清理不完整 | 🔍 待查 |
