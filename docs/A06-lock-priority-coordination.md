# [A06] 锁优先级协调（协调层）

> **阅读顺序**（`[前缀]` 表示阅读顺序）：
> 1. 📄 `docs/A01-sscc-refactor-plan.md` [A01] — SSCC 模块化重构总览
> 2. 📄 `docs/A02-lock-retry-mechanism.md` [A02] — 锁与重试机制
> 3. 📄 `docs/A03-CR-priority-optimization.md` [A03] — CR 提交优先级优化
> 4. 📄 `docs/A04-onchain-retry-limit.md` [A04] — 链上重试次数限制
> 5. 📄 `docs/A05-hotkey-retry-design.md` [A05] — HotKey 链式重试（数据层）
> 6. 📄 `docs/A06-lock-priority-coordination.md` [A06] **（本文）** — 锁优先级协调（协调层）
>
> **运维辅助**（与主线并行的独立文档）：
> - 📄 `docs/B01-log-query-guide.md` [B01] — 日志查询指南
> - 📄 `docs/B02-log-lifecycle.md` [B02] — 交易生命周期日志大全

> **版本**：v5 — Wound-Wait 跨分片锁协调（2026-06-20，设计阶段）
> **前置依赖**：PatchPool v4（详见 [A05-hotkey-retry-design.md §v4](./A05-hotkey-retry-design.md#v4--patchpool-分片本地池化方案)）
> **解决的问题**：HotKeyRetry 中跨分片锁竞争导致的循环冲突死锁
>
> **阅读顺序**：本文是 HotKey Retry 系列的第二篇，建议先阅读第一篇：
> 1. 📄 `docs/A05-hotkey-retry-design.md` — 链式数据依赖与 PatchPool 数据层设计（v1→v4 ✅ 已实现）
> 2. 📄 **本文** — 锁优先级协调：Wound-Wait 跨分片锁竞争解决（v5 📝 设计阶段）


## 1. 背景回顾：PatchPool v4 机制

### 1.1 为什么需要 PatchPool

跨分片交易（SSC）的 HotKeyRetry 链式重试中，一个关键瓶颈是：SimTx 提交后，相关分片的 `RetryCommit` 再次遭遇锁冲突，导致重试失败。

```text
SimTx1 提交（写 Key K）→ chainNextSim → 匹配 retryTx2
  └→ HandleRetrySignal（origin shard 存 ChainPatch）
       └→ tryToReSimulation → 各 shard 并发 RetryCommit
            ├─ origin shard: 有 ChainPatch → 跳过锁冲突 ✅
            └─ related shard: 无 ChainPatch → 锁冲突失败 ❌
```

### 1.2 PatchPool 设计

每个 shard 的 Leader 维护一个本地 PatchPool，存储该 shard 已提交 SimTx 的 WriteSet。倒排索引（key → txHash）实现 O(r) 匹配。

```go
type PatchPool struct {
    mu       sync.RWMutex
    Patches  map[common.Hash]*ChainPatchNode
    KeyIndex map[LockKey]map[common.Hash]struct{}
}

type ChainPatchNode struct {
    TxHash    common.Hash
    Patch     *RWSet
    Consumed  bool        // 临时独占标记，防止多笔 retryTx 抢同一份 Patch
    CreatedAt time.Time
}
```

### 1.3 PatchPool 的局限性

PatchPool 只能在**本地分片**的 RetryCommit 中跳过锁冲突。对于跨分片 retry，只有提交了上游 SimTx 的那个 shard 有 Patch 可用，其他 related shard 没有 Patch，仍需走正常 TempLockView 锁竞争。

```text
SimTx1（写 Key K）在 shard 0 提交
  └→ shard 0 的 PatchPool.Add(SimTx1, {K=v1})
  └→ OnPatchPoolUpdated → 匹配到 retryTx2
       └→ sendChainSignal → origin shard 并发 RetryCommit

shard 0: PatchPool 有 K → TryConsume → locked ✅
shard 1: PatchPool 无 K → TempLockView.TryLock → 锁冲突 ❌
shard 2: PatchPool 无 K → TempLockView.TryLock → 锁冲突 ❌
```

**核心问题：** `tryToReSimulation` 要求**所有 related shard** 的 RetryCommit 都返回 `Locked: true`。任意一个失败就集体回滚。PatchPool 只帮了本地 shard，跨 shard 的锁竞争问题没有解决。

### 1.4 循环冲突与死锁

多笔 retryTx 在多个 shard 上并发竞争不同的 key：

```
retryTx_A 需要锁 {K0, K1}
retryTx_B 需要锁 {K0, K1}

shard 0: retryTx_A 抢到 K0
shard 1: retryTx_B 抢到 K1
→ 双方都差一把锁 → 同时失败 → 释放 → 下次可能又是同样的对称性 → 循环
```

循环冲突的代价：每轮循环浪费一次链下模拟 + N 次 `RetryCommit` RPC 通信（耗时 ≈ 1-2 区块）。


## 2. 问题定义

### 2.1 关键观察

1. **锁冲突是两阶段问题**：`RetryCommit`（链下临时锁）和 `VerifySimulation`（链上锁）是不同的锁空间
2. **循环冲突的本质是缺乏全局排序**：所有 retryTx 平等竞争，没有谁应该优先
3. **Patch 的 Consumed 标记只能防止同份 Patch 被多笔 retryTx 消费，不能解决跨 shard 锁竞争**

### 2.2 设计约束

| 约束 | 说明 |
|------|------|
| **0 新增跨 shard 通信** | PatchPool 是分片本地的，协调不能依赖额外的跨 shard RPC |
| **链下决策** | 所有锁竞争在链下（RetryCommit/模拟阶段）解决，不上链 |
| **阶段 4 不可抢占** | 一旦 SimTx 提交（CommitSimulation），不能再被其他 retryTx 踢掉 |
| **与现有机制兼容** | 不破坏 chainNextSim、HandleRetrySignal、OnBlockCommitted 等现有流程 |

### 2.3 三阶段不可抢占边界

| 阶段 | 动作 | 可被抢？ | 描述 |
|------|------|:--------:|------|
| 1 | `RetryCommit` (TryLock) | ✅ | 临时锁可被高优先级踢掉 |
| 2 | `tryToReSimulation` → `StartReSimulation` | ✅ | 模拟期间可被高优先级踢掉 |
| 3 | `StartReSimulation` → `aggregateResults` | ✅ | 结果聚合前二次验证锁仍在 |
| 4 | `CommitSimulation` → SimTx 提交 | ❌ | 不可被抢，Finalized 锁死 |


## 3. 核心方案：基于时间戳的 Wound-Wait

### 3.1 理论依据

Wound-Wait 是分布式数据库中经典的死锁预防协议（Rosenkrantz et al., "System Level Concurrency Control for Distributed Database Systems", ACM TODS 1978）。

核心思想：
- **Wound（创伤）**：高优先级交易发现锁被低优先级占着 → 踢掉低优先级，自己拿锁
- **Wait（等待）**：低优先级交易发现锁被高优先级占着 → 自动失败，回头重试

本方案借鉴 Wound-Wait，将其应用于分片本地的 TempLockView，实现**无额外通信的跨 shard 锁协调**。

### 3.2 优先级定义

每笔 retryTx 的全局唯一优先级：

```go
type Priority struct {
    SimulationNum int    // 重试次数少的优先（最高优先级）
    Nonce         uint64 // 先提交的优先
    OriginShardID uint32 // 同 nonce+simNum 时 shardId 小的优先
}
```

比较规则：`(SimulationNum, Nonce, OriginShardID)` 按字典序比较。**重试次数（SimulationNum）优先级最高**——重试次数越多的交易越优先拿到锁，避免无限循环重试。

**优先级一旦分配，在交易生命周期内不改变。**

### 3.3 数据结构变更

#### 3.3.1 ChainPatchNode 三态消费标记

```go
// PatchConsumeStatus 表示 ChainPatchNode 的消费状态
type PatchConsumeStatus int

const (
    PatchFree      PatchConsumeStatus = iota  // 尚未被任何 retryTx 使用
    PatchConsumed                               // 被某 retryTx 占用，仍可被 Wound
    PatchFinalized                              // 已被 CommitSimulation 提交，不可被 Wound
)

type ChainPatchNode struct {
    TxHash      common.Hash
    Patch       *RWSet
    Status      PatchConsumeStatus    // Free → Consumed → Finalized
    Consumer    common.Hash           // 占用此 Patch 的 retryTx hash
    Priority    Priority              // 占用者的优先级
    CreatedAt   time.Time
}
```

#### 3.3.2 TempLockView 优先级锁

```go
// tempLockEntry 记录锁持有者和它的优先级
type tempLockEntry struct {
    Holder   common.Hash   // 持有锁的 retryTx hash
    Priority Priority      // 持有者的优先级
}

// 扩展 TempLockView 的写锁存储
// tempWriteLocks map[LockKey]common.Hash → tempWriteLocks map[LockKey]tempLockEntry
```

### 3.4 核心算法

#### 3.4.1 TryLockWithPriority

```go
// TryLockWithPriority 尝试对读写集上锁。
// 如果锁空闲 → 上锁 ✅
// 如果锁被低优先级占着 → Wound（踢掉低优先级的，自己拿锁）✅
// 如果锁被高优先级占着 → 失败（返回 locked=false）❌
// 如果锁是 Finalized 状态 → 不可踢，直接失败 ❌
func (v *TempLockView) TryLockWithPriority(
    txHash common.Hash,
    priority Priority,
    readSet, writeSet []api.LockKey,
) (locked bool, wounded bool) {
    v.mu.Lock()
    defer v.mu.Unlock()

    // 检查写锁
    for _, key := range writeSet {
        if entry, exists := v.tempWriteLocks[key]; exists {
            if entry.Holder == txHash {
                continue // 自己已持有，跳过
            }
            if !canWound(entry.Priority, priority) {
                // 被高优先级占着 → 失败
                return false, false
            }
            // 被低优先级占着 → Wound
            v.woundLock(key, txHash, priority)
        }
    }

    // 检查冲突读锁
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

    // 所有冲突已解决 → 上锁
    for _, key := range writeSet {
        v.tempWriteLocks[key] = tempLockEntry{
            Holder:   txHash,
            Priority: priority,
        }
    }
    v.txReadWriteSets[txHash] = &readWriteSet{
        Reads:  readSet,
        Writes: writeSet,
    }

    return true, false
}

// canWound 判断是否可以踢掉低优先级占锁者
func canWound(holderPri, requesterPri Priority) bool {
    // Finalized 的锁不能踢
    if isFinalized(holderPri) { return false }
    // 优先级数值越小越高
    return less(requesterPri, holderPri)
}

// woundLock 踢掉低优先级占锁者
func (v *TempLockView) woundLock(key api.LockKey, newHolder common.Hash, priority Priority) {
    oldHolder := v.tempWriteLocks[key].Holder
    v.tempWriteLocks[key] = tempLockEntry{
        Holder:   newHolder,
        Priority: priority,
    }
    // 通知被踢的交易（通过标记 wounded flag）
    v.woundedTxs[oldHolder] = struct{}{}
}
```

#### 3.4.2 被踢检测

```go
// IsWounded 检查当前交易是否已被踢
func (v *TempLockView) IsWounded(txHash common.Hash) bool {
    v.mu.RLock()
    defer v.mu.RUnlock()
    _, exists := v.woundedTxs[txHash]
    return exists
}
```

被踢的 retryTx 在以下时机发现：
1. **`RetryCommit` 返回时**：如果 wounded=true，`tryToReSimulation` 放弃本轮
2. **`tryToReSimulation` 二次验证时**：在 `TriggerReSimulation` 前再次调用 `IsWounded`
3. **`StartReSimulation` 结果聚合前**：检查 wounded flag，若被踢则放弃本轮

#### 3.4.3 阶段 4 锁死（Finalized）

```go
// CommitSimulation 中，在提交 SimTx 前锁死 Patch
func (s *sscService) CommitSimulation(commit *api.SimulationCommit) {
    // ... 现有逻辑 ...

    // 锁死 Patch：标记为 Finalized，不可再被 Wound
    s.retryScheduler.patchPool.Finalize(commit.TxHash)

    // 验证锁仍在
    if s.retryScheduler.IsWounded(commit.TxHash) {
        // 被踢了 → 放弃提交
        s.closeTransaction(commit.TxHash, false, "WoundedByHigherPriority")
        return
    }

    // 现有逻辑：提交 SimTx
    s.buildSignaturesForSimulation(tx.Ctx, simulation)
    err = s.txSubmitter.SubmitSimulationTx(simulation)
    // ...
}
```


## 4. 数据结构变更

### 4.1 ChainPatchNode

| 字段 | 类型 | 说明 |
|------|------|------|
| `Status` | `PatchConsumeStatus` | `Free` → `Consumed` → `Finalized` |
| `Consumer` | `common.Hash` | 占用者的 txHash |
| `Priority` | `Priority` | 占用者的优先级 |

### 4.2 TempLockView 扩展

| 字段 | 类型 | 说明 |
|------|------|------|
| `tempWriteLocks` | `map[LockKey]tempLockEntry` | 原来为 `map[LockKey]common.Hash` |
| `woundedTxs` | `map[common.Hash]struct{}` | 被踢的交易集合 |
| `TryLockWithPriority` | 方法 | 新增优先级加锁 |
| `IsWounded` | 方法 | 被踢检测 |

### 4.3 Priority

```go
type Priority struct {
    SimulationNum int    `json:"simulation_num"` // 重试次数多的优先（最高优先级）
    Nonce         uint64 `json:"nonce"`          // Nonce 小的优先
    OriginShardID uint32 `json:"origin_shard_id"`// ShardID 小的优先
}
```

> ⚠️ `Less()` 的比较顺序是 `(SimulationNum >, Nonce <, OriginShardID <)`，与字段声明顺序一致。


## 5. 端到端数据流

```text
Phase 1: 初始竞争
────────────────
retryTx_A (priority=high, nonce=50) 和 retryTx_B (priority=low, nonce=60)
竞争锁 {K0, K1}

T1: retryTx_B 先到 {
      shard 0: TryLockWithPriority(K0) → 空闲 → locked ✅
      shard 1: TryLockWithPriority(K1) → 空闲 → locked ✅
    }

T2: retryTx_A 后到 {
      shard 0: TryLockWithPriority(K0, A.pri > B.pri)
              → B 优先级低 → Wound! 踢掉 B
              → 自己拿 K0 ✅
      shard 1: 同 → Wound! 踢掉 B → 自己拿 K1 ✅
    }

T3: retryTx_B 检测到 wounded=true → 放弃本轮 → Release consumed patches

Phase 2: 高优先级的独占执行
─────────────────────────────
T4: retryTx_A all locked ✅ → tryToReSimulation → TriggerReSimulation
T5: StartReSimulation 模拟中
    (即使 C 来了，C 优先级比 A 低 → 不会被打扰)

Phase 3: 锁死提交
─────────────────
T6: StartReSimulation 完成 → aggregateResults → CommitSimulation
T7: PatchPool.Finalize(SimTx_A) → Status = Finalized
T8: 最终验证锁仍在 → 提交 SimTx_A → 不可再被抢

Phase 4: 被踢的交易自动恢复
───────────────────────────
T9: retryTx_B 在下一个 OnBlockCommitted 被 retryPool 重新触发
T10: 再次 RetryCommit 竞争锁
```


## 6. 正确性论证

### 6.1 不死锁

优先级 `(nonce, originShardId, simulationNum)` 构成全序。任意两笔 retryTx 总能分出高低。高优先级的一定能踢掉低优先级的，最终一定有一笔 retryTx 拿到所有锁。不存在循环等待。

### 6.2 不浪费区块空间

只有通过阶段 4（Finalized）的交易才会提交 SimTx 到链上。被踢的交易在阶段 1-3 就放弃了，不会出现两笔 SimTx 同时上链然后一个回滚的情况。

### 6.3 无消息增量

Wound-Wait 完全在已有 `RetryCommit` RPC 内完成。没有新增跨 shard 消息类型。`Priority` 作为参数附加在现有请求中。

### 6.4 幂等性

被踢的交易释放所有临时锁和 Consumed Patch 后，其状态与从未执行过 RetryCommit 一致。下次 OnBlockCommitted 正常重新触发。


## 7. 实验验证

### 7.1 关键指标

| 指标 | 含义 | 目标 |
|------|------|:----:|
| `retry commit success unique` | 成功锁住所有 related shard 的交易数 | >= 前轮 |
| `TriggerReSimulation` | 成功进入链上验证的交易数 | > 0（前轮为 0） |
| `leader close, commit: true` | 最终 commit 的交易数 | 显著提升 |
| `wounded count` | 被踢的交易数 | 跟踪即可 |

### 7.2 日志关键词

| 日志 | 含义 |
|------|------|
| `TryLockWithPriority: lock conflict, canWound` | 高优先级踢低优先级 |
| `TryLockWithPriority: lock conflict, canWait` | 低优先级遇到高优先级 |
| `TryLockWithPriority: lock conflict, finalied` | Finalized 状态不可踢 |
| `IsWounded: true, aborting retry` | 被踢的交易放弃本轮 |
| `Finalize patch for tx` | Patch 被锁死 |
| `WoundedByHigherPriority` | closeTransaction 原因 |


## 8. 文件变更清单

| 文件 | 变更 | 优先级 | 估算 |
|------|------|:------:|:----:|
| `ssc/api/types.go` | 新增 `Priority` 结构体；`ChainPatchNode.Consumed` 改为三态 `PatchConsumeStatus` | P0 | ~40 行 |
| `ssc/temp_lock_view.go` | 新增 `TryLockWithPriority`、`IsWounded`、`woundedTxs` | P0 | ~60 行 |
| `ssc/retry_scheduler.go` | `RetryCommit` 用新 TryLock；失败路径加 `wounded` 判断 | P0 | ~30 行 |
| `ssc/simulator_leader.go` | `CommitSimulation` 前二次验证 + `Finalize` | P0 | ~15 行 |
| `ssc/impl.go` | `closeTransaction` 加 `WoundedByHigherPriority` | P1 | ~5 行 |


## 9. 设计决策记录

| # | 决策 | 结论 | 理由 |
|---|------|------|------|
| D1 | 优先级来源 | `(nonce, originShardId, simulationNum)` | 全局唯一可排序，不新增字段 |
| D2 | 不可抢占边界 | 阶段 4（CommitSimulation） | 过了此阶段 SimTx 已提交，踢掉也没意义 |
| D3 | 被踢检测方式 | `woundedTxs` 集合 + `IsWounded` | 不需要 RPC，本地 map 检查 |
| D4 | 三态 vs 二态 | 三态（Free/Consumed/Finalized） | 二态无法表达「不可踢」语义 |
|| D5 | TempLockView vs PatchPool 做 Wound | TempLockView | 锁是 TempLockView 管的，PatchPool 只管 Patch 的消费状态 |


## 10. TODO 落地清单

### Phase 1：数据结构准备（P0，~80 行）

| # | 任务 | 文件 | 说明 | 状态 |
|---|------|------|------|:----:|
| 1.1 | 新增 `Priority` 结构体 | `ssc/api/types.go` | 3 字段：`Nonce, OriginShardID, SimulationNum` + 比较函数 `Less(p)` | 📝 |
| 1.2 | 新增 `PatchConsumeStatus` 枚举 | `ssc/api/types.go` | `Free(0)` / `Consumed(1)` / `Finalized(2)` | 📝 |
| 1.3 | `ChainPatchNode.Consumed bool` → `Status PatchConsumeStatus` | `ssc/api/types.go` | 加 `Consumer common.Hash` 和 `Priority Priority` 字段；同步更新 `Bytes()` 序列化 | 📝 |
| 1.4 | 更新 `PatchPool.Finalize` 方法 | `ssc/api/types.go` | 新增：`func (pp *PatchPool) Finalize(txHash) { node.Status = Finalized }` | 📝 |

### Phase 2：TempLockView 扩展（P0，~60 行）

| # | 任务 | 文件 | 说明 | 状态 |
|---|------|------|------|:----:|
| 2.1 | 新增 `tempLockEntry` 结构体 | `ssc/temp_lock_view.go` | `Holder common.Hash` + `Priority Priority` | 📝 |
| 2.2 | `tempWriteLocks` 改为 `map[LockKey]tempLockEntry` | `ssc/temp_lock_view.go` | 全局替换 `common.Hash` 为 `tempLockEntry`；所有读 `Holder` 的地方改为 `.Holder` | 📝 |
| 2.3 | 新增 `woundedTxs map[common.Hash]struct{}` | `ssc/temp_lock_view.go` | 初始化在 `NewTempLockView` 中 | 📝 |
| 2.4 | 新增 `TryLockWithPriority` | `ssc/temp_lock_view.go` | 实现 Wound-Wait 逻辑（见 §3.4.1） | 📝 |
| 2.5 | 新增 `canWound` 辅助函数 | `ssc/temp_lock_view.go` | 比较优先级 + 检查 Finalized | 📝 |
| 2.6 | 新增 `woundLock` 辅助函数 | `ssc/temp_lock_view.go` | 踢锁 + 标记 woundedTxs | 📝 |
| 2.7 | 新增 `IsWounded` | `ssc/temp_lock_view.go` | 检查 `woundedTxs[txHash]` | 📝 |
| 2.8 | `GarbageCollect` 清理 `woundedTxs` | `ssc/temp_lock_view.go` | 在清除交易时同时清理 `woundedTxs` | 📝 |
| 2.9 | 编译验证 | `go build ./ssc/...` | 确保旧调用方兼容 | 📝 |

### Phase 3：RetryCommit 集成 Wound-Wait（P0，~30 行）

| # | 任务 | 文件 | 说明 | 状态 |
|---|------|------|------|:----:|
| 3.1 | `RetryCommit` 调用 `TryLockWithPriority` | `ssc/retry_scheduler.go` | 替换 `tempLockView.TryLock` 为 `TryLockWithPriority(txHash, priority, reads, writes)`；从 retryTx 获取 nonce/simulationNum 构造 Priority | 📝 |
| 3.2 | 失败路径检查 `wounded` | `ssc/retry_scheduler.go` | 如果 `wounded=true`，跳过 `tryToReSimulation` 失败路径的锁释放（锁已经被抢走，不需要我们释放） | 📝 |
| 3.3 | `tryToReSimulation` 二次验证 | `ssc/retry_scheduler.go` | 在 `TriggerReSimulation` 前调用 `IsWounded(txHash)`；如果被踢则放弃本轮 | 📝 |
| 3.4 | `RetryCommit` RPC 参数扩展 | `ssc/api/sscs.go` | `Method_RetryCommit` 消息体加 `Priority` 字段 | 📝 |

### Phase 4：CommitSimulation Finalized（P0，~15 行）

| # | 任务 | 文件 | 说明 | 状态 |
|---|------|------|------|:----:|
| 4.1 | `CommitSimulation` 先 `Finalize` 后验证 | `ssc/simulator_leader.go` | 在 `StartReSimulation` 完成 `aggregateResults` 后、`thresholdSignSimulationCommit` 前调用 `patchPool.Finalize(txHash)` | 📝 |
| 4.2 | 提交前 wounded 检查 | `ssc/simulator_leader.go` | 调 `IsWounded(txHash)`，被踢则 `closeTransaction` + return | 📝 |
| 4.3 | 添加 `WoundedByHigherPriority` 关闭原因 | `ssc/impl.go` | `closeTransaction` 的 reason 参数 | 📝 |

### Phase 5：onChainPatches 清理归属（P0，~10 行）

| # | 任务 | 文件 | 说明 | 状态 |
|---|------|------|------|:----:|
| 5.1 | `CommitOrRollbackWithProof` 加 `RemoveOnChainPatch` | `ssc/committer.go` | CR 完成后链上清理 `onChainPatches[txHash]` | 📝 |
| 5.2 | `closeTransaction` 只清链下数据 | `ssc/impl.go` | 保持现有 `StaleTx`（清 `patches`） + `patchPool.Remove`，**不加** `onChainPatches` 清理 | 📝 |
| 5.3 | `OnBlockCommitted` 清理 `woundedTxs` | `ssc/retry_scheduler.go` | 被踢的交易在 retryPool 中重新触发时，清理其 `wounded` 标记 | 📝 |
| 5.4 | 日志加 wounded 统计 | `ssc/retry_scheduler.go` | 新增 Info 级别日志：`wounded count: {len(v.woundedTxs)}` 在 `OnBlockCommitted` 中 | 📝 |

### Phase 6：CXTSimulation 扩展 + onChainPatches 写入（P0，~40 行）

| # | 任务 | 文件 | 说明 | 状态 |
|---|------|------|------|:----:|
| 6.1 | `CXTSimulation` 加 `ChainPatch`/`UpstreamTxHash`/`UpstreamSimNum` | `ssc/api/types.go` | 结构体 + `Bytes()` + `WithoutSignature` 同步 | 📝 |
| 6.2 | `onChainPatches` 字段 + 方法 | `ssc/retry_scheduler.go` | `AddOnChainPatch`/`GetOnChainPatch`/`RemoveOnChainPatch`/`MergeChainPatches` | 📝 |
| 6.3 | `CommitSimulation` 构建 SimTx 时含 ChainPatch | `ssc/impl.go` | 从 callStates 提取 WriteSet 传入 CXTSimulation | 📝 |
| 6.4 | 收到 SimTx 后入 `onChainPatches` | SimTx handler | 所有节点在收到多播/链上 SimTx 时写入 | 📝 |

### Phase 7：测试与验证（P0）

| # | 任务 | 说明 | 状态 |
|---|------|------|:----:|
| 7.1 | 编译验证 | `go build ./ssc/...` + `go build ./cmd/...` 无报错 | 📝 |
| 7.2 | 实验跑通 | `test_single` 正常跑完，无 fatal error | 📝 |
| 7.3 | 漏斗验证 | `TriggerReSimulation` > 0，PatchPool 命中率不降 | 📝 |
| 7.4 | 日志确认 | 能 grep 到 `TryLockWithPriority` / `Wound` / `Finalized` / `WoundedByHigherPriority` / `onChainPatch` 日志 | 📝 |
