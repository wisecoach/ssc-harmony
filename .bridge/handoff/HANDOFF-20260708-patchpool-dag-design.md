# HANDOFF — PatchPool 命中率低与 DAG 链式依赖方案

> from_session: `20260707_dsn22_lockwait_retrycommit_fix`
> from_role: hermes-agent (hentai_coder)
> to_role: 继续优化 PatchPool 匹配率 + DAG 依赖链的会话

## 实验摘要

| 实验 | 提交率 | TPS | P50 | P90 | 说明 |
|:----|:-----:|:---:|:---:|:---:|:-----|
| Ablation B（无重试，基线） | 73.3% | 69.2 | — | — | |
| stateDB 预检 + 信号聚合 | 96.1% | 91.5 | 62.5s | 141.8s | |
| v1 DSN-22（三阶段，无 LockWait） | 51.0% | 49.0 | 20.1s | 49.3s | Phase 2b 失败后交易丢失 |
| v2 DSN-22（+ LockWait Pool） | **80.1%** | **76.9** | 39.4s | 96.1s | 当前 |

## 已完成改动

### 1. RetryCommit 锁检查顺序重构（DSN-22 v1）

**文件**: `ssc/retry_scheduler.go` — `RetryCommit()`

旧逻辑：
```
PatchPool.HasConflict → 任意 1 key 匹配就跳所有锁 → Locked=true
```

新逻辑（三阶段）：
```
Phase 1: TryLockWithPriority (tempLockView)
Phase 2: stateDB.CheckLock (遍历全部 key)
Phase 2b: 仅冲突 key 查 PatchPool.FindCovering → 覆盖全部则跳锁，否则 OnChainLockConflict
```

**效果**：消除了「PatchPool 跳锁 → StartReSimulation 模拟时撞 stateDB 锁」的死循环。但代价是 PatchPool 命中率从之前的无条件跳锁骤降。

### 2. LockWait Pool（DSN-22 v2）

**文件**: `ssc/retry_scheduler.go`

Phase 2b 失败（stateDB 冲突 + 无 Patch 覆盖）时，`tryToReSimulation` 将该 tx 移入 LockWait Pool（从 retryPool 删除），`OnBlockCommitted` 中扫描 LockWait Pool，stateDB 解锁后移回 retryPool。

新增：
- `lockWaitPool` / `lockWaitPoolEnterBlock` / `lockWaitTxs` → 封装为 `LockWaitStore`（自带 Mutex）
- `LockWaitStore.Add` / `Remove` / `ForEach` / `ForEachEntry` / `Len`
- 超时兜底（用 `MaxRetriesTotal` 阈值）

**效果**：提交率从 51% → 80%。但 unfinished 1984（19.8%）中的原因需要进一步分析。

### 3. `rs.mu` 全局锁删除 + sync.Map 重构

**文件**: `ssc/retry_scheduler.go`

所有 map 字段均从 `map[K]V` + `rs.mu` 改为 `sync.Map`：

| 字段 | 保护方式 |
|:-----|:---------|
| `retryPool` | `sync.Map` |
| `passivePool` | `sync.Map` |
| `staleTxs` | `sync.Map` |
| `signals` | `sync.Map` + `*signalMap`（内层带锁） |
| `patches` | `sync.Map` + `*patchMap`（内层带锁） |
| `onChainPatches` | `sync.Map` + `*patchMap`（内层带锁） |
| `consumedPatches` | `sync.Map` |
| `reSimInFlight` | `sync.Map` |
| `lockWait` | `LockWaitStore`（自带 Mutex） |

`rs.mu sync.RWMutex` 已完全删除。

### 4. patches/onChainPatches/signals 内层 map 加锁

新增 `patchMap` / `signalMap` 结构体，每个 txHash 维度的内层 map 自带 `sync.Mutex`，实现 per-tx 粒度并发保护，避免 `concurrent map read/write`。

### 5. 代码变更清单

```
ssc/retry_scheduler.go:
  - RetryCommit 改为三阶段（DSN-22 v1）
  - 新增 LockWait Pool（DSN-22 v2）— LockWaitStore + tryToReSimulation 入池 + OnBlockCommitted 出池
  - 新增 patchMap/signalMap 结构体（内层 map 加锁）
  - 所有 map 字段改为 sync.Map（consumedPatches/reSimInFlight/retryPool/passivePool/staleTxs/signals/patches/onChainPatches）
  - rs.mu 全局锁删除
  - printState 注释（cycle() 中调用已注释）
  - OnPatchPoolUpdated 内加日志统计
  - chainNextSim 加 keyOverlap 统计
  - OnBlockCommitted 加热 key 统计

ssc/api/types.go:
  - 新增 PatchPool.FindCovering(conflictKeys) 方法
```

## 当前数据

### 最新实验（v2 DSN-22, 80.14% 提交率）

```
chainNextSim scanning:           14,166
  → no downstream:               13,652 (96.4%)
  → reservation completed:        3,244 (22.9%)
  → skipped reserved:            48,995 (15:1 vs completed)

HandleRetrySignal:               105,602
PatchPool FindCovering 命中:        214 (各 shard 合计)
LockWait 进池:                    1,481
LockWait 出池(unlocked):         1,220

lockWaitPoolSize (末区块):         7~217
hotWriteKeys (max):               48~235
maxWriteContention (max):         24~102
```

### 对比旧实验（v1 DSN-22 无条件跳锁时代）

```
stateDB lock conflict:            8,213 (port 9000)  → 这次类似
PatchPool 命中 (旧无条件):         229  → 这次 35 (仅 conflict 覆盖)
retry commit success:             7,169  → 这次 35,217 (包含重试)
```

## 发现的问题

### 1. PatchPool 命中率低（核心问题）

`FindCovering` 要求单笔 Patch 覆盖**全部**冲突 key。热点 key 交易往往读写多个 key，Patch 只覆盖了其中部分 key，导致无法跳锁。

**当前实现细节**：
```go
// FindCovering 遍历 conflictKeys，只返回覆盖全部 conflictKeys 的 Patch
for _, key := range conflictKeys {
    if owners, ok := pp.KeyIndex[key]; ok {
        for txHash := range owners {
            candidates[txHash]++
        }
    } else {
        return false, nil  // 任意 key 无 Patch → 立即失败
    }
}
// 只有 cnt == len(conflictKeys) 的 Patch 才返回
```

### 2. 大量 tradeoff（当笔交易读写多个 key 时命中率骤降）

单 key 交易（1 个冲突 key）命中率 = `P(该 key 有 Patch) = ~22.9%`（chainNextSim 的 reservation completed 比例）。
多 key 交易（N 个冲突 key）命中率 = `P(每个 key 都有单笔 Patch 覆盖所有) = 22.9%^N` → 极低。

### 3. unfinished 1984 的根因

- 一部分在 LockWait Pool 中等待 stateDB 解锁（lockWait 出池 1,220 vs 进池 1,481，差值 261 还在池中或超时被关了）
- 另一部分是在 `chainNextSim` 的 `reservation` 中被过滤掉的（48,995 skipped reserved）
- 还有一部分是 tempLockView 层面被 Wound 掉的

## 建议继续方向

### 1. PatchPool DAG 依赖链（关键方向）

**问题**：`FindCovering` 要求单笔 Patch 覆盖全部冲突 key → 多 key 交易几乎无法命中。

**方案**：允许多笔 Patch 联合覆盖。改动点：

```go
// 返回能联合覆盖全部 conflictKeys 的一组 Patch
func (pp *PatchPool) FindCovering(conflictKeys []api.LockKey) (bool, []*ChainPatchNode)
```

- 对每个 conflictKey 找其 owner Patch
- 取步进交集/并集：找到一组 Patch 使得它们的 KeyIndex 并集覆盖全部 conflictKeys
- 全部 TryConsume，合并 WriteSet 后 SetChainPatch
- 如果任意一个 TryConsume 失败（已被别的 retryTx 抢了），全部 Release

**约束**：
- 所有匹配 Patch 都不能是 Finalized 或 Consumed
- 合并多个 WriteSet 时冲突 key 以后消费的优先
- 消费失败时全部 Release（单个 retryTx 的原子性）

**风险**：
- 多上游 SimTx 可能在不同时间 Commit/回滚，DAG 一致性需要保证
- 但当前架构中 Patch 被 Consumed 后不会释放给其他 retryTx（只有 Release 才会），所以多 Patch 的原子消费可以保证

### 2. 减少 `skipped reserved` 的 48,995

`chainNextSim` 的 reservation 机制选中一笔 tx 后，把所有 key 封入 reservedKeys，后续同 key 的 tx 全被跳过。在热 key 场景下这是过度串行化。

**方向**：reservation 改为只封被选中的 key 本身，不封 retryTx 的完整 key 集合。或多个同 key tx 可以同时被选中发信号（让锁层面 Wound-Wait 仲裁）。

### 3. 进一步降低时延

P50 39.4s、P90 96.1s。主要瓶颈仍在跨分片验证阶段（SP1 等待 + 共识），retry 路径的优化对时延改善有限。

## 配置参考

| 参数 | 当前值 |
|:-----|:------:|
| `MaxOnChainRetries` | 0（不限） |
| `MaxRetriesTotal` | 0（不限） |
| `PoolTimeout` | 0（不限） |
| `EnableLockOnConflict` | false |

`delay=10`, `rate=100`, `4 shards`, `4 validators/shard`

## 已知坑

- `rs.mu` 已删除，所有 map 操作用 `sync.Map` 代替。但 `patches`/`onChainPatches`/`signals` 的内层 map（`map[int]*...`）需要 `patchMap`/`signalMap` 的 Mutex 保护——任何新加的内层 map 写操作都必须加锁
- `printState` 已被注释（cycle 中调用也被注释），如果需要重新启用需要重写使用 sync.Map 的方式
- 本地缺 BLS 库无法编译，远程编译用 `go build ./ssc/ ./ssc/api/`
