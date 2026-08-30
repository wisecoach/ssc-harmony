---
id: DSN-50
title: DAG 救援收敛：救援前覆盖完整性校验 + 链深上限
type: DSN
status: implemented
priority: P0
author: Designer
created: 2026-08-29
updated: 2026-08-29
scope: [ssc/patchpool.go, ssc/retry_scheduler.go, ssc/impl.go, ssc/config.go, docs/designs/active/DSN-49-unify-offchain-patch-pool.md]
refs: [DSN-49, DSN-46, DSN-23, DSN-26, BUG-13]
---

> **状态**：已实施（2026-08-29）
> **背景**：rate=300 实验（2026-08-29）显示 DAG 已大量生效（`retryCommitPatchHit=5956`、`PatchMiss=0`、rescue-share=77.8%），但成功率仅 57.77%、`PoolTimeout=10022`、`retryAdd=103845`、`sp1Started=372930`、`chainLengthDist` 最大深达 **37**。根因是**链式救援是“尽力而为的局部覆盖”**：只覆盖“救回那一刻能看到的、被 Free Patch 写过的冲突 key”，重模拟是全新执行，会触碰到未被覆盖的 key → 再次冲突 → 再次救援 → 无限 churn。本 DSN 对救援做两道收敛：**救援前覆盖完整性校验** + **链深上限**。

## 1. 概述

在不改变 DSN-49「单一链下 DAG」存储结构的前提下，给 DAG 救援加两道“闸门”，把“无脑 chaining”收敛为“**只在能完整覆盖、且链深可控时才 chaining，否则退回等锁释放**”：

1. **覆盖完整性校验（Coverage Completeness）**：救援前，确认所选 Patch 联合覆盖该交易**完整 ReadSet ∪ WriteSet**（而非仅当时的冲突 key）。覆盖不全 → 不链式救援，退回 `OnChainLockConflict` → 等 `OnBlockCommitted` 锁释放再试。
2. **链深上限（Chain Depth Cap）**：为链下 DAG 节点显式记录 `Depth`，救援时若生成的新节点深度将超过 `MaxChainDepth` → 不链式救援，退回等锁。

> 原则：**DAG 只在“能一次救干净、且不会失控变深”时才用**；否则“等锁释放”是更便宜、更不会 churn 的路。这与 DSN-46（提交顺序/批处理）正交：本 DSN 收敛链下救援的“覆盖面”与“深度”，DSN-46 解决 SimTx 之间的排序/并行。

## 2. 现状与问题（证据）

### 2.1 为什么“覆盖了还会再冲突”（rate=300 数据 + 代码）

| 现象 | 代码依据 |
|---|---|
| 只覆盖“被 Free Patch 写过”的 key | `findCoveringSet` 走 `keyIndex`（谁写了该 key），且只选 `Status()==PatchFree` |
| 救回基于“上一次模拟的 RWSet” | `RetryCommit` 用 `retryTx.ReadSet∪WriteSet`（`AddToRetry` 时从上一次 callStates 提取）或 `conflictKeys` |
| 重模拟会换一批 key | `StartReSimulation` 用 `lastReq.Tx` 全新执行，读到 patched 值后控制流可能走向不同分支 → 新 key |
| ChainPatch 只是上游 WriteSet 合并 | `mergeRWSet` 只合并 `WriteState`；上游没写的 key 无法被覆盖 |
| Phase 2b 只允许 1 个 Patch | `const maxPatches = 1` → 多 Patch 联合覆盖场景直接放弃 |
| 救回后有新锁 | 消费 Patch 到真正重模拟之间存在时间窗，其他交易可能锁住新 key |

### 2.2 链深失控

- `chainLengthDist` 最大 37、`chainCommitDist` 最大 32、`retry_limit_exceeded` simNum 到 27。
- 每条链式救援都会 `sendChainSignal → HandleRetrySignal → StartReSimulation`，若再冲突又救援 → 链无限变深 → 直到 sp1/pool 超时或 MAX_TOTAL。
- DSN-49 已把 `findCoveringSet` 收敛为“严格更早轮次”（`SimulationNum < max`），把链深压回“轮次级”，但轮次本身仍可涨到几十，**仍需显式深度上限**。

### 2.3 结论

不是 DAG 没生效，而是**救援缺少“可救性判定”**：不校验覆盖面、不控制深度，导致大量交易被拖入无限重试。必须先加这两道闸门。

## 3. 设计方案

### 3.1 覆盖完整性校验

#### 3.1.1 定义

对一笔要救援的 retryTx，记其**已知读写集**为：

```
needKeys = retryTx.ReadSet ∪ retryTx.WriteSet   // 来自上一次模拟的 callStates（AddToRetry 已提取）
```

一组候选 Patch `P` 称为**完整覆盖**，当且仅当：

```
∀ key ∈ needKeys : ∃ p ∈ P, key ∈ writes(p)   // 每个 key 至少被某个选中 Patch 写过
```

（注：`findCoveringSet` 已保证“它被传入的 key 集合”被覆盖；本校验是把传入集合从“仅冲突 key”扩展为“完整 needKeys”，并显式校验。）

#### 3.1.2 新增方法（`ssc/patchpool.go`）

```go
// isFullyCovered 检查一组 Patch 是否联合覆盖 needKeys 的全部 key。
func (dag *offChainDAG) isFullyCovered(needKeys []api.LockKey, patches []*OffChainPatchNode) bool {
    covered := make(map[api.LockKey]bool)
    for _, p := range patches {
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
```

#### 3.1.3 应用点（救援决策处都加闸门）

| 救援路径 | 当前 | 改后 |
|---|---|---|
| `scanPatchSubscribers`（建上游边 + sendChainSignal） | `findCoveringSet(allKeys, ...)`，allKeys=ReadSet∪WriteSet | 加 `isFullyCovered(allKeys, patches)`，不全则跳过该候选 |
| `RetryCommit` Phase 1b（TLV 冲突补救） | `findCoveringSet(allKeys, ...)`，allKeys=ReadSet∪WriteSet | 加 `isFullyCovered(allKeys, patches)`，不全则不救、走等锁 |
| `RetryCommit` Phase 2b（stateDB 冲突补救） | 只用 `conflictKeys` + `maxPatches=1` | **改用完整 `needKeys`** 做 `findCoveringSet`（去掉/放宽 `maxPatches` 硬限制），并 `isFullyCovered`；不全则不救 |

> 关键变化：Phase 2b 从“只覆盖冲突 key”改为“覆盖完整已知读写集”。这样才能保证重模拟不会因为“没被覆盖的已知 key”再次冲突。新出现的 key（控制流变化导致）无法预知，仍可能冲突——但覆盖面大幅提升，churn 显著下降；剩余冲突交给“等锁释放”路径处理。

#### 3.1.4 不完整时的行为

- 不消费任何 Patch、不 `SetChainPatch`、不发 `sendChainSignal`。
- 走原有“等锁”路径：`RetryCommit` 返回 `Locked=false, OnChainLockConflict=true` → 等待 `OnBlockCommitted` 锁释放后重试（`OnBlockCommitted` 里已有 `querySubscribers(releasedKeys)` 的二次救援机会）。

### 3.2 链深上限

#### 3.2.1 定义

为链下 DAG 节点显式记录**DAG 深度**（根节点 Depth=1，非根 = max(上游 Depth)+1）。救援时若新节点深度将超过 `MaxChainDepth`，则不链式救援。

#### 3.2.2 数据结构（`ssc/patchpool.go`）

```go
type OffChainPatchNode struct {
    ...
    Depth int  // DAG 深度：根=1；否则 max(上游 Depth)+1
}

// MaxChainDepth 默认值（可配置）
const defaultMaxChainDepth = 5
```

`AddNode` 在写入时根据 `upstreamTxList` 计算 Depth：

```go
func (dag *offChainDAG) AddNode(txHash common.Hash, simNum int, patch *api.RWSet, upstreamTxList []api.TxSimKey) {
    depth := 1
    for _, up := range upstreamTxList {
        if nv, ok := dag.nodes.Load(up.TxHash); ok {
            if d := nv.(*OffChainPatchNode).Depth + 1; d > depth {
                depth = d
            }
        }
    }
    ...
    node.Depth = depth
}
```

#### 3.2.3 救援前深度判定

在 `scanPatchSubscribers` / `RetryCommit` 里，消费 Patch 前计算“将会生成的深度”：

```go
// 计算以这些 patches 为上游时，新节点的深度
func chainDepthOf(patches []*OffChainPatchNode) int {
    d := 1
    for _, p := range patches {
        if p.Depth+1 > d { d = p.Depth + 1 }
    }
    return d
}

if chainDepthOf(patches) > maxChainDepth {
    // 不链式救援，走等锁
    continue / return Locked=false
}
```

#### 3.2.4 配置

- 新增配置项 `SSC.MaxChainDepth`（默认 5），由 `api.Config` / `TimeoutConfig` 传入 `retryScheduler`。
- 统计：`chainRetryStats` 增加差分 `SigChainDepthCapped`（“因深度上限放弃救援”次数），进 `CHAIN_RETRY_STATS`，便于实验观察。

### 3.3 与“严格更早轮次”（DSN-49）的关系

- DSN-49 的 `SimulationNum < max` 保证依赖边按轮次递增、**图无环**；
- 本 DSN 的 `Depth` 保证**图深度有界**（不随轮次/重试次数无限增长）；
- 二者互补：一个管“无环”，一个管“有界深度”。`Depth` 与 `SimulationNum` 不完全等价（同一轮次可多次重试），故需独立字段。

### 3.4 行为汇总（决策表）

| 条件 | 动作 |
|---|---|
| 有 Free Patch 联合覆盖完整 needKeys，且深度 ≤ MaxChainDepth | ✅ 链式救援（消费 Patch + SetChainPatch + sendChainSignal） |
| 覆盖不全 或 深度 > MaxChainDepth | ❌ 不救援，返回 `Locked=false, OnChainLockConflict=true`，等 `OnBlockCommitted` 锁释放 |
| 等锁释放后仍有冲突 | 再次进入 RetryCommit，重复上述判定（但不会无限 churn，因为每次都走“覆盖/深度”闸门） |

## 4. 变更文件清单

| 文件 | 改动 |
|:-----|:-----|
| `ssc/patchpool.go` | `OffChainPatchNode` 加 `Depth`；`AddNode` 计算 Depth；新增 `isFullyCovered` / `chainDepthOf`；`scanPatchSubscribers` 加覆盖+深度闸门 |
| `ssc/retry_scheduler.go` | `RetryCommit` Phase 1b/2b 改用完整 `needKeys` + `isFullyCovered` + 深度判定；Phase 2b 移除/放宽 `maxPatches=1`；`MaxChainDepth` 字段与初始化；`chainRetryStats` 增加 `SigChainDepthCapped` |
| `ssc/impl.go` | 从配置读取 `MaxChainDepth` 传入 `newRetryScheduler` |
| `ssc/api/*` | （如需要）`TimeoutConfig`/`Config` 增加 `MaxChainDepth` 字段 |
| 测试 | 新增：`isFullyCovered` 正确性（覆盖/不覆盖）、`chainDepthOf` 计算、深度上限时放弃救援、覆盖不全时放弃救援、完整覆盖且深度够时正常救援 |

## 5. 备选方案

### 方案 A：覆盖完整性 + 链深上限（选择）
- 优点：直接掐断“覆盖不全/无限变深”两条 churn 主因；改动集中在救援判定，风险可控。
- 缺点：覆盖完整性基于“上一次模拟的已知 RWSet”，控制流变化产生的新 key 仍可能漏（剩余冲突交给等锁路径兜底）。

### 方案 B：仅链深上限，不做覆盖完整性
- 优点：改动更小。
- 缺点：仍会因“覆盖不全”反复冲突（同一深度内反复重试），churn 压不下来。

### 方案 C：直接上 DSN-46（提交顺序/批处理 + 等锁 vs chaining 决策）
- 优点：从根本上统一调度。
- 缺点：改动面大、周期长；本 DSN 的两道闸门可作为 DSN-46 的前置最小止血，独立推进。

## 6. 关键决策
- **救援必须“完整覆盖已知读写集 + 深度受控”，否则退回等锁释放**。
- **`Depth` 独立于 `SimulationNum`**：用显式 DAG 深度而非轮次号，避免“同轮多次重试”导致深度被低估。
- **Phase 2b 放弃 `maxPatches=1` 硬限制**，改为完整覆盖判定（多 Patch 联合覆盖是合法的，只要覆盖全）。
- 不覆盖/超深时不发 `sendChainSignal`、不 `SetChainPatch`、不消费 Patch，保证“没救成就不产生副作用”。

## 7. 风险与边界
- **覆盖完整性基于已知 RWSet**：控制流变化引入的新 key 无法预先覆盖，仍可能触发等锁后重试；这是可接受边界，交给 DSN-46 的“等锁 vs chaining”决策进一步收敛。
- **`MaxChainDepth` 取值**：过小会牺牲 DAG 收益（rescue-share 下降），过大又回到深链；默认 5，需实验调参。
- 与 DSN-49 的 `MarkReady → scanPatchSubscribers` 触发链保持兼容，本 DSN 只在救援判定处加闸门，不改存储结构与触发时序。
- 本 DSN 只收敛链下救援的覆盖面与深度，**不涉及 SimTx 排序/批处理（DSN-46 范畴）**。

## 8. 验证方式
- 单元测试：覆盖完整性、深度计算、深度上限放弃、覆盖不全放弃、正常救援。
- 实验（rate=300 重跑，用 `scripts/local-ssc-retry-stats.py`）对比：
  - `chainLengthDist` / `chainCommitDist` 最大值应明显回落（≤ MaxChainDepth 附近）；
  - `retryAdd` / `sp1Started` / `poolStarted` 应大幅下降；
  - `PoolTimeout` 下降、成功率回升；
  - 新增 `SigChainDepthCapped` 可观测“因深度放弃”的次数，用于调 MaxChainDepth。

## 9. 实施记录（2026-08-29）

按本设计完成两道闸门：

- **覆盖完整性**：`offChainDAG.isFullyCovered(needKeys, patches)` 校验每个 key 都被至少一个选中 Patch 写过；
  - `scanPatchSubscribers`（建上游边路径）在候选收集处加 `isFullyCovered` 闸门；
  - `RetryCommit` Phase 1b 加 `isFullyCovered` 闸门；
  - `RetryCommit` Phase 2b 从“仅 conflictKeys + `maxPatches=1`”改为“完整 `needKeys` + `isFullyCovered`”，移除 `maxPatches=1` 硬限制。
- **链深上限**：`OffChainPatchNode` 增加 `Depth`（根=1，非根=max(上游 Depth)+1，`AddNode` 写入时计算）；
  - 新增 `chainDepthOf(patches)` 计算“救援后新节点深度”；
  - `TimeoutConfig.MaxChainDepth`（默认 `defaultMaxChainDepthValue=5`，`defaultMaxChainDepth` 归一化）；
  - 三条救援路径均加 `chainDepthOf(patches) > maxChainDepth` 闸门，超深放弃救援；
  - `chainRetryStats.SigChainDepthCapped` 记录“因深度放弃”次数，进 `CHAIN_RETRY_STATS`。
- 不满足覆盖/深度时：不消费 Patch、不 `SetChainPatch`、不发 `sendChainSignal`，返回 `OnChainLockConflict` → 等 `OnBlockCommitted` 锁释放。

**测试**：新增 `TestOffChainDAGDepthComputation`、`TestOffChainDAGIsFullyCovered`、`TestOffChainDAGChainDepthOf`、`TestDefaultMaxChainDepth`；`go build ./ssc/... ./rpc/... ./node/...` 通过，DAG 相关单测全部 PASS。
