---
id: DSN-49
title: 链下 Patch 池统一：合并 localPatches 与 patches 为单一链下 DAG 存储
type: DSN
status: implemented
priority: P0
author: Designer
created: 2026-08-28
updated: 2026-08-29
scope: [ssc/patchpool.go, ssc/retry_scheduler.go, ssc/impl.go, ssc/verify.go, ssc/simulator.go, docs/designs/active/DSN-46-ssc-internal-pool-dag.md]
refs: [DSN-46, DSN-23, DSN-26, DSN-27, BUG-13]
---

> **状态**：已实施（2026-08-29）
> **背景**：DAG/ChainPatch 目前被实现成两套并行的链下结构 `localPatches` 与 `patches`，职责重叠、数据同源却分开维护，导致资源泄漏（`patches` 从不清理）、依赖边可成环（stack overflow）、以及 `isChainTx` 误判连锁问题。本 DSN 将其合并为**单一链下 DAG 存储**，消除冗余与不同步风险。

## 1. 概述

将当前分散在 `patchpool.go`（`localPatches` + `keyIndex` + `subscriber`）和 `retry_scheduler.go`（`patches`）的两套**链下** Patch 结构，合并为**一个**链下 DAG 存储 `offChainDAG`，同时承担「调度匹配」与「读取/构建上游」两个职责；`onChainPatches` 保持独立（链上、validator、VerifySimulation 用）。

> 原则：**链下 = 一份**，**链上与链下分开**。链下只做“能否构成 DAG / 如何调度”，链上只做“验证时是否有被依赖的 OnChainPatch”。

### 1.1 问题分层（与 DSN-46 的区别，重要）

本 DSN（DSN-49）与 DSN-46 解决的是**不同层面**的问题，两者正交、不是前后置关系：

| | DSN-49（本） | DSN-46 |
|---|---|---|
| 问题层面 | **链下模拟期**的 key 冲突 | **SimTx 与 SimTx 之间**的提交顺序/批处理 |
| 要解决什么 | 模拟时某 key 被未解锁的交易占用，用 DAG **提前读取“锁持有者的 patch 状态”**，不必等锁释放 | 去 nonce 强制串行、进池仲裁分组、给并行验证提供互不冲突批次 |
| 生命周期 | 发生在**模拟（Simulator / 重试）时**，是状态可见性/并发问题 | 发生在**提交 / 区块构建**时，是排序/批处理问题 |
| 存储 | 链下 `offChainDAG`（+ 链上 `onChainPatches` 作为验证事实） | 内部交易池 / 出块顺序 |

因此：本 DSN 合并链下存储、保证无环、统一清理，**不依赖也不前置 DSN-46**；反过来 DSN-46 也不依赖本 DSN。二者各自独立演进。

## 2. 现状与问题

### 2.1 现状：同一个“已提交 SimTx 的 WriteSet 池”存在两份链下表示

| | `localPatches` (patchpool.go) | `patches` (retry_scheduler.go) |
|---|---|---|
| 元素 | `ChainPatchNode`（状态 Free/Consumed/Finalized、Consumer、Priority） | `api.ChainNode`（含 `UpstreamTxList`） |
| 索引 | `keyIndex` + `subscriber`/`txSubKeys` | 无 |
| 写入 | `addPatch()` impl.go:1044（leader） | `AddPatch()` impl.go:1011 + `sendChainSignal()` |
| 用途 | 匹配/调度（`findCoveringSet`、`scanPatchSubscribers`） | 读/构（`readPatchChain`、`GetUpstreamTxRef`、拼 `UpstreamTxList`） |
| 清理 | `removePatch()` | **无（泄漏）** |

两者都是**链下、leader 侧、以 txHash 为 key、存同一批 SimTx 的 WriteSet**，只是形态不同、服务不同步骤。

### 2.2 问题清单（全部同源）

1. **资源泄漏**：`patches` 从不清理（实测 ~9828 残留）；`localPatches` 与 `patches` 生命周期不同步，close 时只清了其中一个。
2. **可成环 / stack overflow**：`UpstreamTxList` 由 `findCoveringSet` 从“当前 Free 且覆盖冲突 key 的 patch”推导，**不含全局顺序、也不排除自己**，依赖边可在不同轮次/节点间互相引用 → 环。
3. **误判连锁**：`isChainTx` 之前用 `ChainPatch != nil` 判断，而 `AddPatch` 给每笔都填非 nil ChainPatch → 锁检测整体失效 → 回滚-重模拟循环 → 高时延。
4. **双份同步负担**：同一 SimTx 提交要做 `AddPatch`（impl.go:1011）和 `addPatch`（impl.go:1044）两次写入、两份清理，极易不一致。

### 2.3 结论

不是某一处 bug，而是**设计上缺少“单一事实来源”**。必须先把链下合并成一份，再谈 DAG 正确性。

## 3. 设计方案

### 3.1 目标：单一链下 DAG 存储 `offChainDAG`

用一个结构同时承载调度与读取：

```go
// 链下 DAG 节点 —— 由 ChainPatchNode(调度) 与 api.ChainNode(读构) 合并而成
type OffChainPatchNode struct {
    TxHash        common.Hash      // 本节点（SimTx）的 hash
    SimulationNum int
    Patch         *api.RWSet       // 本节点 WriteSet
    UpstreamTxList []api.TxSimKey  // 上游依赖（DAG 边）—— 原 api.ChainNode 字段
    Consumer      common.Hash      // 消费它的 retryTx（原 ChainPatchNode）
    Priority      api.Priority
    CreatedAt     time.Time
    status        atomic.Int32     // PatchFree / PatchConsumed / PatchFinalized
}

// 链下 DAG 存储
type offChainDAG struct {
    nodes   sync.Map // txHash -> *OffChainPatchNode      （合并 localPatches 与 patches）
    keyIndex sync.Map // api.LockKey -> *sync.Map(txHash)  （反查：谁写了这个 key）
    subscriber sync.Map // api.LockKey -> *sync.Map(retryTxHash)
    txSubKeys  sync.Map // retryTxHash -> []api.LockKey
}
```

- **删除** `retryScheduler.localPatches` 与 `retryScheduler.patches` 两个字段，替换为 `offChainDAG` 一个字段。
- `api.ChainNode`（proto/API 层）保持不变，仅在读取/构建时从 `OffChainPatchNode` 生成，不落地为第二份存储。

### 3.2 存储关系总览（合并后）

```
链下 offChainDAG（leader 侧，一份）
  ├─ 调度：findCoveringSet / scanPatchSubscribers / tryConsume / release / finalize
  └─ 读构：readPatchChain / GetUpstreamTxRef / 拼 UpstreamTxList

链上 onChainPatches（所有 validator，一份）
  └─ AddOnChainPatch(验证通过后) / ReadOnChainPatch(验证时) / RemoveOnChainPatch(CR后)
```

### 3.3 写入路径合并（关键改动）

原「提交 SimTx」的两次写入（impl.go:1011 的 `AddPatch` + impl.go:1044 的 `addPatch`）**合并为一次**：

```go
// 提交 SimTx 前：链下登记（含本节点 WriteSet + 上游依赖）
dag.AddNode(txHash, simNum, chainPatch, upstreamTxList)   // 状态 = PatchFree

// 提交成功后（leader）：进入“可被匹配”状态 + 触发 subscriber 扫描
dag.MarkReady(txHash)                                     // 原 addPatch 的 keyIndex 写入 + OnPatchPoolUpdated
```

即：`AddNode` 建节点并记录 `UpstreamTxList`（替代原 `AddPatch`），`MarkReady` 建立 `keyIndex` 反查并触发 `scanPatchSubscribers`（替代原 `addPatch`）。这样不再需要两份写入。

### 3.4 读取路径合并

- `readPatchChain`（原读 `patches`）与 `findCoveringSet`（原读 `localPatches`）**改为读同一个 `offChainDAG.nodes`**。
- `GetChainPatchRef` / `GetUpstreamTxRef` 同样读 `offChainDAG.nodes`。
- `OnChainPatch` 系列（`ReadOnChainPatch` / `AddOnChainPatch` / `RemoveOnChainPatch`）不动，仍走 `onChainPatches`。

### 3.5 清理路径统一

`closeTransaction` 里对链下的清理收敛为**一处**：

```go
dag.Remove(txHash)   // 原子删除 node + keyIndex + subscriber + txSubKeys
```

（替代原来的 `removePatch(localPatches)` + `patches.Delete` 两处。）

### 3.6 无环保障（由构造保证 + 双保险）

1. **构造保证（主）**：`findCoveringSet` / `scanPatchSubscribers` 在挑选上游时：
   - **排除自己**（`up.TxHash != 本节点`）；
   - **只选严格更早/更小顺序**的节点（按 `(Priority, TxHash)` 全局确定序，或按提交序号），保证依赖图按构造无环。
2. **遍历防环（副）**：`readPatchChain` / `ReadOnChainPatch` 保留 visited 集合（已落地），作为兜底。

### 3.7 一致性 / 确定性

- 链下 DAG 是 leader 侧的内存调度视图，**不序列化、不落盘**；其节点顺序仅用于“模拟期提前读取未解锁状态”的调度判定，不代表链上提交顺序。
- 链上提交顺序仍由既有的区块/共识流程决定（与 DSN-46 的 SimTx 排序属不同层，本 DSN 不涉及）。
- `onChainPatches` 写入时点仍为“VerifySimulation 验证通过后”，与链下 `MarkReady` 的先后关系在文档中明确（链下先 MarkReady 做调度，链上验证通过后才成为可依赖事实）。

## 4. 变更文件清单

| 文件 | 改动 |
|:-----|:-----|
| `ssc/patchpool.go` | 新增 `OffChainPatchNode` / `offChainDAG`；`addPatch`→`AddNode`+`MarkReady`；`removePatch`→`Remove`；`findCoveringSet`/`findCoveringPatch`/`patchesHaveConflict` 改读 `offChainDAG` |
| `ssc/retry_scheduler.go` | 删除 `localPatches` 与 `patches` 字段，换 `offChainDAG`；`readPatchChain`/`GetChainPatchRef`/`GetUpstreamTxRef` 改读新存储；`sendChainSignal` 用新节点；`closeTransaction` 清理收敛 |
| `ssc/impl.go` | 合并 impl.go:1011 与 1044 的两次写入为一次；`closeTransaction` 用 `dag.Remove` |
| `ssc/verify.go` | `AddOnChainPatch`/`ReadOnChainPatch` 调用不变（仍 onChainPatches）；`isChainTx` 逻辑保留已修复版本 |
| `ssc/simulator.go` | `readPatchChain` 调用点同步改新存储 |
| 测试 | 新增单测：无自环、无环、findCoveringSet 正确性、close 后 offChainDAG 清空、多上游消费 |

## 5. 备选方案

### 方案 A：合并为单一 `offChainDAG`（选择）
- 优点：单一事实来源，一份写入/清理；环与泄漏根因消除；为“模拟期 DAG 提前读状态”打稳基础。
- 缺点：改动面较大，需回归验证。

### 方案 B：继续保留两套，仅补清理 + 防环
- 优点：改动小、风险低。
- 缺点：不治本；两份仍要同步，后续任何新逻辑都会再踩不一致。

### 方案 C：另行落地 DSN-46（SimTx 排序/批处理）
- 优点：解决 SimTx 层排序与并行验证。
- 缺点：与 DSN-49 是**两个独立层面**，不替代也不被替代；二者可各自推进，本 DSN 不阻塞也不依赖它。

## 6. 关键决策
- **链下合并为一份，链上 `onChainPatches` 保持独立**：语义本就不同（调度 vs 验证事实），不强行合一。
- **`api.ChainNode` 不落地为第二份存储**：仅在读取时从 `OffChainPatchNode` 生成，避免再造冗余。
- **无环由“构造保证（排除自己 + 严格顺序）”为主，visited 兜底**。
- **不序列化/不落盘**：链下 DAG 是 leader 内存视图，顺序权威仍为区块数组顺序。

## 7. 风险与边界
- 合并期间需保证 `readPatchChain`/`GetUpstreamTxRef` 读到的数据与改造前语义一致（回归重点）。
- `onChainPatches` 的写入时点与链下 `MarkReady` 的先后关系要在代码注释中明确，避免“链下认为可依赖、链上还没有”。
- 本 DSN 只解决“单一事实来源 + 无环 + 清理统一”，**不涉及 SimTx 层的排序/批处理（那是 DSN-46 的独立范畴）**。链下模拟 DAG 的无环由构造保证（排除自己 + 按确定性优先级选上游），不依赖 DSN-46。

## 8. 实施记录（2026-08-29）

已按本设计完成核心合并：

- **新增** `OffChainPatchNode`（= 原 `ChainPatchNode` + `UpstreamTxList`）与 `offChainDAG`（`nodes`/`keyIndex`/`subscriber`/`txSubKeys`），位于 `ssc/patchpool.go`。
- **删除** `retryScheduler.localPatches` 与 `retryScheduler.patches` 两个字段，替换为 `retryScheduler.offChainDAG` 一个字段；`onChainPatches` 保持独立。
- **写入合并**：`impl.go` 中 `AddPatch` + `addPatch`/`OnPatchPoolUpdated` 两处合并为 `offChainDAG.AddNode(...)`（无条件建节点）+ `offChainDAG.MarkReady(...)`（leader 建立 keyIndex，并通过 `onReady` 回调触发 `scanPatchSubscribers`，等价于原 `OnPatchPoolUpdated`）。
- **读取统一**：`readPatchChain` / `GetChainPatchRef` / `GetUpstreamTxRef` 全部改读 `offChainDAG.nodes`；`sendChainSignal` 通过 `AddNode` 写入单一 DAG，不再落地第二份 `api.ChainNode`。
- **清理收敛**：`closeTransaction` 收敛为 `offChainDAG.Remove(txHash)` 一处，原子删除 node + keyIndex + subscriber + txSubKeys（同时消除原 `patches` 泄漏）。
- **无环保障**：`findCoveringSet` 新增 `exclude`（排除自己）与 `maxSimNum`（仅选**严格更早** `SimulationNum<maxSimNum`，依赖边必须指向更早轮次，链深有界）；`scanPatchSubscribers` 传入 `retryTx.SimulationNum`，`RetryCommit` 传 `0`（仅排除自己）以保留既有 rescue；`readPatchChain` 保留 visited 兜底。

> **回归修复（2026-08-29）**：初版实现中 `MarkReady` 只建 keyIndex 却未触发 subscriber 扫描，导致 SimTx 提交后 DAG chaining 失效（retryTx 无法被及时救援）→ 实验中超时明显增多。已通过 `MarkReady` 增加 `onReady` 回调恢复 `scanPatchSubscribers` 触发，并补充 `TestOffChainDAGMarkReadyTriggersScan` 回归测试。
- **测试**：新增 `ssc/offchain_dag_test.go`，覆盖 findCoveringSet 正确性、排除自己、严格更早、Remove 清空（node/keyIndex/subscriber/txSubKeys）、多上游消费与 release。

> 注：`go test ./ssc/` 目前因仓库内 `simulation_test.go`/`simulation_params_test.go` 与新版 `Simulator` 的既有冲突而无法整体编译（与本次改动无关）；本次新增测试在临时移开这两个旧测试文件后通过（PASS）。

> **链深回归修复（2026-08-29，rate=300 实验验证）**：放宽为 `<=` 后，同一轮次内可无限 chaining，实测 `chainLengthDist` 深达 37、`retryAdd=103845`/`sp1Started=372930`（每笔 ~5/19 次），最终 `PoolTimeout=10022`。已恢复**严格更早 `<`**，使依赖图按轮次严格递增、链深有界，阻断同轮无限 churning。
