---
id: DSN-51
title: DAG-patch 重写：以“同块多写”为目标的确定性主动建图（链下建图 / 链上验证）
type: DSN
status: planned
priority: P0
author: Designer
created: 2026-08-29
updated: 2026-08-29
scope: [ssc/patchpool.go, ssc/retry_scheduler.go, ssc/impl.go, ssc/simulator.go, ssc/temp_lock_view.go, ssc/internal_pool.go, ssc/api/types.go, node/worker/worker.go]
refs: [BRF-09, DSN-46, DSN-49, DSN-50, DSN-23, DSN-26, BUG-11, BUG-13]
---

> **状态**：设计阶段（重写 DAG-patch 方案）
> **背景**：BRF-09 决定重写整个 DAG-patch 方案。本 DSN 明确方案的核心价值 = **在一个区块内对同一状态进行多次修改**，并据此给出**链下建图 / 链上验证**的清晰分界与重写设计。

## 1. 概述

DAG-patch 的核心不是“锁冲突的救援”，而是**打破“每个区块同一状态只能写一次”的读写集锁限制**：

- 常规锁模型下，SimTx A 写 key `K` → `K` 上锁 → 同块另一笔 B 也写 `K` → B 撞锁 → 必须等下一块。
- DAG-patch 用**依赖边（UpstreamTxList）**让 B 在**同一区块内**基于 A 产生的 `K` 新值继续执行并再次写 `K`，顺序由“A→B”这条边确定，验证时 `isChainTx` 跳过写锁检查。

因此重写**必须保留这个能力**，而不是把它当成可去掉的救援。要重写的是**DAG 的构造时机与确定性**：从“冲突后反应式找 Patch 救援、反复重试”改为“在模拟/重试期（交易还未打包成 SimTx 时）主动、确定、完整地建图”。

## 2. 现状与问题

### 2.1 核心能力现状（工作正常，要保留）

| 环节 | 代码 | 作用 |
|---|---|---|
| 链上判定链式交易 | `verify.go:338` `isChainTx := len(simulation.UpstreamTxList)>0` | 链式交易**跳过 `CheckLock` 写锁检查** → 同块多写的合法性来源 |
| 链上查上游期望值 | `verify.go:424` `ReadOnChainPatch` | 验证 B 读到的值确实来自上游 A |
| 链上落验证事实 | `verify.go:610` `AddOnChainPatch`（验证通过后） | 成为可被依赖的事实 |
| 链下读上游 | `simulator.go:878/928/1048` `readPatchChain`/`GetChainPatchRef` | 模拟期 B 直接读 A 的 Patch 值 |

实验证据：DSN-49/50（rate=300）`PatchMiss=0`、rescue-share≈77% —— 这个“同块多写”机制**确实在工作**。

### 2.2 问题：DAG 是“事后救援”，导致 churn

现在的实现是**被动/反应式**：

```
B 进 retry 池 → 正常模拟 → 撞上 A 的锁
  → RetryCommit 失败 → findCoveringSet 找 A 的 Patch（只覆盖已知 RWSet）
  → 消费 Patch + sendChainSignal → 重模拟 B
  → 基于 A 的新值执行，但控制流可能引入没被覆盖的新 key
  → 又冲突 → CallForRetry（SimulationNum+1）→ 又救……直到超时
```

- `chainLengthDist=66` 不是“DAG 深 66”，而是**同一笔被重模拟 66 次**（`SimulationNum` 每次 +1）。
- `findCoveringSet` 只覆盖“上一次模拟看到的 RWSet”，控制流变化产生的新 key 覆盖不全 → 反复 churn。
- DSN-50 的 `isFullyCovered`/`MaxChainDepth` 闸门因此“冗余空转”：`findCoveringSet` 本身已保证覆盖传入 key，`MaxChainDepth` 又管不住 `SimulationNum`。

### 2.3 结论

**要重写的是“DAG 的构造时机和确定性”，不是 DAG 本身。** 把“冲突后才找 Patch 救援”改成“在模拟/重试期就把依赖边确定性地建好，同块内按 DAG 串行，直接读上游 Patch，不再需要救援重试循环”。

## 3. 链上 / 链下分界（本方案的地基）

判断标准不是“数据在不在内存”，而是**“写入/消费发生在哪个执行阶段、是否所有 validator 共识一致”**。

### 3.1 链上（on-chain）——保留，几乎不动

| 对象/路径 | 位置 | 说明 |
|---|---|---|
| `onChainPatches`（`api.ChainNode`） | `retry_scheduler.go` 字段 | 验证事实存储 |
| `AddOnChainPatch` | `verify.go:610`（验证通过后） | 验证通过才落成可依赖事实 |
| `ReadOnChainPatch` | `verify.go:424` | 链式 tx 校验上游值 |
| `RemoveOnChainPatch` | `committer.go:147-148`（CR 后） | 链上清理 |
| `VerifySimulation` 的 `stateDB.CheckLock` | `verify.go` + `core/vm/ssc_contracts_write.go:55` | 真正的链上锁检查 |
| `isChainTx` 判定 | `verify.go:338` | 同块多写的合法性来源 |
| `CommitSimulation` / `CommitOrRollbackWithProof` | `core/vm/ssc_contracts_write.go:55/68` | 链上提交/回滚 |

> `onChainPatches` 物理上仍是内存 map，但语义是“链上事实”：写入点在 EVM 验证路径，所有 validator 一致，且验证通过才可被依赖（防 BUG-11 脏 Patch）。

### 3.2 链下（off-chain）——重写主体

| 对象/路径 | 位置 | 说明 |
|---|---|---|
| `offChainDAG`（nodes/keyIndex/subscriber/txSubKeys） | `patchpool.go` | 链下 DAG 存储，不落盘 |
| `AddNode`/`MarkReady`/`Remove`/`findCoveringSet`/`scanPatchSubscribers`/`tryConsumePatch`/`releasePatch`/`finalizePatch`/`isPatchFinalized` | `patchpool.go` | 链下调度/救援 |
| `readPatchChain`/`GetChainPatchRef`/`GetUpstreamTxRef` | `retry_scheduler.go:1565-1638` | 链下读上游（模拟期） |
| `retryScheduler`（retryPool/passivePool/consumedPatches/signals/lockWait） | `retry_scheduler.go` | leader 调度状态 |
| `RetryCommit`（Phase1b/2b DAG 救援） | `retry_scheduler.go:1249` | leader 侧调度 |
| `sendChainSignal`/`HandleRetrySignal`/`SignalReSimulation` | `retry_scheduler.go` + `comm.go` | 链下跨分片调度信号 |
| `tempLockView`（`canWound` 用 `isPatchFinalized`） | `temp_lock_view.go:189` | 链下临时锁视图 |
| `Simulator`/`StartReSimulation` | `simulator.go`/`simulator_leader.go` | 链下预执行 |
| `SSCInternalPool`（DSN-48） | `internal_pool.go` | **链下出块层**：只存“已打包成 SimTx/CRTx”的内部交易，供 worker `Extract` 上链；**不是建图层** |

### 3.3 DAG 的两类参与者 / 两个阶段（关键澄清）

`internal_pool.go` 只维护**已打包成 SimTx/CRTx 的内部交易**（写入点仅在 `tx_submitter.go:60` 的 `SubmitSimulationTx/SubmitCommitOrRollbackTx`），它是**出块层**，不包含“还未打包成 SimTx”的交易。

而 DAG 建图必须覆盖**未打包成 SimTx 的交易**——它们在模拟/重试期就已经在消费上游 Patch。`offChainDAG` 本来就横跨这两个阶段：

| 阶段 | 参与者 | 何时进入 DAG | 代码 |
|---|---|---|---|
| **模拟/重试期（未打包）** | 下游消费者：原始跨分片交易（retryTx） | `AddToRetry` 时 `subscribeRetryTx`（订阅它要读/写的 key） | `retry_scheduler.go:521/747/1187` |
| **提交期（已打包 SimTx）** | 上游提供者：已 `commitSimulation` 的 SimTx | `commitSimulation` 时 `AddNode` + `MarkReady`（暴露其 WriteSet 作为 Patch） | `impl.go:1012/1046` |
| **出块期（已打包 SimTx/CRTx）** | 出块池 | `SubmitSimulationTx/SubmitCommitOrRollbackTx` 入 `internal_pool` | `tx_submitter.go:60` |

- **模拟期读上游**：未打包的交易在 `simulator.go` 的 `GetState/SetState` 通过 `readPatchChain`/`GetChainPatchRef` 读上游 Patch 值（`simulator.go:878/928/1048`）。
- 因此**建图层 = `offChainDAG`/`retryScheduler`（横跨前两阶段）**；`internal_pool.go` 只是出块消费层，**不是建图的地方**。

### 3.3 链下 → 链上的唯一跨界桥

`CXTSimulation`（含 **`UpstreamTxList` + `ChainPatch`**）是唯一从链下传到链上的载体：

```
链下 leader：
  offChainDAG 建边 → commitSimulation 构建 simulation（UpstreamTxList + ChainPatch）
  → txSubmitter.SubmitSimulationTx → 内部池 → 广播/上链

链上 validator：
  VerifySimulation 读 simulation.UpstreamTxList
  → isChainTx = len>0 → 跳过 CheckLock（同块多写合法性）
  → ReadOnChainPatch 查上游期望值（读 onChainPatches）
  → 验证通过 → AddOnChainPatch（落链上事实）
```

两侧有**一一对应的两套读**：
- 链下读：`readPatchChain`（读 `offChainDAG`）
- 链上读：`ReadOnChainPatch`（读 `onChainPatches`）

**重写最不能错的一条线**：链下建的 `UpstreamTxList` 必须与链上最终验证顺序一致（validator 复算同一 DAG 序），否则分叉。

## 4. 设计方案

### 4.1 核心变化：从“事后救援”到“事前确定性建图”

| 现状（反应式，要改） | 新方案（主动/确定性） |
|---|---|
| B 先模拟 → 撞锁 → 再找 Patch 救援 | 模拟/重试期即建依赖边（A 已提交/已规划 → B 依赖 A），B 的模拟一开始就基于 A 的 Patch 值 |
| 依赖边靠 `findCoveringSet`（只覆盖已知 RWSet） | 依赖边在建 DAG 时显式、确定、完整（按确定性序排同 key 写者链） |
| 冲突 → `CallForRetry` → `SimulationNum+1` 重试 | 同块内按 DAG 序直接串行执行，不需要重试；`SimulationNum` 不再是“重试次数” |
| `scanPatchSubscribers` 被动等 Patch 出现再救 | 建图时一次性把下游就绪并分组，出块直接拿有序批次 |
| 覆盖不全 → 退回等锁 → 下一块 | 链在建图时完整构造，不再有“覆盖不全”半吊子状态 |

### 4.2 链下建图：在 `offChainDAG`/`retryScheduler` 层覆盖“未打包交易”，`internal_pool.go` 只做出块消费

**建图不在 `internal_pool.go`**——它只放已打包的 SimTx/CRTx。建图必须落在 `offChainDAG`/`retryScheduler` 这一层，因为它同时容纳“未打包的下游（retryTx，`subscribeRetryTx`）”和“已提交的上游 SimTx（`AddNode`）”。

把“反应式 findCoveringSet 救援”改为“主动确定性建图”：

```
on 交易进入模拟/重试（未打包成 SimTx）:
  1. 提取 rwset（从模拟 callStates / payload）
  2. 对照“本区块/本 DAG 已规划的写者”检查同 key 冲突：
       无冲突 → 作为根节点，正常模拟
       冲突   → 建立依赖边：新 tx 依赖该 key 的最后一个写者（UpstreamTxList）
                加入 dagWaiting，等上游提交后基于上游 Patch 值模拟
  3. 更新 keyIndex（谁写了这个 key）→ 成为后续交易的上游候选

on 上游 SimTx commitSimulation（已打包）:
  4. AddNode + MarkReady：暴露其 WriteSet 作为 Patch，
     让 dagWaiting 中依赖它的下游按确定性序就绪
```

- **同块多写能力保留**：依赖边让同 key 写者按确定性序串行，验证时 `isChainTx` 跳锁检查。
- **顺序权威 = 区块数组顺序**（leader 定、validator 跟），DAG 内先后只是内存实现细节，不序列化、不落盘（与 DSN-46 一致）。
- **出块**：已打包的 SimTx 进 `internal_pool.go`，`ExtractSSCTransactions` 返回“已按 DAG 排序、可并行/链式有序”的批次，worker 消费；配合 DSN-45 并行 execVerify。

### 4.3 链下救援逻辑退役（但保留能力）

- `findCoveringSet` / `scanPatchSubscribers` / `tryConsumePatch` / `releasePatch` / `subscriber` / `txSubKeys` 这套“事后救援”**退役**，其职责由“offChainDAG/retryScheduler 层的主动建图”取代。
- `RetryCommit` Phase1b/2b 不再做 DAG 救援；冲突交易统一走“模拟期建图（依赖上游）+ 等锁释放重评”。
- `offChainDAG` 的调度匹配（`subscriber`/`consume`）退役，**保留**上游读取（`readPatchChain`/`GetChainPatchRef`/`GetUpstreamTxRef`）供模拟期读值。

### 4.4 链上验证：保留不变

- `isChainTx = len(UpstreamTxList)>0` → 跳 `CheckLock`（同块多写合法性）。
- `ReadOnChainPatch` 查上游期望值。
- `AddOnChainPatch` 验证通过后落事实。
- 这一整套“允许同块多写”的合法性机制**本来就在链上且是对的**，原样保留。

### 4.5 需要解耦的耦合点

| 耦合 | 位置 | 新处理 |
|---|---|---|
| `canWound` 依赖 `isPatchFinalized` | `temp_lock_view.go:189` | 改为基于确定性 DAG 顺序/优先级判定，不再和“救援状态”绑定 |
| `MaxChainDepth` 只管 DAG 深度、管不住重试 | `patchpool.go`/`retry_scheduler.go` | 改为管“同 key 写者链长”（建图时保证链长有界） |

## 5. 变更文件清单（按链上/链下分）

| 文件 | 侧 | 改动 |
|:-----|:---|:-----|
| `ssc/patchpool.go` | 链下 | **建图层**：把 `findCoveringSet`/`scanPatchSubscribers` 反应式救援改为主动建图（`dagWaiting` + 确定性依赖边）；保留只读上游 |
| `ssc/patchpool.go` | 链下 | 退役 `findCoveringSet`/`scanPatchSubscribers`/consume 调度；保留只读上游 |
| `ssc/retry_scheduler.go` | 链下 | 建图协调：`RetryCommit` 去掉 DAG 救援；`subscribeRetryTx`/`dagWaiting` 就绪；保留 `readPatchChain`/`GetUpstreamTxRef` |
| `ssc/internal_pool.go` | 链下出块层 | **不是建图层**；仅存已打包 SimTx/CRTx，`Extract` 返回有序批次 |
| `ssc/impl.go` | 链下 | `commitSimulation` 用建图后的 `UpstreamTxList`；`closeTransaction` 清理收敛到建图池 |
| `ssc/simulator.go` | 链下 | 模拟期读上游仍走 `readPatchChain`（保留） |
| `ssc/temp_lock_view.go` | 链下 | 解耦 `canWound` 与 `isPatchFinalized` |
| `ssc/api/types.go` | 公共 | `MaxChainDepth` 语义改为“同 key 写者链长上限”；`EnableInternalPool` 相关 |
| `node/worker/worker.go` | 链下消费 | 消费新 `ExtractSSCTransactions` 返回的有序批次（接口不变） |
| `ssc/verify.go` | **链上（不动）** | `isChainTx`/`ReadOnChainPatch`/`AddOnChainPatch` 原样保留 |
| `ssc/committer.go` | **链上（不动）** | `RemoveOnChainPatch` 原样保留 |
| `ssc/api/monitor.go` | 公共 | 监控字段保留 + 新增建图/链长指标 |

## 6. 验证计划

1. 默认开关：保留开关，默认关 → 无回归。
2. A/B 实验（rate=300，复用 `scripts/local-ssc-retry-stats.py`）：
   - `retryAdd` / `sp1Started` / `poolStarted` / `chainLengthDist` 应大幅下降（重试消失）；
   - `PatchMiss` 应保持 0，`rescue-share` 语义从“救援”转为“同块多写命中”；
   - `PoolTimeout` 下降、成功率回升；
   - 同 key 写者链长 ≤ 上限（新 `MaxChainDepth` 语义）。
3. validator 与 leader 结果一致、无分叉（守住 3.3 的跨界桥）。

## 7. 风险与开放问题

| 问题 | 说明 | 处理 |
|---|---|---|
| 控制流新 key | B 读 A 值后走不同分支、碰到建图时未纳入的新 key | 建图时纳入完整 RWSet（进池前拿到），或允许同块内对新 key 也建依赖 |
| 热 key 写者链长 | 多 tx 写同一 `K` 排成链，链长=同块写次数 | 新 `MaxChainDepth` 管链长，超限则拆分到下一块 |
| 建图与链上顺序一致 | 链下 `UpstreamTxList` 必须与 validator 复算顺序一致 | 顺序权威=区块数组顺序，leader 定、validator 跟 |
| 链下/链上读同步 | `readPatchChain`（链下）与 `ReadOnChainPatch`（链上）语义必须一致 | 保留一一对应关系；链上验证通过后才 `AddOnChainPatch` |
