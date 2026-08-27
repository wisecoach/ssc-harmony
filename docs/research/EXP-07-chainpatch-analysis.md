---
id: EXP-07
title: ChainPatch 机制研究报告（工作原理 + 交互点 + 约束，避免其他模块设计冲突）
type: EXP
status: active
priority: P0
author: Designer
created: 2026-08-27
updated: 2026-08-27
scope: [ssc, retryScheduler, PatchPool]
refs: [DSN-22, DSN-23, DSN-26, BUG-11, BUG-13, DSN-45, DSN-46]
---

> **目的**：本文不是设计，而是对现有 ChainPatch 机制的**事实性梳理**。它回答「ChainPatch 是怎么工作的、涉及哪些点、有哪些不变量」，供 DSN-45（SimTx 批量并行验证）和 DSN-46（专用内部交易池 + DAG）在设计时**不与它冲突**。

---

## 1. 一句话

ChainPatch 是「已提交 SimTx 的 WriteSet（Patch）+ 依赖关系（DAG）」的记录，核心用途有二：
1. **让 retry 交易消费上游 Patch 来覆盖冲突 key**，从而能基于上游结果重新模拟；
2. **在链上验证时提供上游期望值**，并作为“链式交易”跳过锁检查的依据。

---

## 2. 它解决什么问题

当 SimTx 验证冲突/失败进入 retry 后，重新模拟时它的部分 key 可能已被**上游已提交 SimTx** 改过。若不处理，重试会再次撞上链上锁。ChainPatch 让重试交易**消费**这些上游的 WriteSet，得到“上游结果”作为重试的输入基础，从而：
- 避免 retry 与链上锁冲突；
- 通过 `UpstreamTxList` 表达“我依赖了哪些上游”（DAG）；
- 链上验证时用 `ReadOnChainPatch` 校验上游期望值，保证重试结果可复算。

---

## 3. 两层架构（最容易混的地方）

| 层 | 结构 | 存储位置 | 语义 | 用途 |
|---|---|---|---|---|
| **第 1 层：DAG 记录** | `ChainNode` | `patches`（leader 本地）/ `onChainPatches`（全节点） | 每个 SimTx 写了什么 + 依赖哪些上游 | 链式判定、上游期望值查询、SimTx 构造 `UpstreamTxList` |
| **第 2 层：消费池** | `ChainPatchNode` | `localPatches` + `keyIndex` | 上游 WriteSet 组成的“可被 retry 消费”的池 | retry 冲突覆盖（`findCoveringSet` / `TryConsume`） |

> ⚠️ 两者都叫“Patch”，但一个是 **DAG 记录（只读事实）**，一个是 **可消费的资源池（有状态：Free/Consumed/Finalized）**。任何模块都不应混淆这两层。

---

## 4. 数据结构

### 4.1 ChainNode（第 1 层，`ssc/api/types.go:1043`）
```go
type ChainNode struct {
    TxHash         common.Hash
    SimulationNum  int
    Patch          *RWSet     // 自己这轮 SimTx 的 WriteSet
    UpstreamTxList []TxSimKey // 所有上游（空=根节点；DAG 多上游）
}
type TxSimKey struct { TxHash common.Hash; SimulationNum int }
```

### 4.2 ChainPatchNode（第 2 层，`ssc/patchpool.go:30`）
```go
type ChainPatchNode struct {
    TxHash, SimulationNum
    Patch     *RWSet
    Consumer  common.Hash
    Priority  api.Priority
    status    atomic.Int32 // PatchFree → PatchConsumed → PatchFinalized
}
```

### 4.3 retryScheduler 上的相关字段（`ssc/retry_scheduler.go:299-342`）
```go
patches        sync.Map // key: txHash → patchMap（第 1 层，leader 本地）
onChainPatches sync.Map // key: txHash → patchMap（第 1 层，全节点共享）
localPatches   sync.Map // key: txHash → *ChainPatchNode（第 2 层消费池）
keyIndex       sync.Map // LockKey → {txHash set}（第 2 层反向索引）
subscriber     sync.Map // LockKey → {retryTxHash set}（retry 依赖索引）
txSubKeys      sync.Map // retryTxHash → []LockKey
consumedPatches sync.Map // retryTxHash → []common.Hash（消费的上游列表）
```

---

## 5. 生命周期 / 数据流（含调用点）

```
1) SimTx 提交（leader, impl.go CommitSimulation）
   · 从 callStates 提取 WriteSet → simulation.ChainPatch = writeSet   [impl.go:892]
   · GetUpstreamTxRef(txHash) → upstreamTxList                        [impl.go:877]
   · finalizePatch(txHash)   —— 锁死本 SimTx 的 Patch，不可再被 Wound  [impl.go:903]
   · AddPatch(txHash, simNum, ChainPatch, upstreamTxList)             [impl.go:910]
   · OnPatchPoolUpdated(writeSet) —— 扫描等待该 key 的 retryTx        [impl.go:945]

2) 全节点收到 SimTx 多播（verify.go VerifySimulation 验证通过后）
   · AddOnChainPatch(...) —— 写入 onChainPatches                     [verify.go ~590]
   · （BUG-11：之前还错误地在 impl.go:879 提交前、verify.go:324 锁检查阶段提前调用）

3) 某交易验证冲突/失败 → 进 retry（retry_scheduler.go RetryCommit）
   · 找冲突 key → findCoveringSet(conflictKeys)  [retry_scheduler.go:1286, 1388]
   · tryConsumePatch 逐个消费 Free Patch → 全成功则
     SetChainPatch(txHash, merged) + consumedPatches.Store(...)      [retry_scheduler.go:1305-1306]

4) 重新模拟 → SimTx 带 UpstreamTxList
   · Simulator.SetChainPatch / Simulator 在 GetState/SetState 查 Patch 链（impl.go:200）

5) 链上 VerifySimulation
   · isChainTx = simulation.ChainPatch != nil → 跳过锁冲突检查        [verify.go:318-324]
   · 读 key 期望值：ReadOnChainPatch(upstream, addr, key) 递归查上游  [verify.go:403]

6) 完成清理（committer.go CommitOrRollbackWithProof / impl.go closeTransaction）
   · RemoveOnChainPatch(txHash) —— CR 完成后删 onChainPatches        [committer.go:145-146]
   · consumedPatches 释放残留 Consumed Patch                          [impl.go:1246-1252]
```

---

## 6. 关键算法

### 6.1 findCoveringSet（`ssc/patchpool.go:328`）
- 输入：conflictKeys
- 用 `keyIndex` 找每个 key 的 Free Patch 候选
- **贪心集合覆盖**：每轮选覆盖最多“未覆盖 key”的 Free Patch，直到覆盖全部或无法继续
- 复杂度：O(K²·N)（K=冲突 key 数，N=每 key owner 数），常数级
- 返回 nil = 无法覆盖 → 走 LockWait

### 6.2 TryConsume / Release / Finalize（`patchpool.go:127,151,157`）
- `TryAcquire`：`CompareAndSwap(Free→Consumed)` 原子抢占
- `releasePatch`：失败时 Consumed→Free，归还给别的 retryTx
- `finalizePatch`：提交 SimTx 前 Consumed→Finalized，不可再被 Wound

### 6.3 ReadOnChainPatch（`retry_scheduler.go:1658`）
- 先查自身 node.Patch 是否含 `addr:key`；没有再**遍历所有上游递归**查
- 返回 (value, found)；查不到返回 false（兼容“部分覆盖”）
- ⚠️ 递归深度 = DAG 深度，多上游/深链时是查询热点

### 6.4 OnPatchPoolUpdated → scanPatchSubscribers（`patchpool.go:401`）
- 新增 Patch 后，扫 `subscriber` 中等待该 key 的 retryTx
- 按 nonce 优先级 reservation，避免多 retryTx 争锁

---

## 7. 与外部模块的交互点（设计时不要破坏）

| 外部模块 | 交互点 | 现有行为 / 约束 |
|---|---|---|
| **Simulator** | `SetChainPatch` / `GetState`/`SetState` 查 Patch 链（`impl.go:200-201`） | 模拟时若 key 来自上游 Patch，直接从 Patch 取值，不走 stateDB |
| **VerifySimulation** | `isChainTx`（`ChainPatch != nil`）→ 跳过锁检查；`ReadOnChainPatch` 校验上游 | 当前**每个 SimTx 都有 ChainPatch** → 100% 被判 chain tx（BUG-13 §2.4 隐患） |
| **Committer / closeTransaction** | `RemoveOnChainPatch`、释放 `consumedPatches` | CR 完成后必须清 onChainPatches，否则泄漏 |
| **retryScheduler（retry）** | `findCoveringSet` / `TryConsume` / `SetChainPatch` / `OnPatchPoolUpdated` | Patch 有 Free/Consumed/Finalized 状态机 |
| **ChainNextSim / subscriber** | `subscriber`/`keyIndex` 索引 | 出块时按 reservation 选 retryTx |

---

## 8. 不变量（其他模块不得破坏）

1. **Patch 状态机**：Free → Consumed → Finalized，单向（Finalized 不可回退、不可被 Wound）。
2. **onChainPatches 只存“验证成功的 SimTx 的自身 WriteSet”**：每个 SimTx 独立、有序；`ReadOnChainPatch` 依赖这个“独立性”逐上游查询（BUG-11 的 Merge 破坏了它）。
3. **AddOnChainPatch 只能在“验证成功”后调用**：提前写入会让失败 SimTx 的脏 Patch 污染下游（BUG-11 根因）。
4. **consumedPatches 与 Patch 一一对应**：消费失败/交易关闭时必须 release/清理，否则 Consumed 残留泄漏。
5. **ChainPatch 的“跳过锁检查”语义 = 有上游依赖**：不应把“无上游的普通 SimTx”也标成 chain tx（当前实现过宽，是待修点）。
6. **跨节点一致**：leader 与 validator 对同一 SimTx 的 ChainPatch / onChainPatches 必须一致（否则复算结果不同）。

---

## 9. 已知坑（新设计前应正视）

| # | 问题 | 位置 | 影响 |
|---|---|---|---|
| 1 | **chain tx 判定过宽**：每个 SimTx 都填 `ChainPatch=writeSet` → 100% 跳过锁检查 | impl.go:892 + verify.go:318 | 锁检查形同虚设；进池仲裁分组的意义被架空 |
| 2 | **AddOnChainPatch 时序错误**（提交前/锁检查阶段提前写） | impl.go:879 / verify.go:324 | 失败 SimTx 脏 Patch 污染下游（BUG-11） |
| 3 | **Phase 1b Merge 破坏独立性** | retry_scheduler.go:1260-1264 | onChainPatches 丢失每笔独立 Patch 边界（BUG-11） |
| 4 | **ReadOnChainPatch 递归** | retry_scheduler.go:1658 | 深链/多上游查询热点、可能读到脏 Patch |
| 5 | **两套 Patch 语义并存**（DAG 记录 vs 消费池） | 全局 | 设计/实现易混淆 |

---

## 10. 对其他模块设计的指导（DSN-45 / DSN-46 必须满足）

1. **进池仲裁/冲突分组（DSN-46）**：分组依据是 `RWSet`；这与 PatchPool 的 `keyIndex` 同源。**不要另起一套 key 索引**，应复用/明确边界，否则两套仲裁打架。
2. **DAG 就绪（DSN-46）**：就绪条件 = 所有 `UpstreamTxList` 已 finalize/committed。**这是用显式 DAG 替代 nonce 顺序**，与 ChainPatch 的 `ChainNode.UpstreamTxList` 天然一致，可直接复用。
3. **并行 execVerify（DSN-45）**：只在 `stateDB.Copy()` 上跑，不改 ChainPatch/onChainPatches；写路径按序。ChainPatch 的 AddOnChainPatch 仍只在验证成功后调用。
4. **前置修复**：在做内部池之前，建议先修 §9 的 #1（chain tx 判定过宽）和 #2/#3（BUG-11），否则内部池会继承“所有 SimTx 都跳过锁检查 / 脏 Patch”的问题。
5. **不变量继承**：内部池/批量并行不得破坏 §8 的 6 条不变量。

---

## 11. 调用点清单（速查）

| 文件 | 行 | 函数/调用 |
|---|---|---|
| impl.go | 877 | GetUpstreamTxRef → upstreamTxList |
| impl.go | 892 | simulation.ChainPatch = writeSet |
| impl.go | 903 | finalizePatch |
| impl.go | 910 | AddPatch |
| impl.go | 945 | OnPatchPoolUpdated(writeSet) |
| impl.go | 1246-1252 | closeTransaction 释放 consumedPatches |
| verify.go | 318-324 | isChainTx 判定（跳过锁检查） |
| verify.go | ~590 | AddOnChainPatch（验证成功后） |
| verify.go | 403 | ReadOnChainPatch 上游校验 |
| committer.go | 145-146 | RemoveOnChainPatch（CR 完成后） |
| retry_scheduler.go | 1286/1388 | findCoveringSet |
| retry_scheduler.go | 1305-1306 | SetChainPatch + consumedPatches.Store |
| patchpool.go | 50 | TryAcquire（Free→Consumed） |
| patchpool.go | 157 | finalizePatch（Consumed→Finalized） |
| patchpool.go | 401 | scanPatchSubscribers |
