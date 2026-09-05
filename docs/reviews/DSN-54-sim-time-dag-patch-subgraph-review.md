---
id: REV-DSN-54
title: 评审建议：模拟期 DAG Patch 子图（成员侧反向遍历 + round 生命周期）
type: REV
status: done
author: Evaluator
reviewed: [DSN-54]
verdict: needs-changes
created: 2026-09-05
refs: [DSN-54, DSN-46, DSN-49, DSN-50]
---

# REV-DSN-54 — DSN-54 评审建议（供负责人修订）

> **产出角色**：Evaluator
> **消费角色**：DSN-54 负责人（设计修订）、Implementer
> **本文档目标**：不重写 DSN-54，只给出「问题是否说清、方案能否解决、以及修订必须/建议怎么改」的可执行清单，便于负责人逐条在 DSN-54 上落实。

---

## 1. 评审范围与结论速览

- 对照 DSN-54 全文，并结合代码核验：
  `ssc/retry_scheduler.go`、`ssc/simulator.go`、`ssc/simulator_leader.go`、
  `ssc/simulator_member.go`、`ssc/impl.go`、`ssc/api/types.go`，以及 DSN-46/49/50。
- **问题是否说清**：✅ 基本合格。根因、代码证据、时序论证自洽且经得起对照。
- **方案能否解决**：⚠️ 方向能解决核心信息缺口，但实现路线偏重；存在更简、无竞态、与现有模式一致的替代方案，**必须先对比评估**。
- **结论**：`needs-changes`。

---

## 2. 先说结论性判断

DSN-54 的核心诊断是正确的，且我在代码里验证到了每一个关键环节：

1. `RetryCommit` 仅 leader 执行（`if !rs.state.IsLeader(...)`，`retry_scheduler.go`）；
2. DAG 救援命中后 `rs.state.SetChainPatch(txHash, merged)` 只写**本进程/本节点**的
   `Simulator.SimulationState.ChainPatch`（`retry_scheduler.go:1690/1814`、`impl.go:300`、`simulator.go:163`），**无任何广播**；
3. `offChainDAG.AddNode` 只在 `impl.go:1099` 与 `retry_scheduler.go:1937`（均 leader 路径）调用，`MarkReady` 带 `if s.IsLeader(...)` 守卫——成员侧 `offChainDAG` 基本为空；
4. `StartReSimulation` 构造的 `CXTSimulationRequest`（`simulator_leader.go:715-728`）**不带任何 patch**，而真正跑 EVM 的是各成员 `HandleSimulateRequest`；
5. 成员 `GetState→IsKeyAvailable` 三源皆空 → `ErrLockConflict_OnChain` → “state is locked …” → `SimTx` 建不出来 → Verify 接触不到（拒绝≈0），与 DSN-54 §2 数据一致。

**因此“成员读不到 leader 选中的上游 patch”这一根因判断成立。**

真正的问题在**解法选择**（见 §3 必选修改第 1 条）。

---

## 3. 必选修改（不处理不通过）

### M1. 先与“随 `CXTSimulationRequest` 携带上游 patch”路线对比后再定稿

**背景事实（决定性问题）**：**“消费 patch 的 leader”与“构造 req 并调用 `StartReSimulation` 的节点是同一个进程**——`RetryCommit` 成功 → `TriggerReSimulation` → `service.Simulator.StartReSimulation`（`impl.go:307`）。而 `StartReSimulation` 本来就会把 `req` 广播给全部 committee 成员去跑 `HandleSimulateRequest`。

因此最简、改动最小、与现有代码一致的修法是：**把被选上游的有效写集（或子图）直接加进 `CXTSimulationRequest`，随这次已有的模拟广播一次性到达成员**，而非 DSN-54 的：
- 新增 `Method_StoreSimPatchSubgraph` 广播 RPC；
- 成员侧维护 `simPatch sync.Map` + “返回即清”生命周期；
- 处理“子图先到 vs 模拟请求先到”的时序竞态（§6.3 / Q2）。

代码里已有完全相同先例：`RetrySignal` 带 `ChainPatch` 字段，`HandleRetrySignal` 收到后 `SetChainPatch(signal.TxHash, signal.ChainPatch)`（`retry_scheduler.go:1270`）——“上游值随触发消息携带”在这套系统里是已验证模式。

**要求**：在 DSN-54 增加一节“方案对比”，明确回答为何不采用“随 request 携带”（例如：req 体积过大、需要跨轮/跨成员预置、图数据无法一次性放入 req 等），并给出否决理由；若无充分理由，则把主线方案改为“随 request 携带 + 成员读取时即时消费”，可同时删掉独立 RPC、成员侧存储、D4 生命周期与 Q2 竞态。

### M2. 补齐跨分片（related shards）的分发设计

模拟与锁冲突跨越多个相关 shard，每个 shard 各有一段 `HandleSimulateRequest` 在各自 committee 成员上执行。

**要求**：明确
- 由哪个（origin 还是各 related shard 的）leader 打包子图；
- 如何把子图分发给**所有相关 shard 的 committee 成员**；
- 是否只发各 shard 需要的部分，而不是把整棵图全量发给每个分片。
DSN-54 §4.2/§4.3 目前只写“广播给将执行该轮模拟的 committee 成员”，未区分分片维度，实现时会在跨 shard 场景漏发/错发。

### M3. 明确残留 churn 口径，让 §7 验证可判成败

即使成员可见性修好，重执行仍可能因控制流变化读到“任何选中上游都没写过的新 key”（DSN-50 已承认的残留），那部分仍会 `ErrLockConflict` 继续 churn。

**要求**：在 §7 验证打点区分两类失败——
- `covered-but-invisible`（本次修复目标，应消失）；
- `genuinely-uncovered`（修复后仍残留，属预期剩余）。
否则 `state is locked ~21k → 大幅下降` 无法判定是“修复生效”还是“运气/数据漂移”，也无法给出残留预期。

---

## 4. 推荐修改（建议但非必须）

### R1. 用数据说明“为何必须整图反查，而非扁平有效写集”

对成员读取，多数情况只需“每个 key 的最终生效值”；leader 选中上游时已确定性定序。整图广播的代价是：每个成员收整棵图（大于最小有效值集）、反查逻辑要搬到成员侧、还要承担“同 key 多 writer / 深祖先”的正确性负担（Q3 未定）。

DSN-54 数据（`dagPerBlockMaxSameKey=7-11`、深度≤5）**支持**“同 key 多 writer 存在”，所以图序可能确实必要。**建议**：补充证据说明有多少读取会落到“超出直接上游 writes 的深祖先/非首 writer”；若占比很低，默认采用更省的“随请求携带 leader 已算好的有效写集（mergeRWSet）”，把图只作为 leader 调度内部表示。

### R2. 生命周期/清理正确性写细

`HandleSimulateRequest` 有多处提前 return（header 找不到、stateDB nil、签名失败、`SimulationNum==0` 分支等）。若只在 startSimulation 后 defer 清理，漏 return 会造成 `simPatch` 泄漏/陈旧。
**建议**：给出“每个 return 点都保证清理”的方案（例如包一层统一 defer，或在 request 携带时根本不落库），并审计所有仍读 `SimulationState.ChainPatch` 的路径（至少 `HandleRetrySignal`@1270、`readChainPatch`、`BuildCallStates`/Verify 相关），明确 D5 迁移期是否需要双写兜底。

### R3. 说明成员一致性/门限签名风险

所有成员必须反推出**完全相同**的值才能凑齐门限签名并通过 Verify。单 leader 广播本身确定性 OK，但 Q2 里“未命中则短等待/回拉”的分叉会引入不一致与额外延迟——这再次支持 M1 的“随请求携带”才是更稳的选择。

### R4. 术语/职责再切分

DSN-54 把“值的分发”（信息缺口，问题的本质）与“值的表示：子图 vs 扁平”（优化/一致性选择）捆在一起。**建议**：在 §1 明确——修复的本质是 M1 的“分发”，表示选择（子图反查）只是可选增强，两者解耦，便于负责人先落地最小修复再评估增强。

---

## 5. 给负责人的一句话修订口径

- **先做“随 `CXTSimulationRequest` 携带上游 patch”的对比设计**；能省掉整套 RPC/存储/生命周期/时序处理。
- 若仍坚持“独立子图广播 + 成员反查”，必须补齐：跨 shard 分发（M2）、Q2 时序（建议改为随请求携带）、成员一致性（R3）、多 return 清理（R2）、残留口径（M3）。
- 无论如何，**用数据说明“扁平有效写集不够、必须整图反查”**，避免过度设计。

---

## 6. 评分

| 维度 | 评分 | 说明 |
|:-----|:----:|:-----|
| 问题定义 | 9/10 | 根因、代码证据、时序分析到位且经得起对照 |
| 方案可行性 | 6/10 | 能解决信息缺口，但路线偏重、存在更简替代，跨 shard 未展开 |
| 设计对齐 | 7/10 | 复用 readPatchChainVisited 等成熟做法；但与 RetrySignal 携带模式不一致 |
| 可验证性 | 6/10 | 端到端指标清晰，但缺“covered vs uncovered”残留口径 |
| 实施风险 | 6/10 | 独立广播 + 成员存储 + 时序/清理引入较多新增面 |

## 结论

**needs-changes** —— 至少落实 M1/M2/M3 三项必选修改后，再进行下一轮评审。
