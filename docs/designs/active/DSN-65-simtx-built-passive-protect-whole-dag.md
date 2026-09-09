---
id: DSN-65
title: SimTx 构建即被动链保护 + DAG 整单一致足迹（producer 自保护 / RetrySignal 整单标志 / 失败即移出）
type: DSN
status: design + 代码可行性已核对（待评审/实现）
priority: P0
author: Designer
created: 2026-09-08
updated: 2026-09-08
scope: [ssc/impl.go, ssc/retry_scheduler.go, ssc/patchpool.go, ssc/temp_lock_view.go, ssc/api]
refs: [DSN-55, DSN-56, DSN-57, DSN-58, DSN-60, DSN-61, DSN-62, DSN-64, HANDOFF-20260908-deadlock-rootcause-hotkey-reservation-holdwait]
---

# DSN-65: SimTx 构建即被动链保护 + DAG 整单一致足迹

> 状态：**design（草案，待评审）**。逃生（整单 abort / CMH 扩展）本轮明确**不做、不展开**。
>
> **一句话**：一笔交易**一旦模拟成功、构建出 SimTx 并成为 DAG producer，就不应再可被 Wound**（逻辑上它已
> “commit 到这一步”）。用**被动链保护 `markChainProtected`**（不是 `finalize`，避免 DSN-62(A) 的链式救援
> 回归）把保护时刻**提前到 producer 自身构建 SimTx 时**，配合“失败/wound 即移出 DAG pool + 解绑下游”
> 与“RetrySignal 携带权威整单 DAG 标志、origin 扇出前统一 RetryCommitDAG”，让 DAG 交易的上游链/全腿
> 足迹一致、不再被撕裂，且**不引入超时**。

---

## 0. 背景与要解决的问题

上一 session（HANDOFF-20260908…）定位到：长窗口 4711 的卡死主体是热 key 上的
**固定优先级 reservation hold-and-wait**（非 DAG，88%）+ 少量 DAG-stuck/ready-quorum（12%）。
本 DSN **不解决热 key hold-and-wait 的主体**（那需整单逃生，另行设计），只收敛**DAG 足迹一致性**这一截，
目标是让“被 DAG 保护/依赖”的交易不再因分片间足迹不一致而被撕裂或白等。

DAG 侧遗留的三个不一致（本 DSN 一并修）：

1. **producer（上游 U）构建完 SimTx 仍可被 Wound**：`CommitSimulation` 里
   `finalizePatch`（仅 Consumed 生效）→ `AddNode`（又置回 `PatchFree`）→ 节点实际可被 wound
   （DSN-62 根因：U 形成 SimTx 后被第三方 wound → 无法先于 D 上链 → D 被扣留/回滚）。
2. **DAG 保护是分片局部的**：`markChainProtected/protectHeldLocks/markChainTx` 都只在
   “本分片消费了上游 / 走了 RetryCommitDAG”时置位；同一交易的**其它 non-DAG 腿不保护** → 被另一 DAG
   交易在非 DAG 腿上 wound → 撕裂（partial winner）。保护足迹不是整单一致。
3. **“是否当 DAG 交易重试”的判据不可靠**：origin 靠本地 `isChainTxMarked` + DSN-57 “上游是否本地”
   过滤，可能某腿用了 DAG 而 origin 不判 chain → 其它腿不统一走 `RetryCommitDAG`。

## 1. 设计原则（三条，都不引入超时）
- **P1 构建即保护**：交易一旦模拟成功、构建 SimTx 并入 DAG（成为可被下游消费的 producer），
  其 TLV 写锁从那一刻起不可被 Wound（直到该 SimTx 失败/放弃/终局）。
- **P2 足迹整单一致**：“是否 DAG / 是否不可被 wound”是**交易整单/整次尝试的属性**，由 origin 在
  RetryCommit 扇出前确定，对所有 needing 分片统一执行；不依赖“某分片是否恰好消费了上游”。
- **P3 失败即失效**：任何被 wound 或 verify 失败的上游/leg，立即移出 DAG pool 并解绑依赖它的下游，
  使 D 不会链在一个注定上不了链的 U 上；与 P1 对称，不留“不可 wound 的僵尸”。

---

## 2. 设计

### 2.1 (P1) SimTx 构建即被动链保护（producer 自保护）
- **位置**：`ssc/impl.go` `CommitSimulation` —— `offChainDAG.AddNode(...)` 之后、
  `SubmitSimulationTx` 之前，对 producer 调 `offChainDAG.markChainProtected(txHash)`。
- **机制**：用 `markChainProtected`（被动链保护）而非 `finalizePatch`：
  - `canWound(holder)` 对 `chainProtected` 返回 false → **不可被 wound**（temp_lock_view.go:207 已实现）；
  - 节点仍保持 Free/Consumed **可被下游消费**（`finalize` 会禁用消费，导致 DSN-62(A) 回归，禁用）。
- **效果**：U 一旦构建 SimTx 就不可被第三方 wound，稳定去上链 → DSN-62 那类
  “已构建却被 wound、上不了链”从根上消失。

### 2.2 (P1 延伸) 上游/祖先链被动保护（保护 U 及其祖先）
- 复用 `protectUpstreamHeldLocksBFS`（DSN-62 C/D）：沿 `UpstreamTxList` 递归对祖先
  `markChainProtected`，visited 防环、`depth ≤ maxChainDepth` 封顶、只作用于“实际被消费的链式子图”。
- **跨分片 U 的边界（本 DSN 不做原子保护）**：不能跨分片原子地保护在途 CXT 的上游 U。
  见 §2.4——靠“只消费已注定 commit 的 U”门槛 + §2.3 失败即移出来保证 D 不白等，而不是硬保护 U。

### 2.3 (P3) 被 wound / verify 失败 → 立即移出 DAG pool + 解绑下游
- 已有：`notifyWoundedUpstream(U)`（DSN-62 B′）：被 wound 一律 `offChainDAG.Remove(U)`，
  若 U 已 Consumed 先把它从下游 D 的 `consumedPatches` 解绑 → D 退回正常重试。
- 本 DSN 补强/对称：
  - verify 失败 / SimTx 未上链的 producer 也走同一“移除 + unmarkChainProtected + 解绑下游”路径
    （把 `notifyWoundedUpstream` 推广为通用 `invalidateUpstream`，覆盖 wound 与 verify-fail 两类）；
  - `releasePatch` / `RetryCancel` / 整单 rollback 均保持对称撤销 `unmarkChainProtected`（已实现）。
- 效果：与 P1 对称——**保护只在“确实会去上链”的窗口有效**，失败立即失效，不留僵尸。

### 2.4 (P2) 上游合格门槛：D 只依赖“已注定 commit”的 U（跨分片 U 的解法）
不追求“跨分片原子保护 U”（难以做到，且回到原点），改为**本分片可判定的上游合格门槛**，
D 只消费下列三类 U，否则不消费（D 退回普通重试，等 U 变成其中一类）：
1. **U 已链上 commit**（DSN-64 `committedOnChain`）→ D 读落盘终值，U 不可能再失败；
2. **U 的 DAG 写集全在 D 消费的那个分片内**，且该分片已把 U 节点 `markChainProtected`（本分片内原子）；
3. **U 是 CXT 但所有 related 腿已放好/voted**（只能 commit，不会被 wound 掉）。
这样“D 锁上 ⇒ 其上游已注定 commit ⇒ D 必然成功”，跨分片原子性问题被绕开，且不废掉同分片 DAG 的主收益。

### 2.5 (P2) RetrySignal 携带权威整单 DAG 标志 → origin 扇出前统一 RetryCommitDAG
- 现状：某腿 `sendChainSignal`（携带 `UpstreamTxList`）→ origin `HandleRetrySignal` 里
  **用 DSN-57 “上游是否 origin 本地”过滤后才 `markChainTx`** → `tryToReSimulation` 读 `isChainTxMarked`
  决定 `RetryCommitDAG`。本地过滤是足迹不一致的源头。
- 改法：**不改 proto**，直接复用已在跨分片传输的非空 `UpstreamTxList` 作为权威信号（语义＝“本次该腿
  确实消费了上游 → 整单按 DAG 处理”），**去掉“是否 origin 本地”过滤**；origin 按
  `(txHash, attempt/simulationNum)` 持久化该整单标志。
- `tryToReSimulation` 以该标志为准，对所有 needing 分片**统一 `RetryCommitDAG`**（已 vote/上链的腿
  不重复锁，needingShards 处理）；若任一腿 `Locked:false` → 按现有失败路径对每条 Locked 腿
  `RetryCancel`（→ `revokeDAGProtectionOnRollback` 全量撤保护）——整单回滚撤保护基建已存在。

---

## 3. 改动清单（已按代码可行性核对收敛）
- `ssc/impl.go` `CommitSimulation`：`offChainDAG.AddNode(...)` 之后、`SubmitSimulationTx` 之前，对
  producer 调 `offChainDAG.markProducerProtected(txHash)`（P1，**独立于** DSN-62 的 chainProtected——
  见 §7.1 符合性修复）。**scope 按设计定稿：所有构建 SimTx 的 producer 一律保护**（“构建即不可 wound”
  字面语义），over-protection 风险见 §5。
- `ssc/retry_scheduler.go`：
  - `HandleRetrySignal`（~1491-1511）：去掉 DSN-57 “上游是否 origin 本地”的过滤对 `markChainTx` 的门控——
    收到携带 upstream 的 chain signal 即 `markChainTx`（整单 DAG 语义），与“AddNode 只落本地上游”的结构逻辑解耦；
  - `tryToReSimulation`：以“RetrySignal 携带 upstream”为权威整单标志，对所有 needing 分片统一
    `RetryCommitDAG`；
  - 把 `notifyWoundedUpstream` 推广为覆盖 wound + **verify-fail** 的 `invalidateUpstream`（P3，现有只 cover wound）；
  - 维持 `revokeDAGProtectionOnRollback` 全量对称撤销（含 producer 自保护）。
- `ssc/patchpool.go`：节点失效/移除统一保证 `unmarkChainProtected`（P3 对称）；`AddNode` 不重置
  `chainProtected`（现状已是独立 atomic.Bool，仅核对、无需改）。
- `ssc/api`：**不改 proto**。RetrySignal 复用已跨分片传输的 `UpstreamTxList`（Go struct types.go:1029 +
  proto field 8 均有）作为权威信号，不新增 `UseDAG` 字段。
- `ssc/temp_lock_view.go`：`canWound` 对 `chainProtected` 返回 false（已实现，无需改，仅核对）。

## 4. 验收口径
同配置 rate=200/shard=4/delay=10/vpn=4 clean 长窗口：
- **DAG-stuck（§3.6 型“腿都 vote 却差一条腿”）下降/归零**：不再出现“producer 已构建 SimTx 却被 wound
  掉导致腿凑不齐”；
- `upstream-not-onchain`（DSN-57 确定性回滚）在 DSN-64 基础上继续下降；
- `unfinished` 的 DAG 标记部分占比不上升；DAG 标记交易的 finish 比例（≈72% 基线）不劣化；
- 无“已构建 SimTx 却仍被 wound”的日志（wound 事件里 holder 不应是 in-DAG 的 producer）；
- ring_detected 保持 0；rollback 不因本 DSN 明显上升（逃生不在本 DSN 内）。

## 5. 风险与克制
- **不得用 finalize 实现 P1**（会禁消费、重蹈 DSN-62(A) 回归）；一律走 `markChainProtected`。
- **over-protection**：构建 SimTx 即保护会让更多 producer 进入“不可 wound”集 → 可能抬高热 key 争抢。
  与热 key hold-and-wait 主体（§0）正交；本 DSN 只保证 DAG 足迹一致，热 key 主体仍留待整单逃生设计。
- **P1 与逃生解耦**：producer 构建后不可 wound，若其 SimTx 因真实链上锁冲突长期无法 commit，会成为
  “不可 wound 但没法推进”的节点 → 必须靠 §2.3 失败即失效兜底（有界），真正“不可推进也不失效”的
  长尾仍属整单逃生范畴（本 DSN 范围外）。
- **跨分片 U 原子保护不可行**（用户已确认）：不追求，改 §2.4 合格门槛 + §2.3 失效兜底。

## 6. 范围外 / 后续（本 DSN 不做）
- force-admit 移除 + 热 key reservation hold-and-wait 的整单逃生（另立 DSN）。
- CMH / 整单 abort 对“不可 wound 僵尸”的兜底。

## 7. 代码可行性核对（评审附录，2026-09-08）
> 不改设计决策，仅把改动点落到实际代码，供实现前核对。

| # | 改动 | 现状位置 | 核对结果 |
|---|---|---|---|
| A | P1 producer 自保护 | `impl.go` `CommitSimulation`：`tlv.TryLockWithPriority` → `finalizePatch` → `AddNode` → `SubmitSimulationTx` | ✅ 在 `AddNode` 后插 `markChainProtected` 即可；`s.retryScheduler.offChainDAG.markChainProtected` 现成可达；`canWound` 已查 `isChainProtected`（temp_lock_view.go:207），**无需改** |
| B | 祖先链保护 | `protectUpstreamHeldLocksBFS`（retry_scheduler.go） | ✅ 现成，visited + maxChainDepth，直接复用 |
| C | wound→移出 | `notifyWoundedUpstream`（retry_scheduler.go:1922），调用点 temp_lock_view.go:309 | ✅ 已实现：`offChainDAG.Remove(U)` + 从 D `consumedPatches` 解绑 |
| D | verify-fail→移出 | verify 失败走 `sendRollbackVoteForRetry` / 终局 `closeTransactions`；节点删除在终局 `impl.go:1573 offChainDAG.Remove` | ⚠️ 需把 verify-fail 接进 `invalidateUpstream`（现只 cover wound），**新增接线** |
| E | RetrySignal 整单标志 | RetrySignal `UpstreamTxList`：Go struct types.go:1029 + proto field 8 | ✅ `UpstreamTxList` 已随跨分片 RPC 传输（comm.go `RetrySignalToProto`），**复用即可、不改 proto**；要改的是 `HandleRetrySignal` 的“仅本地上游才 markChainTx”过滤（retry_scheduler.go:1491-1511） |
| F | AddNode 与 chainProtected | `patchpool.go` `AddNode` 只 `SetStatus(PatchFree)`、更新字段，**不重置** `chainProtected` | ✅ 无需改；mark 后可跨 AddNode 复用保留（重模拟续保护）——与 P1 一致 |

**代码级结论**：
1. 直接实现项：A、B、E（工作量小，B/E 复用/小改）；
2. 需新增接线：D（verify-fail → invalidate）；
3. 无需动 proto / temp_lock_view 锁门；
4. P1 scope 已按设计定稿为“所有构建 SimTx 的 producer 一律保护”，over-protection 由 §2.3（失败即失效）与后续整单逃生兜底。
5. 注意区分两个方向：P1 保护“被消费/会成为 producer 的上游 U”；P2/P2.5 标记“消费上游的 chain 成员 D”（RetrySignal/scanPatchSubscribers 触发，patchpool.go:762）。两者都做才闭环。

## 7.1 符合性修复（2026-09-08，短窗口复测已验证）
**发现的缺陷**：A 初版用 `markChainProtected` 给 producer 自保护，与 DSN-62“被下游消费的上游”共用同一
`chainProtected` 旗。日志审计（174615 短窗口，shard1 leader 9040）发现 **561 个被 wound 目标里 260 个在
受伤前 ~1s 刚构建过 SimTx**——根因是下游 D `releasePatch(U)`（patchpool.go:264 原无条件
`unmarkChainProtected(U)`）把 producer 的自保护也一并抹掉，1ms 后 U 即被另一高优 tx wound。

**修复（方向 1：producer 自保护与下游保护解耦）**：
- `ssc/patchpool.go`：`OffChainPatchNode` 新增独立 `producerProtected atomic.Bool`；
  新增 `markProducerProtected / unmarkProducerProtected / isProducerProtected`；
  `isChainProtected` 改为 `chainProtected ∥ producerProtected`；
  `releasePatch / unmarkChainProtected` **只撤下游那份 chainProtected，不碰 producerProtected**。
- `ssc/impl.go`：A 改调 `markProducerProtected`。

**复测（193845 短窗口）**：P1 violations（wound 目标在受伤前刚构建过 SimTx）从数百 → **0/0/0/1**
（4 个 shard leader：9000=1、9040=0、9080=0、9120=0，残留 1 例为 2ms 窗口边缘）；整体结果基本不变
（commit 12248/61.24%、unfinished 7649）——符合预期，因为 A/P1 只治 DAG footprint、不治非 DAG 热 key
死锁。**实现现与 DSN-65 的 P1/P2 设计一致；D(P3 verify-fail 失效)仍未实现（延后）。**
