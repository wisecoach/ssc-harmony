# DSN-60: 允许 DAG 下游读写上游 + Internal Pool 保证“上游先于下游”（定稿设计）

> 状态：**design（先定方案、判据可行后再实现）**
> 关联：DSN-52/53/54/55/56/57/58。DSN-59 为本设计的两部分草案底稿；本文把
> HANDOFF-20260905 的两个待办 **Task A（下游可读写上游）** 与 **Task B（Internal Pool
> 保证 SimTx 序“上游先于下游”）** 落成可实现的设计，并给出**判据（决定 A/B 值不值得做）**、
> **落点（到文件/函数）**、**验收/回归护栏**与**仍需拍板的开放点**。
>
> **一句话**：DSN-58 已消除跨分片 ready 死锁/冻结（rings=0、retryPool 单调下降）。残留 unfinished
> 主要是写同一热 key 的一大批交易，链只建到 depth 2–3。A 让它“真依赖的短链”能同块连续读写上游、
> 少 rollback；B 让“能确定的同分片/同批上游先于下游”不再因找不到 patch 而 rollback——但都**不得**
> 复现 DSN-57 的“扣死跨分片下游”饿死回归。

## 0. 基线（DSN-58 后，2026-09-05 22:54，rate=200）

```
committed 11916 / rollback 2631 / unfinished 5453
CMH ring_detected / victimDied = 0
Shard1: 头号热 key distinct tx=667; chainDepthDist depth1=6806 depth2=5694 depth3=923 depth4=83 depth5=7
chainDepthCapped=0（深度上限非瓶颈）
dagPerBlock maxSameKeyWrites: 最大 15，但多数块=1
```

## 1. 与代码现状的对账（本 session 已核实）

- `ssc/verify.go`（~L400-440）仍含 **DSN-57 确定性 fail-closed**（`upstreamOnChain`+`SigUpstreamNotReady`
  +“upstream patch not on-chain yet -> rollback”）。它与 DSN-57 文档里 REVERT 掉的那版**不同**：现在这版
  建立在 `LocalUpstreamTxRef` 分片收敛之上（每个分片只带本分片确有节点的上游），因此能保留，也正是
  Task B 想压的 rollback 计数来源。
- `ssc/internal_pool.go` `Extract`：SimTx 桶只做**同批** `orderSimTxsByDAG`；**没有** DSN-57 回滚掉的
  `filterReadySimTxs` 扣留门。跨批/跨块、跨分片的上游就绪没有门，只靠 verify fail-closed 兜底。
- `ssc/patchpool.go` 匹配链：`collectBlockedKeys` **已同时统计 Read∪Write**，但只取“**当前被锁占用**
  的 key”作为覆盖需求（`findCoveringSet`/`isFullyCovered`/严格更早 simNum/`maxChainDepth`）。
  → 所以 Task A 的“不只 Read”落点不在“把 WriteSet 塞进 needKeys”（已在做），而在 **“下游要写、但此刻
  锁已被上游 commit 释放的 key 也要能作为续链锚点”**。
- 链式一致性（verify L510-560）只对下游 **ReadState** 里被上游写过的 key 做 patch 校验；下游 **WriteState**
  没有“续接自 (upstreamTx, key)”的显式记录，正确性纯靠 DAG 全序 + 跳过锁检查。
- 关键事实：SimTx 只有“**模拟/撞锁后**”才有读写集，入队阶段没有读写集（交接§5.3），因此一切链判断
  只能在 DAG/放行/verify 阶段做，不能在入队阶段假设依赖。

---

## 2. Task A：允许下游 SimTx 读上游写值并继续写（含先判据）

### 2.1 目标
让“真读-写依赖”的下游 SimTx 能：读上游写出的值（已支持，verify 校验 Read）、并**继续写**同一/相关 key
（可同块连续落多笔），由 DAG 全序 + 块内顺序保证正确；在 verify/匹配端把下游 **Read∪Write** 都纳入
“需被前序覆盖/续接”的判定，并记录“本 key 接续自哪个上游 (TxHash,simNum)”。

### 2.2 先判据（可行性闸门，**先做这一步**）
抽 Shard1 头号热 key ~667 笔，判断是否为真读-写依赖链：
- 判据1：第 n+1 笔的 `UpstreamTxList` 是否引用该 key 上一写者 n（TxSimKey）。
- 判据2：第 n+1 笔的 `ReadState` **或** `WriteState` 是否覆盖第 n 笔 `WriteState` 的 key。
- 分类输出：`read-write`(读到并续写) / `write-only 同 key`(只撞槽不读) / `不同槽位`。
  - read-write / write-only 同 key → A/B 有收益；
  - 纯不同槽位 → 只能走争用削减（不在本 DSN 范围）。
- 工具：复用 `scripts/remote_ssc_retry_stats.py` 模式写抽样脚本，按 `chainDepthDist` + 热 key 的
  `conflictKey` 逐笔对 `UpstreamTxList`/`ReadState`/`WriteState` 比对；勿裸 grep。

### 2.3 设计要点
1. **匹配锚点从“当前被锁占用的 key”扩展为“上一写者已产出该 key 的节点”**：
   - `ssc/patchpool.go` `scanPatchSubscribers`：当下游 Write 命中某 Free/已 Finalized 上游节点写过的 key，
     即使该 key 锁已被上游 commit 释放（`collectBlockedKeys` 判为不阻塞），也应把该上游视为可续接点，
     而不只依赖“锁仍占用才要覆盖”。
   - `findCoveringSet` 现有约束（严格更早 simNum、覆盖完整、`maxChainDepth`）保留；只放宽“哪些 key
     需要上游覆盖/续接”的来源口径。
   - 一个已完成上游可同时续接多个下游（按 nonce/全序成批），对应 DSN-59 §B2/B3；链深仍 ≤ maxChainDepth。
2. **verify 对下游 Write 也记录续接自上游**：
   - `ssc/verify.go` 链式分支：除 Read 命中上游校验外，对下游 Write 中“也是某上游（递归）写过的 key”，
     在 `AddOnChainDAGPatch`/ChainNode 记录 `本 key 接续自 (upstreamTxHash, simNum)`（书签/观测）。
   - 语义澄清：**Write 没有独立“期望值”可比**——正确性靠块内顺序“上游先执行写出、下游再执行”。
     因此这里的“一致性/记录”是续链书签与可观测，不是把下游写值与某期望比对。
3. **不变量**：同 key 写按 DAG 全序串行，但可同块连续落多笔（`dagPerBlock>1`），由块内顺序保证正确。

### 2.4 落点清单（实现阶段）
- `ssc/patchpool.go`：`collectBlockedKeys` / `scanPatchSubscribers` / `findCoveringSet` 覆盖口径扩展；
  新增 `chainMatched(ReadOnly/ReadWrite)` 打点、`每 key 每块续接笔数`分布。
- `ssc/verify.go`：链式分支对下游 Write 命中上游的续接记录/校验（书签 + `AddOnChainDAGPatch` 时写回）。
- `ssc/api/types.go`：如需要，`ChainNode`/`SimTx` 增可选字段“续接自 (upstreamTx,key)”（先看能否仅用观测）。
- `ssc/retry_scheduler.go`：`OnBlockCommitted`/admission 优先续已有链、批放行同 key（若 A 判据确认热 key
  是 read-write，再做 B1/B2 式“一把热 key 一次放行更多有依赖 tx”）。

---

## 3. Task B：Internal Pool 保证 SimTx 序“上游先于下游”（避开 DSN-57 坑）

### 3.1 目标
让“能确定前序依赖”的下游 SimTx 在放行/上链时其**本分片本地**上游已注册/已入批，从而压
`VerifySimulation: upstream patch not on-chain yet -> rollback`。

### 3.2 必须避开的坑（DSN-57 教训，交接§1 Task B + DSN-57 §8）
DSN-57 曾在 internal_pool 按“上游已上链才放行”扣留 not-ready 下游，实测饿死**跨分片**下游 → rollback+
unfinished 双升 → REVERT。根因：链式 tx 是跨分片的，上游 SimTx 只落在“真正相关”的分片；下游在其它分片
验证时本地无该上游 → fail-closed 误回滚 / internal_pool 误扣留。

### 3.3 设计边界（本 DSN 的收敛结论）
**只对“本分片确有本地依赖”的下游设序，绝不对跨分片下游长期扣留。**
- 判“本地依赖”用现成语义：`LocalUpstreamTxRef(txHash)` 非空（本分片 offChainDAG 确有该上游节点）。
  为空的分片本来就把该 SimTx 当非链（fail-open 自由放行）——**永远不去扣**。
- 在这个前提下，门只解决**同分片内的确定性时序**，分两层：
  1. 同批：`orderSimTxsByDAG` 已保证（上游在该批 D 之前执行）。
  2. 跨批/跨块：若本地上游 U 尚未 `onChainDAGPatches` 注册、且 U 不在本批 D 前面 → 把 D **有界扣留**
     （下块再评估），而不是放出去撞 fail-closed。

### 3.4 有界扣留 + 兜底（绝不无限饿死）
- 复用 DSN-58 的 `reservationSkipCnt`/反饥饿思路：给 D 计“等待本地上游”的块数；
  未到阈值前 D 留池不占块预算；达到阈值仍未就绪 → 放行 D → 走 verify 确定性 fail-closed
  （本地上游真缺失则回滚，符合“回滚传播”），不无限滞留。
- **区分两类“上游未就绪”**：
  - 本地上游“真还没排到/还在 re-sim” → 应继续等（DSN-58 反饥饿保证它会来）；
  - 本地上游“已终局 rollback / 永不出现” → 应让 D 尽早回滚，而非干等。
- **只查本分片 onChainDAGPatches / 本分片 internalPool**（`upstreamOnChain` / `upstreamQueued`），
  不做任何“跨分片必见上游”判定（DSN-57 死因）。

### 3.5 落点清单（实现阶段）
- `ssc/internal_pool.go`：`Extract` 的 SimTx 桶在 `orderSimTxsByDAG` 之后，加**只针对本地链式且本地
  上游未就绪**的有界过滤（参考 DSN-57 回滚掉的 `filterReadySimTxs`，但限定本地 + 有界）；not-ready 留池、
  不占块预算。
- `ssc/retry_scheduler.go`：提供“本地上游是否已注册/是否仍在排”的只读判定（`upstreamOnChain` 现为
  txHash 级，可能需要按 `(txHash,simNum)` 且限定在 LocalUpstreamTxRef 语义内）；等待块数/阈值常量。
- `ssc/factory.go`/`ssc/impl.go`：把该谓词接给 internalPool（同 DSN-57 的接线方式，但语义限定本地）。
- 打点：`ready 放行 / not-ready 有界扣留 / 超时放行` 三组计数，便于判断门是否过严/过松。

---

## 4. 观测 / 验收指标

Task B 主目标（压 rollback）：
- `SigUpstreamNotReady`（“upstream patch not on-chain yet -> rollback”）**下降**；
- `unfinished / rollback` **不得上升**（防 DSN-57 回归）；`ring_detected` 保持 0。

Task A 主目标（读-写续链提速）：
- 同热 key 批内 `dagPerBlock maxSameKeyWrites` **明显 >1**（不再多数块=1）；
- `chainDepthDist` 深链占比提升；`chainMatched(ReadOnly/ReadWrite)>0`；
- `每 key 每块续接笔数`分布上升、`chainDepthCapped` 不显著增长。

## 5. 风险 / 回归护栏
1. **DSN-57 已 REVERT**：Task B 的“上游先于下游”只做同分片/同批可确定序 + 有界扣留；跨分片下游绝不扣死。
2. **积极长链被否**：Task A 是“允许真依赖的短链读写上游”，不是把一堆同 key tx 绑成长链成批并行——任一环
   失败整链崩。A 的价值是让真依赖短链被救起、少 rollback，不是无脑并行同 key。
3. 同块同 key 链变长会增加单块验证/执行成本与顺序耦合；用 `dagPerBlock`、块验证时长、`unfinished` 监控回退。
4. 读写集只在模拟/撞锁后才有，方案不在“无读写集”的入队阶段假设依赖。

## 6. 实现顺序（判据门先行）
1. **先判据**（§2.2）：确认热 key 批是否真 read-write 依赖。为假 → 本 DSN 对该批无效，转向争用削减，A/B 不做。
2. 判据成立后按 A→B：先落 Task A 的匹配/verify 书签（含打点与单元测试），再落 Task B 的有界 internal_pool 门。
3. 每步 `go build ./ssc/... ./rpc/...`；远程 rate=200 与基线 11916/2631/5453 对比（按 AGENTS.md）。

## 7. 开放决策点（实现前需拍板）
- D1（A 判据口径）：判据只看“上一写者直接引用”，还是也看“传递读-写闭包”？(建议先直接引用)
- D2（A 匹配键）：续接锚点放宽后，是否允许“一个上游→多个下游”成批同 key 放行？链深上限维持 5 还是放开？
- D3（B 扣留阈值）：等待本地上游的块数阈值取多少（建议沿用 reservation 反饥饿的 N，约 50 块量级）？
- D4（B 就绪谓词粒度）：`upstreamOnChain` 是否升级为 `(txHash,simNum)` 级（DSN-57 曾因 simNum 级过严，
  需结合 LocalUpstreamTxRef 语义再定）。
- D5（验证环境）：本轮判据用远端 199 的最新日志/结果，还是本机先出脚本框架？

## 8. 关联文档
- `HANDOFF-20260905-dag-downstream-rw-internal-pool-order.md`（本设计的来源）
- `DSN-59-dag-downstream-rw-and-eager-chainmatch.md`（草案底稿，含“积极长链被否”原因）
- `DSN-57-simtx-upstream-ready-admission.md`（为何 internal_pool 就绪门被 REVERT、教训）
- `DSN-58-crossshard-ready-quorum-liveness.md`（reservation 反饥饿 / needing ready 聚合，B 的有界扣留复用其阈值思想）
