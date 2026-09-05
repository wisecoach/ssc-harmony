# DSN-59: 让 DAG 下游“读写上游状态”并更高效地成链/续链

> 状态：**draft（先定方案，再实现）**
> 关联：DSN-50/52/54/55/56/57/58。
>
> **一句话**：DSN-58 已消除跨分片 ready 死锁/冻结（rings=0、retryPool 单调下降）。残留 unfinished 主要是一大批
> 写同一热 key 的交易：它们**本该互为“读-写”依赖、能被 DAG 高速成链处理**，但实测链只建到 **depth 2–3** 就断，
> `dagPerBlock maxSameKeyWrites` 多数块仍为 1，剩下退化成普通串行 reservation → 慢。
> 本文分两部分：**(A) 确认并实现“下游可同时读写上游状态”**；**(B) 更高效地把同一 key 的更多 DAG 项匹配/续链成批**。

## 1. 实测基线（DSN-58 后，2026-09-05 22:54，rate=200）
```
committed 11916 / rollback 2631 / unfinished 5453   (vs DSN-58 前 11001/2285/6714)
CMH ring_detected / victimDied = 0（死锁/冻结已消除）
reservation_skip：Shard1 3.09M 行，头号热 key 上 distinct tx=667（各分片 ~700–840 个 conflictKey、每 key 堆百~千笔）
chainDepthDist(Shard1): depth1=6806 depth2=5694 depth3=923 depth4=83 depth5=7
chainDepthCapped = 0（maxChainDepth=5 未触发 → 深度上限不是瓶颈）
dagPerBlock maxSameKeyWrites：Shard1 最大 15，但多数块=1
```
⇒ 链很浅（2–3），不是被 depth cap 截断，而是**没被续接成更长的同 key 链**。

## 2. 背景语义：下游“读写上游”
- 链式下游 SimTx 应可**读上游写出的值**，并**继续写**（包括写上游也写过的 key）。它基于上游 patch 的值
  构建自己的 SimTx，verify 时 `isChainTx` 跳过锁冲突、靠 DAG 全序 + 块内顺序保证“上游先执行、下游读到其写”。
- 现状代码点（verify.go）：
  - 链式一致性只校验下游 **ReadState** 里被上游写过的 key（`ReadOnChainDAGPatch`）；
  - 下游的 **WriteState** 不与上游做显式续链匹配，只依赖 DAG 顺序 + 锁冲突覆盖。
  ⇒ 需确认/补强：把下游**要写的 key** 也纳入“该 key 由哪个上游产出/前序写出”的续链判定，形成真正的
  读-写依赖链（下游读上游输出、写同一/相关槽位，前序完成后继可接）。

## 3. Part A：判断是否“读-写依赖”，并让下游可读写上游
### 3.1 先判（诊断，决定 A/B 值不值得做）
抽 Shard1 头号热 key 那 ~667 笔，判断它们是否构成“读-写依赖链”：
- 判据1：第 n+1 笔的 `UpstreamTxList` 是否引用第 n 笔（该 key 的上一写者）；
- 判据2：第 n+1 笔的 ReadState/WriteState 是否包含第 n 笔 WriteState 的 key（读它写的值 / 接着写它）；
- 若成链 → 做 Part A+B 把链续起来，可显著提速；
- 若只是“写同一合约不同槽位、互不读” → 不能链，只能串行（Part A/B 对该批无效，需另想争用削减）。

### 3.2 实现：下游读写上游
- 让 DAG 救援/匹配时，把下游的 **Read ∪ Write** 都作为“需要被前序上游覆盖/续接”的 key（不只 Read）；
- verify 的 patch 一致性：下游 Read 中命中上游写值 → 校验一致；下游 Write 命中的 key 由块内顺序保证
  （上游 SimTx 先执行写出，下游再执行），并在 ChainNode/UpstreamTxList 记录“本 key 接续自上游 TxHash@simNum”。
- 语义不变量：同一 key 的写按 DAG 全序串行，但可**同块连续落多笔**（`dagPerBlock>1`），由块内顺序保证正确性。

## 4. Part B：更高效匹配更多 DAG 项（成链/续链）
现状每轮只在“下游已冲突/待放行”里找覆盖上游，链只续 2–3 层。目标“一把热 key 一次尽量放行更多有依赖的 tx”：
- **B1 续链优先**：同一 key 的待放行 tx 里，凡能“接到最新已完成上游(DAG 节点)之后”的（读/写其输出）就立即
  续进当批，而不等它先撞锁再被 rescue；链深度只要 ≤ maxChainDepth 就继续（实测 cap=0 触发，说明可放开更长链
  或按批推进，不构成瓶颈）。
- **B2 批匹配**：一个已完成上游节点可同时作为多个下游的覆盖/续接点；匹配从“1 上游→1 下游”改为
  “1 上游→多下游(按 nonce/全序)”成批放行同 key 链，提高 `dagPerBlock maxSameKeyWrites`。
- **B3 匹配键 = Read∪Write（呼应 Part A）**：覆盖/续接按下游完整读写集找前序，减少“读到了但写没接上/写被锁”漏配。
- **B4 观测**：新增计数 `chainMatched(ReadOnly/ReadWrite)`、`dagPerBlock sameKey>1 的块占比`、`每 key 每块续接笔数分布`，
  验证 A/B 后 `maxSameKeyWrites`、链深、以及 unfinished 是否改善。

## 5. 实现位置（待 Part A 判据确认后按此落地）
- `ssc/offchain_dag.go` / `ssc/patchpool.go`：续链/批匹配逻辑、Read∪Write 覆盖；
- `ssc/verify.go`：链式一致性对下游 Write 命中上游的校验/记录；
- `ssc/retry_scheduler.go`：OnBlockCommitted/admission 里优先“续已有链”，批放行同 key；
- `ssc/api`：如需在 ChainNode/SimTx 记录“接续自 (upstreamTx,key)”的可选字段。
- 每步 `go build ./ssc/... ./rpc/...` 验证。

## 6. 验收
- Part A 判据成立时：同热 key 批内 `dagPerBlock maxSameKeyWrites` 明显 >1、链深提升、`unfinished` 再降；
- Part B：`chainMatched(ReadWrite)` >0、每 key 每块续接笔数上升、不再退化成同 key 逐块 1 笔的串行。
- 无回归：不重新引入 §3/DSN-58 的孤儿/死锁（rings 保持 0、ready 门不拆）。

## 7. 风险
- 若热 key 批并非读-写依赖（Part A 判据1/2 为假）→ Part A/B 对该批无效，不能靠续链提速，需转向争用削减。
- 更长的同块同 key 链增加单块验证/执行成本与顺序耦合；需以 `dagPerBlock`、块验证时长监控回退。
