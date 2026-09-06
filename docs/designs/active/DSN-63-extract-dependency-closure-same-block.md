# DSN-63: InternalPool Extract 依赖闭包同块打包（UU<U<D，按先上游后下游处理）

> 状态：**design + 首版已实现（上游优先前置），待实验验证**
> 关联：DSN-50/52/55/56/57/58/60/61/62。来源：DSN-60~62 后仍残留
> `VerifySimulation: upstream patch not on-chain yet -> rollback`（~2900 次）的根因与方向收敛。
>
> **一句话**：同块链式本可工作——同一块内 `orderSimTxsByDAG` 已把 `UU<U<D` 排好，verify 成功即在本块
> `AddOnChainDAGPatch`，故 D 能看到先验证的 U。当前失败点是 `filterReadySimTxs` 把"上游 U 不在本批"
> 当 not-ready 去**跨块扣留**，而不是把**仍在本池的 U/UU… 递归取进同一块**。本文把 Extract 改成
> **依赖闭包同块打包**：凡 D 的本地上游 U 未 on-chain 且仍在本池，就递归把 U、UU…一并取进本块，
> 按 `UU<U<D` 确定性排序；只有真缺失(既不在池也未 on-chain)才不打包。目标是让链式交易"一次同块推进
> 整条本地链"，取代"每块只能推进一跳 + 50 块有界扣留"的慢路径。

## 0. 为什么还有"上游未上链"回滚（对 DSN-60/62 的再定位）
- DSN-60(TaskB) 4372 → DSN-61(保上游) 2978 → DSN-62(B′)(D) 2894，之后不再明显下降。
- B′/D 清掉的是"被 Wound 的 doomed 上游"那类（upstreamWoundedInvalidated 565→2695），残留 ~2900 次
  主体是：**U 已正常构建进池、却还没在 D 的 50 块有界窗口内完成"从构建→提案→verify→on-chain"那一跳**。
- 根因：链式救援被实现成"**每块只能推进一跳**"——
  1. D 引用的本地 U 若不在本批且未 on-chain，`filterReadySimTxs` 就把 D **扣留**等跨块；
  2. U 自己若是下游（依赖 UU），也会被同理由扣留 → 整条链 U0→UU→U→D 必须从根逐块往前推进；
  3. 任一跳稍慢/verify 失败换 simNum，D 的 `simNotReadyHoldBlocks=50` 就可能耗尽 → 强制放行 → fail-closed 回滚。
- 但**同块链式本可成立**：同一块内 `orderSimTxsByDAG` 排序保证 UU 在 U 前、U 在 D 前；verify 成功分支
  即刻 `AddOnChainDAGPatch`（块内注册）。所以把整条本地链取进同一块、按序验证，即可一次推进多跳。

## 1. 现状核对（代码证据）
- `ssc/internal_pool.go` `Extract`：对 SimTx 桶先 `sort`(确定性优先级) → `orderSimTxsByDAG(row)`
  （只对**已在 row 内**的依赖排序）→ `filterReadySimTxs(row)`。
- `filterReadySimTxs` / `simTxLocalDepsReady`：对每个 D，若本地 U 不在 `inRow` 且 `upstreamOnChain(U)=false`
  → not-ready → 有界扣留（不入本块，`notReadyHoldCnt[D]`++，≥50 块强制放行）。
- **缺口**：它只把"上游不在同批"当作 not-ready 的扣留理由，**从不主动把仍在池中的 U/UU…并入本批**。
  于是本该同块连落的链被拆成跨块一跳一跳，且 50 块上界易被耗尽。
- 补充（why 可同块）：`AddOnChainDAGPatch` 在 verify 成功分支块内即时执行；`onChainDAGPatches`
  在同一块后续 SimTx verify 时可读到 → 同块 UU→U→D 顺序验证成立。

## 2. 设计：Extract 做"依赖闭包同块打包"
对 SimTx 桶，把原来"先 sort→orderSimTxsByDAG→filterReadySimTxs"改为：

1. **候选闭包收集（BFS）**：从按确定性优先级排序后的 row 出发，对每个拟放行 SimTx D，若其本地上游 U
   **不在本批**、**未 on-chain**、但**仍在本 internal_pool 中** → 把 U 递归并入本批（继续向上取 U 的
   上游 UU，visited 防环、`maxChainDepth` 封顶）。
2. **排序**：闭包取齐后 `orderSimTxsByDAG` 对整个（含新并入的 U/UU…）排序，保证 `UU<U<D`。
3. **就绪门后置为真缺失判定**：`filterReadySimTxs` 只在"本地 U **既不在本池、也未 on-chain**"
   （真缺失，正常应极少）时才 not-ready 扣留；**不再把"U 在池中但本轮没一起取"当 not-ready**。
4. **预算**：受块内 SimTx 预算(maxTotal)约束；闭包超预算/超深时，优先保证先上游后下游的部分落块，
   尾段回落到下一块（仍保持确定性）。

### 为什么是确定性/可复现的
- 出块内容须各 validator 复现；"本池是否含 U"由确定性规则（同池状态 + DAG + 优先级排序）决定，
  与 leader 内存时序无关的依赖都已在池/on-chain 反映。闭包 BFS 用同一套本地 DAG/池状态 → 各节点一致。
- 扣留计数 `notReadyHoldCnt` 仅作"真缺失"兜底，仍保留。

### 为何不会重蹈 DSN-57 饿死
- 只对**本分片本地**链做闭包（`LocalUpstreamTxRef` 已收敛）；绝不为"上游在别分片"的下游做闭包，
  避免把跨分片下游长期扣死。真缺失的本地 U（已 CR 终局/从 DAG 移除）才可能触发扣留 + 50 块强制放行。

## 3. 风险与克制
- **深度/预算**：一条深链全取会占块预算 → `maxChainDepth` 封顶，超出部分按确定规则落下一块；
  不追求"一块塞下无限深链"，只求"池内可达的本地链尽量一次推进多跳"。
- **块内 verify 顺序**：依赖 `orderSimTxsByDAG` 对闭包排序正确；若出现异常环（确定性排序兜底追加）
  仍不阻塞出块。
- **跨分片**：只作用于本分片本地依赖；跨分片腿的推进仍靠 DSN-58 就绪聚合/CR，非本文范围。
- **与 Task B 有界扣留共存**：`filterReadySimTxs` 语义收窄为"真缺失才扣留"，不再误扣"本池可闭包"的 D。

## 4. 改动清单（首版已实现——聚焦"上游优先前置"，最小改动）
- `ssc/internal_pool.go`：
  - 新增 `simUpstreamRefSet(row)`：收集"本池内被其它 SimTx 引用为上游"的 txHash 集合（上游提供者 U）。
  - `Extract` SimTx 分支：在确定性优先级排序后、`orderSimTxsByDAG` 前，用 `sort.SliceStable` 把
    `upstreamRefSet` 中的 SimTx **前置**（上游优先进入本块切片）；同组内保持原优先级相对顺序；
    之后再 `orderSimTxsByDAG` 保证 `UU<U<D`，最后 `filterReadySimTxs`（仅真缺失扣留）。
  - 说明：这是对"依赖闭包同块"的**预算侧落地**——不做递归把整条深链都塞进一块，而是**保证上游
    提供者 U 不因低优先级而排在预算切片之外**（U 被取走提案→下块即 on-chain→D 只需多等 1 块而非
    50 块）。普通(非上游)SimTx 排后面。

## 5. 验收
同配置（rate=200/shard=4/delay=10/vpn=4）对比：
- `VerifySimulation: upstream patch not on-chain yet -> rollback` 在 ~2894 基础上**显著下降**；
- `sameBlockChainPack > 0`（说明确实在把链闭包进同块）；`unfinished/rollback` 不上升、`ring_detected` 保持 0；
- DAG 链式救援仍活跃（`dagHoldProtected` / `retryCommitPatchHit` 不退化）。
