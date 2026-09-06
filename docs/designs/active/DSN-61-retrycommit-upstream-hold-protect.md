# DSN-61: RetryCommit 救援下游时连带“保锁/提权”其上游，防上游被 Wound 饿死

> 状态：**design + 首版已实现（待实验验证）**
> 关联：DSN-52/53/54/55/56/57/58/60。来源：DSN-60 Task B 实验后对残留
> `VerifySimulation: upstream patch not on-chain yet -> rollback (DSN-57 deterministic)` 的根因分析。
>
> **一句话**：Task B 让 leader 把下游 D 扣住等“上游 U 上链”，但若 **U 不是 DAG 救援交易**、仍按普通
> 优先级暴露，就可能被**更高优先第三方 Wound** 拉回重试 → U 迟迟无法先于 D 上链 → D 被有界扣留 50 块后
> 强制放行 → 仍 DSN-57 回滚。本 DSN 在 `RetryCommit` 的 DAG 救援路径上，除了保护下游 D 的锁外，
> **连带把 D 实际消费的上游 U 的 TLV 写锁也提权保锁**，让“U→D”整条链免疫被 Wound，上游得以稳定先上链。

## 0. 背景：为什么 Task B 之后仍有“上游未上链”回滚
- Task B（DSN-60）在 internal_pool `Extract` 加“本分片本地依赖就绪门”：D 的本地 U 既不在同批也未
  on-chain 时，D 被有界扣留（`simNotReadyHoldBlocks=50`），达到 50 块仍不满足则强制放行 → verify
  fail-closed（`upstreamOnChain(U)=false`）→ 确定性回滚。
- 实测 Task B 把该回滚从 18018(旧二进制/无 TaskB) 降到 4372，说明门有效但**未清零**。
- 根因（本 DSN）：剩余那批的 U，其本身“不是 DAG 救援交易”，仍按普通全局优先级参与 TLV Wound-Wait。
  链上 Verify 的成功路径会对**下游 D** 调 `protectHeldLocks(D)`（DSN-55 rev2，把 D 已持 TLV 写锁提到
  最高优先），但**从不碰 U 的锁**。于是：
  1. 第三方更高优先 W 抢 U 的 key → `canWound(U)=true` → U 被 Wound、`woundedTxs` 标记、U 拉回重试；
  2. U 被反复 Wound → 无法把 SimTx 送上链 → `AddOnChainDAGPatch(U)` 迟迟不发生 → `upstreamOnChain(U)=false`；
  3. D 即使已被 `protectHeldLocks` 保锁不被 Wound，仍因“上游 U 未上链”被 50 块有界扣留后强制放行 → DSN-57 回滚。
- 本质：**“保护下游 D 不被 Wound”却放任“D 所依赖的上游 U 被 Wound”**，优先级提升只覆盖链尾、没覆盖链头。

## 1. 设计：救援 D 时连带保护其被消费的上游
在 `retryCommit`（`ssc/retry_scheduler.go`）里，凡是 D **成功消费上游并成为链式下游**的路径
（这代表 U 必须先于 D 上链），除了对 D 调用 `protectHeldLocks(D)` 外，再对本次消费的每个上游 U
调用 `protectHeldLocks(U)`：
- 快速路径：`consumedPatches[txHash]` 已存在（之前已消费）→ 从该列表逐个保 U；
- Phase1b（TLV 冲突被 DAG 救起）成功分支 → 保本次 `consumedTxHashes`；
- Phase2b（stateDB 真实锁冲突被 DAG 救起）成功分支 → 保本次 `consumedTxHashes`。

实现用一个辅助 `protectUpstreamHeldLocks(upstreams []common.Hash)`：对每个 U 调
`rs.tempLockView.protectHeldLocks(U)`（把 U 当前持有的每把 TLV 写锁提到最高优先 `{0,0,0x0}`），
并累计 `SigUpstreamHoldProtected`（Info 级 dump `upstreamHoldProtected`）。

### 为什么只保护“被消费的上游”就够
- 只保护 D 实际依赖、必须先于 D 上链的 U（`consumedTxHashes`），不做全量同 key 交易优先级通胀，
  避免把无关的更高优先 W 无谓饿死；
- `protectHeldLocks` 只作用于 U **当前已持有**的 TLV 写锁；U 走上链（变为真链上锁）或失败被
  GC/RetryCancel 释放后，保护随之失效（临时保护），不会永久提高全局 Priority / 改变 wound 语义；
- 相比给 U 的 `Priority` 永久加权，保锁只是“U 一旦拿到 TLV 锁就不再被后来者抢”，仍保留抢锁阶段
  （`TryLockWithPriority`）按普通全局优先级公平竞争——不改变谁先抢到锁，只保证抢到后不被半路打掉。

## 2. 风险与克制（避免重蹈 DSN-57 饿死）
- **跨分片**：`protectHeldLocks` 只作用于本分片 TLV；真正跨分片的那部分 U，其锁落在其它分片，
  本分片够不到，仍靠 DSN-58 就绪聚合/CMH 兜底——本改动只消除“本分片本地 U 被 Wound 饿死”这一截。
- **优先级通胀/饿死 W**：被保护的上游数量是“实际被消费为链上的 U”，有界且临时；若担心 W 被饿死，
  由 reservation 反饥饿（DSN-58）+ CMH 死锁检测兜底，不新增永久饥饿。
- **不改变全局 wound 语义**：只在“DAG 救援成功、D 已成为某 U 的下游”这一确定语义下触发，绝不在
  无依赖关系的裸冲突上触发。

## 3. 改动清单（已实现）
- `ssc/retry_scheduler.go`：
  - 新增计数器 `SigUpstreamHoldProtected` + dump `upstreamHoldProtected`；
  - 新增 `protectUpstreamHeldLocks`；在 retryCommit 三处 DAG 救援成功路径（快速路径 / Phase1b /
    Phase2b）对 `consumedTxHashes`（或 `consumedPatches` 快照）的上游逐一 `protectHeldLocks`。

## 4. 验收
跑 Task B 同配置（rate=200/shard=4/delay=10/vpn=4），对比：
- `VerifySimulation: upstream patch not on-chain yet -> rollback` 是否在 4372 基础上进一步下降；
- `upstreamHoldProtected` > 0（说明确实触发保护）；
- `unfinished/rollback` 不上升、`ring_detected` 保持 0、CMH 无新增 victim。
