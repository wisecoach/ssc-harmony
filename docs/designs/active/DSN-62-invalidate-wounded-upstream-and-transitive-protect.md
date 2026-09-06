# DSN-62: 让“被 Wound 的上游”不再卡死其下游 —— finalize 顺序 + 上游无效化 + 保护沿链传递

> 状态：**design + (A)(B)(C) 已实现，待实验验证**
> 关联：DSN-55/56/57/58/60/61。来源：DSN-61 实验后仍残留
> `upstream patch not on-chain yet -> rollback`（4372→2978 仍不为 0）的根因收敛。
>
> **一句话**：DSN-61 只保了“下游 D”，没保 D 依赖的“上游 U”；而 U 即使“形成了 SimTx”也可能因
> Patch 状态机的缺陷仍可被 Wound，且 U 被 Wound 后其被 D 消费的 Patch 不释放，导致 D 永远链在一个
> 注定上不了链的 U 上、50 块后被 fail-closed 回滚。本文给三个修法：**(A) 修正 finalize/AddNode 顺序让
> “构建 SimTx 的上游不再可被 Wound”；(B) U 被 Wound 时无效化其 Patch 并解除依赖它的下游；(C) 保护
> 沿被消费的上游链递归传递**。

## 0. 为什么还有“上游未上链”回滚（对 DSN-61 残留的精确定位）
- DSN-61（保护被消费的直接上游）实验：`upstream-not-onchain` 4372→2978（↓32%），但未清零。
- 逐条排除后，真正可动手的是两类（其余归为有界扣留/纯调度/跨分片腿的固有时序，非“上游被 wound 卡死”）：
  1. **U 形成了 SimTx 却仍可被 Wound** → U 被 Wound → 迟迟无法先于 D 上链 → D 被 50 块有界扣留后回滚；
  2. **U 被 Wound 后其 Patch 仍被 D 消费着** → D 链在一个注定上不了链的 U 上 → 必回滚；
  3. **保护不沿链传递**：U 自己的上游 UU 也能被 Wound → 只保 U 不够，链头仍断。

## 1. 现状核对（代码证据）
- `patchpool.go`：
  - `AddNode(...)` **无条件 `SetStatus(PatchFree)`**；`OffChainPatchNode.Consumer` 记录“谁消费了本节点”。
  - `finalizePatch(U)` **仅在 U 已是 `PatchConsumed` 时**才置 `PatchFinalized`（=不可被 Wound 的闸门）。
  - `releasePatch(U)` 只在“**消费方 D** 重试失败”时被调；**上游 U 被 Wound 时没有任何 release/无效化**。
  - `isPatchFinalized(U)` 是 `canWound` 的闸门（`temp_lock_view.go`：holder patch Finalized → 不可被 wound）。
- `impl.go`（CommitSimulation 构建 SimTx）行序：
  ```
  1124: offChainDAG.finalizePatch(txHash)   // 意图：锁死不可被 Wound
  ...
  1132: offChainDAG.AddNode(txHash, writeSet, upstreamTxList)  // AddNode 又 SetStatus(PatchFree)
  ```
  → **finalizePatch 先于 AddNode 且要求 PatchConsumed；AddNode 后重置为 Free**：U 构建完 SimTx 后
  节点实际仍多为 PatchFree（woundable）。即“形成 SimTx 的上游不可被 Wound”的**意图未真正生效**。

## 2. 设计

### (A) 修正 finalize 语义：构建 SimTx 的上游真正不可被 Wound
- 目标：一笔 U 一旦“被某下游 D 消费（PatchConsumed）+ 自身构建 SimTx 提交”，就应转为 `PatchFinalized`
  （不可被 Wound），直到它 on-chain 或 CR 终局。
- 做法：
  1. `impl.go` 把 `finalizePatch(txHash)` 移到 `AddNode(...)` **之后**（AddNode 建/更新节点后再 finalize）；
  2. `AddNode` **不得把已是 `PatchFinalized` 的节点降级回 `PatchFree`**（若节点已存在且为 Finalized，
     保留 Finalized 并只更新 Patch/UpstreamTxList/Depth）；
  3. `finalizePatch` 放宽为：节点 `PatchConsumed` **或** `PatchFree`（本节点自身构建 SimTx）都转 Finalized，
     以覆盖“U 首次构建（Free）即应锁死”的情形。
- 效果：U 提交 SimTx 后即不可被第三方 Wound → “U 形成 SimTx 仍被 wound”这类直接消失。

### (B) 上游 U 被 Wound 时无效化其 Patch + 解除其下游（治“链在一个 doomed 上游上”）
- 触发点：`TempLockView.TryLockWithPriority` 中 `canWound(U)=true` 把 U wound（写入 `woundedTxs`、
  替换锁）之后；或专门的 wound 回调。
- 动作：
  1. 在 `offChainDAG` 把 U 的节点标记无效/移除（`Remove(U)` 或其 `releasePatch` + 置失活），
     使 U 不再作为可匹配上游被 `findCoveringSet` 选中；
  2. 用 `node.Consumer` 找到消费 U 的下游 D，把 D 从“链在 U 上”解除：
     - 从 `consumedPatches[D]` 中去掉 U；
     - 若 D 因 U 而处于 not-ready/被扣留，则允许 D 退回正常冲突重试（等 U 重新模拟出**新** patch 后再续链），
       而非继续扣到 50 块。
- 效果：被 Wound 的上游不会再“拖死”下游；下游不会在 doomed 的 U 上白白等待 50 块。

### (C) 保护沿被消费的上游链递归传递（治“链头 UU 也被 wound”）
- 在 DSN-61 的 `protectUpstreamHeldLocks` 基础上：保护 U 时，读 U 的 `UpstreamTxList`
  （或其 `consumedPatches[U]`）继续向上保护其祖先。
- 约束：visited 防环；深度 ≤ `maxChainDepth`；只作用于“实际被消费的链式子图”，不做全量通胀；
  `protectHeldLocks` 只对“当前已持 TLV 写锁”的节点有效，未持锁的祖先为 no-op。

## 3. 风险与克制
- (A) 是状态机修正，需保证不把“本应可被 re-consume 的 Free 节点”误锁死；仅在“本节点自身构建 SimTx 提交”
  时才 Finalize，不误伤仍自由可被多路消费的中间节点。
- (B) 解除下游要正确清理 `consumedPatches` 反向引用，避免下游以为自己仍有 U；解除后回到正常冲突重试即可，
  不应无限重试（仍受 retry limit / 有界扣留保护）。
- (C) 保护深链加大 W 饥饿风险 → 由 reservation 反饥饿（DSN-58）+ CMH 兜底，不新增永久饥饿。
- 跨分片：以上都作用于本分片 TLV/DAG；真正跨分片腿的时序仍靠 DSN-58/CR，非本文范围。

## 3.5 实现落点（已实现，代码编译通过）
- (A) `ssc/impl.go`（finalizePatch 移到 AddNode 之后）+ `ssc/patchpool.go`（finalizePatch 放宽、不降级）。
- (B) `ssc/retry_scheduler.go`（`notifyWoundedUpstream`：解绑消费方 + `offChainDAG.Remove` 无效化；
  计数 `upstreamWoundedInvalidated`）+ `ssc/temp_lock_view.go`（两处 wound 后调 `notifyUpstreamWounded`）。
- (C) `ssc/retry_scheduler.go`（`protectUpstreamHeldLocks` 改 BFS 沿链传递，visited+maxChainDepth）。

## 4. 验收
同配置（rate=200/shard=4/delay=10/vpn=4）对比：
- `VerifySimulation: upstream patch not on-chain yet -> rollback` 在 2978 基础上进一步下降；
- 新增/已有计数（如“被 Wound 上游无效化次数”）>0，说明 (B) 触发；
- `upstreamHoldProtected` 持续增长、(A) 生效后“形成 SimTx 仍被 wound”的观测下降；
- `unfinished/rollback` 不上升、`ring_detected` 保持 0。
