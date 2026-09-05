# HANDOFF-20260904(续)-按用户决策先移除 ChainPatch —— 交接记录

> 承接 `HANDOFF-20260904-chainpatch-removal-upstreamtxlist-cachedag.md`
> 用户决策：**先移除扁平 ChainPatch，在纯 DAG/UpstreamTxList 上修 bug**（不保留混用中间态）。
> 契约文档：`docs/designs/active/DSN-56-remove-chainpatch-pure-dag-contract.md`（权威，改动前先读）。

## 0. 已定基线/前提
- DSN-54/55、simDAGPatches、RetryCommitDAG 等未提交改动**保留**；它们可编译、与 DAG 方案同构。
- 成员侧“读上游写集”改由 simDAGPatches（leader 广播）承载；leader 读 offChainDAG。**不再有扁平写集搬运**。
- proto 可安全再生（本机版本与生成头一致）。

## 1. 已完成（全部 `go build ./ssc/... ./rpc/...` 通过）
A. **断链修复（前置，让 origin/RetryCommit 的 DAG 上游边落盘）**
   - `retryCommit` Phase1b/2b：消费上游后 `offChainDAG.AddNode(txHash, simNum, merged, upstreamTxList)`。
   - `RetrySignal` 新增 `UpstreamTxList`（struct+proto 字段8+convert+pb 再生成）；`sendChainSignal` 填入；
     `HandleRetrySignal` 在 origin 侧按上游边 `AddNode`。
B. **DSN-56 step1：移除 simulator 读路径扁平 ChainPatch**
   - 删 `SimulationState.ChainPatch` 字段。
   - 删 `Simulator.SetChainPatch`、`RetrySchedulerStateAccessor.SetChainPatch` 字段、impl 接线、
     retryScheduler 3 处调用。
   - `readChainPatch` → `readUpstreamValue`（只查 offChainDAG 链 + simDAGPatches，不再查 SimState.ChainPatch）。
   - `GetState/SetState` 去掉 SimState.ChainPatch 直查分支。

## 2. 未做（下一步，耦合点，需远程验证）
- **verify 侧扁平清理**：`CXTSimulation.ChainPatch` 字段 + `verify.go` `AddOnChainPatch` +
  `onChainPatches`(Add/Get/Remove/Read/readOnChainPatchVisited) + `committer.RemoveOnChainPatch` +
  monitor `OnChainPatches` 计数 + proto 字段。
- 依据 DSN-56：成员 verify 的一致性检查改从 simDAGPatches 反查；删除前必须先补“下游 Verify 排在上游
  之后”的显式就绪门（现状是 ReadOnChainPatch found=false 即跳过，属 spec 缺口）。
- 删除后 grep 基线：`ChainPatch`、`SetChainPatch`、`readChainPatch`、`AddOnChainPatch`、`GetOnChainPatch`、
  `RemoveOnChainPatch`、`ReadOnChainPatch`、`readOnChainPatchVisited`、`onChainPatches`、`ChainPatchRef` 应为 0。
- 验收口径（沿用）：rate=200，SimTx 带 `UpstreamTxList`→`chain tx detected`→CR commit；unfinished 下降。

## 3. 追加：verifier 侧扁平 ChainPatch 已移除（DSN-56 step4 的 SimTx 载体）
- 删 `CXTSimulation.ChainPatch`（struct/Bytes/proto 字段9/convert/impl 构造）。
- `verify.go`：验证成功后用 `simOwnWriteSet(sim)`（由 CallStates 推导自身 WriteSet）调 `AddOnChainPatch` 导入
  onChainDAGPatches；下游一致性检查本就按 `UpstreamTxList` 反查上游 patch（`ReadOnChainPatch`）。
  **全程无扁平 ChainPatch**。
- `go build ./ssc/... ./rpc/...` 通过。
- 剩余（下一步）：`RetrySignal.ChainPatch`、`CXTSimulationState.ChainPatch/ChainPatchRef`、`onChainPatches` 命名
  与 monitor 计数；把 onChainPatches 更名 onChainDAGPatches。


## 4. ChainPatch 已全量移除（DSN-56 完成）
- 字段级：RetrySignal.ChainPatch / CXTSimulation.ChainPatch / SimulationState.ChainPatch / CXTSimulationState.ChainPatch/ChainPatchRef 全删。
- 存储/方法：onChainPatches → onChainDAGPatches；Add/Get/Remove/ReadOnChainPatch → *OnChainDAGPatch；
  GetChainPatchRef → GetDAGNodeRef；monitor/proto 统计 on_chain_patches → on_chain_dag_patches。proto 已再生成。
- 语义：消费上游不再 merged 扁平写集；救援节点 AddNode 只落 UpstreamTxList（own Patch nil，由 CommitSimulation 覆盖）。
- ssc 内无 ChainPatch 标识符（仅“已移除”注释）。`go build ./ssc/... ./rpc/...` 通过。
- 下一步独立课题：上游 SimTx 先于下游执行/先导入 onChainDAGPatches 的“就绪门”（见 DSN-56 [next]）。

## 5. DSN-57 上游就绪门（internal_pool）已实现
- internal_pool：simUpstreamOnChain 谓词 + SetSimUpstreamOnChain + filterReadySimTxs（not-ready 留池、不占预算）。
- retryScheduler.upstreamOnChain(txHash)；factory 接线。
- verify fail-closed：上游未注册 → 发回滚票（SigUpstreamNotReady 计数）。
- go build ./ssc/... ./rpc/... 通过。待远程验证 + 活性超时（本期不做）。

## 6. DSN-57 已回退（实验回归）
- 现象：rollback+unfinished 双双上升，simNum 级改 txHash 级仍无效。
- 根因：跨分片链式 tx 的下游在“非相关分片”验证时本地并无上游 → fail-closed 误回滚 / internal_pool 误扣留。
- 已回退 verify fail-closed、internal_pool 就绪门及其接线/谓词；恢复“查不到上游就跳过”原语义。go build 通过。
- 保留：DSN-56(去 ChainPatch) + 断链修复(UpstreamTxList 上链)。DSN-57 文档标记 REVERTED 并给出重设计方向。
