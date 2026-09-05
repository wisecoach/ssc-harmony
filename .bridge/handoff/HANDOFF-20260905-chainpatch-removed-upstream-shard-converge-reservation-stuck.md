# HANDOFF-20260905: ChainPatch 移除 + 断链修复 + 分片收敛已落地；下一卡点=reservation-skip→unfinished

> 交接给新 session。请先读：`docs/designs/active/DSN-56-remove-chainpatch-pure-dag-contract.md`
> （ChainPatch 已全删，契约权威）、`docs/designs/active/DSN-57-simtx-upstream-ready-admission.md`
> （REVERT 记录 + 分片收敛修正）。
> 本文件是“本次会话做到哪、下一卡点是什么、怎么继续”的索引。

## 1. 已确认修好（数据实证，rate=200）
- rollback 从 ~160-174 → **0-6**（几乎归零）。
- `upstream SimTx not on-chain in this shard` 从 2925 → **1**（absent 误回滚消除）。
- DAG 链路存活：`VerifySimulation: chain tx detected`=19417、`[vsCommit] VerifySimulation success`=100509。
- commit 稳定（~2000-3200/shard）。

## 2. 本 session 实际做的代码（相对交接时新增，全部 `go build ./ssc/... ./rpc/...` 通过）
A. **移除扁平 ChainPatch（DSN-56 完成）**
   - 删字段：`CXTSimulation.ChainPatch`、`RetrySignal.ChainPatch`、`SimulationState.ChainPatch`、
     `CXTSimulationState.ChainPatch/ChainPatchRef`（struct/Bytes/proto/convert，pb 已重生成）。
   - 删方法/路径：`SetChainPatch`/`readChainPatch`；`onChainPatches`→`onChainDAGPatches`，
     `*OnChainPatch`→`*OnChainDAGPatch`、`GetChainPatchRef`→`GetDAGNodeRef`；monitor/proto 统计改 `on_chain_dag_patches`。
   - SimTx 只带 `CallStates`(含自身 WriteSet) + `UpstreamTxList`；verify 用 `simOwnWriteSet(sim)` 由 CallStates 推导自身 WriteSet 导入 onChainDAGPatches。
B. **断链修复（UpstreamTxList 端到端）**
   - `retryCommit` Phase1b/2b 消费上游后 `offChainDAG.AddNode(txHash, simNum, nil, upstreamTxList)`（原来只 SetChainPatch）。
   - `RetrySignal` 新增并携带 `UpstreamTxList`；`HandleRetrySignal` 在 origin 落节点。
   - `sendChainSignal`/救援点 AddNode 只落上游边（own Patch=nil，由 CommitSimulation 覆盖）。
C. **分片收敛（DSN-57 修正，消除跨分片 absent）**
   - `retryScheduler.LocalUpstreamTxRef(txHash)`：只保留“本分片 offChainDAG 有节点”的上游；空→nil。
   - `impl.go CommitSimulation` 构建 SimTx 用 `LocalUpstreamTxRef`（每分片只带本分片上游）。
   - `HandleRetrySignal`：origin 只记录本分片本地存在的上游；无本地 → 不 markChain、按非链。
   - verify fail-closed：仅“本分片该管(queued/knownOffchain)却没上链”才回滚；`absent` 一律 **fail-open**。
D. **探针/诊断**（仍在代码里，可复用于下一 session）
   - retryScheduler：`upstreamOnChain`、`upstreamQueued`、`SetQueuedSimCheck`（internalPool.Has 接线）；
     `SigUpstreamNotReady` 计数。
   - internalPool：`Has(txHash)`。
   - internal_pool 的跨批就绪门 `filterReadySimTxs` 已删除（会扣死跨分片下游，不再用）。

## 3. 下一卡点（本 session 定位，尚未解决）
**症状**：unfinished 仍高（Shard1~2437、Shard3~1893）；`reservation skipped candidate`= **862,059**（巨量）。
**判读**：这是 OnBlockCommitted 的 reservation（同块同 key 只放一个、其余跳过，retry_scheduler.go ~892-931）
造成的“低优先/热 key 饥饿 + 反复顺延”，与交接文档中 STUCK=reservation-skip 一致。DSN-55 的
chain-ready 前插只给“已消费 DAG”的 tx 提权，没解决普通热 key 饿死。这是新 session 要挖的第二个卡点。

**别再用 ad-hoc grep 猜**：请在远端跑结构化统计（之前出 Shard dict 的脚本）+ `=== CHAIN_RETRY_STATS ===`
段，对照：`chainTxDetected` vs `chainReadyAdmission`（判链 vs 真被放行）、`retryCommitPatchHit/Miss`、
`retryCommitTryLockFail`、`chainTxCRCommitted`、per-shard commit/unfinished/rollback。

## 4. 保留/取舍
- 保留：DSN-56(去 ChainPatch) + 断链修复 + 分片收敛(fail-open absent)。实验证明这几项是净改善。
- 不做：internal_pool 按“上游上链才放行”扣留（会饿死跨分片下游）。
- 注意副作用待验证：origin 无本地节点被改成“非链”后，若 origin 确实依赖上游值会退回撞锁重试；
  若下轮 unfinished 里这类占比高，再按“本分片确实要读上游 key”更精细判链，而非一刀切。
