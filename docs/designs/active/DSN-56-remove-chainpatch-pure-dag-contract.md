# DSN-56: 移除扁平 ChainPatch —— 纯 DAG / UpstreamTxList 契约（权威定义）

> 状态：active（本设计**废除 ChainPatch 作为任何数据载体**，是后续所有改动的唯一契约）
> 关联：DSN-46/49/50/51/54/55；取代旧 `PatchPool`/`ChainPatch` 全部遗留
> 为什么要这份文档：ChainPatch 扁平写集搬运与 DAG/UpstreamTxList 长期混用、且无契约，
> 反复导致实现者(AI/人)在错误层打补丁。本文以“只有一套机制”的权威定义收口。

---

## 1. 一句话结论

**链式救援里，patch 的“内容/来源”只存在 DAG 节点里，tx 之间只认 `UpstreamTxList`（DAG 上游边）。**
任何 `ChainPatch`、`SetChainPatch`、`onChainPatches` 等“把上游写集压平成一份 RWSet 搬运/直查”的字段与路径一律删除，不再作为事实来源。

## 2. 术语与唯一事实源（禁止再造新词）

| 名称 | 是什么 | 谁持有 | 何时写入 |
|---|---|---|---|
| **offChainDAG** | 链下 DAG，`nodes[txHash]` = { own WriteSet(Patch), UpstreamTxList, Depth } | 分片 leader | ① tx 的 SimTx 被构建/提交时 `AddNode`(own write + 上游边)；② DAG 救援消费上游后 `AddNode`(上游边) |
| **simDAGPatches** | offChainDAG 的**成员侧只读镜像**（leader 广播的上游子图） | 每个成员 | leader 在 RetryCommit 消费上游、返回 Locked 前广播 |
| **UpstreamTxList** | 一笔 tx 的所有上游 SimTx 列表（DAG 边）。SimTx/RetrySignal 上链/跨 shard 的唯一上游索引 | SimTx、RetrySignal | 由 offChainDAG 节点推导/随信号传播 |
| ~~ChainPatch~~ | ~~合并后的扁平上游写集，随 SimTx/RetrySignal/SimState 搬运~~ | — | **删除，本 DSN 后禁止再出现** |

**约束（写进 code review 检查项）**：
- 禁止在 `CXTSimulation` / `RetrySignal` / `(CXTSimulation)State` / `api.ChainNode` 上新增或保留 `ChainPatch` 语义字段。
- 禁止 `SetChainPatch` / `readChainPatch` / `AddOnChainPatch` / `GetOnChainPatch` / `RemoveOnChainPatch` / `ReadOnChainPatch` 及其 `onChainPatches` 缓存。
- 读上游产生的值 = 只从 DAG 按 `(txHash, simNum)` 反查：leader 查 `offChainDAG`，成员查 `simDAGPatches`。

## 3. 为什么要能成立（依赖前置，缺一不可）

1. **成员侧必须能拿到上游写集**，且来源是 DAG 镜像而非扁平搬运：
   - leader 在“某 tx 消费上游、将成为 DAG 救援 tx”时，把该 tx 的**上游子图**（含每个上游节点的 WriteSet）广播给成员 → `simDAGPatches`。
   - 成员模拟/verify 需要上游值时按 `UpstreamTxList` 反查 `simDAGPatches`（已有 visited 防环，DSN-54 模板）。
2. **“下游 Verify 排在上游之后”的就绪门**（现有缺口，必须显式化）：
   - 语义：下游 SimTx 只有在**上游 SimTx 的 patch 已进入可被 on-chain 确认的状态**后才上链 Verify；否则 Verify 对上游值查不到时**不得因“链式标记”盲目放行**（现状是 found=false 直接跳过）。
   - 具体做法（后续子 DSN/实现）二选一或组合：admission 阶段要求“上游已 commit/已广播 simDAGPatch”才放行；Verify 对 `simDAGPatches`/offChainDAG 查不到上游节点时按“上游未就绪”处理（等待/回退/不判成功），而不是默认成功。

## 4. 迁移顺序（删除即替换，非先删后补）

> 代码上不存在“先删字段、后接 DAG”的中间态（会编不过或丢数据）。正确做法：**整条 read/verify 路径一次切到 DAG，再删字段**，但全程只以“去除 ChainPatch 机制”为目标、不新增任何扁平用法。

1. **simulator 读路径**：`GetState/SetState/IsKeyAvailable` 的 Step1/补救，去掉 `SimState.ChainPatch` 直查，改走 `offChainDAG`(leader)/`simDAGPatches`(成员) 反查；删除 `SetChainPatch`、`readChainPatch`。
2. **origin 侧节点落 DAG**：`RetrySignal` 只带 `UpstreamTxList`；`HandleRetrySignal` 按上游边 `AddNode` 进 origin `offChainDAG`（成员上游写集经 `simDAGPatches` 同步，不靠扁平写集）。
3. **verify**：`isChainTx` 只以 `UpstreamTxList` 判定（已如此）；去掉 `simulation.ChainPatch→AddOnChainPatch`；一致性检查改从 DAG(`simDAGPatches`)反查；删除 `onChainPatches`(Add/Get/Remove/Read) 及 committer/monitor 计数。
4. **SimTx 载体**：删 `CXTSimulation.ChainPatch`；自身 WriteSet 由 `CallStates` 推导或直接作为 offChainDAG 节点 Patch，不再随 SimTx 搬运。
5. **proto**：同步删字段并再生成（本机 protoc v3.21.12 / protoc-gen-go v1.36.11 已验证可复现）。
6. **验收**：rate=200，DAG 救援 tx 的 SimTx 带 `UpstreamTxList` → `chain tx detected` → CR commit；DAG-consumed→close 转换率、unfinished；确认无任何 `ChainPatch` 标识符残留。

## 5. 禁止名单（grep 基线，迁移完成后为 0）
`ChainPatch`, `SetChainPatch`, `readChainPatch`, `AddOnChainPatch`, `GetOnChainPatch`,
`RemoveOnChainPatch`, `ReadOnChainPatch`, `readOnChainPatchVisited`, `onChainPatches`,
`ChainPatchRef`, `ChainNode.Patch`(若仅承载扁平写集), `SimulationState.ChainPatch`,
`CXTSimulation.ChainPatch`, `RetrySignal.ChainPatch`。

## 进度日志（勿回退）
- [done] DSN-56 step 1（simulator 读路径）：移除 simulator 读路径的扁平 ChainPatch——
  `SimulationState.ChainPatch` 字段、`Simulator.SetChainPatch`、`readChainPatch`(改名 `readUpstreamValue`，
  只走 offChainDAG/simDAGPatches 反查)、GetState/SetState 的 SimState.ChainPatch 直查分支、
  `RetrySchedulerStateAccessor.SetChainPatch` 字段与 impl 接线、retryScheduler 3 处 `SetChainPatch` 调用。
  `go build ./ssc/... ./rpc/...` 通过。
- 注意：此改动后，重模拟读上游只依赖 offChainDAG(leader) + simDAGPatches(成员)；成员侧正确性依赖
  DSN-54 广播时序，需远程验证。
- [done] DSN-56 step 4 的 SimTx 载体部分：删除 `CXTSimulation.ChainPatch` 字段（struct/Bytes/proto 字段9/convert/impl 构造/verify 使用）。
  verifier 侧不再有扁平 ChainPatch：`verify.go` 验证成功后用 `simOwnWriteSet(simulation)`（由 CallStates 推导自身 WriteSet）导入 onChainDAGPatches，
  下游一致性检查按 `UpstreamTxList` 反查上游 patch（verify.go:491 `ReadOnChainPatch`）。`go build ./ssc/... ./rpc/...` 通过。
- [todo] 仍保留的 ChainPatch 残留（下一步，独立于 verifier 已清部分）：
  `RetrySignal.ChainPatch`（origin/sim 侧 merged 搬运，struct+proto 字段7+convert+sendChainSignal/HandleRetrySignal/retryCommit 的 merged AddNode）、
  `CXTSimulationState.ChainPatch/ChainPatchRef`（vestigial）、`onChainPatches` 存储与其方法名(AddOnChainPatch 等)与 monitor 计数。
  建议：把 `onChainPatches` 语义改名为 `onChainDAGPatches`（保留为合法 verifier 存储），`RetrySignal.ChainPatch` 删除、改由 UpstreamTxList+simDAGPatches 承载。
- [done] 全量移除扁平 ChainPatch（DSN-56 完成）：
  - `RetrySignal.ChainPatch`、`CXTSimulationState.ChainPatch/ChainPatchRef`、`SimulationState.ChainPatch`、
    `CXTSimulation.ChainPatch` 全部删除（struct/Bytes/proto/convert）。
  - `onChainPatches` 存储与方法改名 `onChainDAGPatches`/`*OnChainDAGPatch`；monitor/proto 统计改名 on_chain_dag_patches。
  - 消费上游不再 merged 成扁平写集：retryCommit Phase1b/2b、scanPatchSubscribers/sendChainSignal、HandleRetrySignal
    的 AddNode 一律 own Patch=nil + 只落 UpstreamTxList（被救援 tx 尚未产出写集，由 CommitSimulation 以自身 WriteSet 覆盖）。
  - simulator 读上游只走 readUpstreamValue（offChainDAG/simDAGPatches），无扁平直查。
  - `go build ./ssc/... ./rpc/...` 通过；ssc 内已无 `ChainPatch` 标识符（仅剩“已移除”说明注释）。
- [next] 排序/就绪门（独立课题，另立文档）：保证“上游 SimTx 先执行/先导入 onChainDAGPatches，再验证下游”，
  并把 verify 对上游 patch 未命中(found=false)的处理从“默认放行”改为“未就绪→等待/回退”，消除 speculative 放行。
