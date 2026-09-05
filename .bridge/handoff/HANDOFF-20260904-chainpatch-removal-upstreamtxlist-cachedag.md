# HANDOFF-20260904-清理 ChainPatch 扁平残留、统一走 DAG(UpstreamTxList)、缓存 DAG —— 交接给新 session

> 续接方向（用户澄清）：**方案早已是 DAG**（offChainDAG + `UpstreamTxList`），只是大量代码/字段/注释还叫“Chain / ChainPatch”。本次不是推倒重设计，而是：① 清掉 `ChainPatch` 这个**冗余扁平搬运字段**及字面残留，统一只走 DAG/`UpstreamTxList`；② 修复“RetryCommit 消费后没写上游边 → SimTx `UpstreamTxList` 为空 → Verify 不判 chain”的断链；③ 缓存 DAG。
> 前序：DSN-54(simDAGPatches)、DSN-55(rev1 admission → rev2 protectHeldLocks+RetryCommitDAG) 本 session 已实现，但落在错误层，端到端断点见 §2。

---

## 0. 一句话目标

让“DAG 救援 tx”真正端到端生效：**机制统一到 DAG/`UpstreamTxList`（方案已是 DAG），去掉冗余扁平的 `ChainPatch` 写集搬运，确保 SimTx 上链必然携带 `UpstreamTxList` → Verify 判 chain → 跳过锁冲突 → 走完 CR commit**。

---

## 1. 用户决策（勿再翻案）

| # | 决策 |
|---|---|
| D1 | **清掉冗余扁平 `ChainPatch` 搬运**（`CXTSimulation.ChainPatch`、`RetrySignal.ChainPatch`、`SimulationState.ChainPatch`、`SetChainPatch`/`readChainPatch` 等只用于把上游写集扁平搬到模拟/信号里的字段与路径），字面改名别再用“Chain”指代 DAG |
| D2 | **链式依赖只采用 `UpstreamTxList`**（DAG 边，`TxSimKey` 列表），作为“该 tx 的上游”唯一事实来源 |
| D3 | **缓存 DAG（offChainDAG）为唯一事实源**；构建 SimTx / Verify / 读上游值都从它按 `UpstreamTxList` 反查，不再依赖 `ChainPatch` 扁平搬运 |
| D4 | 保留 DSN-54 结论：**不引入扁平 SimPatch**；ForceSimulation 锁冲突 `ret.Err` 保留（救援信号） |
| D5 | DSN-55 rev1/rev2（admission 前插 / protectHeldLocks / RetryCommitDAG）**先标记为“错误层的补丁”，新 session 先判断是保留还是随重构回退**（见 §7） |

---

## 2. 真正的根因（本 session 已定位，勿再在 lock 层打转）

**现象**：unfinished 恒 ~40%；DAG-consumed tx 能完成模拟(≈91%)，但只有 ~61% close commit；stuck(~1100) 全是 reservation-skip。

**逐 tx 追踪**发现（DONE vs STUCK 样本对比）：

| | DONE | STUCK |
|---|---|---|
| Verify `chain tx detected` | 有 | **0** |
| Verify `mu conflict (write)` | 少 | 多 |
| `[vsCommit] VerifySimulation success` / CR commit | 有 | **0** |

**代码根因（verify.go:401）**：
```go
isChainTx := len(simulation.UpstreamTxList) > 0   // 只有带 UpstreamTxList 才跳过锁检查
```
而 SimTx 的 `UpstreamTxList` 在 `CommitSimulation`（impl.go:1017）里由 `GetUpstreamTxRef(txHash)` 得到，它读的是 `offChainDAG.nodes[txHash].UpstreamTxList`。

**断链点**：RetryCommit 消费 DAG 时**只“消费/补救”，从不把“该 tx 的上游边” AddNode 进 offChainDAG**（代码注释原话：“RetryCommit 只消费/补救，不构建新的上游边”）。因此：
- 到 CommitSimulation 构建 SimTx 时，`GetUpstreamTxRef(txHash)` 读不到本 tx 的上游 → `UpstreamTxList` 空；
- → 上链 Verify 见空 → 不判 chain → 撞真实上游锁 → `mu conflict` + 记 waitEdge（Wait-Die 已禁）→ 无 commit 票 → 永不 CR commit → 回 retryPool → reservation-skip 无限循环。

**本质**：旧设计用 `ChainPatch`（SetChainPatch 合并的扁平写集）来让下游模拟读上游值，**但没有把它转换成 DAG 上游边写进 SimTx**，导致 off-chain 救了、on-chain 不认。

---

## 3. “Chain/ChainPatch”字面与冗余扁平搬运落点盘点（清理范围）

> 说明：这些大多只是**历史命名**——`ChainNode`/`offChainDAG` 本就是 DAG 结构，真正需要**删除/改换**的是“把上游写集扁平搬运”的那份 `ChainPatch`，以及把 DAG 叫成 Chain 的字面。

代码内 161 处引用（含 proto 生成），非生成源约：
- `ssc/retry_scheduler.go` 41
- `ssc/patchpool.go` 32
- `ssc/simulator.go` 31
- `ssc/impl.go` 13
- `ssc/api/types.go` 12
- `ssc/verify.go` 6
- `ssc/committer.go` 4
- `ssc/api/monitor.go` 2、`ssc/api/sscs.go` 1
- 另 proto 生成（pb.go/grpc/convert）

关键字段/函数：
- `api.CXTSimulation.ChainPatch`（564）、`api.RetrySignal.ChainPatch`（1043）、`api.ChainNode`（1048）、`api.SimulationState.ChainPatch`(1198/1223)、`ChainPatchRef`(1226)
- `SetChainPatch` / `readChainPatch` / `AddOnChainPatch` / `GetOnChainPatch` / `RemoveOnChainPatch` / `ReadOnChainPatch` / `readOnChainPatchVisited`
- `SimState.ChainPatch` 直查路径（simulator.go GetState/SetState Step1）
- 监控 `onChainPatches` 计数等

目标：`ChainNode/offChainDAG`(本就是 DAG) 保留；删掉 `ChainPatch` 扁平字段与 SetChainPatch/readChainPatch 搬运路径；读上游值统一从缓存 DAG 按 `UpstreamTxList` 反查（复用 simDAGPatches 的 visited 防环反查模式）。

---

## 4. 新 session 必做（顺序建议）

1. **定基线**：确认当前未提交改动（DSN-54/55、RetryCommitDAG、protectHeldLocks、pb 重生成）是保留还是回退。若要“干净重构”，建议先 `git stash` 或起一个仅含 DAG 基础的分支，避免被锁层补丁干扰。
2. **设计确认**：去掉 `ChainPatch` 扁平字段后，链式模拟“读上游值”改由 `UpstreamTxList` + 缓存 DAG 反查（成员侧 simDAGPatches 已具备，可作模板）；DAG 仍是唯一方案。
3. **DAG 缓存落地**：确保 leader 侧 `offChainDAG` 是唯一事实源；下游 tx 的上游边（`UpstreamTxList`）在**它真正成为 DAG 救援时就被持久 AddNode/更新**（修复 §2 断链：RetryCommit 消费后把 upstream 写进 offChainDAG 节点）。
4. **CommitSimulation**：`simulation.UpstreamTxList = GetUpstreamTxRef(txHash)` 不再为空 → 移除对 `ChainPatch` merged 的依赖。
5. **Verify**：只以 `UpstreamTxList` 判 chain；删掉 `ChainPatch` 相关一致性检查残留。
6. **移除路径**：删字段/函数/监控，跑通 `go build ./ssc/... ./rpc/...` + DAG 单测。
7. **验证**：rate=200，确认 STUCK 样本的 SimTx 从此带 `UpstreamTxList` → Verify `chain tx detected` → CR commit；DAG-consumed→close commit 转换率上升、unfinished 下降。

---

## 5. 验证口径（沿用）

- `dagHoldProtected`/`chainReadyAdmission` 等 DSN-55 计数器若保留，观察其与 CR commit 是否相关；
- 主要看：stuck tx 的 Verify 是否开始 `chain tx detected`、`[vsCommit] VerifySimulation success`、`commit or rollback with proof`、`close commit:true`；
- 端到端：`simulation committed OK → CR committed → close` 转换率，unfinished 比例。

---

## 6. 相关文档 / 分支
- DSN-54（simDAGPatches）、DSN-55（rev2：得锁后提权保锁 + RetryCommitDAG）
- 本 session 根因分析记录（STUCK/DONE trace、§2）
- 前置：DSN-46/49/50/51/52/53
- 代码分支：`ssc_master_58aae5802`

---

## 7. 遗留判断（新 session 先定，勿无脑带/无脑删）

- DSN-55 rev1 admission 前插、rev2 `protectHeldLocks`+`RetryCommitDAG`：它们是为“得锁后不被 wound”加的。**如果 §2 的 UpstreamTxList 断链修好，chain tx 上链后天然不可 wound，这些锁层补丁可能就不必要**——建议在干净基线上只实现 §4 的 DAG/UpstreamTxList 方案，再评估是否需要保留任一锁层改动。
- simDAGPatches（DSN-54 成员广播/反查）与“缓存 DAG 反查”同构，可保留复用；但 `ChainPatch` 扁平字段应清。
