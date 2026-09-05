# DSN-60: Internal Pool 保证 SimTx 序“上游先于下游”（降低“找不到 Patch → Rollback”）

> 状态：**design（定稿）+ Task B 首版已实现，待实验验证**
> 关联：DSN-52/53/54/55/56/57/58。来源：HANDOFF-20260905 的 **Task B**。
> 历史：DSN-59 曾把 Task A（“允许下游读写上游/续链”)与 Task B 并列为两条路；经评估 **Task A /
> patchpool 续链方案不可靠，已放弃**（见 §0），本文只保留 **Task B：在 Internal Pool 里保证 SimTx
> “上游先于下游”，压低“上游 patch 未上链 → verify rollback”**。
>
> **一句话**：verify 已按确定性 fail-closed（上游不在 on-chain → rollback）执行；本 DSN 让 leader 在
> internal_pool `Extract` 阶段，对**本分片本地链式** SimTx 做**有界**“上游先于下游”就绪门——上游同批或已
> on-chain 才放行，否则有界扣留（不放入本块、不占预算），达阈值后强制放行走 fail-closed 回滚——既压
> rollback，又绝不把跨分片下游无限扣死（DSN-57 教训）。

## 0. 为什么放弃 Task A / patchpool 续链（决策记录）
- 原 Task A 想“让更多同 key 下游能在同块连续落多笔(续链)”来提速；实测链条浅(depth 2–3)、
  `dagPerBlock maxSameKeyWrites` 多数块=1。
- 深挖后发现瓶颈主要是**块级串行**：每个下游 hop 的 SimTx 必须等上一 hop 在本分片 verify/on-chain
  后才能成为可续接节点 → 天然近似“每块一个 key 写一次”，不是匹配键 Read∪Write 的问题。
- patchpool 匹配区有明确历史坑（曾因“完整 Read∪Write 覆盖”让热 key 交易凑不齐覆盖集而饿死；
  也曾误把“每笔 SimTx 自带 WriteSet”当链式导致锁检测失效）。据此判断**patchpool 行为级改动不可靠**，
  放弃 Task A/续链，聚焦 Task B（不依赖 patchpool 匹配的行为改动，纯 internal_pool 就绪门）。

## 1. 目标与不变量
让链式 SimTx D（`UpstreamTxList` 非空，且其上游为**本分片本地依赖**）在被放行进块验证时，其本地上游
U 已具备“先于 D”的条件：
- U 与 D **同批** → 块内顺序由 `orderSimTxsByDAG` 保证 U 先执行（U verify 后 on-chain 再轮到 D）；
- 或 U **已 on-chain**（`onChainDAGPatches` 已注册，来自更早块）；
- 否则 D **not-ready**，在 internal_pool 被有界扣留，不放入本块。

验收目标：`VerifySimulation: upstream patch not on-chain yet -> rollback`（`SigUpstreamNotReady`）
计数下降，且 `unfinished/rollback` 不得上升、`ring_detected` 保持 0。

## 2. 现状与缺口（已核实代码）
- `ssc/internal_pool.go` `Extract`：SimTx 桶只做**同批** `orderSimTxsByDAG`；跨批/跨块/跨分片的
  上游就绪没有门。缺口：D 引用的本地上游 U 尚未 submit 到本分片池、也未 on-chain 时，D 会被照常放行
  → verify 时 `upstreamOnChain(U)=false` → 确定性 rollback（`SigUpstreamNotReady`）。
- `ssc/verify.go`：确定性 fail-closed 用 `upstreamOnChain`（txHash 级）判定，缺即回滚（保留）。
- `ssc/retry_scheduler.go`：已有 `upstreamOnChain`（on-chain 注册）与 `upstreamQueued`/`SetQueuedSimCheck`
  （是否在池排队）；`LocalUpstreamTxRef` 已把每个分片的 SimTx 上游收敛为“本分片确有节点”的本地依赖
  （`CommitSimulation` 填 `UpstreamTxList` 时使用）。
- DSN-57 教训：曾按“上游已上链才放行”在 internal_pool 扣留 not-ready 下游 → 饿死**跨分片**下游回归并
  REVERT。根因是它对**所有** related 分片都要求本地见上游；而链式 tx 的上游只落在真正相关分片。

## 3. 设计：有界、仅限本地依赖的就绪门
在 internal_pool 的 SimTx 桶，`orderSimTxsByDAG` 之后加 `filterReadySimTxs`：
1. 对每个 SimTx D，只检查 `D.UpstreamTxList`（已由 LocalUpstreamTxRef 收敛为**本分片本地**依赖）。
2. 就绪判据（`simTxLocalDepsReady`）：
   - U ∈ 本批(inRow) → ready（同批 DAG 序已保证 U 先于 D）；
   - 否则 `upstreamOnChain(U)` → ready；
   - 否则 not-ready。
3. not-ready 的 D：不入 `out`（不放入本块、不占预算），留在池中；累计 `notReadyHoldCnt[D]`。
4. **有界**：`notReadyHoldCnt[D] >= simNotReadyHoldBlocks(50)` 时强制放行一次（清计数）。
   若本地上游真缺失/已终局，D 走 verify fail-closed 回滚（正确传播）；绝不无限扣留。
5. 谓词由 factory 注入：`internalPool.SetSimUpstreamOnChain(retryScheduler.upstreamOnChain)`。
   **只查本分片 onChainDAGPatches**，不做任何“跨分片必见上游”判定 → 从机制上避开 DSN-57 饿死。
6. 清理：D 被放行/移除/落块时清除其 `notReadyHoldCnt`。

### 为什么不会重蹈 DSN-57
- DSN-57 扣的是“**每个分片**都要见上游”，跨分片下游在某分片没有该上游 → 永远 not-ready → 饿死。
- 本设计只扣“**本分片本地**依赖”的下游：`UpstreamTxList` 已收敛，且本地依赖是真实、应会到达的
  （同一 rescue 链上的 U 在该分片确有节点）。即便真缺失，50 块上限保证最终放行回滚，不会无限滞留。

## 4. 落点（Task B 首版已实现）
- `ssc/internal_pool.go`：
  - 字段 `simUpstreamOnChain func(common.Hash) bool`、`notReadyHoldCnt map[hash]int64`；
  - `SetSimUpstreamOnChain`、`filterReadySimTxs`、`simTxLocalDepsReady`；
  - `Extract` 的 SimTx 桶在 `orderSimTxsByDAG` 后调 `filterReadySimTxs`；
  - `Remove`/`OnBlockCommitted` 清理 `notReadyHoldCnt`；
  - 常量 `simNotReadyHoldBlocks = 50`。
- `ssc/factory.go`：接线 `internalPool.SetSimUpstreamOnChain(baseService.retryScheduler.upstreamOnChain)`。
- 单测：`ssc/internal_pool_task_b_test.go`（就绪判定 / 有界扣留后放行 / DAG 序后筛选）。
  ⚠️ `ssc` 测试包因既有 `simulation_*_test.go` 引用已删除字段（Simulator 重构）而无法整体编译，
  单测待既有破损修复后执行；本次源码 `go build ./ssc/... ./rpc/...` 通过、gofmt 干净。

## 5. 观测 / 验收
- 主指标：`SigUpstreamNotReady`（“upstream patch not on-chain yet -> rollback”）**下降**。
- 回归护栏：`unfinished / rollback` 不得上升；`ring_detected` 保持 0；`retryPool` 不冻结。
- 若 `SigUpstreamNotReady` 原本就接近 0（上游缺失并非 rollback 主因），则本门影响有限，需回看
  是否真冲突主导（届时转争用削减，不扩大本门）。

## 6. 风险 / 说明
1. 有界扣留会把“本地上游即将到达”的 D 延后最多 ~50 块；若本地上游常因 wound/回滚而不出现，
   会在 50 块后照旧回滚（无净收益甚至略增延迟）——用实验观察 `SigUpstreamNotReady` 与 rollback 变化判断。
2. 只动 internal_pool Extract（leader 侧块构建），不改 verify/共识判定，validator 侧确定性不变。
3. 不引入 patchpool 匹配行为改动；DAG 匹配仍走原有阻塞 key 覆盖逻辑。

## 7. 关联
- HANDOFF-20260905 Task B；DSN-57（为何 internal_pool 就绪门曾 REVERT、教训）；DSN-58（reservation
  反饥饿的有界思想，`simNotReadyHoldBlocks` 对齐其 50 块）；DSN-52（同批 DAG 序）。
