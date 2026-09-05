# DSN-57: 上游 SimTx 先于下游执行/注册，下游才放行（internal_pool 就绪门）

> 状态：**REVERTED**（实验回归：rollback+unfinished 上升，运行时改动已回退）。本文保留为“为何这条路不对”的记录与重设计基础。
> 关联：DSN-46/48/52/54/56。DSN-56 已把数据面清成纯 DAG/UpstreamTxList；
> 本文补齐“排序/就绪门”：保证下游 SimTx 验证时其上游已上链（同批先于下游，或先前已注册）。

## 1. 要保证的不变量
对任何 SimTx D（`UpstreamTxList` 非空），D 在本分片被放行执行前：
- 其每个上游 SimTx U 已**在本批之前/先前块上链**（U 的 verify 已跑、成功则注册 onChainDAGPatches），或
- U 与 D 同批且块内顺序保证 U 先执行（DSN-52 `orderSimTxsByDAG`），U 的 verify(导入) 先于 D 完成。

不允许 D 带着“上游还没上链/没注册”就放行。

## 2. 现状与缺口
- 已有：`internal_pool.orderSimTxsByDAG` 只解决同批内“U 在 D 前执行”（DSN-52）。
- 缺口：
  1. 跨批/跨块：U 不在 D 本批时，D 会被直接按优先级放行，而 U 的 patch 尚未注册 → D 验证时 `ReadOnChainDAGPatch(U)` found=false。
  2. verify 对 found=false **默认放行**（speculative），断链被掩盖。
- 本 DSN 只补这两点，不动 DSN-54/55 锁层。

## 3. 设计（限制发生在 internal_pool）

### 3.1 internal_pool 注入只读就绪谓词
给 `SSCInternalPool` 注入只读回调（由 sscService 接线到 retryScheduler）：
```
SimUpstreamOnChain func(upstreamTxHash common.Hash) bool // 该上游是否已在本分片上链并注册
```
池不碰 onChainDAGPatches 内部，只按谓词决定放不放行。

### 3.2 Extract 里的就绪门（SimTx 桶）
1. 先按现逻辑排序（确定性优先级 + `orderSimTxsByDAG`）。
2. 计算可放行集（保持 DAG 序，只筛）：
   `ready(tx) = ∀ u∈UpstreamTxList: onChain(u) 或 (u 在本桶且 ready(u))`
3. 只把 **ready** 的放入本块输出（受 maxTotal 预算约束）；**not-ready 留在池中**等下块再评估
   （Extract 只“取走”返回的，未返回者自动保留）。not-ready 不占块预算（D1）。
4. 返回顺序沿用 DAG 序：同批里 U 在 D 前，执行层顺序消费 ⇒ U verify 先于 D。

### 3.3 verify fail-closed
D 是 chain tx，且某上游 U 在 onChainDAGPatches **未命中** → **不再默认放行**：
- 记为“上游未就绪/缺失”，本块不判成功，返回失败（走现有回滚/重试语义，D3）。
- internal_pool 就绪门正常会扣住 D，此分支是“门失效/竞态”的兜底与观测点。

### 3.4 回滚传播（不需要专门逃生口）
- 用户裁定：上游**只要上链即可**，不要求提交成功。
- U 成功 → 注册，D 读到 patch，正常。
- U rollback/失败 → 未注册 → D 执行到它因缺上游 patch **报错回滚**，回滚自然传播。
- 即 3.3 的 fail-closed 承载此语义，无需追踪上游终结状态（D2）。

### 3.5 活性（本期不做）
- “上游一直进不了 InternalPool / 永远不就绪”的超时活性机制本期不做（实验环境通常不出现）（D5）。
- 留作未来：为长期 not-ready 的下游加超时，或按“上游永不出现”回退。

## 4. 决策点（用户已裁定）
- D1（预算）：not-ready 不占块预算，只放行 ready 并对其计预算。
- D2（上游终结判定）：不需要——依赖 fail-closed + 回滚传播，不做专门终结追踪。
- D3（verify 失败）：fail-closed = 缺上游 patch → 报错回滚（复用现有回滚/重试语义），不再静默放行。
- D4（就绪谓词）：只查本分片 onChainDAGPatches；跨分片各自独立，靠 SimTx 多播 + 同批顺序满足。
- D5（活性超时）：本期不做，见 3.5。

## 5. 实现位置
- `ssc/internal_pool.go`：`SSCInternalPool` 增加就绪谓词字段 + `SetSimUpstreamOnChain`；`Extract` 的 SimTx 桶在
  `orderSimTxsByDAG` 后做就绪筛选（只放行 ready，not-ready 留池、不占预算）。
- `ssc/impl.go`：构造 sscService 时把谓词接上（查 retryScheduler.onChainDAGPatches 是否含该上游）。
- `ssc/verify.go`：链式一致性分支 fail-closed（缺上游 patch → 失败回滚）。
- `go build ./ssc/... ./rpc/...` 每步验证。

## 6. 验收
- rate=200：SimTx 不再在“上游未上链/未注册”时放行；verify 不再 found=false 静默放行；
  上游 rollback 时下游报错回滚；DAG 救援 close commit 转换率、unfinished。
- 监控：就绪放行 / not-ready 扣留 计数。


## 7. 实现进度
- [done] internal_pool.go：`SSCInternalPool.simUpstreamOnChain` 谓词 + `SetSimUpstreamOnChain`；
  `Extract` 的 SimTx 桶在 `orderSimTxsByDAG` 后调用 `filterReadySimTxs`（not-ready 留池、不占预算）。
- [done] retry_scheduler.go：`upstreamOnChain(txHash)`（查本分片 onChainDAGPatches 是否注册）。
- [done] factory.go：`NewService` 注入后给 internalPool 接上谓词（闭包到 retryScheduler）。
- [done] verify.go：链式 SimTx 在验证前检查每个上游已注册（`GetOnChainDAGPatch`）；缺失 → 发回滚票（fail-closed）。
- [done] 监控计数 `SigUpstreamNotReady`。`go build ./ssc/... ./rpc/...` 通过。
- [note] 就绪谓词按 txHash 判定；verify fail-closed 按 (txHash,simNum) 判定——若 D 引用的是上游的旧 simNum，
  会在 verify 处回滚（符合“回滚传播”）。如需更早拦截可后续把谓词也做成 simNum 感知。
- [todo] 远程验证：rate=200；观察 SigUpstreamNotReady / not-ready 扣留；unfinished、close commit 转换率。


## 8. REVERT 记录（重要教训）
- 实现后 rate=200 实验：rollback 与 unfinished 双双上升，且把 verify fail-closed 由 simNum 级放宽到 txHash 级仍“还是不行”。
- 根因判断：本门强制“**下游在其每个分片验证时，上游都必须已在本分片 onChainDAGPatches**”——但链式 tx 是**跨分片**的，
  上游 SimTx 只落在“真正相关”的分片；下游在其它分片验证时本地并无该上游 → fail-closed 误回滚、internal_pool 误扣留。
- 已回退：verify.go 的 fail-closed 块、internal_pool 的 filterReadySimTxs/谓词/接线、upstreamOnChain、SigUpstreamNotReady。
  恢复为“ReadOnChainDAGPatch 查不到就跳过”的原语义。go build ./ssc/... ./rpc/... 通过。
- 保留且被证明确实有用的部分（不受本次回退影响）：DSN-56 移除 ChainPatch、断链修复(RetrySignal/RetryCommit 带 UpstreamTxList)。
- 重设计方向（勿重复此坑）：
  1. 就绪/一致性判断**只在下游真正依赖上游的那个分片**做，而不是“每个分片都要求上游注册”。
  2. 同分片内的“上游先于下游”已由 orderSimTxsByDAG 保证（块内串行 verify）；跨分片不要用“本分片必见上游”来判死。
  3. 先量测：到底有多少未完成/回滚来自“跨分片上游未到本分片”vs“真冲突”，再决定要不要任何新门。
