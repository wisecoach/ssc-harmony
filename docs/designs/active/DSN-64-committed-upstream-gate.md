# DSN-64: 记录“已 CR Commit 的上游”，放行其晚到的下游 D（解决生命周期错配误回滚）

> 状态：**design + 已实现（待实验验证）**
> 关联：DSN-52/56/57/58/60/62/63。来源：对 `upstream patch not on-chain yet` 残留(≈2652)的单 tx 轨迹定位。
>
> **一句话**：U 的 patch 不是“没注册/注册慢”，而是 **U 快速 commit 后被 `RemoveOnChainDAGPatch`
> 清掉临时 patch**，而一批下游 D 是 U 还在 in-flight 时被救援创建、引用 U，**D 真正 verify 时 U 早已
> commit+被移除** → verify 反查 onChainDAGPatches 找不到 U → DSN-57 误回滚。修法：在 InternalPool/
> retryScheduler 里**持久记录“已上链 commit 的交易”**，`upstreamOnChain` 对“U 已 commit”也判就绪，
> 放 D 上链去读 U 已落盘的最终值。

## 0. 定位（单 tx 轨迹证据）
U=0x9b54e406…（shard 0）轨迹：
- 23:53:17.63  simTxSubmit（block 11）
- 23:53:18.95  verify 成功 → AddOnChainDAGPatch 导入（block 13）——**只隔 ~1 块，注册很快**
- 23:53:21.17  commit with proof → CXT_COMMITTED → **RemoveOnChainDAGPatch 把 U 临时 patch 删除**
- 23:53:21.24 / 22.99…  多笔下游 D 开始 verify、引用 U → U 已被移除 → DSN-57 回滚

大样本探针：D 回滚事件中，**U 早已 on-chain 且已被移除的占 ≥41%**（跨分片合并有噪声，故为下限）。

## 1. 根因与方向
- U 生命周期：SimTx submit → verify 成功(AddOnChainDAGPatch) → CR Commit → **RemoveOnChainDAGPatch**。
- D 的生命周期错配：D 在 U 的 onChainDAGPatch 还挂着时被“救援”创建（乐观依赖 U），但 **D 真正被提案/
  verify 往往晚于 U 的 CR Commit+Remove** → `upstreamOnChain(U)=false`（只查 onChainDAGPatches）。
- 但此时 U 已 commit：写集已落盘、**锁已释放**，D 完全可以直接读 U 的最终状态值，不应被挡。
- 方向：让"已 commit 的 U"在就绪谓词里返回 true，放行其下游 D。

## 2. 设计
- `retryScheduler` 新增持久集合 `committedOnChain`（txHash→struct{}）：
  - **写入**：Committer CR Commit 成功后回调 `MarkCommittedOnChain(txHash)`（仅 Commit，非 Rollback/
    ReleaseOnly）；新增 committer accessor 字段 `MarkCommittedOnChain` 并在 impl 接线到
    `retryScheduler.MarkCommittedOnChain`。
  - **读取**：`upstreamOnChain(txHash)` = txHash ∈ `onChainDAGPatches` **或** ∈ `committedOnChain`。
- 该谓词同时服务两处：
  - verify.go DSN-57 确定性判定（上游未就绪才回滚）→ 已 commit 的 U 不再触发误回滚；
  - internal_pool `filterReadySimTxs`（Task B 就绪门）→ D 不再被有界扣留 50 块。
- 计数值 `upstreamCommittedGate`（Info dump）用于观测门命中。

### 为什么正确（不与锁/一致性冲突）
- U 已 commit ⇒ U 对相关 key 的锁已释放、写值已在真实 stateDB；
- D 通过 DSN-57 门后，实际 verify 对 U 写出 key 的读，取的是 U commit 后的最终状态（无锁冲突），
  与 D 在救援时基于 U patch 构造的读一致；
- 只有当 U 既不在 onChainDAGPatches、也从未 commit（=真未就绪）时才拦 D/扣留/回滚——符合预期。

## 3. 风险
- `committedOnChain` 无界增长：内存随 commit 数增加。实验窗口可接受；长期需随交易 GC（如 tx 完全
  无人再引用后清理），本文暂不做，标记 TODO。
- 分片一致性：`upstreamOnChain` 仍是本分片本地判定；U 在本分片 commit 才算本分片就绪（跨分片靠
  DSN-58/CR，非本文范围）。

## 4. 改动清单（已实现）
- `ssc/retry_scheduler.go`：`committedOnChain` 字段、`MarkCommittedOnChain`、`upstreamOnChain` 判两处、
  计数 `upstreamCommittedGate`（dump）。
- `ssc/committer.go`：`CommitterStateAccessor` 加 `MarkCommittedOnChain`；CR Commit 成功后回调。
- `ssc/impl.go`：接线 `MarkCommittedOnChain` → `retryScheduler.MarkCommittedOnChain`。

## 5. 验收
同配置（rate=200/shard=4/delay=10/vpn=4）：
- `VerifySimulation: upstream patch not on-chain yet -> rollback` 在 ~2652 基础上**显著下降**；
- `upstreamCommittedGate > 0`（说明该门在触发）；
- commit_rate/rollback/unfinished 不劣化、`ring_detected` 保持 0。
