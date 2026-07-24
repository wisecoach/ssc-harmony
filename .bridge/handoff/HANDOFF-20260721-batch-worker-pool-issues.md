# HANDOFF-20260721-batch-worker-pool-issues

> from_session: 20260721_164402_a74976
> from_role: Designer
> to_role: Designer (新 session)
> 焦点: DSN-41 Batch Worker Pool 实现后的三大问题

## 完成内容

| 事项 | 状态 |
|:-----|:------|
| DSN-41 设计文档 | ✅ 已更新到 v2（batch verify 在 CommitSSCTransactions 之前执行） |
| ssc/api/types.go — 配置字段 | ✅ SimTxParallelism, CallStateParallelism, MaxSimTxPerBlock, MaxBatchTimeMs |
| ssc/api/sscs.go — 接口变更 | ✅ BatchVerifySimulations 返回 int（投递数） |
| ssc/verify.go — worker pool + verifyOneSimTx + CallState 内并行 | ✅ 已实现 |
| core/state_processor.go — doBatchVerify 传 remainingTime | ✅ |
| node/worker/worker.go — 从 pendingSimTxs 反序列化 + 先 batch verify 后提交 | ✅ |
| cmd/build_keys/build_keys.go — 配置 | ✅ SimTxParallelism=32, CallStateParallelism=0, MaxSimTxPerBlock=500, MaxBatchTimeMs=800 |
| 实验验证 | 🔴 三个问题未解决（见下） |

## 实验数据（RATE=100，最新实验）

| Shard | commit | unfinished | retry_limit_exceeded | timeout | rollback |
|:-----:|:------:|:----------:|:--------------------:|:-------:|:--------:|
| 0 | 73 | 1,422 | 236 | 333 | 0 |
| 1 | 54 | 2,185 | 537 | 94 | 2 |
| 2 | 125 | 1,057 | 388 | 120 | 0 |
| 3 | 335 | 1,796 | 708 | 533 | 1 |

## 当前待解决问题

### 问题 1：execFailed 率极高（50%+）

`verifyOneSimTx` 中 `verifyExecuteForCallState` 大量失败。日志显示 `execFailed` 经常超过 submitted 的 50%。

**当前行为**：execFailed 的 SimTx 也被提交上链（worker 处理过就提交），但发了 rollback vote → 触发 retry → 再次 execFailed → `retry_limit_exceeded`。

**待决策**：是否只提交 exec 成功的 SimTx？还是修复 execFailed 的根因？

### 问题 2：`pendingSimTxs` map 遍历顺序问题

`filteredSimTxs` 从 `pendingSimTxs` map 取前 `submitted` 个时，Go map 遍历随机。如果某 sender 的低 nonce SimTx 没被取到（被另一个 sender 占用了前 submitted 个位置），高 nonce 的永远无法提交。

**待决策**：是否需要按 nonce 全局排序后再切分？

### 问题 3：`simTxn` 统计未更新

`CommitSSCTransactions` 后只更新了 `committedCount`，没有更新 `simTxn` 变量（从 365 行的 `simTxn := 0` 改为了 `committedCount`）。日志中 `simTxn=0` 只是统计问题，不影响实际执行。待修复。

## 代码改动清单

| 文件 | 改动 |
|:-----|:------|
| `ssc/api/types.go` | TimeoutConfig 加 4 个配置字段 |
| `ssc/api/sscs.go` | BatchVerifySimulations 签名：加 remainingTime, 返回 int |
| `ssc/verify.go` | BatchVerifySimulations 改为 worker pool 模式 + verifyOneSimTx + lockStates + CallState 并行 |
| `node/worker/worker.go` | SimTx 处理段改为：反序列化 → batch verify → 只提交 submitted 个 |
| `core/state_processor.go` | doBatchVerify 传 remainingTime（10s 兜底） |
| `cmd/build_keys/build_keys.go` | 配置新字段 |

## 参考文档

| 路径 | 说明 |
|:-----|:------|
| `docs/designs/planned/DSN-41-batch-worker-pool.md` | 设计文档 v2 |
| `docs/issues/BUG-09-batch-verify-process-unfinished.md` | 原始 Bug（batch verify 导致产块退化） |
| `docs/issues/BUG-10-phase0.5-arbitration-timeout.md` | Phase 0.5 仲裁超时导致全部 timeout |

## 已知坑

- `pendingSimTxs` 是 `map[common.Address]types.Transactions`，for range 遍历无序→nonce 不连续
- `simTxn` 统计在改为 committedCount 后未同步更新日志输出
- worker pool 的 deadline 从仲裁结束后算，仲裁本身不受 deadline 控制
- `remaining` 初始值 500 可能被 CR Tx 消耗，导致 SimTx 分配到 0
