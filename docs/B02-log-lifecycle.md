# [B02] 交易生命周期日志大全

> **阅读顺序**（`[前缀]` 表示阅读顺序）：
> 1. 📄 `docs/A01-sscc-refactor-plan.md` [A01] — SSCC 模块化重构总览
> 2. 📄 `docs/A02-lock-retry-mechanism.md` [A02] — 锁与重试机制
> 3. 📄 `docs/A03-CR-priority-optimization.md` [A03] — CR 提交优先级优化
> 4. 📄 `docs/A04-onchain-retry-limit.md` [A04] — 链上重试次数限制
> 5. 📄 `docs/A05-hotkey-retry-design.md` [A05] — HotKey 链式重试（数据层）
> 6. 📄 `docs/A06-lock-priority-coordination.md` [A06] — 锁优先级协调（协调层）
>
> **运维辅助**（与主线并行的独立文档）：
> - 📄 `docs/B01-log-query-guide.md` [B01] — 日志查询指南
> - 📄 `docs/B02-log-lifecycle.md` [B02] **（本文）** — 交易生命周期日志大全

> 基于 `ssc/` 目录代码整理，按交易从提交到终结的完整路径排序。

---

## Phase 1：交易提交 & 入池

| 日志 | 等级 | 含义 |
|------|------|------|
| `add a cross shard Tx, preCompiled: false, nonce=X` | `info` | 交易被加入 tx pool，分配 nonce |
| `[SimulationTx] submitting` | `info` | CR submitter 提交 simulation 交易到链上 |
| `[CRSimulationTx] submitting with crSigner` | `info` | CR submitter 提交 CR（commit/rollback）交易 |
| `successfully submitted %s, nonce=%d` | `info` | 签好名提交到本地 tx pool 成功 |
| `nonce too low, retry with nonce+1` | `warn` | nonce 过期，递增重试 |
| `nonce too high, retry with nonce-1` | `warn` | nonce 跳号，递减重试 |
| `failed to submit %s after %d retries` | `error` | 提交失败，已达最大重试次数 |

---

## Phase 2：Simulation（模拟执行）

### 2a. 启动模拟

| 日志 | 等级 | 含义 |
|------|------|------|
| `start simulate cx transaction, start` | `info` | leader 收到 simulation 请求，开始处理 |
| `found pending CXT request` | `info` | 有排队的 CXT 请求 |
| `processing pending CXT request` | `info` | 开始处理排队请求 |
| `start simulation 1.4` | `info` | 开始模拟执行（1.4 阶段） |
| `simulation has been started, simulationNum=X` | `info` | 模拟已开始，记录当前模拟轮次 |
| `the context has been started, currentSimulationNum X, newSimulationNum Y` | `info` | 上下文已启动，从旧轮次推进到新轮次 |
| `extend from origin request, simulationNum X` | `info` | 基于原始请求扩展模拟 |
| `startCXT for tx X, simulationNum Y, FromShard Z` | `info` | 发起 CXT 跨分片调用 |
| `callIndex has been started` | `debug` | 某个 callIndex 已开始执行 |

### 2b. 跨分片调用

| 日志 | 等级 | 含义 |
|------|------|------|
| `handle cxt call, start` | `info` | 处理跨分片调用请求 |
| `add callState, simulationNum=X, callIndex=Y` | `info` | 添加 callState（跨分片调用状态）到交易 |
| `callState synced` | `debug` | 跨分片调用状态同步完成 |
| `callStack.Pop() returned nil, execution successfully` | `info` | 所有跨分片调用完成，执行成功 |
| `call contract with %s` | `debug` | 执行合约 |
| `return locked by other tx` | `debug` | 执行合约时发现 key 被其他交易锁住 |

### 2c. 模拟完成 & 结果汇总

| 日志 | 等级 | 含义 |
|------|------|------|
| `simulation result received, including self` | `info` | 所有分片模拟结果已到达 |
| `failed to simulate transaction` | `error` | 模拟失败 |
| `simulation accomplished, send simulation commit, commit type=%v, status=%v, simulationNum=%d` | `info`/`error` | 模拟完成。commitType=true=可提交，status=LockConflict/ExecutionFailed |

### 2d. TempLockView 预检查

| 日志 | 等级 | 含义 |
|------|------|------|
| `TempLockView pre-check failed, deferring to retry pool` | `warn` | leader 侧 simulation 完成后，TempLockView.TryLock 预检失败。不进链上，直接进 retry pool |
| `retry commit, locked: %v` | `info` | retry 重试时 TryLock 结果 |

---

## Phase 3：CommitSimulation（模拟结果上链）

### 3a. 提交

| 日志 | 等级 | 含义 |
|------|------|------|
| `handle simulate request, start` | `info` | leader 收到 commit simulation 请求，开始执行 |
| `begin to build signatures for simulation` | `debug` | 收集各节点签名 |
| `simulation committed` | `info` | simulation 交易成功提交到链上 |
| `failed to submit simulation tx` | `error` | simulation 交易提交失败 |

### 3b. 结果处理

| 日志 | 等级 | 含义 |
|------|------|------|
| `simulation committed, status=%s` | `info` | simulation commit 成功，status=OK/LockConflict/ExecutionFailed/PoolTimeout |
| `simulation failed, status=%s` | `error` | simulation 失败 |
| `simulation is pool timeout` | `error` | pool 定时器超时，交易强制结束 |

---

## Phase 4：VerifySimulation（链上验证执行）

### 4a. 执行验证

| 日志 | 等级 | 含义 |
|------|------|------|
| `begin to verify simulation` | `info` | 开始验证模拟结果（在 CR 合约中执行） |
| `verify simulation finished` | `info` | 验证完成 |
| `verify callState failed` | `error` | 验证 callState 失败 |
| `failed to verify execution for call state` | `error` | 合约执行验证失败 |

### 4b. 验证结果分发

| 日志 | 等级 | 含义 |
|------|------|------|
| `simulation is valid, mu the rwset and send commit vote` | `info` | 验证通过，上链上锁，发 Commit vote |
| `send CXTCommitVote to %s, type=Commit` | `info` | 发出 Commit vote |
| `send CXTCommitVote to %s, type=Rollback` | `info` | 发出 Rollback vote |
| `send CXTCommitVote to %s, type=Recall` | `info` | 发出 Recall vote（读集不一致，需重新模拟） |
| `attempted to retry but exceeded max retries, send rollback vote` | `info` | 重试超限，发 Rollback vote |

---

## Phase 5：Vote 收集 & SSC 聚合

| 日志 | 等级 | 含义 |
|------|------|------|
| `handle commit vote, type=X` | `info` | 收到一个 commit vote |
| `handle rollback vote` | `info` | 收到一个 rollback vote |
| `received %s vote from shard %d, [%d/%d]` | `info` | 统计某个分片的 vote 收集进度（已有/阈值） |
| `reach threshold for commit votes from shard %d, begin to aggregate ssc commit vote` | `info` | 某个分片的 vote 达到 2/3 阈值 |
| `send SSC commit vote to leader` | `info` | 聚合后发给 leader 做跨分片 SSC 聚合 |
| `received %s ssc vote [%d/%d], %d in %v` | `info` | leader 收到 SSC vote |
| `receive all related ssc votes, commit type: %v, reason: %v` | `info` | **全部相关分片 SSC vote 聚合完成！可出最终决定 ** |

---

## Phase 6：CommitOrRollback（最终决定）

| 日志 | 等级 | 含义 |
|------|------|------|
| `commit or rollback with proof` | `info` | 收到最终 commit/rollback proof，开始执行 |
| `commit with proof, origin: %v` | `info` | 最终决：**Commit** |
| `rollback with proof, origin: %v, reason: %s` | `info` | 最终决：**Rollback** |
| `failed to commit tx with proof` | `error` | Commit 执行失败 |
| `failed to rollback tx with proof` | `error` | Rollback 执行失败 |
| `handle commit or rollback proof, type=%v` | `debug` | CR submitter 层处理 proof |

---

## Phase 7：Close（交易终结）

| 日志 | 等级 | 含义 |
|------|------|------|
| `leader close transaction, commit: %v, status: %s, reason: %s` | `info` | **leader 关闭交易。** commit=true=已提交，false=已回滚。**这就是交易生命周期终结** |
| `close transactions, num=%d` | `info` | 批量关闭 N 笔交易 |
| `cxt has committed or rollback` | `debug` | 重复调用，交易已关闭，忽略 |

---

## 辅助路径：Retry

| 日志 | 等级 | 含义 |
|------|------|------|
| `call for retry` | `info` | leader 开始广播 retry tx 到相关分片 |
| `added to retry pool` | `info` | 分片 leader 收到 retry tx，加入本地 retry pool |
| `retry tx already exists` | `debug` | retry pool 防重入 |
| `skip adding stale tx to retry pool` | `debug` | 交易已 stale，跳过 |
| `retryScheduler onBlockCommitted` | `info` | 每区块提交后检查 retry pool |
| `promoted txs to next round` | `info` | 有交易就绪，发送 ReSimulationSignal |
| `retry tx blocked` | **`warn`** | **交易在 retry pool 检查 CanLock 失败，附冲突详情** |
| `send_re_sim_signals: calling RPC` | `info` | 发送重试信号到 origin shard |
| `handle_re_sim_signal: received` | `info` | origin shard 收到重试信号 |
| `retry commit success` | `info` | tryToReSimulation 全部 shard 锁定成功，开始重试 |

---

## 辅助路径：Recall ReSimulation

| 日志 | 等级 | 含义 |
|------|------|------|
| `receive all related ssc votes, recall` | `info` | SSC vote 全部收到，决定是 Recall |
| `handle cxt recall proof` | `info` / `error` | 处理 Recall proof |
| `recall simulation, simulationNum: X, start` | `info` | 开始重新模拟 |
| `resimulation failed, it has subscribe to resimulate again, nextSimulationNum=X` | `debug` | 重试执行失败，再次加入重试 |
| `resimulation accomplished, send simulation commit` | `info` / `error` | 重新模拟完成 |

---

## 辅助路径：Timer

| 日志 | 等级 | 含义 |
|------|------|------|
| `start pool timer for cxt, timeout at block %d` | `info` | 启动 pool 超时定时器（交易太久没出 block 就超时） |
| `start sp1 timer for cxt, timeout at block %d` | `info` | 启动 sp1 超时定时器 |
| `cxt sp1 timeout at block %d committed` | `info` | sp1 超时，交易被强制回滚 |
| `cxt pool timeout at block %d` | `debug` | pool 超时，强制关闭交易 |

---

## 辅助路径：Lock 系统

| 日志 | 等级 | 含义 |
|------|------|------|
| `GetLockerAt: returning current state locker for root %s` | `info` | simulate 获取当前 stateRoot 的锁状态实例 |
| `GetLockerAt: snapshot not found, use current state locker` | `warn` | 请求的 stateRoot 快照已过期，降级到当前状态 |
| `GetLockerAt: returning historical state locker` | `info` | 基于历史快照创建 stateLocker |
| `handleLockCommit: creating snapshot for stateRoot %s` | `info` | 区块提交时创建锁快照 |
| `locker snapshot created` | `info` | 锁快照创建完毕 |
| `Commit state mu, pendingStates: %d` | `info` | stateLocker.Commit 合并 pending 锁到全局 |
| `applied write unlock to stateLockManager` | `info` | CR 交易的写锁从全局 stateLockManager 释放 |
| `TempLockView on block committed, lockedNum=%d` | `info` | 临时锁在区块提交后清理 |
| `CanLock false: committed write lock conflict` | `info` | **链上锁（stateLockManager）还在锁着** |
| `CanLock false: temp write lock conflict` | `info` | **TempLockView 临时锁还在占着** |
| `CanLock false: read-after-temp-write conflict` | `info` | 本批次前面的交易写了此 key，没法读 |

---

## 快速诊断模板

查一笔交易 `0xabc` 卡在哪，按路径 grep：

```
1. "add a cross shard Tx.*0xabc"             → 已入 pool？没到就是提交失败
2. "start simulate cx transaction.*0xabc"     → 开始模拟了？
3. "simulation accomplished.*0xabc"           → 模拟完成？status 是什么
4. "begin to verify simulation.*0xabc"        → 开始链上验证？
5. "send CXTCommitVote.*0xabc"                → 发了什么 type 的 vote？
6. "handle commit vote.*0xabc"                → vote 被收到了？
7. "receive all related ssc votes.*0xabc"     → 所有分片 vote 齐了？
8. "commit or rollback with proof.*0xabc"     → 最终决定被执行？
9. "leader close transaction.*0xabc"          → 交易终结？
```
