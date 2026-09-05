---
id: DSN-52
title: 跨分片死锁根治：链上 Wait-Die + 链下 TLV wound + 严格门 + DAG 排序（分层收敛）
type: DSN
status: implemented
priority: P0
author: Designer
created: 2026-08-31
updated: 2026-09-01
scope: [ssc/verify.go, ssc/state_lock_impl.go, ssc/state_locker.go, ssc/committer.go, ssc/retry_scheduler.go, ssc/tx_submitter.go, ssc/internal_pool.go, ssc/temp_lock_view.go, ssc/patchpool.go]
refs: [DSN-47, DSN-48, DSN-46, DSN-51, BUG-12, BRF-09]
---

> **⚠️ Wait-Die 弃用声明（2026-09-02 定稿）**：本文所述"链上低优先 Wait-Die die"已**完全弃用**。
> 现在唯一的 die 机制是 DSN-53 的 CMH——检测到环后只对环内最低优先 victim 终局回滚；
> 链上锁冲突不再按优先级主动 die。与本文冲突处，以 DSN-53（v2/v3）为准。

> **关联提示**：本 DSN-52 是"链上 Wait-Die + 链下 TLV wound"的分层收敛基底（resolution）。
> 检测层语义（"被卡住"= global∪pending 链上锁、TLV 非持久锁、记边/探测解耦、探针沿完整 WFG 回环）
> 见 `DSN-53 v2`。DSN-52 中的"链上 wound / 触发记边"描述以 DSN-53 v2 为准。
> **状态**：已实现（分层方案已落地，`go build ./ssc/... ./core/... ./cmd/... ./node/...` 通过；**待最终一轮实验验证**，见 §6 验证清单，部署复跑见交接文档 HANDOFF-20260901-crossshard-layered-waitdie-wound §5.1）
> **背景**：rate=200 压测下跨分片交易在 block 66 后**彻底死锁**（30 分钟零活动），剩余 ~6900 笔卡死。根因是「链上锁先 Commit、等全部分片确认才发 CRTx 释放，但失败无回滚路径」导致孤儿锁级联泄漏（LOCK_STALE 万级）。本 DSN 给出根治方案：链上锁冲突接入确定性优先级 **Wait-Die**（终局回滚，链上不 wound）+ 链下 TLV wound + 严格门 + DAG 排序，把「永久死锁」转为「有界可恢复收敛」。

---

## 1. 问题与根因

### 1.1 问题现象（实测）

| 指标 | 数值 |
|---|---|
| 最后提交 | block 66（之后 30 分钟零活动） |
| 卡死交易 | ~6900（shard 各 1242/2938/759/1991） |
| 孤儿锁（LOCK_STALE） | 万级（3.8w~4.6w），`globalLocked` ~1000 |
| `applyTo released` | 0（孤儿锁持有者从不走释放路径） |

已确认不是变慢，而是**彻底死锁**：block 66 后无任何 verifyOK / close / retryOK / reSim 活动。

### 1.2 根因链

```
TX_A 在 shard0 验证成功 → 锁 Commit 上链（永久）
     shard1/shard3 被其它孤儿锁挡住验证 → 发不出 commit vote
     → origin 凑不齐 relatedShards 的票 → CRTx 永不提交
     → shard0 那份「先 Commit」的锁永远无法释放（无失败回滚路径）
     → 成为孤儿锁 → 挡住后续交易在这些 key 上的验证 → 级联
     → 循环等待 + 无抢占 + 无超时 = 死锁
```

**核心缺陷**：跨分片交易的锁是「**先 Commit、后确认**」，失败时没有「回滚已 Commit 锁」的路径。锁只能由 CRTx 释放，而 CRTx 只在「全部分片验证成功」时产生——任一 shard 失败，其它已上锁 shard 就永久泄漏。

**赢家环（winner ring）**：

```
Tx1 已在 shard0 上锁（赢），在 shard1 被 Tx2 挡
Tx2 已在 shard1 上锁（赢），在 shard0 被 Tx1 挡
→ 双方都是“部分赢家”、互等 → 环破不掉
```

优先级是全局全序（`Nonce > OriginShardID > TxHash`），但光有优先级不够，必须有人“让位/被踢”才能破环。

## 2. 设计目标

1. **预防死锁**：链上锁冲突时按确定性优先级判定「谁让位」，避免循环等待形成（无需分布式死锁检测）。
2. **可恢复**：被让位（die）的交易**彻底回滚并释放全部已 Commit 锁**，不留下孤儿锁，斩断级联。
3. **不违反投票协议**：链上**不允许 wound**——票一旦投出不能被单方面撤销，避免“既 commit 又被 retry”的双重处理。
4. **低成本**：冲突判定是**分片本地**的确定性比较；跨分片清理复用现有 Rollback CRTx 广播，无新增消息类型。

## 3. 最终分层设计

### 3.1 优先级：确定性全局全序

`Priority = Nonce(小=高) > OriginShardID(小=高) > TxHash(小=高)`（`api.Priority.Less`）是确定性全序，且从同一 `RetryTx` 传播到所有分片 → **同一对交易在所有分片优先级判定一致**。

### 3.2 链上 Verify = Wait-Die（`ssc/verify.go`）

冲突分支（`len(conflictLockKeys) > 0`）只做两件事，**链上不允许 wound**：

```
if dsn52ShouldYield(...) {                 // 低优先级 / 持有者优先级未知（保守）
    sendRollbackVoteForDie(txHash, simulation)   // 发终局 Rollback vote（Type=Rollback, Reason=ConflictRWSetFailedLock）
    return                                  // 不重试（终局）
}
// 高优先级 → wait：不释放自身锁、也不 wound 持有者，回重试池等低优先级释放后重试
if checkRetryLimitExceeded(...) { sendRollbackVoteForRetry(...) }
else { callForRetry(...) }
```

- `sendRollbackVoteForDie`（verify.go:915）：发 `Type=Rollback / Reason=ConflictRWSetFailedLock` 的 CommitVote，走正常投票聚合 → origin 广播非 ReleaseOnly 的 Rollback CRTx → 各分片 `RollbackTx + CloseTx(false)` **彻底回滚**。
- `dsn52ShouldYield`（verify.go:734）：含 pending 持有者查询修复，能看同块 pending 写/读锁。
- `woundLowerPriorityHolders` **已弃用、不再被调用**（保留代码便于对照/回滚）；验证信号 `onChainWound` 恒 0。

### 3.3 链下 retryScheduler = 严格门 + TLV wound + DAG patch（`ssc/retry_scheduler.go`）

一笔交易能否构建 SimTx（`RetryCommit`），取决于能否**保证其读写集无冲突**：

1. **Phase 1（TLV wound）**：`TryLockWithPriority`——TLV 锁空闲 → 可；TLV 锁被更低优先级持有 → wound 对方（高优先级先拿到 TLV）→ 可；被更高优先级 wound → `Locked:false` 回池。
2. **Phase 2（严格 on-chain 门）**：对 ReadSet∪WriteSet 逐个 `stateDB.CheckLock`，链上不允许 wound，冲突一律计入 `conflictKeys`。
3. **Phase 1b/2b（DAG patch 补救）**：`offChainDAG.findCoveringSet` + `isFullyCovered` + `chainDepthOf <= maxChainDepth`，多 Patch 联合覆盖冲突 key → 可；否则**不构建 SimTx**（wait/回池）。

`OnBlockCommitted` promote 同样**严格**：只有 on-chain 锁全部空闲才 `Ready`（含 `onChainFreeAdded` 兜底扫描，防止 TLV releasedKeys 静默后 promote 停摆）。

### 3.4 链下模拟 = 忽略低优先级锁（`ssc/simulator.go`）

- `isLowerPriorityLock`（simulator.go:906）+ `lowerPriorityLock`（simulator.go:924）：撞到**更低优先级**持有者的锁 → 高优先级交易可「忽略」继续模拟、构建 SimTx（配合链下 TLV wound）。
- 覆盖：TLV / global 写锁（含 Finalized）/ global 读锁 / pending 写锁 / pending 读锁。
- on-chain 冲突由严格门（§3.3）拦截，链上 Verify 只做 Wait-Die，不在模拟层判死。

### 3.5 internal_pool = 优先级 + DAG 依赖排序（`ssc/internal_pool.go`）

- SimTx 桶按 `simPriority`（Nonce>OriginShardID>TxHash）排序，让同分片处理顺序尽量一致。
- `orderSimTxsByDAG`（internal_pool.go:130）：被依赖（Upstream）的 SimTx 在依赖者之前执行（拓扑排序，环则兜底）。
- 不做冲突感知提取（`simKeys`/`conflictSkipped` 已删除）——internal_pool 只负责排序，冲突缓放在 retryScheduler 严格门。

## 4. 关键实现对照

| 模块 | 位置 | 说明 |
|---|---|---|
| 链上 Wait-Die | `verify.go:660` 冲突分支 | `dsn52ShouldYield` → die / wait |
| 终局回滚 | `verify.go:915` `sendRollbackVoteForDie` | Type=Rollback, Reason=ConflictRWSetFailedLock |
| 弃用 wound | `verify.go:790` `woundLowerPriorityHolders` | 不再调用，`onChainWound` 恒 0 |
| 严格门 | `retry_scheduler.go:1307` `RetryCommit` | Phase1 TLV wound / Phase2 严格 CheckLock / Phase1b/2b DAG patch |
| promote | `retry_scheduler.go:549` `OnBlockCommitted` | on-chain 空闲扫描 + Ready 预检 + `onChainFreeAdded` 兜底 |
| DAG patch | `patchpool.go` `offChainDAG` | `findCoveringSet` / `isFullyCovered` / `tryConsumePatch` / `isPatchFinalized` |
| 链下忽略低优锁 | `simulator.go:906/924` | `isLowerPriorityLock` / `lowerPriorityLock` |
| 池排序 | `internal_pool.go:52/130/185` | `Extract` / `orderSimTxsByDAG` / `simPriority` |
| 锁持有者查询 | `state_locker.go:384` / `state_lock_impl.go:566` | `FindPendingLockHolder`（含读锁）/ `GetRLockHolder` |

**其它已保留的修复**：
- `FindPendingLockHolder` 也能返回 pending 读锁持有者；
- `GetRLockHolder`：global 读锁持有者查询；
- `ReleasePendingLocksFor` / `releaseOrphanWriteLocks`：die/释放路径清理 global + pending 锁；
- proto 已撤掉 `RetryCommitResp.Die`（`ssc.proto:350` 只保留 `tx_hash / locked / on_chain_lock_conflict`）。

## 5. 关键设计决策（与用户对齐）

| # | 决策 | 理由 |
|---|---|---|
| 1 | 链上**不允许 wound**，用 Wait-Die | 投票协议铁律“票一旦投出不能被单方面撤销”；wound 会导致“既 commit 又被 retry”的双重处理 |
| 2 | 低优先级 die = **终局彻底回滚**（不恢复软重试） | 保证锁彻底释放、死锁收敛；牺牲失败率换取确定性收敛 |
| 3 | 冲突在**链下解决**，不靠构建注定失败的 SimTx 去判 | 构建 SimTx 时即确定是否失败，避免上链后被 Wait-Die 终局回滚（失败率高） |
| 4 | 链下严格门 + TLV wound + DAG patch 保证“读写集无冲突才构建” | 把冲突缓放在重试层化解，而不是上链判死 |
| 5 | internal_pool 只做优先级 + DAG 排序，不做冲突感知提取 | 池内感知冲突“为时已晚”，排序只是降低冲突频率 |

## 6. 验证清单（待部署复跑）

rate=200 复跑重点观测：

1. **rollback 明显下降**（链下严格门不再构建冲突 SimTx），`commit` 回升；
2. `unfinished` 保持 ~0（锁持续释放、不死锁）；
3. `onChainWound` / `woundedHolders` 恒 0（链上无 wound）；
4. 时延 P90/P99 可控。

若 rollback 仍偏高，按序排查：
1. `SigRetryCommitPatchHit/PatchMiss`——DAG patch 覆盖是否不全；
2. `SigRetryCommitTryLockWounded`——TLV wound 是否生效；
3. 严格门是否仍放行冲突 SimTx；
4. TOCTOU 竞态（严格门检查后、上链 Verify 前更高优先级抢锁）导致的固有 die。

> **注**：TOCTOU 竞态是 Wait-Die + 严格门的固有代价，rollback **不会归零**，只应明显下降。

## 7. 风险与边界

- **TOCTOU 竞态**：严格门在 RetryCommit 时检查锁空闲，但 SimTx 上链 Verify 有延迟，期间更高优先级可能抢到 on-chain 锁 → 低优先级上链仍会被终局回滚。这是固有代价，rollback 不会归零。
- **未知持有者优先级 → 保守 die**：`dsn52ShouldYield` 对未知优先级一律让位。若大量旧锁/未注册优先级，会推高 rollback，需观测 die 中 unknown 占比。
- **die 是永久判死**：这是已确认的决策（保证收敛），但会牺牲失败率；若 rollback 仍偏高且非 TOCTOU/unknown 主导，再考虑「有界重试」兜底（需与用户确认）。
- **高优先级撞低优先级 on-chain 持有者会 off-chain 干等**：严格门不构建，依赖低优先级 die/commit 释放，时延 P90/P99 需实测。

---

## 16. 方案1：首次模拟也占 TLV 锁（链下并行协调，2026-09-02）

### 16.1 动机
链下首次模拟是**乐观**的：`IsKeyAvailable` 只「检查」TLV（`HasConflict`），从不「获取」TLV 锁
（`TryLockWithPriority` 只在 RetryCommit 调用）。所以并行的首次模拟互相看不见 → 都构建 SimTx →
上链同块 pending 冲突 → Wait-Die 终局回滚 → rollback 高、冲突 SimTx 多。

### 16.2 实现（`ssc/impl.go` CommitSimulation）
首次/重模拟成功后、构建 SimTx 前，对该分片完整 RWSet（read+write）调 `TryLockWithPriority` 占 TLV 锁：
```
成功 → 继续提交 SimTx（TLV 锁保留，OnBlockCommitted 在 SimTx 上链时释放）
失败（与并行模拟冲突且无法 wound）→ 不构建 SimTx，CallForRetry 回重试池
```
这样并行的首次模拟只有一个能拿到 TLV 锁构建 SimTx，冲突方回重试池 → 阻止冲突 SimTx 被构建。

### 16.3 决策（与用户确认）
- 占锁时机：模拟成功后（能拿完整 RWSet），非模拟开始时增量占锁。
- 锁滞留：**不设超时解锁**（2b）——TLV 锁靠 OnBlockCommitted 在 SimTx 上链时释放，避免掩盖问题。

### 16.4 待验证
- rollback 是否明显下降（链下不再构建冲突 SimTx）；
- `DSN-52 方案1: first-sim failed to acquire TLV lock` 计数；
- commit 回升、unfinished ~0、时延可控。
