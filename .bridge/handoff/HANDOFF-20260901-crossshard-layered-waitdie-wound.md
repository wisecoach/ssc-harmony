# HANDOFF-20260901-crossshard-layered-waitdie-wound.md

> from_session: 20260901（跨分片交易冲突治理：从 Wound-Wait 演进到「链上 Wait-Die + 链下 TLV wound + 严格门 + DAG 排序」）
> from_role: Designer
> to_role: Designer (next session)
> 状态：**进行中** —— 分层方案已落地实现（构建通过），但**尚未做最终一轮实验验证**。需在新 session 部署复跑确认收敛。

---

## 0. 一句话现状

跨分片交易曾因「先 Commit、后确认 + 无失败回滚」导致孤儿锁级联死锁；经多轮演进，现采用**分层设计**：
- **链上 Verify**：Wait-Die（低优先级立即终局彻底回滚，高优先级 wait，**不允许 wound**）。
- **链下 retryScheduler**：严格门 + TLV wound（高优先级抢占 TLV）+ DAG patch 覆盖，**保证只有“读写集可保证无冲突”的 SimTx 才被构建**。
- **internal_pool**：按优先级排序 + DAG 依赖排序（被依赖者先执行）。

代码已按此改完，`go build ./ssc/... ./core/... ./cmd/... ./node/...` 全绿。**待部署复跑验证。**

---

## 1. 背景与问题

从 DSN-52（`docs/designs/active/DSN-52-crossshard-deadlock-resolution.md`）继续。目标：rate=200 压测下跨分片交易全部收敛提交。

核心根因（原始）：跨分片交易锁是「先 Commit、后确认」——某分片验证成功即上锁，但整笔交易要等所有 related shard 都验证通过才发 CRTx；任一 shard 失败，其它已上锁 shard 的锁变孤儿（`LOCK_STALE`）→ 级联死锁。

### 1.1 赢家环（winner ring）
```
Tx1 已在 shard0 上锁（赢），在 shard1 被 Tx2 挡
Tx2 已在 shard1 上锁（赢），在 shard0 被 Tx1 挡
→ 双方都是“部分赢家”、互等 → 环破不掉
```
优先级是全局全序（`Nonce > OriginShardID > TxHash`），但光有优先级不够，必须有人“让位/被踢”才能破环。

---

## 2. 演进路线（为什么走到现在这版）

| 阶段 | 方案 | 结果 |
|---|---|---|
| 早期 | 无抢占，冲突双方都回滚重试 | 活锁（livelock），churn 无限 |
| 中 | 链上 Wound（高优先级主动释放低优先级持有者锁） | **违反投票协议**：被 wound 者可能“已投过 commit vote 却在提交中被撤销”→ 既 commit 又被 retry 双重处理，错误提交风险 |
| 当前 | **分层**：链上 Wait-Die（终局回滚）+ 链下 TLV wound + 严格门 + DAG 排序 | 已实现，待验证 |

### 关键结论
- **链上不允许 wound**：投票类协议铁律“票一旦投出不能被单方面撤销”。wound 会破坏它，导致双重提交。
- **链上用 Wait-Die**：低优先级冲突者立即彻底回滚（终局），高优先级 wait。
- **冲突要在链下解决**：不要在链上靠“构建注定失败的 SimTx 去判”，而应在重试层保证“构建的 SimTx 读写集无冲突”。

---

## 3. 最终分层设计（当前代码实现）

### 3.1 链上 Verify（`ssc/verify.go`）— Wait-Die
冲突分支（`len(conflictLockKeys) > 0`）：
```
if dsn52ShouldYield(...) {           // 低优先级 / 持有者优先级未知（保守）
    sendRollbackVoteForDie(txHash, simulation)   // 发终局 Rollback vote（Type=Rollback, Reason=ConflictRWSetFailedLock）
    return                             // 不重试
}
// 高优先级 → wait：不 wound，回重试池等低优先级释放后重试
```
- `sendRollbackVoteForDie`（verify.go:915）：走正常投票聚合 → origin 广播非 ReleaseOnly Rollback CRTx → 各分片 `RollbackTx` + `CloseTx(false)` 彻底回滚。
- `woundLowerPriorityHolders` 已标记弃用、**不再被调用**（保留代码便于对照/回滚）。
- `dsn52ShouldYield`（verify.go:738）：含 pending 持有者查询修复（能看同块 pending 写/读锁）。

### 3.2 链下 retryScheduler（`ssc/retry_scheduler.go`）— 严格门 + TLV wound + DAG patch
一笔交易能否模拟（`RetryCommit`），取决于能否**保证其读写集无冲突**：
1. TLV 与链上锁都空闲 → 可；
2. TLV 锁被更低优先级持有 → `TryLockWithPriority` **wound**（高优先级先拿 TLV 去模拟）→ 可；
3. 其余冲突 key 由 **DAG patch**（`offChainDAG.findCoveringSet` / Phase 1b/2b）覆盖 → 可；
4. **否则不构建 SimTx**（wait/回池）。

具体：
- `RetryCommit` Phase 1：`TryLockWithPriority`（TLV wound，wounded 返回 locked=false）。
- `RetryCommit` Phase 2：**严格** on-chain `stateDB.CheckLock`（链上不允许 wound，冲突一律 conflictKeys）。
- `RetryCommit` Phase 1b/2b：PatchPool DAG 覆盖冲突 key 补救。
- `OnBlockCommitted` promote：on-chain 空闲扫描 + Ready 预检恢复**严格**（只有 on-chain 锁全空闲才 promote）。

### 3.3 链下模拟（`ssc/simulator.go`）— 恢复 wound
- `isLowerPriorityLock`（simulator.go:906）+ `lowerPriorityLock`（simulator.go:926）：撞到低优先级持有者的锁 → 高优先级交易可「忽略」继续模拟（配合链下 TLV wound）。
- 覆盖 TLV / global 写锁（含 Finalized）/ global 读锁 / pending 写读锁。

### 3.4 internal_pool（`ssc/internal_pool.go`）— 优先级 + DAG 依赖排序
- SimTx 桶按优先级排序（`simPriority`）。
- `orderSimTxsByDAG`（internal_pool.go:132）：被依赖（Upstream）的 SimTx 在依赖者之前执行（拓扑排序，环则兜底）。
- 去掉了之前尝试的「冲突感知提取」（`simKeys`/`conflictSkipped`）——用户判定 internal_pool 感知冲突“为时已晚”，只负责排序。

### 3.5 其它已保留的修复
- `FindPendingLockHolder`（state_locker.go:383）：也能返回 pending 读锁持有者。
- `GetRLockHolder`（state_lock_impl.go）：global 读锁持有者查询。
- `ReleasePendingLocksFor` / `releaseOrphanWriteLocks`：wound/释放时清理 global+pending 锁（Part B 残留，现主要供 die/释放路径用）。

---

## 4. 实验数据（历史，供对比）

### 4.1 原始死锁（彻底回滚版，未收敛）
```
Shard 0: {'commit': 3234, 'rollback': 501, 'unfinished': 682}
Shard 1: {'commit': 3013, 'rollback': 1060, 'unfinished': 1778}
Shard 2: {'commit': 2718, 'rollback': 314, 'unfinished': 345}
Shard 3: {'commit': 4207, 'rollback': 931, 'unfinished': 1216}
```
### 4.2 Wait-Die + 终局回滚（锁彻底释放，但失败率过高）
```
Shard 0: {'commit': 2825, 'rollback': 1588, 'unfinished': 3}
Shard 1: {'commit': 2260, 'rollback': 3590, 'unfinished': 1}
Shard 2: {'commit': 2549, 'rollback': 822, 'unfinished': 7}
Shard 3: {'commit': 3462, 'rollback': 2891, 'unfinished': 1}
时延: avg 12276ms / P90 38280ms / P99 52406ms
```
- ✅ unfinished≈0 → 锁彻底释放、死锁解决。
- ❌ rollback 极高 → 因为同一块塞入太多冲突 SimTx，低优先级被终局回滚。

### 4.3 根因（本轮定位）
**“构建了注定会在链上失败的冲突 SimTx”**。之前为让“低优先级去链上 die”而放宽了重试门，导致大量冲突 SimTx 被构建 → 上链后被 Wait-Die 终局回滚 → 失败率高。

**因此当前设计改为：链下严格门保证“只有读写集无冲突的 SimTx 才被构建”**——冲突在重试层（TLV wound / DAG patch）化解，而不是靠上链判死。

---

## 5. 下一步（新 session）

### 5.1 首要：部署验证当前分层实现
- 复跑 rate=200，重点看：
  - `rollback` 是否明显下降（链下不再构建冲突 SimTx）；
  - `commit` 是否回升；
  - `unfinished` 是否保持 ~0（锁持续释放、不死锁）；
  - `onChainWound`/`woundedHolders` 恒 0（链上无 wound）；
  - 时延（P90/P99）是否可控。

### 5.2 若 rollback 仍偏高（分层未完全解决）
可能原因与排查方向：
1. **链下严格门仍放行了一些冲突 SimTx**：检查 `RetryCommit` 是否所有 key 都真正过了 TLV-wound / DAG-patch 覆盖；看日志里 `[SSCInternalPool] conflict-aware...`（已删）之外的冲突来源统计。
2. **DAG patch 覆盖不全**：`findCoveringSet` 未覆盖某些冲突 key → SimTx 仍会冲突。看 `SigRetryCommitPatchHit/PatchMiss`。
3. **TLV wound 未生效**：`SigRetryCommitTryLockWounded` 是否非 0；wounded 交易是否被正确踢回池。
4. **链上 Wait-Die 的终局回滚是主因**：如果大量交易在链上仍被判 die，说明链下没拦住；需要回到链下门补漏。

### 5.3 潜在改进方向（如需要）
- **给终局回滚加“有界重试”兜底**：当前 die=永久判死，失败率可能仍偏高。可在低优先级 die 前加一个“重试次数上限”，有界重试后再判死（需与用户确认，避免掩盖观测）。
- **链下冲突感知进一步前移**：用户已否掉 internal_pool 感知冲突（“为时已晚”），重点应放在 retryScheduler 的严格门。
- **时延优化**：如果收敛但时延高，排查 `OnBlockCommitted` promote 的每块推进量、`releasedKeys` 是否仍会为 0 导致 promote 停（DSN-52 §14 的 onChainFreeAdded 兜底是否有效）。

---

## 6. 环境 / 复现
- 远程 199：`ssh -F /dev/null -p 10022 -i ~/.ssh/id_ed25519 zjnu@10.7.95.199`（必须 `-F /dev/null` 避开系统 ssh_config 权限报错）。
- 日志：`/home/zjnu/go/src/github.com/harmony-one/logs/harmony-sscc/shard=4_validator=4_ssc=1_delay=10_rate=200_vpn=4/`（`ssc-validator-*.log` 才是 SSC 日志）。
- 聚合脚本：`scripts/local-ssc-retry-stats.py`、`scripts/local-txstages.py`。
- 关键日志：`OnBlockCommitted stats`（retryPool/globalLocked/globalFinished）、`[TempLockView] OnBlockCommitted`（releasedKeys）、`mu conflict`（conflictSource）、`DSN-52: ...`、`[SSCInternalPool]`、`leader close transaction`（含 reason）。

## 7. 构建 / 坑
- 构建：`GOCACHE=/tmp/gocache GOFLAGS=-mod=mod go build ./ssc/... ./core/... ./cmd/... ./node/...`。
- `go test ./ssc/` 预编译失败（`simulation_test.go` 旧 API，无关，勿被误导）。
- proto 手改：`ssc/api/proto/ssc.proto` + `ssc.pb.go` + `convert.go` 需同步（本轮已撤掉 `RetryCommitResp.Die`，恢复原样）。
- 工作区已有大量未提交改动（前几轮 DSN-52 的 Part A/B、lock 修复等），别混淆。

## 8. 关键文件速查
- `ssc/verify.go`：`dsn52ShouldYield`(738)、`sendRollbackVoteForDie`(915)、`MulticastRollbackProof`(80)、`woundLowerPriorityHolders`(790，弃用)。
- `ssc/retry_scheduler.go`：`RetryCommit`(约1300)、`tryToReSimulation`(1074)、`OnBlockCommitted`(约600)。
- `ssc/simulator.go`：`isLowerPriorityLock`(906)、`lowerPriorityLock`(926)、`GetState/SetState/IsKeyAvailable`。
- `ssc/internal_pool.go`：`Extract`(52)、`orderSimTxsByDAG`(132)、`simPriority`(187)。
- 设计文档：`docs/designs/active/DSN-52-crossshard-deadlock-resolution.md` §17（尤其 §17.7）。
