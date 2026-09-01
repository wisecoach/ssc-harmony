# HANDOFF-20260901-crossshard-priority-rounds

> from_session: 20260901（跨分片交易优先级 Wound-Wait 多轮实现与排查）
> from_role: Designer
> to_role: Designer (next session)
> 状态：**进行中** —— 已实现多套机制（Part A/B + dsn52ShouldYield 修复），rate=200 仍 block~95 冻结，尚未收敛

---

## 0. 背景

从 20260831 的 HANDOFF（跨分片死锁根治 DSN-52）继续，目标是让 rate=200 压测下跨分片交易全部收敛提交。前 session 已实现 ReleaseOnly + 链上优先级让位（§4.2）。本 session 连续 5 轮部署，逐步实现了「高优先级定向 Wound + 链下忽略低优先锁 + 让位判定覆盖 pending」，但仍复现 block~95 冻结。

## 1. 死锁模型（与用户对齐，关键结论）

```
Tx1 的 SimTx 在 shard1 因撞 Tx2 的锁而无法上链/验证失败；但 Tx1 的 SimTx 已在 shard0 上锁
Tx2 的 SimTx 在 shard0 因撞 Tx1 的锁而失败；但 Tx2 的 SimTx 已在 shard1 上锁
→ Tx1 卡在 shard1（被 Tx2），Tx2 卡在 shard0（被 Tx1）→ 赢家互等环 → 系统彻底卡死
```

- 优先级是**全局全序**（`Nonce > OriginShardID > TxHash`，取跨分片交易字段，所有分片判定一致）。
- 但优先级只是「判罚一致」，要执行还得靠 wound 把输家锁真正释放。
- **关键盲点**：`VerifySimulation` 报的冲突**几乎全部来自同块 `pendingStates`**（`[TempLockView] locked by tx` 是误导性命名，实为 on-chain 同块 pending 锁，不是 retry 的 TLV），而非 `globalLockedStates`。

## 2. 各轮部署结果（rate=200）

| 轮 | 已部署 | 结果 |
|---|---|---|
| 1 | ReleaseOnly | block 66 冻结，retryPool ~4244，globalLocked ~788 |
| 2 | + 链上优先级让位（dsn52ShouldYield 只认 global） | block 66-71 冻结，releasedKeys=0 → promote 停 |
| 3 | + §14 promote 兜底（on-chain 空闲扫描） | promote 生效（candidate ~3695），但 verify 塌方，retryPool 仍冻 ~3702 |
| 4 | + 链上 wound 直接解锁（仅 global）+ victim 拉回重试 | 冲突全 pending，wound 找不到持有者（woundedHolders=0） |
| 5 | + Part B（wound 释放 pending+global）+ Part A（链下忽略低优先锁）| Part A/B 生效（wound 59 次、孤儿 SimTx 能上链），但仍冻结；发现 dsn52ShouldYield 看不到 pending → 已修 |

**每轮都复现同一签名**：block ~95-100 冻结，globalLocked ~500（on-chain 赢家孤儿）、retryPool ~3800-4200、globalFinished 冻结、VerifySimulation 塌方（4000+/分 → 30/分）。

## 3. 本 session 已实现（ssc/）

| 文件 | 改动 |
|---|---|
| `state_lock_impl.go` | `txMeta`/`LockHolderMeta`/`txMetaMap`；`RegisterTxPriority/GetTxPriority/GetLockHolder/GetLockHolderMeta`（含 relatedShards/epochs/simNum/Finalized） |
| `verify.go` | `RegisterTxMeta` 上链注册；`woundLowerPriorityHolders`（Part B：`releaseOrphanWriteLocks` global + `ReleasePendingLocksFor` pending + `CallForRetry` victim）；`dsn52ShouldYield`（改：也查 pending 持有者，低优先级正确让位）；`MulticastRollbackProof` |
| `simulator.go` | **Part A**：`currentTxPriority`/`isLowerPriorityLock`；`IsKeyAvailable` 撞低优先锁忽略；`GetState/SetState` 用 `GetStateWithoutLock` 绕过 |
| `temp_lock_view.go` | `GetTempLockHolder`（TLV 持有者+优先级） |
| `state_locker.go` | `FindPendingLockHolder`/`ReleasePendingLocks`（按 txHash 反向索引删 pending 写/读锁） |
| `api/state_lock.go` | `StateLocker` 接口加 `ReleasePendingLocks`/`FindPendingLockHolder` |
| `core/state/statedb.go` | `FindPendingLockHolder`/`ReleasePendingLocksFor` 访问器 |
| `internal_pool.go` | §4.6 SimTx 桶按优先级排序 |
| `retry_scheduler.go` | §14 promote 兜底（on-chain 空闲扫描）+ `onChainWound` 计数 |

**文档**：`docs/designs/active/DSN-52-crossshard-deadlock-resolution.md` 已更新到 §15.6（实现记录 + 各轮复现 + 修复）。

## 4. 关键调试结论（新 session 直接复用）

- **冲突源统计**（第 5 轮 shard0）：`PENDING=5266`（同块 pending），`ON_CHAIN=0`。
- **wound 触发**：Part B 后 `woundedHolders=1` 出现 59 次，但 `woundedHolders=0` 仍占 1328 —— 说明大部分冲突当前交易其实是**低优先级**（无法 wound），但因 `dsn52ShouldYield` 看不到 pending 而误进 wound 分支。**该 bug 已在 session 末尾修复**（dsn52ShouldYield 增加 pending 查询）。
- **孤儿特征**（如 `0xdf3570808f0ae5`）：在 shard0/shard2 `vsOK`（已上锁），在 shard3 只有 `verify`/`simCommit`/`retry` 但**无 `vsOK`**（被 shard3 持有者挡住）→ 部分提交孤儿，从不完成，也从没被 wound 当 victim。
- **frozen 状态**：retryPool 里的交易 promote 后 RetryCommit 大量执行（4520 次），但 verify 仍失败回池（pending 冲突），不产生完成。

## 5. 下一步（新 session 继续）

### 5.1 首要：部署验证 dsn52ShouldYield 修复（§15.6）
- 预期：低优先级交易在同块 pending 冲突上正确让位、高优先级正确 wound → `woundedHolders=0` 比例大降、`globalLocked` 下降、`retryPool` 排空。
- 重点监控：`woundedHolders` 分布、`onChainWound`、`globalLocked` 趋势、`globalFinished` 是否持续增长。

### 5.2 若仍不行：补「有界孤儿回收」（方案 C 精准版，之前用户因怕掩盖而搁置，现在可重新评估）
- 对「确证卡死 N 块 + 未 Finalized」的 on-chain 孤儿自动 ReleaseOnly（本地 + 拉回重试）。
- 理由：数据强烈表明纯 Wound-Wait 面对高并发跨分片赢家环收敛性不足，需要兜底。
- 注意：会掩盖「wound 是否真生效」的观测，需和用户确认取舍。

### 5.3 其它待排查
- **wound 后 winner 能否完成**：winner wound 低优先者后自己也回 retry（`callForRetry`），依赖 promote 管线重新验证。需确认 promote/verify 在修复后是否真的能把 winner 推完（而不是一直回池）。
- **verify 塌方**：为什么 RetryCommit 4520 次但 verify 只有 37/分——verify 是否被「全部分片 Ready」聚合卡住（`HandleReSimulationSignal` 要求 `readyCnt==len(RelatedShards)`）。

### 5.4 新发现（代码审计，2026-09-01 续）：Part A 未接入 RetryCommit/promote 门 + pending 读锁盲区
> 即使 dsn52ShouldYield 修复已部署，赢家环仍可能破不掉。审计 `ssc/` 后定位到两处实现缺口：

1. **Part A（链下忽略低优先级锁）只实现了 Simulator 内部，没接入“重试上链的两道门”**
   - `ssc/simulator.go` 的 `isLowerPriorityLock/IsKeyAvailable/GetState/SetState` 已实现「撞低优先锁 → 忽略 → 产出 SimTx」。
   - 但环里的交易是**重试路径**交易，要重新上链触发链上 Wound，必须先过：
     - `OnBlockCommitted` promote 的 on-chain 空闲扫描 + Ready 预检（`retry_scheduler.go:621-647`、`735-750`）—— 全部用 `stateDB.CheckLock` **严格判冲突**；
     - `RetryCommit` Phase 2（`retry_scheduler.go:1450-1463`）—— 同样严格 `CheckLock`，冲突即 `OnChainLockConflict:true` 回池。
   - 这两处都**没有复用 Part A 的“忽略低优先级锁”** → 高优先级交易 C 在“重新模拟”前就被挡死，永远到不了 `verify.go` 的 `woundLowerPriorityHolders` → 赢家环破不掉。
   - 结论：Part A 只帮了「首次模拟」或「已能过门」的交易，**没帮到环里这批“卡在门前”的重试交易**。

2. **`FindPendingLockHolder` 只查 pending 写锁，查不到 pending 读锁持有者**
   - `ssc/state_locker.go:383` 只搜 `pendingStates.lockedStates`（写锁），没搜 `pendingStates.rlockedStates`（读锁）。
   - 但 `Lockable`（写路径，`state_locker.go:164`）**把 pending 读锁也判成 `ErrLockConflict_OnChain`**。
   - 所以「C 想写、H 持 pending 读锁」的同块冲突，链上会报冲突，但 `FindPendingLockHolder`/`GetLockHolder` 都找不到 H → `dsn52ShouldYield` 无法让位、`woundLowerPriorityHolders` 无法 wound，只能干等重试。
   - 与 HANDOFF §4 的 `PENDING=5266`（同块 pending 冲突）高度相关：其中相当一部分可能是 pending 读锁，导致 wound 彻底找不到持有者。
   - 对称盲区：`GetLockHolder`/`GetLockHolderMeta` 也只查 global 写锁，无 global 读锁持有者查询（global 读锁在写路径 `Lockable` 里不判冲突，影响较小但仍不对称）。

**本次已改（实现完成，待部署验证）**：
- ✅ `FindPendingLockHolder` 扩展为也返回 pending 读锁持有者（`state_locker.go`）。
- ✅ 补 `stateLockManager.GetRLockHolder`（global 读锁持有者查询，`state_lock_impl.go`）。
- ✅ **最终分层设计**（链上 Wait-Die + 链下 TLV wound + 严格门 + DAG 排序）：
  - **链上 Verify（保留现状）**：Wait-Die，低优先级 → `sendRollbackVoteForDie`（立即终局彻底回滚）；
    高优先级 → wait。**链上不允许 wound**（避免投票后仍回滚）。
  - **链下 retryScheduler（恢复 wound + 严格门）**：能否模拟取决于能否保证读写集——
    ① TLV+链上锁空闲；② TLV 锁被更低优先级持有 → `TryLockWithPriority` wound（高优先级先拿 TLV）；
    ③ 其余冲突 key 由 DAG patch 覆盖。**所有 key 通过才构建 SimTx**。
    - `RetryCommit` Phase 2 恢复严格 on-chain 检查；promote 恢复严格；
    - 移除 `isBlockedByHigherPriority`（恢复 `isLowerPriorityLock`）与 `RetryCommitResp.Die`。
  - **链下模拟（恢复 wound）**：`simulator.go` 恢复 `isLowerPriorityLock`（高优先级忽略低优先级锁继续模拟）。
  - **internal_pool**：去掉冲突感知提取（`simKeys`/`conflictSkipped`），改为按优先级排序 + DAG 依赖排序
    （`simUpstreams`/`orderSimTxsByDAG`，被依赖者在依赖者之前执行）。
  - 移除 `RetryCommitResp.Die`（types/proto/convert/pb）。
  - 详见 `docs/designs/active/DSN-52-crossshard-deadlock-resolution.md` §17.7。
- ⏳ 待验证：`onChainWound`/`woundedHolders` 恒 0；链下只构建“读写集无冲突”的 SimTx；`rollback` 下降、`commit` 回升、锁持续释放。

## 6. 环境 / 复现
- 远程 199：`ssh -F /dev/null -p 10022 -i ~/.ssh/id_ed25519 zjnu@10.7.95.199`（注意必须 `-F /dev/null` 避开系统 ssh_config 权限报错）。
- 日志：`/home/zjnu/go/src/github.com/harmony-one/logs/harmony-sscc/shard=4_validator=4_ssc=1_delay=10_rate=200_vpn=4/`（ssc-validator-*.log 才是 SSC 日志；zerolog/log-*.log 不是）。
- 聚合脚本：`scripts/local-ssc-retry-stats.py`、`scripts/local-txstages.py`。
- 关键日志：`OnBlockCommitted stats`（retryPool/globalLocked/globalFinished）、`[TempLockView] OnBlockCommitted`（releasedKeys）、`mu conflict`（conflictSource=[stateDB]/[TempLockView]）、`DSN-52: wounded lower-priority holder`、`LOCK_STALE`、`VerifySimulation success`。

## 7. 构建 / 坑
- 构建：`GOCACHE=/tmp/gocache GOFLAGS=-mod=mod go build ./ssc/...`（本机 go-build 缓存只读）。
- `go test ./ssc/` 预编译失败（simulation_test.go 旧 API，无关）。
- 别混淆预先存在的工作区改动：test/configs、cmd/build_keys.go、harmony 二进制、retry_scheduler 的 LockedKeys 合并等是之前就有的。
- 用户明确：**不做全量方案 C（陈旧锁自动释放）会掩盖测试结果**；但若 dsn52ShouldYield 修复后仍不行，需重新和用户确认是否接受「精准有界孤儿回收」。
