# HANDOFF-20260707-hotkey-retry-diagnosis

> from_session: `20260706_hotkey_retry_diagnosis`
> from_role: hermes-agent (hentai_coder)
> to_role: 继续优化 HotKeyRetry 链式重试的会话

## 实验摘要

| 实验 | 提交率 | TPS | P50 | P90 | 说明 |
|:----|:-----:|:---:|:---:|:---:|:-----|
| Ablation B (无重试) | 73.3% | 69.2 | — | — | 基线 |
| 关 OnBlockCommitted 信号聚合 | 74.2% | 72.2 | 44.7s | 96.5s | chainNextSim 补不上 |
| 恢复信号聚合 + stateDB 预检 | 96.1% | 91.5 | 62.5s | 141.8s | stateDB 预检有效 |
| 修复 OffChain→OnChain 错误分类 | 81.2% | 78.2 | 45.3s | 95.5s | 被动池未验证(日志缺失) |

## 已完成的改动

### 1. 信号聚合阶段加 stateDB 预检

**文件**: `ssc/retry_scheduler.go` — `OnBlockCommitted`

**问题**: 信号聚合的 `CanLock` 只查 TempLockView，不查 stateDB。标记 Ready 后跨 shard RPC (`tryToReSimulation → RetryCommit`) 跑一圈，结果 stateDB 锁冲突浪费。

**改动**:
- `OnBlockCommitted` 中 `rs.mu.Lock()` 后缓存 `bc.State()`（每区块只取一次）
- `CanLock` 通过后再逐个 key 调 `currentStateDB.CheckLock()`
- WriteSet → ReadSet 顺序检查，任一 key 被锁则 `NotReady`
- 不满足条件的 tx 不浪费跨 shard RPC

**效果**: 提交率从 74.2% → 96.1%（信号聚合恢复 + stateDB 预检共同作用）

### 2. pendingStates 锁错误分类修正

**文件**: `ssc/state_locker.go` — `Lockable()` 方法

**问题**: `Lockable()` 中 `pendingStates`（本 instance 未提交的变更）报 `ErrLockConflict_OffChain`，`baseSnapshot` 报 `ErrLockConflict_OnChain`。但两者都是 stateLocker 中的链上锁，不应区分。

`RetryCommit` 中检查 `OnChainLockConflict` 决定是否触发被动池——由于 `pendingStates` 的锁报了 `OffChain`，被动池入口 `hasOnChainConflict` 永远为 false → **被动池 0 次触发**。

**改动**: 两处 `ErrLockConflict_OffChain` → `ErrLockConflict_OnChain`：
- `ssc/state_locker.go:188` — pendingStates 写锁
- `ssc/state_locker.go:202` — pendingStates 读锁

### 3. Ablation 实验：注释 OnBlockCommitted 信号聚合

**文件**: `ssc/retry_scheduler.go` — `OnBlockCommitted`

**改动**: 用 `// /* ... // */` 注释了信号聚合（路径 A）。被动池超时（路径 B/C）未动。**已恢复**（现在信号聚合是打开的）。

## 当前代码状态

| 文件 | 改动 | 状态 |
|------|------|:----:|
| `ssc/retry_scheduler.go` | 信号聚合 + stateDB 预检（OnBlockCommitted） | ✅ 已部署 |
| `ssc/state_locker.go` | pendingStates 锁报错统一为 OnChain | ✅ 已部署 |
| `ssc/retry_scheduler.go` | Ablation 注释（信号聚合关闭） | ⏪ 已恢复 |

## 发现的链条瓶颈

### chainNextSim 漏斗（最后一轮实验数据）

```
chainNextSim scanning:            15,524
  → no downstream:                11,366 (73.2%) — 不共享 key，负载特性导致
  → reservation completed:         4,158 (26.8%)
    → 选中的 retryTx:               4,736 (avg 1.14/call)

HandleRetrySignal received:       122,060  — 大部分来自 OnPatchPoolUpdated 路径
retry commit success:              26,033
  → startReSimulation:              9,342 (35.9% of success)
    → resimulation accomplished:     3,114 (12.0% of success)
    → resimulation failed:          22,841 (87.8% of success)
```

### 核心问题

`retry commit success` 26,033 次，其中 9,342 次进入 `startReSimulation`，仅 3,114 次完成（`resimulation accomplished`）。

**22,841 次 resimulation failed 的根因**：`RetryCommit` 阶段在**本 shard leader 侧**通过 stateDB `CheckLock`，但 `StartReSimulation` 在模拟执行时拿到的是**更新版本的 stateDB**（期间别的交易提交了 SimTx，新锁合并进 baseSnapshot）。即使 `RetryCommit` 的所有 shard 都返回 `locked=true`，模拟执行时仍然可能撞 stateDB 锁。

**被动池未触发**：`RetryCommit` 的 stateDB `CheckLock` 返回 `ErrLockConflict_OffChain`，导致 `OnChainLockConflict=false` → `hasOnChainConflict=false` → 被动池不触发。修复已部署（`ErrLockConflict_OffChain` → `OnChain`），但未验证效果。

### 问号

- `retry commit success` 26,033 → `startReSimulation` 9,342 差了 16,691（64%）。这部分是被 `reSimInFlight` 防重入或 `IsWounded` 二次验证拦截的？
- `resimulation failed` 22,841 次中，多少比例是 stateDB 锁冲突 vs 其他原因（context cancel/worker pool 积压/P2P 超时）？
- `chainNextSim` 匹配的 4,736 个 retryTx 有多少最终提交成功？

## 配置参考

| 参数 | 当前值 | 说明 |
|:-----|:------:|------|
| `MaxOnChainRetries` | 0 | 未限制链上重试 |
| `MaxRetriesTotal` | 0 | 未限制总重试次数 |
| `PoolTimeout` | 0 | 未超时关闭 |
| `EnableLockOnConflict` | false | 冲突时不锁局部锁 |

`delay=10`, `rate=100`, `4 shards`, `4 validators/shard`

## 代码变更清单

```
ssc/state_locker.go:
  - Lockable(): pendingStates 写锁冲突报 ErrLockConflict_OnChain（原 OffChain）
  - Lockable(): pendingStates 读锁冲突报 ErrLockConflict_OnChain（原 OffChain）

ssc/retry_scheduler.go:
  - OnBlockCommitted(): 缓存 bc.State() 用于 stateDB 预检
  - OnBlockCommitted(): CanLock 通过后加 stateDB.CheckLock 过滤
  - OnBlockCommitted(): [已恢复]信号聚合注释（ablation 实验）
```

## 建议继续方向

1. **验证被动池修复效果**：跑实验确认 `AddToPassivePool` 日志是否出现，查看被动池唤醒率和 `resimulation failed` 的下降比例
2. **减少 StartReSimulation 的 stateDB 锁冲突**：考虑在 `RetryCommit` 阶段对所有 related shard 的 stateDB 都做一次 CheckLock（不只是本 shard），或者缩短 `RetryCommit` → `StartReSimulation` 的时间窗口
3. **诊断 26k→9k 的 16k 差距**：`reSimInFlight` 和 `IsWounded` 拦截了多少，这些 tx 的后续命运如何
4. **时延问题**：P50 45s，P90 95s。通过 `trace-tx-timing.py` 分析 gap 分布，确认是 block interval 主导还是锁竞争等待主导

## 已知坑

- `dumpChainRetryStats` 只在 `NewRetryScheduler` 构造时调用一次（`cycle()` 中的调用被注释了），运行时的 CHAIN_RETRY_STATS 不会自动输出
- `download_log` 在 `test_single` 中可能失败 → 日志不可用于分析，只依赖客户端输出
- `build_keys.go` 修改后需手动 `go run cmd/build_keys/build_keys.go` 重新生成 genesis，否则配置不生效
