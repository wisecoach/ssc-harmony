---
id: DSN-20
title: 模拟管线 + 重试管线全链路计时埋点
type: DSN
status: proposed
priority: P1
author: Designer
created: 2026-07-01
scope: [ssc, scripts]
refs: [EXP-01, DSN-19, DSN-17, DSN-09]
---

# 模拟管线 + 重试管线全链路计时埋点

## 一、动机

当前实验显示 latency P50=36s, P99=101s，但只有 `closeTransaction` 有完整 timing 分解（DSN-17, DSN-18, DSN-19 已完成 stateLock 优化），
**模拟入口到 SimTx 提交的全管线 + 重试管线均没有分段计时**，无法定位延迟根因。

### 现有埋点覆盖

| 函数 | 覆盖 | 问题 |
|------|:----:|:----:|
| `closeTransaction` timing breakdown | ✅ `stateLock`/`simCleanup`/`verCleanup`/`totalClose` | — |
| `Simulator.Cleanup` timing | ✅ | — |
| `Verifier.Cleanup` timing | ✅ | — |
| `retryScheduler.StaleTx` timing | ✅ | — |
| `CommitOrRollbackWithProof` timing breakdown | ✅ `unmarshal`/`isFinished`/`commitTx`/`closeTx`/`total` | — |
| `RemoveOnChainPatch` timing | ✅ | — |
| `CXTTimerManager.RemoveTx` timing | ✅ | — |
| `PatchPool.Remove` timing | ✅ | — |
| `RemoveFromPassivePool` timing | ✅ | — |
| `SimulateCXTransaction`（入队） | ❌ 只有 `pushTime` stats | 无结构化的 timing breakdown 日志 |
| `processSimulationTask`（出队→RPC） | ❌ 只有总 `cost` 日志 | 队列等待 vs RPC 调用未分离 |
| `StartSimulateCXTransaction`（leader 端） | ❌ 只有总 `cost` 日志 | 内部未分解 |
| `CommitSimulation`（SimTx 提交） | ❌ **完全没有 timing** | — |
| `HandleCommitVote`（投票聚合） | ❌ **完全没有 timing** | — |
| `AddToRetry`（入重试池） | ❌ **完全没有 timing** | — |
| `chainNextSim`（链式扫描） | ❌ **完全没有 timing** | — |
| `HandleRetrySignal`（接收链式信号） | ❌ **完全没有 timing** | — |
| `tryToReSimulation`（重试编排） | ❌ **完全没有 timing** | — |
| `RetryCommit`（锁竞争） | ❌ **完全没有 timing** | — |

### 目标

对模拟管线 + 重试管线的每一段加结构化的 `timing breakdown` 日志（Info 级别），配合现有的分析脚本 `analyze-all-timing.py` 一站式输出 P50/P90/P99/max，使延迟定位从「猜」变为「看」。

---

## 二、全链路调用树

### 2.1 首次通过路径（模拟管线）

```
Inject tx
  ├─ SimulateCXTransaction (simulator.go)
  │     └─ 入队 time.Now()* 保留到 task.pushTime
  │
  ├─ worker pop → processSimulationTask (simulator.go)
  │     └─ [new] processSimulationTask timing breakdown
  │            ├─ queueWait     — push 到 pop
  │            ├─ p2pCall       — RPC Call→leader
  │            └─ total
  │
  ├─ StartSimulateCXTransaction (simulator_leader.go)
  │     └─ [new] StartSimulateCXTransaction timing breakdown
  │            ├─ callMembers     — 并行 HandleSimulateRequest
  │            ├─ aggregate       — aggregateSimulationResults
  │            ├─ thresholdSign   — thresholdSignSimulationCommit
  │            ├─ commitSend      — Multicast CommitSimulation
  │            └─ total
  │
  ├─ CommitSimulation (impl.go)
  │     └─ [new] CommitSimulation timing breakdown
  │            ├─ buildCallStates  — BuildCallStates + writeSet
  │            ├─ buildSignatures  — buildSignaturesForSimulation
  │            ├─ onChainPatch     — AddOnChainPatch
  │            ├─ submitTx         — SubmitSimulationTx
  │            └─ total
  │
  ├─ HandleCommitVote (impl.go)
  │     └─ [new] HandleCommitVote timing breakdown
  │            ├─ voteTracking     — commitLock + vote map ops
  │            ├─ aggregate        — aggregateSSCCommitVote
  │            ├─ sendVote         — RPC to origin shard leader
  │            └─ total
  │
  └─ closeTransaction timing breakdown (已有)
```

### 2.2 重试路径

```
Retry trigger (e.g. CallForRetry)
  ├─ AddToRetry (retry_scheduler.go)
  │     └─ [new] AddToRetry timing breakdown
  │            ├─ extractRWSet    — 从 SimulationCallStates 提取 RWSet
  │            ├─ poolInsert      — mu.Lock + retryPool insert + stale check
  │            └─ total
  │
  ├─ OnBlockCommitted (retry_scheduler.go) — 每块触发
  │     └─ [new] OnBlockCommitted timing breakdown
  │            ├─ staleClean      — 清理 stale txs
  │            ├─ scanPool        — 遍历 retryPool + CanLock + signal 构建
  │            ├─ sendSignals     — sendReSimulationSignals (goroutine fire)
  │            └─ passiveExpire   — 被动池超时 close
  │            └─ total
  │
  ├─ SimTx commit → chainNextSim (retry_scheduler.go)
  │     └─ [new] chainNextSim timing breakdown
  │            ├─ scanPool        — RLock + 遍历 retryPool 匹配依赖
  │            ├─ reserveSelect   — 预留 + 选择不冲突的 retryTx
  │            ├─ sendSignals     — sendChainSignal (逐个发)
  │            └─ total
  │
  ├─ HandleRetrySignal (retry_scheduler.go)
  │     └─ [new] HandleRetrySignal timing breakdown
  │            ├─ setPatch        — SetChainPatch
  │            ├─ poolLookup      — RLock + 查找 retryPool
  │            ├─ dispatch        — go tryToReSimulation
  │            └─ total
  │
  ├─ tryToReSimulation (retry_scheduler.go)
  │     └─ [new] tryToReSimulation timing breakdown
  │            ├─ inFlightCheck   — reSimInFlight 防重入
  │            ├─ retryCalls      — 并行 RetryCommit 到所有 related shards
  │            ├─ resultCheck     — 结果检查 (wound/passive pool)
  │            ├─ triggerSim      — TriggerReSimulation (成功) / AddToPassivePool
  │            └─ total
  │
  │  (TriggerReSimulation →)
  ├─ StartReSimulation (simulator_leader.go) — 重试模拟执行（与首次模拟路径结构一致）
  │     └─ [new] StartReSimulation timing breakdown
  │            ├─ getState        — GetTxState + 构建 req from lastReq
  │            ├─ callMembers     — 并行 HandleSimulateRequest
  │            ├─ aggregate       — aggregateSimulationResults
  │            ├─ thresholdSign   — thresholdSignSimulationCommit
  │            ├─ commitSend      — CommitSimulation (local + Multicast)
  │            └─ total
  │
  └─ RetryCommit (retry_scheduler.go)
        └─ [new] RetryCommit timing breakdown
               ├─ passiveCheck    — 被动池检查 + Wound 检查
               ├─ patchPoolCheck  — PatchPool.HasConflict + TryConsume
               ├─ tryLock         — TryLockWithPriority
               ├─ stateDbCheck    — stateDB.CheckLock
               └─ total
```

---

## 三、埋点规范

### 3.1 代码模式

所有新埋点遵循已有 `timing breakdown` 模式：

```go
t0 := time.Now()

// 阶段 A
tA := time.Since(t0)

// 阶段 B
tB := time.Since(t0)

utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
    Str("stageA", tA.String()).
    Str("stageB", tB.String()).
    Str("total", time.Since(t0).String()).
    Msg("FunctionName timing breakdown")
```

**规则**：
- `time.Since(t0)` 累积值（非增量），便于跨阶段累计
- 所有耗时字段用 `Str("name", duration.String())`，统一序列化为 `"1.234ms"` 格式
- 日志消息命名格式：`"<FunctionName> timing breakdown"`
- 统一 Info 级别（需 grep 统计）
- 在函数 `return` 前打印，确保所有分支（包括 error 分支）都出 timing log
- `txHash` 作为公共字段，便于按交易关联各阶段

### 3.2 各埋点部署详情

#### 3.2.1 `processSimulationTask timing breakdown` — simulator.go

**文件位置**：`processSimulationTask` 函数末尾（`simulator.go:441-487`），已有 `cost` + `p2pStart`。

**字段**：

| 字段 | 含义 | 计算方式 |
|------|------|:--------:|
| `queueWait` | 队列等待时间（push→pop） | `time.Since(task.pushTime)` at pop |
| `p2pCall` | RPC 调用耗时 | `time.Since(p2pStart)`（已有） |
| `total` | 总耗时（含 cleanup） | `time.Since(startTime)`（已有） |

**变更**：在原有「simulate cx transaction, end」log 之后，加一条 `processSimulationTask timing breakdown` log。

#### 3.2.2 `StartSimulateCXTransaction timing breakdown` — simulator_leader.go

**文件位置**：`StartSimulateCXTransaction` return 前（`simulator_leader.go:329`）。

**字段**：

| 字段 | 含义 | 锚点 |
|------|------|:----:|
| `callMembers` | 并行调 HandleSimulateRequest | loop start → wg.Wait |
| `aggregate` | aggregateSimulationResults | 函数调用前后 |
| `thresholdSign` | thresholdSignSimulationCommit | 函数调用前后 |
| `commitSend` | Multicast CommitSimulation | 函数调用前后 |
| `total` | 总耗时 | 已有 `startTime` at line 55 |

**变更**：在 return `sscResult` 前，加一条 `StartSimulateCXTransaction timing breakdown` log。

#### 3.2.3 `CommitSimulation timing breakdown` — impl.go

**文件位置**：`CommitSimulation` return 前（`impl.go:863`）。

**字段**：

| 字段 | 含义 | 锚点 |
|------|------|:----:|
| `buildCallStates` | BuildCallStates + writeSet 构建 | `line 766` (`callStates`) 前 → `line 814` 的 `"build commit simulation completed"` log |
| `buildSignatures` | buildSignaturesForSimulation | `line 819` 调用前 |
| `onChainPatch` | AddOnChainPatch | `line 823` 调用前 |
| `submitTx` | SubmitSimulationTx | `line 825` 调用前 |
| `total` | 总耗时 | 函数入口 |

**变更**：在函数入口设 `t0`，在各阶段记录累积时间，在 `"simulation committed"` log 前加一条 `CommitSimulation timing breakdown` log。

#### 3.2.4 `HandleCommitVote timing breakdown` — impl.go

**文件位置**：`HandleCommitVote` 末尾 return 前（`impl.go:686`）。

**字段**：

| 字段 | 含义 | 锚点 |
|------|------|:----:|
| `voteTracking` | commitLock + vote map 操作 | 匿名函数入口到 aggregate 前 |
| `aggregate` | aggregateSSCCommitVote | 函数调用前后 |
| `sendVote` | RPC to origin shard leader | `Comm.Call` 前后 |
| `total` | 总耗时 | 函数入口 |

**变更**：在函数入口设 `t0`，在各子阶段记录累积时间，在 return 前（或 sendVote 后）加一条 `HandleCommitVote timing breakdown` log。

#### 3.2.5 `AddToRetry timing breakdown` — retry_scheduler.go

**文件位置**：`AddToRetry` 末尾（`retry_scheduler.go:387`），在 `"added to retry pool"` log 后。

**字段**：

| 字段 | 含义 | 锚点 |
|------|------|:----:|
| `extractRWSet` | 从 SimulationCallStates 提取 RWSet | 函数入口到 `line 361` (`tx.ReadSet = readSet`) 后 |
| `poolInsert` | mu.Lock + retryPool insert + stale check | `line 363` (`rs.mu.Lock()`) 到函数出口 |
| `total` | 总耗时 | 函数入口 |

**变更**：在函数入口设 `t0`，在 return 前加一条 `AddToRetry timing breakdown` log。注意提前 return 分支（nil tx、非 leader、已存在、stale）也要出 log。

#### 3.2.6 `chainNextSim timing breakdown` — retry_scheduler.go

**文件位置**：`chainNextSim` 末尾（`retry_scheduler.go:594`），在 signal 发送循环后。

**字段**：

| 字段 | 含义 | 锚点 |
|------|------|:----:|
| `scanPool` | RLock + 遍历 retryPool 匹配依赖 | 函数入口到 `line 549` (`rs.mu.RUnlock()`) |
| `reserveSelect` | 预留 + 选择不冲突 retryTx | `line 563` 到 `line 588` |
| `sendSignals` | 逐个发送 chain signal 到 downstream | `line 591` 到 return |
| `total` | 总耗时 | 函数入口 |

**附加字段**：`matchedCount` / `selectedCount`（已在日志中）。

**变更**：在函数入口设 `t0`，在 return 前加一条 `chainNextSim timing breakdown` log。

#### 3.2.7 `HandleRetrySignal timing breakdown` — retry_scheduler.go

**文件位置**：`HandleRetrySignal` 末尾（`retry_scheduler.go:657`），在 `Warn` log 之后。

**字段**：

| 字段 | 含义 | 锚点 |
|------|------|:----:|
| `setPatch` | SetChainPatch | line 637-639 |
| `poolLookup` | RLock + 查找 retryPool | line 643-645 |
| `dispatch` | goroutine launch tryToReSimulation | line 652 |
| `total` | 总耗时 | 函数入口 |

**变更**：在函数入口设 `t0`，在 return 前加一条 `HandleRetrySignal timing breakdown` log。

#### 3.2.8 `tryToReSimulation timing breakdown` — retry_scheduler.go

**文件位置**：`tryToReSimulation` return 前（`retry_scheduler.go:877` 附近，Locked 分支和 failed 分支都有 return）。

**字段**：

| 字段 | 含义 | 锚点 |
|------|------|:----:|
| `inFlightCheck` | reSimInFlight 防重入 | line 754-763 |
| `retryCalls` | 并行 RetryCommit 到所有 related shards | line 774-792 (wg.Wait) |
| `resultCheck` | 结果检查 + wound 检查 | line 794-828 / 835-870 |
| `triggerSim` | TriggerReSimulation / AddToPassivePool | line 828 / 859 |
| `total` | 总耗时 | 函数入口 |

**变更**：在函数入口设 `t0`，在两个 return 分支（成功 line 834、失败 line 877）前各加一条 `tryToReSimulation timing breakdown` log。

#### 3.2.9 `RetryCommit timing breakdown` — retry_scheduler.go

**文件位置**：`RetryCommit` 所有 return 前（`retry_scheduler.go:943`-1043+）。

**字段**：

| 字段 | 含义 | 锚点 |
|------|------|:----:|
| `passiveCheck` | 被动池检查 + Wound 检查 | line 947-960 |
| `patchPoolCheck` | PatchPool.HasConflict + TryConsume | line 980-996 |
| `tryLock` | TryLockWithPriority | line 1007-1017 |
| `stateDbCheck` | stateDB.CheckLock | line 1026-1043 |
| `total` | 总耗时 | 函数入口 |

**变更**：在函数入口设 `t0`，在所有 return 前加一条 `RetryCommit timing breakdown` log。

> **注意**：RetryCommit 有多个提前 return（passive wound → patch miss → lock fail → stateDB fail），每个分支都要打印 timing log。

#### 3.2.10 `StartReSimulation timing breakdown` — simulator_leader.go

**文件位置**：`StartReSimulation` 末尾（`simulator_leader.go:894`），在最后一条 `"startReSimulation: after Multicast"` log 后。

**字段**：

| 字段 | 含义 | 锚点 |
|------|------|:----:|
| `getState` | GetTxState + 构建 req from lastReq | line 645-675 |
| `callMembers` | 并行 HandleSimulateRequest | line 698-743 (wg.Wait) |
| `aggregate` | aggregateSimulationResults | line 760 调用前后 |
| `thresholdSign` | thresholdSignSimulationCommit | line 858 调用前后 |
| `commitSend` | CommitSimulation (local + Multicast) | line 881-890 |
| `total` | 总耗时 | 函数入口 `startTime` at line 677 |

**变更**：在函数末尾 return 前加一条 `StartReSimulation timing breakdown` log。

---

## 四、分析脚本更新

### 4.1 analyze-all-timing.py 新增 LOG_MARKERS

在现有的 `LOG_MARKERS` 字典中新增 **10** 个条目：

```python
LOG_MARKERS = {
    # ... 现有 9 个条目保持不变 ...
    # 新增：模拟管线
    'processSimulationTask': ('processSimulationTask timing breakdown',
        ['queueWait', 'p2pCall', 'total']),
    'StartSimulateCXTransaction': ('StartSimulateCXTransaction timing breakdown',
        ['callMembers', 'aggregate', 'thresholdSign', 'commitSend', 'total']),
    'CommitSimulation': ('CommitSimulation timing breakdown',
        ['buildCallStates', 'buildSignatures', 'onChainPatch', 'submitTx', 'total']),
    'HandleCommitVote': ('HandleCommitVote timing breakdown',
        ['voteTracking', 'aggregate', 'sendVote', 'total']),
    # 新增：重试管线
    'AddToRetry': ('AddToRetry timing breakdown',
        ['extractRWSet', 'poolInsert', 'total']),
    'chainNextSim': ('chainNextSim timing breakdown',
        ['scanPool', 'reserveSelect', 'sendSignals', 'total']),
    'HandleRetrySignal': ('HandleRetrySignal timing breakdown',
        ['setPatch', 'poolLookup', 'dispatch', 'total']),
    'tryToReSimulation': ('tryToReSimulation timing breakdown',
        ['inFlightCheck', 'retryCalls', 'resultCheck', 'triggerSim', 'total']),
    'StartReSimulation': ('StartReSimulation timing breakdown',
        ['getState', 'callMembers', 'aggregate', 'thresholdSign', 'commitSend', 'total']),
    'RetryCommit': ('RetryCommit timing breakdown',
        ['passiveCheck', 'patchPoolCheck', 'tryLock', 'stateDbCheck', 'total']),
}
```

`SimulateCXTransaction` 本身只是入队 push（无耗时阶段），不加 breakdown 日志。

### 4.2 输出示例

```
==================================================
  processSimulationTask
==================================================
  queueWait          n=  423  P50=    0.02ms  P90=    0.10ms  P99=    1.50ms  avg=    0.05ms  max=    5.23ms
  p2pCall            n=  423  P50=  120.50ms  P90=  450.20ms  P99=  950.10ms  avg=  180.30ms  max= 1200.50ms
  total              n=  423  P50=  121.00ms  P90=  452.00ms  P99=  952.00ms  avg=  181.00ms  max= 1205.00ms

==================================================
  StartSimulateCXTransaction
==================================================
  callMembers        n=  423  P50=   80.00ms  P90=  300.00ms  P99=  600.00ms  avg=  120.00ms  max=  800.00ms
  aggregate          n=  423  P50=    0.01ms  P90=    0.05ms  P99=    0.20ms  avg=    0.02ms  max=    0.50ms
  ...

==================================================
  RetryCommit
==================================================
  passiveCheck       n= 1394  P50=    0.01ms  P90=    0.05ms  P99=    0.20ms  avg=    0.02ms  max=    0.50ms
  patchPoolCheck     n= 1394  P50=    0.05ms  P90=    0.20ms  P99=    1.00ms  avg=    0.10ms  max=    3.00ms
  tryLock            n= 1394  P50=    0.10ms  P90=    0.50ms  P99=    5.00ms  avg=    0.30ms  max=   10.00ms
  stateDbCheck       n= 1394  P50=    0.50ms  P90=    2.00ms  P99=   10.00ms  avg=    1.00ms  max=   20.00ms
  total              n= 1394  P50=    1.00ms  P90=    3.00ms  P99=   15.00ms  avg=    1.50ms  max=   25.00ms
```

---

## 五、预期收益

| 维度 | 现状 | 埋点后 |
|------|:----:|:------:|
| 延迟定位 | 只能看 `closeTx` max=5s | 可定位到模拟/重试管线任一段 |
| 队列积压检测 | 无 | `queueWait` 揭示 worker 是否饱和 |
| P2P 瓶颈识别 | 仅 stats 总量 | `p2pCall` P50/P90 量化透传延迟 |
| 锁竞争瓶颈 | 仅 `chainRetryStats` 计数器 | `RetryCommit` 的 tryLock / stateDbCheck 分段量化 |
| 链式重试效率 | 只有 `chainRetryStats`（计数） | `chainNextSim` 的 scanPool / reserveSelect / sendSignals 分段 |
| 重试编排开销 | 无 | `tryToReSimulation` 拆分为 retryCalls / resultCheck / triggerSim |
| 被动池效果 | 只有 `passiveAdd/Waken/Timeout` | `tryToReSimulation` 的 triggerSim 区分成功/进池分支 |
| 分析效率 | 每轮实验手动 grep 拼凑 | `analyze-all-timing.py` 一站式输出全部 19 个 LOG_MARKERS |

实验配置覆盖的延迟区间预期（shard=4, validator=4, ssc=1, delay=10, rate=100, vpn=4）：

| 阶段 | 预期 P50 | 预期上限 | 说明 |
|------|:--------:|:--------:|------|
| queueWait | < 1ms | 100ms | 队列正常情况下应瞬发 |
| p2pCall | 50-200ms | 1-5s | 取决于网络+leader 处理速度 |
| callMembers | 50-150ms | 1-3s | 并行 + 等待集合 |
| aggregate | < 0.1ms | 10ms | 纯本地聚合 |
| thresholdSign | 10-50ms | 1s | 并行签名收集 |
| buildSignatures | 10-50ms | 1s | 同上 |
| submitTx | 1-10ms | 100ms | 本地交易池提交 |
| extractRWSet | 0.1-1ms | 10ms | SimulationCallStates 遍历 |
| poolInsert | 0.01-0.1ms | 1ms | mu.Lock + map insert |
| scanPool (chainNextSim) | 0.5-5ms | 50ms | RLock + 遍历 retryPool |
| retryCalls | 20-100ms | 1s | 并行 RPC 到 all related shards |
| tryLock | 0.05-0.5ms | 5ms | TryLockWithPriority |
| stateDbCheck | 0.1-1ms | 10ms | stateDB.CheckLock 遍历 WriteSet |

---

## 六、实验验证方案

1. **编译部署**：加埋点 → scp 到 199 → 编译 → 部署实验
2. **数据收集**：`test_single` 跑一轮（shard=4, validator=4, rate=100, delay=10）
3. **一站式分析**：日志目录下 `../analyze-all-timing.py`
4. **校验标准**：
   - 19 个 LOG_MARKERS 都有非零样本数（至少 10 个新埋点都有数据）
   - RetryCommit 样本数 > chainNextSim 样本数（多次 retry = 多次 RetryCommit）
   - tryToReSimulation 样本数 ≈ retry lock success + retry lock fail 之和
   - `total ≈ 子阶段之和` 校验（累积值应约等于各字段 max）
5. **发现瓶颈**：看哪个阶段的 P90/P99/max 异常高，决定下一轮分解方向

---

## 七、变更清单

| 文件 | 改动 |
|:-----|:-----|
| `ssc/simulator.go` | `processSimulationTask` 加 timing breakdown 日志（queueWait / p2pCall / total） |
| `ssc/simulator_leader.go` | `StartSimulateCXTransaction` 加内部 timing breakdown（callMembers / aggregate / thresholdSign / commitSend / total） |
| `ssc/impl.go` | `CommitSimulation` 加 timing breakdown（buildCallStates / buildSignatures / onChainPatch / submitTx / total） |
| `ssc/impl.go` | `HandleCommitVote` 加 timing breakdown（voteTracking / aggregate / sendVote / total） |
| `ssc/retry_scheduler.go` | `AddToRetry` 加 timing breakdown（extractRWSet / poolInsert / total） |
| `ssc/retry_scheduler.go` | `chainNextSim` 加 timing breakdown（scanPool / reserveSelect / sendSignals / total） |
| `ssc/retry_scheduler.go` | `HandleRetrySignal` 加 timing breakdown（setPatch / poolLookup / dispatch / total） |
| `ssc/retry_scheduler.go` | `tryToReSimulation` 加 timing breakdown（inFlightCheck / retryCalls / resultCheck / triggerSim / total） |
| `ssc/retry_scheduler.go` | `RetryCommit` 加 timing breakdown（passiveCheck / patchPoolCheck / tryLock / stateDbCheck / total） |
| `ssc/simulator_leader.go` | `StartReSimulation` 加 timing breakdown（getState / callMembers / aggregate / thresholdSign / commitSend / total） |
| `analyze-all-timing.py` | LOG_MARKERS 新增 **10** 条，总计 **19** 个 marker |
