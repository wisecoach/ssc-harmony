# Timing Breakdown 日志大全

> **参考**：DSN-20-simulation-timing-instrumentation.md（10 处新增埋点设计）
> **已有**：CR/closeTransaction 9 处已有埋点（EXP-01, DSN-17, DSN-19 产物）
> **总计**：19 个 timing breakdown 日志消息

所有 timing breakdown 日志统一走 Info 级别，使用 `time.Since(t0)` 累积值（非增量），字段用 `Str()` 序列化为 `"1.234ms"` 格式。

---

## 1. 已有埋点（9 处）

### 1.1 `CommitOrRollbackWithProof timing breakdown`

| 属性 | 值 |
|:-----|:----|
| **文件** | `ssc/committer.go` |
| **触发** | 每次链上执行 CR 交易（leader + validator 均产生） |
| **grep** | `"CommitOrRollbackWithProof timing breakdown"` |

**字段**：

| 字段 | 含义 | 典型值 |
|------|------|:------:|
| `unmarshal` | json.Unmarshal commitProof 字节 | 10-50µs |
| `isFinished` | IsTxFinished 检查 | 1-5µs |
| `commitTx` | stateDB.CommitTx/RollbackTx（txLock 抢锁 + 解锁所有 key） | 0.1-5ms |
| `closeTx` | SetStatus + CloseTx 总耗时（见下一级 closeTransaction） | 0.1-1500ms |
| `total` | 全部（含 setCxtStage + RemoveOnChainPatch） | 0.2-1500ms |

### 1.2 `closeTransaction timing breakdown`

| 属性 | 值 |
|:-----|:----|
| **文件** | `ssc/impl.go` |
| **触发** | 每次 commit or rollback 后的 cleanup 阶段 |
| **grep** | `"closeTransaction timing breakdown"` |

**字段**：

| 字段 | 含义 | 典型值 |
|------|------|:------:|
| `stateLock` | stateLock 抢锁 + 释放 | 0.05-500ms (DSN-18 后已优化到 0.01ms) |
| `simCleanup` | Simulator.Cleanup | 0.1-500ms |
| `verCleanup` | commitLock + Verifier.Cleanup | 0.1-500ms |
| `totalClose` | 全部（含 timerMgr + retryScheduler + patchPool + passivePool） | 0.2-1500ms |

### 1.3 `Simulator.Cleanup timing`

| 属性 | 值 |
|:-----|:----|
| **文件** | `ssc/simulator.go` |
| **触发** | `closeTransaction` → `Simulator.Cleanup` 内部 |
| **grep** | `"Simulator.Cleanup timing"` |

**字段**：

| 字段 | 含义 | 典型值 |
|------|------|:------:|
| `delSimState` | DeleteSimState 耗时 | 0.01-0.5ms |
| `pendingLock` | pendingLock 抢锁 + delete pendingRequests | 0.01-0.5ms |
| `simuLock` | simuLock 抢锁 + delete simuResultCh/simuWaitingChs（DSN-19 后去掉全局锁） | 0.01-0.5ms |

### 1.4 `Verifier.Cleanup timing`

| 属性 | 值 |
|:-----|:----|
| **文件** | `ssc/verify.go` |
| **触发** | `closeTransaction` → `Verifier.Cleanup` |
| **grep** | `"Verifier.Cleanup timing"` |

**字段**：`duration`（总耗时）

### 1.5 `retryScheduler.StaleTx timing`

| 属性 | 值 |
|:-----|:----|
| **文件** | `ssc/retry_scheduler.go` |
| **触发** | `closeTransaction` → `StaleTx`（包含 GarbageCollect + mu.Lock） |
| **grep** | `"retryScheduler.StaleTx timing"` |

**字段**：`duration`（总耗时）

### 1.6 `RemoveOnChainPatch timing`

| 属性 | 值 |
|:-----|:----|
| **文件** | `ssc/retry_scheduler.go` |
| **触发** | `CommitOrRollbackWithProof` 最后阶段 `RemoveOnChainPatch` |
| **grep** | `"RemoveOnChainPatch timing"` |

**字段**：`duration`（总耗时）

### 1.7 `CXTTimerManager.RemoveTx timing`

| 属性 | 值 |
|:-----|:----|
| **文件** | `ssc/cxt_timer.go` |
| **触发** | `closeTransaction` → `timerMgr.RemoveTx` |
| **grep** | `"CXTTimerManager.RemoveTx timing"` |

**字段**：`duration`（总耗时）

### 1.8 `PatchPool.Remove timing`

| 属性 | 值 |
|:-----|:----|
| **文件** | `ssc/api/types.go` |
| **触发** | `closeTransaction` → `patchPool.Remove` |
| **grep** | `"PatchPool.Remove timing"` |

**字段**：`duration`（总耗时）
**说明**：迭代 WriteState key 清理 KeyIndex，锁持有时间长。

### 1.9 `RemoveFromPassivePool timing`

| 属性 | 值 |
|:-----|:----|
| **文件** | `ssc/retry_scheduler.go` |
| **触发** | `closeTransaction` → `RemoveFromPassivePool` |
| **grep** | `"RemoveFromPassivePool timing"` |

**字段**：`duration`（总耗时）

---

## 2. 新增埋点（10 处）— DSN-20

### 2.1 `processSimulationTask timing breakdown`

| 属性 | 值 |
|:-----|:----|
| **文件** | `ssc/simulator.go`（`processSimulationTask` 函数末尾） |
| **触发** | worker 出队后完成整个模拟任务（dequeue → RPC Call→leader → 返回） |
| **grep** | `"processSimulationTask timing breakdown"` |

**字段**：

| 字段 | 含义 | 计算方式 | 预期 P50 |
|------|------|:--------:|:--------:|
| `queueWait` | 队列等待时间（push→pop） | `time.Since(task.pushTime)` at pop | < 1ms |
| `p2pCall` | RPC 调用耗时 | `time.Since(p2pStart)`（已有 stats） | 50-200ms |
| `total` | 总耗时 | `time.Since(startTime)`（已有） | 50-200ms |

### 2.2 `StartSimulateCXTransaction timing breakdown`

| 属性 | 值 |
|:-----|:----|
| **文件** | `ssc/simulator_leader.go`（`StartSimulateCXTransaction` return 前） |
| **触发** | leader 端收到 simulation request，协调全部分片执行 |
| **grep** | `"StartSimulateCXTransaction timing breakdown"` |

**字段**：

| 字段 | 含义 | 锚点 | 预期 P50 |
|------|------|:----:|:--------:|
| `callMembers` | 并行调 HandleSimulateRequest | start → wg.Wait | 50-150ms |
| `aggregate` | aggregateSimulationResults | 函数调用前后 | < 0.1ms |
| `thresholdSign` | thresholdSignSimulationCommit | 函数调用前后 | 10-50ms |
| `commitSend` | Multicast CommitSimulation | 函数调用前后 | 10-30ms |
| `total` | 总耗时 | 已有 `startTime` | 100-300ms |

### 2.3 `CommitSimulation timing breakdown`

| 属性 | 值 |
|:-----|:----|
| **文件** | `ssc/impl.go`（`CommitSimulation` 末尾） |
| **触发** | 模拟结果上链（SimTx 提交） |
| **grep** | `"CommitSimulation timing breakdown"` |

**字段**：

| 字段 | 含义 | 锚点 | 预期 P50 |
|------|------|:----:|:--------:|
| `buildCallStates` | BuildCallStates + writeSet 构建 | line 766 → 814 | 0.5-2ms |
| `buildSignatures` | buildSignaturesForSimulation | line 819 | 10-50ms |
| `onChainPatch` | AddOnChainPatch | line 823 | 0.05-0.2ms |
| `submitTx` | SubmitSimulationTx | line 825 | 1-10ms |
| `total` | 总耗时 | 函数入口 | 15-80ms |

### 2.4 `HandleCommitVote timing breakdown`

| 属性 | 值 |
|:-----|:----|
| **文件** | `ssc/impl.go`（`HandleCommitVote` 末尾 return 前） |
| **触发** | 收到来自其他 shard 或本 shard 成员的一票 Commit/Rollback 投票 |
| **grep** | `"HandleCommitVote timing breakdown"` |

**字段**：

| 字段 | 含义 | 锚点 | 预期 P50 |
|------|------|:----:|:--------:|
| `voteTracking` | commitLock + vote map 操作 | 匿名函数入口 → aggregate 前 | 0.05-0.5ms |
| `aggregate` | aggregateSSCCommitVote | 函数调用前后 | 0.01-0.1ms |
| `sendVote` | RPC to origin shard leader | `Comm.Call` 前后 | 5-20ms |
| `total` | 总耗时 | 函数入口 | 5-25ms |

### 2.5 `AddToRetry timing breakdown`

| 属性 | 值 |
|:-----|:----|
| **文件** | `ssc/retry_scheduler.go`（`AddToRetry` 末尾） |
| **触发** | 交易被加入 retry pool |
| **grep** | `"AddToRetry timing breakdown"` |

**字段**：

| 字段 | 含义 | 锚点 | 预期 P50 |
|------|------|:----:|:--------:|
| `extractRWSet` | 从 SimulationCallStates 提取 RWSet | 入口 → `tx.ReadSet = readSet` | 0.1-1ms |
| `poolInsert` | mu.Lock + retryPool insert + stale check | `rs.mu.Lock()` → 函数出口 | 0.01-0.1ms |
| `total` | 总耗时 | 函数入口 | 0.1-1ms |

### 2.6 `chainNextSim timing breakdown`

| 属性 | 值 |
|:-----|:----|
| **文件** | `ssc/retry_scheduler.go`（`chainNextSim` 末尾 return 前） |
| **触发** | SimTx 提交后，扫描 retryPool 找下游依赖 |
| **grep** | `"chainNextSim timing breakdown"` |

**字段**：

| 字段 | 含义 | 锚点 | 预期 P50 |
|------|------|:----:|:--------:|
| `scanPool` | RLock + 遍历 retryPool 匹配依赖 | 入口 → `rs.mu.RUnlock()` | 0.5-5ms |
| `reserveSelect` | 预留 + 选择不冲突的 retryTx | line 563 → 588 | 0.1-1ms |
| `sendSignals` | 逐个发送 chain signal | line 591 → return | 5-50ms |
| `total` | 总耗时 | 函数入口 | 5-60ms |

### 2.7 `HandleRetrySignal timing breakdown`

| 属性 | 值 |
|:-----|:----|
| **文件** | `ssc/retry_scheduler.go`（`HandleRetrySignal` 末尾） |
| **触发** | 收到来自 upstream shard 的 chain signal |
| **grep** | `"HandleRetrySignal timing breakdown"` |

**字段**：

| 字段 | 含义 | 锚点 | 预期 P50 |
|------|------|:----:|:--------:|
| `setPatch` | SetChainPatch | line 637-639 | 0.01-0.1ms |
| `poolLookup` | RLock + 查找 retryPool | line 643-645 | 0.01-0.05ms |
| `dispatch` | goroutine launch tryToReSimulation | line 652 | 0.001-0.01ms |
| `total` | 总耗时 | 函数入口 | 0.05-0.5ms |

### 2.8 `tryToReSimulation timing breakdown`

| 属性 | 值 |
|:-----|:----|
| **文件** | `ssc/retry_scheduler.go`（`tryToReSimulation` 各 return 前） |
| **触发** | 尝试重新模拟一笔 retry tx |
| **grep** | `"tryToReSimulation timing breakdown"` |

**字段**：

| 字段 | 含义 | 锚点 | 预期 P50 |
|------|------|:----:|:--------:|
| `inFlightCheck` | reSimInFlight 防重入 | line 754-763 | 0.05-0.5ms |
| `retryCalls` | 并行 RetryCommit 到所有 related shards | line 774-792 (wg.Wait) | 20-100ms |
| `resultCheck` | 结果检查 + wound 检查 | line 794-828 / 835-870 | 0.1-1ms |
| `triggerSim` | TriggerReSimulation / AddToPassivePool | line 828 / 859 | 0.1-1ms |
| `total` | 总耗时 | 函数入口 | 20-120ms |

### 2.9 `StartReSimulation timing breakdown`

| 属性 | 值 |
|:-----|:----|
| **文件** | `ssc/simulator_leader.go`（`StartReSimulation` 末尾 return 前） |
| **触发** | TriggerReSimulation 后，leader 端重新执行模拟（结构与首次模拟一致） |
| **grep** | `"StartReSimulation timing breakdown"` |

**字段**：

| 字段 | 含义 | 锚点 | 预期 P50 |
|------|------|:----:|:--------:|
| `getState` | GetTxState + 构建 req from lastReq | line 645-675 | 0.1-1ms |
| `callMembers` | 并行 HandleSimulateRequest | line 698-743 (wg.Wait) | 50-150ms |
| `aggregate` | aggregateSimulationResults | line 760 | < 0.1ms |
| `thresholdSign` | thresholdSignSimulationCommit | line 858 | 10-50ms |
| `commitSend` | CommitSimulation (local + Multicast) | line 881-890 | 10-30ms |
| `total` | 总耗时 | `startTime` at line 677 | 70-250ms |

### 2.10 `RetryCommit timing breakdown`

| 属性 | 值 |
|:-----|:----|
| **文件** | `ssc/retry_scheduler.go`（`RetryCommit` 所有 return 前） |
| **触发** | 重试路径中获取锁（被动池检查 → PatchPool → TryLock → stateDB.CheckLock） |
| **grep** | `"RetryCommit timing breakdown"` |

**字段**：

| 字段 | 含义 | 锚点 | 预期 P50 |
|------|------|:----:|:--------:|
| `passiveCheck` | 被动池检查 + Wound 检查 | line 947-960 | 0.01-0.05ms |
| `patchPoolCheck` | PatchPool.HasConflict + TryConsume | line 980-996 | 0.05-0.2ms |
| `tryLock` | TryLockWithPriority | line 1007-1017 | 0.05-0.5ms |
| `stateDbCheck` | stateDB.CheckLock 遍历 WriteSet/ReadSet | line 1026-1043 | 0.1-1ms |
| `total` | 总耗时 | 函数入口 | 0.2-2ms |

**注意**：RetryCommit 有多个提前 return（passive wound → patch miss → lock fail → stateDB fail），每个分支都打印 timing log（耗时不同）。

---

## 3. 分析脚本

### 3.1 `analyze-all-timing.py`

| 属性 | 值 |
|:-----|:----|
| **位置** | `scripts/analyze-all-timing.py`（远程：`~/go/src/github.com/harmony-one/logs/harmony-sscc/`） |
| **覆盖** | 全部 19 个 timing breakdown 日志，一站式输出 P50/P90/P99/avg/max |
| **用法** | `../analyze-all-timing.py`（cd 到日志目录后）或 `../analyze-all-timing.py <glob>` |

### 3.2 子级分析脚本

| 脚本 | 覆盖埋点 |
|:-----|:---------|
| `analyze-cr-timing.py` | `CommitOrRollbackWithProof timing breakdown` |
| `analyze-close-timing.py` | `closeTransaction timing breakdown` |
| `analyze-tx-timing.py` | SSC tx / Normal tx commit timing |
| `analyze-total-timing.py` | CR vs SSC vs Normal 三类交易总量占比 |

---

## 4. 快速查询模板

### 4.1 一键查全部耗时

```bash
cd ~/go/src/github.com/harmony-one/logs/harmony-sscc/shard=4_validator=4_ssc=1_delay=10_rate=100_vpn=4/
# 看全部 19 个 timing breakdown
python3 ../analyze-all-timing.py
```

### 4.2 单类 grep

```bash
# 模拟管线
bash ssc_grep.sh "processSimulationTask timing breakdown"
bash ssc_grep.sh "StartSimulateCXTransaction timing breakdown"
bash ssc_grep.sh "CommitSimulation timing breakdown"
bash ssc_grep.sh "HandleCommitVote timing breakdown"

# 重试管线
bash ssc_grep.sh "AddToRetry timing breakdown"
bash ssc_grep.sh "chainNextSim timing breakdown"
bash ssc_grep.sh "HandleRetrySignal timing breakdown"
bash ssc_grep.sh "tryToReSimulation timing breakdown"
bash ssc_grep.sh "StartReSimulation timing breakdown"
bash ssc_grep.sh "RetryCommit timing breakdown"

# 关闭管线（已有）
bash ssc_grep.sh "CommitOrRollbackWithProof timing breakdown"
bash ssc_grep.sh "closeTransaction timing breakdown"
bash ssc_grep.sh "Simulator.Cleanup timing"
bash ssc_grep.sh "Verifier.Cleanup timing"
bash ssc_grep.sh "retryScheduler.StaleTx timing"
```

### 4.3 按交易 hash 跟踪全生命周期

```bash
TXHASH="0x..."
bash ssc_grep.sh "$TXHASH" | grep "timing breakdown"
# 输出示例：
# processSimulationTask timing breakdown  queueWait=0.02ms p2pCall=120ms total=121ms
# StartSimulateCXTransaction timing breakdown  callMembers=80ms aggregate=0.01ms ...
# CommitSimulation timing breakdown  buildCallStates=0.5ms ... submitTx=5ms total=35ms
# HandleCommitVote timing breakdown  voteTracking=0.1ms ... sendVote=10ms total=12ms
# closeTransaction timing breakdown  stateLock=0.01ms simCleanup=0.1ms ...
```

---

## 5. 注意事项

### 5.1 累计值（非增量）

所有 `timing breakdown` 字段使用 `time.Since(t0)` — 到起点的累积值。两个连续字段的 max 接近时（如 stateLock=963ms, simCleanup=965ms），尖峰发生在第一阶段（delta=2ms），第二阶段本身快速。

### 5.2 所有 return 分支都要打印

新增埋点的函数（尤其是 `RetryCommit`、`AddToRetry`、`tryToReSimulation`）有多个提前 return 分支，每个分支都必须打印 timing log。使用 `defer` 在 return 前统一打印可避免遗漏。

### 5.3 `ns` 解析必须在 `s` 之前

Python `parse_dur()` 函数中，`ns` 检查必须在 `s` 之前，否则 `"665ns"` 会误匹配 `endswith('s')` → 崩溃。

```python
def parse_dur(s):
    if not s: return 0
    s = s.strip()
    if s.endswith('ms'):   return float(s[:-2])
    if s.endswith('µs'):   return float(s[:-2]) / 1000
    if s.endswith('ns'):   return float(s[:-2]) / 1_000_000  # ← 必须在前！
    if s.endswith('s'):    return float(s[:-1]) * 1000
    return 0
```
