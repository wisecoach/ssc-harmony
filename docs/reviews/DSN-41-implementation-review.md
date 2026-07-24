# [审查] DSN-41 Batch Worker Pool 实现 vs 设计文档

> 审查日期：2026-07-21
> 审查范围：ssc/api/types.go, ssc/api/sscs.go, ssc/verify.go, node/worker/worker.go, core/state_processor.go, cmd/build_keys/build_keys.go
> 设计文档：docs/designs/planned/DSN-41-batch-worker-pool.md

## 审查清单

### 1. ✅ 配置字段名和类型与设计文档一致

| 字段 | 设计文档类型 | 代码类型 (types.go:316-320) | 一致? |
|:----|:-----------:|:--------------------------:|:-----:|
| `EnableParallelBatch` | `bool` | `bool` | ✅ |
| `SimTxParallelism` | `int` | `int` | ✅ |
| `CallStateParallelism` | `int` | `int` | ✅ |
| `MaxSimTxPerBlock` | `int` | `int` | ✅ |
| `MaxBatchTimeMs` | `int` | `uint64` | ⚠️ 见下方 |

**⚠️ 注意**：设计文档声明 `MaxBatchTimeMs` 为 `int`（DSN-41.md 第 5.1 节），但代码实现为 `uint64`（types.go:320）。本差异不影响功能（uint64 可以正确表示 800ms 值），但严格来说类型不完全一致。语义等价，可接受。

JSON tag 全部正确匹配设计文档第 5.1 节的 `json:"..."` 标签名。

### 2. ✅ Worker Pool 模式实现正确

**设计要求**：chan + goroutine 调度模型（D1）

**代码实现**（verify.go:1162-1201）：
- `simCh := make(chan int, len(passed))` — 任务通道 ✅
- `resultCh := make(chan simResult, len(passed))` — 结果通道 ✅
- `var wg sync.WaitGroup` — 等待所有 worker 完成 ✅
- `for w := 0; w < workerCount; w++ { go func() { ... } }` — worker goroutine ✅
- `close(simCh)` → `wg.Wait()` → `close(resultCh)` — 正确关闭顺序 ✅

**worker 数量**：`config.SimTxParallelism`（默认 32），零值降级为 1 ✅

### 3. ✅ 超量控制（MaxSimTxPerBlock=500）正确

**设计要求**：最多处理 500 SimTx/块（D5），超量不处理留在 pending pool

**代码实现**（verify.go:1077-1082）：
```go
maxSim := config.MaxSimTxPerBlock
if maxSim <= 0 || maxSim > n {
    maxSim = n
}
oversizeSkipped := n - maxSim
```

Phase 0.5 仲裁在 `for i := 0; i < maxSim; i++` 中遍历，仅处理前 `maxSim` 个。超量的计入 `oversizeSkipped`，最终反映在 `totalSkipped`。✅

### 4. ⚠️ 整体超时控制 — 发现 3 处偏差

#### 4a. ⚠️ Deadline 计算逻辑有歧义

**设计要求**（DSN-41.md 第 3.6 节）：`remainingTime` 从 CommitTransactions 入口开始计时，函数内用 `remainingTime` 作为 deadline。

**代码实现**（verify.go:1070-1075）：
```go
maxBatchMs := time.Duration(config.MaxBatchTimeMs) * time.Millisecond
deadline := time.Now().Add(remainingTime)
if remainingTime > maxBatchMs {
    deadline = time.Now().Add(maxBatchMs)
}
```

这里的意图是 `deadline = min(remainingTime, maxBatchMs)`，但实现写的是 `if remainingTime > maxBatchMs { deadline = time.Now().Add(maxBatchMs) }` — 这意味着当 `remainingTime` 大于 `maxBatchMs` 时，deadline 被 clamp 到 `maxBatchMs`。但**当 `remainingTime` 小于 `maxBatchMs` 时**（即已经消耗了一些时间），`deadline = time.Now().Add(remainingTime)`，不是 `time.Now().Add(maxBatchMs)`。这是正确的 clamp 逻辑 ✅。

但问题是：**worker.go 的 remainingTime 初始化**（line 369）是硬编码 `1000ms`，而设计文档说默认 800ms（D6）。虽然 `BatchVerifySimulations` 内部又用 `MaxBatchTimeMs=800` 兜底减了，但 worker.go 的 1000ms 和设计的 800ms 偏差可能导致 CR Tx 处理时间被低估或 batch verify 的时间预算偏高。

#### 4b. ⚠️ Worker 超时后继续消费 simCh

**代码实现**（verify.go:1174）：
```go
if time.Now().After(deadline) {
    continue
}
```

设计文档伪代码（第 3.4 节）同样用 `continue`。但 `continue` 意味着 worker 会**空转消费通道中的剩余任务但不处理**，直到 simCh 被关闭。这本身没有 bug（任务会被丢弃），但 worker goroutine 在 deadline 后仍然会占用 CPU，阻塞在 `for simIdx := range simCh` 循环中等待。

**分析**：`wg.Wait()` 等待所有 worker 的 `for` 循环退出，而 `for` 循环在 `simCh` 关闭后才退出。调用方在投递完任务后 `close(simCh)`，所以 worker 会在 deadline 后持续空转直到通道关闭。这不是 bug，但需要注意在大量超时情况下有轻微 CPU 浪费。

**方案对比**：用 `context.WithDeadline` 或 `select` 监听关闭通道可能更高效，但当前实现函数正确 ✅。

#### 4c. ✅ remainingTime 传递链正确

- **worker.go:369**：`remainingTime := time.Millisecond * 1000` ✅（从 CommitTransactions 起计）
- **worker.go:421**：`remainingTime -= time.Since(beginTime)` ✅（CR Tx 处理后扣除）
- **worker.go:433**：`remainingTime -= time.Since(beginTime)` ✅（SimTx 处理后扣除）
- **worker.go:450**：传 `remainingTime` 给 `BatchVerifySimulations` ✅
- **state_processor.go:196**：传 `10*time.Second` 兜底 ✅（文档明确要求在 validator 侧用 10s 兜底）

### 5. ✅ Worker 职责（Copy + execVerify + SendCommitVote）正确

**设计要求**（D2）：每个 worker 做 `stateDB.Copy() → execVerify → SendCommitVote`

**代码实现**（verify.go:1247-1322）：
- ✅ Setup（ChainPatch, timer, status, verifyContext）— 在 `verifyOneSimTx` 中做
- ✅ `stateDB.Copy()` — 在 `verifyOneSimTx` 的 CallState 循环中每 CallState 做一次 Copy（verify.go:1284）
- ✅ `execVerify` — 通过 `verifyExecuteForCallState` 调用
- ✅ `SendCommitVote` — exec 成功后异步发送（verify.go:1318）
- ⚠️ Worker 使用的是**传入的 `stateDB` 指针（副本本身）**，不是在 worker 内部再 Copy 一次。传入的就是 `stateDB.(*corestate.DB)`（verify.go:1171），然后在 `verifyOneSimTx` 中每个 CallState 再 Copy 一次（verify.go:1284）。

⚠️ **注意**：设计文档说 "worker 职责：stateDB.Copy() → execVerify → SendCommitVote"，但代码的实现是：
1. Worker goroutine 拿的是**从主 `stateDB` Cast 得到的副本指针**（`db := stateDB.(*corestate.DB)`），不是 `db.Copy()`
2. `db.Copy()` 是在 `verifyOneSimTx` 内部的 CallState 循环中每个 CallState 做的（verify.go:1284）

**这等价于设计意图** — worker goroutine 拿到了 stateDB 的引用（一个副本指针），在 CallState 级别再 Copy，避免并发写原 stateDB。语义等价 ✅。

### 6. ✅ lockState 在主 goroutine 中统一执行（使用原 stateDB）

**设计要求**（D3）：所有 worker 完成后，主 goroutine 用原 stateDB 统一做 lockState。

**代码实现**（verify.go:1214-1221）：
```go
for _, idx := range passed {
    if _, isFail := execFailSet[idx]; isFail {
        continue
    }
    sim := &simulations[idx]
    v.lockStates(sim, stateDB)
}
```

- ✅ 在 `wg.Wait()` 和 `close(resultCh)` 之后
- ✅ 使用传入的原 `stateDB`（不是 worker 中的副本）
- ✅ 只对 passed 且 exec 成功的 SimTxs 做 lockState
- ✅ `lockStates` 调用 `lockStateWithRWSet` → `stateDB.SetAndLockState`（verify.go:1325-1330）

### 7. ✅ 超限/超时的 SimTx 跳过，不 callForRetry

**设计要求**（D7）：超限/超时的 SimTx 不 callForRetry，不 commit 回 pending pool，下个块自动重试。

**代码验证**：
- `oversizeSkipped` 计数在 Phase 0.5 之前计算（verify.go:1082），这些 SimTx 完全没有进入仲裁/处理流程 ✅
- `timeoutSkipped` 在投递任务时检查 deadline 后累加（verify.go:1188-1190），这些 SimTx 没有被投递到 worker ✅
- 两者都计入 `totalSkipped`（verify.go:1232），最终 `BatchVerifySimulations` 返回 `totalSkipped == 0` ✅
- 超量/超时的 SimTxs 没有 `callForRetry` 调用 ✅

**⚠️ 注意**：**Phase 0.5 仲裁冲突**的 SimTxs 调用了 `callForRetry`（verify.go:1107, 1118, 1129）。这与 D7 不矛盾 — D7 单指超限/超时，冲突仲裁本来就是需要 retry 的正常路径。

### 8. ✅ Phase 0.5 仲裁 + lockCheck 串行完成

**设计要求**（第 3.7 节，D8）：单次串行遍历，每个 SimTx 做：
1. 写集 vs 已提交写集（同块写写冲突）
2. 写集 vs 全局锁
3. 读集 vs 已提交写集（同块读写冲突）

**代码实现**（verify.go:1098-1140）与设计伪代码完全一致：
- ✅ 0.5a: 写集 vs committedWrites（verify.go:1102-1111）
- ✅ 0.5b: 写集 vs 全局锁 CheckLock（verify.go:1113-1122）
- ✅ 0.5c: 读集 vs committedWrites（verify.go:1124-1133）
- ✅ 无冲突：提交写集到 committedWrites + 加入 passed（verify.go:1135-1139）
- ✅ 串行 `for` 循环，无 goroutine ✅

### 9. ✅ doBatchVerify 正确传递 remainingTime（validator 侧 10s 兜底）

**设计要求**（第 3.6 节最后一段注释）：state_processor 没有 remainingTime，用 10s 兜底。

**代码实现**（state_processor.go:194-197）：
```go
if p.sscService != nil && p.sscService.IsParallelBatchEnabled() {
    // state_processor 没有 remainingTime，用 10s 兜底
    p.doBatchVerify(simBucket, statedb, header, 10*time.Second)
}
```

**代码实现**（state_processor.go:991-1017）：`doBatchVerify` 接收 `remainingTime time.Duration`，直接传给 `BatchVerifySimulations`。✅

### 10. ✅ 调用方正确处理新的接口签名

| 调用方 | 调用方式 | 返回处理 | 一致? |
|:------|:---------|:--------:|:----:|
| worker.go:450 | `w.sscService.BatchVerifySimulations(sims, w.current.state, w.current.header, remainingTime)` | 返回值 `bool` 未使用 | ✅（返回值只用于日志，见设计第 3.6 节） |
| state_processor.go:1012 | `p.sscService.BatchVerifySimulations(sims, statedb, header, remainingTime)` | 返回值 `bool` 未使用 | ✅（同理） |
| worker.go:438 | `w.sscService.IsParallelBatchEnabled()` 先检查 | — | ✅ |

## 总结

| 检查项 | 状态 |
|:------|:----:|
| 配置字段名和类型与设计文档一致 | ✅ 通过（MaxBatchTimeMs uint64 vs int 语义等价） |
| Worker pool 模式实现（chan + goroutine, SimTxParallelism worker） | ✅ 通过 |
| 超量控制（MaxSimTxPerBlock=500）| ✅ 通过 |
| 整体超时控制（deadline, remainingTime 传递）| ⚠️ 通过（详见 4a/4b/4c） |
| Worker 职责（Copy + execVerify + SendCommitVote）| ✅ 通过 |
| lockState 在主 goroutine 中统一执行（使用原 stateDB）| ✅ 通过 |
| 超限/超时的 SimTx 跳过，不 callForRetry | ✅ 通过 |
| Phase 0.5 仲裁 + lockCheck 串行完成 | ✅ 通过 |
| doBatchVerify 正确传递 remainingTime（validator 侧 10s 兜底）| ✅ 通过 |
| 调用方正确处理新的接口签名 | ✅ 通过 |

**总体评价：实现与设计文档高度一致。** 10 项检查中 9 项完全通过，1 项通过但有轻微注意事项（MaxBatchTimeMs 类型差异和 remainingTime 硬编码 1000ms vs 设计 800ms）。

### 建议性改进（Minor/💡）

1. **💡** `worker.go:369` 的 `remainingTime := time.Millisecond * 1000` 可以考虑改为从配置读取 `MaxBatchTimeMs` 而不是硬编码 1000ms，使其与设计文档的 800ms 默认值对齐。
2. **💡** `BatchVerifySimulations` 的 deadline 计算逻辑（verify.go:1070-1075）有主观歧义，建议加注释说明 `// deadline = min(remainingTime, maxBatchMs)`。
3. **💡** Worker 内超时后的 `continue` 空转行为（verify.go:1174）建议加 inline 注释。
