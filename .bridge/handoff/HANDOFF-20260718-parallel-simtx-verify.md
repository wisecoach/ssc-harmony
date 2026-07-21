HANDOFF-20260718-parallel-simtx-verify (续)
>
> from_session: 当前 Designer session
> from_role: Designer
> to_role: Designer (新 session)
> 焦点: SimTx 并行验证实现

## 代码状态

**已完成并编译通过：**

| 文件 | 改动 | 
|:-----|:------|
| `ssc/api/types.go` | `ShardSimulateCommitteeConfig` 新增 `EnableParallelBatch bool` |
| `ssc/api/sscs.go` | `InternalService` 新增 `BatchVerifySimulations(simulations []CXTSimulation, ...)` + `IsParallelBatchEnabled() bool` |
| `ssc/verify.go` | `VerifySimulation` 拆出 `verifySimulationParsed` |
| `ssc/verify.go` | 新增 `BatchVerifySimulations`（Phase 0-0.5 冲突仲裁 + 调 `batchVerifyPassed`） |
| `ssc/verify.go` | 新增 `batchVerifyPassed`（批量相位验证骨架，有编译错）|
| `ssc/verify.go` | 新增 `extractRWSet` 辅助函数 |
| `ssc/verify.go` | 新增 `IsParallelBatchEnabled` |
| `core/vm/sscvm.go` | 导出 `Run()` 方法 |
| `core/vm/sscis_execution_verify.go` | ExecutionVerify 指令集优化 |
| `ssc/state_locker.go` | CheckLock 优化 |

**batchVerifyPassed 编译错误（待修）：**

```
ssc/verify.go:1206:42: cs.RWSet.WriteState.Copy undefined
  → 参考 verifySimulationParsed 中 subCtx 的 currentState 创建方式
ssc/verify.go:1207:23: undefined: callFrame
  → 参考 verifySimulationParsed 中 subCtx 的 callFrame 创建方式
ssc/verify.go:1272:34: undefined: api.VERIFIED
  → 检查 api 包中的状态常量名
```

**待实现（P0→P3）：**

| P | 项 | 文件 |
|:-:|:---|:-----|
| 0 | 修 batchVerifyPassed 编译错 | `ssc/verify.go` |
| 1 | Precompile 检测 `EnableParallelBatch` 跳过 verify | `core/vm/ssc_contracts_write.go` |
| 2 | Worker `CommitTransactions` 并行分支 | `node/worker/worker.go` |
| 3 | MischiefProxy 代理方法 | `ssc/mischief/proxy.go` |
| 4 | state_processor.go 并行 | `core/state_processor.go` |

## 设计文档

- `docs/designs/active/DSN-33-verifycontext-cleanup.md` ✅
- `docs/designs/active/DSN-34-parallel-exec-verify.md` ✅
- `docs/designs/active/DSN-35-batch-parallel-verify.md` ✅（终版）

## 实验数据

rate=150 实验（并行 execVerify + Copy，无 batch）：

## 当前实验环境

| 服务 | 值 |
|:-----|:----|
| 远程编译机 | zjnu@10.7.95.199 -p 10022 |
| 项目路径 | `~/go/src/github.com/harmony-one/harmony-sscc/` |
| 日志目录 | `~/go/src/github.com/harmony-one/logs/harmony-sscc/` |
| 实验结果 | `data_process/output/throughput/` |
| 测试命令 | `ssh → auto_test.sh → test_single` |
| 日志分析 | `ssc_grep.sh` / `analyze-*.py` |

## 背景数据

### SimTx 耗时分布（rate=100，已验证）

| 阶段 | avg | 占比 | 能否并行 |
|:----|:---:|:----:|:--------:|
| lockCheck | 2.56ms | 46% | ❌ 串行（读 stateDB 锁状态，依赖前序） |
| execVerify | 2.69ms | 48% | ✅ 可并行（EVM 快照执行，read-only on trie） |
| lockState | 0.12ms | 2% | ❌ 串行（写 stateDB） |
| post-lock | 0.14ms | 3% | ✅ 已 async |
| chainPatch | 0.03ms | 0.5% | ❌ 串行 |

### Shard 3 瓶颈（rate=100）

Shard 3 的 execVerify=5.22ms，是 Shard 1（1.16ms）的 **4.5x**。原因：Shard 3 收到更多计算量大的 SSC 合约调用。

### rate=200 现状

| Shard | 预算命中率 | 最后20块 avg commitTxs |
|:----:|:---------:|:---------------------:|
| 3 | **52%** | **1,174ms**（超出预算 17%） |
| 0 | 28% | 669ms |
| 1 | 20% | 544ms |
| 2 | 5% | 753ms |

PoolTimeout=358（2.1%），系统在 capacity 边界。

## 并行验证设计方案（待讨论 + 落地）

### 核心思路

将 VerifySimulation 拆成三段，execVerify 用 goroutine 并行：

```
Phase 1（串行）: lockCheck 全部 SimTx → 收集"通过集"
Phase 2（并行）: 通过集的 execVerify 同时跑（goroutine × N）
Phase 3（串行）: 逐笔 lockState + post-lock
```

### 预期收益

| Shard | 串行（150 SimTxs） | 并行 Phase 2 | 降幅 |
|:----:|:-----------------:|:------------:|:----:|
| 普通 | 150×5.51=826ms | 150×2.56+2.69+150×0.26=**425ms** | **-48%** |
| **Shard 3** | **150×6.55=1,206ms** | **150×2.86+5.22+150×0.33=727ms** | **-40%** |

Shard 3 降到 727ms，低于 1s 预算，不再触发截断。

### 需要实现的内容

**1. 拆 `verify.go` 的 VerifySimulation 为三段**

```
VerifySimulation (原始体) →
    VerifyLockCheck(sim) bool          // 只做 lockCheck，返回是否通过
    VerifyExec(sim) (result, error)    // 只做 execVerify，可并行
    VerifyLockState(sim, result)       // 只做 lockState + post-lock
```

边界情况处理：
- lockCheck 失败 → 走 retry 路径（跟现状一样）
- execVerify 失败 → 发回滚投票（goroutine 内处理）
- Phase 3 lockState 冲突 → 走 retry 路径

**2. 改 `worker.go` 的 CommitSSCTransactions 循环**

```go
// Phase 1: 串行 lockCheck
var ready []tx
for {
    if !lockCheck(tx) { continue }
    ready = append(ready, tx); txs.Shift()
    if budget exceeded { break }
}

// Phase 2: 并行 execVerify
var wg sync.WaitGroup
results := make([]result, len(ready))
for i, tx := range ready {
    wg.Add(1); go func(i int, tx T) {
        defer wg.Done()
        results[i] = execVerify(tx)
    }(i, tx)
}
wg.Wait()

// Phase 3: 串行 lockState
for i, tx := range ready {
    if results[i].ok { lockState(tx); postLock(tx) }
}
```

**3. 与时间预算的协同**

Phase 1 前算好 deadline。Phase 1 截断 → 剩余 SimTx 留到下个块。Phase 2/3 不受预算限制（已在通过集内）。

## 待办清单

- [ ] 讨论并行验证设计细节（是否需要 DSN，还是直接落地）
- [ ] 拆 `verify.go` 为三段（~80 行）
- [ ] 改 `worker.go` 循环逻辑（~30 行）
- [ ] 同步到 199 跑实验验证
- [ ] 对比 rate=100 和 rate=200 的吞吐量变化

## 关键路径 / 参考文档

| 路径 | 说明 |
|:-----|:------|
| `ssc/verify.go` | VerifySimulation 单体，需要拆三段 |
| `node/worker/worker.go` | CommitSSCTransactions 循环，需要重组 |
| `docs/designs/active/DSN-32-commit-txs-time-budget.md` | 时间预算设计文档 |
| `ssc/state_locker.go:480` | 已修复的 globalLockedStates 泄漏 |

## 已知坑

- `stateDB.GetState()` 支持并发读（trie 层）✅
- `verifyExecuteForCallState` 写临时内存对象，不写 stateDB ✅
- 拆三段时注意 chainPatch 逻辑属于 Phase 1，不可并行
- 并行 goroutine 内如果 execVerify 失败需要发回滚投票，goroutine 安全
- Phase 3 lockState 顺序必须与 Phase 1 的 pass 顺序一致（避免死锁）
