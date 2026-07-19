# HANDOFF-20260718-parallel-simtx-verify

> from_session: 当前 Designer session
> from_role: Designer
> to_role: Designer (新 session)
> 焦点: SimTx 并行验证设计方案讨论 + 落地

## 已完成

| 事项 | 状态 |
|:-----|:------|
| DSN-32: CommitTxs 时间预算 | 已实现（`worker.go`，1s SimTx / 200ms NormalTx） |
| 修复 state_locker.go `globalLockedStates` Store→Delete 泄漏 | 已实现 |
| rate=200 实验分析 | 已分析 |
| 并行验证设计讨论 | 待推进（本交接焦点） |

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
