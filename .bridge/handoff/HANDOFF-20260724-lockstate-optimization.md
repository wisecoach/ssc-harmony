# HANDOFF-20260724-lockstate-optimization

> from_session: 20260723_214122_5a827a (hentai_coder-designer)
> from_role: Designer
> to_role: Designer (next session)

## 完成内容

| 事项 | 状态 | 文档/位置 |
|:-----|:------|:---------|
| 性能埋点基础设施 | ✅ 已完成 | `ssc/perf/aggregator.go` — PerfAggregator + 全局 RecordPkg |
| `impl.go` BlockCommitted 末尾调用 | ✅ | `impl.go:401` — `perf.GetAggregator().OnBlockCommitted(block)` |
| retryScheduler 埋点 | ✅ 18 个 | OnBlockCommitted (11 phase) + RetryCommit (6 phase) |
| verify.go 埋点 | ✅ 3 个 | VerifySimulation lockCheck/execVerify/lockState |
| simulator_leader.go 埋点 | ✅ 3 个 | StartSimulateCXTransaction / StartReSimulation / HandleCXTSSCCall |
| simulator_member.go 埋点 | ✅ 2 个 | HandleCXTCall / HandleSimulateRequest |
| committer.go 埋点 | ✅ 1 个 | CommitOrRollbackWithProof |
| tx_submitter.go 埋点 | ✅ 1 个 | submitWithRetry |
| state_locker.go 埋点 | ✅ 3 个 | Lockable / CommitTx / RollbackTx |
| 分析脚本 `analyze_sscc_perf.py` | ✅ | 自动读 .env 定位日志目录，支持 --cat/--top/--raw |
| 清理 research 文档 | ✅ | 删除了过时的 RSH-03-perf-util-impl.md 和残留 .bkp/.gitkeep |
| 远程 199 部署分析脚本 | ✅ | `~/go/src/github.com/harmony-one/logs/harmony-sscc/analyze_sscc_perf.py` |
| 实验数据分析 | ✅ | rate=200 的日志已有 [perf] 数据（rate=300 重复了） |
| perf aggregator 加 Enabled 开关 | ✅ | `ssc/perf/aggregator.go` — `perf.Enabled` bool + `RecordPkg` 守卫 |
| pprof 自动采样 | ✅ | `test/test.sh` — 后台启动客户端 → sleep 10s → 4 节点并发 `curl pprof/profile?seconds=60` |

## 当前服务栈 / 环境

| 服务 | 状态 | 备注 |
|:-----|:------|:------|
| Go 编译 `./ssc/` + `./ssc/perf/` | ✅ | 全部通过 |
| `go vet ./ssc/` | ⚠️ 预存问题 | `simulation_test.go` 重名，非本次引入 |
| 远程 199 同步 | ✅ | 代码同步、编译、部署均可 |
| 分析脚本 | ✅ | 199 上已有，自动读 .env |

## 待办清单

- [ ] **Issue 讨论：lockState 阶段跳过 GetStateWithoutLock 是否安全**
  - 背景：`lockStateWithRWSet` 对每个 key 调 `SetAndLockState`，其中 `GetStateWithoutLock` 读旧值（走 trie/snapshot）是最耗时的操作
  - 发现：`Lock` 存的 oldValue 只用于 `RollbackTx` 写回，但 `RollbackTx` 自己重读了当前值，不依赖 Lock 时存的 value；`applyTo` 只删锁不写回状态
  - 候选方案：写 `SetAndLockStateFast` 跳过 GetStateWithoutLock，预期收益 ~120s/窗口
  - **风险讨论点**：是否有其他路径依赖 Lock 的 oldValue？（journal.Replay、pendingUnlocks journal、Snapshot/RevertToSnapshot）
- [ ] **跑新实验收集 perf 数据** — 当前 rate=200 的日志有埋点，rate=150/100 是旧 binary。需要重新编译部署跑实验才能拿到多个 RATE 的对比数据
- [ ] **分析脚本 avg 列计算修正** — 当前 avg = total/windows（窗口数），应为 total/cnt（调用次数）
- [ ] **其他模块埋点扩展** — patchpool.go（findCoveringSet 等）尚未添加，但被 retryScheduler 的 dagSearchPhase1b/2b 覆盖了

## 关键路径 / 参考文档

| 路径 | 说明 |
|:-----|:------|
| `ssc/perf/aggregator.go` | PerfAggregator 核心实现 + 全局 RecordPkg |
| `ssc/impl.go:401` | BlockCommitted 末尾 flush perf 窗口 |
| `scripts/analyze_sscc_perf.py` | 分析脚本（199: logs/harmony-sscc/ 下） |
| `docs/research/RSH-04-perf-sampling-architecture.md` | 埋点架构设计文档 |
| `docs/research/RSH-01-retryscheduler-complexity.md` | retryScheduler 复杂度 + P0-P4 优化方向 |
| `docs/research/RSH-03-full-perf-instrumentation.md` | 全模块埋点清单 |

## 已知坑

- **`state_lock_impl.go` 被误列为 0 埋点** — 正确路径是 `state_locker.go`（已加 3 个埋点），`state_lock_impl.go` 是管理器入口不需要直接加
- **`patchpool.go` 无独立埋点** — findCoveringSet 被 retryScheduler 的 dagSearch 埋点覆盖，非必要
- **rate=200 和 rate=300 日志重复** — 是同一个实验的数据（md5 相同），分析时注意不要重复计算
- **分析脚本 `parse_dur` 需要 float 保护** — 已修复，支持 `total` 字段为数字或字符串
