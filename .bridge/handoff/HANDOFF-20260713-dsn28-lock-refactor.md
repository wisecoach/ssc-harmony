# HANDOFF-20260713-dsn28-lock-refactor

> from_session: `$HERMES_SESSION_ID`
> from_role: Designer
> to_role: Designer

## 完成内容

| 事项 | 状态 |
|:-----|:------|
| DSN-28: stateLockManager 无锁重构 | **设计完成 + 实现完成 + 实验验证** |
| stateLockManager.mu → sync.Map + atomic | ✅ 移除全局锁 |
| stateLocker.txLock → per-tx 锁（txLocks[txHash]） | ✅ 不同 txHash 互不阻塞 |
| stateLocker.baseSnapshot → baseVersion + MVCC 版本过滤 | ✅ 2 版本设计验证通过 |
| handleLockCommit 深拷贝 + clear → version++ O(1) | ✅ 运行中 |
| journal revert 中的 locker.mu 引用移除 | ✅ 修复 |
| statistics.go 中 mgr.mu 引用移除 | ✅ 修复 |
| state_lock_entry.go 中 mu 残留引用移除 | ✅ 修复 |
| GetLockerAt 版本边界兼容（当前/上一个版本请求分离） | ✅ 通过实验验证 |
| 日志增强：CommitTx slow lock wait、GetLockerAt version 追踪 | ✅ |

## 实验结果

**锁优化效果：**

| 指标 | 改前 | 改后 | 变化 |
|:-----|:---:|:---:|:----:|
| CRTx avg | 10.39ms | **1.85ms** | ↓5.6× |
| CRTx max | 815ms | **56ms** | ↓14.5× |
| SimTx avg | 7.07ms | **6.08ms** | ↓14% |
| RetryCommit avg | 6.51ms | **1.65ms** | ↓3.9× |
| RetryCommit >100ms 占比 | 3.2% | **0%** | ✅ 消除 |
| StartReSimulation avg | 563ms | **180ms** | ↓3.1× |
| ReSimulation 次数 | 6,360 | **1,580** | ↓4× |
| ProposeNewBlock commitTxs P50 | ~1,500ms | **523ms** | ↓2.9× |
| `CommitTx: slow lock wait` | 有 | **0 条** | ✅ 消除 |
| `GetLockerAt: unknown root` | N/A | **0 条** | ✅ 2 版本设计通过 |

**端到端延迟（客户端视角）：**
- avg=36s, P50=28s, P90=75s, P95=78s, P99=107s
- 延迟高不是因为锁争抢——是因为 PatchPool 触发 ReSimulation 太乐观，98% 的重试浪费

## 当前服务栈 / 环境

| 服务 | 状态 |
|:-----|:------|
| 远程编译/实验 | 10.7.95.199:10022 zjnu |
| 远程日志 | `~/go/src/github.com/harmony-one/logs/harmony-sscc/shard=4_validator=4_ssc=1_delay=10_rate=100_vpn=4/` |
| 本地代码 | `/mnt/E/gowork/src/github.com/wisecoach/harmony-sscc` |
| 实验配置 | shard=4, validator=4, ssc=1, delay=10, rate=100, vpn=4 |

## 待办清单

- [ ] **PatchPool 触发条件优化**：OnPatchPoolUpdated 触发 chainSignal 前先查 TLV 锁状态，降低 ReSimulation 失败率（9,583/11,163 = 86% 浪费）
- [ ] `maxPatches=1` 限制是否可以放宽？当前限制导致跨多个 SimTx 的冲突无法被单 Patch 覆盖
- [ ] 实验验证 PatchPool 优化后的延迟改善效果
- [ ] 确认 ChainRetryStats 的 zero 问题是 stat reset 时序导致的误报（experiment 已验证 chainNextSim 实际在运行：5,787 scanning calls）

## 关键路径 / 参考文档

| 路径 | 说明 |
|:-----|:------|
| `docs/designs/active/DSN-28-stateLockManager-lock-free.md` | DSN-28 完整设计文档 |
| `ssc/state_lock_impl.go` | stateLockManager 重写（version MVCC, sync.Map 无锁化） |
| `ssc/state_locker.go` | stateLocker 重写（per-tx 锁, baseVersion） |
| `ssc/state_lock_entry.go` | journal revert 无锁化 |
| `ssc/statistics.go` | mgr.mu 引用迁移到 sync.Map |
| `ssc/retry_scheduler.go` | OnPatchPoolUpdated — 触发条件待优化 |
| `node/worker/worker.go` | 交易级时序日志增强 |

## 已知坑

- 本地 `go build` 因 go 1.22.2 vs go.mod 要求 1.22.5 失败，远程 `make` 正常工作
- `make test` 需要 Docker TTY，本地不满足
- `CHAIN_RETRY_STATS` 每 30s 被 `dumpChainRetryStats` 重置，stats ticker 触发时机可能导致全零误报
- GetLockerAt 的 2 版本设计（current + previous）经实验验证足够，0 次 unknown root 降级
- 当前新实验覆盖了 DSN-28 改动，日志路径同旧实验目录（7月8日），实验时间是7月13日21:48
