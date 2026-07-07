# HANDOFF-20260704-hotkey-retry-optimization

> from_session: `20260703_044921_c470e3`
> from_role: hermes-agent (hentai_coder)
> to_role: 优化 HotKeyRetry 的会话

## 完成内容

| 事项 | 状态 |
|:-----|:------|
| DSN-09 设计文档 | ✅ 已定稿（`docs/designs/active/DSN-09-retry-timeout-and-limit.md`） |
| DSN-09 代码实现 | ✅ 已编译通过并部署到远程 199 服务器 |
| 重试机制实验验证 | ⚠️ 首次实验因 build_keys.go 未重新运行导致配置无效，等待重新实验 |
| 实验数据（重试开启时） | 📊 提交率 62.66%（对比无重试的 73.3%） |
| 实验数据（无重试时） | 📊 提交率 73.34%，TPS 69.17 |

## 代码变更

| 文件 | 变更 |
|------|------|
| `ssc/api/types.go` | `TimeoutConfig` 加 `EnableLockOnConflict` 字段 |
| `ssc/verify.go` | 恢复 `CallForRetry`；`checkRetryLimitExceeded` 提前到 `lockStateWithExecution` 之前；公式 `>=` 改为 `>`；新增 `callForRetry` 方法 |
| `ssc/retry_scheduler.go` | 结构体加 `poolTimeout`/`sp1Timeout`/`retryPoolEnter`；`NewRetryScheduler` 签名加 `config *api.TimeoutConfig`；`AddToRetry` 加 `MaxRetriesTotal` 拦截 + enter block 记录；`OnBlockCommitted` 加主动池超时扫描（已上链仅 warn，未上链 CloseTransaction）；被动池超时修复（已上链跳过，未上链用 `poolTimeout`） |
| `ssc/impl.go` | `NewRetryScheduler` 传入 `sscConfig.Timeout` |
| `cmd/build_keys/build_keys.go` | 加 `EnableLockOnConflict: false` |

## 当前环境

| 项目 | 值 |
|------|-----|
| 项目根目录 | `/mnt/E/gowork/src/github.com/wisecoach/harmony-sscc` |
| 远程代码 | `~/go/src/github.com/harmony-one/harmony-sscc` (zjnu@10.7.95.199 -p 10022) |
| 远程日志 | `~/go/src/github.com/harmony-one/logs/harmony-sscc/` |
| 当前配置 | `MaxOnChainRetries=0, MaxRetriesTotal=3, EnableLockOnConflict=false` |
| 实验命令 | `ssh -p 10022 ...` → `test_single` |
| 配置生效需手动执行 | `go run cmd/build_keys/build_keys.go`（不在 auto_test 中） |

## 发现的关键问题

### 1. `build_keys.go` 配置不生效

`build_keys.go` 是一个代码生成器，输出 genesis 配置。修改后需要**手动运行** `go run cmd/build_keys/build_keys.go` 重新生成，仅 `go_executable_build.sh` 编译二进制不够。`auto_test.sh` 的 `preset` 步骤只编译不生成配置。

### 2. HotKeyRetry 链式重试几乎不触发

实验数据（允许重试时）：

| 阶段 | 触发次数 |
|:-----|:--------:|
| `AddToRetry` | 0（全被 `maxRetries=0` 拦截） |
| `call for retry`（RPC 发送） | 2,486 |
| `chainNextSim`（有写入集） | 37,058 |
| `chainNextSim`（有下游匹配） | 0 |
| `HandleRetrySignal` | 1 |
| `tryToReSimulation` | 1 |
| `retry commit success` | 1 |
| `TriggerReSimulation` | 1 |
| `ReadOnChainPatch` | 1 |
| `VerifySimulation success with simNum>0` | 0 |

**链条在 `chainNextSim → retryPool 匹配` 断了**。`chainNextSim` 在 SimTx 提交后扫描 `retryPool` 找下游依赖，但匹配到为 0（37,058 次 `chainNextSim` 全部 `matchedCount=0`）。可能的根因：

- `retryPool` 中 tx 的 `ReadSet/WriteSet` 为空 → `dependsOn` 始终返回 false
- `AddToRetry` 的 RWSet 提取（`SimulationCallStates[tx.SimulationNum-1]`）索引越界或拿不到数据
- `chainNextSim` 的写集（`writeSet`）与 `retryPool` 中 tx 的读写集无交集

### 3. 无重试时效果很好

关闭重试（`MaxOnChainRetries=0, MaxRetriesTotal=0` + `>=` 公式，实际等效于 ablation B）：
- 提交率 73.34%，TPS 69.17
- 超时仅 2 条，retry_limit_exceeded 2,664
- P50 时延 5.6s，P90 9.6s

## 待办清单（后续优化方向）

- [ ] **先确认正确配置下的重试效果**：跑实验前手动执行 `go run cmd/build_keys/build_keys.go` 生成新配置，然后 `test_single`。观察重试机制是否恢复提交率
- [ ] **诊断 HotKeyRetry 匹配失败**：`chainNextSim` 不匹配的原因可能是 `dependsOn()` 中 `retryTx.ReadSet/WriteSet` 为空。跟踪一期 tx 的完整生命周期（`AddToRetry` 时的 RWSets vs `chainNextSim` 时的 RWSets）
- [ ] **修复 HotKeyRetry**：目标让 `chainNextSim → HandleRetrySignal → tryToReSimulation → TriggerReSimulation` 通路跑通，从 ~1 次提升到可观测的次数
- [ ] **考虑 HotKey 检测逻辑**：当前 `chainNextSim` 全量扫描 `retryPool`，复杂度 O(pool_size × key_count)。如果 pool 大了，需要 HotKey 集合来加速

## 建议技能

- `blockchain-experiment-development/harmony-experiment-runner` — 实验流程
- `blockchain-experiment-development/harmony-sscc-development` — 代码架构
- `harmony-experiment-debugging` — 日志分析

## 参考文档

| 路径 | 说明 |
|:-----|:------|
| `docs/designs/active/DSN-09-retry-timeout-and-limit.md` | 重试超时与超限设计文档 |
| `docs/designs/active/DSN-08-retry-limit.md` | 原有重试限制设计 |
| `docs/designs/active/DSN-02-lock-retry-mechanism.md` | 锁与重试总览 |
| `ssc/retry_scheduler.go` | retryScheduler 实现（chainNextSim:648, SendChainSignal:1260） |
| `ssc/verify.go` | Verifier 实现（checkRetryLimitExceeded:527, callForRetry:567） |

## 已知坑

- `build_keys.go` 配置不随自动编译生效，需手动 `go run cmd/build_keys/build_keys.go`
- `checkRetryLimitExceeded` 的公式已从 `>=` 改为 `>`。`MaxOnChainRetries=0` 时允许多一次重试机会（首次冲突不计入上限）
- `AddToRetry` 入口的 `>` 公式需与 `checkRetryLimitExceeded` 保持同步（两者已同步）
- 实验时 `download_log` 可能不生效 → `data_handler` 没跑 → `result.txt` 没有 → 看 `分片状态` 来自客户端。要分析回滚原因需直接从验证器日志 grep
