# HANDOFF-20260722-block-timing-analysis

> from_session: 当前 session
> from_role: Designer
> to_role: Designer (新 session)
> 焦点: 部分区块少量交易大量耗时的根因分析，以及 CPU 监控工具部署

## 背景

RATE=150 (4shard×4val delay=10 vpn=4) 实验中，Shard 3 早期块（block 9-37）出现少量交易（17-46 笔 SimTx）耗时 1000-1500ms 的异常现象。正常块 300-400 笔 SimTx 也只耗 ~1000ms。

## 核心发现

### 1. SimTx 验证耗时瓶颈在 execVerify（EVM 重新执行）

从 `VerifySimulation timing breakdown` 数据：

```
正常:  execVerify=~2.5ms/笔
异常:  execVerify=~560ms/笔，callStates=1
```

`lockCheck`（4-12ms）和 `Copy`（0.02-0.43ms）不是瓶颈。**瓶颈是 Phase 2 的并行 EVM 重新执行（`verifyExecuteForCallState` → `sscvm.Call` → `vm.run`）**。

### 2. 深层嵌套 CALL 是慢的根因

同一笔 SimTx 对合约 `0xc2Ee93E2FFda23cad0B7b4E56E7751363b3bF198` 调用了 8 次 `SSCVM.Call`（内部 CALL），run 时间逐层递增：92ms → 172ms → 184ms → 392ms → 489ms → 536ms → 559ms → 575ms。

`SetSimuState` 只花了 0.016ms（24 次调用）。瓶颈不是 stateDB 也不是锁，**是这个合约本身的 EVM 解释执行时间**。

### 3. CPU 不是瓶颈

工作节点（200-203）CPU 峰值 ~20.4%（32 核），平均 <10%。不是 CPU 争用。

### 4. Leader vs Validator 差异

同一笔 SimTx，leader（9120）花 547ms，validator（9122）花 30ms。18x 差异。原因是 leader 侧 stateDB 副本有更多 dirty objects（来自同一块中前面的 CRTx/SimTx）。

### 5. SimulationNum 分布

5165 笔首次验证（simNum=0），只有 7 笔重试（simNum=1）。重试不是问题。

## 已加的日志

| 文件 | 位置 | 日志消息 | 级别 |
|:-----|:-----|:---------|:----:|
| `ssc/verify.go` | verifyExecuteForCallState | `verifyExecuteForCallState: sscvm.Call timing` | Info |
| `core/vm/sscvm.go` | SSCVM.Call | `SSCVM.Call timing (slow)` — 仅 ExecutionVerify 且 >100ms | Info |
| `ssc/verify.go` | SetSimuState | 累计 `simuStateCount` + `simuStateDur` 在 sscvm.Call timing 中输出 | Info |

`SSCVM.Call timing (slow)` 输出 `preState`（总时间）和 `run`（EVM 执行时间）。注意 **`preState` 是累积计时，包含了 `run` 时间**，请看 `run` 字段作为真实 EVM 执行时间。

## CPU 监控工具

### 部署
- 监控脚本: `/tmp/cpu_monitor.sh`（已在 199 和 200-203 部署）
- 自动集成: `auto_test.sh` 的 `test_single` 函数启动时自动部署到工作节点并启动
- 输出: `/tmp/cpu_usage_日期_时间.log`，格式 `time,user,system,idle,iowait`
- 生命周期: 120 秒自动退出

### 查看方法
```bash
# 在工作节点直接看
ssh -p 10022 zjnu@10.7.95.200 'cat /tmp/cpu_usage_*.log'

# 或在 199 上收集全部
for N in 200 201 202 203; do
  echo "=== 10.7.95.$N ==="
  ssh -p 10022 zjnu@10.7.95.199 "ssh -p 10022 zjnu@10.7.95.$N 'cat /tmp/cpu_usage_*.log'" 2>/dev/null
done
```

## 待继续的分析

1. **ExecutionVerify 模式跳过同分片 CALL 的 `vm.run`** — `opCall_SSC_EV` 中 `isCrossCall=false` 时走 `interpreter.vm.Call` 完整执行。如果用 `GetResult` 替代（类似跨分片 CALL），深层嵌套 CALL 从 8 层降到 1 层。风险：同分片内部调用的结果缓存是否可靠。
2. **`remainingTime=1000ms` 是否应该动态化** — 当前 hardcode，稳定后每块刚好填满。改大受 `maxTxns=500` 和 gas 限制，收益有限。
3. **早期块预热慢的问题** — 跨分片连接、stateDB trie cache 预热，这些是一次性开销，不影响稳态吞吐。

## 代码状态

| 文件 | 改动 | 状态 |
|:-----|:-----|:-----|
| `auto_test.sh` | test_single 加了 CPU 监控部署/收集 | ✅ 已同步远程 |
| `core/vm/sscvm.go` | SSCVM.Call 加分段计时（preState/run） | ⚠️ 需重新编译同步 |
| `ssc/verify.go` | verifyExecuteForCallState 加 timing，SetSimuState 加统计 | ⚠️ 需重新编译同步 |
| `cmd/harmony/main.go` | 日志级别改为 Debug | ⚠️ 需重新编译同步 |
| `node/worker/worker.go` | timing breakdown 日志改为 Info | ⚠️ 需重新编译同步 |

当前实验用的二进制已包含前两项改动。后续改代码后需 `touch ssc/*.go && bash ./scripts/go_executable_build.sh -S` 重新编译。

## 建议下一个 session 使用的 skill

- `harmony-experiment-runner` — 实验同步/编译/分析流程
- `handoff` — 继续多 session 开发
