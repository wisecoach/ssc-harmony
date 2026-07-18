# DSN-10: 日志查询与分级指南（DSN-29+ 更新版）

> **更新**: 2026-07-16 — 配合 DSN-29/DSN-30 PatchPool 重构 + 日志等级规范化

---

## 一、日志架构

### 1.1 双日志器体系

```
utils.Logger()             →  全局 zerolog（Harmony 本体日志）
   └─ 文件: zerolog-validator-*.log
   └─ Level: 固定 Info（不可按需调低，否则核心共识不可见）

utils.SSCLogger()          →  SSC 专用 zerolog（SSC/Retry 日志）
   └─ 文件: ssc-validator-*.log
   └─ Level: 每条日志独立选择 Info 或 Debug
```

**核心规则**：
- `Logger()` 始终 Info 以上（核心共识日志不可关闭）
- `SSCLogger()` 的每条 `.Info()` / `.Debug()` 由 `data_handler` 需求决定
- `main.go:268` 中 `utils.SetLogVerbosity(log.LvlWarn)` 只控制 Logger() 的全局兜底，不影响 SSCLogger()
- 调试时修改 Logger() 等级，生产时保持 Warn 以上

### 1.2 日志文件布局

```bash
# 实验日志根目录
~/go/src/github.com/harmony-one/logs/harmony-sscc/

# 实验配置子目录（自动按参数命名）
shard=4_validator=4_ssc=1_delay=10_rate=100_vpn=4/
├── ssc-validator-*.log          # SSC 日志（主分析目标）
├── zerolog-validator-*.log      # Harmony 本体日志
├── log-*.log                    # 共识引擎日志（ProposeNewBlock 等）
└── bootnode.log                 # 启动节点日志
```

### 1.3 日志等级速查

| 等级 | Logger() (Harmony) | SSCLogger() (SSC) |
|:-----|:-------------------|:-------------------|
| Error | 🔴 **启用** | 🔴 **启用** — 异常/失败 |
| Warn | 🟡 **启用** | 🟡 **启用** — 性能警告 |
| Info | 🔵 固定 Info | 🔵 **按需** — data_handler 需要 |
| Debug | Disabled | ⚪ **按需** — 诊断调试用 |

---

## 二、SSC 日志等级分类

### 2.1 分类原则

```
┌─  data_handler 需要 → .Info()
├─  data_handler 不需要，但偶尔诊断 → .Debug()
├─  性能瓶颈分析用 → .Debug()（如所有 timing breakdown）
└─  核心异常/失败 → .Error() / .Warn()
```

### 2.2 各模块日志等级一览

#### verify.go — VerifySimulation（最高频模块）

| 日志内容 | 等级 | 理由 |
|:---------|:----:|:------|
| `VerifySimulation: success, sending Commit vote` | `Info` | data_handler 统计 verify 成功率 |
| `VerifySimulation: chain tx detected (from SimTx ChainPatch), skipping lock confl` | `Info` | data_handler 统计 PatchPool 覆盖率 |
| `simulation is valid` | `Info` | data_handler 统计验证结果 |
| `SIMULATION_COMMIT_TX` | `Info` | data_handler 统计 SimTx 提交量 |
| `AddOnChainPatch: stored from SimTx` | `Info` | data_handler 统计链上 Patch |
| `VerifySimulation timing breakdown` | `Debug` | 性能分析时临时开启 |
| `lockStateWithRWSet timing breakdown` | `Debug` | 性能分析时临时开启 |
| `begin to verify simulation` | `Debug` | 逐笔追踪，无需常规输出 |
| 验证失败/异常 | `Error` | 不可降级 |

#### simulator.go / simulator_leader.go — 模拟调度

| 日志内容 | 等级 | 理由 |
|:---------|:----:|:------|
| `add a cross shard Tx` | `Info` | data_handler 统计 tx 注入量 |
| `simulation has been started, simulationNum=N` | `Info` | data_handler 统计模拟次数 |
| `start simulate cx transaction` | `Info` | data_handler 统计模拟启动 |
| `simulation accomplished` | `Info` | data_handler 统计模拟完成 |
| `send CXTCommitVote to http://...` | `Info` | data_handler 统计跨分片投票 |
| `SIMULATION_COMMIT_TX` | `Info` | data_handler 统计 SimTx 上链 |
| `StartSimulateCXTransaction timing breakdown` | `Info` | 性能基线（低频，每 tx 一次） |
| `processSimulationTask timing breakdown` | `Debug` | 性能分析时临时开启 |
| `Simulator.Cleanup timing` | `Debug` | 内部清理，无需常规输出 |
| `Verifier.Cleanup timing` | `Debug` | 同上 |
| `callSSCPrecompiledContract: caller=0x...` | `Debug` | 逐笔预编译合约调用追踪 |
| `RunWriteCapablePrecompiledContract: 0xfF...` | `Debug` | 同上，极高频 |
| `worker started / worker stopped` | `Info` | 启动/停止一次，低频 |

#### retry_scheduler.go — 重试调度

| 日志内容 | 等级 | 理由 |
|:---------|:----:|:------|
| `added to retry pool` | `Info` | data_handler 统计 retry 入池量 |
| `promoted txs to next round` | `Info` | data_handler 统计重试量 |
| `retry commit success / failed` | `Info` | data_handler 统计重试结果 |
| `retryScheduler onBlockCommitted` | `Info` | data_handler 统计出块 |
| `OnBlockCommitted: reservation completed` | `Info` | TLV 预检结果 |
| `consumed patches found, skipping TLV locks` | `Info` | PatchPool 覆盖量统计 |
| `HandleRetrySignal: received` | `Info` | data_handler 统计 chain signal |
| `scanPatchSubscribers: matched and signaled` | `Info` | PatchPool 匹配量统计 |
| `RetryCommit timing (各种细分)` | `Debug` | 性能分析时开启 |
| `tryToReSimulation: retry pool stats` | `Debug` | 调试时开启（高频） |
| `retryScheduler.StaleTx timing` | `Debug` | 内部清理 |
| `RemoveFromPassivePool timing` | `Debug` | 内部清理 |
| `TempLockView.OnBlockCommitted timing breakdown` | `Info` | 出块性能基线 |
| lock acquire/release 单步 | `Debug` | 调试时开启 |

#### committer.go — CR 提交

| 日志内容 | 等级 | 理由 |
|:---------|:----:|:------|
| `commit or rollback with proof` | `Info` | data_handler 统计 CR |
| `leader close transaction` | `Info` | 交易终结统计 |
| `CommitSimulation timing breakdown` | `Info` | SimTx 上链性能基线 |
| `closeTransaction timing breakdown` | `Debug` | 性能分析时开启 |
| `CommitOrRollbackWithProof timing breakdown` | `Debug` | 性能分析时开启 |
| `RemoveOnChainPatch timing` | `Debug` | 内部清理 |

#### temp_lock_view.go — TLV 锁管理

| 日志内容 | 等级 | 理由 |
|:---------|:----:|:------|
| `Wound: high priority tx took lock from low` | `Info` | Wound-Wait 事件统计 |
| `TempLockView on block committed` | `Info` | 出块时 TLV 快照 |
| TryLock 成功/失败 | `Debug` | 调试时开启 |

#### impl.go — SimTx 上链

| 日志内容 | 等级 | 理由 |
|:---------|:----:|:------|
| `StartSimulateCXTransaction timing breakdown` | `Info` | 模拟性能基线 |
| `commitTransaction timing breakdown` | `Info` | 交易提交性能基线 |
| `build commit simulation completed` | `Info` | SimTx 构建完成 |
| `CommitSimulation Multicast` 相关 | `Info` | 跨分片多播状态 |

#### committee.go / SL 相关

| 日志内容 | 等级 | 理由 |
|:---------|:----:|:------|
| `COMMITTEE_INIT/UPDATE/UPDATED` | `Info` | 委员会变更（低频） |
| `NEW_EPOCH_SUBMIT/HANDLED` | `Info` | 新 epoch 事件（低频） |
| `REPUTATION_EPOCH` | `Info` | 信誉报告（每 epoch 一次） |
| `SIMULATION_COMMIT_TX` | `Info` | 跨分片 SimTx 提交 |
| SL 测试事件 | `Info` | 低频率，保底检查 |
| BLS_KEY_PARSE_FAILED 等 | `Error` | 异常，不可降级 |

---

## 三、日志查询模板（DSN-29+ 更新版）

### 3.1 ssh 连接与基础查询

```bash
ssh -p 10022 zjnu@10.7.95.199

# SSC 日志（主）
cd ~/go/src/github.com/harmony-one/logs/harmony-sscc/
DIR="shard=4_validator=4_ssc=1_delay=10_rate=100_vpn=4"

# 使用 ssc_grep.sh（自动读 .env 环境变量）
SHARD_NUM=4 VALIDATOR=4 SSC=1 DELAY=10 RATE=100 VALIDATOR_PER_NODE=4 \
  bash ssc_grep.sh "关键词"

# 直接 cat（推荐，避免 .env 不匹配问题）
cat "$DIR"/ssc-validator*.log | grep "关键词"
```

### 3.2 性能分析查询

```bash
# Block gas/usage 信息
cat "$DIR"/ssc-validator*.log | grep "Block gas limit and usage info"

# 块级时序（BlockTiming: BlockCommitted / chainHeadCh）
cat "$DIR"/ssc-validator*.log | grep "\[BlockTiming\]"

# ProposeNewBlock 分解（在 log-*.log 中）
cat "$DIR"/log-*.log | grep "ProposeNewBlock breakdown"

# SimTx 计数
cat "$DIR"/ssc-validator*.log | grep "SIMULATION_COMMIT_TX" | wc -l

# 交易提交细分（commitTransaction timing breakdown）
cat "$DIR"/ssc-validator*.log | grep "commitTransaction timing breakdown" | \
  python3 -c "
import sys, json
from collections import Counter
types = Counter()
for line in sys.stdin:
    d = json.loads(line.strip())
    types[d.get('txType','?')] += 1
for t, c in types.most_common():
    print(f'  {t}: {c}')
"
```

### 3.3 重试分析查询

```bash
# Retry 漏斗
echo "=== added to retry pool ==="
cat "$DIR"/ssc-validator*.log | grep "added to retry pool" | wc -l

echo "=== HandleRetrySignal ==="
cat "$DIR"/ssc-validator*.log | grep "HandleRetrySignal: received" | wc -l

echo "=== retry commit ==="
cat "$DIR"/ssc-validator*.log | grep "retry commit success" | wc -l
cat "$DIR"/ssc-validator*.log | grep "retry commit failed" | wc -l

echo "=== PatchPool ==="
cat "$DIR"/ssc-validator*.log | grep "consumed patches found" | wc -l
cat "$DIR"/ssc-validator*.log | grep "scanPatchSubscribers: matched" | wc -l

echo "=== promoted ==="
cat "$DIR"/ssc-validator*.log | grep "promoted txs" | wc -l
```

### 3.4 VerifySimulation 统计

```bash
# VerifySimulation 总数
cat "$DIR"/ssc-validator*.log | grep "VerifySimulation: success" | wc -l

# PatchPool 覆盖率（ChainPatch 跳过锁冲突的比例）
TOTAL=$(cat "$DIR"/ssc-validator*.log | grep "VerifySimulation: success" | wc -l)
SKIP=$(cat "$DIR"/ssc-validator*.log | grep "skipping lock confl" | wc -l)
echo "ChainPatch coverage: $SKIP / $TOTAL ($(echo "scale=2; $SKIP*100/$TOTAL" | bc)%)"

# verify 耗时分布（需开启 timing breakdown 的 Debug 日志）
cat "$DIR"/ssc-validator*.log | grep "VerifySimulation timing breakdown" | \
  python3 -c "
import sys, json
vals = []
for line in sys.stdin:
    d = json.loads(line.strip())
    t = d.get('total', 0)
    if isinstance(t, (int,float)) and t > 0:
        vals.append(t)
vals.sort()
n = len(vals)
print(f'n={n} avg={sum(vals)/n:.2f}ms P50={vals[n//2]:.2f}ms P90={vals[int(n*0.9)]:.2f}ms')
"
```

### 3.5 跨分片分析

```bash
# 按 port 分组（port=9000=shard0 leader, 9040=shard1 leader, etc.）
cat "$DIR"/ssc-validator*.log | grep "added to retry pool" | \
  grep -oP '"port":"[0-9]+"' | sort | uniq -c | sort -rn

# 特定 shard 的完整重试追踪
PORT=9040  # Shard 1（通常最卡）
cat "$DIR"/ssc-validator*.log | grep '"port":"'"$PORT"'"' | grep "retry commit success"
```

### 3.6 交易生命周期追踪

```bash
TX="0xYOUR_TX_HASH"
cat "$DIR"/ssc-validator*.log | grep "$TX" | \
  python3 -c "
import sys, json
for line in sys.stdin:
    d = json.loads(line.strip())
    t = d.get('time','?')[-12:]
    p = d.get('port','?')
    m = d.get('message','')[:120]
    print(f'[{t}] port={p} {m}')
"
```

---

## 四、data_handler 依赖的 Info 日志清单

以下日志是 `data_handler`（实验结果处理脚本）分析所需的 **Info 级别**日志，**不可降级为 Debug**：

| # | 日志消息 | 用途 |
|:-:|:---------|:------|
| 1 | `VerifySimulation: success, sending Commit vote` | 统计 verify 成功率 |
| 2 | `VerifySimulation: chain tx detected ... skipping lock confl` | 统计 PatchPool 覆盖率 |
| 3 | `simulation is valid` | 统计模拟验证结果 |
| 4 | `SIMULATION_COMMIT_TX` | 统计 SimTx 上链量 |
| 5 | `AddOnChainPatch: stored from SimTx` | 统计链上 Patch |
| 6 | `add a cross shard Tx` | 统计 tx 注入量 |
| 7 | `simulation has been started, simulationNum=N` | 统计模拟次数 |
| 8 | `send CXTCommitVote to http://...` | 统计跨分片投票 |
| 9 | `added to retry pool` | 统计 retry 入池 |
| 10 | `promoted txs to next round` | 统计 retry 信号 |
| 11 | `retry commit success / failed` | 统计 retry 结果 |
| 12 | `consumed patches found, skipping TLV locks` | 统计 PatchPool bypass |
| 13 | `HandleRetrySignal: received` | 统计 chain signal |
| 14 | `scanPatchSubscribers: matched and signaled` | 统计 PatchPool 匹配 |
| 15 | `commit or rollback with proof` | 统计 CR |
| 16 | `leader close transaction` | 统计交易终结 |
| 17 | `commitTransaction timing breakdown` | 统计 tx 处理时间 |
| 18 | `StartSimulateCXTransaction timing breakdown` | 统计模拟时间 |
| 19 | `CommitSimulation timing breakdown` | 统计 SimTx 上链时间 |
| 20 | `[BlockTiming] BlockCommitted breakdown` | 块级时序 |
| 21 | `Block gas limit and usage info` | 块 gas 使用情况 |
| 22 | `TempLockView.OnBlockCommitted timing breakdown` | TLV 出块性能 |

---

## 五、修改日志等级流程

```bash
# 1. 找到要改的日志
cd /mnt/E/gowork/src/github.com/wisecoach/harmony-sscc
grep -rn "SSCLogger().Info()" ssc/ --include="*.go" | grep -v _test

# 2. 修改 .Info() ↔ .Debug()
#    data_handler 需要 → .Info()
#    性能诊断用 → .Debug()

# 3. 编译
source ./scripts/setup_bls_build_flags.sh && go build -o bin/harmony ./cmd/harmony

# 4. 运行实验验证
#    确认日志量符合预期，data_handler 能正常工作
```

---

## 六、实验中临时调整日志等级

在 `main.go:268` 中修改全局日志等级：

```go
// cmd/harmony/main.go:268
// utils.SetLogVerbosity(log.LvlDebug)  // 打开所有 Debug 日志（性能诊断）
// utils.SetLogVerbosity(log.LvlInfo)   // 标准实验模式
// utils.SetLogVerbosity(log.LvlWarn)   // 生产/大数据实验（仅 Warn+Error）
```

**推荐实践**：
- 跑新实验前确认 `LvlWarn` 或 `LvlInfo`
- 需要 timing breakdown 时临时改为 `LvlWarn`，SSCLogger 的 Debug 日志不受影响
- 需要看 Harmony 本体 Debug 日志时改为 `LvlDebug`

---

## 七、SSC 日志等级汇总表

| 模块 | Info 条数 | Debug 条数 | 主导类型 |
|:-----|:---------:|:----------:|:---------|
| `verify.go` | 6 | 4 | Info（data_handler 依赖） |
| `simulator.go` | 12 | 8 | Info/Debug 混合 |
| `retry_scheduler.go` | 12 | 6 | Info（重试统计） |
| `temp_lock_view.go` | 3 | 2 | Info |
| `committer.go` | 3 | 4 | Debug（timing 为主） |
| `committee.go` | 15 | 0 | Info（低频） |
| `impl.go` | 5 | 0 | Info |

> **基线日志量**（info-only）：~5-10MB/实验
> **开启 Debug（含 timing breakdown）**：~300MB/实验
> **全部开启**：~500MB+/实验
