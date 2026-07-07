# [B01] 日志查询指南（修订版）

> **阅读顺序**（`[前缀]` 表示阅读顺序）：
> 1. 📄 `docs/A01-sscc-refactor-plan.md` [A01] — SSCC 模块化重构总览
> 2. 📄 `docs/A02-lock-retry-mechanism.md` [A02] — 锁与重试机制
> 3. 📄 `docs/A03-CR-priority-optimization.md` [A03] — CR 提交优先级优化
> 4. 📄 `docs/A04-onchain-retry-limit.md` [A04] — 链上重试次数限制
> 5. 📄 `docs/A05-hotkey-retry-design.md` [A05] — HotKey 链式重试（数据层）
> 6. 📄 `docs/A06-lock-priority-coordination.md` [A06] — 锁优先级协调（协调层）
>
> **运维辅助**（与主线并行的独立文档）：
> - 📄 `docs/B01-log-query-guide.md` [B01] **（本文）** — 日志查询指南
> - 📄 `docs/B02-log-lifecycle.md` [B02] — 交易生命周期日志大全
> - 📄 `docs/DSN-20-simulation-timing-instrumentation.md` [DSN-20] — 全链路计时埋点设计
>
> 节点：10.7.95.199 | 端口：10022 | 用户：zjnu

---

## 一、SSH 连接

```bash
ssh -p 10022 zjnu@10.7.95.199
```

---

## 二、日志位置与结构

日志在实验节点上，由实验脚本自动收集到各机器。**在本机（服务器）上通过 `ssc_grep.sh` 统一查询所有远程节点的日志。**

### 日志目录

```bash
# 当前实验配置的日志目录
cd ~/go/src/github.com/harmony-one/logs/harmony-sscc/
ls
# → 按实验参数命名的子目录，如：
#   shard=4_validator=4_ssc=4_delay=10_rate=100_vpn=4/
#   shard=4_validator=4_ssc=4_delay=10_rate=300_vpn=4/
```

### 子目录结构

每个子目录对应一次实验运行，包含各节点的日志文件：

```
shard=4_validator=4_ssc=4_delay=10_rate=100_vpn=4/
├── ssc-validator-0.log      # 节点 0 日志
├── ssc-validator-1.log      # 节点 1 日志
├── ssc-validator-2.log      # 节点 2 日志
├── ...
├── bootnode.log             # 启动节点日志
└── r.log                    # 汇总日志（可能为空）
```

---

## 三、ssc_grep.sh 脚本用法

**核心用法**：读取 `.env` 配置，自动进入当前配置对应的日志目录，跨所有 `ssc-validator-*.log` 文件 grep。

```bash
cd ~/go/src/github.com/harmony-one/logs/harmony-sscc/

# 基本 grep
bash ssc_grep.sh "关键词"

# 带管道过滤
bash ssc_grep.sh "关键词" | grep "过滤条件"

# 按交易 hash 查
bash ssc_grep.sh "0x84e921b0f873ea7111e66709e281040ee3de04d48e813c420b48d6604e163049" | head -20

# 带上下文看前后行
bash ssc_grep.sh "retry tx blocked" | tail -5

# 统计出现次数
bash ssc_grep.sh "关键词" | wc -l
```

### 当前配置确认

ssc_grep.sh 读取 `../../.env` 决定用哪个日志目录：

```bash
cat ~/go/src/github.com/harmony-one/.env
# → SHARD_NUM=4, VALIDATOR=4, SSC=1, RATE=100, DELAY=20, ...
```

需要换实验配置时，修改 `.env` 中的参数。

### ⚠️ ssc_grep.sh 管道陷阱：行号前缀污染 JSON 解析

`ssc_grep.sh` 内部用 `grep -n`，输出带行号前缀（如 `123:{"level":...}`）。管道到 Python/jq 会失败：

```bash
# ❌ JSONDecodeError（行号前缀破坏 JSON）
bash ssc_grep.sh "leader close transaction" | python3 -c "import json, sys; [json.loads(l) for l in sys.stdin]"

# ✅ 跳过行号前缀
bash ssc_grep.sh "leader close transaction" | sed 's/^[0-9]*://' | python3 -c "..."

# ✅ 或直接 cat 日志文件（推荐用于管道场景）
DIR="shard=4_validator=4_ssc=1_delay=20_rate=100_vpn=4"
cat "$DIR"/ssc-validator*.log | grep "关键词" | python3 -c "..."
```

---

## 四、关键查询模板

### 4.1 查交易是否卡死

```bash
bash ssc_grep.sh "retry tx blocked" | grep "port.*9120" | head -5
```

返回：
```
conflictKey=0x... holderTx=0x... fromCommitted=true/false conflictType=temp-write/committed-write
```

- `fromCommitted=true` → 链上锁（stateLockManager）挡路
- `fromCommitted=false` → TempLockView 临时锁挡路
- `conflictType=temp-write` → TempLockView 临时写锁
- `conflictType=committed-write` → 链上已提交写锁

### 4.2 查交易的完整生命周期

```bash
# 按阶段依次 grep，确认卡在哪一步
bash ssc_grep.sh "0xHASH" | grep "add a cross shard Tx"        # Phase 1: 入池
bash ssc_grep.sh "0xHASH" | grep "start simulate cx transaction" # Phase 2: 开始模拟
bash ssc_grep.sh "0xHASH" | grep "simulation accomplished"       # Phase 2c: 模拟完成
bash ssc_grep.sh "0xHASH" | grep "begin to verify simulation"    # Phase 4: 链上验证
bash ssc_grep.sh "0xHASH" | grep "send CXTCommitVote"            # Phase 4b: 发投票
bash ssc_grep.sh "0xHASH" | grep "leader close transaction"      # Phase 7: 交易终结
```

### 4.3 查特定 port（节点）的日志

```bash
# port 9120 在 10.7.95.203 上（shard 0 leader）
# port 9000 在 10.7.95.200 上（shard 3 leader）
# port 9080 在 10.7.95.202 上（shard 2 leader）

bash ssc_grep.sh "关键词" | grep "port.*9120"   # 只看 shard 0
bash ssc_grep.sh "关键词" | grep "port.*9000"   # 只看 shard 3
```

### 4.4 查 holderTx（锁持有者）的状态

```bash
# 查 holderTx 是否已 close
bash ssc_grep.sh "0xHOLDER" | grep -E "leader close transaction|commit or rollback with proof|closeTransaction"

# 如果没结果，说明 holderTx 没走到 close，锁可能残留
```

### 4.5 统计卡死规模

```bash
# 全部 retry tx blocked 计数
bash ssc_grep.sh "retry tx blocked" | wc -l

# 按冲突类型统计
bash ssc_grep.sh "retry tx blocked" | grep -o '"conflictType":"[^"]*"' | sort | uniq -c

# 按来源统计（committed vs temp）
bash ssc_grep.sh "retry tx blocked" | grep -o '"fromCommitted":[a-z]*' | sort | uniq -c

# 按 holderTx 统计（找到最占锁的 holder）
bash ssc_grep.sh "retry tx blocked" | grep -o '"holderTx":"0x[^"]*"' | sort | uniq -c | sort -rn | head -10
```

---

## 五、日志字段说明

### 5.1 通用字段

每条 JSON 日志包含以下常用字段：

| 字段 | 含义 |
|------|------|
| `level` | 日志级别：info / warn / error |
| `port` | 节点端口：9120(shard0)、9122(shard0 validator)、9000(shard3)、9080(shard2) 等 |
| `ip` | 节点 IP |
| `txHash` | 交易哈希 |
| `message` | 日志消息内容 |
| `time` | 时间戳（格式：`2026-06-12T02:58:08.04097249+08:00`）|
| `caller` | 代码位置（文件:行号）|
| `simulationNum` | 模拟轮次编号 |

### 5.2 锁冲突字段

| 字段 | 含义 |
|------|------|
| `conflictKey` | 锁冲突的 key |
| `holderTx` | 锁持有者的交易哈希 |
| `fromCommitted` | `true`=链上锁，`false`=TempLockView 临时锁 |
| `conflictType` | 冲突类型：`committed-write` / `temp-write` / `read-after-temp-write` |
| `readSetSize` | 读集大小 |
| `writeSetSize` | 写集大小 |

### 5.3 Timing Breakdown 字段

所有 timing breakdown 日志消息命名格式为 `"<FunctionName> timing breakdown"`，使用 `time.Since(t0)` 累积值（非增量），字段用 `Str()` 序列化为 `"1.234ms"` 格式。

#### 已有埋点（9 处）

| 日志消息 | 文件 | 字段 |
|:---------|:-----|:-----|
| `CommitOrRollbackWithProof timing breakdown` | `committer.go` | `unmarshal` / `isFinished` / `commitTx` / `closeTx` / `total` |
| `closeTransaction timing breakdown` | `impl.go` | `stateLock` / `simCleanup` / `verCleanup` / `totalClose` |
| `Simulator.Cleanup timing` | `simulator.go` | `delSimState` / `pendingLock` / `simuLock` |
| `Verifier.Cleanup timing` | `verify.go` | `duration` |
| `retryScheduler.StaleTx timing` | `retry_scheduler.go` | `duration` |
| `RemoveOnChainPatch timing` | `retry_scheduler.go` | `duration` |
| `CXTTimerManager.RemoveTx timing` | `cxt_timer.go` | `duration` |
| `PatchPool.Remove timing` | `api/types.go` | `duration` |
| `RemoveFromPassivePool timing` | `retry_scheduler.go` | `duration` |

#### 新增埋点（10 处）— DSN-20

| 日志消息 | 文件 | 字段 | 预期 P50 |
|:---------|:-----|:-----|:--------:|
| `processSimulationTask timing breakdown` | `simulator.go` | `queueWait` / `p2pCall` / `total` | ~121ms |
| `StartSimulateCXTransaction timing breakdown` | `simulator_leader.go` | `callMembers` / `aggregate` / `thresholdSign` / `commitSend` / `total` | ~135ms |
| `CommitSimulation timing breakdown` | `impl.go` | `buildCallStates` / `buildSignatures` / `onChainPatch` / `submitTx` / `total` | ~35ms |
| `HandleCommitVote timing breakdown` | `impl.go` | `voteTracking` / `aggregate` / `sendVote` / `total` | ~12ms |
| `AddToRetry timing breakdown` | `retry_scheduler.go` | `extractRWSet` / `poolInsert` / `total` | ~0.5ms |
| `chainNextSim timing breakdown` | `retry_scheduler.go` | `scanPool` / `reserveSelect` / `sendSignals` / `total` | ~10ms |
| `HandleRetrySignal timing breakdown` | `retry_scheduler.go` | `setPatch` / `poolLookup` / `dispatch` / `total` | ~0.1ms |
| `tryToReSimulation timing breakdown` | `retry_scheduler.go` | `inFlightCheck` / `retryCalls` / `resultCheck` / `triggerSim` / `total` | ~60ms |
| `StartReSimulation timing breakdown` | `simulator_leader.go` | `getState` / `callMembers` / `aggregate` / `thresholdSign` / `commitSend` / `total` | ~135ms |
| `RetryCommit timing breakdown` | `retry_scheduler.go` | `passiveCheck` / `patchPoolCheck` / `tryLock` / `stateDbCheck` / `total` | ~1.2ms |

---

## 六、快速诊断流程

### 6.1 交易卡死诊断

发现交易卡死时，按以下顺序排查：

```
1. 统计卡死规模
   bash ssc_grep.sh "retry tx blocked" | wc -l

2. 找到最卡的 holderTx
   bash ssc_grep.sh "retry tx blocked" | grep -o '"holderTx":"0x[^"]*"' | sort | uniq -c | sort -rn | head -3

3. 查 holder 是否 close
   bash ssc_grep.sh "0xHOLDER" | grep -E "leader close transaction|commit or rollback with proof"
   → 有结果 = holder 已 close，但 TempLockView 锁残留
   → 无结果 = holder 本身卡在重试流程里

4. 查 holder 的完整路径
   bash ssc_grep.sh "0xHOLDER" | grep -E "simulation has been started|TempLockView pre-check|retry commit success|failed to get last simulation|retry tx blocked"

5. 判断断点
   - 只到 "TempLockView pre-check failed" → 没进链上
   - 有 "retry commit success" + 无后续 → startReSimulation 卡住
   - 有 "retry commit success" + "recall simulation" → 重试执行中
   - 有 "leader close transaction" → 交易已终结，锁应释放
```

### 6.2 性能瓶颈诊断

#### 一键查全部时序耗时（19 个 timing breakdown）

```bash
cd ~/go/src/github.com/harmony-one/logs/harmony-sscc/shard=4_.../
python3 ../analyze-all-timing.py
```

输出示例：
```
==================================================
  processSimulationTask
==================================================
  queueWait          n=  423  P50=    0.02ms  P90=    0.10ms  P99=    1.50ms  avg=    0.05ms  max=    5.23ms
  p2pCall            n=  423  P50=  120.50ms  P90=  450.20ms  P99=  950.10ms  avg=  180.30ms  max= 1200.50ms
  total              n=  423  P50=  121.00ms  P90=  452.00ms  P99=  952.00ms  avg=  181.00ms  max= 1205.00ms
```

#### 分段定位流程（由粗到精）

```text
宏观延迟高 (P50 > 20s)
  │
  ├─ 看 StartSimulateCXTransaction timing
  │   ├─ callMembers  慢 → leader 端 member 调用慢
  │   ├─ thresholdSign 慢 → 签名收集瓶颈
  │   └─ commitSend   慢 → CommitSimulation 多播慢
  │
  ├─ 看 processSimulationTask timing
  │   ├─ p2pCall 慢     → 网络延迟 / leader 处理慢
  │   └─ queueWait 慢   → worker 池饱和（SimulationLimit=100 不够）
  │
  ├─ 看 CommitSimulation timing
  │   ├─ buildSignatures 慢 → 签名聚合
  │   ├─ submitTx       慢 → tx pool 提交阻塞
  │
  ├─ 看 RetryCommit timing（重试次数多时）
  │   ├─ stateDbCheck 慢 → 链上锁检查（key 多）
  │   ├─ tryLock      慢 → TempLockView 锁竞争
  │   └─ patchPoolCheck 慢 → PatchPool 匹配
  │
  └─ 看 tryToReSimulation timing
      ├─ retryCalls 慢     → 跨 shard RetryCommit 汇总慢
      └─ resultCheck/Wound → 大量 wound 失败
```

#### 按管线 grep

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

#### 按交易 hash 跟踪全生命周期 + 时序

```bash
TXHASH="0x..."
# 看完整生命周期
bash ssc_grep.sh "$TXHASH" | grep -E "add a cross shard Tx|start simulate|cx transaction, end|simulation accomplished|handle commit vote|leader close transaction"
# 看时序分解
bash ssc_grep.sh "$TXHASH" | grep "timing breakdown"
```

---

## 七、分析脚本速查

### 7.1 analyze-all-timing.py（一站式全部时序）

| 属性 | 值 |
|:-----|:----|
| **位置** | `scripts/analyze-all-timing.py`（日志目录下运行或传入文件路径） |
| **覆盖** | 全部 19 个 timing breakdown 日志，一站式输出 P50/P90/P99/avg/max |
| **用法** | `../analyze-all-timing.py`（cd 到日志目录后）或 `../analyze-all-timing.py <glob>` |

### 7.2 子级分析脚本

| 脚本 | 覆盖埋点 | 用法 |
|:-----|:---------|:-----|
| `analyze-cr-timing.py` | `CommitOrRollbackWithProof timing breakdown` | `../analyze-cr-timing.py` |
| `analyze-close-timing.py` | `closeTransaction timing breakdown` | `../analyze-close-timing.py` |
| `analyze-tx-timing.py` | SSC tx / Normal tx commit timing | `../analyze-tx-timing.py --type ssc` |
| `analyze-total-timing.py` | CR vs SSC vs Normal 三类总量占比 | `../analyze-total-timing.py` |

### 7.3 统计脚本

| 脚本 | 用途 |
|:-----|:------|
| `scripts/chain-stats-accumulate.py` | 跨 dump 累计 CHAIN_RETRY_STATS |
| `scripts/latency-breakdown.py` | 三段式时延分解（入池→模拟→跨分片） |
| `scripts/retry-pool-wait.py` | RP 等待时间分析 + 被动池效果评估 |
| `scripts/lock-conflict-stats.py` | 锁冲突覆盖度 + HotKeyRetry 链效率 |

---

## 八、注意事项

### 8.1 Timing Breakdown 累计值语义

所有 `timing breakdown` 字段使用 `time.Since(t0)` — 到起点的累积值，不是增量。两个连续字段的 max 接近时（如 stateLock=963ms, simCleanup=965ms），尖峰发生在第一阶段（delta=2ms），第二阶段本身快速。

### 8.2 所有 return 分支都要打印

`RetryCommit`、`AddToRetry`、`tryToReSimulation` 有多个提前 return 分支，每个分支都必须打印 timing log。使用 `defer` 在 return 前统一打印可避免遗漏。

### 8.3 `ns` 解析必须在 `s` 之前

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

---

*编写于 2026-06-12，修订于 2026-07-01（合并 DSN-20 全链路计时埋点）*
