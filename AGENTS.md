# Harmony SSCC — Agent Guide

## 1. Project Overview

**Harmony SSCC** (Shared-State Cross-shard Consensus) — extended from Harmony blockchain. Implements cross-shard transaction processing with SSC (Speculation-based State Consensus), supporting:

- **Cross-shard transactions** via CX (Cross-shard Transfer) protocol
- **Speculative execution + verification** (SimTx → Verify → Commit/Rollback)
- **Retry mechanism** with priority-based Wound-Wait scheduling
- **TempLockView (TLV)** — leader-side lock reservation for retry scheduling
- **PatchPool DAG** — multi-Patch upstream coverage for chain retry
- **HotKey chain retry** — priority-based retry chaining for high-contention keys

### Branch

Current branch: `ssc_shared_address_space` (ahead of main, experimental SSC work)

### Key dirs

| Path | What |
|------|------|
| `ssc/` | SSC core — simulator, verifier, retry scheduler, state lock, TLV, commit |
| `ssc/retry_scheduler.go` | Retry scheduler (OnBlockCommitted → retry signals → re-simulation) |
| `ssc/temp_lock_view.go` | TempLockView — leader-side lock reservation |
| `ssc/state_lock_impl.go` | stateLockManager — chain-level key locking |
| `ssc/state_locker.go` | stateDB level locker (pendingStates → CommitOrRollback) |
| `ssc/simulator.go` | SimTx simulation execution |
| `ssc/simulator_leader.go` | Leader-side simulation orchestration |
| `ssc/verify.go` | VerifySimulation — chain-level verification |
| `test/dev_deploy.sh` | Multi-node deployment for experiments |
| `auto_test.sh` | Test entrypoint (test_single, test_delay, test_shard) |
| `scripts/` | Build, deploy, log analysis utilities |
| `docs/` | Design documents (DSN), research reports (EXP), changelogs |
| `consensus/` | Consensus engine (BFT) |
| `node/` | Node service |

## 2. Design Documents (Read on Demand)

Docs are in `docs/` following DSN / EXP / DEC / BUG naming.

| DSN | Title | Key Topic |
|-----|-------|-----------|
| DSN-01 | SSCC Refactor Plan | Module split (Simulator/Verifier/Committer/RetryScheduler) |
| DSN-02 | Lock Retry Mechanism | Original retry + lock design |
| DSN-03 | CR Priority Optimization | Commit/Rollback priority ordering |
| DSN-05 | HotKey Retry Design | Priority + chain retry for hot keys |
| DSN-06 | Lock Priority Coordination | Wound-Wait + priority |
| DSN-07 | Force Simulation | Force re-simulation when state locked |
| DSN-09 | Active-Passive Retry Pool | Passive pool + active scheduling |
| DSN-22 | RetryCommit Lock Order Fix | Three-phase RetryCommit (TLV → stateDB → DAG) |
| DSN-23 | PatchPool DAG Design | Multi-Patch DAG covering upstream txs |
| DSN-24 | Unified Lock Check | TLV + SLM + Patch three-layer arbitration |
| DSN-25 | RetryCommit StateDB Cache | OnBlockCommitted stateDB cache for retry race |
| DSN-26 | PatchPool DAG → TLV Phase 1 | Using Patch to cover TLV lock conflict |
| DSN-27 | TLV → sync.Map | Remove global v.mu, full sync.Map (DONE, committed) |

Research reports (EXP) in `docs/research/` for deep bug analysis.

**Always read the relevant DSN before modifying a system.** The docs directory indexes everything — start there.

## 3. Architecture — Key Components

### 3.1 Transaction Lifecycle

```
User submits tx → [Add a cross shard Tx]
  → Simulator (StartSimulateCXTransaction) → SimTx created
    → Multicast SimTx to related shards
      → VerifySimulation on each related shard
        → Commit or Rollback
        → If conflict → retry pool
          → RetryScheduler.OnBlockCommitted → retry signals
            → RetryCommit (TLV Phase 1 → stateDB Phase 2a → DAG Phase 2b)
              → StartReSimulation → CommitSimulation → close or retry again
```

### 3.2 Lock System (Three Layers)

| Layer | Scope | File | Check |
|-------|-------|------|-------|
| **TLV (TempLockView)** | Leader-side per-key reservation | `temp_lock_view.go` | `TryLockWithPriority` via shard-locality key |
| **SLM (stateLockManager)** | Chain-level per-key pending lock | `state_lock_impl.go` | `IsKeyLocked` in CheckLock |
| **stateDB locker** | EVM state-level per-key lock | `state_locker.go` | `Lockable()` in GetState/SetState |

### 3.3 Retry Paths

| Path | Trigger | Coverage |
|------|---------|----------|
| **Normal retry** | OnBlockCommitted signals | All unfinished txs (may be disabled for HotKey-only) |
| **ChainNextSim (HotKey)** | Committed SimTx triggers downstream retry | Only txs with upstream SimTx dependency |
| **Passive pool retry** | Pool timeout / signal | Backup pool (DSN-09, DSN-12 deprecated) |

**Known pitfall**: Normal retry path may be blocked (`retry_scheduler.go:376`). Check logs for `normal retry signals blocked` before diagnosing high unfinished count.

## 4. Development Workflow

### 4.1 Build

```bash
# Full build (MCL/BLS libs + harmony binary)
make

# Quick binary build (after libs done)
bash ./scripts/go_executable_build.sh -S

# Binary goes to ./bin/harmony — this is what ships to remote nodes
```

### 4.2 Code Sync → Remote Experiment

See `harmony-experiment-runner` skill for the complete debug loop:

```
1. Sync code to remote 199
   source sync_code.sh && uploadCode zjnu@10.7.95.199

2. Verify binary freshness (critical step):
   ssh -p 10022 zjnu@10.7.95.199 'cd ~/go/src/github.com/harmony-one/harmony-sscc && \
     BIN_TS=$(stat -c "%Y" bin/harmony) && \
     SRC_TS=$(find . -name "*.go" -newer bin/harmony -not -path "./vendor/*" -not -path "./.git/*" | head -1) && \
     if [ -n "$SRC_TS" ]; then echo "⚠️ bin/harmony old! Recompile needed"; \
     else echo "✅ Fresh"; fi'

3. User runs test_single (ssh → auto_test.sh)

4. Analyze results
```

### 4.3 Test

```bash
make test          # Full Go test suite
cd ssc && go test  # SSC-specific tests
```

### 4.4 Log Analysis

**MUST use ssc_grep.sh / zero_grep.sh** — never raw grep on SSC logs.

```bash
# In remote log directory:
./ssc_grep.sh "keyword"       # SSC-specific logs
./zero_grep.sh "panic|fatal"  # Harmony node logs
```

When ssc_grep.sh env mismatch: manually set `SHARD_NUM`, `VALIDATOR`, `SSC`, `DELAY`, `RATE`, `VALIDATOR_PER_NODE`.

**Log level rule**: Any log used for counting/statistics → `Info`, not `Debug` (learned the hard way).

## 5. Common Pitfalls (Must-Know)

### 5.1 Remote Git corruption → `error obtaining VCS status`

Fix: rsync full source with `.git` from local to remote:
```bash
rsync -azP --delete -e "ssh -p 10022" \
  /mnt/E/gowork/src/github.com/wisecoach/harmony-sscc/ \
  zjnu@10.7.95.199:~/go/src/github.com/harmony-one/harmony-sscc/ \
  --exclude='.hmy' --exclude='tmp_log' --exclude='bin' --exclude='vendor' \
  --exclude='*.key' --exclude='validators.json' --exclude='expr_deploy_accounts.json'
```

⚠️ rsync preserves timestamps → `make` may skip recompilation. Always verify binary freshness.

### 5.2 StateDB trie pruning → `missing trie node`

`getStateDB` uses `StateAt(header.Root())` which gets pruned after ~2000 blocks. Fix: `rs.bc.State()` (current head).

### 5.3 Lock leaks

Known leaks (all fixed upstream, but verify):
- `deleteLockedState` didn't `delete(s.lockedStates, key)` — key zombie → leaked into global stateLockManager
- VerifySimulation Recall → lock not cleaned up in Phase 2

### 5.4 PoolTimeout = 100000 hardcoded

In `build_keys.go:286` — unfinshed txs never PoolTimeout-close. Intentional for debugging; change for production runs.

### 5.5 Normal retry path disabled

`retry_scheduler.go:376` may drop OnBlockCommitted signals (only chainNextSim active). Toggle by replacing blocking code with `go rs.sendReSimulationSignals(signals)`.

### 5.6 SSH conda pollution

```
ssh -p 10022 zjnu@10.7.95.199 'env -i HOME=$HOME PATH=$HOME/miniconda3/bin:/usr/bin:/bin \
  bash -l -c "cd ~/go/src/github.com/harmony-one/harmony-sscc && \
  source ~/miniconda3/etc/profile.d/conda.sh && conda activate cli-py && \
  source auto_test.sh && test_single"'
```

## 6. Experiment Environment

| Resource | Detail |
|----------|--------|
| Local code | `/mnt/D/e_backup/gowork/src/github.com/wisecoach/harmony-sscc` (or `/mnt/E/...`) |
| Remote code server (199) | `zjnu@10.7.95.199 -p 10022` → `~/go/src/github.com/harmony-one/harmony-sscc/` |
| Remote conda env | `cli-py` |
| Remote log dir | `~/go/src/github.com/harmony-one/logs/harmony-sscc/` |
| Experiment results | `data_process/output/throughput/RATE=*/HMY-SSCC/*_result.txt` |
| Test entry | `auto_test.sh` → `test_single` |
| Log tools | `ssc_grep.sh` (SSC), `zero_grep.sh` (Harmony node) |

### 6.1 VM Architecture

| Type | IP | Port | Role |
|:-----|:---|:----:|:-----|
| Code server | `10.7.95.199` | 10022 (SSH) | 代码同步、编译分发、日志汇总 |
| Worker VMs | `10.7.95.40` - `10.7.95.50` | 10022 (SSH) | 实验节点（容器化） |
| Container IPs | `10.7.95.200` - `10.7.95.203` | 9000+ | 实验内网（shard 0-3 leader） |

Port → Shard 映射：`shard = (port - 9000) / 40`。

**Before running any experiment**: User confirms environment ready. Don't spin up experiments autonomously.

## 7. Code Style & Conventions

- **Go 1.22.5**, GOPATH layout (`$GOPATH/src/github.com/harmony-one/harmony-sscc`)
- Log format: structured JSON via `utils.SSCLogger().Info().Str("key","val").Msg("message")`
- Timing: `defer` pattern with `time.Since(t0)` — `time` must be imported
- New Info-level logs for any new count/grep target
- Don't touch `vendor/` unless forced — dependencies via `go.mod`
- Git: branch off `ssc_shared_address_space`, commit with descriptive prefix
- **Never raw grep on logs** — use ssc_grep.sh/zero_grep.sh
- Always verify binary freshness before analyzing experiment results

## 8. Remote Log Operations

### 8.1 Log File Types

| 文件 | 内容 | 分析目标 |
|:-----|:-----|:---------|
| `ssc-validator-*.log` | SSC 日志（utils.SSCLogger） | **主要分析对象** — VerifySimulation, retry, commit |
| `zerolog-validator-*.log` | Harmony 本体日志（utils.Logger） | 共识层、区块提交 |
| `log-*.log` | 共识引擎日志 | ProposeNewBlock, BFT 消息 |

### 8.2 找目录

日志目录名从实验参数生成，格式：`shard={SHARD_NUM}_validator={VALIDATOR}_ssc={SSC}_delay={DELAY}_rate={RATE}_vpn={VALIDATOR_PER_NODE}`。

```bash
ssh -p 10022 zjnu@10.7.95.199

# 进入日志目录
cd ~/go/src/github.com/harmony-one/logs/harmony-sscc/

# 查有哪些实验
ls -d */

# 看到目录名后设变量
DIR="shard=4_validator=4_ssc=1_delay=10_rate=150_vpn=4"
```

`ssc_grep.sh` 通过 `source ../../.env` 加载环境变量，但 `.env` 可能不存在。替代方案：手动设环境变量或直接用 `cat | grep`。

### 8.3 Quick Start — 查日志

```bash
# 查看 SSC 日志
cat $DIR/ssc-validator*.log | grep "关键词" | head -20

# 查看共识日志
cat $DIR/log-*.log | grep "ProposeNewBlock" | head -20
```

### 8.4 文件名 → 节点映射

日志文件名格式：`ssc-validator-{IP}-{port}.log`

| port | shard | 节点角色 |
|:----:|:-----:|:---------|
| 9000 | 0 | shard 0 leader |
| 9040 | 1 | shard 1 leader |
| 9080 | 2 | shard 2 leader |
| 9120 | 3 | shard 3 leader |
| 9020-9039 | 0 | shard 0 validator |
| 9060-9079 | 1 | shard 1 validator |
| 9100-9119 | 2 | shard 2 validator |
| 9140-9159 | 3 | shard 3 validator |

对应容器 IP：`10.7.95.200` = shard 0, `10.7.95.201` = shard 1, 依此类推。

**只查 leader 日志**（大部分 SSC 事件只在 leader 端口出现）：
```bash
cat $DIR/ssc-validator-10.7.95.200-9000.log \
    $DIR/ssc-validator-10.7.95.201-9040.log \
    $DIR/ssc-validator-10.7.95.202-9080.log \
    $DIR/ssc-validator-10.7.95.203-9120.log | grep "关键词"
```

### 8.5 ssc_grep.sh 使用

`ssc_grep.sh` 和 `zero_grep.sh` 依赖环境变量查找目录。如果 env 未设置，手动指定：

```bash
SHARD_NUM=4 VALIDATOR=4 SSC=1 DELAY=10 RATE=100 VALIDATOR_PER_NODE=4 \
  bash ssc_grep.sh "VerifySimulation: success"
```

`ssc_grep.sh` 搜索 SSC 日志（ssc-validator-*.log），`zero_grep.sh` 搜索共识日志（log-9*.log）。

**注意**：`ssc_grep.sh` 可能因 env 缺失报错。安全做法是设好 `DIR` 后用 `cat $DIR/ssc-validator*.log | grep`。

### 8.6 常用日志分析模式

```bash
# 查看总日志量
wc -l $DIR/ssc-validator*.log

# 查 VerifySimulation 统计
cat $DIR/ssc-validator*.log | grep "VerifySimulation: success" | wc -l

# 查 retry 漏斗
cat $DIR/ssc-validator*.log | grep "retry commit success" | wc -l
cat $DIR/ssc-validator*.log | grep "retry commit failed" | wc -l
cat $DIR/ssc-validator*.log | grep "HandleRetrySignal: received" | wc -l
cat $DIR/ssc-validator*.log | grep "consumed patches found" | wc -l

# 查 CHAIN_RETRY_STATS（Info 级别，每块输出）
cat $DIR/ssc-validator*.log | grep "=== CHAIN_RETRY_STATS ===" | tail -5

# 查 OnBlockCommitted stats（所有模块数据大小）
cat $DIR/ssc-validator*.log | grep "OnBlockCommitted stats" | tail -5

# 查 stack overflow / panic
cat $DIR/ssc-validator*.log | grep -i "stack overflow"
cat $DIR/log-*.log 2>/dev/null | grep -i "stack overflow"
cat $DIR/zerolog-validator*.log | grep -i "panic"

# 查 blocks 提交
cat $DIR/log-*.log 2>/dev/null | grep "ProposeNewBlock breakdown"

# 查 SSC tx commit timing（块内逐笔耗时）
cat $DIR/ssc-validator*.log | grep "SSC tx commit timing"

# 查 commitTransaction timing breakdown（交易类型区分）
cat $DIR/ssc-validator*.log | grep "commitTransaction timing breakdown"
```

### 8.7 性能分析 — JSON 管道模式

日志是 JSON 格式，可通过管道给 Python 做统计：

```bash
cat $DIR/ssc-validator*.log | grep '"message"."SSC tx commit timing"' | \
  python3 -c "
import sys, json
durations = []
for line in sys.stdin:
    d = json.loads(line.strip())
    dur = d.get('duration', '0ms')
    ms = float(dur.replace('ms','').replace('µs','').replace('s',''))
    durations.append(ms)
n = len(durations)
if n > 0:
    durations.sort()
    print(f'n={n} avg={sum(durations)/n:.2f}ms P50={durations[n//2]:.2f}ms P90={durations[int(n*0.9)]:.2f}ms')
"
```

### 8.8 日志陷阱

- **`ssc_grep.sh` 依赖 `.env` 文件** — 如果 source 失败，手动设 env 或用 `cat | grep` 代替
- **日志 JSON 含非标准字段**（如 `message` 带 `===` 前缀）— grep `message` 字段时用 `grep '"message"."..."'` 绕过不标准格式
- **大日志（100K txs ~ 46GB）** — `cat 整个文件 | grep` 可能在 SSH 中超时。用 `head/tail` 限制范围，或进 SSH 交互模式
- **`leader close transaction` 无 blockNum** — 无法直接关联到块高度，用 CRTx 时序近似
