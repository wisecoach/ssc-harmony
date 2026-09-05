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

Docs live under `docs/designs/` (`planned/`, `active/`, `archived/`) following `DSN-xx` naming.
**Always read the relevant active DSN before modifying a system** — the full index is `docs/README.md`.

### Current / active (read first)

| DSN | Title | Key Topic |
|-----|-------|-----------|
| DSN-01 | SSCC Refactor Plan | Module split (Simulator/Verifier/Committer/RetryScheduler) |
| DSN-02 | Lock Retry Mechanism | Lock + retry design (TLV / SLM / stateDB three layers) |
| DSN-03 | CR Priority Optimization | Commit/Rollback priority ordering |
| DSN-04 | On-Chain Retry Limit | On-chain retry bound (design thread; impl via DSN-08) |
| DSN-05 | HotKey Retry Design | Hot-key chain / PatchPool lineage |
| DSN-06 | Lock Priority Coordination | Wound-Wait / priority coordination lineage |
| DSN-08 | Retry Limit | Chain + total retry limits (implemented) |
| DSN-09 | Retry Timeout & Limit | Off-chain retryScheduler timeout / cap |
| DSN-10 / 11 | Log guide / log lifecycle | SSC log levels & lifecycle catalog (运维参考, 非设计) |
| DSN-22 | RetryCommit Lock Order Fix | Three-phase RetryCommit (TLV → stateDB → DAG) |
| DSN-24 | Unified Lock Check | TLV + SLM + Patch three-layer arbitration |
| DSN-25 | RetryCommit StateDB Cache | OnBlockCommitted stateDB cache for retry race |
| DSN-27 | TLV → sync.Map | Remove global v.mu, full sync.Map |
| DSN-28 | SLM lock-free | MVCC / per-tx lock refactor (draft) |
| DSN-45~48 | Internal tx / pool / parallel verify | SSC internal-tx structure + pool (current basis) |
| DSN-49~51 | Off-chain DAG | offChainDAG unify + rescue caps + DAG rewrite (in flight) |
| DSN-52 | Deadlock resolution | Wait-Die + TLV wound layered convergence (implemented) |
| DSN-53 | Deadlock detection | Priority-aware reverse CMH probe (active) |

### Archived (历史只读, `docs/designs/archived/`)

Superseded / deprecated / one-off / shelved designs — keep for history only:
`DSN-07` (ForceSimulation 暂关), `DSN-09` passive pool (deprecated), `DSN-12` (rollback v1 pool, done),
`DSN-13~16` (old chaining/lock), `DSN-23/26/29/30` (old PatchPool/DAG generation, rewritten by DSN-49~51).

Research reports (EXP / RSH) in `docs/research/` for deep bug analysis.

### 从 hindsight 检索文档摘要 / 关系（镜像检索，非权威）

每份 DSN / DEC 文档的「一句话摘要 + 与其它文档的关系」会被同步到本机 **hindsight** 记忆库
（bank = `harmony-sscc`，UI: http://localhost:9999/banks/harmony-sscc）。
它是 `docs/README.md` 的**可检索镜像**——需要快速判断“某设计讲什么、和谁相关/被谁取代”时优先用它，
但**任何改动前请回到仓库里读原始 DSN 全文**（`docs/` 是唯一权威来源）。

- bank 中每条 memory：`document_id` = DSN/DEC 编号；`metadata.path` 指向仓库相对路径；`tags` 含目录/状态；`content` 含摘要与关系。
- 摄入脚本（增量维护，改 manifest 后重跑）：
  ```bash
  python3 docs/tools/hindsight/ingest.py --dry-run   # 预览
  python3 docs/tools/hindsight/ingest.py             # 写入 harmony-sscc（async）
  ```
- 检索示例（数据面 API，:8888；控制面 :9999 只是 UI）：
  ```bash
  # 语义检索（需 LLM 可用）
  curl -s -X POST http://localhost:8888/v1/default/banks/harmony-sscc/memories/recall \
    -H 'content-type: application/json' \
    -d '{"query":"跨分片死锁 Wait-Die 由哪个 DSN 实现？","tags":["doc"]}'

  # 无 LLM 的稳妥回退：按 document_id（=DSN/DEC 编号）精确取回该文档被抽取的事实
  curl -s "http://localhost:8888/v1/default/banks/harmony-sscc/memories/list?document_id=DSN-52&limit=20"
  # 或按关键词 q 过滤：curl ".../memories/list?q=Wait-Die"
  ```
  `recall` 返回语义命中的事实文本（适合“找相关设计/关系”）；若 LLM 不可用，用 `list?document_id=<编号>` 直接取回单份文档的事实（含关系边），不需要 LLM。

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

### 4.3 Test — ⚠️ 禁止本地 go test / make test

> **规则**：本项目**不做本地 `go test` / `make test`**——没有本地测试环境，且存在引用旧 API 的陈旧 `*_test.go`（如 `ssc/simulation_test.go`、`ssc/simulation_params_test.go`，最后更新于 `fb77273b6`），`go test` 会把它们编进测试二进制导致编译失败（这是**预存问题**，与改动无关，别被它误导）。

**代码验证的正确方式**（本地）：

```bash
go build ./ssc/...    # ✅ 编译全部生产代码 + 改动，这是本地的验证入口
go vet ./ssc/...      # ✅ 类型/静态检查（如遇 BLS CGo 报错，grep -v bls.h 过滤）
```

**行为验证的唯一方式**：远程部署重编译 binary（见 4.2）→ 跑实验 → 分析日志。改代码后的验收**永远靠远程实验结果，不是本地测试套件**。

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
| Local code | `/mnt/E/gowork/src/github.com/wisecoach/harmony-sscc` |
| Remote code server | `zjnu@10.7.95.199 -p 10022` → `~/go/src/github.com/harmony-one/harmony-sscc/` |
| Remote conda env | `cli-py` |
| Remote log dir | `~/go/src/github.com/harmony-one/logs/harmony-sscc/` |
| Experiment results | `data_process/output/throughput/RATE=*/HMY-SSCC/*_result.txt` |
| Test entry | `auto_test.sh` → `test_single` |
| Log tools | `ssc_grep.sh` (SSC), `zero_grep.sh` (Harmony node) |

### 6.1 SSH 连接远程（必须按此方式）

**远程地址**：`zjnu@10.7.95.199 -p 10022`（仅 SSH key 认证，无密码）。

**⚠️ 三个必须**：
1. **必须 `-F /dev/null`**：本机 `/etc/ssh/ssh_config.d/20-systemd-ssh-proxy.conf` 权限损坏，不带 `-F /dev/null` 会直接报 `Bad owner or permissions ...`。
2. **必须显式 `-i ~/.ssh/id_ed25519 -o IdentitiesOnly=yes`**：199 只授权 `~/.ssh/id_ed25519` 这把 key。不要依赖默认 key 发现（连接会不稳定/挂起），也不要试 `~/.ssh/id_rsa_new`（199 未授权，`Permission denied`）。
3. **连接可能间歇性不稳**（尤其从沙箱环境）：失败/无输出时重试即可；大批量命令尽量合成一条远程命令执行，减少往返。

**标准命令**（SSH / SCP 都按这个参数）：

```bash
SSH_KEY=~/.ssh/id_ed25519
# SSH
ssh -F /dev/null -i "$SSH_KEY" -p 10022 -o ConnectTimeout=15 -o BatchMode=yes \
    -o StrictHostKeyChecking=no -o IdentitiesOnly=yes zjnu@10.7.95.199 '<cmd>'
# SCP 上传
scp -F /dev/null -i "$SSH_KEY" -P 10022 -o StrictHostKeyChecking=no -o IdentitiesOnly=yes \
    <local_file> zjnu@10.7.95.199:<remote_path>
# SCP 拉取
scp -F /dev/null -i "$SSH_KEY" -P 10022 -o StrictHostKeyChecking=no -o IdentitiesOnly=yes \
    zjnu@10.7.95.199:<remote_file> <local_dir>/
```

**验证连通**：`ssh ... 'echo AUTH_OK; whoami'` → 期望 `AUTH_OK` + `zjnu`。

**本地一键拉取脚本**（已内置上述全部参数）：`scripts/local-ssc-retry-stats.py`（`--rate 200` 指定 RATE，或 `--dir '<logdir>'` 直接指定日志目录）。

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
