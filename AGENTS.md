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
| Local code | `/mnt/E/gowork/src/github.com/wisecoach/harmony-sscc` |
| Remote code server | `zjnu@10.7.95.199 -p 10022` → `~/go/src/github.com/harmony-one/harmony-sscc/` |
| Remote conda env | `cli-py` |
| Remote log dir | `~/go/src/github.com/harmony-one/logs/harmony-sscc/` |
| Experiment results | `data_process/output/throughput/RATE=*/HMY-SSCC/*_result.txt` |
| Test entry | `auto_test.sh` → `test_single` |
| Log tools | `ssc_grep.sh` (SSC), `zero_grep.sh` (Harmony node) |

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
