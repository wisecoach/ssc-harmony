# 进度总览（PROGRESS）

> 由文档整理自动刷新（2026-09）。文件移动本身即状态变更，见 `docs/README.md`。


## 设计文档 — Planned

| 编号 | 状态 |
|:-----|:-----|
| DSN-40-batch-verify-fix | planned |
| DSN-41-batch-worker-pool | active |

## 设计文档 — Active

| 编号 | 状态 |
|:-----|:-----|
| DSN-01-sscc-refactor-plan | active |
| DSN-02-lock-retry-mechanism | active |
| DSN-03-cr-priority-optimization | implemented |
| DSN-04-onchain-retry-limit | active |
| DSN-05-hotkey-retry-design | implemented |
| DSN-06-lock-priority-coordination | implemented |
| DSN-08-retry-limit | active |
| DSN-09-retry-timeout-and-limit | active |
| DSN-10-log-query-guide | active |
| DSN-11-log-lifecycle | active |
| DSN-17-stateLock-optimization-plan | active |
| DSN-18-sync-map-txstates | planned |
| DSN-19-simuLock-per-tx | implemented |
| DSN-20-simulation-timing-instrumentation | proposed |
| DSN-21-retryScheduler-per-tx-lock | proposed |
| DSN-22-retrycommit-lock-order-fix | design |
| DSN-24-unified-lock-check | design |
| DSN-25-retrycommit-state-cache | design |
| DSN-27-tlv-mutex-contention | active |
| DSN-28-stateLockManager-lock-free | draft |
| DSN-31-skip-crossshardtx-pool | active |
| DSN-32-commit-txs-time-budget | active |
| DSN-33-verifycontext-cleanup | active |
| DSN-34-parallel-exec-verify | design |
| DSN-35-batch-parallel-verify | draft |
| DSN-36-verifycontext-simnum-isolation | planned |
| DSN-37-StateProcessor-BatchVerify | active |
| DSN-42-worker-throttle | active |
| DSN-43-gRPC-migration | active |
| DSN-44-txsubmitter-priority-heap | active |
| DSN-45-batch-simtx-parallel-verify | planned |
| DSN-46-ssc-internal-pool-dag | planned |
| DSN-47-internal-tx-structure | implemented |
| DSN-48-simple-ssc-internal-pool | implemented |
| DSN-49-unify-offchain-patch-pool | implemented |
| DSN-50-ssc-dag-rescue-coverage-and-depth-cap | implemented |
| DSN-51-dag-deterministic-chainbuild | planned |
| DSN-52-crossshard-deadlock-resolution | implemented |
| DSN-53-crossshard-deadlock-detection | active |

## 设计文档 — Archived（只读）

| 编号 | 状态 |
|:-----|:-----|
| DSN-07-force-simulation | implemented |
| DSN-09-active-passive-retry-pool | design |
| DSN-12-rollback-v1-passive-pool | active |
| DSN-13-cr-hotkey-chaining-design | active |
| DSN-14-sim-dag-chaining-design | active |
| DSN-15-sim-dag-chaining-progress | active |
| DSN-16-lock | active |
| DSN-23-patchpool-dag-design | design |
| DSN-26-patchpool-dag-tlv-extension | design |
| DSN-29-patchpool-refactor-subscriber-index | active |
| DSN-30-gRPC-migration | active |
| DSN-30-patchpool-merge-into-retryscheduler | active |

## 研究 / 评审

Research: EXP-01-stateLock-analysis, EXP-02-bug-report, EXP-03-bug-fix-record, EXP-04-retry-tx-chain-build-simtx, EXP-05-chain-lock-leak-analysis, EXP-07-chainpatch-analysis, RSH-01-retryscheduler-complexity, RSH-02-perf-instrumentation, RSH-03-full-perf-instrumentation, RSH-03-perf-util-impl, RSH-03-priority-aware-reverse-cmh, RSH-04-perf-sampling-architecture
Reviews:  B01-log-query-guide-rev, DSN-29-patchpool-refactor-review, DSN-41-implementation-review, REV-01-design-parallel-simtx, timing-breakdown-log-catalog

## 决策记录

DEC-01-priority-first-sim-block

## 简报 / 交接

BRF-01-handoff-2026-06-25, BRF-02-handoff-2026-06-28, BRF-03-handoff-2026-06-29, BRF-04-handoff-2026-06-30, BRF-05-handoff-DSN47-48-json2protobuf-2026-08-27, BRF-06-handoff-DSN47-48-fixes-v1body-rlp-2026-08-27, BRF-07-handoff-DSN48-per-shard-isolation-block-time-2026-08-28, BRF-08-handoff-DAG-monitor-fixes-DSN49-2026-08-28, BRF-09-handoff-DAG-patch-rewrite-2026-08-29

## 缺陷（Issues）

Open:   BUG-09-batch-verify-process-unfinished, BUG-10-phase0.5-arbitration-timeout, BUG-11-chainpatch-timing-and-merge, BUG-12-shard1-lock-leak, BUG-13-high-latency-handoff
Closed: BUG-04-retry-concurrent-reenter, BUG-08-templock-leak-after-recall-conflict
