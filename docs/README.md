# Harmony SSCC 文档

> 本文档结构遵循文档模板：`/mnt/E/pywork/hermes-a2a-coding-workflow/docs_template/`

> 归档=状态变更：文件从 `active/`/`open/` 移入 `archived/`/`closed/` 即表示该设计/缺陷已结束，**只读不改**。


## 目录 → 文档类型对照表

| 目录 | 放的文档 | 前缀 | 说明 |
|:-----|:---------|:----:|:-----|
| `briefs/` | 客户简报 + HANDOFF 交接 | `BRF` | 每日交接记录 |
| `designs/planned/` | 设计文档（规划/草案） | `DSN` | 🔵 未来方向 |
| `designs/active/` | 设计文档（在讨论或实施） | `DSN` | 🟡 进行中 |
| `designs/archived/` | 设计文档（已实现/已废弃，只读） | `DSN` | 🟢 历史，不删不改 |
| `decisions/` | 决策记录 | `DEC` | 固定 done，不可变 |
| `research/` | 探索报告 + 现状审计 | `EXP`,`RSH`,`SIT` | 性能分析、bug 报告 |
| `issues/open/` | 未关闭缺陷/任务 | `BUG`,`FEAT`,`TASK` | 🔴 未解决 |
| `issues/closed/` | 已关闭缺陷/任务 | `BUG`,`TASK` | 🟢 已修复 |
| `reviews/` | 代码/设计评审报告 | `REV` | 固定 done |
| `changelogs/` | 进度总览 + 变更记录 | `PROGRESS`,`CHANGELOG` | 持续更新 |
| `_templates/` | 各类文档模板 | 各类 | 新文档从此复制 |


## 文档索引


### Designs — planned

- [DSN-40-batch-verify-fix — DSN-40-batch-verify-fix.md](designs/planned/DSN-40-batch-verify-fix.md)
- [DSN-41-batch-worker-pool — DSN-41-batch-worker-pool.md](designs/planned/DSN-41-batch-worker-pool.md)

### Designs — active

- [DSN-01-sscc-refactor-plan — SSCC 模块化重构总览](designs/active/DSN-01-sscc-refactor-plan.md)
- [DSN-02-lock-retry-mechanism — 锁与重试机制](designs/active/DSN-02-lock-retry-mechanism.md)
- [DSN-03-cr-priority-optimization — CR 提交优先级优化](designs/active/DSN-03-cr-priority-optimization.md)
- [DSN-04-onchain-retry-limit — 链上重试次数限制](designs/active/DSN-04-onchain-retry-limit.md)
- [DSN-05-hotkey-retry-design — HotKey 链式重试（数据层）](designs/active/DSN-05-hotkey-retry-design.md)
- [DSN-06-lock-priority-coordination — 锁优先级协调（协调层）](designs/active/DSN-06-lock-priority-coordination.md)
- [DSN-08-retry-limit — 重试次数限制（Retry Limit Control）](designs/active/DSN-08-retry-limit.md)
- [DSN-09-retry-timeout-and-limit — [DSN-09] RetryScheduler 链下超时与重试超限](designs/active/DSN-09-retry-timeout-and-limit.md)
- [DSN-10-log-query-guide — DSN-10: 日志查询与分级指南（DSN-29+ 更新版）](designs/active/DSN-10-log-query-guide.md)
- [DSN-11-log-lifecycle — 交易生命周期日志大全](designs/active/DSN-11-log-lifecycle.md)
- [DSN-17-stateLock-optimization-plan — stateLock 锁范围优化分析 — 所有代码位置与优化判断](designs/active/DSN-17-stateLock-optimization-plan.md)
- [DSN-18-sync-map-txstates — sync.Map + per-entry lock 改造方案](designs/active/DSN-18-sync-map-txstates.md)
- [DSN-19-simuLock-per-tx — Simulator 全局锁 → sync.Map + per-entry 改造](designs/active/DSN-19-simuLock-per-tx.md)
- [DSN-20-simulation-timing-instrumentation — 模拟管线 + 重试管线全链路计时埋点](designs/active/DSN-20-simulation-timing-instrumentation.md)
- [DSN-21-retryScheduler-per-tx-lock — retryScheduler 全局锁 rs.mu → sync.Map + per-entry 改造](designs/active/DSN-21-retryScheduler-per-tx-lock.md)
- [DSN-22-retrycommit-lock-order-fix — [DSN-22] RetryCommit 锁检查顺序修正 — PatchPool 仅覆盖 stateDB 冲突](designs/active/DSN-22-retrycommit-lock-order-fix.md)
- [DSN-24-unified-lock-check — [DSN-24] 统一锁检查 — TLV + SLM + ChainPatch 三层联合仲裁](designs/active/DSN-24-unified-lock-check.md)
- [DSN-25-retrycommit-state-cache — [DSN-25] OnBlockCommitted StateDB 缓存 — 消除 RetryCommit ↔ ReSimulation 锁检查 Race](designs/active/DSN-25-retrycommit-state-cache.md)
- [DSN-27-tlv-mutex-contention — [DSN-27] TempLockView 全局 Mutex 争抢优化 — 全部 sync.Map 化](designs/active/DSN-27-tlv-mutex-contention.md)
- [DSN-28-stateLockManager-lock-free — [DSN-28] stateLockManager 无锁重构 — 版本化 MVCC + per-tx 锁](designs/active/DSN-28-stateLockManager-lock-free.md)
- [DSN-31-skip-crossshardtx-pool — DSN-31: CrossShardTx 不入池，直启模拟](designs/active/DSN-31-skip-crossshardtx-pool.md)
- [DSN-32-commit-txs-time-budget — DSN-32: CommitTxs 时间预算](designs/active/DSN-32-commit-txs-time-budget.md)
- [DSN-33-verifycontext-cleanup — DSN-33: VerifyContext 清理 + GetResult 子上下文化](designs/active/DSN-33-verifycontext-cleanup.md)
- [DSN-34-parallel-exec-verify — DSN-34: SimTx 并行验证设计方案](designs/active/DSN-34-parallel-exec-verify.md)
- [DSN-35-batch-parallel-verify — DSN-35: 跨 SimTx 批量并行验证](designs/active/DSN-35-batch-parallel-verify.md)
- [DSN-36-verifycontext-simnum-isolation — DSN-36 — VerifyContext 分 SimulationNum 隔离](designs/active/DSN-36-verifycontext-simnum-isolation.md)
- [DSN-37-StateProcessor-BatchVerify — DSN-37: StateProcessor 支持 BatchVerify](designs/active/DSN-37-StateProcessor-BatchVerify.md)
- [DSN-42-worker-throttle — DSN-42 — 模拟 Worker 动态调速](designs/active/DSN-42-worker-throttle.md)
- [DSN-43-gRPC-migration — DSN-43 — JSON-RPC → gRPC + Protobuf 迁移](designs/active/DSN-43-gRPC-migration.md)
- [DSN-44-txsubmitter-priority-heap — DSN-44: TxSubmitter 排序 Buffer + Priority Heap](designs/active/DSN-44-txsubmitter-priority-heap.md)
- [DSN-45-batch-simtx-parallel-verify — DSN-45-batch-simtx-parallel-verify.md](designs/active/DSN-45-batch-simtx-parallel-verify.md)
- [DSN-46-ssc-internal-pool-dag — DSN-46-ssc-internal-pool-dag.md](designs/active/DSN-46-ssc-internal-pool-dag.md)
- [DSN-47-internal-tx-structure — DSN-47-internal-tx-structure.md](designs/active/DSN-47-internal-tx-structure.md)
- [DSN-48-simple-ssc-internal-pool — DSN-48-simple-ssc-internal-pool.md](designs/active/DSN-48-simple-ssc-internal-pool.md)
- [DSN-49-unify-offchain-patch-pool — DSN-49-unify-offchain-patch-pool.md](designs/active/DSN-49-unify-offchain-patch-pool.md)
- [DSN-50-ssc-dag-rescue-coverage-and-depth-cap — DSN-50-ssc-dag-rescue-coverage-and-depth-cap.md](designs/active/DSN-50-ssc-dag-rescue-coverage-and-depth-cap.md)
- [DSN-51-dag-deterministic-chainbuild — DSN-51-dag-deterministic-chainbuild.md](designs/active/DSN-51-dag-deterministic-chainbuild.md)
- [DSN-52-crossshard-deadlock-resolution — DSN-52-crossshard-deadlock-resolution.md](designs/active/DSN-52-crossshard-deadlock-resolution.md)
- [DSN-53-crossshard-deadlock-detection — DSN-53 v2 —— 跨分片死锁检测（优先级感知反向 CMH）](designs/active/DSN-53-crossshard-deadlock-detection.md)

### Designs — archived（历史只读）

- [DSN-07-force-simulation — ForceSimulation（冲突容忍模拟）](designs/archived/DSN-07-force-simulation.md)
- [DSN-09-active-passive-retry-pool — 被动 Retry Pool — 跨分片锁冲突的 Event-driven 等待机制](designs/archived/DSN-09-active-passive-retry-pool.md)
- [DSN-12-rollback-v1-passive-pool — 修复清单：回退 v1 被动池，对齐 v2 设计](designs/archived/DSN-12-rollback-v1-passive-pool.md)
- [DSN-13-cr-hotkey-chaining-design — CR HotKey Chaining Design (DEPRECATED)](designs/archived/DSN-13-cr-hotkey-chaining-design.md)
- [DSN-14-sim-dag-chaining-design — Sim DAG Chaining Design](designs/archived/DSN-14-sim-dag-chaining-design.md)
- [DSN-15-sim-dag-chaining-progress — Sim DAG Chaining — 实现进度表](designs/archived/DSN-15-sim-dag-chaining-progress.md)
- [DSN-16-lock — Harmony SSC 锁与重试方案](designs/archived/DSN-16-lock.md)
- [DSN-23-patchpool-dag-design — [DSN-23] PatchPool DAG — 多 Patch 联合覆盖冲突 key](designs/archived/DSN-23-patchpool-dag-design.md)
- [DSN-26-patchpool-dag-tlv-extension — [DSN-26] PatchPool DAG 扩展至 TLV Phase 1 — 用 Patch 覆盖 TLV 锁冲突](designs/archived/DSN-26-patchpool-dag-tlv-extension.md)
- [DSN-29-patchpool-refactor-subscriber-index — DSN-29: PatchPool 重构 + Subscriber 索引 + 增量扫描](designs/archived/DSN-29-patchpool-refactor-subscriber-index.md)
- [DSN-30-gRPC-migration — DSN-30 — JSON-RPC → gRPC + Protobuf 迁移](designs/archived/DSN-30-gRPC-migration.md)
- [DSN-30-patchpool-merge-into-retryscheduler — DSN-30: PatchPool 合并入 RetryScheduler](designs/archived/DSN-30-patchpool-merge-into-retryscheduler.md)

### Research（探索 / 审计 / 论文向）

- [EXP-01-stateLock-analysis — EXP-01-stateLock-analysis.md](research/EXP-01-stateLock-analysis.md)
- [EXP-02-bug-report — Bug 反馈报告：A06 / A07 设计文档 vs 代码实现偏差](research/EXP-02-bug-report.md)
- [EXP-03-bug-fix-record — Bug 修复记录：A05 / A06 / A07 代码实现问题](research/EXP-03-bug-fix-record.md)
- [EXP-04-retry-tx-chain-build-simtx — Bug 追踪：链式重试交易无法完成 SimTx 上链](research/EXP-04-retry-tx-chain-build-simtx.md)
- [EXP-05-chain-lock-leak-analysis — EXP-05: 链上锁泄露分析报告](research/EXP-05-chain-lock-leak-analysis.md)
- [EXP-07-chainpatch-analysis — EXP-07-chainpatch-analysis.md](research/EXP-07-chainpatch-analysis.md)
- [RSH-01-retryscheduler-complexity — RSH-01: retryScheduler 计算复杂度分析](research/RSH-01-retryscheduler-complexity.md)
- [RSH-02-perf-instrumentation — RSH-02: retryScheduler 性能指标埋点方案](research/RSH-02-perf-instrumentation.md)
- [RSH-03-full-perf-instrumentation — RSH-03: Harmony-SSCC 全模块性能埋点方案](research/RSH-03-full-perf-instrumentation.md)
- [RSH-03-perf-util-impl — perfLog 工具函数 — 统一性能埋点](research/RSH-03-perf-util-impl.md)
- [RSH-03-priority-aware-reverse-cmh — RSH-03: Priority-Aware Reverse Chandy-Misra-Haas for Cross-Shard Deadlock Detection](research/RSH-03-priority-aware-reverse-cmh.md)
- [RSH-04-perf-sampling-architecture — RSH-04: 性能埋点架构 — 采样 + 分级 + 降损](research/RSH-04-perf-sampling-architecture.md)

### Decisions

- [DEC-01-priority-first-sim-block — Priority 饥饿问题：FirstSimBlock 替代 SimulationNum](decisions/DEC-01-priority-first-sim-block.md)

### Briefs（交接）

- [BRF-01-handoff-2026-06-25 — Handoff：2026-06-25 三连 Bug 修复 + 优先级重构](briefs/BRF-01-handoff-2026-06-25.md)
- [BRF-02-handoff-2026-06-28 — HANDOFF — Harmony SSCC 开发交接](briefs/BRF-02-handoff-2026-06-28.md)
- [BRF-03-handoff-2026-06-29 — HANDOFF — Harmony SSCC 毒性循环分析与被动池方案](briefs/BRF-03-handoff-2026-06-29.md)
- [BRF-04-handoff-2026-06-30 — HANDOFF — Harmony SSCC 被动池回退与 v2 设计交接](briefs/BRF-04-handoff-2026-06-30.md)
- [BRF-05-handoff-DSN47-48-json2protobuf-2026-08-27 — HANDOFF — DSN-47/48 内部交易结构 + 简单内部池 + JSON→protobuf 迁移](briefs/BRF-05-handoff-DSN47-48-json2protobuf-2026-08-27.md)
- [BRF-06-handoff-DSN47-48-fixes-v1body-rlp-2026-08-27 — HANDOFF — DSN-47/48 执行流适配修复 + JSON→protobuf 迁移 + v1 区块体 SSC 丢失 BUG](briefs/BRF-06-handoff-DSN47-48-fixes-v1body-rlp-2026-08-27.md)
- [BRF-07-handoff-DSN48-per-shard-isolation-block-time-2026-08-28 — HANDOFF — DSN-48 落地回归：跨分片 SimTx 隔离 + 出块 1s 预算 + 日志 SSC 计数](briefs/BRF-07-handoff-DSN48-per-shard-isolation-block-time-2026-08-28.md)
- [BRF-08-handoff-DAG-monitor-fixes-DSN49-2026-08-28 — HANDOFF — SSC 模块监控接口 + DAG 泄漏/环/误判修复 + DSN-49 合并方案（待实现）](briefs/BRF-08-handoff-DAG-monitor-fixes-DSN49-2026-08-28.md)
- [BRF-09-handoff-DAG-patch-rewrite-2026-08-29 — HANDOFF — DAG Patch 方案总结：DSN-49/50 已实施但未解决超时，决定重写整个 DAG patch 方案](briefs/BRF-09-handoff-DAG-patch-rewrite-2026-08-29.md)

### Issues — open

- [BUG-09-batch-verify-process-unfinished — BUG-09-batch-verify-process-unfinished.md](issues/open/BUG-09-batch-verify-process-unfinished.md)
- [BUG-10-phase0.5-arbitration-timeout — BUG-10-phase0.5-arbitration-timeout.md](issues/open/BUG-10-phase0.5-arbitration-timeout.md)
- [BUG-11-chainpatch-timing-and-merge — BUG-11-chainpatch-timing-and-merge.md](issues/open/BUG-11-chainpatch-timing-and-merge.md)
- [BUG-12-shard1-lock-leak — BUG-12 Shard 1 锁泄漏导致跨分片时序放大（RATE=150）](issues/open/BUG-12-shard1-lock-leak.md)
- [BUG-13-high-latency-handoff — BUG-13 时延高分析 — 交接文档（新 session 接手）](issues/open/BUG-13-high-latency-handoff.md)

### Issues — closed

- [BUG-04-retry-concurrent-reenter — BUG-04：链式重试交易永久卡住（锁泄漏 + 并发重入）](issues/closed/BUG-04-retry-concurrent-reenter.md)
- [BUG-08-templock-leak-after-recall-conflict — BUG-08：Patch 路径缺失 + Wound 误终结导致链上锁泄漏](issues/closed/BUG-08-templock-leak-after-recall-conflict.md)

### Reviews

- [B01-log-query-guide-rev — 日志查询指南（修订版）](reviews/B01-log-query-guide-rev.md)
- [DSN-29-patchpool-refactor-review — DSN-29 PatchPool 重构审查报告](reviews/DSN-29-patchpool-refactor-review.md)
- [DSN-41-implementation-review — [审查] DSN-41 Batch Worker Pool 实现 vs 设计文档](reviews/DSN-41-implementation-review.md)
- [REV-01-design-parallel-simtx — REV-01-design-parallel-simtx.md](reviews/REV-01-design-parallel-simtx.md)
- [timing-breakdown-log-catalog — Timing Breakdown 日志大全](reviews/timing-breakdown-log-catalog.md)

### Changelogs

- [CHANGELOG — 变更记录](changelogs/CHANGELOG.md)
- [PROGRESS — 进度总览](changelogs/PROGRESS.md)
- [SUMMARY-2026-06-25 — Session 总结：BUG-08 修复 + 优先级重构 + TempLockView 设计演进](changelogs/SUMMARY-2026-06-25.md)

---


## 命名规则

```
{前缀}-{序号}-{slug}.md
```
- 前缀全大写：`BRF`,`REQ`,`DSN`,`DEC`,`EXP`,`RSH`,`BUG`,`FEAT`,`TASK`,`REV`,`ENHANCE`,`SIT`


## 文档生命周期

```
需求/探索 → 设计(planned → active → archived)
                        │
                 决策记录(done, immutable)
                        │
                 任务/缺陷(open → closed)
                        │
                 评审报告(done)
```


> 提示：`designs/active` 中部分早期文档内部仍以 `[Axx]/[Bxx]` 旧前缀自称（如 DSN-01=A01、DSN-10/11=B01/B02），文件本身已按 `DSN-xx` 编号；仅内部文字沿用旧名。

