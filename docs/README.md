# Harmony SSCC 文档

> 本文档结构遵循 [DSN-02-文档规范](../designs/active/DSN-02-文档规范.md)（待创建）
>
> 模板来源：`/mnt/E/pywork/hermes-a2a-coding-workflow/docs_template/`

---

## 目录 → 文档类型对照表

| 目录 | 放的文档 | 前缀 | 说明 |
|:-----|:---------|:----:|:-----|
| `briefs/` | 客户简报 + HANDOFF | `BRF` | 每日交接记录 |
| `designs/active/` | 设计文档（进行中） | `Axx`, `Bxx` | SSCC 架构/机制设计 |
| `designs/archived/` | 设计文档（已归档） | `Zxx` | 旧版设计，不删不改 |
| `decisions/` | 决策记录 | `DEC` | 固定 done，不可变 |
| `research/` | 探索报告 + 现状审计 | `EXP`, `E` | 性能分析、bug 报告 |
| `issues/closed/` | 已关闭的缺陷/任务 | `BUG`, `TASK` | 已修复 |
| `changelogs/` | 进度总览 + 变更记录 | `PROGRESS`, `CHANGELOG` | 持续更新 |
| `reviews/` | 代码评审报告 | `REV` | （待产出） |
| `_templates/` | 文档模板 | 各类 | 新文档从此复制 |

---

## 文档索引

### Designs (active)

- [DSN-01 - SSCC Refactor Plan](designs/active/DSN-01-sscc-refactor-plan.md)
- [DSN-02 - Lock Retry Mechanism](designs/active/DSN-02-lock-retry-mechanism.md)
- [DSN-03 - CR Priority Optimization](designs/active/DSN-03-cr-priority-optimization.md)
- [DSN-04 - On-Chain Retry Limit](designs/active/DSN-04-onchain-retry-limit.md)
- [DSN-05 - HotKey Retry Design](designs/active/DSN-05-hotkey-retry-design.md)
- [DSN-06 - Lock Priority Coordination](designs/active/DSN-06-lock-priority-coordination.md)
- [DSN-07 - Force Simulation](designs/active/DSN-07-force-simulation.md)
- [DSN-08 - Retry Limit](designs/active/DSN-08-retry-limit.md)
- [DSN-09 - Active-Passive Retry Pool](designs/active/DSN-09-active-passive-retry-pool.md)
- [DSN-10 - Log Query Guide](designs/active/DSN-10-log-query-guide.md)
- [DSN-11 - Log Lifecycle](designs/active/DSN-11-log-lifecycle.md)
- [DSN-12 - Rollback v1 Passive Pool](designs/active/DSN-12-rollback-v1-passive-pool.md)
- [DSN-22 - RetryCommit 锁检查顺序修正](designs/active/DSN-22-retrycommit-lock-order-fix.md)
- [DSN-23 - PatchPool DAG 多 Patch 联合覆盖](designs/active/DSN-23-patchpool-dag-design.md)

### Designs (archived)

- [DSN-13 - CR HotKey Chaining Design](designs/archived/DSN-13-cr-hotkey-chaining-design.md)
- [DSN-14 - Sim DAG Chaining Design](designs/archived/DSN-14-sim-dag-chaining-design.md)
- [DSN-15 - Sim DAG Chaining Progress](designs/archived/DSN-15-sim-dag-chaining-progress.md)
- [DSN-16 - Lock Design](designs/archived/DSN-16-lock.md)

### Research

- [EXP-01 - stateLock Analysis](research/EXP-01-stateLock-analysis.md) ← 最新
- [EXP-02 - Bug Report](research/EXP-02-bug-report.md)
- [EXP-03 - Bug Fix Record](research/EXP-03-bug-fix-record.md)
- [EXP-04 - Retry Tx Chain Build SimTx](research/EXP-04-retry-tx-chain-build-simtx.md)
- [analyze_unfinished.py](research/analyze_unfinished.py) — 日志分析辅助脚本

### Decisions

- [DEC-01 - Priority-First Sim Block](decisions/DEC-01-priority-first-sim-block.md)

### Issues (closed)

- [BUG-04 - Retry Concurrent Re-enter](issues/closed/BUG-04-retry-concurrent-reenter.md)
- [BUG-08 - TempLock Leak After Recall Conflict](issues/closed/BUG-08-templock-leak-after-recall-conflict.md)

### Briefs

- [HANDOFF 2026-06-25](briefs/HANDOFF-2026-06-25.md)
- [HANDOFF 2026-06-28](briefs/HANDOFF-2026-06-28.md)
- [HANDOFF 2026-06-29](briefs/HANDOFF-2026-06-29.md)
- [HANDOFF 2026-06-30](briefs/HANDOFF-2026-06-30.md)

---

## 命名规则

```
{前缀}-{序号}-{slug}.md
```

- 前缀全大写：`BRF`, `REQ`, `DSN`, `DEC`, `EXP`, `BUG`, `FEAT`, `TASK`, `REV`
- 现有前缀：`A`, `B`, `Z`, `E`, `BUG`, `DEC`, `HANDOFF`, `FIX`

## 文档生命周期

```
需求/探索 → 设计(planned → active → archived)
                              ↓
                     决策记录(done, immutable)
                              ↓
                     任务/缺陷(open → closed)
                              ↓
                     评审报告(done)
```
