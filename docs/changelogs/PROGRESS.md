# 进度总览

## 设计文档 (Designs)

### Active

| 编号 | 标题 | 状态 |
|:----|------|:----:|
| DSN-01 | SSCC Refactor Plan | active |
| DSN-02 | Lock Retry Mechanism | active |
| DSN-03 | CR Priority Optimization | active |
| DSN-04 | On-Chain Retry Limit | active |
| DSN-05 | HotKey Retry Design | active |
| DSN-06 | Lock Priority Coordination | active |
| DSN-07 | Force Simulation | active |
| DSN-08 | Retry Limit | active |
| DSN-09 | Active-Passive Retry Pool | active |
| DSN-10 | Log Query Guide | active |
| DSN-11 | Log Lifecycle | active |
| DSN-12 | Rollback v1 Passive Pool | active |
| DSN-22 | RetryCommit 锁检查顺序修正 | active |
| DSN-23 | PatchPool DAG 多 Patch 联合覆盖 | active |
| DSN-24 | 统一锁检查 — TLV+SLM+Patch 三层仲裁 | active |
| DSN-25 | RetryCommit StateDB 缓存 — OnBlockCommitted | active |
| DSN-26 | PatchPool DAG 扩展到 TLV Phase 1 | active |

### Archived

| 编号 | 标题 | 状态 |
|:----|------|:----:|
| DSN-13 | CR HotKey Chaining Design | archived |
| DSN-14 | Sim DAG Chaining Design | archived |
| DSN-15 | Sim DAG Chaining Progress | archived |
| DSN-16 | Lock Design | archived |

## 探索报告 (Research)

| 编号 | 标题 | 状态 |
|:----|------|:----:|
| EXP-01 | stateLock Analysis | done |
| EXP-02 | Bug Report (initial) | done |
| EXP-03 | Bug Fix Record | done |
| EXP-04 | Retry Tx Chain Build SimTx | done |

## 决策记录 (Decisions)

| 编号 | 标题 | 状态 |
|:----|------|:----:|
| DEC-01 | Priority-First Sim Block | done |

## 简报 (Briefs)

| 编号 | 标题 | 状态 |
|:----|------|:----:|
| BRF-01 | HANDOFF 2026-06-25 | done |
| BRF-02 | HANDOFF 2026-06-28 | done |
| BRF-03 | HANDOFF 2026-06-29 | done |
| BRF-04 | HANDOFF 2026-06-30 | done |

## 缺陷 (Issues)

### Closed

| 编号 | 标题 | 状态 |
|:----|------|:----:|
| BUG-04 | Retry Concurrent Re-enter | closed |
| BUG-08 | TempLock Leak After Recall Conflict | closed |

## 当前实验 (2026-07-01)

- 定位到 `closeTx` max=1.5s 瓶颈在 `stateLock` 锁争抢（closeTransaction 锁内持锁 ~960ms）
- 下一轮：缩小 closeTransaction 锁范围，去掉 CtxCancel 和日志
