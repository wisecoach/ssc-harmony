---
id: BUG-10
title: Shard 3 Phase 0.5 仲裁耗时 45 秒，全部 passed SimTx 因超时被跳过
type: BUG
status: active
priority: P1
reporter: BOSS
module: ssc/verify.go (Phase 0.5 arbitration)
severity: high
created: 2026-07-21
refs: [DSN-41, BUG-09]
---

## 问题描述

DSN-41（worker pool）实施后，Shard 3 的 Phase 0.5 仲裁耗时 45 秒，导致所有 passed SimTx 因 deadline 超时被跳过（submitted=0, timeoutSkipped=557）。Worker pool 收不到任何任务。

## 实验数据（RATE=100）

### Shard 3 (port 9120) 仲裁耗时

| block | passed | failed | phase0.5 耗时 |
|:-----:|:------:|:------:|:-------------:|
| ~ | 36 | 8 | 19ms |
| ~ | 77 | 18 | 41ms |
| ~ | 89 | 35 | 96ms |
| ~ | 98 | 26 | 77ms |
| ~ | 96 | 51 | 162ms |
| ~ | 87 | 55 | 165ms |
| ~ | 79 | 70 | 216ms |
| ~ | 81 | 63 | **3,606ms** |
| ~ | 211 | 193 | **15,143ms** |
| ~ | 557 | 525 | **45,552ms** |

### 全部 timeout 跳过

```
Shard 3 (9120): submitted=0, timeoutSkipped=557, execFailed=0
Shard 3 (9120): totalSkipped=557
```

### 各 shard 最终区块

| Shard | 最终 block | 备注 |
|:-----:|:----------:|:------|
| 0 | 96 | 正常 |
| 1 | 60 | 仍在产块（172 SimTx, 24ms） |
| 2 | 95 | 正常 |
| 3 | 35 | 卡死（101 SimTx, 47s） |

### worker pool 日志

```
Shard 0 (9000): submitted=0, timeoutSkipped=37~143  ← Shard 0 也有不少超时
Shard 3 (9120): submitted=0, timeoutSkipped=557       ← 全部超时
```

## 复现条件

1. `EnableParallelBatch: true`
2. Shard 3 运行到后期（~35 blocks）
3. 大量 SimTx 堆积在 pending pool

## 根因分析

### Phase 0.5 仲裁中 `CheckLock` 遍历开销过大

仲裁串行遍历每个 SimTx 的 writeKeys，对每个 key 调用 `stateDB.CheckLock(key, txHash)`。`CheckLock` 内部使用 `sync.Map.Load()` 从 `globalLockedStates` 查找。

当全局锁数量随实验线性增长（到后期可能 2000+ 条目），每个 `CheckLock` 的 `Load` 虽然 O(1) 哈希查找，但在大量 key × 大量 SimTx 的情况下（557 passed + 525 failed = 1082 SimTx × 平均每个 N 个 write key），总调用次数爆炸。

### 仲裁必须在 worker pool 之前完成

当前设计：Phase 0.5 仲裁（串行）→ deadline 检查 → worker pool。仲裁太慢导致 deadline 耗尽，worker pool 无用。

## 修复方向

| 方案 | 做法 | 工作量 |
|:-----|:------|:-------|
| A: Phase 0.5 中 `CheckLock` 批量检查 | 一个 SimTx 的所有 key 只调一次 `CheckLock` | 小 |
| B: 减少全局锁检查频率 | 同块 SimTx 的 `committedWrites` map 已经覆盖了同块冲突，跨块 CheckLock 在大块时减少频率 | 中 |
| C: 仲裁与 worker 流水线化 | 仲裁完一批 SimTx 就立即投递给 worker，不等全部仲裁完 | 中 |
| D: 限制仲裁的 SimTx 总量 | 超量控制同时用于仲裁（仲裁处理 500 个后剩下的不仲裁，直接等下个块） | 小 |

## 未解决的根本问题

Worker pool 本身没有正确工作——所有任务因超时被跳过，`submitted=0`。这是仲裁耗时问题导致的**连带故障**，不是 worker pool 逻辑错误。

交易无法完成的根本原因仍然是**SimTx 堆积 → 大块 → 处理慢 → 进一步堆积**的正反馈循环。限制超量（500）没有生效，因为仲裁阶段没有限制。
