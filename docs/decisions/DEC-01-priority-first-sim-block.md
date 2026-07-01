# Priority 饥饿问题：FirstSimBlock 替代 SimulationNum

> **创建日期**：2026-06-25
> **状态**：🟢 已实现（2026-06-25）
> **涉及文件**：`ssc/api/types.go`、`ssc/retry_scheduler.go`、`ssc/simulator_leader.go`

---

## 问题背景

### 现状

Wound-Wait 锁竞争的优先级定义为：

```go
type Priority struct {
    SimulationNum int     // 重试次数越多越优先（最高优先级）
    Nonce         uint64  // Nonce 越小越优先
    OriginShardID uint32  // ShardID 越小越优先
}
```

`SimulationNum` 在每次 `CallForRetry` 时递增。理论上是让重试次数多的交易优先拿到锁，避免无限重试。

### 问题

实验数据显示反效果：**优先级军备竞赛**。

```
最高重试交易: SimulationNum=74（未 close）
次高重试交易: SimulationNum=71（未 close）
第三高重试交易: SimulationNum=63（未 close）
```

同一 HotKey 的所有交易都在以几乎相同的速度重试，`SimulationNum` 差距永远拉不开。谁也无法绝对压制其他人。结果 5 分钟内一笔交易都 close 不了。

**根因**：`SimulationNum` 在每次 `CallForRetry` 时对所有竞争交易同步增长，无法拉开差距。优先级应该**在交易出生时就固定**，不应随重试变化。

---

## 方案：FirstSimBlock 替代 SimulationNum

### 优先级新定义

```go
type Priority struct {
    FirstSimBlock uint64  // 首次模拟时的区块高度（越小越优先 ← 最高优先级）
    Nonce         uint64  // Nonce 越小越优先
    OriginShardID uint32  // ShardID 越小越优先
}
```

`Less()` 比较顺序：`FirstSimBlock <, Nonce <, OriginShardID <, TxHash <`。**区块高度越小的交易越早进入系统，优先级越高。** TxHash 作为最终 tiebreaker，确保所有交易有确定全序。

**关键**：`FirstSimBlock` 在交易第一次 `CallForRetry` 时由 **origin shard 的 leader** 设置，之后永不改变。优先级全程固定。

---

### 数据流

```
交易首次模拟（origin shard leader）:
  → LockConflict → CallForRetry(&RetryTx{FirstSimBlock: bc.CurrentHeader().NumberU64()})
  → retryScheduler 存到 retryPool

交易第二次重试:
  → 再次 LockConflict → CallForRetry(&RetryTx{...})
  → retryScheduler.CallForRetry 内部逻辑:
      if tx.FirstSimBlock == 0 {
          // 从 retryPool 已有的 RetryTx 继承
          if existing := rs.retryPool[tx.TxHash]; existing != nil && existing.FirstSimBlock > 0 {
              tx.FirstSimBlock = existing.FirstSimBlock
          } else {
              // 首次进入（不应发生，兜底）
              tx.FirstSimBlock = rs.bc.CurrentHeader().NumberU64()
          }
      }

RetryCommit（所有 shard）:
  → 从 retryPool 读 RetryTx → 取 FirstSimBlock → 构造 Priority
  → TryLockWithPriority 中比较 (FirstSimBlock, Nonce, OriginShardID)
```

### 预期效果

- 最早进入系统的交易优先级最高 → 一定能拿到锁 → 完成
- 它完成后，次早的变成最高 → 完成
- **不会军备竞赛**：重试不涨优先级，排在你前面的永远在你前面

---

## 文件变更

| 文件 | 变更 | 说明 |
|------|------|------|
| `ssc/api/types.go` | `Priority` 结构体：`SimulationNum` → `FirstSimBlock`；更新 `Less()` | 核心变更 |
| `ssc/api/types.go` | `RetryTx` 新增 `FirstSimBlock uint64` | 跨 shard 传递优先级信息 |
| `ssc/retry_scheduler.go` | `CallForRetry` 内首次进入时设 FirstSimBlock；`RetryCommit` 构造 Priority 用 FirstSimBlock | 设值 + 使用 |
| `ssc/simulator_leader.go` | 3 处 `CallForRetry` 加 `FirstSimBlock: sim.bc.CurrentHeader().NumberU64()` | 调用点传递 |
| `ssc/verify.go` | 1 处 `CallForRetry` 无需改（`CallForRetry` 内部继承已有值） | 调用点不修改 |

> `verify.go` 的 `CallForRetry` 不需要传 `FirstSimBlock`，因为该路径发生在 origin shard 已调过 `CallForRetry` 之后，`retryPool` 中已有 `FirstSimBlock` 值，`CallForRetry` 内部自动继承。
