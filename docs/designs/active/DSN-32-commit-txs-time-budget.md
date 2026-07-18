# DSN-32: CommitTxs 时间预算

## 1. 动机

### 1.1 问题

当前 `CommitTransactions` 仅用 `maxTxns`（单块 500 笔）控制交易数量，没有时间控制。Shard 3 上活跃块（commitTxs > 500ms）的 commitTxs 耗时分布：

| Shard | 活跃块数 | avg | P50 | P90 | max |
|:----:|:--------:|:---:|:---:|:---:|:---:|
| 3 | 26 | 1,964ms | 2,197ms | 2,640ms | 2,869ms |
| 1 | 45 | 742ms | 729ms | 884ms | 1,154ms |
| 0 | 44 | 663ms | 651ms | 865ms | 916ms |
| 2 | 6 | 570ms | 589ms | 592ms | 592ms |

Shard 3 最差块 commitTxs 达 **2.87s**，是其他 shard 的 **3-4x**。这导致：

1. **级联延迟** —— Shard 3 块间隔拉长后，跨分片交易在 BLS 共识阶段需要等待最慢 shard
2. **块间隔不稳定** —— 空闲期 ~1.5s，活跃期膨胀到 ~3.4s
3. **吞吐量受短板约束** —— 整个系统以降速匹配最差 shard

### 1.2 诊断结论

SimTx 占块内处理时间的 **86.5%**，是绝对瓶颈。笔级耗时：

| 类型 | avg | P50 | P90 | P99 |
|:----|:---:|:---:|:---:|:---:|
| SimTx | 6.17ms | 4.09ms | 12.65ms | 35.66ms |
| CRTx | 0.65ms | 0.51ms | 0.97ms | 2.45ms |

VerifySimulation 内部（后 async SendCommitVote 时代）：

| 子阶段 | 占比 | avg |
|:------|:----:|:---:|
| execVerify | ~48% | 2.69ms |
| lockCheck | ~46% | 2.56ms |
| 其余阶段 | ~6% | <0.2ms |

两个瓶颈并重，没有单个超线性耗时的内部阶段。因此优化思路不是加速单笔 SimTx，而是**控制每块的处理量，让尾块不再拖垮系统**。

## 2. 设计方案

### 2.1 核心思想

在 `CommitSSCTransactions` 的 for 循环中加入**时间预算**检查。达到预算后停止处理新交易，剩余交易留在 tx pool 中，由下个块的自然出块流程处理。

```
每块 ProposeNewBlock ─→ CommitTransactions
                            │
  CRTx 循环（优先，不受限）   │
                            │
  SSC SimTx 循环 ←─── 带 1000ms 时间预算
                            │
  NormalTx 循环（轻量预算）
```

**关键约束**：
- CRTx 优先级最高，**不设时间预算**（它们完成 SimTx 生命周期，必须尽快处理）
- SimTx（SSC）设 1000ms 预算，达到后 break，未处理的 SimTx 自动浮到下个块
- NormalTx 设 200ms 轻量预算（普通交易占比 <0.1%，影响极小）
- 时间预算与现有 `maxTxns`（500）、`gasPool` 是**三条件 OR 关系**，任一条件达到即停止

### 2.2 改动范围

**文件**：`node/worker/worker.go`

**`CommitSSCTransactions` 签名变更**：

```go
func (w *Worker) CommitSSCTransactions(
    txs *types.TransactionsByPriceAndNonce,
    coinbase common.Address,
    maxTxns int,
    maxTime time.Duration,  // ← 新增
)
```

当 `maxTime > 0` 时，进入循环前算 deadline，每次迭代结束后检查：

```go
maxTime := 1000 * time.Millisecond  // 实验参数
if maxTime > 0 && time.Now().After(deadline) {
    utils.SSCLogger().Info().
        Int("processed", count-1).
        Dur("budget", maxTime).
        Dur("elapsed", time.Since(startTime)).
        Uint64("blockNum", w.current.header.NumberU64()).
        Msg("CommitSSCTransactions: reached time budget, stopping")
    break
}
```

**`CommitTransactions` 调用处**（`worker.go:289`）：

```go
// CRTx — 不设时间预算
w.CommitSSCTransactions(crTxns, coinbase, remaining, 0)

// SimTx — 1000ms 预算
w.CommitSSCTransactions(sscTxns, coinbase, remaining, 1*time.Second)

// NormalTx — 200ms 轻量预算
w.CommitSortedTransactions(normalTxns, coinbase, remaining, 200*time.Millisecond)
```

### 2.3 安全论证

| 检查项 | 结论 |
|:------|:-----|
| **原子性** | ✅ 每次 `commitTransaction()` 是完整原子操作。break 只发生在迭代之间，不会截断正在执行的交易 |
| **状态一致性** | ✅ 未处理的 SimTx 留在 tx pool，下个块 `pendingPoolTxs` 会重新捡起 |
| **CRTx 完整性** | ✅ CRTx 先于 SimTx 执行，不受时间预算影响 |
| **重复执行** | ✅ SimTx 如果被截断，下个块重新验证（VerifySimulation 幂等：相同 callStates + 相同 stateDB → 相同结果） |
| **Gas 双重限制** | ✅ maxTxns + gasPool + timeBudget 是 OR 关系，谁先到谁触发 break |

### 2.4 日志变更

新增 Info 级日志（用于实验统计）：

```
CommitSSCTransactions: reached time budget, stopping
```

字段：`processed`、`budget`、`elapsed`、`blockNum`。

## 3. 预期收益

基于 Shard 3 数据的推算（假设预算 1000ms）：

| 指标 | 现状 | 预期（1000ms 预算） | 变化 |
|:----|:----:|:------------------:|:----:|
| Shard 3 活跃块 P90 commitTxs | 2,640ms | ≤1,000ms | -62% |
| Shard 3 尾块 max commitTxs | 2,869ms | ≤1,000ms | -65% |
| 每块 SimTx 量 | ~200 | ~160 | -20% |
| 总块数 | 统计不变 | 增加 ~25% | +25% BLS 轮次 |

**净收益**：虽然块数增加导致 BLS 签名轮次增加 ~25%，但**最差块的 commitTxs 从 2.87s 降到 1s**，消除了级联延迟。系统吞吐量由最慢 shard 的块间隔决定，1s 块+1.5s BLS=2.5s 间隔，优于现状的 3.4s+。

## 4. 实验验证

### 4.1 验证指标

| 指标 | 采集方式 | 目标值 |
|:----|:---------|:------|
| Shard 3 活跃块 P90 commitTxs | ProposeNewBlock breakdown | ≤1,200ms |
| Shard 3 max commitTxs | ProposeNewBlock breakdown | ≤1,100ms |
| 总吞吐量 TPS | result.txt | 不下降或小幅提升 |
| 未截断 SimTx 的存在 | 日志 `reached time budget` | >0 说明预算生效 |

### 4.2 验证步骤

1. 改 `worker.go` 加时间预算
2. 编译：`touch ssc/*.go && bash scripts/go_executable_build.sh -S`
3. 同步到 199：`source sync_code.sh && uploadCode zjnu@10.7.95.199`
4. 跑 `test_single`（4shard × 4validator, delay=10, rate=100）
5. 验证：
   - `ProposeNewBlock breakdown` 中 commitTxs > 1000ms 的块数下降
   - `SSC tx commit timing` 的 n 是否分散到更多块
   - result.txt 的 TPS 是否稳定

### 4.3 预算值调试策略

| 预算值 | 适用场景 |
|:------|:---------|
| 1000ms | 初始值（Shard 3 现状 1.96s 的一半） |
| 800ms | 激进（匹配 Shard 0/1 正常水平） |
| 1200ms | 保守（降级幅度小） |

如果`reached time budget` 日志量占 SSC 块的 >50%，说明预算太紧，SimTx 频繁跨块 → TPS 下降。调整到 1200ms 再试。

## 5. 备选方案

### 方案 A：时间预算（选择）

- **优点**：改动最小（~10 行），效果可预测，风险低
- **缺点**：不解决根本原因（Shard 3 为什么慢），只是症状缓解

### 方案 B：块内 SimTx 并行验证

- **优点**：理论吞吐量提升 2-4x
- **缺点**：需处理 stateDB 写冲突 + goroutine 同步，改动量 ~200 行，风险高

### 方案 C：分片键均衡

- **优点**：根治 Shard 3 过载问题
- **缺点**：需要改实验配置（`build_keys.go`），实验设计层面变动，非协议层可解决

## 6. 关键决策

| 决策 | 选择 | 理由 |
|:----|:----|:------|
| 预算值 | 1000ms 起始 | 基于 Shard 3 P50=2.2s 取约一半，保守可接受 |
| CRTx 是否受限制 | 否 | CRTx 仅 0.65ms 笔，优先级最高 |
| 检查位置 | 每次迭代后 | 不影响原子性，精度足够 |
| NormalTx 预算 | 200ms | NormalTx 占比 <0.1%，但防止极端情况 |
| 预算与 maxTxns 关系 | OR | 任一条件触发即停止，最严格获胜 |

## 7. 风险与边界

| 风险 | 影响 | 缓解 |
|:----|:-----|:------|
| **TPS 下降** | 块数增加 → BLS 轮次增加 25%，可能抵消 tail latency 收益 | 实验验证，fallback 到 1200ms |
| **重试延迟增加** | SimTx 跨块后重试延迟增加 ~1.5s | 已在 retry 路径内，无额外处理 |
| **NormalTx 预算过紧** | 200ms 可能不够 | NormalTx 占比极低，观察后再调 |
| **不影响根本原因** | Shard 3 的 execVerify=5.22ms 问题仍然存在 | 本 DSN 是 Step 1，DSN-33 可针对 lockCheck 优化 |
