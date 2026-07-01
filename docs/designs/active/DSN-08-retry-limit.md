# [A08] 重试次数限制（Retry Limit Control）

> **版本**：v1（2026-06-28）
> **解决的问题**：交易无限重试导致时延大幅上涨（部分 tx 重试 16 次，耗时 40s）

---

## 1. 问题背景

实验中观察到部分交易重试 3-16 次，耗时 30-40s，远超 P50 的 9s：

| 重试次数 | txs 占比 | P50 时延 | P90 时延 |
|:--------:|:--------:|:--------:|:--------:|
| 0 | 92.7% | 6s | 14s |
| 1-2 | 3.1% | 14s | 15s |
| 3-5 | 2.6% | 20s | 28s |
| 6+ | 1.6% | 28s+ | — |

这些慢 tx 的耗时几乎全部花在**重试等待锁释放上**（各阶段分解：入池→SimTx 耗时 30-40s，SimTx→close 仅 3-4s）。限制重试次数后，超限的交易直接放弃，避免"尾延迟"拖累整体。

## 2. 配置项

```go
type TimeoutConfig struct {
    // ... 原有字段 ...

    MaxOnChainRetries uint64 // 链上验证阶段的重试次数上限
                             // 0 = 不重试（首次失败即放弃）
                             // N = 最多重试 N 次

    MaxRetriesTotal   uint64 // 总重试次数上限（链下+链上）
                             // 0 = 不限制
                             // N = SimulationNum > N 时放弃
}
```

### 两个维度的区别

| 维度 | MaxOnChainRetries | MaxRetriesTotal |
|------|:-----------------:|:---------------:|
| 控制范围 | 链上 verify 阶段 | 全链路（链下+链上） |
| 检查位置 | `verify.go:checkRetryLimitExceeded` | `retry_scheduler.go:AddToRetry` |
| 超限后果 | 发送回滚投票 | 拦截入 pool |
| 适用场景 | 防止链上验证循环重试 | 防止无限重试拖尾 |

## 3. 实现方案

### 3.1 链上限（MaxOnChainRetries）— 已有

`verify.go:531-536` 每次链上验证时检查：

```go
func checkRetryLimitExceeded(simulation *CXTSimulation) bool {
    return simulation.SimulationNum >= lockedSimNum + MaxOnChainRetries
}
```

当 `MaxOnChainRetries=0`：`SimulationNum >= lockedSimNum + 0`。首次 retry（simNum=0）即超限 → 不重试。

超限后调用 `sendRollbackVoteForRetry`，各 related shard 回滚投票 → 关闭交易。

### 3.2 总上限（MaxRetriesTotal）— 新增

两层兜底：

**第一层：`AddToRetry` 入口拦截**（链下，未上链的交易）

```go
if rs.maxRetriesTotal > 0 && tx.SimulationNum > rs.maxRetriesTotal {
    // 超限，不加入 retryPool
    rs.state.RetryFailCount()
    return
}
```

已上链的交易不受这层拦截——它们需要通过链上验证流程来触发回滚。

**第二层：`verify.go:checkRetryLimitExceeded` 链上回滚**

```go
chainExceeded := simulation.SimulationNum >= lockedSimNum + MaxOnChainRetries
totalExceeded := simulation.SimulationNum > MaxRetriesTotal
return chainExceeded || totalExceeded
```

SimTx 上链后，verify.go 会同时检查两个上限，任一超限即发送回滚投票。

### 3.3 超限后的处理逻辑

**方案选择**（2026-06-28 决策）：采用方案 1，方案 2 作为备选。

#### 方案 1 ✅ 已实现：等待 verify.go 兜底

上链后的交易即使链下超限，也不在 AddToRetry 拦截：

```
AddToRetry 发现 SimulationNum > MaxRetriesTotal
  ├── 未上链 → 直接丢弃（不入 retryPool）
  └── 已上链 → 放行（不拦截）
                  ↓
          模拟完成 → SimTx 上链
                  ↓
          verify.go 检查两个上限
            chainExceeded || totalExceeded
                  ↓
          sendRollbackVoteForRetry → CR 回滚
```

优点：无需新增接口，逻辑统一
缺点：上链后的超限交易需要多走一轮完整的"模拟→上链→验证→回滚"，效率较低

#### 方案 2 🔜 备选：AddToRetry 直接触发回滚

在 `RetrySchedulerStateAccessor` 中加入回滚触发回调：

```go
type RetrySchedulerStateAccessor struct {
    // ...
    SendRollbackVote func(txHash common.Hash, simNum int, epochs []Epoch, originShardId uint32)
}
```

AddToRetry 超限时直接调用，立即发起回滚投票：

```
AddToRetry 超限 + 已上链
  → 通知 Member 签署 Rollback 的 SSCVote
  → 发送给 OriginLeader
  → OriginLeader 构建回滚 CRTx → 提交链上
```

优点：不需要等一轮完整的"模拟→上链→验证"
缺点：需要在 Accessor 中暴露回滚接口，增加模块耦合

### 3.4 判断条件

| 条件 | 公式 | 说明 |
|:-----|:-----|:------|
| 总上限 | `SimulationNum > MaxRetriesTotal` | SimulationNum 从 0 开始，第 N 次重试的 SimulationNum = N-1 |
| 链上限 | `SimulationNum ≥ lockedSimNum + MaxOnChainRetries` | lockedSimNum = 首次上链时的 SimulationNum |

## 4. 配置建议

| 参数 | 值 | 理由 |
|:-----|:--:|:-----|
| `MaxOnChainRetries` | 5 | 允许链上验证阶段最多重试 5 次 |
| `MaxRetriesTotal` | 10 | 总重试上限 10 次，覆盖绝大部分场景，拦截极端拖尾 |

当前实验数据：97.5% 的交易在 5 次重试内完成，99.8% 在 10 次内。`MaxRetriesTotal=10` 仅影响最极端的 ~0.2% 交易。

## 5. 文件变更清单

| 文件 | 变更 |
|------|------|
| `ssc/api/types.go` | `TimeoutConfig` 加 `MaxRetriesTotal` 字段 |
| `ssc/retry_scheduler.go` | `retryScheduler` 加 `maxRetriesTotal` 字段；`AddToRetry` 加超限检查 |
| `ssc/impl.go` | 构造 retryScheduler 后传入 `MaxRetriesTotal` |
| `cmd/build_keys/build_keys.go` | 设置 `MaxOnChainRetries: 5, MaxRetriesTotal: 10` |
