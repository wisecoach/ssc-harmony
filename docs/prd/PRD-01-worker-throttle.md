# PRD-01 — 模拟 Worker 动态调速

## 1. 背景

实验数据表明，固定 worker 数量下，模拟服务和验证服务竞争 CPU 资源：

| Worker 数 | TPS | 超时总数 | 问题 |
|-----------|-----|---------|------|
| 30 | 115 | 1848 | 验证 CPU 争用过大 |
| 8 | 135 | 457 | 改善 |
| 3 | 186 | 63 | 最佳 |
| 1 | 152 | 25+1192 unsim | 模拟跟不上 |

不同负载下最优 worker 数不同，固定值无法自适应。需要一个**动态调速机制**，根据系统负载实时调整 worker 的工作频率。

## 2. 目标

- 在 `Simulator` 中增加动态调速层
- 不改变 `numWorkers`（固定 8），通过**调节 worker 的等待间隔**控制模拟速率
- 支持多种策略模式，可配置切换
- 反馈信号：**上一个区块的已提交交易数**（`onBlockCommitted` 时获知）

## 3. 策略接口

```go
// ThrottleStrategy 模拟 worker 调速策略接口
type ThrottleStrategy interface {
    // WaitDuration 根据上一个区块的交易数，返回本次 worker 应等待的时间
    // committedTxCount: 上一个区块已提交的交易数
    // 返回值: 等待时长（0 表示不等待）
    WaitDuration(committedTxCount int) time.Duration

    // Name 返回策略名称（用于日志/配置）
    Name() string
}
```

## 4. 内置策略

### 4.1 FixedDelayStrategy（固定延迟）
- 配置参数：`delay time.Duration`
- 所有 worker 每次 pop 任务前固定等待 `delay`
- 用途：基线对比，替代手动改 worker 数

### 4.2 ThresholdStrategy（阈值策略）
- 配置参数：`minTxCount int`（基准阈值）
- 逻辑：`committedTxCount < minTxCount` → 降速（线性增加延迟）
- 线性公式：`delay = baseDelay * (1 - committedTxCount/minTxCount)`
- 当 `committedTxCount >= minTxCount` → 不等待

### 4.3 TrendStrategy（趋势策略）
- 不需要基准值
- 比较**当前块**与**上一块**的交易量变化
- 下降趋势 → 降速，上升/稳定 → 不等待
- 公式：`if current < prev * 0.7: wait = baseDelay; else: wait = 0`
- 需要保存 `prevBlockTxCount`

### 4.4 MovingAverageStrategy（移动平均策略）
- 维护最后 N 个区块的移动平均交易数
- 当前块交易数 < 移动平均 * 阈值 → 降速
- 更平滑，避免单块波动误判

## 5. 配置

在 `SSCConfig` 中增加：

```go
type SSCConfig struct {
    // ... 已有字段

    // ThrottleConfig 模拟 worker 调速配置
    ThrottleConfig struct {
        Strategy  string // "fixed", "threshold", "trend", "moving_avg"
        BaseDelay Duration // 基础等待时长（默认 10ms）
        MinTxCount int    // threshold 策略的基准值
        WindowSize int    // moving_avg 策略的窗口大小（默认 5）
        TrendRatio float64 // trend 策略的下降比率（默认 0.7）
    }
}
```

默认值：`strategy=""`（关闭调速，保持现有行为）。

## 6. 集成点

### 6.1 OnBlockCommitted 回调

`Simulator` 已有 `OnBlockCommitted` 方法（或类似）。在该回调中：

1. 获取刚提交的区块交易数
2. 传入策略计算下一次 wait duration
3. 更新 worker 循环中使用的 `atomic.Value` 或 channel

### 6.2 Worker 循环改动

当前 worker 循环：

```go
for {
    task, ok := sim.simulateTaskPQ.PopOrWait()
    if !ok { return }
    sim.processSimulationTask(task, id)
}
```

改为：

```go
for {
    task, ok := sim.simulateTaskPQ.PopOrWait()
    if !ok { return }
    // 动态等待
    waitDur := sim.currentWait.Load().(time.Duration)
    if waitDur > 0 {
        time.Sleep(waitDur)
    }
    sim.processSimulationTask(task, id)
}
```

`currentWait` 是 `atomic.Value`，由 `OnBlockCommitted` 中的策略计算结果更新。

## 7. 非目标

- 不改 worker 数量（固定 8）
- 不改 `simulateSemaphore`（信号量保持 8）
- 不引入外部监控指标（只依赖 onBlockCommitted 的交易数）
- 不做分布式协调（每个节点独立调速）

## 8. 验收标准

1. 默认配置（strategy=""）下行为不变
2. threshold 策略下，低交易量块自动降速
3. 降速后下一个块交易量上升，自动恢复正常速度
4. 日志清晰显示每次调速的原因和参数
