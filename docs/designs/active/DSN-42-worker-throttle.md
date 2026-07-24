# DSN-42 — 模拟 Worker 动态调速

## 1. 摘要

在 `Simulator` 中引入可插拔的调速策略层，根据每个区块的已提交交易数动态调节模拟 worker 的处理间隔，在不改动 worker 数量的前提下优化模拟/验证的 CPU 资源分配。

## 2. 动机

| Worker 数 | TPS | 超时 | 问题 |
|-----------|-----|------|------|
| 30 | 115 | 1848 | 验证 CPU 争用过大 |
| 3 | 186 | 63 | 最优 |
| 1 | 152 | 1192 unsim | 模拟跟不上 |

不同负载下最优 worker 工作频率不同，固定值无法自适应。

## 3. 设计

### 3.1 策略接口

```go
// ThrottleStrategy 模拟 worker 调速策略接口
type ThrottleStrategy interface {
    // WaitDuration 根据上一个区块的交易数，返回 worker 应等待的时间
    // committedTxCount: 上一个区块的交易数（SimTx + CRTx + 普通 tx）
    // 返回值：等待时长，0 表示不等待
    WaitDuration(committedTxCount int) time.Duration

    // OnBlockCommitted 通知策略区块已提交（策略可在此更新内部状态）
    OnBlockCommitted(block *types.Block)

    // Name 返回策略名称
    Name() string
}
```

### 3.2 内置策略

#### FixedDelayStrategy
- `delay time.Duration` — 固定等待时间
- 用途：基线对比，替代手动改 worker 数

#### ThresholdStrategy
- `minTxCount int` — 基准阈值
- `baseDelay time.Duration` — 最大等待时间
- 逻辑：
  - `txCount >= minTxCount` → 不等待
  - `txCount < minTxCount` → `delay = baseDelay * (1 - txCount/minTxCount)`
  - 线性递减：交易越少，等待越长

#### TrendStrategy
- `baseDelay time.Duration` — 最大等待时间
- `ratio float64` — 下降比率（默认 0.7）
- 内部维护 `prevTxCount int`
- 逻辑：
  - 首块 → 不等待
  - `txCount < prevTxCount * ratio` → `delay = baseDelay`（显著下降）
  - 否则 → `delay = 0`

#### MovingAverageStrategy
- `windowSize int` — 移动平均窗口（默认 5）
- `baseDelay time.Duration` — 最大等待时间
- `threshold float64` — 触发阈值（默认 0.7）
- 内部维护环形缓冲区 `txHistory []int`
- 逻辑：
  - 计算窗口内平均交易数 `avg`
  - `txCount < avg * threshold` → `delay = baseDelay * (1 - txCount/(avg*threshold))`
  - 否则 → `delay = 0`

### 3.3 Simulator 改动

```go
type Simulator struct {
    // ... 已有字段

    // 动态调速
    throttleStrategy ThrottleStrategy  // 当前策略（nil 表示关闭）
    currentWait      atomic.Value      // time.Duration，worker 循环读取
}
```

### 3.4 Worker 循环改动

```go
func (sim *Simulator) worker(id int) {
    defer sim.workerWg.Done()
    for {
        task, ok := sim.simulateTaskPQ.PopOrWait()
        if !ok { return }
        // 动态调速：每个 worker 独立读取当前等待时长
        if waitDur, ok := sim.currentWait.Load().(time.Duration); ok && waitDur > 0 {
            utils.SSCLogger().Debug().
                Int("workerId", id).
                Dur("waitDur", waitDur).
                Msg("throttle: worker waiting before simulation")
            time.Sleep(waitDur)
        }
        sim.processSimulationTask(task, id)
    }
}
```

### 3.5 OnBlockCommitted 集成

在 `sscService.BlockCommitted()` 中增加 `Simulator.OnBlockCommitted` 调用：

```go
func (s *sscService) BlockCommitted(block *types.Block) error {
    // ... 已有逻辑
    s.timerMgr.OnBlockCommitted(block.NumberU64())
    s.CommitteeMechanism.OnBlockCommitted(block)
    s.retryScheduler.OnBlockCommitted(block)
    s.Simulator.OnBlockCommitted(block)  // 新增
    // ...
}
```

`Simulator.OnBlockCommitted`：

```go
func (sim *Simulator) OnBlockCommitted(block *types.Block) {
    if sim.throttleStrategy == nil {
        return  // 关闭状态，不变
    }
    txCount := len(block.Transactions())
    sim.throttleStrategy.OnBlockCommitted(block)
    waitDur := sim.throttleStrategy.WaitDuration(txCount)
    sim.currentWait.Store(waitDur)
    if waitDur > 0 {
        utils.SSCLogger().Info().
            Uint64("block", block.NumberU64()).
            Int("txCount", txCount).
            Dur("waitDur", waitDur).
            Str("strategy", sim.throttleStrategy.Name()).
            Msg("[Throttle] worker throttling activated")
    }
}
```

### 3.6 NewSimulator 构造函数改动

新增可选参数 `throttleStrategy ThrottleStrategy`：

```go
func NewSimulator(
    // ... 已有参数
    throttleStrategy ThrottleStrategy,  // nil 表示关闭
) *Simulator {
    sim := &Simulator{...}
    sim.currentWait.Store(time.Duration(0))
    sim.throttleStrategy = throttleStrategy
    return sim
}
```

### 3.7 配置

在 `SSCConfig` 中增加：

```go
type SSCConfig struct {
    // ... 已有字段
    SimulationLimit int  // worker 数量（改为固定值，不再手动改）

    Throttle struct {
        Strategy   string  // "fixed", "threshold", "trend", "moving_avg", ""=关闭
        BaseDelay  Duration // 默认 10ms
        MinTxCount int     // threshold 策略基准值
        WindowSize int     // moving_avg 窗口大小（默认 5）
        TrendRatio float64 // trend 策略下降比率（默认 0.7）
    }
}
```

## 4. 部署流程

1. 默认 `strategy=""`（关闭）→ 行为不变
2. 设置 `strategy="threshold"`, `MinTxCount=300` → 区块交易 <300 时自动降速
3. 实验对比验证效果

## 5. 注意事项

- `PopOrWait()` 在队列空时阻塞，此时 worker 本来就在等，不会误判
- `atomic.Value` 读写无锁，OnBlockCommitted 是单 goroutine 调用，worker 是只读
- 等待是每个 worker 在 pop 到任务后独立执行的，N 个 worker 各自等 `waitDur`，不是串行
- 如果 waitDur 设得太大（如 100ms），总吞吐会降低，但验证端的 CPU 时间会增加
- 关闭策略时 `throttleStrategy == nil`，`currentWait` 始终为 0，无开销
