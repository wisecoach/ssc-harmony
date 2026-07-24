# RSH-04: 性能埋点架构 — 采样 + 分级 + 降损

> **版本**：v2（2026-07-23）  
> **新增 v2 关键修改**：  
> - Aggregator 添加 **窗口重置**（periodic reset）  
> - 新增 **总耗时（Total）** 为核心指标  
> - 新增 **异常数据分类处理**（启动预热、burst、窗口外）  
> - 明确 **Total 作为瓶颈排序的第一指标**  

---

## 1. 问题

Debug 级别全量埋点有三个层面的损耗：

| 损耗层面 | 来源 | 度量 |
|:---------|:------|:-----|
| **① time.Now()** | 每个埋点至少 1 次 | ~30-50ns/次 |
| **② 日志格式化 + 写文件** | zerolog 写 JSON+flush | ~1-5μs/次 |
| **③ 数据统计（脚本）** | 事后 grep + JSON 解析日志 | 实验后一次性，可接受 |

**组合效应**：高频路径下 ② 的写文件开销会挤占 CPU。同时，**纯事后分析无法捕获瞬态瓶颈**（如某块突然慢 10x，但被平均掩盖）。

---

## 2. 核心指标设计

### 2.1 必须记录

| 指标 | 含义 | 瓶颈判定作用 |
|:-----|:------|:-------------|
| **Total** | 窗口内该阶段的总耗时 = Σ dur | **第一优先**：反映真实 CPU 消耗总量 |
| **Count** | 窗口内被调次数 | 辅助判断频繁度 |
| **Avg** | Total / Count | 平均每次的耗时 |
| **Max** | 窗口内单次最大耗时 | 毛刺检测 |

### 2.2 为什么 Total 是第一指标

```
例：两个阶段的统计数据

phase A: cnt=1000, avg=50μs, total=50ms
phase B: cnt=10,   avg=5ms,  total=50ms

只看 avg: B 是 100× 比 A"重"
只看 total: 两者消耗一样
```

**p50/p99 只看单次分布，不看全局消耗。** Total 真实反映该阶段每秒/每窗口消耗了多少 CPU 时间。优化优先级应该按 Total 排序，不是按 avg。

### 2.3 异常数据处理

| 异常类型 | 描述 | 处理方式 |
|:---------|:------|:---------|
| **启动预热** | 节点刚启动，状态缓存未就绪 | 前 10 秒数据单独标记 `warmup=true`，不混入常规统计 |
| **Burst 毛刺** | 单次耗时 >> 平均值（如 100ms vs avg 1ms） | 保留在 Total 中（反映真实消耗），但统计时标注 `cntBurst` |
| **窗口外暂停** | GC Stop-the-World 导致阶段耗时异常 | 无法从内部区分，但 Total 已经包含这段被 pause 的耗时——**它仍然反映了真实经过时间**，保留 |
| **零值/负值** | timer 回绕或错误 | 丢弃，不计入 Count |

**原则：不丢数据，但分窗口。** 每个窗口独立统计，异常值留在窗口里。

---

## 3. 窗口设计（核心）

### 3.1 两个窗口并行

```
时间轴
├─── 窗口 1 (10s) ───┤─── 窗口 2 (10s) ───┤─── 窗口 3 (10s) ───┤
                      ↑
                 窗口 1 结束时:
                 1) 输出汇总日志
                 2) reset 窗口 1 的聚合计数
                 3) 窗口 2 仍在接收新事件
```

**为什么需要窗口 reset：**

| 不 reset | 有 reset |
|:---------|:---------|
| avg 被早期数据稀释，不能反映近期趋势 | 每个窗口独立，可做时序对比 |
| retryPool 从 0 → 5000 的完整演化不可见 | 能看到 pool 增长过程的变化 |
| 10 小时实验只有一个快照 | 每 10 秒一个快照，~3600 组数据 |
| 如果有 GC pause 污染了某个窗口，其他窗口也被污染 | GC pause 只影响单个窗口 |

### 3.2 实现

```go
type PerfWindow struct {
    mu       sync.Mutex
    startAt  time.Time
    total    time.Duration  // Σ dur
    count    int
    max      time.Duration
    burstCnt int            // 超过阈值 thresh 的调用次数
}

type PerfAggregator struct {
    mu        sync.Mutex
    active    map[perfKey]*PerfWindow   // 当前窗口
    config    *PerfConfig
}

func (pa *PerfAggregator) Record(cat, funcName, phase string, dur time.Duration) {
    key := perfKey{cat, funcName, phase}
    
    pa.mu.Lock()
    w, ok := pa.active[key]
    if !ok {
        w = &PerfWindow{startAt: time.Now()}
        pa.active[key] = w
    }
    pa.mu.Unlock()

    w.mu.Lock()
    w.total += dur
    w.count++
    if dur > w.max {
        w.max = dur
    }
    if dur > pa.config.BurstThreshold {
        w.burstCnt++
    }
    w.mu.Unlock()
}
```

### 3.3 窗口切换

```go
// PeriodicFlush 定时调（每 PeriodicInterval 秒，在 OnBlockCommitted 或后台 goroutine 中）
func (pa *PerfAggregator) PeriodicFlush(now time.Time) {
    pa.mu.Lock()
    oldActive := pa.active
    pa.active = make(map[perfKey]*PerfWindow)  // 新窗口
    pa.mu.Unlock()

    // 输出旧窗口
    for key, w := range oldActive {
        w.mu.Lock()
        avg := time.Duration(0)
        if w.count > 0 {
            avg = time.Duration(int64(w.total) / int64(w.count))
        }
        utils.SSCLogger().Info().
            Str("cat", key.cat).
            Str("func", key.funcName).
            Str("phase", key.phase).
            Dur("total", w.total).          // 窗口内总耗时 ← 第一指标
            Int("cnt", w.count).
            Dur("avg", avg).
            Dur("max", w.max).
            Int("burstCnt", w.burstCnt).
            Msg("[perf] perf window")
        w.mu.Unlock()
    }
}
```

---

## 4. 最终异常/边界处理

| 场景 | 行为 | 理由 |
|:-----|:------|:------|
| `count == 0` | 不输出该 key 的行 | 节省日志空间 |
| `total == 0` | 不输出（1ns 都不记录） | CPU TSC 精度问题 |
| 窗口被 GC STW 打断 | max 会很高，窗口整体 total 偏高 | **保留**，这反映真实经过时间 |
| 窗口内没有任何事件 | 不输出 | 无信息输出无意义 |
| 同一 phase 跨窗口 | 各自独立统计 | 每个窗口都 reset |
| 配置变更（PerfConfig 切换）| 立即重置所有活动窗口 | 新旧模式不混 |

---

## 5. 模式矩阵

| Mode | Aggregator（常开） | RingBuf（按需） | 适用场景 |
|:-----|:------------------|:----------------|:---------|
| **off** | ❌ 不记录 | ❌ | 生产运行，零损耗 |
| **aggregated** | ✅ 每 T 秒输出窗口汇总 | ❌ | 日常监控，定位哪个 phase 最耗时 |
| **sampled** | ✅ | ✅ 采样率 1/N | 需要精确分布但不想全量 |
| **full** | ✅ | ✅ 全量 | 调试具体问题 |

**注意：** Aggregator 在 `full`/`sampled` 模式下也要开——它提供 **Total** 指标，RingBuf 提供 p50/p99 分布，两者互补。

---

## 6. OnBlockCommitted 作为统一锚点

### 6.1 统一接口

所有需要做性能统计的核心模块，实现统一接口：

```go
type OnBlockCommittedHandler interface {
    OnBlockCommitted(block *types.Block)
}
```

### 6.2 当前已有的 OnBlockCommitted

| 模块 | 文件 | 签名 | 备注 |
|:-----|:-----|:------|:------|
| `sscService.BlockCommitted` | impl.go:374 | `(block *types.Block) error` | 顶层调度者 |
| `retryScheduler.OnBlockCommitted` | retry_scheduler.go:491 | `(block *types.Block)` | ✅ 已有 |
| `CommitteeMechanism.OnBlockCommitted` | committee.go:430 | `(block *types.Block) error` | ✅ 已有 |
| `CXTTimerManager.OnBlockCommitted` | cxt_timer.go:120 | `(blockNum uint64)` | ✅ 已有（参数不同） |
| `TempLockView.OnBlockCommitted` | temp_lock_view.go:279 | `(block *types.Block) []api.LockKey` | ✅ 已有（内部调） |

### 6.3 新增 OnBlockCommitted

| 模块 | 文件 | 职责 |
|:-----|:-----|:------|
| **PerfAggregator** | `ssc/perf/aggregator.go` | **核心**：Flush + Reset 所有窗口 |
| **Verifier** | `verify.go` | VerifySimulation 的统计（验证批次/交易数/总耗时） |
| **Simulator (Leader)** | `simulator_leader.go` | CommitTransactions/StartSimulate 统计 |
| **Simulator (Member)** | `simulator_member.go` | startReSimulation/aggregateSSCCallRequest 统计 |
| **Committer** | `committer.go` | CR 提交/回滚统计 |
| **StateLockManager** | `state_lock_impl.go` | 锁状态的统计数据快照 |
| **stateLocker** | `state_locker.go` | 锁池大小/竞争数统计 |
| **TxSubmitter** | `tx_submitter.go` | 提交量统计 |

### 6.4 BlockCommitted 调度流程（拟）

```go
// impl.go:374
func (s *sscService) BlockCommitted(block *types.Block) error {
    t0 := time.Now()

    // 1. 核心模块的 OnBlockCommitted（按序）
    s.timerMgr.OnBlockCommitted(block.NumberU64())
    s.CommitteeMechanism.OnBlockCommitted(block)
    s.retryScheduler.OnBlockCommitted(block)

    // 2. PerfAggregator Flush + Reset ← 所有模块的埋点已在本块中写入
    s.perfAggregator.OnBlockCommitted(block)

    // 3. 日志输出 BlockCommitted 分解耗时（已有）
}
```

**为什么 PerfAggregator 放在最后：** 它 flush 的是从上一个区块提交到当前区块提交之间的所有记录。放在最后确保所有模块的 `Record()` 调用已完成。

---

## 7. 实施路径

```
Phase 1: 实现 PerfWindow + PerfAggregator（ssc/perf/ 目录）
         - 结构体：PerfWindow（total/count/max/burstCnt）
         - Record(cat, funcName, phase, dur)
         - OnBlockCommitted → Flush + Reset 所有窗口
         - 启动预热：前 10 秒的 record 标记 warmup 但不影响常规窗口

Phase 2: 在 impl.go BlockCommitted 末尾调 s.perfAggregator.OnBlockCommitted(block)

Phase 3: 给 retryScheduler 安插埋点（15-20 个）
         - OnBlockCommitted 各阶段
         - RetryCommit Phase 1/1b/2/2b
         - tryToReSimulation

Phase 4: 跑实验 → 按 Total 排序输出瓶颈表
Phase 5: 推广到 verify / state_lock / sim_leader / sim_member 等模块
```
