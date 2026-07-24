# RSH-02: retryScheduler 性能指标埋点方案

> **版本**：v1（2026-07-23）
> **范围**：retry_scheduler.go + patchpool.go 各阶段耗时埋点
> **目标**：每笔交易/每个区块的关键阶段记录耗时，配合分析脚本量化瓶颈

---

## 1. 现有埋点

| 位置 | 格式 | 输出频率 |
|:-----|:-----|:---------|
| `RemoveFromPassivePool` (rs:380-383) | `"RemoveFromPassivePool timing"` + duration | 每次被动池唤醒 |
| `retryScheduler.StaleTx` (rs:1162-1165) | `"retryScheduler.StaleTx timing"` + duration | 每次 stale |
| `retryCommit timing` (rs:1171-1175) | `"retryCommit timing"` + cost | 每次 RetryCommit |
| `PatchPool.Remove timing` (pp:106-108) | `"PatchPool.Remove timing"` + duration | 每次 Patch 删除 |
| `RetryCommit Phase2 CheckLock: slow` (rs:1313-1318) | Warn 级，>50ms 才打 | 触发时 |

**不足：**
- OnBlockCommitted 内部各阶段没拆分
- RetryCommit Phase 1b / Phase 2b 的 DAG 搜索耗时没记录
- findCoveringSet / scanPatchSubscribers 没埋
- tryToReSimulation 内部没分解
- 缺一个 **"全路径累计耗时"** 的统一结构

---

## 2. 需要新增的埋点

### 2.1 OnBlockCommitted 内部（按阶段拆分）

```
阶段                             标志                          期望精度
├── staleTxs 清理                staleCleanup                  ~μs
├── querySubscribers             querySubscribers              ~μs
├── 优先级排序 (sort.SliceStable)  sortCandidates               ~ms
├── Reservation 去重               reservation                  ~μs
├── stateDB.CheckLock 预检        stateCheckLock                ~ms
├── sendReSimulationSignals      sendSignals                   ~RPC ms
├── wounded retry 恢复            woundedRecovery               ~μs
├── hotKey 统计                   hotKeyStats (P1)             ~ms ⚠️可删
├── lockWaitPool 扫描             lockWaitScan                  ~ms
├── 日志统计 6×Range              statsRange (P0)              ~ms ⚠️合并
```

### 2.2 RetryCommit 内部（按 Phase 拆分）

```
阶段                             标志
├── Phase 1: TL TryLock          tlvTryLock
├── Phase 1b: Patch DAG search   dagSearchPhase1b
├── Phase 1b: tryConsumePatch    dagConsumePhase1b
├── Phase 2: stateDB CheckLock   stateCheckLock        ✅ 已有（但只有>50ms打）
├── Phase 2b: Patch DAG search   dagSearchPhase2b
├── Phase 2b: tryConsumePatch    dagConsumePhase2b
```

### 2.3 findCoveringSet

```
阶段                             标志
├── keyIndex 遍历收集候选         collectCandidates
├── 贪心选取循环                  greedySelect
```

### 2.4 scanPatchSubscribers

```
阶段                             标志
├── querySubscribers              querySubscribers
├── 逐个 sorted 的 findCoveringSet findCoveringSetPerCandidate
├── Reservation                   reservation
```

### 2.5 tryToReSimulation 内部

```
阶段                             标志
├── retryPool 状态日志             poolStats (already own)
├── reSimInFlight 检查            reSimFlightCheck
├── 并行 RPC RetryCommit          parallelRetryCommit   ~RPC ms
├── Wound 后处理                   woundHandling
├── LockWait 加入                 lockWaitAdd
├── 释放 consumed patches         releaseConsumedPatches
```

---

## 3. 埋点格式约定

### 3.1 统一字段

所有新增埋点用 `Debug` 级别，格式：

```go
t0 := time.Now()
// ... 操作 ...
utils.SSCLogger().Debug().
    Str("cat", "retryScheduler").
    Str("func", "OnBlockCommitted").
    Str("phase", "staleCleanup").
    Dur("dur", time.Since(t0)).
    Int("ctx", extraInfo).    // 上下文：tx 数 / key 数 / selected 数
    Msg("[perf] retryScheduler perf")
```

关键字段：
- `cat`: `"retryScheduler"` 或 `"patchpool"`
- `func`: 外层函数名
- `phase`: 阶段名（见 §2 的「标志」列）
- `dur`: `time.Duration`，用 `Dur()` 而非 `Str()` 以便 grep 统一解析

### 3.2 日志标记前缀

所有性能埋点统一用 `[perf]` 前缀：

```
[perf] retryScheduler perf  →  retryScheduler 模块
[perf] patchpool perf       →  patchpool 模块
```

这样分析脚本可以直接 `grep "\[perf\]"` 抽出。

### 3.3 上下文字段

| 阶段 | 上下文字段 | 说明 |
|:-----|:-----------|:------|
| staleCleanup | `staleN` | 清理的 tx 数 |
| querySubscribers | `keys`, `candidatesN` | 释放 key 数、命中 tx 数 |
| sortCandidates | `candidatesN` | 候选 tx 数 |
| reservation | `selected`, `reservedKeys` | 选中 tx 数、预留 key 数 |
| stateCheckLock | `selected`, `writeKeys`, `readKeys` | CheckLock 的 tx 数 |
| hotKeyStats | `poolSize` | retryPool 大小 |
| lockWaitScan | `poolSize` | LockWaitPool 大小 |
| statsRange | `poolSize` | 各池汇总 |
| tlvTryLock | `writeKeys`, `readKeys` | 锁竞争 key 数 |
| dagSearchPhase1b/2b | `conflictKeys`, `candidates` | 冲突 key 数、候选 Patch 数 |
| greedySelect | `keys`, `patches`, `rounds` | 覆盖循环轮次 |

---

## 4. 分析脚本设计

### 4.1 输入

从实验日志中提取 `[perf]` 行。

### 4.2 输出结构

针对每个日志文件（按 shard），输出：

```
## OnBlockCommitted 耗时分布 (单位: ms)
┌────────────────────┬──────┬──────┬──────┬──────┐
│ phase              │ p50  │ p90  │ p99  │ max  │
├────────────────────┼──────┼──────┼──────┼──────┤
│ staleCleanup       │ 0.02 │ 0.05 │ 0.10 │ 0.50 │
│ querySubscribers   │ 0.01 │ 0.03 │ 0.08 │ 0.30 │
│ sortCandidates     │ 0.50 │ 2.00 │ 5.00 │ 15.0 │
│ reservation        │ 0.10 │ 0.30 │ 0.80 │ 2.00 │
│ stateCheckLock     │ 1.00 │ 5.00 │ 20.0 │ 50.0 │
│ hotKeyStats        │ 2.00 │ 10.0 │ 30.0 │ 80.0 │
│ lockWaitScan       │ 0.50 │ 2.00 │ 5.00 │ 10.0 │
│ statsRange         │ 1.00 │ 5.00 │ 15.0 │ 40.0 │
└────────────────────┴──────┴──────┴──────┴──────┘

#### 耗时 Top 3 阶段 (OBC 总耗时中位数 Xms)
1. stateCheckLock        →   X%  ← checkLock 是瓶颈
2. hotKeyStats           →   X%  ← 纯日志浪费
3. statsRange            →   X%  ← 6×Range

## RetryCommit 耗时分布
┌────────────────────┬──────┬──────┬──────┬──────┐
│ phase              │ p50  │ p90  │ p99  │ max  │
├────────────────────┼──────┼──────┼──────┼──────┤
│ tlvTryLock         │ X    │ X    │ X    │ X    │
│ stateCheckLock     │ X    │ X    │ X    │ X    │
│ dagSearchPhase1b   │ X    │ X    │ X    │ X    │
│ dagConsumePhase1b  │ X    │ X    │ X    │ X    │
│ dagSearchPhase2b   │ X    │ X    │ X    │ X    │
└────────────────────┴──────┴──────┴──────┴──────┘
```

事件数统计：
```
## 各阶段被触发/被跳过次数
┌────────────────────┬──────┬────────┐
│ phase              │ cnt  │ note   │
├────────────────────┼──────┼────────┤
│ OnBlockCommitted   │ 1000 │ 1000 block │
│ dagSearchPhase1b   │ 200  │ TLV 失败率 20%  │
│ dagSearchPhase2b   │ 150  │ stateDB 冲突率 15% │
└────────────────────┴──────┴────────┘
```

### 4.3 脚本接口

```bash
python3 scripts/analyze_retryscheduler_perf.py <logfile>
```

输出：以上三张表 + 总耗时排序。

---

## 5. 实施步骤

```
Step 1: 新增 OnBlockCommitted 内 8 个阶段的埋点
Step 2: 新增 RetryCommit 内 6 个阶段的埋点
Step 3: 新增 findCoveringSet 内 2 个阶段的埋点
Step 4: 新增 tryToReSimulation 内 6 个阶段的埋点
Step 5: 编写 analyze_retryscheduler_perf.py 脚本
Step 6: 跑一次实验验证埋点正常输出
Step 7: 用脚本分析 → 输出瓶颈表 → 决策哪个 DSN 做
```
