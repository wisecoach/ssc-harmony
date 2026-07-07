---
id: DSN-21
title: retryScheduler 全局锁 rs.mu → sync.Map + per-entry 改造
type: DSN
status: proposed
priority: P1
author: Designer
created: 2026-07-01
scope: [ssc]
refs: [DSN-19, DSN-18, DSN-20, EXP-01]
---

# retryScheduler 全局锁 rs.mu → sync.Map + per-entry 改造

## 一、动机

DSN-20 的 timing breakdown 数据显示 `AddToRetry.poolInsert` 存在严重锁竞争：

| 字段 | P50 | P90 | P99 | max |
|:-----|:---:|:---:|:---:|:---:|
| `poolInsert` | **0.05ms** | **638ms** | **4210ms** | **5456ms** |

P50=0.05ms（单次操作瞬发），但 P90=638ms——相差 4 个数量级。**根因不是单次插入慢，而是多笔交易在 `rs.mu.Lock()` 上互相阻塞。**

`rs.mu`（`sync.RWMutex`）是 `retryScheduler` 的全局锁，同时保护 8 张 map：

| 字段 | 类型 | key 类型 | 适合 per-tx？ |
|:-----|:-----|:--------:|:------------:|
| `retryPool` | `map[hash]*RetryTx` | txHash | ✅ 天然 per-tx |
| `passivePool` | `map[hash]struct{}` | txHash | ✅ |
| `passivePoolEnterBlock` | `map[hash]uint64` | txHash | ✅ |
| `staleTxs` | `map[hash]struct{}` | txHash | ✅ |
| `consumedPatches` | `map[hash]hash` | txHash | ✅ |
| `reSimInFlight` | `map[hash]struct{}` | txHash | ✅ |
| `patches` | `map[hash]map[int]*ChainNode` | txHash | ✅ sync.Map + per-entry |
| `onChainPatches` | `map[hash]map[int]*ChainNode` | txHash | ✅ sync.Map + per-entry |
| `signals` | `map[hash]map[int]map[uint32]*RetrySignal` | txHash | ✅ 拆外层 |

**全部 8 张 map 的 key 都是 `common.Hash`（txHash）**——DSN-18/19 已验证 `sync.Map + per-entry mu` 模式可以无缝适用。

---

## 二、实验证据

DSN-20 实验数据（shard=4, validator=4, ssc=1, delay=10, rate=100, vpn=4）：

![](images/exp-01-poolInsert-distribution.png)

| 分位 | poolInsert | 推断 |
|:----:|:----------:|:-----|
| P50 | 0.05ms | 无竞争时极快 |
| P90 | 638ms | 被 2+ 个 writer 阻塞 |
| P99 | 4210ms | 被长时间 writer 卡住 |
| 样本 | 9770 笔 | 数据充足 |

`AddToRetry.poolInsert` 内部在 `rs.mu.Lock()` 下只做：exists 检查、stale 检查、map insert、log——每项 < 0.01ms。所以 P90 高不是慢在 insert 本身，而是在 `rs.mu` 上排队。

**谁持锁卡住了 `AddToRetry`？**

| 竞争者 | 持锁时长因子 |
|:-------|:-------------|
| `OnBlockCommitted` | 遍历整个 `retryPool`，生成 signals → 锁住直到所有 signal 构建完成 |
| `tryToReSimulation` cleanup | Lock + 删 `retryPool` + 删 `signals` + 删 `consumedPatches` + 释放 PatchPool |
| `AddToRetry` 自身 | Lock + insert（P50=0.05ms，但排队导致 P90=638ms） |

---

## 三、数据结构改造

### 3.1 原则

- key 为 `common.Hash` 的 map → `sync.Map`
- 值需要"读→算→写"原子操作的 → `sync.Map` + per-entry `sync.Mutex` wrapper
- 值是指针（无复合操作）→ 纯 `sync.Map`
- 嵌套 map（`patches`、`onChainPatches`）→ `sync.Map` + per-entry `sync.Mutex`
- `signals`（3 层嵌套）→ 拆外层为 `sync.Map`，内层保留

### 3.2 改造后结构体

```go
type retryScheduler struct {
    // rs.mu ← 删除！以下 map 各自独立同步

    // 纯 sync.Map（值是指针/标量，无复合操作）
    retryPool             sync.Map   // key: common.Hash, value: *api.RetryTx
    passivePool           sync.Map   // key: common.Hash, value: struct{}
    passivePoolEnterBlock sync.Map   // key: common.Hash, value: uint64
    staleTxs              sync.Map   // key: common.Hash, value: struct{}
    consumedPatches       sync.Map   // key: common.Hash, value: common.Hash
    reSimInFlight         sync.Map   // key: common.Hash, value: struct{}

    // sync.Map + per-entry sync.Mutex（需读→改→写原子性）
    patches               sync.Map   // key: common.Hash, value: *chainPatchEntry
    onChainPatches        sync.Map   // key: common.Hash, value: *chainPatchEntry

    // signals 拆外层为 sync.Map，内层保留小 map + 自身锁
    signals               sync.Map   // key: common.Hash, value: *signalEntry
    // signalEntry 包含内层 map + 自身 Mutex

    // 其余字段不变
    patchPool *api.PatchPool
    // ...
}
```

### 3.3 新增辅助结构体

```go
// patches 和 onChainPatches 的 per-entry 结构
type chainPatchEntry struct {
    mu      sync.Mutex
    patches map[int]*api.ChainNode  // simulationNum → chain node
}

// signals 的 per-entry 结构
type signalEntry struct {
    mu      sync.Mutex
    // innerMap[simulationNum][shardId] = signal
    inner   map[int]map[uint32]*api.RetrySignal
}
```

---

## 四、访问点逐一改造

### 4.1 `AddToRetry`（当前 L363-387）

**当前**：`rs.mu.Lock()` 保护 insert + exists 检查 + stale 检查。

**改后**：RWSet 提取已在锁外（`t0` → `tXrw` 之间），只把 insert 操作改为 `sync.Map.Store`：

```go
// extractRWSet …（保持原样，在锁外）

// 用 LoadOrStore 防重复写入
if _, loaded := rs.retryPool.LoadOrStore(tx.TxHash, tx); loaded {
    // 已存在 → 跳过
    return
}
// 无需显式 stale 检查（下个 OnBlockCommitted 会处理）
```

⚠ stale 检查需要拆离：保留 `rs.isStale(tx)` 作为前置过滤，但它只是读 `rs.staleTxs`——`staleTxs` 改为 `sync.Map` 后，`Load` 操作无锁。

```go
if _, stale := rs.staleTxs.Load(tx.TxHash); stale {
    return
}
// 检查通过后 insert
rs.retryPool.Store(tx.TxHash, tx)
```

### 4.2 `OnBlockCommitted`（当前 L409-500）

**当前**：`rs.mu.Lock()` 遍历 `retryPool`，清理 `staleTxs`，构建 `signals`。

**改后**：从 `retryPool.Range` 开始，逐项处理：

```go
// 1. 清理 staleTxs（从 retryPool + staleTxs 中删除）
rs.staleTxs.Range(func(key, _ interface{}) bool {
    txHash := key.(common.Hash)
    rs.retryPool.Delete(txHash)
    rs.staleTxs.Delete(txHash)
    return true
})

// 2. 遍历 retryPool，为每个 tx 构建 signal
shard2Epoch2RetrySignals := ...
rs.retryPool.Range(func(key, value interface{}) bool {
    txHash := key.(common.Hash)
    tx := value.(*api.RetryTx)

    // passive pool 检查（sync.Map.Load）
    if _, inPassive := rs.passivePool.Load(txHash); inPassive {
        return true
    }

    // CanLock 检查
    // ...（不变）

    // 将 signal 写入 signals（不经过 rs.mu，直接 per-entry）
    sigEntry := rs.getOrCreateSignalEntry(txHash)
    sigEntry.mu.Lock()
    sigEntry.inner[signal.SimulationNum][shardId] = signal
    sigEntry.mu.Unlock()

    // 写入 shard2Epoch2RetrySignals
    // ...
    return true
})

// 3. 发送 signals（不变，走 goroutine）

// 4. passive pool 超时检查
expired := []common.Hash{}
rs.passivePoolEnterBlock.Range(func(key, value interface{}) bool {
    txHash := key.(common.Hash)
    enterBlock := value.(uint64)
    if currentBlock >= enterBlock+uint64(rs.maxRetriesTotal) {
        expired = append(expired, txHash)
    }
    return true
})
for _, txHash := range expired {
    rs.passivePool.Delete(txHash)
    rs.passivePoolEnterBlock.Delete(txHash)
    // closeTransaction…
}
```

### 4.3 `chainNextSim`（当前 L568-577）

**当前**：`rs.mu.RLock()` → 遍历 `retryPool` → `RUnlock`。

**改后**：`sync.Map.Range` 无需锁：

```go
rs.retryPool.Range(func(key, value interface{}) bool {
    txHash := key.(common.Hash)
    retryTx := value.(*api.RetryTx)
    if bytes.Equal(retryTx.TxHash.Bytes(), upstreamTxHash.Bytes()) {
        return true
    }
    if dependsOn(retryTx, keySet) {
        allMatched = append(allMatched, matchedTx{txHash: txHash, retryTx: retryTx})
    }
    return true
})
```

### 4.4 `HandleRetrySignal`（当前 L688-692）

**当前**：`rs.mu.RLock()` → 读 `retryPool[txHash]` → `RUnlock`。

**改后**：`sync.Map.Load` 无锁：

```go
val, exists := rs.retryPool.Load(signal.TxHash)
if exists && val != nil {
    retryTx := val.(*api.RetryTx)
    // ...
}
```

### 4.5 `tryToReSimulation` 成功/失败路径

**当前**：`rs.mu.Lock()` 删除 `retryPool`、`signals`、`consumedPatches`。

**改后**：各自独立 `sync.Map.Delete`：

```go
// 成功路径
go rs.state.TriggerReSimulation(retryTx.TxHash, retryTx.SimulationNum)
rs.retryPool.Delete(txHash)
rs.signals.Delete(txHash)
rs.consumedPatches.Delete(txHash)

// 失败路径 — consumedPatches 释放
if consumedSimTx, loaded := rs.consumedPatches.LoadAndDelete(txHash); loaded {
    rs.patchPool.Release(consumedSimTx.(common.Hash))
}
```

### 4.6 `RetryCommit`（当前 RLock 读 retryPool）

**当前**：
```go
rs.mu.RLock()
retryTx := rs.retryPool[txHash]
rs.mu.RUnlock()
```

**改后**：
```go
val, ok := rs.retryPool.Load(txHash)
if !ok {
    return &api.RetryCommitResp{Locked: false, TxHash: txHash}
}
retryTx := val.(*api.RetryTx)
```

### 4.7 `HandleReSimulationSignal`（当前 L740-770）

**当前**：`rs.mu.Lock()` 读写 `signals` + `retryPool`。

**改后**：

```go
for _, signal := range signals.Signals {
    sigEntry := rs.getOrCreateSignalEntry(signal.TxHash)
    sigEntry.mu.Lock()
    sigEntry.setSignal(fromShard, signal)
    readyCnt := sigEntry.countReady(signal.SimulationNum)
    sigEntry.mu.Unlock()

    val, ok := rs.retryPool.Load(signal.TxHash)
    if ok {
        retryTx := val.(*api.RetryTx)
        if readyCnt == len(retryTx.RelatedShards) {
            go rs.tryToReSimulation(retryTx)
        }
    }
}
```

### 4.8 `patches` / `onChainPatches` 访问

**当前**：`rs.mu.Lock()` 写 `rs.onChainPatches[txHash]`。

**改后**：per-entry `sync.Mutex`：

```go
type chainPatchEntry struct {
    mu      sync.Mutex
    patches map[int]*api.ChainNode
}

func (rs *retryScheduler) getOrCreateOnChainPatchEntry(txHash common.Hash) *chainPatchEntry {
    val, _ := rs.onChainPatches.LoadOrStore(txHash, &chainPatchEntry{
        patches: make(map[int]*api.ChainNode),
    })
    return val.(*chainPatchEntry)
}

// 写操作
entry := rs.getOrCreateOnChainPatchEntry(txHash)
entry.mu.Lock()
entry.patches[simNum] = node
entry.mu.Unlock()

// 读操作
val, ok := rs.onChainPatches.Load(txHash)
if !ok { return nil }
entry := val.(*chainPatchEntry)
entry.mu.Lock()
defer entry.mu.Unlock()
return entry.patches[simNum]
```

---

## 五、`signals` 拆解详细设计

`signals` 当前是 3 层嵌套 map：

```go
map[common.Hash]map[int]map[uint32]*api.RetrySignal
//  txHash →      simNum →  shardId →  signal
```

外层（txHash）拆为 `sync.Map`，内层通过 `signalEntry` 保护：

```go
type signalEntry struct {
    mu    sync.Mutex
    inner map[int]map[uint32]*api.RetrySignal  // simNum → shardId → signal
}

func (rs *retryScheduler) getOrCreateSignalEntry(txHash common.Hash) *signalEntry {
    val, _ := rs.signals.LoadOrStore(txHash, &signalEntry{
        inner: make(map[int]map[uint32]*api.RetrySignal),
    })
    return val.(*signalEntry)
}

// 写
entry := rs.getOrCreateSignalEntry(txHash)
entry.mu.Lock()
entry.setSignal(fromShard, signal)
entry.mu.Unlock()

// 清理
rs.signals.Delete(txHash)
```

---

## 六、需要处理的数据竞争

### 6.1 `tryToReSimulation.inFlightCheck`

当前 `reSimInFlight` 用 `rs.mu` 保护。改为 `sync.Map` 后：

```go
// 检查是否有进行中的 reSim
if _, loaded := rs.reSimInFlight.LoadOrStore(txHash, struct{}{}); loaded {
    return // 已有进行中
}
defer rs.reSimInFlight.Delete(txHash)
```

### 6.2 `AddToRetry` 与 `OnBlockCommitted` 并发

问题：`AddToRetry.Store(txHash, tx)` 刚写入，`OnBlockCommitted.Range` 可能还没看到这条记录——`sync.Map.Range` 不保证快照一致性。但这不是问题：`OnBlockCommitted` 每块跑一次，没看到的 tx 下一块会处理。

### 6.3 `OnBlockCommitted` 与 `chainNextSim` 并发

同样：`chainNextSim.Range` 看到的 `retryPool` 快照包含或不包含刚插入的 tx，都正确（链式依赖有因果关系，刚插入的 tx 没有上游 CommitSimulation 触发它）。

---

## 七、改造步骤

### Step 1：定义辅助结构体 + 工具函数

- `chainPatchEntry` + `getOrCreateOnChainPatchEntry` / `getOrCreatePatchEntry`
- `signalEntry` + `getOrCreateSignalEntry`
- 所有在 `simulator.go` 还是 `retry_scheduler.go`？→ `retry_scheduler.go`

### Step 2：`retryPool` + `passivePool` + `passivePoolEnterBlock` → sync.Map

- 改字段声明
- 改 `AddToRetry`：`rs.mu.Lock()` → `retryPool.LoadOrStore` + `staleTxs.Load` 前置过滤
- 改 `HandleRetrySignal`：`rs.mu.RLock()` → `retryPool.Load`
- 改 `RetryCommit`：`rs.mu.RLock()` → `retryPool.Load`
- 改 `chainNextSim`：`rs.mu.RLock()` → `retryPool.Range`
- 改 `OnBlockCommitted`：`rs.mu.Lock()` → `retryPool.Range`（见 4.2）
- 改 `tryToReSimulation` 成功/失败：`retryPool.Delete` + `consumedPatches.Delete`
- 改 `HandleReSimulationSignal`：`rs.mu.RLock()` → `retryPool.Load`

### Step 3：`staleTxs` → sync.Map

- `isStale` 读：`staleTxs.Load`
- `addToStale`：`staleTxs.Store`
- `OnBlockCommitted` 清理：`staleTxs.Range` + `retryPool.Delete`

### Step 4：`consumedPatches` + `reSimInFlight` → sync.Map

- `tryToReSimulation` 中的读写改为 `sync.Map.LoadOrStore` / `Delete`
- `RetryCommit` 中的 `rs.consumedPatches[txHash]` → `consumedPatches.Load`

### Step 5：`patches` + `onChainPatches` → sync.Map + per-entry

- `setChainPatch` / `GetChainPatch` / `GetOnChainPatch` / `AddOnChainPatch` / `ReadOnChainPatch` / `RemoveOnChainPatch` 全部改为 per-entry

### Step 6：`signals` → sync.Map + per-entry

- `setSignal` / `countReady` 改为 `signalEntry` 方法
- `getSignals` 读操作改为 `signals.Load` + entry.mu
- `deleteSignals` → `signals.Delete`

### Step 7：删除 `rs.mu`

- 确认所有 `rs.mu.Lock` / `rs.mu.RLock` / `rs.mu.Unlock` / `rs.mu.RUnlock` 均已移除
- 编译验证

### Step 8：实验验证

- `go build` 编译
- `test_single` 跑一轮
- 确认 `AddToRetry.poolInsert` P90 降至 < 10ms

---

## 八、预期效果

| 指标 | 改造前 | 预期改造后 |
|:-----|:------:|:----------:|
| `AddToRetry.poolInsert` P50 | 0.05ms | < 0.05ms（不变） |
| `AddToRetry.poolInsert` P90 | **638ms** | **< 1ms** |
| `AddToRetry.poolInsert` P99 | **4210ms** | **< 10ms** |
| `RetryCommit` total | P50=0.64ms, P90=12ms | P50 略降，P90 大幅降 |
| `OnBlockCommitted` 持锁 | 全局锁遍历全池 | per-entry 锁，无互斥 |
| `rs.mu` | 存在 | 彻底删除 |

预期 P90 从 638ms 降到 < 1ms 的依据：`AddToRetry.poolInsert` 内部操作只有 3 次 `sync.Map` 调用（Load + LoadOrStore + Store）加一次 log，总耗时 < 0.1ms。不再被其他 writer 阻塞。

---

## 九、变更清单

| 文件 | 改动 |
|:-----|:------|
| `ssc/retry_scheduler.go` | 删 `rs.mu sync.RWMutex`；6 个 map 改为 `sync.Map`；`patches`/`onChainPatches` 改为 per-entry；`signals` 拆为 `signalEntry`；所有访问点逐一改造 |
| `ssc/retry_scheduler.go` | `NewRetryScheduler`/`reset` 初始化改为空 `sync.Map`（无需初始化） |
