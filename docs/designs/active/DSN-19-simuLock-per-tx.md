---
id: DSN-19
title: Simulator 全局锁 → sync.Map + per-entry 改造
type: DSN
status: implemented
priority: P1
author: Designer
created: 2026-07-01
scope: [ssc]
refs: [DSN-18, EXP-01]
---

# Simulator 全局锁 → sync.Map + per-entry 改造

## 一、动机

DSN-18 消除 `stateLock` 后，`closeTransaction` 的 `stateLock` 段从 419ms 降到 0.01ms，但 `Simulator.Cleanup` 中仍有两个全局锁贡献尖峰：

| 锁 | P50 | P90 | P99 | max |
|:-:|:---:|:---:|:---:|:---:|
| `simuLock` | 0.00ms | 0.00ms | 17.20ms | 299.75ms |
| `pendingLock` | 0.00ms | 0.00ms | 17.20ms | 299.75ms |

300 笔 CR × 每个 Cleanup 抢这两把锁 → 3600 次全局锁竞争，尾延迟与旧 `stateLock` 同级。

**根因**：`simuLock` 和 `pendingLock` 都是全局锁，但保护的 4 张 map 都是 `map[common.Hash]...X` 结构——key 是 `txHash`，每笔交易独立。DSN-18 的 `sync.Map + per-tx mu` 模式可以直接复用。

---

## 二、数据结构改造

### 2.1 `simuChannels` — 替代 simuWaitingChs + simuResultCh

```go
// 每笔交易独立的模拟结果通知通道
type simuChannels struct {
    mu         sync.Mutex
    waitingChs []chan *api.CXTSimulationSSCResult
    resultCh   chan *api.CXTSimulationSSCResult
}
```

### 2.2 Simulator 层改造

```go
// simulator.go
type Simulator struct {
    // 原：simuLock       lm.Mutex
    //     simuWaitingChs map[common.Hash][]chan ...
    //     simuResultCh   map[common.Hash]chan ...
    simuChMap    sync.Map    // key: common.Hash, value: *simuChannels  ← 新增
    // simuLock ← 删除
    // simuWaitingChs ← 删除
    // simuResultCh ← 删除

    // 原：pendingLock     sync.Mutex
    //     pendingRequests map[common.Hash][]*pendingCXTRequest
    pendingReqs  sync.Map    // key: common.Hash, value: []*pendingCXTRequest  ← 新增
    // pendingLock ← 删除
    // pendingRequests ← 删除

    // 原：syncLock     lm.Mutex
    //     callStatesInWaiting map[common.Hash][]*api.SimulationCallState
    callStates   sync.Map    // key: common.Hash, value: []*api.SimulationCallState  ← 新增
    // syncLock ← 删除
    // callStatesInWaiting ← 删除

    // 原：simLock + simStates: 全局 RWMutex
    // simLock    lm.RWMutex
    // simStates  map[common.Hash]*api.SimulationState
    simStates    sync.Map    // key: common.Hash, value: *api.SimulationState  ← 新增
    // simLock ← 删除

    // ... 其余字段不变
}
```

### 2.3 sscService.txTraces 独立锁

`recordTraceBlock` 当前滥用 Simulator 的 `simuLock` 保护 `sscService.txTraces`，必须在删除 `simuLock` 前独立。

```go
// impl.go - sscService 追加
type sscService struct {
    // ...
    txTraces   map[common.Hash]*TxBlockTrace
    traceLock  sync.Mutex    // ← 新增：独立锁，不再借用 simuLock
    // ...
}
```

---

## 三、访问点逐一改造（simuChannels）

### 3.1 辅助函数

```go
// getOrCreateChannels 获取或创建 per-tx 的 simuChannels
func (sim *Simulator) getOrCreateChannels(txHash common.Hash) *simuChannels {
    val, _ := sim.simuChMap.LoadOrStore(txHash, &simuChannels{})
    return val.(*simuChannels)
}

// getChannels 只读获取（不创建）
func (sim *Simulator) getChannels(txHash common.Hash) *simuChannels {
    val, ok := sim.simuChMap.Load(txHash)
    if !ok {
        return nil
    }
    return val.(*simuChannels)
}
```

### 3.2 `StartSimulateCXTransaction`（simulator_leader.go:61-73）

当前：
```go
func() {
    sim.simuLock.Lock()
    defer sim.simuLock.Unlock()
    if sim.simuWaitingChs[txHash] == nil {
        sim.simuWaitingChs[txHash] = make([]chan *api.CXTSimulationSSCResult, 0)
    } else {
        waitingCh = make(chan *api.CXTSimulationSSCResult)
        sim.simuWaitingChs[txHash] = append(sim.simuWaitingChs[txHash], waitingCh)
    }
    if sim.simuResultCh[txHash] == nil {
        sim.simuResultCh[txHash] = make(chan *api.CXTSimulationSSCResult, 1)
    }
}()
```

改后：
```go
func() {
    ch := sim.getOrCreateChannels(txHash)
    ch.mu.Lock()
    if len(ch.waitingChs) > 0 || ch.resultCh != nil {
        // 已有等待者或已设置结果通道 → 加入等待队列
        waitingCh = make(chan *api.CXTSimulationSSCResult)
        ch.waitingChs = append(ch.waitingChs, waitingCh)
    }
    // else: 首次，不创建 waitingCh → 该 goroutine 自己跑模拟
    if ch.resultCh == nil {
        ch.resultCh = make(chan *api.CXTSimulationSSCResult, 1)
    }
    ch.mu.Unlock()
}()
```

deferred cleanup（L79-83）：
```go
defer func() {
    ch := sim.getOrCreateChannels(txHash)
    ch.mu.Lock()
    ch.waitingChs = nil  // 清理等待者（本 goroutine 完成后不再有新的等待）
    ch.mu.Unlock()
}()
```

### 3.3 结果到达通知（simulator_leader.go:262-270）

当前：
```go
func() {
    sim.simuLock.Lock()
    defer sim.simuLock.Unlock()
    simState, _ := sim.GetSimState(txHash)
    if simState != nil {
        simState.SimulationCallStates[state.SimulationNum][0].TopSSCResult = sscResult
        simState.SimulationResult = sscResult
    }
    chs = sim.simuWaitingChs[txHash]
}()
```

改后：
```go
func() {
    simState, _ := sim.GetSimState(txHash)
    if simState != nil {
        simState.SimulationCallStates[state.SimulationNum][0].TopSSCResult = sscResult
        simState.SimulationResult = sscResult
    }
    if ch := sim.getChannels(txHash); ch != nil {
        ch.mu.Lock()
        chs = ch.waitingChs
        ch.waitingChs = nil  // 取出后清空
        ch.mu.Unlock()
    }
}()
```

### 3.4 `GetSimuResultCh` / `SetSimuResultCh` / `DeleteSimuResultCh`

```go
func (sim *Simulator) GetSimuResultCh(txHash common.Hash) chan *api.CXTSimulationSSCResult {
    ch := sim.getChannels(txHash)
    if ch == nil {
        return nil
    }
    ch.mu.Lock()
    ret := ch.resultCh
    ch.mu.Unlock()
    return ret
}

func (sim *Simulator) SetSimuResultCh(txHash common.Hash, ch chan *api.CXTSimulationSSCResult) {
    c := sim.getOrCreateChannels(txHash)
    c.mu.Lock()
    c.resultCh = ch
    c.mu.Unlock()
}

func (sim *Simulator) DeleteSimuResultCh(txHash common.Hash) {
    if ch := sim.getChannels(txHash); ch != nil {
        ch.mu.Lock()
        ch.resultCh = nil
        ch.mu.Unlock()
    }
}
```

### 3.5 `AddSimuWaitingCh` / `PopSimuWaitingChs`

```go
func (sim *Simulator) AddSimuWaitingCh(txHash common.Hash, ch chan *api.CXTSimulationSSCResult) {
    c := sim.getOrCreateChannels(txHash)
    c.mu.Lock()
    c.waitingChs = append(c.waitingChs, ch)
    c.mu.Unlock()
}

func (sim *Simulator) PopSimuWaitingChs(txHash common.Hash) []chan *api.CXTSimulationSSCResult {
    c := sim.getChannels(txHash)
    if c == nil {
        return nil
    }
    c.mu.Lock()
    chs := c.waitingChs
    c.waitingChs = nil
    c.mu.Unlock()
    return chs
}
```

### 3.6 `Cleanup`（simulator.go:194-223）

当前：
```go
sim.simuLock.Lock()
delete(sim.simuResultCh, txHash)
delete(sim.simuWaitingChs, txHash)
sim.simuLock.Unlock()
// ...
sim.pendingLock.Lock()
delete(sim.pendingRequests, txHash)
sim.pendingLock.Unlock()
// ...
sim.syncLock.Lock()
delete(sim.callStatesInWaiting, txHash)
sim.syncLock.Unlock()
```

改后：
```go
sim.simuChMap.Delete(txHash)     // 原子删除整个 simuChannels（无需 per-entry lock）
// ...
sim.pendingReqs.Delete(txHash)    // 原子删除
// ...
sim.callStates.Delete(txHash)     // 原子删除
```

---

## 四、访问点逐一改造（pendingRequests）

`pendingRequests`：`map[common.Hash][]*pendingCXTRequest`，2 处访问。

### 4.1 `PopPendingRequests`

当前：
```go
func (sim *Simulator) PopPendingRequests(txHash common.Hash) []*pendingCXTRequest {
    sim.pendingLock.Lock()
    defer sim.pendingLock.Unlock()
    reqs := sim.pendingRequests[txHash]
    delete(sim.pendingRequests, txHash)
    return reqs
}
```

改后（sync.Map + `LoadAndDelete`）：
```go
func (sim *Simulator) PopPendingRequests(txHash common.Hash) []*pendingCXTRequest {
    val, ok := sim.pendingReqs.LoadAndDelete(txHash)
    if !ok {
        return nil
    }
    return val.([]*pendingCXTRequest)
}
```

### 4.2 `AddPendingRequest`

当前：
```go
func (sim *Simulator) AddPendingRequest(txHash common.Hash, req *pendingCXTRequest) {
    sim.pendingLock.Lock()
    defer sim.pendingLock.Unlock()
    sim.pendingRequests[txHash] = append(sim.pendingRequests[txHash], req)
}
```

改后（sync.Map + `LoadOrStore` + cas loop）：
```go
func (sim *Simulator) AddPendingRequest(txHash common.Hash, req *pendingCXTRequest) {
    for {
        val, loaded := sim.pendingReqs.LoadOrStore(txHash, []*pendingCXTRequest{req})
        if !loaded {
            return // 首次存入成功
        }
        reqs := val.([]*pendingCXTRequest)
        reqs = append(reqs, req)
        if sim.pendingReqs.CompareAndSwap(txHash, val, reqs) {
            return
        }
        // CAS 失败 → 重试（并发写入时）
    }
}
```

不过 CAS loop 对 append 操作来说比较啰嗦。**简化方案**：将 `pendingRequests` 改为 `sync.Map` 存指针值，值类型改为带锁的结构体：

```go
type pendingRequestList struct {
    mu   sync.Mutex
    reqs []*pendingCXTRequest
}

type Simulator struct {
    pendingReqsMap sync.Map  // key: common.Hash, value: *pendingRequestList
}

func (sim *Simulator) getOrCreatePendingReqs(txHash common.Hash) *pendingRequestList {
    val, _ := sim.pendingReqsMap.LoadOrStore(txHash, &pendingRequestList{})
    return val.(*pendingRequestList)
}

func (sim *Simulator) AddPendingRequest(txHash common.Hash, req *pendingCXTRequest) {
    l := sim.getOrCreatePendingReqs(txHash)
    l.mu.Lock()
    l.reqs = append(l.reqs, req)
    l.mu.Unlock()
}

func (sim *Simulator) PopPendingRequests(txHash common.Hash) []*pendingCXTRequest {
    val, ok := sim.pendingReqsMap.LoadAndDelete(txHash)
    if !ok {
        return nil
    }
    l := val.(*pendingRequestList)
    l.mu.Lock()
    reqs := l.reqs
    l.reqs = nil
    l.mu.Unlock()
    return reqs
}
```

---

## 五、访问点逐一改造（callStatesInWaiting）

`callStatesInWaiting`：`map[common.Hash][]*api.SimulationCallState`，2 处访问。

### 5.1 `AddCallStatesInWaiting`

```go
type callStateList struct {
    mu   sync.Mutex
    states []*api.SimulationCallState
}

type Simulator struct {
    callStates sync.Map  // key: common.Hash, value: *callStateList
}

func (sim *Simulator) AddCallStatesInWaiting(txHash common.Hash, callState *api.SimulationCallState) {
    val, _ := sim.callStates.LoadOrStore(txHash, &callStateList{})
    l := val.(*callStateList)
    l.mu.Lock()
    l.states = append(l.states, callState)
    l.mu.Unlock()
}
```

### 5.2 Cleanup 中删除

```go
sim.callStates.Delete(txHash)
```

### 5.3 `PopCallStatesInWaiting`（在 loop 中调用）

需要查这个函数的位置：

```go
// 当前：
func (sim *Simulator) PopCallStatesInWaiting(blockHash common.Hash) []*api.SimulationCallState {
    sim.syncLock.Lock()
    defer sim.syncLock.Unlock()
    states := sim.callStatesInWaiting[blockHash]  // 注意 key 是 blockHash！
    delete(sim.callStatesInWaiting, blockHash)
    return states
}
```

⚠ **注意力**：`callStatesInWaiting` 的 key 是 `blockHash`（`common.Hash`），**不是 txHash**。不适合 per-tx 分解！

所以 `callStatesInWaiting` 保留 `syncLock`，本次不改。

---

## 六、`recordTraceBlock` 锁独立化

当前 `sscService.recordTraceBlock` 借用 `s.simuLock`（即 `Simulator.simuLock`），必须在删除 `simuLock` 前解耦。

```go
// impl.go - sscService 新增字段
type sscService struct {
    // ...
    txTraces   map[common.Hash]*TxBlockTrace
    traceLock  sync.Mutex       // ← 新增：独立锁
    // ...
}

// tx_trace.go
func (s *sscService) recordTraceBlock(txHash common.Hash, stage TraceStage, blockNum uint64) {
    s.traceLock.Lock()
    defer s.traceLock.Unlock()
    trace, exists := s.txTraces[txHash]
    if !exists {
        trace = &TxBlockTrace{TxHash: txHash}
        s.txTraces[txHash] = trace
    }
    trace.Record(stage, blockNum)
    // ...
}
```

`txTraces` 同样可以做 `sync.Map` 改造（per-tx 写），但访问频率低、不构成性能热点，独立锁就够了。**在 `traceLock` 可用之前，不能删除 `simuLock`。**

---

## 七、改造步骤

### Step 1：`sscService` 加 `traceLock`
- `impl.go`: 加 `traceLock sync.Mutex`
- `tx_trace.go`: `recordTraceBlock` 改为用 `s.traceLock`
- 🔒 必须先做这步，否则 `simuLock` 删除后 `txTraces` 裸奔

### Step 2：`simuChannels` + `sync.Map` 替换 `simuWaitingChs` + `simuResultCh`
- `simulator.go`:
  - 定义 `simuChannels` 结构体
  - 删 `simuLock`、`simuWaitingChs`、`simuResultCh`
  - 加 `simuChMap sync.Map`
  - 改 `getOrCreateChannels` / `getChannels` 辅助函数
  - 改 `GetSimuResultCh` / `SetSimuResultCh` / `DeleteSimuResultCh` / `AddSimuWaitingCh` / `PopSimuWaitingChs`
  - 改 `Cleanup`：`simuChMap.Delete(txHash)` 替代 3 条 delete

### Step 3：`pendingReqs` + `sync.Map` 替换 `pendingLock` + `pendingRequests`
- `simulator.go`:
  - 定义 `pendingRequestList`（含 `mu sync.Mutex`）
  - 删 `pendingLock`、`pendingRequests`
  - 加 `pendingReqsMap sync.Map`
  - 改 `AddPendingRequest` / `PopPendingRequests`
  - 改 `Cleanup`：`pendingReqsMap.Delete(txHash)`

### Step 4：`callStatesInWaiting` 确认不改
- key 是 `blockHash` 不是 `txHash`，`sync.Map` 不适合
- 保留 `syncLock`

### Step 5：`simStates` + `sync.Map` 替换 `simLock` + `simStates map`
- `simulator.go`:
  - 删 `simLock`、`simStates map[common.Hash]*api.SimulationState`
  - 加 `simStates sync.Map`
  - 改 `GetSimState` / `SetSimState` / `SetChainPatch` / `DeleteSimState` / `GetBlockHash`
  - 改 `Cleanup`：已通过 `DeleteSimState` 间接使用 `sync.Map`
- 值与 DSN-18/19 中需要 per-entry `mu` 的 map 不同：`simStates` 的 value 是 `*api.SimulationState`（指针），无「读旧→算新→写回」复合操作，`sync.Map` 自带原子性就够

### Step 6：编译 + 实验验证
- `go build` 编译
- 跑一轮实验，确认 `simuLock` / `pendingLock` / `simLock` 不再出现，`Cleanup` 尾延迟降至 ~0ms

---

## 八、预期效果

### 改造范围

| 数据结构 | 当前锁 | 改造方案 |
|:---------|:-------|:---------|
| `simuWaitingChs` + `simuResultCh` | `simuLock` (全局) | `sync.Map` + per-entry `mu` |
| `pendingRequests` | `pendingLock` (全局) | `sync.Map` + per-entry `mu` |
| `callStatesInWaiting` | `syncLock` (全局) | **不改**（key 是 blockHash） |
|| `simStates` | `simLock` (RWMutex) | `sync.Map`（无 per-entry `mu`，值是指针） |
| `txTraces` | 借用 `simuLock` | 独立 `traceLock` |

### 竞争消除效果

| 指标 | 当前 | 改造后 |
|:----|:----:|:------:|
| `simuLock` max | 299.75ms | **0ms（删除）** |
| `pendingLock` max | 299.75ms | **0ms（删除）** |
| `simLock` max | 17.20ms（P99） | **0ms（删除）** |
| 300 笔 CR 锁竞争 | 3600 次全局排队 | 零阻塞（per-tx） |
| 高风险 | 跨结构锁污染（txTraces 借用 simuLock） | 锁域隔离 |

### 注意

- `simStates` 已在本 DSN 中一并改造为 `sync.Map`（值是指针，无需 per-entry `mu`）。剩余全局锁 `simLock`（`lm.RWMutex`）已删除。
- `callStatesInWaiting` 的 key 是 `blockHash`，不适合 per-tx 分解——未来如需优化，可考虑按 block batch 分片锁。
