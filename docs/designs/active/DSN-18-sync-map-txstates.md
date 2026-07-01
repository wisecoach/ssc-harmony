---
id: DSN-18
title: sync.Map + per-entry lock 改造方案
type: DSN
status: planned
priority: P1
author: Designer
created: 2026-07-01
scope: [ssc]
refs: [DSN-17, EXP-01]
---

# sync.Map + per-entry lock 改造方案

## 一、动机

当前 `stateLock` 优化后 `closeTransaction` 持锁从 960ms 降到 0.01ms，但 `stateLock` 上仍有 419ms 排队竞争——来自其余 13 处写站点。300 笔 CR × ~12 次/笔 = 3600 次抢锁，队尾等 419ms。

方案：用 `sync.Map` + per-tx `sync.Mutex` 替换全局 `stateLock + map + finishedTxs`，不同 tx 的操作不再互锁。

---

## 二、TxState 改造

```go
// api/types.go
type TxState struct {
    mu           sync.Mutex         // ← 新增：per-tx 锁
    Closed       bool               // ← 新增：替代 finishedTxs
    TxHash       common.Hash
    Nonce        uint64
    TxSender     common.Address
    Epochs       []Epoch
    Status       CXTStatus
    SimulationNum int
    OriginShardId uint32
    RelatedShards RelatedShards
    RetrySignals  map[int]map[uint32]*RetrySignal
    Ctx           context.Context    `json:"-"`
    CtxCancel     context.CancelFunc `json:"-"`
}
```

### 注意事项

- `sync.Mutex` 值拷贝语义问题：`sync.Mutex` 不可拷贝。`sync.Map` 中存储 `*TxState` 指针即可（原本已有 `map[common.Hash]*TxState`，指针不变）
- `Closed` bool 合并 `finishedTxs`：不再需要独立的 `finishedTxs map[common.Hash]bool`

---

## 三、sscService 改造

```go
// impl.go - 去掉 stateLock 和 finishedTxs
type sscService struct {
    txStates    sync.Map    // key: common.Hash, value: *api.TxState
    // stateLock  ← 删除
    // finishedTxs ← 删除（合并到 TxState.Closed）
    // txStates map[common.Hash]*api.TxState  ← 删除
}
```

---

## 四、21 处访问点逐一改造

### 4.1 读模式：单笔查询

当前：
```go
s.stateLock.RLock()
tx := s.txStates[txHash]
s.stateLock.RUnlock()
```

改后：
```go
if val, ok := s.txStates.Load(txHash); ok {
    tx := val.(*api.TxState)
    tx.mu.Lock()
    // ... 读字段
    tx.mu.Unlock()
}
```

#### 涉及的访问点（6 处）

| # | 函数 | 读取内容 |
|:-:|------|---------|
| 1 | `GetTxState` (Simulator accessor, L233) | `txStates[txHash]` + `finishedTxs[txHash]` |
| 2 | `IsTxFinished` (Verifier accessor, L314) | `finishedTxs[txHash]` |
| 3 | `IsTxFinished` (Committer accessor, L334) | `finishedTxs[txHash]` |
| 4 | `handleTxPoolTimeout` (L448) | `txStates[txHash]` |
| 5 | `HandleCXTCommitSSCVote` (L946) | `txStates[txHash]` + `tx.RelatedShards` |
| 6 | `getTxState` (L1089) | 通用读 |

**技巧**：`IsTxFinished` 原来是读 `finishedTxs`，现在读 `tx.Closed`：
```go
if val, ok := s.txStates.Load(txHash); ok {
    tx := val.(*api.TxState)
    tx.mu.Lock()
    closed := tx.Closed
    tx.mu.Unlock()
    return closed
}
return false  // tx not in map → 等价于已关闭
```

### 4.2 写模式：单字段写入

当前：
```go
s.stateLock.Lock()
if tx := s.txStates[txHash]; tx != nil {
    tx.Status = status
}
s.stateLock.Unlock()
```

改后：
```go
if val, ok := s.txStates.Load(txHash); ok {
    tx := val.(*api.TxState)
    tx.mu.Lock()
    tx.Status = status
    tx.mu.Unlock()
}
```

#### 涉及的访问点（9 处）

| # | 函数 | 写入字段 |
|:-:|------|---------|
| 7 | `SetTxStatus` (Simulator, L244) | `tx.Status` |
| 8 | `MergeRelatedShards` (Simulator, L251) | `tx.RelatedShards` |
| 9 | `SetSimulationNum` (Simulator, L260) | `tx.SimulationNum` |
| 10 | `SetStatus` (Verifier, L307) | `tx.Status` |
| 11 | `SetWaitingForResimu` (Verifier, L320) | `tx.Status` |
| 12 | `SetStatus` (Committer, L340) | `tx.Status` |
| 13 | `SignCXTSimulation` (L562) | `tx.RelatedShards` |
| 14 | `HandleCommitVote` (L598) | `tx.Status` |
| 15 | `CommitSimulation #2` (L840) | `tx.Status` |

### 4.3 读 + 写模式

当前：
```go
s.stateLock.Lock()
tx, err := s.getTxStateLocked(txHash)
tx.RelatedShards = tx.RelatedShards.Merge(commit.RelatedShards)
s.stateLock.Unlock()
```

改后：
```go
if val, ok := s.txStates.Load(txHash); ok {
    tx := val.(*api.TxState)
    tx.mu.Lock()
    tx.RelatedShards = tx.RelatedShards.Merge(commit.RelatedShards)
    tx.mu.Unlock()
}
```

#### 涉及的访问点（3 处）

| # | 函数 | 操作 |
|:-:|------|------|
| 16 | `CommitSimulation #1` (L741) | get + Merge RelatedShards |
| 17 | `HandleCXTCommitProof` (L1072) | get + set Status |
| 18 | `CreateTxState` (L267) | map insert |

**`CreateTxState` 特殊处理**：不需要 `tx.mu`（新创建的 tx 不会被并发访问），直接：
```go
s.txStates.Store(txHash, txState)
```

### 4.4 写 + delete 模式

#### `closeTransaction` (L1186)

当前（已经优化过）：
```go
s.stateLock.Lock()
tx, _ := s.getTxStateLocked(txHash)
delete(s.txStates, txHash)
s.finishedTxs[txHash] = commitOrRollback
s.stateLock.Unlock()
```

改后：
```go
if val, ok := s.txStates.Load(txHash); ok {
    tx := val.(*api.TxState)
    tx.mu.Lock()
    tx.Closed = true
    tx.mu.Unlock()
    tx.CtxCancel()
    // ... 日志
}
s.txStates.Delete(txHash)
```

#### `closeTransactions` (L1226) — 批量

```go
// 锁内只收集
var closedTxs []*api.TxState
s.txStates.Range(func(key, val any) bool {
    txHash := key.(common.Hash)
    if _, ok := txs[txHash]; ok {
        tx := val.(*api.TxState)
        tx.mu.Lock()
        tx.Closed = true
        closedTxs = append(closedTxs, tx)
        tx.mu.Unlock()
        s.txStates.Delete(txHash)
    }
    return true
})
// 锁外 cleanup
for _, tx := range closedTxs {
    tx.CtxCancel()
    // Cleanup...
}
```

---

## 五、`getTxStateLocked` 函数调整

当前函数要求调用方持有 `stateLock`。改造后它应该改为返回已锁住的 tx：

```go
// lockAndGetTx 返回 locked 的 TxState，调用方用完后需 tx.mu.Unlock()
func (s *sscService) lockAndGetTx(txHash common.Hash) *api.TxState {
    for {
        val, ok := s.txStates.Load(txHash)
        if !ok {
            // 不存在的 tx → 可能是已删除（被 closeTransaction 清理）
            // 但需要区分"已关闭"和"从未创建"
            return nil  
        }
        tx := val.(*api.TxState)
        tx.mu.Lock()
        if tx.Closed {
            tx.mu.Unlock()
            return nil  // 已关闭
        }
        // Check if still in map (Validate + reacquire pattern)
        if _, ok := s.txStates.Load(txHash); ok {
            return tx  // ✅ still alive, caller owns tx.mu
        }
        tx.mu.Unlock()
        // tx was deleted between mu.Lock and re-check → retry
    }
}
```

但**这个模式太复杂**。大多数访问点是单字段写入，不需要复杂的 retry 逻辑。建议**不保留通用函数**，每个访问点独立 `Load → Lock → Write → Unlock`。

---

## 六、其他可改造的 map

普查了全 ssc 包 `map[common.Hash]` 类型的数据结构：

### 6.1 高收益候选（类似 pattern：per-tx 独立操作）

| 数据结构 | 文件 | 当前锁 | 建议 |
|:---------|:----|:------|:-----|
| `txStates` | impl.go | `stateLock` ★ | **优先改造** |
| `simStates` | simulator.go | `simLock` | 次优先，Simulator 内部锁 |
| `callStatesInWaiting` | simulator.go | `syncLock` | 时机成熟再改 |
| `executionVerifyContexts` | verify.go | `verifyCtxLock` | 次优先 |

### 6.2 中等收益候选（锁仅保护 map，操作简单）

| 数据结构 | 当前锁 | 说明 |
|:---------|:------|:-----|
| `retryPool` / `passivePool` 等 | `retryScheduler.mu` | 操作频繁但大多 per-tx |
| `txs` (cxt_timer.go) | `CXTTimerManager.lock` | 增加/删除 tx，per-tx |
| `patches` (api/types.go) | `PatchPool.mu` | Add/Remove pattern |

### 6.3 低收益或不适用

| 数据结构 | 原因 |
|:---------|:-----|
| `bkNum2txForSp1` (按 blockNum 索引) | key 不是 txHash，不适合 |
| `callIndex2lockedState` (state_lock_impl.go) | 数据结构复杂，非简单 per-tx |
| `txBlockTraces` (statistics.go) | 读多写少，非性能热点 |
| `finishedTxs` (state_locker.go) | snapshot 模式，非持久并发 |

---

## 七、改造步骤

### Step 1：TxState 加 `mu` + `Closed`
- `api/types.go`: 给 `TxState` 加 `mu sync.Mutex` + `Closed bool`
- 删除 `finishedTxs` 引用，统一为 `tx.Closed`

### Step 2：sscService 去 stateLock
- `impl.go`: 删 `stateLock` + `finishedTxs`，`txStates` 改为 `sync.Map`
- 改 `getTxState` / `getTxStateLocked` / `lockAndGetTx`

### Step 3：逐个改写 21 处访问点
- 读 6 处：`Load + Lock + RUnlock`
- 写 9 处：`Load + Lock + write + Unlock`
- 读+写 3 处：`Load + Lock + read+write + Unlock`
- 闭包 3 处（accessor 函数简化）

### Step 4：`closeTransaction` / `closeTransactions` 适配
- 用 `Load` + per-tx lock + `Delete`

### Step 5：编译 + 实验验证
- `go build` 编译
- 跑一轮实验，确认 `stateLock` 不再出现，`closeTx` 的 849ms 是否降至 ~0ms

---

## 八、预期效果

| 指标 | 当前 | 改造后 |
|:----|:----:|:------:|
| `stateLock` max | 419ms | **0ms（删除）** |
| `closeTx` max | 849ms | **~Simulator.Cleanup + Verifier.Cleanup 的实际耗时** |
| 300 笔 CR 锁竞争 | 3600 次全局排队 | 零阻塞（per-tx） |
| 高风险 | stateLock 误用 | per-tx mu 死锁 |

**最终 `closeTx` 848ms 将降为 Simulator.Cleanup(188ms) + Verifier.Cleanup + 其他 cleanup 的实际耗时，不再有锁排队等待。**
