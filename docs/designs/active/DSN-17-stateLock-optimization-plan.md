# stateLock 锁范围优化分析 — 所有代码位置与优化判断

> 前置文档：`research/EXP-01-stateLock-analysis.md`

## stateLock 保护的数据

```go
stateLock   lm.RWMutex
txStates    map[common.Hash]*api.TxState    // 交易生命周期状态
finishedTxs map[common.Hash]bool            // 已完成交易标记
```

**核心原则**：只有在读/写 `txStates` 和 `finishedTxs` 时才需要持锁。函数调用、日志、`CtxCancel` 等操作不应在锁内。

---

## 一、可优化位置（锁内含无关操作）

### 位置 #1：`closeTransaction` (impl.go:1186-1196) ← 实验确认 960ms 瓶颈

当前代码：
```go
s.stateLock.Lock()
tx, _ := s.getTxStateLocked(txHash)      // 需要锁
if tx != nil {
    tx.CtxCancel()                        // ✗ 不需要锁
    if s.IsLeader(...) { log }            // ✗ 日志不需要锁
}
delete(s.txStates, txHash)                // 需要锁
s.finishedTxs[txHash] = commitOrRollback  // 需要锁
s.stateLock.Unlock()
```

**优化方案**：
```
s.stateLock.Lock()
tx, _ := s.getTxStateLocked(txHash)
delete(s.txStates, txHash)
s.finishedTxs[txHash] = commitOrRollback
s.stateLock.Unlock()

// 以下在锁外执行
if tx != nil {
    tx.CtxCancel()
    if s.IsLeader(...) { log }
}
```

**CtxCancel 安全性**：`context.CancelFunc` 是线程安全的（标准库保证），可在锁外安全调用。`tx.CtxCancel()` 取消的是该交易独立的 `context.Context`，不会影响 `txStates` map。

---

### 位置 #2：`closeTransactions` (impl.go:1235-1244) ← 批量关闭

当前代码：
```go
s.stateLock.Lock()
for ... {
    tx.CtxCancel()                       // ✗ 不需要锁
    s.Simulator.Cleanup(txHash)          // ✗ 不需要锁（Simulator 自管理锁）
    s.finishedTxs[txHash] = true         // 需要锁
    delete(s.txStates, txHash)           // 需要锁
}
s.stateLock.Unlock()
```

**优化方案**：先收集需要清理的 txHash，锁内只做 map 操作，Cleanup 移到锁外。

---

### 位置 #3：`SetChainPatch` (impl.go:200-210) ← retryScheduler 回调

当前代码：
```go
service.stateLock.Lock()
defer service.stateLock.Unlock()
if tx := service.txStates[txHash]; tx != nil {
    sim, ok := service.Simulator.GetSimState(txHash)  // ✗ Simulator 自管理
    if ok && sim != nil {
        sim.ChainPatch = patch                         // ✗ 写 SimState，非 txStates
    }
}
```

**优化方案**：`txStates[txHash]` 存在性检查在锁内做，`GetSimState` + `sim.ChainPatch = patch` 移到锁外。

---

## 二、必须保留在锁内的位置（仅有 map 操作）

### 写操作（14 处），每处 <0.01ms

| # | 函数 | 锁内操作 | 可移出？ |
|:-:|------|----------|:--------:|
| 4 | `SetTxStatus` | `tx.Status = status` | 否，写 TxState 字段 |
| 5 | `MergeRelatedShards` | `tx.RelatedShards = tx.RelatedShards.Merge(...)` | 否 |
| 6 | `SetSimulationNum` | `tx.SimulationNum = num` | 否 |
| 7 | `CreateTxState` | `txStates[txHash] = txState` | 否，写 map |
| 8 | `SetStatus(Verifier)` | `tx.Status = status` | 否 |
| 9 | `SetWaitingForResimu` | `tx.Status = status` | 否 |
| 10 | `SetStatus(Committer)` | `tx.Status = status` | 否 |
| 11 | `SignCXTSimulation` | `tx.RelatedShards = ...` | 否，仅一行赋值 |
| 12 | `HandleCommitVote` | `tx.Status = BUILDING_COMMIT_PROOF` | 否 |
| 13 | `CommitSimulation #1` | `getTxStateLocked + Merge RelatedShards` | 否 |
| 14 | `CommitSimulation #2` | `getTxStateLocked + set Status` | 否 |
| 15 | `HandleCXTCommitProof` | `getTxStateLocked + set Status` | 否 |

### 读操作（6 处）

| # | 函数 | 锁内操作 | 说明 |
|:-:|------|----------|:----:|
| 16-21 | `GetTxState`, `IsTxFinished(x2)`, `handleTxPoolTimeout`, `HandleCXTCommitSSCVote`, `getTxState` | `txStates[txHash]` / `finishedTxs[txHash]` | RLock，读 map |
| — | `handleTxPoolTimeout` 的 RLock 后紧接着 `closeTransaction`（写锁） | 先 RLock 检查存在 → 释放 → closeTransaction（写锁） | 两次锁操作间有竞态窗口，但不可压缩 |

---

## 三、优化方案汇总

### 高优先级（直接消除 960ms 瓶颈）

| 位置 | 修改方式 | 效果 |
|:----:|---------|:----:|
| `closeTransaction` | CtxCancel + 日志移到锁外 | 持锁从 ~960ms → <0.01ms |
| `closeTransactions` | Simulator.Cleanup + CtxCancel 移到锁外 | 批量关闭不再长时间持锁 |

### 中优先级（消除锁内的多余函数调用）

| 位置 | 修改方式 | 效果 |
|:----:|---------|:----:|
| `SetChainPatch` | GetSimState + ChainPatch 写移到锁外 | 消除锁内的跨模块调用 |

### 低优先级（已是最优）

其余 20 处：锁内只有 1-2 次 map 操作 + 1 次字段赋值，持锁时间 <0.01ms，即使有 4200 次/块的调用，总耗时 <42ms，不是瓶颈。

---

## 四、修改后的锁持有对比

```
closeTransaction 当前：
    stateLock.Lock()
        getTxState ~0.001ms
        CtxCancel ~0.010-960ms  ← 主要抖动来源
        log ~0.010-10ms         ← 次要抖动
        delete txStates ~0.001ms
        set finishedTxs ~0.001ms
    stateLock.Unlock()

closeTransaction 优化后：
    stateLock.Lock()
        getTxState ~0.001ms
        delete txStates ~0.001ms
        set finishedTxs ~0.001ms
    stateLock.Unlock()
    // → 总持锁时间 <0.01ms，无变量调用
```

## 五、涉及文件与函数

| 文件 | 函数 | 修改类型 |
|:----|------|:--------:|
| `ssc/impl.go` | `closeTransaction` (L1183) | CtxCancel + log 移出锁 |
| `ssc/impl.go` | `closeTransactions` (L1233) | CtxCancel + Simulator.Cleanup 移出锁 |
| `ssc/impl.go` | `SetChainPatch` closure (L200) | GetSimState 移出锁 |
