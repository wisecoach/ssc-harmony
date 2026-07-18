# DSN-30: PatchPool 合并入 RetryScheduler

## 1. 动机

### 1.1 问题

DSN-29 将 PatchPool 提取为独立包（`ssc/patchpool/`），后合并为同包单文件（`ssc/patchpool.go`）。但独立结构导致了严重的调用方耦合：

| 耦合类型 | 表现 |
|:---------|:------|
| **回调擦屁股** | `OnPatchPoolUpdated` 需要 `loadRetryTx` 回调访问 `retryPool`，暴露了不应暴露的数据结构 |
| **数据跨域** | `consumedPatches` 在 `retryScheduler`，但 `OnPatchPoolUpdated` 的匹配结果需要它——不得不通过 `MatchResult` 结构体传递再写回 |
| **中间跳转** | RS → PatchPool 15+ 处 `rs.patchPool.XXX()` 调用，层层委托无意义 |
| **权限交叉** | `temp_lock_view.go` 直接调 `patchPool.IsFinalized`，外部模块穿透 RS 访问子组件 |

### 1.2 根因

PatchPool 和 `retryScheduler` 共享**同一组 txHash 生命周期**——PatchPool 不管理独立的生命周期，它只是 RS 的一个功能模块。

```
独立思考：PatchPool 能否有自己的生命周期？
  → 不能。Patch 的 Add/Consume/Release 时机由 RS 的 retry 决策驱动。
    没有独立的触发源，没有独立的定时器，没有独立的状态机。
  → 它只是一个数据索引 + 查询算法的组合，不是有独立生命周期的模块。
```

## 2. 方案

### 2.1 合并

PatchPool 的 4 个 `sync.Map` 字段提升为 `retryScheduler` 的直接字段：

```
合并前                               合并后
─────                                ─────
retryScheduler {                     retryScheduler {
  patchPool *PatchPool {               patches       sync.Map  // PatchPool.Patches
    Patches      sync.Map              keyIndex      sync.Map  // PatchPool.KeyIndex
    KeyIndex     sync.Map              subscriber    sync.Map  // PatchPool.Subscriber
    Subscriber   sync.Map              txSubKeys     sync.Map  // PatchPool.txSubKeys
    txSubKeys    sync.Map              consumedPatches sync.Map
  }                                    retryPool     sync.Map
  consumedPatches sync.Map             // ...其他字段
  retryPool     sync.Map           }
  // ...其他字段
}
```

### 2.2 接口迁移

PatchPool 方法 → `retryScheduler` 方法（命名规整为 动词+名词 风格）：

| PatchPool 方法 | → retryScheduler 方法 |
|:---------------|:----------------------|
| `Add(txHash, simNum, writeSet)` | `addPatch(txHash, simNum, writeSet)` |
| `Remove(txHash)` | `removePatch(txHash)` |
| `Stats()` | `patchStats()` |
| `HasConflict(retryTx)` | `patchesHaveConflict(retryTx)` |
| `FindCovering(keys)` | `findCoveringPatch(keys)` |
| `FindCoveringSet(keys)` | `findCoveringSet(keys)` |
| `TryConsume(txHash, consumer, pri)` | `tryConsumePatch(txHash, consumer, pri)` |
| `Release(txHash)` | `releasePatch(txHash)` |
| `Finalize(txHash)` | `finalizePatch(txHash)` |
| `IsFinalized(txHash)` | `isPatchFinalized(txHash)` |
| `Subscribe(txHash, reads, writes)` | `subscribeRetryTx(txHash, reads, writes)` |
| `Unsubscribe(txHash)` | `unsubscribeRetryTx(txHash)` |
| `ReSubscribe(txHash, reads, writes)` | `resubscribeRetryTx(txHash, reads, writes)` |
| `QuerySubscribers(keys)` | `querySubscribers(keys)` |
| `OnPatchPoolUpdated(writeSet, loadRetryTx)` | `scanPatchSubscribers(writeSet)` — 无回调 |

### 2.3 `OnPatchPoolUpdated` 变化（关键）

```
// 当前 (解耦债)
results := rs.patchPool.OnPatchPoolUpdated(writeSet, func(txHash common.Hash) *api.RetryTx {
    val, ok := rs.retryPool.Load(txHash)
    if !ok { return nil }
    return val.(*api.RetryTx)
})
for _, r := range results {
    r.RetryTx.Status = api.RetryConsumed
    rs.patchPool.Unsubscribe(r.TxHash)
    rs.consumedPatches.Store(r.TxHash, r.ConsumedPatches)
    rs.sendChainSignal(...)
}

// 合并后 (直接)
func (rs *retryScheduler) scanPatchSubscribers(writeSet *api.RWSet) {
    keys := extractWriteKeys(writeSet)
    candidates := rs.querySubscribers(keys)       // 直接调自己的 subscriber
    for _, txHash := range candidates {
        retryTx, _ := rs.retryPool.Load(txHash)    // 直接访问 retryPool
        if retryTx == nil { continue }
        // 跳过非 active 状态
        // rs.findCoveringSet(allKeys)
        // rs.tryConsumePatch(...)
        // rs.consumedPatches.Store(...)            // 直接写自己的字段
        // rs.sendChainSignal(...)
    }
}
```

### 2.4 纯数据结构保留

`patchpool.go` 保留为纯类型+纯函数文件，无 `retryScheduler` 依赖：

```
ssc/patchpool.go
  ├── ChainPatchNode          ← 数据结构 + atomic Status
  ├── PatchConsumeStatus      ← 枚举 (Free / Consumed / Finalized)
  ├── extractWriteKeys()      ← 纯函数
  └── mergeRWSet()            ← 纯函数
```

`MatchResult` 删掉——合并后循环直接在 RS 内，不需要传递结构体。

### 2.5 调用方改动

| 文件 | 改前 | 改后 |
|:-----|:------|:------|
| `temp_lock_view.go` | `v.slm.sscService.rs.patchPool.IsFinalized(holder)` | `v.slm.sscService.rs.isPatchFinalized(holder)` |
| `impl.go` | `s.rs.patchPool.Finalize(txHash)` | `s.rs.finalizePatch(txHash)` |
| `impl.go` | `s.rs.patchPool.Release(txh)` | `s.rs.releasePatch(txh)` |
| `impl.go` | `s.rs.patchPool.Remove(txHash)` | `s.rs.removePatch(txHash)` |
| `impl.go` | `s.rs.patchPool.Add(...)` | `s.rs.addPatch(...)` |
| `retry_scheduler.go` 内部 | `rs.patchPool.XXX` | `rs.XXX` |

## 3. 收益

| 维度 | 当前 | 合并后 |
|:-----|:------|:-------|
| 字段归属 | 分散在 2 个结构体 | 统一在 RS |
| 回调 | `loadRetryTx` ugly hack | 无回调，直接访问 |
| 中间方法 | `rs.patchPool.XXX()` 15+ 处 | `rs.XXX()` 直调 |
| 跨模块访问 | TLV 穿透 RS 调 patchPool | RS 统一出口 |
| 文件数 | `patchpool.go` 570 行 + RS 1500 行 | RS 继承 570 行，`patchpool.go` 缩为 ~150 行 |
| 理解成本 | 需要跳转理解 PatchPool 和 RS 的关系 | 单文件内全部可见 |

## 4. 决策记录

| # | 决策 | 结论 |
|:-:|:-----|:------|
| 1 | PatchPool 字段归属 | 4 个 sync.Map 全部提升到 retryScheduler |
| 2 | PatchPool 方法名 | 动词+名词风格，无点号（不叫 `rs.patchPool.XXX`） |
| 3 | `OnPatchPoolUpdated` | 重命名 `scanPatchSubscribers`，直接访问 RS 字段，无回调 |
| 4 | `loadRetryTx` 回调 | 删除 |
| 5 | `MatchResult` 结构体 | 删除 |
| 6 | `patchpool.go` | 保留为纯类型+纯函数文件，无 RS 依赖 |
