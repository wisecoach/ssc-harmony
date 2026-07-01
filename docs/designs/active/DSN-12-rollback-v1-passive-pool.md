# 修复清单：回退 v1 被动池，对齐 v2 设计

> 当前代码在多个 bug fix 之上叠加了 v1 被动池代码。本清单只列出需要**修改**的代码段，不涉及回退其他修复。

## 文件：`ssc/retry_scheduler.go`

### 1. `passivePoolEntry` 结构体（行 204-214）→ 简化

**删除** `passivePoolEntry` struct（waitingKeys / EnterBlock 在 v2 中不需要）。

**替换为**：v2 只需要 `map[common.Hash]struct{}` 做标记。不需要结构体。

### 2. `retryScheduler` 中的 `passivePool` 字段（行 219）→ 改类型

**旧**：`passivePool map[common.Hash]*passivePoolEntry`
**新**：`passivePool map[common.Hash]struct{}`

### 3. `poolTimeout` 字段（行 251）→ 删除

**已存在**：`poolTimeout uint64`
**动作**：删除此字段。v2 的 timeout 不在 retryScheduler 层处理。

### 4. `NewRetryScheduler` 中的初始化（行 181）→ 改

**旧**：`passivePool: make(map[common.Hash]*passivePoolEntry),`
**新**：`passivePool: make(map[common.Hash]struct{}),`

### 5. `AddToPassivePool` 方法（行 259-281）→ 重写

**旧**：接收 `lockedKeys`，创建 `passivePoolEntry` 带 waitingKeys。

**新**：只做标记：
```go
func (rs *retryScheduler) AddToPassivePool(txHash common.Hash) {
    rs.mu.Lock()
    defer rs.mu.Unlock()
    rs.passivePool[txHash] = struct{}{}
    chainRetryStats.SigPassiveAdd.Add(1)
}
```

### 6. `RemoveFromPassivePool` 方法（行 283-287）→ 保留

方法名和逻辑不变，仅改类型：`delete(rs.passivePool, txHash)` 对 `map[common.Hash]struct{}` 同样有效。

### 7. `IsInPassivePool` 方法（行 289-293）→ 保留

仅改 `_, exists := rs.passivePool[txHash]` 检查类型变化。

### 8. `moveToActivePool` 方法（行 296-313）→ 删除

v2 不需要此方法。唤醒走 RetrySignal 路径，不靠 chainNextSim 移池。

### 9. `scanPassivePool` 方法（行 315-330）→ 删除

v2 不需要 chainNextSim 扫描被动池。

### 10. `OnBlockCommitted` 中的被动池跳过逻辑 → 修改

在当前 `OnBlockCommitted` 的 `for txHash, tx := range rs.retryPool` 循环（约行 496-519）中，**不在首部加跳过逻辑**。而是在 `CanLock` 之前判断：

```go
for txHash, tx := range rs.retryPool {
    // v2: 被动池中的 tx 不参与 poll
    if _, inPassive := rs.passivePool[txHash]; inPassive {
        continue
    }
    signal := &api.RetrySignal{...}
    ...
}
```

### 11. `OnBlockCommitted` 中的被动池超时代码（行 517-538）→ 删除

**删除**以下代码块：
```go
// 扫描被动池超时
var expiredPassive []common.Hash
currentBlock := block.NumberU64()
for txHash, entry := range rs.passivePool { ... }
for _, txHash := range expiredPassive { ...; delete(rs.passivePool, txHash) }
for _, txHash := range expiredPassive { ... CloseTransaction(...) }
```

以及日志行的 `passivePoolSize` / `passiveExpired`。

### 12. `RetryCommit` 中的 onChainLockedKeys 逻辑（行 1038-1072）→ 回退

**删除**以下改动：
```go
var onChainLockedKeys []api.LockKey
... errors.Is(err, api.ErrLockConflict_OnChain) ... onChainLockedKeys = append(...)
if len(onChainLockedKeys) > 0 { ... AddToPassivePool(...) }
```

**恢复为**原始逻辑：遇到 `stateDB.CheckLock` 失败直接 `return &api.RetryCommitResp{Locked: false, TxHash: txHash}`。

### 13. `RetryCommit` 中收到 RetryCommit 时检查被动池 → 新增

在 `RetryCommit` 函数开始处（约行 875-877），检查 tx 是否在被动池中：

```go
func (rs *retryScheduler) RetryCommit(txHash common.Hash) *api.RetryCommitResp {
    // v2: 如果在被动池中，移出（被 O 的 RetrySignal 唤醒）
    if _, inPassive := rs.passivePool[txHash]; inPassive {
        delete(rs.passivePool, txHash)
        chainRetryStats.SigPassiveWaken.Add(1)
        utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
            Msg("retryCommit: woken from passive pool")
    }
    ...
```

### 14. `chainNextSim` 末尾的被动池扫描代码（行 647-652）→ 删除

**删除**：
```go
// 4. 扫描被动池：检查是否有 tx 等待的 key 已释放
waken := rs.scanPassivePool(keySet)
for _, txHash := range waken {
    chainRetryStats.SigPassiveWaken.Add(1)
    rs.moveToActivePool(txHash)
}
```

---

## 文件：`ssc/impl.go`

### A. `poolTimeout` 注入（行 229）→ 删除

```go
if sscConfig.Timeout != nil && sscConfig.Timeout.PoolTimeout > 0 {
    service.retryScheduler.poolTimeout = uint64(sscConfig.Timeout.PoolTimeout)
}
```

### B. `closeTransaction` 被动池清理（行 1217）→ 保留

```go
s.retryScheduler.RemoveFromPassivePool(txHash)
```

---

## 文件：`ssc/temp_lock_view.go`

无修改。`committedWriteLocks` 删除是正确的，保留。

---

## 其他需要保留的改动（非被动池，不改）

| 内容 | 文件 | 说明 |
|:-----|:-----|:------|
| `maxRetriesTotal` 字段 + 注入 | `retry_scheduler.go` + `impl.go` | A08 重试限制 |
| `IsOnChain` / `CloseTransaction` in Accessor | `retry_scheduler.go` + `impl.go` | A08 重试限制 |
| `ChainDepthMu` / `ChainDepthCnt` / `ChainDepthCommitCnt` | `retry_scheduler.go` | 链式深度统计 |
| `SigPassiveAdd` / `SigPassiveWaken` / `SigPassiveTimeout` | `retry_scheduler.go` stats | **保留**，v2 会复用 |
| stats dump 中的 `Int64("passiveAdd/Waken/Timeout")` | `retry_scheduler.go` | **保留**，v2 会复用 |

---

## 修改总结

| 操作 | 数量 | 说明 |
|:-----|:----:|:------|
| 删除结构体 | 1 | `passivePoolEntry` → 不需要 |
| 删除方法 | 2 | `moveToActivePool` / `scanPassivePool` |
| 删除 RetryCommit 逻辑 | 1 | `onChainLockedKeys` 分支 |
| 删除 chainNextSim 逻辑 | 1 | 被动池扫描代码块 |
| 删除 OnBlockCommitted 逻辑 | 1 | 超时代码块 |
| 删除 impl.go 注入 | 1 | `poolTimeout` |
| 重写方法 | 1 | `AddToPassivePool`（简化） |
| 新增逻辑 | 1 | `RetryCommit` 开头的唤醒检查 |
| 保留 | 4 | closeTransaction 清理、stats 计数器、IsIn、Remove |
