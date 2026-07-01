# BUG-04：链式重试交易永久卡住（锁泄漏 + 并发重入）

> **创建日期**：2026-06-24
> **状态**：🟡 已修复，待实验验证
> **涉及文件**：`ssc/retry_scheduler.go`
> **前置修复**：`E03-retry-tx-chain-build-simtx.md`（CommitSimulation Multicast 修复）

---

## 问题现象

实验跑了 8.5 小时后，分片状态显示大量 unfinished 交易：

```
Shard 0: {'commit': 131, 'timeout': 3, 'unfinished': 70}
Shard 1: {'commit': 150, 'timeout': 30, 'unfinished': 120}
Shard 2: {'commit': 122, 'unfinished': 40, 'timeout': 4}
Shard 3: {'commit': 216, 'timeout': 12, 'unfinished': 101, 'rollback': 1}
```

这些 unfinished 交易：
- 没有 `leader close transaction`（既没 commit 也没 rollback）
- SimTx 在 origin shard 成功上链 + verify 通过
- Sp1 超时后 origin shard 发了 rollback vote
- **永远等不齐所有 related shard 的 vote**

---

## 根因分析

### 根因 1：`tryToReSimulation` 并发重入

`HandleRetrySignal` 被多个 shard 同时调用时，每个都 `go rs.tryToReSimulation(retryTx)`。`tryToReSimulation` **没有任何防重入检查**，导致同一笔 tx 的 `retryCommit` 被并发执行多次。

流程链条：

```
HandleRetrySignal（shard 3）→ go tryToReSimulation
HandleRetrySignal（shard 1）→ go tryToReSimulation  ← 并发！
```

多个 `tryToReSimulation` 都调用 `RetryCommit` 到各 related shard。TempLockView 的 Wound-Wait 会让其中一个胜出，但**所有 RPC 调用可能都返回 `Locked=true`**（不同 shard 的 TempLockView 独立），导致**多次 `retry commit success`**。

### 根因 2：共享 `txState.Ctx` 被过早 cancel

每次 `tryToReSimulation` → `retryCommit success` → `TriggerReSimulation` → `startReSimulation`。

`startReSimulation` 第 689 行从 `txState.Ctx` 创建子 ctx，末尾 `defer cancel()`。**多个并发 `startReSimulation` 共享同一个 `txState.Ctx`，任何一个结束时的 `defer cancel()` 都会 cancel 这个共享 ctx**，导致其他正在执行的 `startReSimulation` 中的 `Multicast(ctx, ...)` 立即失败（`ctx canceled before call`）。

### 根因 3：SimTx 通知缺失 → close 流程阻塞

`CommitSimulation Multicast` 因为 `ctx canceled before call` 失败，导致 **shard 1 没有收到 SimTx 的 ChainPatch 通知**。

shard 1 没有 `AddOnChainPatch` → 没有 `VerifySimulation` → 没有 Sp1 timer → 没有 vote。

origin shard（shard 3）在 Sp1 超时后发了 rollback vote，但**需要等齐所有 related shard 的 SSC vote 才能 close**。shard 1 永远不会发 vote → close 永久阻塞 → `stateLockManager` 的锁永远残留。

---

## 日志证据

### 并发 `recall simulation`
```
16:08:29.996 recall simulation, simulationNum: 1, start
16:08:29.996 recall simulation, simulationNum: 1, start  ← 并发！
16:08:30.014 recall simulation, simulationNum: 1, start  ← 并发！
```

### Multicast 因 ctx 过期失败
```
CommitSimulation Multicast failed
  error: ctx canceled before call
  memberCount: 2
```

### Shard 1 缺失 SimTx 通知
（shard 1 上搜索这笔 tx：有 `handle cxt ssc call` 但无 `AddOnChainPatch`/`VerifySimulation`）

### 锁残留 15,409 个 block
```
00:42:27 LOCK_STALE: lock held more than 10 blocks, possible leak
  txHash: 0xea0890f90ab1bb...
  heldBlocks: 15409
```

---

## 修复方案

### 修改：`ssc/retry_scheduler.go` — 加 `reSimInFlight` 防重入

在 `retryScheduler` struct 中新增 `reSimInFlight map[common.Hash]struct{}`：

```go
type retryScheduler struct {
    // ... 原有字段
    reSimInFlight map[common.Hash]struct{} // 防止同一笔 tx 并发 reSim
}
```

在 `tryToReSimulation` 开头检查：

```go
func (rs *retryScheduler) tryToReSimulation(retryTx *api.RetryTx) {
    txHash := retryTx.TxHash

    // 防重入
    rs.mu.Lock()
    if _, inFlight := rs.reSimInFlight[txHash]; inFlight {
        rs.mu.Unlock()
        return // 已在执行中，跳过
    }
    rs.reSimInFlight[txHash] = struct{}{}
    rs.mu.Unlock()

    defer func() {
        rs.mu.Lock()
        delete(rs.reSimInFlight, txHash)
        rs.mu.Unlock()
    }()
    // ... 原有逻辑
}
```

改动量：`+27` 行（struct 字段 + 初始化 + 3 行检查 + 7 行清理）

---

## 待验证

1. 编译后同步到远程服务器
2. 重新运行实验 `shard=4_validator=4_ssc=1_delay=20_rate=100_vpn=4`
3. 检查漏斗：
   - `HandleRetrySignal: received` → `retry commit success` ✅ 应保持 1:1（不再并发触发多次）
   - `unfinished` 应大幅减少或归零
   - `LOCK_STALE` 不应再出现（锁不再残留）

---

## 修复前数据（基线）

| 指标 | 修复前 |
|------|:------:|
| 总用户 tx | 1,000 |
| committed | 641 |
| timeout (rollback) | 36 |
| unfinished | **322** |
| LOCK_STALE（shard 3 残留） | 1 笔 tx，9 个锁，15,409 blocks |
| lock 最大持有时间 | 8.5 小时（实验全程） |
