---
id: EXP-01
title: stateLock 锁争抢分析与优化方案
type: EXP
status: done
author: Designer
created: 2026-07-01
conclusion: stateLock 争抢是 closeTx 960ms 瓶颈的根因，缩小锁范围即可解决
refs: [DSN-A06]
---

## 问题

`closeTransaction` 中 `stateLock` 持锁时间异常，max=963ms，导致出块共识被拖慢。

## 方法

### 数据来源

1. `commitTx` / `closeTx` 计时（CommitOrRollbackWithProof timing breakdown）
2. `stateLock` / `simCleanup` / `verCleanup` 三级计时（closeTransaction timing breakdown）

### 实验配置

- shard=4, validator=4, ssc=1, delay=10, rate=100, vpn=4
- 实验期间共处理 6279 笔 CR 交易

## 发现

### 瓶颈定位

| 字段 | P50 | P90 | P99 | **max** |
|------|:---:|:---:|:---:|:------:|
| commitTx | 0.00ms | 0.00ms | 0.02ms | 0.05ms |
| stateLock | 0.00ms | 0.42ms | 13.96ms | **963ms** |
| simCleanup(累计) | 0.01ms | 0.46ms | 23.59ms | **965ms** |
| verCleanup(累计) | 0.02ms | 0.49ms | 24.16ms | **965ms** |
| Verifier.Cleanup | 0.00ms | 0.00ms | 0.01ms | 4.53ms |
| StaleTx | 0.00ms | 0.00ms | 0.02ms | 4.71ms |
| RemoveOnChainPatch | 0.00ms | 0.00ms | 0.01ms | 0.29ms |
| PatchPool.Remove | 0.02ms | 0.04ms | 0.10ms | 0.45ms |

**关键结论**：stateLock / simCleanup / verCleanup 是累计计时（`time.Since(t0)`），三者 max 几乎一致（963→965→965ms），差值 <2ms。说明 **960ms 全花在 stateLock 段开头**，后面 Cleanup 本身仅 1-2ms。

### 根因分析

`closeTransaction` 在 stateLock 锁内做了过多操作：

```go
s.stateLock.Lock()
tx, _ := s.getTxStateLocked(txHash)      // 需要锁
if tx != nil {
    tx.CtxCancel()                       // ✗ 不需要锁！
    if s.IsLeader(...) { log }           // ✗ 不需要锁！
}
delete(s.txStates, txHash)               // 需要锁
s.finishedTxs[txHash] = commitOrRollback // 需要锁
s.stateLock.Unlock()
```

`stateLock` 是一个全局 `sync.RWMutex`，保护 `txStates` 和 `finishedTxs` 两个 map。全代码库有 21 处访问点（15 写 6 读）。当 300 笔 CR 同时提交时，所有 goroutine 排队抢同一把写锁，队尾的等 1s。

## 结论

`feasible` — 缩小锁范围方案可行。

### 推荐方案：缩小 `closeTransaction` 锁范围

将 `CtxCancel()` 和日志移到锁外：

```go
// 锁内只做 map 操作（瞬发）
var relatedShards api.RelatedShards
var leader bool
s.stateLock.Lock()
tx, _ := s.getTxStateLocked(txHash)
if tx != nil {
    relatedShards = tx.RelatedShards
    leader = s.IsLeader(tx.Epochs[s.SelfShard])
}
delete(s.txStates, txHash)
s.finishedTxs[txHash] = commitOrRollback
s.stateLock.Unlock()

// CtxCancel 和日志在锁外
if tx != nil {
    tx.CtxCancel()
    if leader { ... log ... }
}
```

**效果**：持锁时间从 960ms 降至 <0.01ms（仅两次 map 查找 + 一次 delete + 一次 insert）。

### 其他方案

| 方案 | 改动量 | 收益 | 风险 |
|:----|:------:|:----:|:----:|
| A. 缩小锁范围 | 小（改 1 函数） | 大（960ms→0） | 低 |
| B. closeTransactions 批量解耦 | 小 | 中 | 低 |
| C. txStates 改用多分片 map | 大（21 处访问点） | 大 | 中 |
| D. 改用 sync.Map | 中 | 中 | 中 |
