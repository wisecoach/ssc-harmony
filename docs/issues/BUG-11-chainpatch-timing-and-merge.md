---
id: BUG-11
title: ChainPatch 时序问题 + Merge 丢失独立性
type: BUG
status: active
priority: P1
reporter: BOSS
module: ssc/verify.go, ssc/impl.go, ssc/retry_scheduler.go
severity: high
created: 2026-07-21
refs: [DSN-23, DSN-26, DSN-41]
---

## 问题描述

ChainPatch 存在两个独立但相互加剧的问题：

### 问题 A：AddOnChainPatch 时序错误

`AddOnChainPatch` 在三个时机被调用，其中两个过早：

| # | 调用位置 | 时机 | 问题 |
|---|----------|------|------|
| 1 | `impl.go:879` | Leader 提交 SimTx **前**（还没上链） | SimTx 可能出块失败，但 Patch 已经入了 `onChainPatches`，无法被清理 |
| 2 | `verify.go:324-327`（`VerifyLockCheck`） | 锁检查阶段，**验证成功前** | 即使 SimTx 验证失败（冲突/exec 错误/链式不一致），Patch 已入 `onChainPatches`，污染下游 |
| 3 | `VerifySimulation` 验证成功后 | 理应在此执行 | 但当前 #2 已提前做了，#3 只是重复，且 leader 会做三次 |

**后果**：验证失败的 SimTx 的 ChainPatch 残留在 `onChainPatches`，下游链式交易的 `ReadOnChainPatch` 可能读到脏数据。

### 问题 B：Phase 1b PatchPool DAG merge 破坏独立性

`retry_scheduler.go:1240-1270`（RetryCommit Phase 1b）中，从 `localPatches` 找到多个 Free Patch 覆盖冲突 key 时：

```go
merged = mergeRWSet(patch, merged)
// ...
rs.state.SetChainPatch(txHash, merged)  // 覆盖了 SimTx 原本的 ChainPatch（自身 WriteSet）
```

这个 merge 将多个独立 Patch 合并成一个无来源信息的值拷贝，然后通过 `SetChainPatch` 覆盖 SimTx 的 `ChainPatch`。后续 `AddOnChainPatch` 把这个 merge 值存进 `onChainPatches` → 再下游读到的 Patch 丢失了原始边界。

## 根因

ChainPatch 在代码中有两个不同含义，但在同一字段上混用：

| 含义 | 正常场景 | Phase 1b 补救后 |
|------|----------|-----------------|
| **自身 WriteSet** | impl.go:830-842 从 callStates 提取 | 被 merge 结果覆盖 |
| **上游 merge 视图** | 不存在 | SetChainPatch 覆盖后变为 merge |

`onChainPatches` 存储的应该是"已验证通过的 SimTx 自身的 WriteSet"——每个 SimTx 一个，独立、有序、可被 `ReadOnChainPatch` 逐一查询。merge 破坏了这一假设。

## 定位

### 关键行号

| 文件 | 行 | 问题 |
|------|----|------|
| `ssc/impl.go` | 879 | Leader 在提交 SimTx 前 AddOnChainPatch → 应改为加链下 patches |
| `ssc/verify.go` | 324-327 | VerifyLockCheck 中 AddOnChainPatch → 应移到 HandleSuccess |
| `ssc/retry_scheduler.go` | 1260-1264 | Phase 1b mergeRWSet + SetChainPatch 覆盖自身 ChainPatch |

### 调用链

```
CommitSimulation (impl.go:879)
  └─ AddOnChainPatch(自身 WriteSet)  ← ❌ 太早，还没上链
  
Multicast SimTx → 所有节点
  └─ VerifySimulation
       └─ VerifyLockCheck (verify.go:324)
            └─ AddOnChainPatch(自身 WriteSet)  ← ❌ 验证还没通过
       └─ VerifyExec
       └─ VerifyHandleSuccess
            └─ lockStateWithRWSet + SendCommitVote
                                          ← ✅ 这里才是正确的时机

RetryCommit (retry_scheduler.go:1240)
  └─ findCoveringSet → patches
  └─ mergeRWSet + SetChainPatch  ← ❌ 丢失独立性
```

## 修复方案

### 修复 1：调整 AddOnChainPatch 时序

- **`verify.go`**: 把 `AddOnChainPatch` 从 `VerifyLockCheck` 移到 `VerifyHandleSuccess`（lockStateWithRWSet 之后）
- **`impl.go:879`**: 改为写入链下 `localPatches`（供 leader 本地 chainNextSim 查询用），不写 `onChainPatches`

### 修复 2：Phase 1b 不 merge，独立消费

- **`retry_scheduler.go:1260-1264`**: `SetChainPatch` 时传的应该是 `consumedPatches`（[]common.Hash）对应的原始 Patch 列表，而非 merge 后的单一 RWSet
- 或者：下游侧 `ReadOnChainPatch` 改为支持多上游查询（已支持——遍历 `UpstreamTxList`），此时 ChainPatch 保持为自身 WriteSet 不变

## 影响评估

| 指标 | 影响 |
|------|------|
| 链式交易正确性 | 高——脏 Patch 可能被下游误用 |
| onChainPatches 内存泄漏 | 中——验证失败的 Patch 残留 |
| Phase 1b 补丁命中率 | 中——merge 后下游无法读到原始 Patch |
| 非链式交易 | 无影响（ChainPatch= nil 时跳过） |
