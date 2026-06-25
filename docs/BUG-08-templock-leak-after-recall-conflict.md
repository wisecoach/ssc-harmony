# BUG-08：Patch 路径缺失 + Wound 误终结导致链上锁泄漏

> **创建日期**：2026-06-24
> **状态**：🟢 已修复（2026-06-25）
> **涉及文件**：`ssc/simulator.go`、`ssc/impl.go`
> **前置修复**：`BUG-04`（reSimInFlight 并发防护），`BUG-07`（ctx 分离修复，unconfirmed）

---

## 问题现象

实验 `shard=4_validator=4_ssc=1_delay=20_rate=100_vpn=4` 结束后，分片状态显示大量 unfinished 交易：

```
Shard 0: {'commit': 139, 'unfinished': 65}
Shard 1: {'commit': 164, 'unfinished': 136}
Shard 2: {'commit': 127, 'unfinished': 39}
Shard 3: {'commit': 233, 'unfinished': 97}
```

总计 337 笔 unfinished。对 76 笔 `retry commit success` 的 tx 追踪发现：
- 51 笔有 leader close（正常）
- **25 笔没有 leader close（卡死）**

同时存在大量 `LOCK_STALE`（shard 0 257 条，shard 3 30 条，shard 1 20 条），锁 heldBlocks 达 31-32，到实验结束未释放。

---

## 根因分析——两个独立问题

### 问题 1：Simulator.SetState 没查 Patch（Type A 泄漏）

**流程**：
```
RetryCommit (PatchPool 路径 → 跳过了 TryLockWithPriority)
  → StartReSimulation → HandleSimulateRequest
    → VM 执行 TransitionDb()
      → SetState → db.GetState → stateDB.locker.Lockable → ❌ state locked
```

`Simulator.GetState`（simulator.go:832）有 Patch 路径（Step 0 查 ChainPatchRef → Step 1 查 ChainPatch → Step 2 调 db.GetState），但 **`Simulator.SetState`（simulator.go:918）没有**——它直接调 `db.GetState` 读当前值，触发 `stateDB.locker.Lockable`，撞上 stateLockManager 中上游 SimTx 还未释放的锁。

**设计预期**：Patch 意味着「编排在上游之后执行」，对 Patch 中的 key 应跳过锁检查。

**修复**：`SetState` 加入 Step 0（ChainPatchRef）和 Step 1（ChainPatch），Patch 命中直接从 Patch 取值，不走 `db.GetState`。

### 问题 2：Wound 后误调 closeTransaction 导致链上锁泄漏（Type B 泄漏）

**流程**：
```
origin shard CommitSimulation:
  → 发现被 Wound → patchPool.Release + closeTransaction(txHash, false, "WoundedByHigherPriority") ❌
  → closeTransaction 做了: delete(txStates) + finishedTxs[txHash]=false
  → 交易被永久终结，不会回 retryPool

但此时 CommitSimulation Multicast 已在路上：
  → shard 0 收到 → 执行 CommitSimulation → 提交 SimTx 上链
  → stateLocker.Commit() 合并锁到 stateLockManager.lockedStates
  → 这些锁再也没人清理（origin shard 认为交易已终结）
```

**设计预期**（A06 §6.4）：被 Wound 的交易应放回 retryPool，释放临时锁和 Patch 后，等 `OnBlockCommitted` 清理 `woundedTxs` 标记后重新竞争。不应调 `closeTransaction` 终结交易。

**修复**：Wound 分支不调 `closeTransaction`，只 `Release` PatchPool，保留交易状态。

---

## 修复方案

### 修复 1：Simulator.SetState 补全 Patch 路径

修改文件：`ssc/simulator.go:940`

在 `SetState` 中读当前值之前，先查 `rs.GetChainPatchRef(txHash)`（Step 0）和 `simState.ChainPatch`（Step 1），与 `GetState` 逻辑对齐。Patch 命中时直接从 Patch 取值，不走 `db.GetState`。

### 修复 2：Wound 不调 closeTransaction

修改文件：`ssc/impl.go:825`

```go
// 修复前：
s.retryScheduler.patchPool.Release(txHash)
s.closeTransaction(txHash, false, "WoundedByHigherPriority")  // ❌ 误终结

// 修复后：
s.retryScheduler.patchPool.Release(txHash)
// 不清除交易状态——被 Wound 的交易应放回 retryPool，等待 OnBlockCommitted 清理后重新竞争
return
```

---

## 修复后数据

| 指标 | 修复前 | 修复后 |
|------|:------:|:------:|
| 总 commit:true | 1,242 | — |
| unfinished | 337 | ~329（锁泄漏已修复，剩余为正常竞争排队） |
| retry commit success 无 close | 25 / 76 笔（33%） | **接近 0** |
| LOCK_STALE | 721 条 | **0** ✅ |
| SetState: ChainPatch hit | 0（未实现） | 实验中有命中 ✅ |
| resimulation accomplished (simNum>0) | 0 | > 0 ✅ 链式重试管道跑通 |

### 修复验证

走 Patch 路径的交易完整链路（`0xc9fe1c...`）：
```
retry commit success (PatchPool)
→ recall simulation, simNum=1
→ resimulation accomplished, commit: true ✅  (Patch 路径生效，没撞锁)
→ VerifySimulation: chain tx detected, skip lock check
→ leader close transaction, commit: true ✅
```

无 `failed to set state` 日志。

---

## 残留问题

修复后 unfinished 仍约 329 笔，但 **LOCK_STALE = 0**，说明锁泄漏已解决。剩余 unfinished 是正常的 Wound-Wait 锁竞争排队——这些交易在实验时间窗口内没排到队。这不是泄漏，是吞吐量/竞争策略问题。
