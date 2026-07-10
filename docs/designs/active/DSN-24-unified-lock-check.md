# [DSN-24] 统一锁检查 — TLV + SLM + ChainPatch 三层联合仲裁

> **版本**：v1（2026-07-08）
> **状态**：设计阶段
> **关联文档**：`DSN-23-patchpool-dag-design.md`（DAG 多 Patch）、`DSN-22-retrycommit-lock-order-fix.md`（三阶段 RetryCommit）

---

## 1. Problem Statement

### 1.1 两类锁各行其道

当前代码中有**两套独立锁系统**：

| 锁系统 | 别名 | 管理位置 | 作用域 |
|:-------|:-----|:---------|:-------|
| stateLockManager | **SLM** | `state_lock_impl.go` | 所有节点（链上/链下） |
| TempLockView | **TLV** | `temp_lock_view.go` | Leader 侧（链下） |

SLM 锁住 key 后，TLV 不会感知。反之亦然。两套锁保护的是**同一批 key**，但彼此不知道对方的存在。

### 1.2 当前锁检查路径碎片化

共有 **11 个锁检查点**散落在 3 个文件中：

| # | 位置 | 检查方式 | 备注 |
|:-:|:-----|:---------|:------|
| 1 | `retry_scheduler.go:539,546` | `currentStateDB.CheckLock` | OnBlockCommitted 重检查 |
| 2 | `retry_scheduler.go:628,635` | `currentStateDB.CheckLock` | LockWait 出池 |
| 3 | `retry_scheduler.go:1211,1216` | `stateDB.CheckLock` | RetryCommit Phase 2 |
| 4 | `simulator.go:933-935` | `db.GetState` → `IsLockConflictDBErr` | GetState Step 2 |
| 5 | `simulator.go:1006-1010` | `db.GetState` → `IsLockConflictDBErr` | SetState Step 2 |
| 6 | `verify.go:318,343` | `stateDB.GetState` → ErrLockConflict_OnChain | VerifySimulation |
| 7 | `verify.go:698,711` | `stateDB.GetState` → ErrLockConflict_OnChain | VerifySimulation |
| 8 | `verify.go:441` | `lockStateWithExecution` | VerifySimulation |
| 9 | `verify.go:502` | `lockStateWithRWSet` | VerifySimulation |

**没有一处检查 TLV**。所有检查点都只查 SLM — 导致 TLV 已经预约的 key 被误判为冲突。

### 1.3 实验数据

22,590 次 `resimulation failed: state is locked by other tx on chain` — 全部来自 TLV 已预约但 SLM 锁未释放的场景。

---

## 2. 设计方案

### 2.0 链上 vs 链下的仲裁体系差异

**链下（模拟执行 / RetryCommit）** — `IsKeyAvailable` 函数：

- TLV（调度层预约）+ SLM（状态层锁定）→ ChainPatch 补救
- 用于 `simulator.go:GetState/SetState`、`retry_scheduler.go:CheckLock`

**链上（VerifySimulation）** — 保持当前逻辑不变：

- 只查 **SLM**（链上锁存在）
- 通过 `ReadOnChainPatch` 查询 `onChainPatches` 做一致性检查（非补救）
- TLV 在链上不存在（TLV 是 leader 侧调度器）
- `onChainPatches` 已通过监听 SimTx 多播写入，全局共享

| 上下文 | 检查对象 | 补救来源 | 对应位置 |
|:-------|:---------|:---------|:---------|
| 链下模拟 | TLV + SLM | **ChainPatch** (SimState/patches) | `simulator.go` |
| 链下 RetryCommit | TLV + SLM | **PatchPool** (FindCoveringSet) | `retry_scheduler.go` |
| 链上 VerifySimulation | SLM 只 | **onChainPatches** (ReadOnChainPatch) | `verify.go` |

### 2.1 核心思路：三层联合仲裁（链下）

锁检查统一为一个函数，在任何需要判断 key 是否可用时调用：

```
      查询 key K 是否可访问
              │
     ┌────────┴────────┐
     │                  │
  TLV 检查        SLM 检查
  (其他交易          (stateLockManager
   预约了K?)          锁了K?)
     │                  │
     └────────┬────────┘
              │
         任一锁了?
         ├── 否 → 可用 ✅
         │
         └── 是 → ChainPatch 补救
              ├── Patch 覆盖 K → 从 Patch 读值 ✅
              └── 未覆盖 → 冲突 ❌
```

### 2.2 统一仲裁函数

```go
// IsKeyAvailable 检查 key K 在当前 tx 的上下文中是否可访问。
// 三层仲裁：TLV 预约 → SLM 锁 → ChainPatch 补救。
// 返回 (available bool, patchedValue *common.Hash, err error)
//
// patchedValue != nil 表示 ChainPatch 提供了预期值，调用方应使用此值。
// err != nil 表示 key 被锁且 Patch 未覆盖 → 调用方应报锁冲突。
func (sim *Simulator) IsKeyAvailable(
    txHash common.Hash,
    db api.StateDB,
    address common.Address,
    key common.Hash,
) (bool, *common.Hash, error) {

    // Step 1: 检查 TLV — 是否有其他交易预约了这个 key
    if sim.tempLockView.HasConflict(txHash, address, key) {
        goto checkPatch
    }

    // Step 2: 检查 SLM — stateLockManager 是否锁了这个 key
    lockKey := api.FormKey(address, key)
    if err := db.Lockable(lockKey, txHash); err == nil {
        return true, nil, nil  // 未锁 ✅
    }

checkPatch:
    // Step 3: ChainPatch 补救 — 看该交易消费的 Patch 能否覆盖
    val, found := sim.readChainPatch(txHash, address, key)
    if found {
        return true, &val, nil  // Patch 覆盖 ✅
    }
    return false, nil, api.ErrLockConflict  // 未覆盖 ❌
}
```

### 2.3 在 GetState/SetState 中使用

```go
// GetState 改后
func (sim *Simulator) GetState(db, txHash, address, key) {
    // Step 1: ChainPatch 直接查（快速路径）
    // ... 现有代码不变 ...

    // Step 2: 统一锁仲裁
    available, patchedVal, err := sim.IsKeyAvailable(txHash, db, address, key)
    if err != nil {
        callState.LockedByOtherTx = err  // 冲突
        return
    }
    if patchedVal != nil {
        val = *patchedVal  // Patch 提供了值
    } else {
        val, err = db.GetStateWithoutLock(txHash, address, key)  // 无锁读
    }
    // ... 继续 ...
}
```

### 2.4 被改造的锁检查点

所有 11 个点都用 `IsKeyAvailable` 替代：

| # | 现在 | 改为 |
|:-:|:-----|:------|
| 1,2 | `currentStateDB.CheckLock` | `IsKeyAvailable`（传入 TLV + SLM + ChainPatch）|
| 3 | `stateDB.CheckLock` | `IsKeyAvailable` |
| 4,5 | `db.GetState + IsLockConflictDBErr` | `IsKeyAvailable` 前置判断 |
| 6,7 | `stateDB.GetState + ErrLockConflict` | VerifySimulation 中通过 `ReadOnChainPatch` 已有熔断逻辑，保持 |
| 8,9 | `lockStateWithExecution/RWSet` | VerifySimulation 的上锁操作（写入 SLM），不是检查—保持不变 |

### 2.5 关键细节

#### TLV 的「冲突」语义

`HasConflict(txHash, addr, key)` 检查的是**其他交易**是否预约了 key，不是自己：

```go
func (v *TempLockView) HasConflict(txHash common.Hash, address common.Address, key common.Hash) bool {
    v.mu.RLock()
    defer v.mu.RUnlock()
    lockKey := api.FormKey(address, key)
    if entry, held := v.tempWriteLocks[lockKey]; held {
        if bytes.Compare(entry.Holder.Bytes(), txHash.Bytes()) != 0 {
            return true  // 其他交易写锁了此 key
        }
        return false  // 自己持有，不算冲突
    }
    // 读锁检查类似
    return false
}
```

#### TLV 的 ClearWounded 不应替代 TempLock

`ClearWounded` 只清 wounded 标记，不解锁。PatchPool 命中后的流程不变：

```
RetryCommit P2b: PatchPool 命中
  → ClearWounded (只清 wounded 标记)
  → Locked=true
  → TempLock 仍然持有 ✅  ← GetState 能查到
```

#### `readChainPatch` 需要能从 SimState 和 patches 两处读

当前 `readChainPatch` 只查 `rs.patches`。需要也查 `SimState.ChainPatch` 作为兜底：

```go
func (sim *Simulator) readChainPatch(txHash common.Hash, address common.Address, key common.Hash) (common.Hash, bool) {
    // 优先查 SimState.ChainPatch（SetChainPatch 写入的位置）
    simState, ok := sim.GetSimState(txHash)
    if ok && simState != nil && simState.ChainPatch != nil {
        if addrState, ok := simState.ChainPatch.WriteState.State[address]; ok {
            if val, exists := addrState[key]; exists {
                return val, true
            }
        }
    }
    // 其次查 patches（链式依赖链的完整路径）
    rs := sim.sscService.retryScheduler
    chainPatchRef := rs.GetChainPatchRef(txHash)
    if chainPatchRef != nil {
        return rs.readPatchChain(chainPatchRef.TxHash, chainPatchRef.SimulationNum, address, key)
    }
    return common.Hash{}, false
}
```

---

## 3. 文件变更清单

| 文件 | 变更 | 行数 | 复杂度 |
|:-----|:------|:----:|:------:|
| `ssc/simulator.go` — 新增 `IsKeyAvailable` | 三层仲裁函数 | ~25 | 中等 |
| `ssc/simulator.go` — 新增 `readChainPatch` | 统一 Patch 读取 | ~15 | 简单 |
| `ssc/simulator.go` — `GetState` Step 2 | 替换为 `IsKeyAvailable` | ~10 | 简单 |
| `ssc/simulator.go` — `SetState` Step 2 | 替换为 `IsKeyAvailable` | ~10 | 简单 |
| `ssc/temp_lock_view.go` — 新增 `HasConflict` | per-key 冲突查询 | ~15 | 简单 |
| `ssc/retry_scheduler.go` — `CheckLock` 调用点 x6 | 改为 `IsKeyAvailable` | ~30 | 简单 |
| `ssc/api/state_lock.go` — 新增 `GetStateWithoutLock` | 无锁读取接口 | ~5 | 简单 |
| `ssc/api/sscs.go` — 接口扩展 | 同上 | ~2 | 简单 |

**合计：~112 行变更，4 文件，复杂度低。**

---

## 4. 决策记录

| ID | 决策项 | 结论 | 理由 |
|:---|:-------|:------|:------|
| D24-01 | 仲裁顺序 | TLV → SLM → ChainPatch | TLV 是调度层预约，SLM 是状态层锁定，Patch 是补救 |
| D24-02 | TLV 冲突定义 | **其他交易**占锁才算冲突，自己持有不算 | retryTx 自己的 TempLock 应该跳过锁检查 |
| D24-03 | Patch 读取源 | SimState.ChainPatch + rs.patches 两处 | patches 存链式依赖链，ChainPatch 存合并值 |
| D24-04 | `ClearWounded` | 不改——只清标记不解锁 | TempLock 在模拟期间仍然持有 |
| D24-05 | VerifySimulation 锁检查 | 不动 | VerifySimulation 的锁检查是链上验证逻辑，与链下仲裁不同 |
