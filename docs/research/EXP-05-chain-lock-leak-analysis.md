# EXP-05: 链上锁泄露分析报告

## 1. 问题现象

```
LOCK_STALE: lock held more than 10 blocks, possible leak
txHash: 0x4d8a1394c4946d
lockKey: 0x0279daca51618678cf262D4194D6D8EA28f292f3:0x94ef354b9efc89deed49b2b9fd35f2a86980309a7018045d0d0f073bed553f47
heldBlocks: 11
caller: ssc/state_lock_impl.go:508
```

锁在全局 `stateLockManager.lockedStates` 中存在超过 10 个区块未被释放。

---

## 2. 链上锁完整生命周期

### 2.1 相关文件

| 文件 | 角色 |
|------|------|
| `ssc/state_lock_impl.go` | stateLockManager 全局锁管理 + handleLockCommit 快照/泄露检测 |
| `ssc/state_locker.go` | 每个块执行的 stateLocker 实例（pendingStates + pendingUnlocks） |
| `ssc/state_lock_entry.go` | journal 回滚机制 |
| `ssc/verify.go` | VerifySimulation — 链上验证（锁获取入口） |
| `ssc/committer.go` | CommitOrRollbackWithProof — CR 执行（锁释放入口） |
| `ssc/impl.go` | closeTransaction 清理 |
| `core/state/statedb.go` | stateDB.Commit() → locker.Commit(newRoot) |
| `core/blockchain_impl.go:1885` | 块处理入口，GetLockerAt + state.New |

### 2.2 锁获取路径

**两类锁获取路径，最终都经过 `stateDB.SetAndLockState` → `locker.Lock()` → pendingStates：**

**路径 1 — `lockStateWithRWSet`（成功路径）**
```
verify.go:521-523  (条件: 无冲突，验证通过)
  └─ for callState := range simulation.CallStates:
      └─ lockStateWithRWSet(txHash, callState, stateDB)      [verify.go:794-803]
          └─ RWSet.WriteState 中的每对 (addr, key) → stateDB.SetAndLockState(...)
              └─ locker.Lock(txHash, callIndex, key, value)  [state_locker.go:208-231]
                  └─ 写入 pendingStates.addLockedState + journal
              └─ 不发 CommitTx/RollbackTx
  └─ SendCommitVote(...)
```

**路径 2 — `lockStateWithExecution`（冲突重执行路径）**
```
verify.go:417  (条件: conflictCallStateIndex >= 0)
  └─ v.lockStateWithExecution(...)                            [verify.go:736-791]
      └─ sscvm.Call(sender, addr, input, gas, value)
          └─ 合约内调 SetAndLockState → locker.Lock → pendingStates
  └─ 执行失败 ⇒ RollbackTx(txHash) 释放                          [verify.go:422]
  └─ 执行成功 ⇒ 锁应持有，等待 CR 释放
      ├─ 但 retry 超限 ⇒ RollbackTx(446) ✓ (放弃此tx，释放)
      └─ retry 未超限 ⇒ RollbackTx(452) ✗ **bug**——执行成功却主动释放
```

**关键**：`lockStateWithExecution` 是**边执行边上锁**。成功时锁应留在 `pendingStates` → merge 到全局 → 随该块提交进入 `stateLockManager.lockedStates`，后续重试 SimTx 在新块的 baseSnapshot 中能看到该锁，形成**连续锁持有语义**。第 452 行的主动释放破坏了这一语义。

> ⚠️ `verifyExecuteForCallState` (verify.go:385) 仅验证合约执行结果与模拟结果是否一致，**不调 `SetAndLockState`，不上锁**。但它执行失败后的 `RollbackTx` (verify.go:396) 仍会调 `pendingUnlocks.addTx(txHash)` → applyTo 可能误释放全局 `callIndex2lockedState[txHash]`（如果该 txHash 有来自前序 SimTx 块的残留条目）。

### 2.3 锁释放路径

两条路径都经过同一个 `applyTo` 机制——它们在 CR 块 `locker.Commit()` 到达前将 `txHash` 记入 `pendingUnlocks`。

**路径 A — Commit 释放：**
```
committer.go:76  (commitProof.Type == api.Commit)
  └─ stateDB.CommitTx(txHash)                              [state_locker.go:357-395]
      └─ 1. 遍历 pendingStates.callIndex2lockedState[txHash] → unlock
      └─ 2. pendingUnlocks.addTx(txHash)                  // 只记录 txHash
      └─ 3. tmpFinishedTxs[txHash] = true
  └─ (块提交) stateDB.Commit() → locker.Commit(newRoot)  [state_locker.go:450-532]
      └─ Phase 1: merge pendingStates → 全局 stateLockManager
      └─ Phase 2: pendingUnlocks.applyTo(stateLockManager)  [state_locker.go:561-614]
          └─ 按 txHash 查找全局 callIndex2lockedState[txHash]
          └─ 删除全局 lockedStates 中的对应 key
          └─ 删除全局 callIndex2lockedState[txHash]
      └─ handleLockCommit(newRoot) → 快照 + 泄露检测
```

**路径 B — Rollback 释放：**
```
committer.go:94  (commitProof.Type == api.Rollback)
  └─ stateDB.RollbackTx(txHash)                            [state_locker.go:398-445]
      └─ 1. 遍历 pendingStates.callIndex2lockedState[txHash] → unlock + 恢复 stateDB 旧值
      └─ 2. pendingUnlocks.addTx(txHash)                  // 只记录 txHash
      └─ 3. tmpFinishedTxs[txHash] = false
  └─ (块提交) stateDB.Commit() → locker.Commit(newRoot)
      └─ Phase 1: merge pendingStates → 全局 stateLockManager
      └─ Phase 2: pendingUnlocks.applyTo(stateLockManager)  // 同上，删除全局对应条目
      └─ handleLockCommit(newRoot)
```

**关键**：两条路径都依赖**全局 `callIndex2lockedState[txHash]`**（SimTx 块 merge 时写入），而不是当前 locker 的 `pendingStates`。当前 CR 块的 `pendingStates` 对这个 txHash 是空的，所以 path A 的 unlock 循环无操作，但 `addTx(txHash)` 被执行——`applyTo` 负责真正的全局清理。

### 2.4 时序

```
Block N     (SimTx):  VerifySimulation → Lock() → pendingStates
                       locker.Commit(N's newRoot) merge → 全局 lockedStates + callIndex2lockedState[txHash]
                       锁进入全局，pendingUnlocks 为空
Block N+1..K        : 锁在全局 lockedStates 中持续存在
                       handleLockCommit 每次块提交都检测 → LOCK_STALE 当 heldBlocks > 10
Block N+K   (CR):    CommitOrRollbackWithProof → CommitTx → pendingUnlocks.addTx(txHash)
                       locker.Commit(CR's newRoot) → applyTo → 删除全局 lockedStates 条目
```

---

## 3. 已定位的问题

### 3.1 Issue-A: `lockStateWithRWSet` 静默吞错误

**文件**: `verify.go:794-803`

```go
func (v *Verifier) lockStateWithRWSet(txHash common.Hash, callState *api.CXTCallState, stateDB api.StateDB) {
    for address, stateMap := range callState.RWSet.WriteState.State {
        for key, value := range stateMap {
            stateDB.SetAndLockState(txHash, callState.CallIndex, address, key, value) // ← 错误丢弃
        }
    }
}
```

**问题**：`SetAndLockState` 返回 error（如 `ErrLockConflict_OnChain`），但被静默丢弃。如果部分 key 上锁成功、部分失败 → 已成功的锁仍进入 `pendingStates` → merge 到全局 → 永不被释放。

**修复建议**：`lockStateWithRWSet` 应返回 error，`VerifySimulation` 的成功路径应检查该 error 并调用 `RollbackTx(txHash)` 回滚部分锁。

### 3.2 Issue-B: VerifySimulation 的所有 error 路径是否释放锁

**文件**: `verify.go:391-546`

已核实的各 error 路径：

| 路径 | 行号 | 是否调 RollbackTx | 结果 |
|------|------|-------------------|------|
| execErr != nil | 396 | ⚠️ `stateDB.RollbackTx(txHash)` | **误调用**——`verifyExecuteForCallState` 不上锁，RollbackTx 为空操作；但 `addTx(txHash)` 导致 applyTo 可能误释放全局 `callIndex2lockedState[txHash]` |
| conflictCallStateIndex + lockExec err | 419-422 | ✅ `RevertToSnapshot + RollbackTx` | 正确释放（执行失败） |
| conflictCallStateIndex + retry exceeded | 443-448 | ✅ `stateDB.RollbackTx(txHash)` | 正确释放（放弃此tx） |
| conflictCallStateIndex + retry not exceeded | 452 | ✗ `stateDB.RollbackTx(txHash)` | **bug**——`lockStateWithExecution` 执行成功，锁应持有，不应主动释放 |
| conflictLockKeys > 0 + retry not exceeded | 494 | ✅ `stateDB.RollbackTx(txHash)` | 正确释放（此路径无 lockStateWithExecution 上锁） |
| ✅ **success** (无冲突) | 516-546 | ❌ 不调 CommitTx/RollbackTx | 锁 goto 全局，依赖 CR |

**注意**：execErr 路径的 RollbackTx 是**历史遗留**——注释声称 `sscvm.Call` 在 `verifyExecuteForCallState` 内调了 `SetAndLockState`，但实际 **`ExecutionVerify` 模式不上锁**。这行 RollbackTx 应当移除。

**结论**：Success 路径是唯一不主动释放锁的路径。这是**设计如此**（锁由 CR 的 applyTo 释放），但如果 CR 不处理则泄露。

### 3.3 Issue-C: `IsTxFinished` 提前返回导致跳过 CommitTx

**文件**: `committer.go:56-59`

```go
func (c *Committer) CommitOrRollbackWithProof(commitProofBytes []byte, stateDB api.StateDB, blockNum uint64) error {
    txHash := commitProof.TxHash
    if c.state.IsTxFinished(txHash) {
        utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).Msg("cxt has committed or rollback")
        return nil  // ← 不调 CommitTx！pendingUnlocks 为空！
    }
    // ...
    err := stateDB.CommitTx(txHash)
```

**问题**：`IsTxFinished` 检查 `TxState.Closed`（`impl.go:306-314`）。如果 `closeTransaction` 因为其他原因（`PoolTimeout`、`ExecutionFailed` 等）提前被调用，`tx.Closed = true`。后续 CR 块到达时跳过 `CommitTx` → `pendingUnlocks` 为空 → `applyTo` 不做任何事 → 锁永驻全局 `lockedStates`。

**触发场景**：
1. SimTx 块提交 → 锁进入全局
2. 在 CR 块到达前，`closeTransaction` 被其他路径调用（如 timeout）
3. `tx.Closed = true`
4. CR 块到达 → `IsTxFinished` 返回 true → 跳过 CommitTx → 锁泄露

### 3.4 Issue-D: CR 块的 newRoot 快照误释放

**文件**: `state_locker.go:465-496`（merge 阶段）

在 CR 块的 `locker.Commit()` 中，merge 阶段运行**在 applyTo 之前**：
```go
// Phase 1: merge pendingStates → 全局
for key, pendingState := range s.pendingStates.lockedStates {
    s.stateLockManager.lockedStates.lockedStates[key] = &lockedState{...}
}
// Phase 2: applyTo
s.pendingUnlocks.applyTo(s.stateLockManager)
```

如果 CR 执行期间有其他合约**重新获取了同一 key 的锁**（如跨分片合约回调），CR 块 `pendingStates` 中有该 key 的条目。merge 会**覆盖全局 `lockedStates[key]`** 中的 `lockedBy`。然后 applyTo 只按 `txHash` 查找 `callIndex2lockedState` → 如果合约锁的 txHash 与 CR 的 txHash 不同 → 锁不会被 applyTo 删除 → 但 `lockedBy` 已被覆盖 → 泄露。

**概率**：低，但需要核验 CR 执行期间是否存在合约回调获取锁。

### 3.5 Issue-E: `GetLockerAt` 当前 root 情况下 baseSnapshot 浅引用

**文件**: `state_lock_impl.go:575-577`

```go
baseSnapshot: &lockedStatesSnapshot{
    lockedStates:  s.lockedStates.lockedStates,   // 直接引用全局 map
    rlockedStates: s.lockedStates.rlockedStates,
    finishedTxs:   s.finishedTxs,
},
```

**问题**：`baseSnapshot` 直接引用全局 `stateLockManager.lockedStates` 的 map，不是深拷贝。如果某块中有多个交易并发处理（理论不应该，但实现中有没有可能的通信回调？），一个交易写全局 map 会影响另一个交易的 baseSnapshot 视图。

### 3.6 Issue-F: `clear()` 与 `callIndex2lockedState` 不同步

**文件**: `state_lock_impl.go:90-96`

```go
func (s *lockedStates) clear() {
    for key, state := range s.lockedStates {
        if bytes.Compare(state.lockedBy.Bytes(), common.Hash{}.Bytes()) == 0 {
            delete(s.lockedStates, key)   // 只删 lockedStates
            // ← callIndex2lockedState 未同步清理
        }
    }
}
```

**问题**：`clear()` 从 `lockedStates` 删除 `lockedBy==0` 的条目，但**不同步清理 `callIndex2lockedState`**。虽然正常流程中 `lockedBy` 不会被其他路径清零（只有 `deleteLockedState` 显式清零再删），但如果未来某条路径只清了 `lockedBy` 而未调 `deleteLockedState`，会导致 `callIndex2lockedState` 残留。

---

## 4. 日志证据分析

```
LOCK_STALE: heldBlocks=11
```

heldBlocks = 11 说明锁从第一个记录块到现在已经过了 11 个区块。这个阈值较小——CR 提交收集投票 + 打包入块可能就需要数块。需要区分：

- **如果 CR 块已提交但锁还在** → 优先排查 Issue-C（`IsTxFinished` 提前 return）
- **如果 CR 块尚未提交** → 正常行为，阈值需调大，或优化 CR 提交速度
- **如果 CR 块提交失败**（submit error）→ Issue-B 的补集（锁进入全局后再无释放路径）

---

## 5. 修复优先级

| 优先级 | Issue | 影响 | 修复难度 |
|--------|-------|------|---------|
| P0 | Issue-C: `IsTxFinished` 导致跳过 CommitTx | 锁永驻泄露 | 低（committer.go 中 CheckTx 后补调 pendingUnlocks 或强制清理） |
| P1 | Issue-A: `lockStateWithRWSet` 静默吞错误 | 部分锁泄露 | 低（返回 error + 成功路径加 RollbackTx） |
| P2 | Issue-D: CR merge 覆盖锁状态 | 低概率 | 中（需要更严格地隔离 CR 的 pendingStates） |
| P3 | Issue-F: clear() 与 callIndex2lockedState 不同步 | 潜在风险 | 低（clear 中同步清理） |
| P4 | Issue-E: baseSnapshot 浅引用 | 安全风险 | 低（current root 场景改深拷贝） |

---

## 6. 快速修复建议

### Fix 1: committer.go — `IsTxFinished` 后补强制解锁

```go
// committer.go:56-59 — 替换 return nil 为强制解锁
if c.state.IsTxFinished(txHash) {
    utils.SSCLogger().Warn().Str("txHash", txHash.Hex()).Msg("tx already finished, force cleanup locks")
    // 强制从 stateLockManager 清理此 txHash 的锁
    // 调用 stateDB 的 locker 获取 stateLockManager 引用
    if locker, ok := stateDB.(interface{ Locker() *stateLockManager }); ok {
        locker.ForceUnlockTx(txHash)
    }
    return nil
}
```

需要在 `stateLockManager` 上添加 `ForceUnlockTx(txHash)` 方法，直接从全局 `callIndex2lockedState` 和 `lockedStates` 中删除。

### Fix 2: verify.go — `lockStateWithRWSet` 返回 error

```go
// verify.go:794
func (v *Verifier) lockStateWithRWSet(txHash common.Hash, callState *api.CXTCallState, stateDB api.StateDB) error {
    for address, stateMap := range callState.RWSet.WriteState.State {
        for key, value := range stateMap {
            if err := stateDB.SetAndLockState(txHash, callState.CallIndex, address, key, value); err != nil {
                return err
            }
        }
    }
    return nil
}
```

成功路径（516-546）：
```go
if err := v.lockStateWithRWSet(txHash, callState, stateDB); err != nil {
    stateDB.RollbackTx(txHash)
    // 发 Rollback vote ...
    return
}
```

### Fix 3: state_lock_impl.go — `clear()` 同步清理 callIndex2lockedState

```go
func (s *lockedStates) clear() {
    for key, state := range s.lockedStates {
        if bytes.Compare(state.lockedBy.Bytes(), common.Hash{}.Bytes()) == 0 {
            delete(s.lockedStates, key)
            // 同步清理 callIndex2lockedState
            for txHash, callIndexMap := range s.callIndex2lockedState {
                for callIndex, lockKeyMap := range callIndexMap {
                    delete(lockKeyMap, key)
                    if len(lockKeyMap) == 0 {
                        delete(callIndexMap, callIndex)
                    }
                }
                if len(callIndexMap) == 0 {
                    delete(s.callIndex2lockedState, txHash)
                }
            }
        }
    }
}
```

---

## 7. 总结

链上锁释放机制的正确性依赖一条**全局 `callIndex2lockedState[txHash]` 链**：`Lock()` → `pendingStates.callIndex2lockedState[txHash]` → merge 到全局 → CR 块中 `applyTo` 通过 `callIndex2lockedState[txHash]` 找到锁并删除。

最脆弱的环节是 **P0 (Issue-C)**：`IsTxFinished` 提前 return 会导致整条释放链断裂，锁永驻全局。这应该优先修复。其次是 **P1 (Issue-A)** 修复部分锁残留。
