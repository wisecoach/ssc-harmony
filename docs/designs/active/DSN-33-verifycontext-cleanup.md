# DSN-33: VerifyContext 清理 + GetResult 子上下文化

> 对应代码变更：并行验证 Phase 1.5 引入子上下文后，`executionVerifyContexts` 中的 `CurrentState`/`CallFrame` 已成为僵尸字段。

## 1. 动机

### 1.1 现状

DSN-32 的并行验证改造引入了 `callVerifyContext` 存储每个 CallState 独立的验证状态。但 `ExecutionVerifyContext` 仍保留了三个不再活跃的字段：

| 字段 | 当前状态 | 问题 |
|:----|:---------|:-----|
| `Simulation` | ✅ 存活 | 只读共享 |
| `CallStateMap` | ✅ 存活 | 只读，回调初始化子上下文时用 |
| `CurrentState` | 💀 移除 | 全部走子上下文的 `currentState` |
| `CallFrame` | 💀 移除 | 全部走子上下文的 `callFrame` |
| `DependentResults` | ⚠️ 保留 | LockExecution 初始化子上下文时用 |

此外 `executionVerifyContexts` 仍用 `map + RWMutex` 维护，存储方式不一致。

### 1.2 GetResult 的数据竞争隐患

`GetResult(txHash)` 从根上下文的 `CallFrame.PC` 读取结果索引。并行验证下不同 goroutine 同时写入不同的子上下文，但 `GetResult` 读的是根上的 `CallFrame.PC`，读到错误的 PC 值。

### 1.3 存储结构的三次演化

| 阶段 | subCtx 存储 | Cleanup 复杂度 |
|:----|:-----------|:--------------|
| **初始**（平层） | `sync.Map` key=`callCtxKey{txHash, callIndex}` | O(N) Range 遍历 |
| **嵌套**（独立） | `Verifier.subVerifyCtx` key=`txHash` → `*sync.Map(key=callIndex)` | O(1) 外层 Delete |
| **最终**（内嵌）⭐ | `ExecutionVerifyContext.SubCtx` key=`callIndex` | O(1) 随根 Delete |


## 2. 设计方案

### 2.1 核心变更

```go
// 最终的 ExecutionVerifyContext — SubCtx 内嵌在根上下文中
type ExecutionVerifyContext struct {
    Simulation       *CXTSimulation
    CallStateMap     map[string]*CXTCallState
    SubCtx           sync.Map          // key: api.CallIndex → *callVerifyContext
    DependentResults []*CXTCallSSCResult  // 仅 LockExecution 初始化用
}

// Verifier 中的存储 — 只有两层，无独立 subVerifyCtx
executionVerifyContexts sync.Map  // key: common.Hash → *ExecutionVerifyContext（含内嵌 SubCtx）
```

**辅助方法**（收敛所有访问，通过根上下文路由）：

```go
func (v *Verifier) loadSubCtx(txHash common.Hash, callIndex api.CallIndex) *callVerifyContext {
    root := v.getVerifyContext(txHash)
    if root == nil { return nil }
    val, ok := root.SubCtx.Load(callIndex)
    if !ok { return nil }
    return val.(*callVerifyContext)
}

func (v *Verifier) storeSubCtx(txHash common.Hash, callIndex api.CallIndex, ctx *callVerifyContext) {
    root := v.getVerifyContext(txHash)
    if root != nil {
        root.SubCtx.Store(callIndex, ctx)
    }
}
```

**清理清单：**

| 移除项 | 替换 | 影响 |
|:-------|:-----|:-----|
| `verifyCtxLock` RWMutex | `sync.Map` 原子操作 | 所有存储访问无锁 |
| `executionVerifyContexts` map | `sync.Map` | 同上 |
| `ExecutionVerifyContext.CurrentState` | 子上下文 `currentState` | 无 |
| `ExecutionVerifyContext.CallFrame` | 子上下文 `callFrame` | 无 |
| `Verifier.subVerifyCtx` | `ExecutionVerifyContext.SubCtx` | 少一层嵌套，Cleanup 随根自动释放 |
| 所有回调 fallback 路径 | 删除（改用 `return error`） | 所有路径（含 LockExecution）都创建子上下文，fallback 不可达 |
| `callCtxKey` 复合 key 类型 | 删除 | 不再需要 |

**`Cleanup`：** 仅 `executionVerifyContexts.Delete(txHash)`。内嵌 `SubCtx` 随根自动释放，无需额外清理。

### 2.2 GetResult 改造

`GetResult` 改为子上下文唯一路径，无 fallback：

```go
func (v *Verifier) GetResult(txHash common.Hash, callIndex api.CallIndex) (...) {
    subCtx := v.loadSubCtx(txHash, callIndex)
    if subCtx == nil {
        return nil, 0, api.ErrInvalidExecution
    }
    ret := subCtx.dependentResults[subCtx.callFrame.PC]
    subCtx.callFrame.Next()
    return ret.Result, ret.LeftOverGas, nil
}
```

接口签名同步加 `callIndex api.CallIndex` 参数，7 处调用点全部持有 `CrossCallIndex`，直接传入。

### 2.3 LockExecution 子上下文化

`lockStateWithExecution` 在调用 `sscvm.Call()` 前创建子上下文，`defer Delete` 自动清理：

```go
lockCtx := &callVerifyContext{
    currentState:     newStateSet(),
    callFrame:        &api.CallFrame{CallIndex: callIndex, PC: 0},
    dependentResults: callState.DependentResults,
}
root := v.getVerifyContext(txHash)
root.SubCtx.Store(callIndex, lockCtx)
defer root.SubCtx.Delete(callIndex)

sscvm.Call(...)  // 内部 GetResult 通过子上下文读取
```

### 2.4 数据流对比

**改造前（串行）：**
```
VerifySimulation →
  StoreVerifyContext(txHash, rootCtx with CurrentState/CallFrame/DependentResults)
  → verifyExecuteForCallState → 覆写 rootCtx.CurrentState/CallFrame
  → 回调读 rootCtx.CurrentState → GetResult 读 rootCtx.CallFrame.PC
  → LockExecution 读 rootCtx.DependentResults/CallFrame（fallback）
```

**改造后（统一子上下文）：**
```
VerifySimulation →
  StoreVerifyContext(txHash, rootCtx with SubCtx sync.Map)
  → Phase 1.5: root.SubCtx.Store(callIndex, subCtx)  各 CallState
  → Phase 2: verifyExecuteForCallState → 回调读 loadSubCtx
  → GetResult(txHash, callIndex) → loadSubCtx
  → LockExecution: root.SubCtx.Store(callIndex, lockCtx) → sscvm.Call → defer Delete
  → Cleanup: executionVerifyContexts.Delete(txHash)  // SubCtx 自动释放
```

## 3. 改动范围

| 文件 | 改动量 | 类型 |
|:----|:------|:-----|
| `ssc/api/sscs.go` | 1 行 | `GetResult` 接口加 `callIndex` |
| `ssc/api/types.go` | 4 行 | `ExecutionVerifyContext`: 移除 `CallFrame`/`CurrentState`，新增 `SubCtx sync.Map` |
| `ssc/verify.go` | ~120 行 | 存储层重构：`map→sync.Map` + `SubCtx` 内嵌 + 辅助方法 + Cleanup/GetResult/回调/LockExecution 更新 |
| `core/vm/sscvm.go` | 1 行 | `GetResult` 传 `CrossCallIndex` |
| `core/vm/sscis_lock_execution.go` | 1 行 | 同上 |
| `core/vm/sscis_execution_verify.go` | 4 行 | 同上（4 处） |

## 4. 安全验证

| 检查项 | 论证 |
|:-------|:-----|
| **`sync.Map` 替代 `map+RWMutex`** | `StoreVerifyContext` 只写一次，后续只读。`Cleanup` 单线程调用。安全。 |
| **`SubCtx` 内嵌 sync.Map 并发安全** | 不同 txHash 的 `SubCtx` 完全隔离。同一 txHash 内的不同 `callIndex` 由 `sync.Map` 保证安全。 |
| **`Cleanup` 无泄漏** | `executionVerifyContexts.Delete(txHash)` 释放整个 `ExecutionVerifyContext`，内嵌 `SubCtx` 的 `sync.Map` 由 GC 回收。 |
| **LockExecution 子上下文** | `lockStateWithExecution` 进入 sscvm 前创建子上下文，defer Delete。无泄漏，无竞争。 |
| **`GetResult` 无 fallback** | 所有路径（Phase 2、LockExecution）必创建子上下文，否则 `GetResult` 返回 `ErrInvalidExecution`。不再存在不可达的 fallback 路径。 |

## 5. 关键决策

| 决策 | 选择 | 理由 |
|:----|:----|:------|
| `executionVerifyContexts` 存储方式 | `sync.Map` | 无锁原子操作 |
| 子上下文位置 | `ExecutionVerifyContext.SubCtx` 内嵌 | 零额外管理，Cleanup 随根自动释放 |
| `CallFrame`/`CurrentState` | 彻底移除 | 不再有根/子分裂，全部走子上下文 |
| 回调 fallback 路径 | 删除 | 所有路径必创建子上下文，fallback 不可达 |
| `GetResult` | 子上下文唯一路径 | 回退路径已删除，LockExecution 也会创建子上下文 |
| `lockStateWithExecution` | 调用前创建子上下文 | 统一 GetResult 路径，消除数据竞争 |

## 6. 后续优化：WriteState 锁检查改用 CheckLock

### 6.1 优化点

Phase 1 lockCheck 中，WriteState 的锁冲突检查（`verify.go:331`）之前使用 `stateDB.GetState(txHash, address, key)`。

但 `GetState` 内部同时做两件事：
```go
func (db *DB) GetState(txHash, addr, key) {
    err := db.locker.Lockable(key, txHash)  // ✅ 需要（查 in-memory sync.Map）
    state := db.GetStateWithoutLock(addr, key) // ❌ 不需要（写锁检查不读值）
    return state, err
}
```

而 `CheckLock(key, txHash)` 只做 `Lockable` 调用，**不读 trie**：
```go
func (db *DB) CheckLock(key, txHash) {
    return db.locker.Lockable(key, txHash)  // 纯内存操作
}
```

### 6.2 收益

- 写锁检查跳过一次 trie 读取（`GetStateWithoutLock` 走 merkle trie）
- 每个 WriteState key 省一次 I/O
- 典型 SimTx 有 N 个写 key，收益线性

### 6.3 边界说明

```go
// 写锁检查 — 只查锁状态，不读 trie（已改）
stateErr := stateDB.CheckLock(lockKey, txHash)

// 读锁检查 + 值对比 — 仍需读 trie（不改）
onChainValue, stateErr := stateDB.GetState(txHash, address, key)
```

读侧需要 `onChainValue` 与 simulation 的期望值做 `bytes.Compare`，必须读 trie，不改。

### 6.4 未定：读侧不匹配率

当前没有实验数据统计 `conflictCallStateIndex >= 0`（ReadState 值与链上不匹配）的触发频率。该数据需要在未来实验中通过日志 `VerifySimulation %s mu conflict (read) for key` 的出现频率统计。如果该路径命中率极低，读侧也可考虑用 `CheckLock` + 条件读取优化。
