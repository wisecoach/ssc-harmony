# DSN-36 — VerifyContext 分 SimulationNum 隔离

> **产出角色**: Designer
> **消费角色**: Implementer
> **状态**: `planned`

---

## 1. 问题

`executionVerifyContexts` 的 key 只有 `txHash`（`common.Hash`），不区分 `SimulationNum`。

当同一笔交易（同 txHash）的不同 SimulationNum（如 simNum=0 和 simNum=1）的 SimTx 同时进入同一 batch 验证时（Phase 0.5→passed），随后的流程会**相互覆盖**同 (txHash, callIndex) 的验证上下文：

1. Phase 1.5 `storeSubCtx(txHash, callIndex, subCtx)` → `sync.Map.Store` 覆盖上一条
2. Phase 3 两个 goroutine `loadSubCtx(txHash, callIndex)` → 拿到同一 `subCtx` 指针
3. 两个 goroutine 同时调用 `GetResult` 读写 `callFrame.PC` → **data race → off-by-1 / index out of range**

### 触发条件

- 同一笔交易的 retry（simNum=0→simNum=1）发生在同一轮 `CommitSSCTransactions` → `PendingBatchSimulations()` 之间
- `pendingBatchSims` 累积了两笔同 txHash 不同 simNum 的 SimTx
- Phase 0.5 仲裁未去重 → 两笔都进 `passed`
- Phase 1.5 按 (txHash, callIndex) 存 subCtx → 重叠

## 2. 方案 — Key 加入 SimulationNum

### 2.1 思路

`executionVerifyContexts` 的 key 从 `txHash` 改为 `(txHash, simNum)` 复合 key。不同 SimulationNum 的 SimTx 各有独立的 `ExecutionVerifyContext` 和内嵌 `SubCtx` 空间，互相隔离。

### 2.2 存储结构

```go
// 当前
executionVerifyContexts sync.Map // key: common.Hash → *ExecutionVerifyContext

// 改为
type verifyContextKey struct {
    txHash common.Hash
    simNum int
}
executionVerifyContexts sync.Map // key: verifyContextKey → *ExecutionVerifyContext
```

`struct key` 在 `sync.Map` 上按全部字段比较，值语义，无需拼字符串。

### 2.3 变动的 API 签名

| 函数 | 当前签名 | 新签名 | 备注 |
|------|---------|--------|------|
| `StoreVerifyContext` | `(txHash common.Hash, ctx *ExecutionVerifyContext)` | `(txHash common.Hash, simNum int, ctx *ExecutionVerifyContext)` |  |
| `getVerifyContext` | `(txHash common.Hash) *ExecutionVerifyContext` | `(txHash common.Hash, simNum int) *ExecutionVerifyContext` | 内部函数 |
| `HasVerifyContext` | `(txHash common.Hash) bool` | `(txHash common.Hash, simNum int) bool` | 见下文注 |
| `loadSubCtx` | `(txHash common.Hash, callIndex CallIndex) *callVerifyContext` | `(txHash common.Hash, simNum int, callIndex CallIndex) *callVerifyContext` | 内部调用 getVerifyContext |
| `storeSubCtx` | `(txHash common.Hash, callIndex CallIndex, ctx *callVerifyContext)` | `(txHash common.Hash, simNum int, callIndex CallIndex, ctx *callVerifyContext)` | 内部调用 getVerifyContext |
| `verifyExecuteForCallState` | `(simulation *CXTSimulation, txHash Hash, callState *CXTCallState, stateDB *DB) error` | 同左（simNum 从 simulation.SimulationNum 取） | 已经有 simulation 参数 |
| `StartPoolTimer` | `(txHash Hash, epochs []Epoch, blockNum uint64, originShardId uint32)` | `(txHash Hash, simNum int, epochs []Epoch, blockNum uint64, originShardId uint32)` | txInfo 需要 simNum |
| `StartSp1Timer` | `(txHash Hash, epochs []Epoch, blockNum uint64, originShardId uint32)` | `(txHash Hash, simNum int, epochs []Epoch, blockNum uint64, originShardId uint32)` | txInfo 需要 simNum |

> **注 — `HasVerifyContext`**：在 `impl.go:474` 的 `handleTxPoolTimeout` 中，调用 `HasVerifyContext` 是为了判断"这笔交易是否已经在链上被验证过"。加 simNum 参数后需从 `txInfo` 中取得 simNum 传入。见 §2.7。

### 2.4 内部 Key 辅助函数

```go
func verifyContextKey(txHash common.Hash, simNum int) verifyContextKey {
    return verifyContextKey{txHash: txHash, simNum: simNum}
}
```

### 2.5 调用点变更清单

#### verify.go（13 处）

| 位置 | 行号 | 改动 |
|------|------|------|
| `verifySimulationParsed` — StoreVerifyContext | 310 | 补 `simulation.SimulationNum` |
| `verifySimulationParsed` — storeSubCtx (Phase 1.5 loop) | 443 | storeSubCtx 加 simNum |
| `verifySimulationParsed` — getVerifyContext (cleanup) | 864, 913, 942, 971, 999 | 逐处补 simNum |
| `verifyExecuteForCallState` — getVerifyContext | 672 | `simulation.SimulationNum` |
| `verifyExecuteForCallState` — loadSubCtx | 755 | 补 `simulation.SimulationNum` |
| `batchVerifyPassed` — StoreVerifyContext | 1179 | 补 `sim.SimulationNum` |
| `batchVerifyPassed` — Phase 1.5 storeSubCtx | 1229 | 补 simNum |
| `batchVerifyPassed` — Phase 3 verifyExecuteForCallState | 1265 | simNum 从 `simulations[r.simIdx].SimulationNum` 取 |
| `loadSubCtx` 定义 | 97 | 加 simNum 参数 |
| `storeSubCtx` 定义 | 110 | 加 simNum 参数 |
| `StoreVerifyContext` 定义 | 156 | 加 simNum 参数 |
| `getVerifyContext` 定义 | 160 | 加 simNum 参数 |
| `HasVerifyContext` 定义 | 168 | 加 simNum 参数 |

#### impl.go（1 处）

| 位置 | 行号 | 改动 |
|------|------|------|
| `handleTxPoolTimeout` — HasVerifyContext | 474 | 补 `info.simNum` |

#### cxt_timer.go（4 处）

| 位置 | 行号 | 改动 |
|------|------|------|
| `StartPoolTimer` — 签名 | 46 | 加 `simNum int` 参数 |
| `StartPoolTimer` — 新建 txInfo | 67 | 加 `simNum: simNum` |
| `StartSp1Timer` — 签名 | 98 | 加 `simNum int` 参数 |
| `StartSp1Timer` — 新建 txInfo | 110 | 加 `simNum: simNum` |

#### 其他调用点（cxt_timer 函数的外部调用者）

通过 `search_files` 搜 `StartPoolTimer` 和 `StartSp1Timer` 的调用点补传 simNum。

### 2.6 串行 vs batch 两路径确认

- **串行路径** `verifySimulationParsed`：单一 simNum，改 key 后正确（多了 simNum 维度，不影响行为）。
- **Batch 路径** `batchVerifyPassed`：`passed` 中若有同 txHash 不同 simNum → 各自独立的 VerifyContext → **修复**。
- **旧版非 batch 单笔 VerifySimulation**：不受影响。

### 2.7 txInfo 扩展

`handleTxPoolTimeout` 中需知道当前是哪个 simNum 的验证上下文过期。`txInfo` 当前无 simNum 字段：

```go
type txInfo struct {
    txHash        common.Hash
    simNum        int              // + 新增
    epochs        []api.Epoch
    blockNum      uint64
    originShardId uint32
    poolTimeout   uint64
    sp1           uint64
}
```

**影响范围**：

| 构造点 | 位置 | 改动 |
|--------|------|------|
| `StartPoolTimer` — 新建 txInfo | cxt_timer.go:67 | 加 `simNum`（需函数参数传入） |
| `StartPoolTimer` — 更新已有 txInfo | cxt_timer.go:64 | 已有 tx 对象（赋值传递 simNum） |
| `StartSp1Timer` — 新建 txInfo | cxt_timer.go:110 | 加 `simNum`（需函数参数传入） |

`StartPoolTimer` 和 `StartSp1Timer` 的签名分别加 `simNum int` 参数，调用点补传（simulator.go 和 verify.go 的调用方都有 simNum）。

### 2.8 Cleanup 适配

`Cleanup(txHash)` 当前通过 `executionVerifyContexts.Delete(txHash)` 直接删除。改复合 key 后，需要按 txHash 前缀清理所有 simNum：

```go
func (v *Verifier) Cleanup(txHash common.Hash) {
    v.executionVerifyContexts.Range(func(k, _ interface{}) bool {
        if key, ok := k.(verifyContextKey); ok && key.txHash == txHash {
            v.executionVerifyContexts.Delete(k)
        }
        return true
    })
    v.txLockedSimNum.Delete(txHash)
}
```

或者更直接——`Cleanup` 加 `simNum int` 参数，调用方 closeTransaction 已有 `tx.SimulationNum`。

推荐第二种（更精确，避免 Range scan）。

## 3. 风险与注意事项

1. **编译检查**：Go 编译器会捕获所有签名不匹配的调用点（新增参数不会漏）。
2. **向后兼容**：未使用的 `getVerifyContext(txHash, 0)` 调用可能在主线程中被遗漏。确认 `impl.go` 中所有 `HasVerifyContext` 调用都已拿到正确的 simNum。
3. **cleanup**：`executionVerifyContexts` 的清理逻辑（`releaseLock` 等）同样需要 simNum。查看 `releaseLock` 和 `RemoveVerifyContext` 是否存在。
