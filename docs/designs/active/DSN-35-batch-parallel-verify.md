# DSN-35: 跨 SimTx 批量并行验证（v2）

> **状态**：设计完成，待实现
> **对应**：HANDOFF-20260718-parallel-simtx-verify
> **版本历史**：
>   - v2 (2026-07-20): 修正 subCtx.currentState 为 ReadState 预初始化；lockCheck/exec 失败改为逐笔处理；补充 worker.go 整合逻辑

## 1. 动机

### 1.1 问题

单 SimTx 耗时 P50=1.91ms，足够快，但 `CommitSSCTransactions` 逐个处理：

```
for each SimTx in pool:
    commitTransaction(tx) → ApplyTransaction → precompile → VerifySimulation
```

串行瓶颈：块内 50+ 笔 SimTx × 1.91ms ≈ 95ms/块。

### 1.2 目标

跨 SimTx 批量验证，把 verifySimulation 的流程拆成独立相位，所有 SimTx 统一过各相位：

```
反序列化 → 冲突仲裁 → lockCheck → 子上下文 → Copy → execVerify → lockState → commitVote
```

各相位内部聚合所有 SimTxs 的 CallStates 并行处理。

## 2. 架构

### 2.1 BatchVerifySimulations 相位（Phase 0-5）

```
BatchVerifySimulations(batch, statedb, header):
│
├─ Phase 0: 提取所有 SimTxs 的 RWSet
│   writeKeys[n][key], readKeys[n][key]
│
├─ Phase 0.5: 同块冲突仲裁（串行，按 batch 顺序）
│   committedWrites = {}
│   for i, sim in batch:
│     writeKeys[i] ∩ committedWrites ≠ ∅?    → failed, callForRetry
│     writeKeys[i] ∩ globalLock?              → failed, callForRetry
│     readKeys[i]  ∩ committedWrites ≠ ∅?    → failed, callForRetry
│     passed = append(passed, i)
│     committedWrites ∪= writeKeys[i]
│   → passed 列表：所有 SimTxs 互不冲突，真并行安全
│
├─ Setup: 每个 passed SimTx 初始化
│   链式补丁、定时器、状态、验证上下文
│
├─ Phase 1: 跨 SimTx lockCheck（goroutine pool）
│   for each CallState of passed:
│     CheckLock(WriteState keys)
│   → 收集 conflictSims（有冲突的 SimTx 索引）
│
├─ Phase 1.5: 子上下文预分配
│   for each CallState:
│     subCtx := {
│       currentState:     newStateSetFromRead(ReadState),  // 从读集预初始化
│       callFrame:        &api.CallFrame{CallIndex, PC: 0},
│       dependentResults: cs.DependentResults,
│     }
│
├─ Phase 2: stateDB.Copy() × totalCallStates
│   for each CallState:
│     copies[i] = stateDB.Copy()
│
├─ Phase 3: 全并行 execVerify（goroutine pool）
│   for each CallState:
│     verifyExecuteForCallState(sim, txHash, callState, copy)
│   → 收集 execFailSims（执行失败的 SimTx 索引）
│
├─ Phase 4: 串行 lockStateWithRWSet
│   for each passed CallState (非 conflict、非 execFail):
│     lockStateWithRWSet(txHash, callState, statedb)
│
├─ Phase 4.5: cleanup — 冲突/失败的 SimTxs 发 retry
│   for each conflictSim or execFailSim:
│     stateDB.RollbackTx(txHash)
│     callForRetry(txHash, simulation) 或 sendRollbackVote
│
└─ Phase 5: cleanup — 成功的 SimTxs 发 commit vote
    for each passed SimTx (非 conflict、非 execFail):
      sendCommitVote(txHash)
```

### 2.2 同块冲突仲裁（Phase 0.5）详解

```
committedWrites = {}  // 已通过的 SimTx 写入的所有 key

for i, sim := range batch:
    // 写写冲突：同块内两笔 SimTx 写同一 key
    for key := writeKeys[i]:
        if key ∈ committedWrites:
            callForRetry(sim); failed++; continue

    // 全局锁冲突：key 被之前块的 SimTx 锁了
    for key := writeKeys[i]:
        if stateDB.CheckLock(key, sim.TxHash) fails:
            callForRetry(sim); failed++; continue

    // 读写冲突：SimTx 读了一个被前面 SimTx 写了的 key
    for key := readKeys[i]:
        if key ∈ committedWrites:
            callForRetry(sim); failed++; continue

    // 通过
    committedWrites ∪= writeKeys[i]
    passed++
```

当前实验冲突率 1/78,641，几乎总是全部 passed。

### 2.3 Phase 1 lockCheck — 逐 SimTx 冲突追踪

lockCheck 不再用全局 `conflictAll` boolean 标记整个 batch 失败，而是逐 SimTx 追踪：

```
Phase 1（goroutine pool）:
  for each CallState of passed:
    for each WriteState key:
      if CheckLock(key, txHash) fails:
        mark simIdx as conflicted  // 原子标记
        return                      // 该 CallState 停止检查

Phase 1 → 收集 conflictSims:
  conflictSims = {simIdx | simIdx 被标记为冲突}
  // 冲突的 CallState 所属的 SimTx 整体 retry（CallStates 间有依赖）

Phase 3 后:
  for each conflictSim:
    stateDB.RollbackTx(txHash)
    callForRetry(txHash, simulation)
```

### 2.4 subCtx.currentState 语义

**当前状态（v1 已实现但语义不完整）：**
- 串行路径：`newStateSet()` — 空 StateSet，EVM 回调 `GetSimuState` 回退到 root.ReadState 取初始值
- batch 路径：`cs.RWSet.WriteState.Copy()` — 编译错误，语义也不对

**v2 修正：**
统一用 `newStateSetFromRead(readState)` 从 ReadState 预初始化 currentState：

```go
func newStateSetFromRead(src *api.StateSet) *api.StateSet {
    if src == nil {
        return &api.StateSet{
            Balance: make(map[common.Address]*big.Int),
            State:   make(map[common.Address]map[common.Hash]common.Hash),
        }
    }
    // 浅引用 — ReadState 校验后不再使用，无副作用
    return &api.StateSet{
        Balance: src.Balance,
        State:   src.State,
    }
}
```

这样：
- `GetSimuState` 直接命中 currentState，不再回退 root.ReadState
- `SetSimuState` 写入 currentState
- EVM 执行后 `currentState.Equal(WriteState)` 语义正确

### 2.5 与串行路径的关系

| 能力 | 串行 | 批量并行 |
|:-----|:----|:---------|
| 同块冲突检测 | 隐式（A lock→A lockState→B check→发现冲突） | Phase 0.5 显式仲裁 |
| lockCheck | 串行 | 跨 SimTx goroutine 并行 |
| execVerify | 单 SimTx 内 CallStates 并行 | 跨 SimTx CallStates 全并行 |
| lockState | 串行 | 串行（不变） |
| 失败处理 | 单 SimTx 独立 retry/rollback | 逐笔 retry/rollback |
| vote 发送 | 每 SimTx 独立 | 每 SimTx 独立 |
| subCtx.currentState | v1: 空 newStateSet() + 回退到 ReadState | **统一用 newStateSetFromRead(ReadState)** |

### 2.6 CallFrame 修正

batch 路径当前写 `&callFrame{PC: 0}`（编译错误），改为与串行路径一致：

```go
callFrame: &api.CallFrame{CallIndex: callIndex, PC: 0}
```

### 2.7 成功路径不设 VERIFIED 状态

batch 路径当前写 `v.state.SetStatus(txHash, api.VERIFIED)`，但 `VERIFIED` 常量不存在。

串行路径成功时也不设特殊状态（入口已设为 `VERIFYING_SIMULATION`），只发 commit vote。batch 路径对齐：

```
Phase 5: 成功 SimTxs 只发 commit vote，不设状态
```

## 3. worker.go 整合

### 3.1 替换式调用

`CommitSSCTransactions` 中根据 `EnableParallelBatch` 配置决定走哪条路径：

```go
func (w *Worker) CommitSSCTransactions() {
    // ... 现有 pool 收集逻辑

    if w.isParallelBatchEnabled() {
        // 替换式：所有 current pool SimTxs 一次性 batch
        sims := collectCurrentSims()
        verifier.BatchVerifySimulations(sims, stateDB, header)
    } else {
        // 串行路径（现有逻辑）
        for each sim in pool:
            verifier.VerifySimulation(bytes, stateDB, header)
    }
}
```

### 3.2 时间预算

- Phase 0-0.5（冲突仲裁）不占用预算（极快，< 0.5ms）
- Phase 1 lockCheck + 预分配 + Copy 在预算内完成
- Phase 3 execVerify（并行）可能超过 Phase 1 预算时截断
- 对于不属于「通过集」的 SimTxs，继续保留在 pool 中等待下个块

和现有 `CommitSSCTransactions` 的时间预算逻辑一致——由外层的 `processCommitTransactions` 控制预算截断。

## 4. 已验证的底层能力

| 能力 | 来源 | 状态 |
|:-----|:-----|:-----|
| stateDB.Copy() | DSN-34 | ✅ 0.02ms/副本，已投产 |
| 单 SimTx 内 CallStates 并行 execVerify | DSN-34 | ✅ P50=1.65ms |
| lockCheck 内并行（CheckLock 线程安全） | DSN-34 | ✅ 已实现 |
| verifyExecuteForCallState 独立调用 | DSN-33 | ✅ 子上下文隔离 |
| lockStateWithRWSet 线程安全 | 现有 | ✅ stateLocker 保护 |

## 5. 实现计划

| 步骤 | 内容 | 文件 | 优先级 |
|:----|:-----|:-----|:------|
| 1 | `newStateSetFromRead` 辅助函数 | `ssc/impl.go` | P0 |
| 2 | 修正 batchVerifyPassed 3 个编译错误 + currentState 语义 | `ssc/verify.go` | P0 |
| 3 | Phase 1 lockCheck 冲突追踪改为逐 SimTx | `ssc/verify.go` | P0 |
| 4 | Phase 4 exec 失败逐笔处理 | `ssc/verify.go` | P0 |
| 5 | 串行路径也统一用 newStateSetFromRead | `ssc/verify.go` | P0 |
| 6 | worker.go 并行分支 | `node/worker/worker.go` | P1 |
| 7 | 编译 + 部署 199 实验 | | P2 |

## 6. 已知风险

| 风险 | 缓解 |
|:-----|:------|
| `currentState` 浅引用 ReadState → 写入 currentState 会改到原 ReadState | ReadState 校验后不再使用，无副作用 |
| `stateDB.(*corestate.DB)` 类型断言可能 panic | 如现有用法 `stateDB.(*corestate.DB)` |
| 跨 SimTx lockCheck 无顺序保证（但 Phase 0.5 已滤除冲突） | 同一 key 无冲突 = 无竞争 |
| `callForRetry` 在 Phase 1 后统一调用 | 串行调用，线程安全 |
| batch size 1 时额外开销（Phase 0 提取 RWSet + 仲裁） | 极低（< 0.5ms），可忽略 |
