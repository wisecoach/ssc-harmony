# DSN-34: SimTx 并行验证设计方案

> **状态**：方案设计阶段，待决策后落地
> **对应**：HANDOFF-20260718-parallel-simtx-verify

## 1. 动机

### 1.1 问题

VerifySimulation 占每块 commitTxs 时间的 86.5%。内部耗时：

| 阶段 | avg | 占比 |
|:----|:---:|:----:|
| lockCheck | 2.56ms | 46% |
| execVerify | 2.69ms | 48% |
| 其余 | ~0.3ms | 6% |

Shard 3 的 execVerify 达 **5.22ms**，单块 150+ CallState 时超过 1s 预算，触发截断。

### 1.2 目标

将 execVerify 并行化，使 Shard 3 的 commitTxs 从 1,206ms 降至 <1s 预算内。

## 2. 已完成的基础设施

### 2.1 子上下文（DSN-33）

`ExecutionVerifyContext.SubCtx`：每个 CallState 独立的 `callVerifyContext`，隔离 `currentState`/`callFrame`/`dependentResults`。

5 个 EVM 回调（`GetSimuState`/`SetSimuState`/`SubSimuBalance`/`AddSimuBalance`/`GetSimuBalance`）+ `GetResult` 全部走子上下文，**无 stateDB 访问**，goroutine-safe。

### 2.2 Phase 1/2/3 拆分

| Phase | 操作 | 串/并行 | 耗时 |
|:------|:-----|:-------|:----:|
| 1 | lockCheck（读 stateDB 锁状态 + trie 值对比） | 串行 | 2.56ms × N |
| 1.5 | 预分配子上下文 + 非共享数据准备 | 串行 | O(N) |
| 2 | execVerify（EVM 执行 + 子上下文读写） | **目标并行** | 2.69ms |
| 2.5 | 检查结果 | 串行 | O(N) |
| 3 | lockState（写 stateDB 锁） | 串行 | 0.3ms × N |

### 2.3 lockCheck 优化

WriteState 锁检查改用 `CheckLock`（纯内存），不再调用 `GetState`（读 trie）。

## 3. 已尝试的并行方案与失败原因

### 3.1 方案 A：直接 goroutine（首次实验）

**做法**：Phase 2 加 `go func` + `sync.WaitGroup`，每个 CallState 一个 goroutine。

**失败**：`fatal error: concurrent map read and map write`

**根因**：`*corestate.DB`（`stateDB`）内部 `stateObjects map[common.Address]*Object`。多个 goroutine 同时 `getStateObject(addr)` 因缓存未命中写入同一 map。

**波及的 map**：

| map | 触发路径 | 写入时机 |
|:----|:---------|:--------|
| `stateObjects` | `getStateObject` → `setStateObject` | 首次加载 account |
| `stateObjectsDestruct` | `createObject` | CreateAccount |
| `journal` | `createObject` | 同上 |
| `validRevisions` / `nextRevisionId` | `Snapshot()` | 每次快照 |

### 3.2 方案 B：Snapshot 跳过 + 预热缓存

**做法**：
- `sscvm.go`：ExecutionVerify 模式跳过 `Snapshot()` / `RevertToSnapshot()`
- `verify.go`：Phase 1.5 用 `GetCode`/`GetCodeHash`/`GetBalance`/`GetNonce` 预热 `stateObjects`

**失败**：仍然 `concurrent map read and map write`

**根因**：
1. `CreateAccount`（`sscvm.Call` 中 `!Exist` → `CreateAccount`）写 `stateObjects` + `stateObjectsDestruct` + `journal`，预热无法覆盖不存在的 account
2. EVM 执行中 `BALANCE`、`EXTCODE*` 等基础指令触发 `getStateObject`，预热可能不完整

### 3.3 方案 C：Verifier 级 `sync.Mutex`

**做法**：`Verifier.verifyMu sync.Mutex` 包住整个 `verifyExecuteForCallState`

**失败**：等价于串行，无并行收益

### 3.4 方案 D：`safeStateDB` 装饰器

**做法**：实现 `api.StateDB` 接口的包装，覆写 `GetCode`/`GetCodeHash`/`Exist` 方法用 mutex 保护

**失败**：`GetCode`/`GetCodeHash` 不在 `api.StateDB` 接口上，`SSCVM.StateDB` 类型为 `*state.DB` 而非接口，无法替换

## 4. 当前候选方案

### 4.1 核心思路

组合两条路径，消除共享 stateDB 的写竞争：

**路径 1：绕过 `sscvm.Call()` 前 40 行预读**
不调 `sscvm.Call()`，直接调 `sscvm.run(contract, input, false)`。在 Phase 1 串行预读 `GetCode`/`GetCodeHash`，Phase 2 goroutine 手动构造 Contract 对象传入。

```
// Phase 1：预读（串行）
codes[addr]       := stateDB.GetCode(addr)
codeHashes[addr]  := stateDB.GetCodeHash(addr)

// Phase 2：直接执行（并行）
contract := NewContract(txHash, caller, to, value, gas)
contract.SetCallCode(&addr, codeHashes[addr], codes[addr])
sscvm := NewSSCVM(vmCtx, stateDBCopy, ...)  // 见路径 2
ret, err := sscvm.run(contract, input, false)
```

需修改：`sscvm.go` 中 `run()` 改为导出（`Run`）。

**路径 2：每个 goroutine 独立的 `stateDB.Copy()`**
`*corestate.DB.Copy()` 深拷贝 `stateObjects`/`journal`/`validRevisions` 等所有 map，仅共享只读 trie。

```
for i := 0; i < execCallStates; i++ {
    copies[i] = stateDB.Copy()  // 每个拷贝独立的 stateObjects map
}
// Phase 2 goroutine i 用 copies[i]
```

开销：O(dirty objects)，只有 Phase 1 读取过的几个 account，微秒级。

### 4.2 改动范围

| 文件 | 改动 | 难度 |
|:----|:-----|:----:|
| `core/vm/sscvm.go` | `run` → `Run`（导出） | 低 |
| `ssc/verify.go` | Phase 1.5 预读 code + Phase 1.5.5 `Copy()` | 中 |
| `ssc/verify.go` | Phase 2 goroutine 用预读 code + `Copy()` 实例 | 中 |
| `ssc/verify.go` | 移除 `verifyMu` | 低 |

### 4.3 安全分析

| 检查项 | 分析 |
|:-------|:-----|
| **`run()` 导出后调用方安全** | `run()` 内部无 `Snapshot`/`RevertToSnapshot`，直接解释字节码。`run()` 读取 `vm.StateDB` 但通过路径 2 隔离。安全。 |
| **`Copy()` 开销** | 仅复制 journal.dirties 中的 state object（~几个 address），微秒级。只在 Phase 1 结束后复制一次。 |
| **EVM 基础指令竞争** | `BALANCE`/`EXTCODE*` 等走 `vm.StateDB` → 自己的 `Copy()` → 自己的 `stateObjects`。无竞争。 |
| **Trie 共享读取** | `Copy()` 共享 `db.db`（leveldb）和 `trie` handle。leveldb 读线程安全。 |

### 4.4 开放问题

| 问题 | 影响 |
|:-----|:------|
| `run()` 导出后，其他调用方是否可能误用？ | 最小权限原则 — 可改为 `Run(contract, input) error` 命名更明确 |
| `Copy()` 后的 stateDB 是否需要 `RollbackTx`？ | Phase 2 执行期间子上下文无 stateDB 写，但 `Snapshot` 仍计入 `validRevisions`。`Copy()` 自始空 journal，退出无需清理。 |
| 跨合约 CALL 的 code 预读？ | 同 shard CALL 会调 `sscvm.Call()` → 前 40 行 stateDB 预读又回来了。但这在有 `Copy()` 后已安全。 |

## 5. 关键决策

| 决策 | 选项 | 建议 |
|:----|:-----|:-----|
| Phase 2 执行入口 | `Call()` vs `Run()` | **`Run()`** — 跳过 prelude，并行安全 |
| stateDB 隔离 | 共享 vs `Copy()` | **`Copy()`** — 每个 goroutine 一份，真并行 |
| code 预读时机 | Phase 1 vs 运行时 | **Phase 1** — 串行安全，`Copy()` 兜底 |
| `verifyMu` | 保留 vs 删除 | **删除** — `Copy()` 后不需要 |
