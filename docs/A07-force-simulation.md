# [A07] ForceSimulation（冲突容忍模拟）

> **版本**：v1（2026-06-20，设计阶段）
> **前置依赖**：PatchPool v4（[A05](./A05-hotkey-retry-design.md)）、Wound-Wait 锁协调（[A06](./A06-lock-priority-coordination.md)）
> **解决的问题**：模拟时遇锁冲突立即中断，无法获取完整状态，导致重试效率低下

---

## 1. 问题背景

### 1.1 现有行为

当前模拟流程（`HandleSimulateRequest`）中，当合约执行的 `SLOAD` 或 `SSTORE` 遇到 `ErrLockedByOtherTx` 时，冲突从 EVM 指令级立即冒泡到 `TransitionDb`，终止整笔交易的执行：

```
opSload: GetAndLockState(K) → 冲突 → return nil, ErrLockedByOtherTx
  ↓
sscvm.Call() 收到 err → 冒泡到 opCall
  ↓
opCall: return nil, ErrLockedByOtherTx
  ↓
TransitionDb 结束，result.VMErr = ErrLockedByOtherTx
  ↓
HandleSimulateRequest: ret.Err = ErrLockedByOtherTx
  ↓
aggregateSimulationResults: 模拟失败 → CallForRetry
  ↓
下次重试 → 同样的冲突 → 循环
```

**后果：** 每次冲突都**零状态产出**（只有冲突前累积的部分 RWSet），下次重试必须从零开始重新模拟，并且很可能再次冲突。这在热点 key 多笔交易竞争时形成循环浪费。

### 1.2 实验场景的特殊性

在 SSCC 实验中，每笔交易需要访问的状态（读写集）是**预先可知的**，并且每个 key 只被一笔交易最终写入。这意味着：
- 冲突后从 stateDB 读取的状态值，就是**最终链上一致的值**
- 冲突后继续执行得到的完整 RWSet，与无冲突环境下模拟得到的结果**完全一致**

### 1.3 设计目标

- 遇到锁冲突时**不中断执行**，从 stateDB 读值继续跑完
- 完整 RWSet 存入 `SimulationState`，下次 `AddToRetry` 直接复用
- 链上可配置（`TimeoutConfig.ForceSimulation`），默认关闭
- 只在模拟阶段（`SimulationCall / SimulationRecall`）生效，不影响链上 VerifySimulation（`LockExecution`）

---

## 2. Error 重命名

### 2.1 问题

原有 error `ErrLockedByOtherTx` 被所有锁冲突路径共用，无法区分冲突来源。`ForceSimulation` 需要只对链上冲突（OnChain）做容忍，链下冲突（OffChain，如 TempLockView）按原有逻辑处理。

### 2.2 变更

```go
// ssc/api/state_lock.go

// 旧：统一 error
ErrLockedByOtherTx = errors.New("state is locked by other tx")

// 新：按来源分拆
ErrLockConflict_OnChain  = errors.New("state is locked by other tx on chain")
ErrLockConflict_OffChain = errors.New("state is locked by other tx off chain")

// 检测函数改名
// 旧：
func IsLockedByOtherTxErr(err string) bool
// 新：
func IsLockConflictErr(err string) bool
```

### 2.3 引用场景映射

| 路径 | 旧 error | 新 error |
|------|----------|----------|
| `state_locker.go: Lockable` | `ErrLockedByOtherTx` + `[stateDB]` | `ErrLockConflict_OnChain` |
| `state_locker.go: Lockable` | `ErrLockedByOtherTx` + `[TempLockView]` | `ErrLockConflict_OffChain` |
| `state_locker.go: RLockable` | `ErrLockedByOtherTx` + `[stateDB]` | `ErrLockConflict_OnChain` |
| `state_locker.go: RLockable` | `ErrLockedByOtherTx` + `[TempLockView]` | `ErrLockConflict_OffChain` |
| 其他全部引用 | `ErrLockedByOtherTx` | `ErrLockConflict_OnChain` |

（22 处引用共拆分为 18 处 OnChain + 4 处 OffChain）

---

## 3. 核心设计

### 3.1 配置项

```go
// ssc/api/types.go — TimeoutConfig
type TimeoutConfig struct {
    Sp1                uint64
    PoolTimeout        uint64
    MaxOnChainRetries  int
    ForceSimulation    bool   // 新增：冲突时强制继续执行
}
```

`build_keys.go` 中默认为 `false`（当前行为），实验时设为 `true`。

### 3.2 核心原则

**`GetAndLockState` / `SetAndLockState` 同时返回正确值和 error。**

Go 中 error 返回不会自动终止执行，调用方可以选择性忽略：

```go
val, err := db.GetAndLockState(txHash, callIndex, addr, key)
if err != nil {
    // 即使冲突了，val 也是从 stateDB 读到的正确值
}
```

调用方（opSload/opSstore）在 `ForceSimulation` 模式下：
- 拿 `val` 正常使用
- 不 return error，而是把 error 记到 `SSCVM.forceVMErr` 中
- 合约继续执行

### 3.3 指令集影响范围

只改以下指令集，**不改 `LockExecution`（_LE）**：

| 指令集 | 文件 | 改动作 |
|--------|------|--------|
| `SimulationCall` (_SC) | `sscis_simulation_call.go` | opSload、opSstore、opCall |
| `SimulationRecall` (_SR) | `sscis_simulation_recall.go` | opSload、opSstore、opCall |

**不改的指令集：**
| 指令集 | 文件 | 理由 |
|--------|------|------|
| `LockExecution` (_LE) | `sscis_lock_execution.go` | 链上 VerifySimulation 使用，不能容忍冲突 |
| `Base` (_Base) | `sccis_base.go` | 跨分片基础调用，不直接涉及锁冲突 |

### 3.4 SSCVM 层：forceVMErr

```go
// core/vm/sscvm.go
type SSCVM struct {
    // ... 现有字段 ...
    Config struct {
        ForceSimulation bool
        // ... 其他配置 ...
    }
    forceVMErr error   // ForceSimulation 模式下记录的冲突
}

func (vm *SSCVM) SetForceVMErr(err error) {
    vm.forceVMErr = err
}

func (vm *SSCVM) GetForceVMErr() error {
    return vm.forceVMErr
}
```

### 3.5 指令层改动

#### 3.5.1 opSload (SimulationCall / SimulationRecall)

```go
func opSload_SSC_SC(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
    interpreter := inp.(*SSCVMInterpreter)
    ctx := interpreter.vm.Context
    loc := stack.peek()
    val, err := interpreter.vm.StateDB.GetAndLockState(
        ctx.TxHash, ctx.CrossCallIndex, contract.Address(), common.BigToHash(loc),
    )
    if err != nil {
        if interpreter.vm.Config.ForceSimulation && IsLockConflictErr(err.Error()) {
            // ForceSimulation: 值已拿到，继续执行，记录冲突
            interpreter.vm.SetForceVMErr(err)
        } else {
            return nil, err   // 正常中断
        }
    }
    loc.SetBytes(val.Bytes())
    return nil, nil
}
```

#### 3.5.2 opSstore (SimulationCall / SimulationRecall)

```go
func opSstore_SSC_SC(pc *uint64, inp Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
    interpreter := inp.(*SSCVMInterpreter)
    ctx := interpreter.vm.Context
    loc := common.BigToHash(stack.pop())
    val := stack.pop()
    err := interpreter.vm.StateDB.SetAndLockState(ctx.TxHash, ctx.CrossCallIndex, contract.Address(), loc, common.BigToHash(val))
    if err != nil {
        if interpreter.vm.Config.ForceSimulation && IsLockConflictErr(err.Error()) {
            // ForceSimulation: 写不了锁，但值已经知道
            interpreter.vm.SetForceVMErr(err)
            interpreter.intPool.put(val)
            return nil, nil
        }
        return nil, err
    }
    interpreter.intPool.put(val)
    return nil, nil
}
```

#### 3.5.3 opCall (SimulationCall / SimulationRecall) — 跨 shard 调用

```go
// 跨 shard 调用部分
ret, returnGas, err = interpreter.vm.SSCService.GetResult(interpreter.vm.Context.TxHash)
if err != nil {
    if interpreter.vm.Config.ForceSimulation && IsLockConflictErr(err.Error()) {
        // ForceSimulation: 远程 shard 也有冲突，标记但不中断
        interpreter.vm.SetForceVMErr(err)
        // ret 仍然有效（远程 shard 正常返回了结果）
    } else {
        return nil, err
    }
}
```

### 3.6 TransitionDb 层

```go
// core/state_processor.go — TransitionDb 末尾
if result.VMErr == nil && vm.GetForceVMErr() != nil {
    // ForceSimulation 模式下冲突未中断执行，把冲突标记写入 result
    result.VMErr = vm.GetForceVMErr()
}
```

### 3.7 HandleSimulateRequest 层

```go
// ssc/simulator_member.go — 第 1069-1074 行
if result.VMErr != nil {
    if IsLockConflictErr(result.VMErr.Error()) {
        if sim.config.ForceSimulation {
            // ForceSimulation: 冲突了但执行完了，标记 key 但不报错
            ret.ConflictKeys = extractConflictKeys(callState)  // 新增字段
            ret.Err = ""   // 不报错，正常返回
        } else {
            ret.Err = api.ErrLockConflict_OnChain.Error()
        }
    } else {
        ret.Err = result.VMErr.Error()
    }
}
```

### 3.8 aggregateSimulationResults 层

```go
// ssc/simulator_leader.go
if len(aggregated.ConflictKeys) > 0 {
    // ForceSimulation 结果：有冲突但完整执行
    // 不走 CommitSimulation → 改走 CallForRetry
    // 但完整的 RWSet 已存入 SimulationState
    // 下次 AddToRetry 时直接从 SimulationState 拿 RWSet
}
```

---

## 4. 端到端流程

```text
Phase 1: 首次模拟（ForceSimulation=true）
─────────────────────────────────────
SimTx 执行到 SLOAD(K)
  → GetAndLockState(K) → 冲突!
  → 从 stateDB 读 K 当前值 → 返回 (val, ErrLockConflict_OnChain)
  → opSload 检测到 ForceSimulation:
      - 用 val 继续执行
      - 设 SSCVM.forceVMErr = ErrLockConflict_OnChain
  → 后续所有 SLOAD/SSTORE 同样处理
  → 合约跑完，得到完整 RWSet

Phase 2: 结果聚合
─────────────────
TransitionDb 结束 → result.VMErr = ErrLockConflict_OnChain
HandleSimulateRequest:
  → ForceSimulation=true
  → ret.ConflictKeys = [K]
  → ret.Err = "" (不报错)

aggregateSimulationResults:
  → 检测到 ConflictKeys 非空
  → 不走 CommitSimulation
  → 走 CallForRetry
  → 把完整 RWSet 存 SimulationState

Phase 3: 下次重试
─────────────────
AddToRetry 检测到 SimulationState 有完整 RWSet
  → 直接从 SimulationState 提取 ReadSet/WriteSet
  → 不需要重新模拟
  → retryPool 中的 RWSet 包含所有 key

Phase 4: RetryCommit
─────────────────────
RetryCommit 用完整 RWSet 匹配 PatchPool
  → HasConflict 命中 → TryConsume → Locked ✅
  → triggerReSimulation → 链下模拟（结果已经完整）
  → CommitSimulation → SimTx 提交
```

---

## 5. 数据结构变更

### 5.1 CXTSimulationResult

```go
type CXTSimulationResult struct {
    // ... 现有字段 ...
    ConflictKeys []LockKey `json:"conflict_keys,omitempty"`  // ForceSimulation 模式下冲突的 key
}
```

### 5.2 SSCVM

```go
type SSCVM struct {
    // ... 现有字段 ...
    Config struct {
        ForceSimulation bool   // 新增
        // ...
    }
    forceVMErr error    // 新增
}
```

### 5.3 TimeoutConfig

```go
type TimeoutConfig struct {
    Sp1               uint64
    PoolTimeout       uint64
    MaxOnChainRetries int
    ForceSimulation   bool    // 新增
}
```

---

## 6. 文件变更清单

| 文件 | 变更 | 优先级 | 估算 |
|------|------|:------:|:----:|
| `ssc/api/state_lock.go` | error 重命名：`ErrLockedByOtherTx` → `ErrLockConflict_OnChain` / `ErrLockConflict_OffChain` | P0 | ~5 行 |
| `ssc/state_locker.go` | 4 处按来源分拆 OnChain/OffChain | P0 | ~5 行 |
| 其他 18 处 `ErrLockedByOtherTx` 引用 | 替换为 `ErrLockConflict_OnChain` | P0 | 机械替换 |
| `core/vm/sscvm.go` | `IsLockedByOtherTxErr` → `IsLockConflictErr`；新增 `forceVMErr` + `Config.ForceSimulation` | P0 | ~20 行 |
| `core/vm/sscis_simulation_call.go` | opSload/opSstore/opCall 加 ForceSimulation 分支 | P0 | ~30 行 |
| `core/vm/sscis_simulation_recall.go` | 同上 | P0 | ~30 行 |
| `core/state_processor.go` | TransitionDb 末尾检查 forceVMErr | P0 | ~5 行 |
| `ssc/simulator_member.go` | HandleSimulateRequest 区分 ForceSimulation 模式 | P0 | ~15 行 |
| `ssc/simulator_leader.go` | aggregateSimulationResults 处理 ConflictKeys | P1 | ~10 行 |
| `ssc/api/types.go` | TimeoutConfig 加 ForceSimulation；CXTSimulationResult 加 ConflictKeys | P0 | ~5 行 |
| `cmd/build_keys/build_keys.go` | 默认 ForceSimulation=false | P0 | ~1 行 |

总估算：~130 行新增/修改

---

## 7. 设计决策记录

| # | 决策 | 结论 | 理由 |
|---|------|------|------|
| D1 | 强制执行范围 | 仅 SimulationCall/SimulationRecall | LockExecution 是链上执行，不能容忍冲突 |
| D2 | 冲突通知方式 | `SSCVM.forceVMErr` | 不需要改 GetAndLockState 签名，error 同时返回值和冲突标记 |
| D3 | 跨 shard 调用 | 远程冲突通过 `CXTCallSSCResult.Err` 带回，ForceSimulation 模式不报错 | 远程 shard 也走同一套逻辑，结果一致 |
| D4 | Error 重命名 | 分 OnChain/OffChain | ForceSimulation 只关心 OnChain 冲突 |
| D5 | Error 检测函数 | `IsLockedByOtherTxErr` → `IsLockConflictErr` | 语义更清晰，与新 error 名称一致 |

---

## 8. 实验验证

### 8.1 日志关键词

| 日志 | 含义 |
|------|------|
| `ForceSimulation: lock conflict, continuing` | ForceSimulation 模式下冲突继续 |
| `ForceSimulation: cross-shard lock conflict` | 远程 shard 冲突继续 |
| `aggregatePartialResult: conflict keys=N` | 聚合时发现 N 个冲突 key |
| `retry from partial simulation result` | 从 SimulationState 拿完整 RWSet 重试 |

### 8.2 预期效果

- **重试模拟次数减少**：冲突后不中断，一次模拟拿到完整状态，不需要重复模拟
- **AddToRetry 时 RWSet 完整**：直接从 SimulationState 取，不需要重新从 callStates 提取
- **PatchPool 命中率提升**：完整 RWSet 让 HasConflict 匹配更精准
