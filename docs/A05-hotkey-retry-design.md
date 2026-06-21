# [A05] HotKey 链式重试（数据层）

> **阅读顺序**（`[前缀]` 表示阅读顺序）：
> 1. 📄 `docs/A01-sscc-refactor-plan.md` [A01] — SSCC 模块化重构总览
> 2. 📄 `docs/A02-lock-retry-mechanism.md` [A02] — 锁与重试机制
> 3. 📄 `docs/A03-CR-priority-optimization.md` [A03] — CR 提交优先级优化
> 4. 📄 `docs/A04-onchain-retry-limit.md` [A04] — 链上重试次数限制
> 5. 📄 `docs/A05-hotkey-retry-design.md` [A05] **（本文）** — HotKey 链式重试（数据层）
> 6. 📄 `docs/A06-lock-priority-coordination.md` [A06] — 锁优先级协调（协调层）
>
> **运维辅助**（与主线并行的独立文档）：
> - 📄 `docs/B01-log-query-guide.md` [B01] — 日志查询指南
> - 📄 `docs/B02-log-lifecycle.md` [B02] — 交易生命周期日志大全

> **当前版本**：v4 — PatchPool 分片本地池化方案（2026-06-19）
> **历史版本**：v1（CR 触发+HotKey 分类，`docs/archived/Z01-cr-hotkey-chaining-design.md`）、v2（DAG 树形，`docs/archived/Z02-sim-dag-chaining-design.md`）、v3（线性链，`docs/A05-hotkey-retry-design.md` 上卷）
>
> **阅读顺序**：本系列文档应按以下顺序阅读，反映机制演进过程：
> 1. 📄 `docs/A05-hotkey-retry-design.md`（本文）— 链式数据依赖与 PatchPool 数据层设计（v1→v4）
> 2. 📄 `docs/A06-lock-priority-coordination.md` — 锁优先级协调：Wound-Wait 跨分片锁竞争解决（v5）
>
> **实现状态**：v1-v3 已上线实验验证 ✅，v4 已上线实验验证 ✅，v5（Wound-Wait）设计待实现

## 1. 实现状态总览

| 阶段 | 内容 | 状态 |
|------|------|:----:|
| 数据结构变更 | `RetrySignal`、`ChainNode`、`TxSimKey` 等 | ✅ 已实现 |
| chainNextSim | SimTx 提交后扫描 retryPool，找依赖下游 | ✅ 已实现 |
| readPatchChain | 递归查 patches 链读上游 WriteSet | ✅ 已实现 |
| HandleRetrySignal | 接收 chain signal 并触发重试 | ✅ 已实现 |
| GetState 递归路径 | Step 0: patches → Step 1: ChainPatch → Step 2: stateDB | ✅ 已实现 |
| VerifySimulation 锁跳过 | 链式交易跳过锁冲突检查 | ✅ 已实现 |
| Patch vs stateDB 一致性检查 | 上游失败则下游自动失败 | ✅ 已实现 |
| BLS 签名修复 | `aggregateCXSSCCallResult` 添加 BLS 聚合签名 | ✅ 已实现 |
| 模块拆分 | `state_impl.go` 已删除，方法归入 `simulator.go`/`verify.go` | ✅ 已实现 |
| `Z01-cr-hotkey-chaining-design.md` | 旧方案（CR 触发） | 🗂️ 已归档 |
| `Z02-sim-dag-chaining-design.md` | 旧方案（DAG 树形） | 🗂️ 已归档 |

### 待办

| 任务 | 优先级 | 备注 |
|------|:------:|------|
| `commitSimulation` 函数拆分（当前 ~120 行） | P1 | `buildCallStates` 已迁入 Simulator |
| `buildSignaturesForSimulation` 可否迁入 Simulator | P2 | 需要 `sscService` 的 `BLSSignerMgr` |
| 删除 `getState` 兼容函数（当前零引用） | P2 | 确认所有调用点已替换 |

## 2. Problem Statement

### 现状瓶颈

跨分片交易（SSC）在模拟期间锁定状态 key，锁持有直到 CR 交易完成上链。典型周期约 **4 个区块**：

```
Block N:   SubmitSimulationTx (锁获取)
Block N+1: Simulation 提交
Block N+2: CR 投票收集
Block N+3: CR 提交 (锁释放)
```

在这 4 个区块内，同一个状态 key 只能被一笔交易修改。对于热点 key，吞吐量被锁竞争严重限制。

### 前版方案（chainHotKeyCR）的局限

前一版设计在 CR 交易提交后触发 chain，仅针对"热点 key"做优化：

- 冷启动振荡：key 需要积攒冲突计数才能被标记为 hot
- 单次链优化：每次 CR 提交最多 chain 出一笔 SimTx
- HotKey 判断冗余：能否 chain 取决于锁竞争关系而非是否"hot"

### 本方案目标

放弃按 key 热度判断的策略，改为**全量线性链式依赖执行**：

1. **区块内多次修改同一个 key** — 链式 SimTx 按 nonce 排序，同一区块内依次执行
2. **线性依赖** — 每个节点最多一个下游，收到一个 signal 就立即尝试
3. **隐式一致性检查** — 通过 Patch vs stateDB 匹配检测上游失败

---

## 3. 核心概念

### 3.1 链式执行（Chained Execution）

```
同一区块 Block N（按 nonce 升序）：

SimTx(crN+1) → 锁 K → 读 patch → 执行 → K=v1
SimTx(crN+2) → 锁 K → 读 patch(SimTx1) → 基于 v1 执行 → K=v2
SimTx(crN+3) → 读 v2 → ...
```

- SimTx 之间通过 **nonce 排序**保证区块内执行顺序
- 下游 SimTx 的 `GetState` 通过 `RetryScheduler.patches` 递归读取上游 WriteSet
- 区块执行时 stateDB 按 nonce 递增依次写入，下游自然看到上游执行结果

### 3.2 线性链（非 DAG）

每个节点最多一个下游。理由：分片场景中，一个交易收到多个 RetrySignal 的处理复杂度高，应收到一个 signal 后立即开始尝试。

```
SimTx1 → SimTx2 → SimTx3 → SimTx4  (线性链)
```

不做分支，不做 DAG 拓扑排序。nonce 天然递增。

### 2.3 乐观锁语义 + 隐式一致性检查

链中任何一笔 SimTx 的 VerifySimulation 通过 **Patch vs stateDB 匹配** 检测上游状态：

```
SimTx2.VerifySimulation:
  1. 从 patch 读到 K=v1（期望值）
  2. 去 stateDB 验证 K 是否 = v1
  3. 匹配 → 上游成功，继续执行
  4. 不匹配 → 上游失败 → SimTx2 自己也失败
```

失败点及下游回滚，上游已成功部分不回滚。

---

## 4. 依赖链构建

### 4.1 触发时机

`chainNextSim` 在 **SimTx 提交到 mempool 后**触发：

```go
// 触发位置：SubmitSimulationTx 成功返回后
func afterSubmitSimulation(txHash common.Hash, writeSet *api.RWSet) {
    s.retryScheduler.chainNextSim(txHash, writeSet)
}
```

### 4.2 依赖定义

一笔 retry tx **依赖**一笔已提交的 SimTx，当且仅当：

```
(retryTx.ReadSet ∪ retryTx.WriteSet) ∩ SimTx.WriteSet ≠ ∅
```

### 4.3 chainNextSim 算法

```go
func (rs *retryScheduler) chainNextSim(upstreamTxHash common.Hash, writeSet *api.RWSet) {
    if writeSet == nil || len(writeSet.WriteState.State) == 0 {
        return
    }

    // 构建 key 查找表
    keySet := make(map[api.LockKey]struct{})
    for addr, state := range writeSet.WriteState.State {
        for key := range state {
            keySet[api.FormKey(addr, key)] = struct{}{}
        }
    }

    // RLock 下收集匹配的 retry tx（只取第一个，线性链）
    rs.mu.RLock()
    var matched *api.RetryTx
    for _, retryTx := range rs.retryPool {
        if bytes.Equal(retryTx.TxHash.Bytes(), upstreamTxHash.Bytes()) {
            continue
        }
        if dependsOn(retryTx, keySet) {
            matched = retryTx
            break  // 线性链：只取第一个匹配
        }
    }
    rs.mu.RUnlock()

    if matched == nil {
        return
    }

    // 发送 chain signal
    rs.sendChainSignal(matched.TxHash, matched, writeSet)
}

func dependsOn(retryTx *api.RetryTx, keySet map[api.LockKey]struct{}) bool {
    for _, key := range retryTx.ReadSet {
        if _, exists := keySet[key]; exists {
            return true
        }
    }
    for _, key := range retryTx.WriteSet {
        if _, exists := keySet[key]; exists {
            return true
        }
    }
    return false
}
```

---

## 5. 数据结构变更

### 4.1 `RetryScheduler` — 新增 patches 存储

```go
type RetryScheduler struct {
    // ... 现有字段 ...

    // 链式 Patch 存储
    // patches[txHash][simulationNum] = ChainNode
    patches map[common.Hash]map[int]*ChainNode
}

type ChainNode struct {
    TxHash        common.Hash
    SimulationNum int
    Patch         *api.RWSet    // 自己的 WriteSet
    UpstreamTxHash common.Hash  // 上游 txHash（零值=根节点）
    UpstreamSimNum int          // 上游 simulationNum
}
```

### 4.2 `RetrySignal`（重命名自 `ReSimulationSignal`）

```go
type RetrySignal struct {
    TxHash        common.Hash
    FromShard     uint32
    Epoch         Epoch
    SimulationNum int
    Condition     ConflictCondition
    Ready         bool

    // ChainPatch carries the WriteSet from the upstream SimTx.
    // nil = normal retry path; non-nil = chain-optimized path.
    ChainPatch *RWSet `json:"chain_patch,omitempty"`
}
```

### 4.3 `CXTSimulationState`

```go
type CXTSimulationState struct {
    // ... 现有字段 ...

    // ChainPatch 引用：当此 tx 是链式 retry 时，指向 RetryScheduler.patches 中的节点
    // GetState 时递归查 patches 链读取上游 WriteSet
    ChainPatchRef *TxSimKey `json:"chain_patch_ref,omitempty"`
}

type TxSimKey struct {
    TxHash        common.Hash
    SimulationNum int
}
```

---

## 6. 数据流端到端

### 6.1 完整流程

```
Phase 1: 链下构建与提交
────────────────────────
1. Leader 发现 retryTx1 可以执行（正常信号或区块边界触发）
2. Leader 链下模拟 retryTx1 → 成功
3. txSubmitter 为 SimTx1 分配 nonce=crN+1
4. SubmitSimulationTx(crSigner, nonce=crN+1)
5. afterSubmitSimulation → chainNextSim(SimTx1.hash, writeSet1)
6. chainNextSim 扫描 retryPool:
   └─ 找到 retryTx2（依赖 K）→ 发 RetrySignal(ChainPatch=writeSet1)

Phase 2: Origin shard 响应
───────────────────────────
7. Shard B 收到 HandleRetrySignal:
   ├─ stateLock → 存储 ChainNode 到 patches[tx2][simNum]
   │              → state.ChainPatchRef = {tx2, simNum}
   └─ rs.setSignal(fromShard, signal) → readyCnt check
       └─ readyCnt == len(RelatedShards)?
           ├─ Yes → tryToReSimulation(Tx2)
           │          ├─ 链下模拟（GetState 递归查 patches）
           │          ├─ 成功 → txSubmitter 分配 nonce=crN+2
           │          └─ afterSubmitSimulation → chainNextSim(SimTx2)
           └─ No  → 等待其他 shard 信号

Phase 3: 区块执行与验证
────────────────────────
8. Block N 打包（按 nonce 升序执行）:
   ├─ SimTx1(nonce=crN+1) → VerifySimulation
   │   → 锁 {K, L} → 执行 → K=v1, L=w1 → 成功
   │
   ├─ SimTx2(nonce=crN+2) → VerifySimulation
   │   → Patch vs stateDB 检查: K=v1 ✓
   │   → ChainPatch 存在 → 跳过锁冲突检查
   │   → 读 K=v1（stateDB，SimTx1 已写入）→ 执行 → K=v2
   │   → 成功
   │
   └─ SimTx3(nonce=crN+3) → VerifySimulation
       → Patch vs stateDB 检查: K=v2 ✓
       → 读 K=v2 → 执行 → K=v3
       → 成功

Phase 4: CR 与清理
────────────────────
9. SimTx1 CR 完成 → 删除 patches[tx1][simNum]
   SimTx2 CR 完成 → 删除 patches[tx2][simNum]
   SimTx3 CR 完成（叶子节点）→ 删除 patches[tx3][simNum] → TempLockView 解锁
```

### 6.2 失败场景

```
Phase 3 变体: SimTx2 VerifySimulation 失败
────────────────────────────────────────────
SimTx1 → 成功 → CR → 删 patch
SimTx2 → Patch vs stateDB 不匹配（或模拟失败）→ 回滚
SimTx3 → 读 patch(tx2) → Patch vs stateDB 不匹配 → 自动失败 → 回滚

结果: SimTx1 成功, SimTx2+SimTx3 回滚
```

---

## 7. GetState 的 Patch 读取路径

```go
func (s *sscService) GetState(db api.StateDB, txHash common.Hash,
    address common.Address, key common.Hash) (common.Hash, error) {

    // Step 0: 递归查 ChainPatch
    s.stateLock.RLock()
    state, err := s.getState(txHash)
    s.stateLock.RUnlock()
    if err == nil && state != nil && state.ChainPatchRef != nil {
        val, found := s.retryScheduler.readPatchChain(
            state.ChainPatchRef.TxHash,
            state.ChainPatchRef.SimulationNum,
            address, key)
        if found {
            // Patch 命中：缓存到 callState 的 RWSet
            if callState := s.GetCallState(txHash); callState != nil {
                callState.StateLock.Lock()
                callState.RWSet.ReadState.State[address][key] = val
                callState.RWSet.CurrentState.State[address][key] = val
                callState.StateLock.Unlock()
            }
            return val, nil
        }
    }

    // Step 1: 正常路径
    callState := s.GetCallState(txHash)
    // ... 现有逻辑 ...
}

// readPatchChain 递归查 patches 链，读到就停
func (rs *retryScheduler) readPatchChain(txHash common.Hash, simNum int,
    address common.Address, key common.Hash) (common.Hash, bool) {

    node, ok := rs.patches[txHash][simNum]
    if !ok {
        return common.Hash{}, false
    }

    // 先查自己的 patch
    if node.Patch != nil {
        if addrState, ok := node.Patch.WriteState.State[address]; ok {
            if val, exists := addrState[key]; exists {
                return val, true
            }
        }
    }

    // 自己没有，递归查上游
    if node.UpstreamTxHash != (common.Hash{}) {
        return rs.readPatchChain(node.UpstreamTxHash, node.UpstreamSimNum, address, key)
    }

    return common.Hash{}, false
}
```

---

## 8. VerifySimulation 的链式处理

### 8.1 锁跳过

当 `state.ChainPatchRef != nil` 时，VerifySimulation 对 patch 中的 key 跳过 lockManager 的冲突检查：

```go
// VerifySimulation 中的锁检查逻辑
if state.ChainPatchRef != nil {
    // 链式交易：对 patch 中的 key 跳过锁冲突
    // nonce 排序保证区块内执行顺序
    // 不需要 lockManager 的互斥保护
    skipLockCheck = true
}
```

### 8.2 上游一致性检查

```go
// VerifySimulation 中的一致性检查
if state.ChainPatchRef != nil {
    // 从 patch 链读取期望值
    expectedVal := rs.readPatchChain(...)
    // 从 stateDB 读取实际值
    actualVal := stateDB.GetState(address, key)
    // 不匹配 → 上游失败 → 自己也失败
    if expectedVal != actualVal {
        return ErrUpstreamFailed
    }
}
```

---

## 9. 清理机制

### 9.1 正常完成

| 事件 | 动作 |
|------|------|
| 节点 CR 完成 | 删除 `patches[txHash][simNum]` |
| 叶子节点 CR 完成 | 删除 patch + 从 TempLockView 解锁 |
| 节点 CR 完成 + 有下游 | 删 patch（下游自验证 stateDB） |

### 9.2 失败清理

| 事件 | 动作 |
|------|------|
| VerifySimulation 失败 | 回滚该节点及下游 |
| Patch vs stateDB 不匹配 | 自动失败，等同于 VerifySimulation 失败 |
| 交易超时/关闭 | `closeTransaction` → `StaleTx` → 删 patch |

### 9.3 隐式传播

不需要显式的"上游失败通知"。每个节点在 VerifySimulation 时自行检查 Patch vs stateDB。上游失败 → stateDB 值不匹配 → 下游自动失败。

---

## 10. 跨区块链延伸

链可以跨区块延伸。每个区块内有一段线性链，区块之间通过 `chainNextSim` 连接：

```
Block N:   SimTx1 提交 → chainNextSim → SimTx2
Block N+1: SimTx1 上链 CR → SimTx2 提交 → chainNextSim → SimTx3
Block N+2: SimTx2 上链 CR → SimTx3 提交 → ...
```

- patch 链跨区块连接：`patches[tx3][1]` → `patches[tx2][1]` → `patches[tx1][1]`
- TempLockView 在整个链存活期间保持锁
- VerifySimulation 的 Patch vs stateDB 检查在跨区块场景同样有效

---

## 11. RetrySignal 命名与聚合

### 11.1 命名变更

```
ReSimulationSignal → RetrySignal
Method_HandleReSimulationSignal → Method_HandleRetrySignal
HandleReSimulationSignal → HandleRetrySignal
```

### 11.2 聚合路径

正常 retry 和链式 retry 共用同一个 `RetrySignal` 结构和 `HandleRetrySignal` 入口，通过 `ChainPatch == nil` 区分：

| 路径 | ChainPatch | 行为 |
|------|-----------|------|
| 正常 retry | `nil` | 走现有信号聚合（`setSignal` + `readyCnt` → `tryToReSimulation`） |
| 链式 retry | 非 nil | 存储 ChainNode 到 patches → 走同样的信号聚合 |

---

## 12. RPC 接口

```go
// ssc/api/sscs.go
SSCServiceServer interface {
    // ... 现有接口 ...

    // HandleRetrySignal receives a retry signal from another shard.
    // If ChainPatch is set, stores the chain node for GetState patch reads.
    HandleRetrySignal(signal *RetrySignal)
}
```

---

## 13. 文件变更清单

| 文件 | 变更 | 优先级 | 状态 |
|------|------|:------:|:----:|
| `ssc/api/types.go` | `ReSimulationSignal` → `RetrySignal`；`ReSimulationSignals` → `RetrySignals`；新增 `ChainNode`, `TxSimKey` 结构体；`CXTSimulationState` 加 `ChainPatchRef`、保留 `ChainPatch` | P0 | ✅ 已实现 |
| `ssc/api/sscs.go` | `Method_HandleChainSimSignal` → `Method_HandleRetrySignal`；接口签名 `HandleRetrySignal(signal *RetrySignal)` | P0 | ✅ 已实现 |
| `ssc/retry_scheduler.go` | 新增 `patches` 字段、`chainNextSim()`、`dependsOn()`、`sendChainSignal()`、`readPatchChain()`、`GetChainPatchRef()`；`HandleChainSimSignal` → `HandleRetrySignal`；`AddToRetry` 内化 RWSet 提取；追加 `GetSimState` accessor | P0 | ✅ 已实现 |
| `ssc/state_impl.go` | ~~`GetState` 加递归 patch 读取路径~~ → 已删除。`GetState` 迁至 `simulator.go`，通过 `sim.sscService.retryScheduler` 访问 patches | P0 | ✅ 已实现 |
| `ssc/simulator.go` | 新增 `HandleCXTRecallProof()`、`BuildCallStates()`；迁入所有模拟执行期 state 方法（`GetCallState`/`GetRWSet`/`GetState`/`SetState`/`GetBalance`/`AddBalance`/`SubBalance`/`CreateAccount`/`EndCTX`） | P0 | ✅ 已实现 |
| `ssc/verify.go` | VerifySimulation 加锁跳过 + Patch vs stateDB 一致性检查；迁入验证期 state 方法（`SubSimuBalance`/`AddSimuBalance`/`GetSimuBalance`/`GetSimuState`/`SetSimuState`） | P0 | ✅ 已实现 |
| `ssc/impl.go` | `chainNextSim` 触发（`CommitSimulation` 末尾，SimTx 提交后）；`HandleRetrySignal` 委托；`AddRetryTx` 简化为委托；`buildSignaturesForSimulation` 签名改为 `(ctx context.Context, ...)`；移除所有 `getState` 调用 | P0 | ✅ 已实现 |
| `rpc/ssc.go` | RPC 方法重命名：`HandleChainSimSignal` → `HandleRetrySignal` | P0 | ✅ 已实现 |
| `ssc/simulator_leader.go` | `aggregateCXSSCCallResult` 添加 BLS 聚合签名（修复 `empty bitmap` bug） | P0 | ✅ 已实现 |

---

## 14. 设计决策记录

| # | 决策 | 结论 | 理由 |
|---|------|------|------|
| D1 | 链结构 | 线性链 | 分片场景中多 signal 处理复杂，收到一个就立即尝试 |
| D2 | 触发点 | SimTx 提交后 | 不等 CR，减少 2-3 区块等待 |
| D3 | Patch 存储 | `patches[txHash][simNum]` 链式指针 | 每节点只存自己的 WriteSet + 上游指针，O(n) 存储 |
| D4 | 读取路径 | 递归查 patches，读到就停 | 不需要合并扁平 patch |
| D5 | 锁跳过 | ChainPatch → VerifySimulation 跳过锁冲突 | nonce 排序保证执行顺序 |
| D6 | 上游失败检测 | Patch vs stateDB 不匹配 → 自动失败 | 隐式一致性检查，不需要显式通知 |
| D7 | 回滚语义 | 失败点及下游回滚 | 上游已成功不回滚 |
| D8 | 清理 | CR 完成后删 patch，下游自验证 | 简化生命周期管理 |
| D9 | 跨区块 | 允许链跨区块延伸 | TempLockView 保持锁，Patch vs stateDB 自动处理 |
| D10 | RetrySignal | 聚合，ChainPatch 区分 | 复用信号聚合逻辑 |
|| D11 | 命名 | ReSimulationSignal → RetrySignal | 语义更清晰 |

---

## 16. TODO List

| 优先级 | 任务 | 文件/模块 | 说明 |
|:------:|------|-----------|------|
| **P0** | 跑实验验证 HotKeyRetry | 全链路 | 修复 BLS 签名后重新跑实验，确认 commit rate 回升 |
| **P1** | `commitSimulation` 函数拆分 | `ssc/impl.go` | 当前 ~120 行，`BuildCallStates` 已迁出，剩余签名+chainNextSim 逻辑可再拆分 |
| **P1** | 验证 chainNextSim 触发覆盖率 | `ssc/retry_scheduler.go` | 上次实验 60 次扫描只找到 1 次下游依赖——确认是 retryPool 为空还是依赖判断问题 |
| **P2** | `buildSignaturesForSimulation` 迁入 Simulator | `ssc/impl.go` | 需要 `sscService.BLSSignerMgr`，可通过 accessor 回调 |
| **P2** | 删除 `getState` 兼容函数 | `ssc/impl.go` | 当前零引用，确认无遗留依赖后可删除 |
| **P2** | `HandleReSimulationSignal` 函数重命名 | `ssc/retry_scheduler.go` | 聚合信号路径，旧名残留，后续改为 `HandleRetrySignal` |
| **P0** | **PatchPool v4 实现** | `ssc/retry_scheduler.go` | SimTx 提交后入 Pool，Leader 独立匹配，RetryCommit 取 Patch |

---

# v4 — PatchPool 分片本地池化方案

> **版本**：v4（2026-06-19）
> **目标**：解决当前 HotKeyRetry 链式重试中 HandleRetrySignal(81) → startReSimulation(14) 大量丢失的问题，让所有相关 shard 都能利用 ChainPatch 跳过锁冲突。

## 1. 问题分析

### 1.1 瓶颈诊断（基于 2026-06-19 实验数据）

| 阶段 | 数量 | 流失率 |
|------|:----:|:------:|
| `chainNextSim` 触发 | ~1,049 | - |
| HandleRetrySignal unique txs | **81** | - |
| startReSimulation unique txs | **14** | 83% 丢失 |
| retry commit success | **17** | - |
| TriggerReSimulation | **1** | 93% 丢失 |
| VerifySimulation success (simNum>0) | **0** | 100% |

### 1.2 根因

```text
SimTx1 提交（写 Key K）→ chainNextSim → 匹配 retryTx2（依赖 K）
  └→ HandleRetrySignal（origin shard 存 ChainPatch）
       └→ tryToReSimulation → 各 shard 并发 RetryCommit
            ├─ origin shard: 有 ChainPatch → 跳过锁冲突 ✅
            └─ related shard: 无 ChainPatch → 锁冲突失败 ❌
```

当前 `ChainPatch` 只在 origin shard 存储（`retryScheduler.patches`），其他 related shard 的 `RetryCommit` 看不到它，导致跨 shard retry 锁竞争失败率极高。

## 2. PatchPool 设计

### 2.1 核心原则

| 原则 | 说明 |
|------|------|
| **分片自治** | 每个 shard 的 leader 维护自己的 PatchPool，不跨 shard 同步 |
| **延迟匹配** | SimTx 提交后先进 Pool，不立即扫 retryPool；匹配由独立触发机制完成 |
| **倒排索引** | key → txHash 的倒排结构，O(1) 匹配 |
| **临时独占** | 被取出的 Patch 标记为 Consumed，防止多笔 retryTx 抢同一个 Patch |

### 2.2 数据结构

```go
// PatchPool 是分片 Leader 本地维护的链式 Patch 池。
// 每个 shard 独立一个实例，只存储当前 shard 已提交 SimTx 的 WriteSet。
type PatchPool struct {
    mu      sync.RWMutex

    // patches[txHash] = ChainPatchNode
    patches map[common.Hash]*ChainPatchNode

    // 倒排索引：key → 持有该 key 的 SimTx 集合
    // 用于快速判断一笔 retryTx 是否依赖 Pool 中的某个 SimTx
    keyIndex map[api.LockKey]map[common.Hash]struct{}
}

// ChainPatchNode 是 PatchPool 中的节点。
// 每笔提交的 SimTx 对应一个节点。
type ChainPatchNode struct {
    TxHash    common.Hash   // 自己的 txHash
    Patch     *api.RWSet    // 自己的 WriteSet
    Consumed  bool          // true = 已被某 retryTx 取走
    CreatedAt time.Time     // 入池时间，用于过期清理
}
```

### 2.3 倒排索引匹配算法

```go
// HasConflict 判断 retryTx 是否依赖 Pool 中的某个 SimTx。
// 返回 true + 命中的节点。
func (pp *PatchPool) HasConflict(retryTx *api.RetryTx) (bool, *ChainPatchNode) {
    pp.mu.RLock()
    defer pp.mu.RUnlock()

    // 收集所有 retryTx 涉及 key 的持有者 txHash，按命中次数计数
    candidates := make(map[common.Hash]int)
    for _, key := range retryTx.ReadSet {
        if owners, ok := pp.keyIndex[key]; ok {
            for txHash := range owners {
                candidates[txHash]++
            }
        }
    }
    for _, key := range retryTx.WriteSet {
        if owners, ok := pp.keyIndex[key]; ok {
            for txHash := range owners {
                candidates[txHash]++
            }
        }
    }

    // 选覆盖度最高的（命中最多的 key 的那个上游 SimTx）
    var best common.Hash
    var bestCnt int
    for txHash, cnt := range candidates {
        node, exists := pp.patches[txHash]
        if cnt > bestCnt && exists && !node.Consumed {
            bestCnt = cnt
            best = txHash
        }
    }
    if bestCnt > 0 {
        return true, pp.patches[best]
    }
    return false, nil
}

// Add 将一笔已提交 SimTx 的 WriteSet 加入 PatchPool。
// 同时更新 keyIndex 倒排索引。
func (pp *PatchPool) Add(txHash common.Hash, writeSet *api.RWSet) {
    pp.mu.Lock()
    defer pp.mu.Unlock()

    pp.patches[txHash] = &ChainPatchNode{
        TxHash:    txHash,
        Patch:     writeSet,
        Consumed:  false,
        CreatedAt: time.Now(),
    }

    // 更新 keyIndex
    for addr, state := range writeSet.WriteState.State {
        for key := range state {
            lockKey := api.FormKey(addr, key)
            if pp.keyIndex[lockKey] == nil {
                pp.keyIndex[lockKey] = make(map[common.Hash]struct{})
            }
            pp.keyIndex[lockKey][txHash] = struct{}{}
        }
    }
}

// TryConsume 尝试取出指定 txHash 的 Patch。
// 成功返回 Patch，节点标记为 Consumed。
func (pp *PatchPool) TryConsume(txHash common.Hash) *api.RWSet {
    pp.mu.Lock()
    defer pp.mu.Unlock()

    node, ok := pp.patches[txHash]
    if !ok || node.Consumed {
        return nil
    }
    node.Consumed = true
    return node.Patch
}

// Release 释放被 Consumed 的 Patch（重试失败时回退）。
func (pp *PatchPool) Release(txHash common.Hash) {
    pp.mu.Lock()
    defer pp.mu.Unlock()

    if node, ok := pp.patches[txHash]; ok {
        node.Consumed = false
    }
}
```

### 2.4 匹配时间复杂度分析

| 操作 | 复杂度 | 说明 |
|------|:------:|------|
| `Add(writeSet)` | O(k) | k=writeSet 中 key 的数量（通常 1-2） |
| `HasConflict(retryTx)` | O(r) | r=retryTx 中 key 的数量（通常 1-5） |
| `TryConsume(txHash)` | O(1) | 哈希表直接定位 |

匹配时间几乎恒定在 O(1~5)，**不是性能瓶颈**。

## 3. 流程变更

### 3.1 Phase 1: SimTx 提交 → PatchPool 写入

```text
CommitSimulation → tx.SubmitSimulationTx() 成功
  └→ afterSubmitSimulation(txHash, writeSet)
       ├─ (旧) chainNextSim(txHash, writeSet)  ← 仍然保留作为后备
       └─ (新) patchPool.Add(txHash, writeSet)  ← 入池
```

**不删除 `chainNextSim`**，保留作为实时匹配路径（低延迟优先）。PatchPool 作为辅助路径。

### 3.2 Phase 2: 独立匹配触发

```go
// OnPatchPoolUpdated 在每次 Add 后触发，也可定时触发。
func (rs *retryScheduler) OnPatchPoolUpdated() {
    rs.mu.RLock()
    pool := rs.patchPool  // 读快照
    retryPool := rs.retryPool
    rs.mu.RUnlock()

    for txHash, retryTx := range retryPool {
        matched, node := pool.HasConflict(retryTx)
        if matched {
            // 临时独占：标记 Consumed 防止其他 retryTx 抢
            pool.TryConsume(node.TxHash)
            // 发 RetrySignal（带上本地 Patch）
            rs.sendChainSignal(txHash, retryTx, node.Patch)
        }
    }
}
```

### 3.3 Phase 3: RetryCommit — 取本地 Patch 跳过锁

```go
func (s *sscService) RetryCommit(txHash common.Hash) *api.RetryCommitResp {
    // ... 现有逻辑 ...

    // 新：检查本地 PatchPool 是否有该 tx 可用的 Patch
    if patch := s.retryScheduler.patchPool.TryConsume(txHash); patch != nil {
        // 有匹配的 Patch → 跳过 TempLockView + stateLocker 的锁冲突检查
        // 直接将 Patch 应用到 simState.ChainPatch
        s.retryScheduler.SetChainPatch(txHash, patch)
        return &api.RetryCommitResp{Locked: true, TxHash: txHash}
    }

    // 旧：走正常锁竞争路径
    // ...
}
```

### 3.4 Phase 4: 清理

| 事件 | 动作 |
|------|------|
| SimTx CR 完成（成功） | 从 Pool 删除 `patches[txHash]`，清理 keyIndex |
| retryTx 重试失败 | `Release(txHash)` — 释放 Consumed 标记，允许其他 retryTx 取走 |
| 交易超时关闭 | `closeTransaction` → `StaleTx` → 从 Pool 清理 |

## 4. 端到端数据流

```text
Phase 1: SimTx 提交
───────────────────
SimTx1 提交（写 Key K）
  ├─ afterSubmitSimulation → chainNextSim(后备)
  └─ patchPool.Add(SimTx1, writeSet={K=v1})
       ├─ patches[SimTx1] = {Patch: {K=v1}, Consumed: false}
       └─ keyIndex[K] = {SimTx1}

Phase 2: 匹配触发
────────────────
OnPatchPoolUpdated() → 扫描 retryPool
  └─ retryTx2(依赖 K) → HasConflict(retryTx2) → 命中共 1 个 key(K)
       └─ TryConsume(SimTx1) → patches[SimTx1].Consumed = true
       └─ sendChainSignal(retryTx2, Patch={K=v1})

Phase 3: Origin shard 响应
───────────────────────────
HandleRetrySignal → setSignal → readyCnt 聚合
  └─ readyCnt == len(RelatedShards)?
       ├─ Yes → tryToReSimulation(Tx2)
       │          ├─ RetryCommit on each shard:
       │          │   ├─ shard A: 本地 PatchPool.TryConsume(Tx2) → nil (Patch 不在本 shard)
       │          │   │   └─ 正常锁竞争
       │          │   ├─ shard B: 本地 PatchPool (写 K 的 shard):
       │          │   │   └─ TryConsume(Tx2) → 找到 SimTx1 的 Patch → 跳过锁 ✅
       │          │   └─ shard C: 同 shard A, 正常锁竞争
       │          └─ 全部 locked:true → TriggerReSimulation
       └─ No → 等待

Phase 4: 链上 VerifySimulation
────────────────────────────────
SimTx2 VerifySimulation:
  ├─ ChainPatchRef → readPatchChain 读上游 WriteSet
  ├─ 跳过锁冲突检查（因为 ChainPatchRef 非 nil）
  └─ Patch vs stateDB 隐式一致性检查

Phase 5: 清理
─────────────
SimTx1 CR 完成 → patchPool.Remove(SimTx1) → keyIndex 清理
SimTx2 重试失败 → patchPool.Release(SimTx1) → 允许其他 retryTx 取用
```

## 5. 设计决策记录

| # | 决策 | 结论 | 理由 |
|---|------|------|------|
| D12 | PatchPool 范围 | 每 shard 独立 | 避免跨 shard 同步复杂度，Leader 自治 |
| D13 | 倒排索引 | key → {txHash} | O(r) 匹配时间，r = retryTx 的 key 数（通常 1-5） |
| D14 | Consumed 语义 | 临时独占，失败释放 | 防止多笔 retryTx 同时抢同一个 Patch |
| D15 | 保留 chainNextSim | 保留 | 低延迟路径（实时匹配）和批量匹配路径（OnPatchPoolUpdated）共存 |
| D16 | 匹配触发 | OnPatchPoolUpdated（Add 后触发） | 不需要定时扫描，事件驱动 |
| D17 | RetryCommit 取 Patch | TryConsume 出本地 Patch | 成功取出就跳过锁冲突，取不到走正常路径 |

## 6. 文件变更清单

| 文件 | 变更 | 优先级 | 估算 |
|------|------|:------:|:----:|
| `ssc/retry_scheduler.go` | 新增 `PatchPool` 结构体 + `Add`/`HasConflict`/`TryConsume`/`Release`；新增 `OnPatchPoolUpdated`；`RetryCommit` 加 Patch 检查分支 | P0 | ~200 行 |
| `ssc/impl.go` | `CommitSimulation` 末尾加 `patchPool.Add`；`afterSubmitSimulation` 逻辑扩展 | P0 | ~10 行 |
| `ssc/verify.go` | 无需改动（ChainPatchRef 逻辑已存在） | - | - |
| `ssc/api/types.go` | 新增 `PatchPool` 结构体定义 | P1 | ~30 行 |

## 7. 预期效果

- **HandleRetrySignal** → **startReSimulation** 转化率提升：Patch 在写 key 的 shard 本地可用，至少该 shard 的 RetryCommit 能跳过锁冲突 ✅
- **startReSimulation** → **TriggerReSimulation** 转化率提升：不保证全部 shard 都有 Patch，但只要 Patch 所在的 shard 锁冲突跳过，其他 shard 竞争到的概率增大
- **VerifySimulation chain tx** 成功率：不变（现有 ChainPatchRef 机制即可）

