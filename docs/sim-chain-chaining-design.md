# Sim Chain Chaining Design (Linear)

## 1. Problem Statement

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

## 2. 核心概念

### 2.1 链式执行（Chained Execution）

```
同一区块 Block N（按 nonce 升序）：

SimTx(crN+1) → 锁 K → 读 patch → 执行 → K=v1
SimTx(crN+2) → 锁 K → 读 patch(SimTx1) → 基于 v1 执行 → K=v2
SimTx(crN+3) → 读 v2 → ...
```

- SimTx 之间通过 **nonce 排序**保证区块内执行顺序
- 下游 SimTx 的 `GetState` 通过 `RetryScheduler.patches` 递归读取上游 WriteSet
- 区块执行时 stateDB 按 nonce 递增依次写入，下游自然看到上游执行结果

### 2.2 线性链（非 DAG）

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

## 3. 依赖链构建

### 3.1 触发时机

`chainNextSim` 在 **SimTx 提交到 mempool 后**触发：

```go
// 触发位置：SubmitSimulationTx 成功返回后
func afterSubmitSimulation(txHash common.Hash, writeSet *api.RWSet) {
    s.retryScheduler.chainNextSim(txHash, writeSet)
}
```

### 3.2 依赖定义

一笔 retry tx **依赖**一笔已提交的 SimTx，当且仅当：

```
(retryTx.ReadSet ∪ retryTx.WriteSet) ∩ SimTx.WriteSet ≠ ∅
```

### 3.3 chainNextSim 算法

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

## 4. 数据结构变更

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

## 5. 数据流端到端

### 5.1 完整流程

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

### 5.2 失败场景

```
Phase 3 变体: SimTx2 VerifySimulation 失败
────────────────────────────────────────────
SimTx1 → 成功 → CR → 删 patch
SimTx2 → Patch vs stateDB 不匹配（或模拟失败）→ 回滚
SimTx3 → 读 patch(tx2) → Patch vs stateDB 不匹配 → 自动失败 → 回滚

结果: SimTx1 成功, SimTx2+SimTx3 回滚
```

---

## 6. GetState 的 Patch 读取路径

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

## 7. VerifySimulation 的链式处理

### 7.1 锁跳过

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

### 7.2 上游一致性检查

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

## 8. 清理机制

### 8.1 正常完成

| 事件 | 动作 |
|------|------|
| 节点 CR 完成 | 删除 `patches[txHash][simNum]` |
| 叶子节点 CR 完成 | 删除 patch + 从 TempLockView 解锁 |
| 节点 CR 完成 + 有下游 | 删 patch（下游自验证 stateDB） |

### 8.2 失败清理

| 事件 | 动作 |
|------|------|
| VerifySimulation 失败 | 回滚该节点及下游 |
| Patch vs stateDB 不匹配 | 自动失败，等同于 VerifySimulation 失败 |
| 交易超时/关闭 | `closeTransaction` → `StaleTx` → 删 patch |

### 8.3 隐式传播

不需要显式的"上游失败通知"。每个节点在 VerifySimulation 时自行检查 Patch vs stateDB。上游失败 → stateDB 值不匹配 → 下游自动失败。

---

## 9. 跨区块链延伸

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

## 10. RetrySignal 命名与聚合

### 10.1 命名变更

```
ReSimulationSignal → RetrySignal
Method_HandleReSimulationSignal → Method_HandleRetrySignal
HandleReSimulationSignal → HandleRetrySignal
```

### 10.2 聚合路径

正常 retry 和链式 retry 共用同一个 `RetrySignal` 结构和 `HandleRetrySignal` 入口，通过 `ChainPatch == nil` 区分：

| 路径 | ChainPatch | 行为 |
|------|-----------|------|
| 正常 retry | `nil` | 走现有信号聚合（`setSignal` + `readyCnt` → `tryToReSimulation`） |
| 链式 retry | 非 nil | 存储 ChainNode 到 patches → 走同样的信号聚合 |

---

## 11. RPC 接口

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

## 12. 文件变更清单

| 文件 | 变更 | 优先级 |
|------|------|:------:|
| `ssc/api/types.go` | `ReSimulationSignal` → `RetrySignal`；新增 `ChainNode`, `TxSimKey` 结构体；`CXTSimulationState` 加 `ChainPatchRef` | P0 |
| `ssc/api/sscs.go` | RPC 方法重命名；接口更新 | P0 |
| `ssc/retry_scheduler.go` | 新增 `patches` 字段、`chainNextSim()`、`readPatchChain()`；`HandleChainSimSignal` → `HandleRetrySignal`；清理旧 `chainHotKeyCR`/`HandleHotKeyRetrySignal`/`hotKeySet` | P0 |
| `ssc/state_impl.go` | `GetState` 加递归 patch 读取路径 | P0 |
| `ssc/impl.go` | `afterSubmitSimulation` 触发 `chainNextSim`；`HandleRetrySignal` 委托；清理旧 RPC 注册 | P0 |
| `ssc/verify.go` | VerifySimulation 加锁跳过 + Patch vs stateDB 一致性检查 | P0 |

---

## 13. 设计决策记录

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
| D11 | 命名 | ReSimulationSignal → RetrySignal | 语义更清晰 |
