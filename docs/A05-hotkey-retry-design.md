# [A05] HotKey 链式重试（数据层）

> **阅读顺序**（`[前缀]` 表示阅读顺序）：
> 1. 📄 `docs/A01-sscc-refactor-plan.md` [A01] — SSCC 模块化重构总览
> 2. 📄 `docs/A02-lock-retry-mechanism.md` [A02] — 锁与重试机制
> 3. 📄 `docs/A03-CR-priority-optimization.md` [A03] — CR 提交优先级优化
> 4. 📄 `docs/A04-onchain-retry-limit.md` [A04] — 链上重试次数限制
> 5. 📄 `docs/A05-hotkey-retry-design.md` [A05] **（本文）** — HotKey 链式重试（数据层）
> 6. 📄 `docs/A06-lock-priority-coordination.md` [A06] — 锁优先级协调（协调层）
> 7. 📄 `docs/A07-force-simulation.md` [A07] — ForceSimulation 冲突容忍模拟（设计阶段，**已暂关**）
>
> **运维辅助**（与主线并行的独立文档）：
> - 📄 `docs/B01-log-query-guide.md` [B01] — 日志查询指南
> - 📄 `docs/B02-log-lifecycle.md` [B02] — 交易生命周期日志大全

> **当前版本**：v4 — PatchPool 分片本地池化方案（2026-06-19）
> **历史版本**：v1（CR 触发+HotKey 分类，`docs/archived/Z01-cr-hotkey-chaining-design.md`）、v2（DAG 树形，`docs/archived/Z02-sim-dag-chaining-design.md`）、v3（线性链，`docs/A05-hotkey-retry-design.md` 上卷）

> **阅读顺序**：本系列文档应按以下顺序阅读，反映机制演进过程：
> 1. 📄 `docs/A05-hotkey-retry-design.md`（本文）— 链式数据依赖与 PatchPool 数据层设计（v1→v4）
> 2. 📄 `docs/A06-lock-priority-coordination.md` — 锁优先级协调：Wound-Wait 跨分片锁竞争解决（v5）

> **实现状态**：v1-v3 已上线实验验证 ✅，v4 已上线实验验证 ✅，v5（Wound-Wait）已上线实验验证 ✅

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

### v6 待办：onChainPatches

| 任务 | 优先级 | 备注 |
|------|:------:|------|
| CXTSimulation 加 `ChainPatch` / `UpstreamTxHash` 字段 | P0 | 所有节点通过 SimTx 同步 Patch |
| `retryScheduler.onChainPatches` 数据结构 | P0 | `map[txHash]map[simNum]*RWSet`，所有节点共享 |
| SimTx 收到后入 `onChainPatches` | P0 | 多播/链上读取时写入 |
| `Committer.CommitOrRollbackWithProof` 中清理 `onChainPatches` | P0 | CR 完成后链上清理 |
| `closeTransaction` 中清理 `patches` / `PatchPool`（链下） | P0 | 已有，不变 |
| VerifySimulation 从 `onChainPatches` 递归读 Patch | P0 | 替代当前 `GetChainPatchRef` |

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
```

---

## 5. 数据结构（v4 PatchPool）

### 5.1 PatchPool

```go
type PatchPool struct {
    mu       sync.RWMutex
    Patches  map[common.Hash]*ChainPatchNode           // SimTx hash → node
    KeyIndex map[LockKey]map[common.Hash]struct{}       // 倒排索引：key → {SimTx hashes}
}

type ChainPatchNode struct {
    TxHash    common.Hash   // SimTx 的 txHash
    Patch     *RWSet        // SimTx 的 WriteSet
    Consumed  bool          // 已被某 retryTx 取走
    CreatedAt time.Time
}
```

### 5.2 onChainPatches（v6 新增）

```go
// retry_scheduler.go
type retryScheduler struct {
    // ...现有字段
    patches         map[common.Hash]map[int]*ChainNode   // 仅 leader：链式触发缓存（链下）
    onChainPatches  map[common.Hash]map[int]*RWSet       // 所有节点：SimTx 同步的 Patch
    patchPool       *PatchPool                            // 仅 leader：锁竞争优化（链下）
}
```

**onChainPatches 与 patches 的职责分工：**

| 组件 | 作用域 | 写入时机 | 清理时机 | 用途 |
|------|:------:|----------|----------|------|
| `patches`（现有） | 仅 leader | `chainNextSim` / `HandleRetrySignal` 发信号时 | `closeTransaction` | 链式触发缓存 |
| `PatchPool`（现有） | 仅 leader | `CommitSimulation` 入池 | `closeTransaction` | 锁竞争优化 |
| **`onChainPatches`（新增）** | **所有节点** | **收到 SimTx（多播）时** | **Committer CR 完成时** | **VerifySimulation 递归查上游 Patch** |

---

### 5.3 CXTSimulation 扩展（v6）

```go
type CXTSimulation struct {
    // ...现有字段
    ChainPatch        *RWSet       `json:"chain_patch,omitempty"`          // 直接上游的 WriteSet
    UpstreamTxHash    common.Hash  `json:"upstream_tx_hash,omitempty"`    // 上游 SimTx hash
    UpstreamSimNum    int           `json:"upstream_sim_num,omitempty"`    // 上游 SimTx 的 simulationNum
}
```

**说明：**
- SimTx 只携带**直接上游**的 Patch，不记录完整链
- VerifySimulation 通过 `UpstreamTxHash` 查 `onChainPatches` 递归获取完整链
- 递归时先确认上游 SimTx 已上链（链上存在性验证），再取 Patch

### 5.4 VerifySimulation 读取路径（v6）

```go
func (v *Verifier) getChainPatch(txHash common.Hash, sim *api.CXTSimulation) *RWSet {
    if sim.ChainPatch == nil {
        return nil  // 不是链式交易
    }
    // 递归从 onChainPatches 合并所有上游 Patch
    return v.retrySchd.MergeChainPatches(sim)
}

// retry_scheduler.go
func (rs *retryScheduler) MergeChainPatches(sim *api.CXTSimulation) *RWSet {
    if sim.UpstreamTxHash == (common.Hash{}) {
        return sim.ChainPatch  // 根节点
    }
    upstream := rs.onChainPatches[sim.UpstreamTxHash][sim.UpstreamSimNum]
    if upstream == nil {
        return sim.ChainPatch  // 上游未同步，只用自己的
    }
    // 递归合并
    upstreamSim := &api.CXTSimulation{
        ChainPatch:     upstream,
        UpstreamTxHash: ???,  // 需要从 onChainPatches 知道上游的 upstream
    }
    merged := MergeChainPatches(upstreamSim)
    return mergeRWSet(sim.ChainPatch, merged)
}
```

> ⚠️ 上述是示意代码，实际需要从 `onChainPatches` 的结构中获知每个 SimTx 的 `UpstreamTxHash`。因此 `onChainPatches` 除了存 `*RWSet` 外，可能需要存 `UpstreamTxHash`，或者直接用 `*CXTSimulation` 的部分字段。具体在实现时确定。

### 5.5 onChainPatches 生命周期

```
写入:
  CommitSimulation 构建 SimTx（含 ChainPatch + UpstreamTxHash）
    → SubmitSimulationTx 多播到所有 shard
    → 所有节点收到 SimTx
    → onChainPatches[txHash][simNum] = ChainPatch   ✅

清理（链上）:
  Committer.CommitOrRollbackWithProof:
    → CR 完成 → rs.RemoveOnChainPatch(txHash)       ✅
    → closeTransaction（清理链下 patches / PatchPool）

清理（链下）:
  closeTransaction（超时/失败）:
    → rs.patchPool.Remove(txHash)                    ✅ 已有的
    → rs.retryScheduler.StaleTx(txHash)              ✅ 已有的（清 patches）
    → onChainPatches 不清理（因为链上未确认，让链上 CR 决定清理）
```

---

## 6. 流程变更（v4 PatchPool）

### 6.1 Phase 1: SimTx 提交 → PatchPool 写入

```text
CommitSimulation → tx.SubmitSimulationTx() 成功
  └→ afterSubmitSimulation(txHash, writeSet)
       ├─ (旧) chainNextSim(txHash, writeSet)  ← 仍然保留作为后备
       └─ (新) patchPool.Add(txHash, writeSet)  ← 入池
```

**不删除 `chainNextSim`**，保留作为实时匹配路径（低延迟优先）。PatchPool 作为辅助路径。

### 6.2 Phase 2: 独立匹配触发

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

### 6.3 Phase 3: RetryCommit — 取本地 Patch 跳过锁

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

### 6.4 Phase 4: 清理

| 事件 | 动作 |
|------|------|
| SimTx CR 完成（成功） | `Committer.CommitOrRollbackWithProof` → `RemoveOnChainPatch(txHash)`；`closeTransaction` → 清 `patches[txHash]`、`PatchPool.Remove` |
| retryTx 重试失败 | `patchPool.Release(txHash)` — 释放 Consumed 标记，允许其他 retryTx 取走 |
| 交易超时关闭 | `closeTransaction` → `StaleTx` → 清 `patches`、`PatchPool.Remove` |

### 6.5 匹配时间复杂度分析

| 操作 | 复杂度 | 说明 |
|------|:------:|------|
| `Add(writeSet)` | O(k) | k=writeSet 中 key 的数量（通常 1-2） |
| `HasConflict(retryTx)` | O(r) | r=retryTx 中 key 的数量（通常 1-5） |
| `TryConsume(txHash)` | O(1) | 哈希表直接定位 |

匹配时间几乎恒定在 O(1~5)，**不是性能瓶颈**。

---

## 7. 端到端数据流

```text
Phase 1: SimTx 提交
───────────────────
SimTx1 提交（写 Key K）
  ├─ CommitSimulation:
  │   ├─ 构建 CXTSimulation{ChainPatch={K=v1}, UpstreamTxHash=..., UpstreamSimNum=...}
  │   ├─ patchPool.Add(SimTx1, writeSet={K=v1})
  │   └─ SubmitSimulationTx → 多播到所有 shard
  └─ 所有节点收到 SimTx:
       └─ onChainPatches[SimTx1][simNum] = {K=v1}

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
  ├─ 从 SimTx 的 UpstreamTxHash → 查 onChainPatches → 递归读上游 Patch
  ├─ 确认上游 SimTx 已上链
  └─ Patch vs stateDB 隐式一致性检查

Phase 5: 清理
─────────────
SimTx1 CR 完成 → Committer.CommitOrRollbackWithProof
  └─ RemoveOnChainPatch(SimTx1)
  └─ closeTransaction → 清 patches、PatchPool.Remove

SimTx2 重试失败 → patchPool.Release(SimTx1) → 允许其他 retryTx 取用
```

---

## 8. 设计决策记录

| # | 决策 | 结论 | 理由 |
|---|------|------|------|
| D12 | PatchPool 范围 | 每 shard 独立 | 避免跨 shard 同步复杂度，Leader 自治 |
| D13 | 倒排索引 | key → {txHash} | O(r) 匹配时间，r = retryTx 的 key 数（通常 1-5） |
| D14 | Consumed 语义 | 临时独占，失败释放 | 防止多笔 retryTx 同时抢同一个 Patch |
| D15 | 保留 chainNextSim | 保留 | 低延迟路径（实时匹配）和批量匹配路径（OnPatchPoolUpdated）共存 |
| D16 | 匹配触发 | OnPatchPoolUpdated（Add 后触发） | 不需要定时扫描，事件驱动 |
| D17 | RetryCommit 取 Patch | TryConsume 出本地 Patch | 成功取出就跳过锁冲突，取不到走正常路径 |
| D18 | onChainPatches vs patches | 分离 | patches 仅 leader 链下缓存，onChainPatches 所有节点共享，职责不同 |
| D19 | onChainPatches 写入时机 | 收到 SimTx 时 | 所有节点统一入池，不依赖 leader 侧流程 |
| D20 | onChainPatches 清理 | Committer CR 完成时 | 链上清理与 CR 绑定，链下清理由 closeTransaction 负责 |
| D21 | SimTx 只带直接上游 Patch | 不记录完整链 | VerifySimulation 通过 onChainPatches 递归查上游，同时验证上游 SimTx 已上链 |

## 9. 文件变更清单

| 文件 | 变更 | 优先级 | 估算 |
|------|------|:------:|:----:|
| `ssc/retry_scheduler.go` | 新增 `PatchPool` 结构体 + `Add`/`HasConflict`/`TryConsume`/`Release`；新增 `OnPatchPoolUpdated`；`RetryCommit` 加 Patch 检查分支；新增 `onChainPatches` 字段 + `AddOnChainPatch`/`GetOnChainPatch`/`RemoveOnChainPatch`/`MergeChainPatches` | P0 | ~300 行 |
| `ssc/impl.go` | `CommitSimulation` 末尾加 `patchPool.Add` + SimTx 构建含 ChainPatch；`afterSubmitSimulation` 逻辑扩展 | P0 | ~20 行 |
| `ssc/committer.go` | `CommitOrRollbackWithProof` 末尾加 `RemoveOnChainPatch` | P0 | ~3 行 |
| `ssc/api/types.go` | `CXTSimulation` 加 `ChainPatch`/`UpstreamTxHash`/`UpstreamSimNum` + `Bytes()` 同步 | P0 | ~15 行 |
| `ssc/verify.go` | `VerifySimulation` 改用 `onChainPatches` 递归读上游 Patch | P0 | ~20 行 |
| `ssc/simulator.go` | `GetState` 读 Patch 路径扩展（查 `onChainPatches` 兜底） | P1 | ~10 行 |

## 10. 预期效果

- **HandleRetrySignal** → **startReSimulation** 转化率提升：Patch 在写 key 的 shard 本地可用，至少该 shard 的 RetryCommit 能跳过锁冲突 ✅
- **startReSimulation** → **TriggerReSimulation** 转化率提升：不保证全部 shard 都有 Patch，但只要 Patch 所在的 shard 锁冲突跳过，其他 shard 竞争到的概率增大
- **VerifySimulation chain tx** 成功率：改进，onChainPatches 让所有节点都能读到上游 Patch
