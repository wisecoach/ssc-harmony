# [DSN-23] PatchPool DAG — 多 Patch 联合覆盖冲突 key

> **版本**：v1（2026-07-08）
> **状态**：设计阶段
> **关联文档**：`designs/active/DSN-22-retrycommit-lock-order-fix.md`（DSN-22 v2 基线）、`designs/active/DSN-05-hotkey-retry-design.md`（PatchPool 原始设计）

---

## 1. Problem Statement

### 1.1 现状：PatchPool 命中率低

DSN-22 v2 将 PatchPool 定位从「无条件跳锁」改为「仅覆盖 stateDB 冲突 key」，但 `FindCovering` 要求**单笔 Patch 覆盖全部冲突 key**。

多 key 交易几乎无法命中：

```
单 key 交易（1 个冲突 key）：        命中率 ≈ 22.9%（≈ chainNextSim reservation completed 比例）
双 key 交易（2 个冲突 key）：        命中率 ≈ 22.9%² ≈ 5%
三 key 交易（3 个冲突 key）：        命中率 ≈ 22.9%³ ≈ 1.2%
```

**实验数据**（DSN-22 v2，4 shards）：

| 指标 | 值 |
|:-----|:---:|
| HandleRetrySignal | 105,602 次 |
| PatchPool FindCovering 命中 | **214 次**（各 shard 合计） |
| 命中率 | 0.2% |
| LockWait 进池 | 1,481 次 |
| 提交率 | 80.1%（vs 基线 96.1%） |

gap 约 16% 的瓶颈是：多 key 交易在 Phase 2b 失败后进 LockWait，等待 stateDB 解锁再重试——这个过程在热 key 场景下反复排队长。

### 1.2 目标

**允许一组 Patch 联合覆盖全部冲突 key**，而非只能单笔 Patch：

```
冲突 key = {K, J, I}

v2（当前）:
  FindCovering → 找覆盖 {K,J,I} 的单笔 Patch → 没有 → LockWait
  
v3（DAG）:
  FindCoveringSet → 找覆盖 K+J 的 PatchA + 覆盖 I 的 PatchB
                    → TryConsume(PatchA) + TryConsume(PatchB)
                    → 合并 WriteSet → Locked=true ✅
```

---

## 2. 设计方案

### 2.1 核心思路：扁平的 PatchPool + 消费者侧 DAG

PatchPool 保持**扁平无结构**——每个 Patch 是独立原子，没有「这几笔 Patch 组成一个 DAG 节点」的概念。DAG 关系只存在消费侧：

```
PatchPool（扁平，不变）
├── PatchA {K: v1}          ← 各自独立
├── PatchB {J: v2, K: v3}
└── PatchC {I: v4}

consumedPatches（消费者侧 ✅ 新增）:
  retryTx1 → [PatchA, PatchB]  ← DAG 关系存在这里
  retryTx2 → [PatchC]
```

### 2.2 数据流

```
Phase 2b: 冲突 key = {K, J, I}

FindCoveringSet(conflictKeys) ──→ 贪心匹配 → [PatchA(覆盖K+J), PatchC(覆盖I)]

TryConsume(PatchA) + TryConsume(PatchC)
  ├─ 任一失败 → Release 全部已消费的 → 返回 false
  └─ 全部成功 → merged = mergeRWSet(PatchA.WriteSet, PatchC.WriteSet)
              → consumedPatches.Store(retryTx, [PatchA.TxHash, PatchC.TxHash])
              → SetChainPatch(retryTx, merged)
              → Locked=true ✅

TriggerReSimulation → SimTx 携带 UpstreamTxList:
  simulation.UpstreamTxList = [
    {TxHash: PatchA.TxHash, SimulationNum: ...},
    {TxHash: PatchC.TxHash, SimulationNum: ...},
  ]

链上 VerifySimulation:
  遍历 retryTx 的 RWSet key:
    if key 来自上游 Patch:
      for each upstream in simulation.UpstreamTxList:
        ReadOnChainPatch(upstream, addr, key) → 查任意一个上游能否提供期望值
```

---

## 3. 数据结构变更

### 3.1 CXTSimulation

| 现在 | DAG 后 |
|:-----|:--------|
| `UpstreamTxHash common.Hash` | `UpstreamTxList []TxSimKey` |
| `UpstreamSimNum int` | 合并为 UpstreamTxList |

```go
// 改前
type CXTSimulation struct {
    // ...
    UpstreamTxHash common.Hash  `json:"upstream_tx_hash,omitempty"`
    UpstreamSimNum int          `json:"upstream_sim_num,omitempty"`
    ChainPatch     *RWSet       `json:"chain_patch,omitempty"`
}

// 改后
type CXTSimulation struct {
    // ...
    UpstreamTxList []TxSimKey   `json:"upstream_tx_list,omitempty"`
    ChainPatch     *RWSet       `json:"chain_patch,omitempty"`
}
```

### 3.2 ChainNode（链下 patches/onChainPatches 存储）

| 现在 | DAG 后 |
|:-----|:--------|
| `UpstreamTxHash common.Hash` | `UpstreamTxList []TxSimKey` |
| `UpstreamSimNum int` | 合并 |

### 3.3 TxSimKey（已有结构，无需新增）

```go
type TxSimKey struct {
    TxHash        common.Hash `json:"tx_hash"`
    SimulationNum int         `json:"simulation_num"`
}
```

够用，无需修改。

### 3.4 consumedPatches

| 现在 | DAG 后 |
|:-----|:--------|
| `value: common.Hash` | `value: []common.Hash` |
| 存单一上游 txHash | 存所有上游 txHash 列表 |

---

## 4. 算法：FindCoveringSet（贪心集合覆盖）

### 4.1 API

```go
// FindCoveringSet 返回一组 Free Patch，联合覆盖全部 conflictKeys。
// 贪心近似：每轮选覆盖最多「未覆盖 key」的 Patch。
// 返回 nil = 无法覆盖全部 key。
func (pp *PatchPool) FindCoveringSet(conflictKeys []LockKey) []*ChainPatchNode
```

### 4.2 算法

```
输入：conflictKeys = {K, J, I}
      KeyIndex =
        K → {PatchA, PatchB}
        J → {PatchB}
        I → {PatchC}

Step 1: 用 KeyIndex 建可用 Patch 表
  for each key in conflictKeys:
    if key not in KeyIndex → 返回 nil（该 key 无任何 Patch）

Step 2: 贪心选 Patch
  remaining = {K, J, I}
  result = []
  
  while remaining != ∅:
    // 找覆盖最多 remaining key 的 Free Patch
    best = argmax |{k ∈ remaining | Patch.k ∈ KeyIndex[k]}|
           for each Free Patch
    
    if best == nil or |best.covered(remaining)| == 0 → 返回 nil（无法继续）
    
    result.append(best)
    remaining -= best.covered(remaining)

Step 3: 返回 result
```

### 4.3 时间复杂度

| 操作 | 复杂度 | 说明 |
|:-----|:------:|:------|
| 建可用 Patch 表 | O(K·N) | K=冲突 key 数(≤5)，N=每 key owner 数(≤5) |
| 贪心循环 | O(K·P) | P=候选 Patch 数(≤K·N) |
| **总计** | **O(K²·N)** | 常数级，秒出 |

### 4.4 消费逻辑

```go
patches := rs.patchPool.FindCoveringSet(conflictKeys)
if len(patches) == 0 {
    goto LockWait  // 无法覆盖 → 走 LockWait
}

// 原子消费
var consumed []common.Hash
var merged *api.RWSet
for _, node := range patches {
    patch := rs.patchPool.TryConsume(node.TxHash, txHash, priority)
    if patch == nil {
        // 某个 Patch 已被其他 retryTx 抢了 → 释放已消费的全部
        for _, txh := range consumed {
            rs.patchPool.Release(txh)
        }
        goto LockWait
    }
    consumed = append(consumed, node.TxHash)
    merged = mergeRWSet(patch, merged)  // mergeRWSet 已存在
}

// 全部消费成功
rs.state.SetChainPatch(txHash, merged)
rs.consumedPatches.Store(txHash, consumed)
rs.tempLockView.ClearWounded(txHash)
return &RetryCommitResp{Locked: true, TxHash: txHash}
```

---

## 5. VerifySimulation — DAG 链上验证

### 5.1 链式交易判断

```go
// 改前
if simulation.UpstreamTxHash != (common.Hash{}) {

// 改后
if len(simulation.UpstreamTxList) > 0 {
```

### 5.2 一致性检查

```go
// 改前 — 单上游链式递归
if simulation.UpstreamTxHash != (common.Hash{}) {
    expectedVal, found := v.retrySchd.ReadOnChainPatch(
        simulation.UpstreamTxHash,
        simulation.UpstreamSimNum,
        address, key)
    // ...
}

// 改后 — DAG 遍历所有上游
if len(simulation.UpstreamTxList) > 0 {
    // 只查 retryTx 实际用到的 key，不显式验证所有上游的存在
    for _, upstream := range simulation.UpstreamTxList {
        expectedVal, found := v.retrySchd.ReadOnChainPatch(
            upstream.TxHash, upstream.SimulationNum, address, key)
        if found {
            // 匹配检查
            if expectedVal == currentVal { /* 通过 */ }
            break
        }
    }
}
```

### 5.3 ReadOnChainPatch — 多上游查寻

```go
// 改前：递归单链
func ReadOnChainPatch(upstreamTxHash, simNum, address, key) (common.Hash, bool) {
    if node.UpstreamTxHash != (common.Hash{}) {
        return ReadOnChainPatch(node.UpstreamTxHash, node.UpstreamSimNum, ...)
    }
    return common.Hash{}, false
}

// 改后：遍历所有上游
// 注意：ReadOnChainPatch 现在被调用方循环调用，函数本身不再递归
// 它只查「该 SimTx 在 onChainPatches 中的 WriteSet 是否包含 addr:key」
func ReadOnChainPatch(upstreamTxHash, simNum, address, key) (common.Hash, bool) {
    // 查 onChainPatches[upstreamTxHash][simNum] 的 WriteSet
    // 存在 → 返回值
    // 不存在 → 在 UpstreamTxList 中递归（如该节点也有多上游）
}
```

---

## 6. WriteSet 合并语义

`mergeRWSet(old, new)` 已存在（`retry_scheduler.go:1472`），逻辑：new 覆盖 old。

```go
// 调用方：新 Patch（后入池）覆盖旧 Patch（先入池）
merged = mergeRWSet(newPatch, merged)  // newPatch 覆盖 merged
```

同 key 冲突实际不会发生（见决策 D23-04），dict merge 即可。

---

## 7. 文件变更清单

| 文件 | 变更 | 行数 | 复杂度 |
|:-----|:------|:----:|:------:|
| `ssc/api/types.go` — `CXTSimulation.UpstreamTxHash/SimNum` → `UpstreamTxList` | 删2行增1行 | ~3 | 简单 |
| `ssc/api/types.go` — `ChainNode.UpstreamTxHash/SimNum` → `UpstreamTxList` | 删2行增1行 | ~3 | 简单 |
| `ssc/api/types.go` — 新增 `FindCoveringSet` | 贪心集合覆盖算法 | ~35 | 中等 |
| `ssc/retry_scheduler.go` — `consumedPatches` 注释 | 值类型语义说明 | ~1 | 简单 |
| `ssc/retry_scheduler.go` — `RetryCommit` Phase 2b | 循环消费+合并 WriteSet | ~20 | 中等 |
| `ssc/retry_scheduler.go` — `staleTx` 失败释放 | range Release | ~5 | 简单 |
| `ssc/retry_scheduler.go` — `ReadOnChainPatch` | 多上游查寻 | ~15 | 中等 |
| `ssc/retry_scheduler.go` — `AddOnChainPatch` 参数 | `upstreamTxList` | ~3 | 简单 |
| `ssc/retry_scheduler.go` — `sendChainSignal` 参数 | `upstreamTxList` | ~3 | 简单 |
| `ssc/retry_scheduler.go` — `GetUpstreamTxRef` 返回 | `[]TxSimKey` | ~5 | 简单 |
| `ssc/retry_scheduler.go` — `readPatchChain` | 多上游 | ~5 | 中等 |
| `ssc/retry_scheduler.go` — `OnPatchPoolUpdated` | 调用 FindCoveringSet | ~15 | 中等 |
| `ssc/verify.go` — `isChainTx` 判断 | `UpstreamTxList` | ~1 | 简单 |
| `ssc/verify.go` — 链式一致性检查 | 遍历所有上游 | ~10 | 中等 |
| `ssc/verify.go` — `AddOnChainPatch` 调用 | 参数变更 | ~1 | 简单 |
| `ssc/impl.go` — `CommitSimulation` 上游引用 | 返回 `[]TxSimKey` | ~5 | 简单 |
| `ssc/impl.go` — SimTx 构建 | `UpstreamTxList` | ~2 | 简单 |
| `ssc/impl.go` — `AddOnChainPatch` 调用 | 参数变更 | ~1 | 简单 |
| `ssc/impl.go` — `closeTransaction` 清理 | consumedPatches 释放 | ~5 | 简单 |

**合计：~138 行变更，4 个文件。复杂度中等。**

---

## 8. 决策记录

| ID | 决策项 | 结论 | 理由 |
|:---|:-------|:------|:------|
| D23-01 | SimTx 上游结构 | `UpstreamTxList []TxSimKey` | 自描述，不增加 SimTx 大小显著（≤5 个上游） |
| D23-02 | 候选 Patch 已被消费 | TryConsume 逐个试，失败跳过 | 不改 PatchPool 原子性 |
| D23-03 | 同 key 合并优先级 | dict merge，后入池的覆盖先入池的 | 实际不会发生（同一 key 的锁被第一笔 SimTx 持有，后续 SimTx 只能依赖它） |
| D23-04 | consumedPatches 值类型 | `common.Hash` → `[]common.Hash` | 单 map、单 lookup，不引入新数据结构 |
| D23-05 | 已消费 Patch 留在 Pool | 匹配时过滤 `Status != Free` | 不改 Release/清理逻辑，匹配多几步无用查询但常数级 |
| D23-06 | DAG 维护位置 | `consumedPatches` + SimTx.`UpstreamTxList`，Pool 自身扁平 | 不污染 Pool 的独立原子语义 |
| D23-07 | onChainPatches | 存 `UpstreamTxList`，读取时查所有上游 | 节点共享数据 |
| D23-08 | VerifySimulation 查上游 | 隐式检查 — 只验证 retryTx RWSet 中来自上游的 key | 不检查上游写了但 retryTx 没碰的 key |
| D23-09 | 匹配算法 | 贪心集合覆盖（每轮选覆盖最多未覆盖 key 的 Free Patch） | K≤3、P≤多个，贪心足够；不追求最小集合 |

---

## 9. 风险与边界

### 9.1 已知风险

| 风险 | 影响 | 缓解 |
|:-----|:------|:------|
| 贪心算法不保最小集合 | 多消费了不必要的 Patch，释放时也更复杂 | K≤3，P 很小，非瓶颈 |
| consumedPatches 在 closeTransaction 未清理 | 成功提交的 retryTx 残留 Consumed 状态 | 在 closeTransaction 中补清理 |
| ReadOnChainPatch 多上游语义从「链式保证」变为「可能部分覆盖」 | 查不到就返回 false | 函数已设计为查不到返回 false，兼容 |

### 9.2 不做

- 不做 Patch 间依赖排序（同一 Patch 出现在两组不同 DAG 中的协调）
- 不做 DAG 的拓扑排序（Patch 的写入顺序由入池时间决定，合并时后入池者优先）
- 不做最小覆盖集最优解（NP-hard，贪心足够）
