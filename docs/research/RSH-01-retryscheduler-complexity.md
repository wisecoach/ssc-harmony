# RSH-01: retryScheduler 计算复杂度分析

> **版本**：v1（2026-07-23）
> **范围**：retry_scheduler.go（1637 行）+ patchpool.go（569 行）共 2206 行
> **目标**：识别 CPU 密集型操作，按优化优先级排序，形成后续 DSN 的输入

---

## 1. 总览

```
retryScheduler (1637行)
├── OnBlockCommitted (334行)      ──── 每区块 1 次，全量遍历
├── RetryCommit (215行)            ──── 每 RelatedShard 1 次，含两次 DAG 补救
├── tryToReSimulation (198行)      ──── 每次重试，含并行 RPC + 信号聚合
├── HandleReSimulationSignal (49行)──── 信号聚合后触发
├── HandleRetrySignal (30行)       ──── 链式信号 → 直调 tryToReSimulation
├── findCoveringSet (68行)         ──── 贪心近似覆盖算法
├── scanPatchSubscribers (121行)   ──── 新增 Patch 后全量扫描 subscriber
└── 其他辅助 (patchpool.go 569行)  ──── subscriber/keyIndex/localPatches 操作
```

---

## 2. 优化优先级表格

| 优先级 | 热点 | 文件:行 | 当前复杂度 | 调用频率 | 影响 |
|:------:|:-----|:--------|:-----------|:---------|:-----|
| **P0** | **OnBlockCommitted 日志统计 — 6× 独立 sync.Map.Range 计数** | rs:769-787 | O(N) × 6 次全遍历 | 每区块 1 次 | 高 |
| **P1** | **OnBlockCommitted 热 key 统计（纯日志无业务价值）** | rs:690-718 | O(N×K) | 每区块 1 次 | 高 |
| **P2** | **RetryCommit Phase 1b + Phase 2b 重复 DAG 逻辑** | rs:1236-1273 / rs:1337-1373 | 两段代码 80% 相同 | 每次 RetryCommit | 中 |
| **P3** | **staleTxs 两次独立 Range** | rs:500-510 | O(N) × 2 | 每区块 1 次 | 低 |
| **P4** | **findCoveringSet 嵌套循环（贪心近似）** | pp:330-397 | O(K×P×A) K=keys, P=候选, A=轮数 | 每次 Patch 兜底 | 中 |

---

## 3. P0 — OnBlockCommitted 日志统计 6× Range

### 3.1 问题

`OnBlockCommitted` 中有六组独立的 `sync.Map.Range` 全量遍历，仅用于日志计数：

```go
// rs:769-774 — PatchPool 计数
rs.localPatches.Range(func(_, _ any) bool { localPatchCount++; return true })
rs.patches.Range(func(_, _ any) bool { chainPatchCount++; return true })
rs.keyIndex.Range(func(_, _ any) bool { keyIdxCount++; return true })
rs.subscriber.Range(func(_, _ any) bool { subCount++; return true })
rs.consumedPatches.Range(func(_, _ any) bool { consumedCount++; return true })

// rs:777-782 — 重试系统计数
rs.retryPool.Range(func(_, _ any) bool { retryCount++; return true })
rs.passivePool.Range(func(_, _ any) bool { passiveCount++; return true })
rs.staleTxs.Range(func(_, _ any) bool { staleCount++; return true })
rs.reSimInFlight.Range(func(_, _ any) bool { reSimInFlightCount++; return true })
rs.woundedRetryTxs.Range(func(_, _ any) bool { woundedRetryCount++; return true })

// rs:785-787 — 信号计数
rs.signals.Range(func(_, _ any) bool { signalTxCount++; return true })
rs.onChainPatches.Range(func(_, _ any) bool { onChainPatchCount++; return true })
```

**每次独立出发、6 条 sync.Map 全遍历的并发迭代器。** retryPool 越大，浪费越显著。在 TPS 高的场景（数千笔在 retryPool），这些 Range 本身就成了 CPU 开销。

### 3.2 候选方案

| 方案 | 描述 | 优点 | 缺点 |
|:-----|:------|:-----|:------|
| A | **原子计数器**（atomic.Int64） | 零遍历开销，每次 Store/Delete 时 ±1 | 需修改所有写操作入口（约 8-10 处） |
| B | **合并为一次 Range** | 代码改动小，只减少遍历数 | 仍要遍历所有 sync.Map |
| C | **移到 Debug 级别 + 按间隔采样** | 不删代码，降频 | retryPool 大时仍要调 |
| D | **删掉不用** | 零代价 | 日志可视化时丢失数据 |

---

## 4. P1 — 热 key 统计（rs:690-718）

### 4.1 问题

```go
// 每区块全量 retryPool 遍历 + 每 tx 遍历写集/读集
hotKeyStats := make(map[api.LockKey]int)
rs.retryPool.Range(func(_, txVal interface{}) bool {
    tx := txVal.(*api.RetryTx)
    for _, lockKey := range tx.WriteSet { hotKeyStats[lockKey]++ }
    for _, lockKey := range tx.ReadSet { hotKeyReadStats[lockKey]++ }
    return true
})
// 统计后只写入日志，不影响任何业务逻辑分支
```

**纯日志消耗，没有下游。** 计算最大竞争数 + 热点 key 数量后仅打印一行日志，业务逻辑完全不依赖。

### 4.2 候选方案

| 方案 | 描述 |
|:-----|:------|
| A | **删除** — 直接移除这三段统计代码 |
| B | **按间隔/周期采样** — 每 N 个区块做一次 |
| C | **用原子计数器** — 在 subscribe/unsubscribe 时累计，不遍历 |

---

## 5. P2 — RetryCommit Phase 1b + Phase 2b 重复

### 5.1 问题

`RetryCommit` 有两段几乎完全相同的 PatchPool DAG 补救逻辑：

**Phase 1b（rs:1236-1273）：** TLV TryLock 失败后 → 取 `retryTx.ReadSet ∪ WriteSet` 作为全集 → `findCoveringSet(allKeys)` → 消费 + merge → SetChainPatch → 返回 Locked: true

**Phase 2b（rs:1337-1373）：** stateDB.CheckLock 有冲突后 → 取 `conflictKeys` → `findCoveringSet(conflictKeys)`（限 maxPatches=1） → 消费 + merge → SetChainPatch → 返回 Locked: true

**差异：**
- Phase 1b 用 `ReadSet ∪ WriteSet`（全 key），Phase 2b 用 `conflictKeys`（仅冲突 key）
- Phase 2b 有 `maxPatches = 1` 的限制
- 其余代码（消费 loop、回滚、SetChainPatch）完全一致，复刻了 30 行

### 5.2 候选方案

| 方案 | 描述 |
|:-----|:------|
| A | **提取公共函数** `consumePatchForConflict(targetKeys, maxPatches)` |
| B | **合并两阶段** — Phase 1 通过后 Phase 2 没有 DAG patch 可消费了（因为 TLV 锁已冲突），所以 Phase 1b 可以直接取代 Phase 2b |

---

## 6. P3 — staleTxs 两次独立 Range（rs:500-510）

### 6.1 问题

```go
// 第一次 Range：清理 stale txs
rs.staleTxs.Range(func(txHash, _ interface{}) bool {
    staleNum++
    txh := txHash.(common.Hash)
    rs.unsubscribeRetryTx(txh)
    rs.retryPool.Delete(txh)
    return true
})
// 第二次 Range：删除 staleTxs 自身条目
rs.staleTxs.Range(func(txHash, _ interface{}) bool {
    rs.staleTxs.Delete(txHash)
    return true
})
```

第一次遍历已完成业务处理（unsubscribe + 从 retryPool 删除），第二次只做 map 清理。但删除期间不允许其他 goroutine 写入 staleTxs，所以是全串行操作。两次 Range = 一次 Range + 第二次 Range 期间其它 goroutine 也想写 staleTxs。

用 `staleTxs.Range(…Delete)` 是线程安全的，但第二次 Range 期间其它 goroutine 的 Store 可能被清掉（Range + Delete 交替时）。

### 6.2 候选方案

| 方案 | 描述 |
|:-----|:------|
| A | **第一次 Range 时直接 Delete** — `staleTxs.Range` 期间调 `Delete` 是安全的（Go sync.Map Range doc 保证） |
| B | **用 List/Slice** 代替 sync.Map — staleTxs 只在 OnBlockCommitted 和 StaleTx 中写，并发度低，锁比 sync.Map 更高效 |

---

## 7. P4 — findCoveringSet 贪心近似复杂度

### 7.1 问题

`findCoveringSet` 用于从 PatchPool 中找一组 Free Patch 联合覆盖所有冲突 key：

```go
// 第一层：遍历每个 conflictKey
for i, key := range conflictKeys {                    // O(K)
    innerVal, ok := rs.keyIndex.Load(key)              // sync.Map load
    inner := innerVal.(*sync.Map)
    inner.Range(func(txHashVal, _ interface{}) bool {  // O(P) — 写此 key 的 Patch 数
        // ... 收集到 available 数组
    })
}
// 第二层：贪心选覆盖最多 key 的 Patch
for len(covered) < len(conflictKeys) {                 // 最多 K 轮
    for idx, entry := range available {                 // O(A) — 候选数
        for k := range entry.keys {                     // O(K) — 每个 Patch 覆盖的 key 数
            if !covered[k] { cnt++ }
        }
    }
    // 选最佳 → 移除
}
```

**最坏情况：O(K × P × K × A)** = O(K²AP)。当 conflictKeys 多（5+）、PatchPool 大（50+）时消耗显著。

### 7.2 候选方案

| 方案 | 描述 |
|:-----|:------|
| A | **限制搜索深度** — available 候选超过 N 个时直接放弃 DAG 路径 |
| B | **缓存 keyIndex 快照在函数入口处** — 避免每次 Range 时 sync.Map 的锁开销 |
| C | **哈希位图法** — conflictKeys 哈希到位图，Patch 覆盖 key 预计算位图，贪心选择用 bitset 操作 |
| D | **改为简单方案** — 不再找「联合覆盖」，只在 Phase 1b 和 2b 各限 1 个 Patch（已有一个 maxPatches=1 的限制） |

---

## 8. 附录：各函数复杂度速查

| 函数 | 行号 | 单次复杂度 | 入口次数 | 备注 |
|:-----|:-----|:----------|:---------|:------|
| `OnBlockCommitted` | 491 | **O(N·K + NlogN)** | 1/block | 全 block 层面最重 |
| `RetryCommit` | 1168 | **O(K·C + K·P)** | N/txs | N=RelatedShards |
| `tryToReSimulation` | 935 | O(M·R) | N/retry | M=RelatedShards, R=RPC |
| `findCoveringSet` | 330 | **O(K²·P)** | N/PatchFail | K=conflictKeys |
| `scanPatchSubscribers` | 401 | O(N·K·P) | N/newPatch | 内层又调 findCoveringSet |
| `querySubscribers` | 225 | O(K·T) | 1/block + N/patch | K=keys, T=txs/key |
| `HandleReSimulationSignal` | 885 | O(S·N) | N/signal | S=signals |
| `subscribeRetryTx` | 181 | O(K) | N/tx | K=ReadSet+WriteSet |
| `unsubscribeRetryTx` | 195 | O(K·T) | N/stale | T=inner.Range判空 |
| `HandleRetrySignal` | 843 | O(1) | N/chain | 直调 tryToReSimulation |
| `sendChainSignal` | 1414 | O(1) | N/chain | local 或 RPC |
| `mergeRWSet` | 540 | O(S) | N/DAG | S=state entries |
| `extractWriteKeys` | 526 | O(S) | N/patch | 和 merge 类似 |
| `tryConsumePatch` | 127 | O(1) | N/DAG | atomic CAS |
| `releasePatch` | 142 | O(1) | N/DAG | atomic CAS |

---

## 9. 建议优化顺序

```
P0 → P1 → P3 → P2 → P4

           ┌──────────────────────────────┐
           │  P0: 6× Range → 原子计数器    │ ← 最大收益，最小风险
           ├──────────────────────────────┤
           │  P1: 删除热 key 统计           │ ← 零风险删除，节省 O(N·K)
           ├──────────────────────────────┤
           │  P3: staleTxs 两次 Range 合并  │ ← 小改动
           ├──────────────────────────────┤
           │  P2: Phase 1b/2b 去重         │ ← 代码质量优化
           ├──────────────────────────────┤
           │  P4: findCoveringSet 优化     │ ← 看实际数据再做
           └──────────────────────────────┘
```
