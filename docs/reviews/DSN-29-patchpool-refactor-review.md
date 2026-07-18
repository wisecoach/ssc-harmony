# DSN-29 PatchPool 重构审查报告

> 审查日期：2026-07-14
> 审查范围：DSN-29 PatchPool 重构 + Subscriber 索引 + 增量扫描
> 审查方式：离线代码审查

## 严重程度说明

| 级别 | 含义 |
|:----:|------|
| 🔴 Critical | 逻辑错误，会导致 crash/数据丢失/死锁 |
| 🟡 Medium | 设计偏差，功能未按预期工作 |
| 🟢 Minor | 代码风格/性能/可读性改进 |
| 💡 Suggestion | 建议性改进，非必需 |

## 发现清单

---

### 🔴 Critical 1: OnPatchPoolUpdated 消费 Patch 后 RetryCommit 死锁

**位置**：`ssc/retry_scheduler.go:1069-1165`（`RetryCommit`）
**描述**：

`OnPatchPoolUpdated` 通过 DAG 消费 Patch 并发送 chain signal 后，同一相关分片上后续的 `RetryCommit` 调用（由 origin shard 的 `tryToReSimulation` 发起）**不会检查 `consumedPatches`**。它直接进入 TLV 锁检查路径：

```
OnPatchPoolUpdated(writeSet)
  ├─ FindCoveringSet → 找到 Free Patch
  ├─ TryConsume → Patch 变 PatchConsumed
  ├─ consumedPatches.Store(retryTxHash, consumedUpstreams)
  └─ sendChainSignal → 送 merged Patch 到 origin shard

origin shard: tryToReSimulation → RetryCommit RPC 到所有分片
  └─ 在 PatchPool 分片上:
       ├─ TryLockWithPriority → FAIL (SimTx 的 TLV 锁尚未释放)
       ├─ Phase 1b DAG → FindCoveringSet → 找不到 Free Patch (已 PatchConsumed)
       └─ return Locked: false → 整个重试失败!
```

**结果**：DSN-29 的核心优化——PatchPool DAG 发 chain signal 后跳过锁竞争——**在实践中几乎不可能成功**。因为被消费的 Patch 对应的 SimTx 的 TLV 锁在 SimTx 提交成功但尚未出块时仍被持有，后续 `RetryCommit` 的锁检查必然失败，而 Phase 1b/2b DAG 又因为 Patch 已被消费（`PatchConsumed`）无法匹配。

**修复思路**：
在 `RetryCommit()` 开头，检查 `consumedPatches[txHash]` 是否存在。如果存在，说明 `OnPatchPoolUpdated` 已经消费了覆盖全部 key 的 Patch 并发送了 chain signal，当前分片不需要再参与锁竞争，直接返回 `Locked: true`。

```go
// RetryCommit 开头新增：
if _, consumed := rs.consumedPatches.Load(txHash); consumed {
    return &api.RetryCommitResp{Locked: true, TxHash: txHash}
}
```

**状态**：📝 待修复

---

### 🔴 Critical 2: OnBlockCommitted 丢弃 TLV 返回值，增量扫描未实现

**位置**：`ssc/retry_scheduler.go:494`
**描述**：

```go
// 当前代码（retry_scheduler.go:494）：
rs.tempLockView.OnBlockCommitted(block)

// 设计文档 3.4 节要求：
releasedKeys := rs.tempLockView.OnBlockCommitted(block)
candidates := rs.patchPool.QuerySubscribers(releasedKeys)
```

当前代码**丢弃了** `OnBlockCommitted` 的返回值 `[]api.LockKey`。这意味着 DSN-29 的另一核心优化——用 subscriber 索引替代全量 `retryPool.Range` 做增量扫描——**完全没有实现**。函数主体仍然在 535 行做全量 `retryPool.Range` 遍历。

**影响**：全量遍历 ≈2000 笔重试交易，与重构前的性能一致，失去了增量扫描的优化收益。

**修复思路**：
1. 捕获 `releasedKeys := rs.tempLockView.OnBlockCommitted(block)`
2. 对 releasedKeys 调用 `rs.patchPool.QuerySubscribers(releasedKeys)` 获取候选 tx 列表
3. 只扫描这部分候选 tx（替代 535 行的全量 `retryPool.Range`）
4. 同时保留 `retryPool` 中 **不做 TLV 检查但 stateDB 锁已释放** 的 tx 扫描（LockWait Pool 路径 658 行可以保留）

**状态**：📝 待修复

---

### 🟡 Medium 3: 统计计数器死代码（chainNextSim 残留）

**位置**：`ssc/retry_scheduler.go:126-130`
**描述**：

以下 4 个 `atomic.Int64` 计数器被声明（125-130 行）、初始化（175-178 行）、在 `dumpChainRetryStats` 中 dump（211-214 行），但从未被 `Add(1)` 调用：

| 变量名 | 原用途 |
|:-------|:------|
| `SigChainNextScan` | chainNextSim 被调用次数 |
| `SigChainNoDownstream` | 无下游依赖 |
| `SigChainSelected` | 选中并发送 chain signal |
| `SigChainReservedSkipped` | reservation 过滤跳过 |

`chainNextSim` 已删除（DSN-29 核心变更之一），这些计数器永远为 0。

**建议**：
- 删除这 4 个计数器及其在 `dumpChainRetryStats` 中的打印
- 或新增对应 `OnPatchPoolUpdated` 的统计计数器（如有需要）

**状态**：📝 待处理

---

### 🟡 Medium 4: api.RetryTx.ChainDepth 字段死代码

**位置**：`ssc/api/types.go:1394`
**描述**：

```go
ChainDepth    int    // 链式依赖深度：0=无上游依赖，1=依赖1跳，2=依赖2跳...
```

此字段被声明但**从未被赋值**（搜索 `\.ChainDepth\s*=` 无结果）。`chainNextSim` 删除后无任何代码设置此字段。

`ChainDepthCnt` 和 `ChainDepthCommitCnt` 在 `dumpChainRetryStats` 中被 dump，但仅在 `impl.go:885-887` 中被写入（但写入的是 `commit.SimulationNum` 而非 `retryTx.ChainDepth`！）。

**修复思路**：
- 删除 `RetryTx.ChainDepth` 字段
- 或启用深度计算逻辑（从 DAG 上游链长度推导）

**状态**：📝 待处理

---

### 🟡 Medium 5: OnBlockCommitted 注释与实际代码不一致

**位置**：`ssc/retry_scheduler.go:491-711`
**描述**：

设计文档声称 `OnBlockCommitted` 的 "wounded 恢复" 流程使用 `ReSubscribe`（见 3.4 节设计文档），但实际代码（609-623 行）使用的是 `Subscribe`：

```go
// 设计文档：
rs.patchPool.ReSubscribe(txHash)

// 实际代码（618行）：
rs.patchPool.Subscribe(txHash, retryTx.ReadSet, retryTx.WriteSet)
```

`ReSubscribe` 在 patchpool/subscriber.go 中已实现（52-55 行），但未被使用。无论使用 `Subscribe` 还是 `ReSubscribe` 在这里效果一样（因为 wounded 时会先 `Unsubscribe`），但存在设计文档与实际实现的不一致。

**状态**：🟢 可忽略（语义等同），建议删除未使用的 `ReSubscribe` 或修改为 `ReSubscribe` 以对齐设计文档。

---

### 🟢 Minor 6: CloseTransaction 中 Remove 的 txHash 可能不是 SimTx hash

**位置**：`ssc/impl.go:1200`
**描述**：

```go
s.retryScheduler.patchPool.Remove(txHash)
```

这里的 `txHash` 是跨分片交易哈希（cross-shard tx hash）。PatchPool 中 key 也是 cross-shard tx hash（`commit.TxHash`，见 impl.go:893）。所以 key 匹配——没有类型错误。

但如果有多次重试（多个 SimTx），只有最后一个 SimTx 的 Patch 会保留在 `Patches` sync.Map 中（新 `Store` 覆盖旧的）。删除时只删除了最后一个 Patch，之前的 Patch 已经被覆盖了。这不是 bug，只是值得注意的行为。

**状态**：💡 文档注明即可

---

### 🟢 Minor 7: 失败路径 re-subscribe 时 retryTx 指针作用域问题

**位置**：`ssc/retry_scheduler.go:994-1008`
**描述**：

审查清单中提到 "retryTx 指针在闭包中是否安全？"。

```go
// tryToReSimulation 失败路径（line 994-1008）
if consumedTxHashesVal, exists := rs.consumedPatches.Load(txHash); exists {
    // ...
    rs.consumedPatches.Delete(txHash)
    if retryTx != nil {
        retryTx.Status = api.RetryActive
        rs.patchPool.Subscribe(txHash, retryTx.ReadSet, retryTx.WriteSet)
    }
}
```

**分析**：`retryTx` 来自 line 1093-1094 的 `rs.retryPool.Load(txHash)`。这个变量是在 `tryToReSimulation` 的函数栈上（line 836），不是在匿名闭包中，所以不存在闭包捕获问题。`retryPool` 中的指针被 Load 一次后，后续 `retryPool.Delete`（未被调用）才会导致悬空指针。这里安全。

**状态**：✅ 安全

---

### 🟢 Minor 8: consumer/priority 字段非 atomic 但安全

**位置**：`ssc/patchpool/consume.go:20-21`
**描述**：

`TryConsume` 在 CAS 成功后写入 `Consumer` 和 `Priority`，`Release` 在检查状态后清零。设计文档在 "6. 未解决的问题" 中已注明此风险。

**分析**：`TryConsume` 的 CAS 确保只有一个 goroutine 能通过。`Release` 的 `if node.Status() != PatchConsumed { return }` 确保只在消费状态下清零。`Finalize` 也不修改 Consumer/Priority。所以安全。

**状态**：✅ 安全

---

## 汇总

| # | 级别 | 文件 | 问题 | 修复难度 |
|:-:|:----:|:-----|:-----|:--------:|
| 1 | 🔴 Critical | `retry_scheduler.go:1069-1165` | OnPatchPoolUpdated 消费 Patch 后 RetryCommit 无法通过锁检查 | 1 行（加 consumedPatches 检查） |
| 2 | 🔴 Critical | `retry_scheduler.go:494` | OnBlockCommitted 丢弃 TLV 返回值，增量扫描未实现 | 中等（改循环逻辑） |
| 3 | 🟡 Medium | `retry_scheduler.go:126-130` | 4 个 chainNextSim 计数器死代码 | 简单（删除/替换） |
| 4 | 🟡 Medium | `api/types.go:1394` | ChainDepth 字段从未赋值 | 简单（删除/启用） |
| 5 | 🟢 Minor | `subscriber.go:52-55` | ReSubscribe 未使用 | 简单（删除或启用） |
| 6 | 🟢 Minor | `impl.go:1200` | Remove 语义说明 | 文档级 |
| 7 | ✅ 安全 | `retry_scheduler.go:1005-1007` | retryTx 指针安全 | - |
| 8 | ✅ 安全 | `consume.go:20-21` | Consumer/Priority 非 atomic 但安全 | - |

### 首要修复

**#1 是阻断性 bug**：OnPatchPoolUpdated 的核心链路（消费 Patch → chain signal → retryCommit）在 PatchPool 所在分片上被切断。修复方法：在 `RetryCommit` 开头检查 `consumedPatches`，如果已存在则直接返回 `Locked: true, TxHash: txHash`。

**#2 是性能回归**：增量扫描未实现，全量遍历 retryPool（≈2000 tx）仍与重构前一致。

### 代码质量总体评价

- `ssc/patchpool/` 新包代码质量好：atomic CAS 正确，sync.Map 使用得当
- Subscriber 索引逻辑正确：Subscribe/Unsubscribe/QuerySubscribers 实现完整
- 并发安全分析：无死锁/竞态，全局 sync.Map 替代 mutex 是正确的
- 但核心调用方（`retryScheduler.OnPatchPoolUpdated` → `RetryCommit`）衔接存在逻辑断点
