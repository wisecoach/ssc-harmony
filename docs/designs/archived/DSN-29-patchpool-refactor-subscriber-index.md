# DSN-29: PatchPool 重构 + Subscriber 索引 + 增量扫描

## 1. 动机

### 1.1 三个问题

| # | 问题 | 症状 |
|:-:|:-----|:------|
| 1 | **chainNextSim 发无用信号** | 上游只覆盖 1/10 的 key 就发 chain signal → RetryCommit 必然失败 → 浪费跨 shard RPC |
| 2 | **全量扫描 retryPool** | OnBlockCommitted / OnPatchPoolUpdated 每次都遍历整个 retryPool (≈2000 笔) → 大部分检查白费 |
| 3 | **PatchPool 全局锁** | TryConsume/Release/Finalize 用全池 mu.Lock → 不同 txHash 的操作被串行化 |

### 1.2 根因

| 问题 | 根因 |
|:-----|:------|
| chainNextSim 乐观 | 匹配条件 `dependsOn()` 只要求任意 key 重叠 → 发信号时不知道其他 key 是否可覆盖 |
| 全量扫描 | 没有 key→retryTx 的反向索引 → 不知道哪些 tx 关心哪些 key |
| 全局锁 | Patches 和 KeyIndex 共用一把 RWMutex → 单 tx 操作锁全池 |

### 1.3 解决思路

1. **删 chainNextSim**，只保留 PatchPool DAG 覆盖（`FindCoveringSet` 保证全量覆盖才发信号）
2. **引入 subscriber 索引**（key→retryTx），让 PatchPool.Add 和 OnBlockCommitted 只扫描相关的 tx
3. **PatchPool 去全局锁**：sync.Map + atomic Status

## 2. 架构

### 2.1 数据流

```
                    PatchPool (独立包 ssc/patchpool/)
                    ╔══════════════════════════════╗
   Add(writeSet) ──→║ Patches  sync.Map[txHash]*Node║
                    ║ KeyIndex sync.Map[key]*Set     ║  ← 谁写了这个 key
   Subscribe ──→    ║ Subscriber sync.Map[key]*Set   ║  ← 谁等这个 key
                    ║ txSubKeys sync.Map[txHash][]LockKey  ║  ← 反向索引用于 Unsubscribe
                    ╚══════════════════════════════╝
                            │
                    ┌───────┴───────┐
                    │               │
            QuerySubscribers   FindCoveringSet
                    │               │
               (增量扫描)       (DAG 覆盖)
```

### 2.2 状态机 (retryTx)

```
                     AddToRetry
                         │
                    ┌────▼────┐
                    │  active  │ ← subscriber 索引中
                    └────┬────┘
                         │
              ┌──────────┼──────────┐
              │          │          │
          进入passive   Patch匹配  被Wound
              │          │          │
         ┌────▼───┐  ┌──▼───┐  ┌──▼────┐
         │ passive │  │consumed│ │wounded │
         └────┬───┘  └──┬───┘  └──┬─────┘
              │          │          │
              │     re-sim成功    OnBlockCommitted
              │          │          └→ re-subscribe
              │     ┌────▼────┐
              │     │  done   │
              │     └─────────┘
              └─── 被信号唤醒 ──→ active
```

| 状态 | subscriber | 说明 |
|:-----|:----------:|:------|
| `active` | ✅ | 可被 PatchPool Add / OnBlockCommitted 扫描匹配 |
| `consumed` | ❌ | Patch 已消费，等 re-sim；失败后回 `active` |
| `passive` | ❌ | 跳过主动扫描，等 origin shard 信号 |
| `wounded` | ❌ | TLV 锁被抢，OnBlockCommitted 时恢复 |
| `done` | ❌ | 从 retryPool 删除 |

## 3. 详细设计

### 3.1 Subscriber 索引

```go
// PatchPool 新增字段
type PatchPool struct {
    Patches     sync.Map // key: common.Hash, val: *ChainPatchNode
    KeyIndex    sync.Map // key: api.LockKey, val: *sync.Map{key: common.Hash}
    Subscriber  sync.Map // key: api.LockKey, val: *sync.Map{key: common.Hash}
    txSubKeys   sync.Map // key: common.Hash, val: []api.LockKey  — 反向索引
}

// ── 订阅 ──
func (pp *PatchPool) Subscribe(txHash common.Hash, reads, writes []api.LockKey)
    // 1. 合并 reads + writes 为 allKeys
    // 2. 对每个 key: Subscriber.LoadOrStore → set.Store(txHash)
    // 3. txSubKeys.Store(txHash, allKeys)

func (pp *PatchPool) Unsubscribe(txHash common.Hash)
    // 1. txSubKeys.Load(txHash) → 取出所有订阅的 key
    // 2. 对每个 key: Subscriber.Load → set.Delete(txHash)
    // 3. txSubKeys.Delete(txHash)

func (pp *PatchPool) QuerySubscribers(keys []api.LockKey) []common.Hash
    // 1. 对每个 key: Subscriber.Load → 读 set 中的 txHashes
    // 2. 合并去重返回
```

### 3.2 PatchNode atomic Status

```go
type ChainPatchNode struct {
    TxHash        common.Hash
    SimulationNum int
    Patch         *api.RWSet
    status        atomic.Int32  // PatchConsumeStatus → 无锁切换
    Consumer      common.Hash
    Priority      api.Priority
    CreatedAt     time.Time
}

func (n *ChainPatchNode) Status() PatchConsumeStatus
    return PatchConsumeStatus(n.status.Load())

func (n *ChainPatchNode) SetStatus(s PatchConsumeStatus)
    n.status.Store(int32(s))

// 仅 TryConsume 用 CAS
func (n *ChainPatchNode) TryAcquire() bool
    return n.status.CompareAndSwap(int32(PatchFree), int32(PatchConsumed))
```

### 3.3 增量扫描 (替代全量)

```go
// 触发点: impl.go 中 SimTx 提交成功
s.patchPool.Add(txHash, simNum, writeSet)

// PatchPool.Add 内部 —— 新增后查 subscriber
func (pp *PatchPool) Add(txHash common.Hash, simNum int, writeSet *api.RWSet) {
    // 1. Patches.Store(txHash, node)
    // 2. 更新 KeyIndex
    // 3. 从 writeSet 提 keySet
    // 4. candidates = pp.QuerySubscribers(keySet)
    // 5. return candidates  // ← 返回给 RS 做后续 FindCoveringSet
}
```

```go
// RS 端
func (rs *retryScheduler) OnPatchPoolUpdated(writeSet *api.RWSet) {
    candidates := rs.patchPool.QuerySubscribers(extractKeys(writeSet))
    for _, txHash := range candidates {
        retryTx := loadFromRetryPool(txHash)
        if !shouldSkip(retryTx) {  // 跳过被动/已在 consumed
            if patches := rs.patchPool.FindCoveringSet(retryTx.AllKeys()); patches != nil {
                allOk := TryConsume 所有 patches
                if allOk {
                    rs.sendChainSignal(txHash, retryTx, merged, upstreamTxList)
                }
            }
        }
    }
}
```

### 3.4 OnBlockCommitted 增量扫描

```go
func (rs *retryScheduler) OnBlockCommitted(block *types.Block) {
    // 1. TLV 清理 → 返回释放的 key
    releasedKeys := rs.tempLockView.OnBlockCommitted(block)

    // 2. 清理 staleTxs + Unsubscribe

    // 3. subscriber 替代全量 retryPool.Range
    candidates := rs.patchPool.QuerySubscribers(releasedKeys)
    for _, txHash := range candidates {
        // TLV.CanLock + stateDB.CheckLock（同原有逻辑）
    }

    // 4. 恢复 wounded txs
    rs.woundedRetryTxs.Range(func(txHash, _) {
        rs.patchPool.ReSubscribe(txHash)
        rs.woundedRetryTxs.Delete(txHash)
    })
}
```

## 4. 删改

### 4.1 删除

| 文件 | 函数 | 原因 |
|:-----|:------|:------|
| `ssc/retry_scheduler.go` | `chainNextSim()` | 被 PatchPool DAG 覆盖，后者有 `FindCoveringSet` 全量检查 |
| `ssc/retry_scheduler.go` | `dependsOn()` | 只被 chainNextSim 用 |
| `ssc/retry_scheduler.go` | `dependsOnReserved()` | 只被 chainNextSim 用 |
| `ssc/impl.go:892` | `chainNextSim(...)` 调用 | 删除 |

### 4.2 改动

| 文件 | 改动 |
|:-----|:------|
| `ssc/retry_scheduler.go` | `OnPatchPoolUpdated(writeSet)` 增量扫描 |
| `ssc/retry_scheduler.go` | `OnBlockCommitted` → subscriber 替代全量 |
| `ssc/retry_scheduler.go` | `AddToRetry` → 新增 Subscribe |
| `ssc/retry_scheduler.go` | 新增 `woundedRetryTxs sync.Map` |
| `ssc/retry_scheduler.go` | retryTx 失败路径 → 检测 wound → 存入 woundedRetryTxs |
| `ssc/retry_scheduler.go` | 成功路径 → Status=done + Unsubscribe |
| `ssc/temp_lock_view.go` | `OnBlockCommitted` 返回 `[]api.LockKey` |
| `ssc/api/types.go` | `RetryTx` 加 `Status RetryStatus` 字段 |

### 4.3 新增

```
ssc/patchpool/
  ├── pool.go          PatchPool 核心 + Add/Remove/Stats
  ├── node.go          ChainPatchNode + Status atomic
  ├── query.go         HasConflict/FindCovering/FindCoveringSet
  ├── consume.go       TryConsume/Release/Finalize/IsFinalized
  └── subscriber.go    Subscribe/Unsubscribe/ReSubscribe/QuerySubscribers
```

## 5. 决策记录

| # | 决策 | 结论 |
|:-:|:-----|:------|
| 1 | 删 chainNextSim | ✅ 保留仅 OnPatchPoolUpdated |
| 2 | 增量扫描 | ✅ 新增 Patch 时只查 subscriber 中 key 重叠的 tx |
| 3 | subscriber 索引归属 | ✅ PatchPool 内置，RS 通过公开方法访问 |
| 4 | retryTx 加 Status | ✅ active / consumed / passive / wounded |
| 5 | consumed 退出则 unsubscribe | ✅ 失败后 re-subscribe |
| 6 | wounded 恢复 | ✅ woundedRetryTxs sync.Map → OnBlockCommitted re-subscribe |
| 7 | passive 退出 subscriber | ✅ 等 origin shard 信号唤醒 |
| 8 | OnBlockCommitted 增量 | ✅ TLV 返回 releasedKeys → subscriber 查 |
| 9 | TLV 数据传递 | ✅ 通过返回值，非回调（无耦合） |
| 10 | PatchPool 去全局锁 | ✅ sync.Map + atomic Status |
| 11 | PatchPool 独立模块 | ✅ `ssc/patchpool/` 独立包 |

## 6. 未解决的问题

- `woundedRetryTxs` 的写入点：选 TLV 回调 vs RetryCommit 失败路径 → 当前倾向后者（不增加 TLV 接口）
- ChainPatchNode.Consumer/Priority 字段仍非 atomic，但只在 TryConsume 时写入一次、Release 时清零一次，无竞争
