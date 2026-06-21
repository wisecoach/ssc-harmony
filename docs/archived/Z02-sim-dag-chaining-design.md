# Sim DAG Chaining Design

## 1. Problem Statement

### 现状瓶颈

跨分片交易（SSC）在模拟期间锁定状态 key，锁持有直到 CR 交易完成上链。典型周期约 **4 个区块**：

```
Block N:   SubmitSimulationTx (锁获取)
Block N+1: Simulation 提交
Block N+2: CR 投票收集
Block N+3: CR 提交 (锁释放)
```

在这 4 个区块内，同一个状态 key 只能被一笔交易修改。对于**热点 key**（所有交易都会触碰的状态 key），吞吐量被锁竞争严重限制：

```
Hot Key K:  tx1  →  tx2  →  tx3  →  tx4
            [locked for 4 blocks]
                     [locked for 4 blocks]
                              [locked for 4 blocks]
                                       [locked for 4 blocks]
```

在 RATE=100、每个 key 锁定约 8 秒的情况下，系统吞吐量实质上受限于热点 key 的锁竞争。

### 原有方案（chainHotKeyCR）的局限

前一版设计（`docs/Z01-cr-hotkey-chaining-design.md`）在 CR 交易提交后触发 chain，仅针对"热点 key"做优化：

- 触发时机：CR 交易 `thresholdSignSimulationCommit` 之后
- 优化对象：仅 HotKeyConflicts 阈值以上被标记为"hot"的 key
- 目标：每笔交易节省 2-3 个区块的等待，但不改变**每个区块只能修改同一个 key 一次**的格局

该方案存在几个局限：
1. **冷启动振荡**：key 需要积攒冲突计数才能被标记为 hot → chain 开始 → 冲突下降 → 移出 hot set → 回到慢路径
2. **单次链优化**：每次 CR 提交最多 chain 出一笔 SimTx，无法在同一个区块内连续修改同一个 key
3. **HotKey 判断冗余**：能否 chain 取决于锁竞争关系而非是否"hot"，hotkey 判断引入不必要的复杂度

### 本方案目标

放弃按 key 热度判断的优化策略，改为**全量链式 DAG 依赖执行**，实现：

1. **区块内多次修改同一个 key** — 链式 SimTx 按 nonce 排序，同一区块内依次执行
2. **DAG 并行** — 互不依赖的分支可以并行构建和提交
3. **乐观锁语义** — 依赖链中任一环节失败，整条链原子回滚

---

## 2. 核心概念

### 2.1 链式执行（Chained Execution）

```
同一区块 Block N（按 nonce 排序）：

SimTx(crN+1) → 锁 K → 读 patch → 执行 → K=v1
SimTx(crN+2) → 锁 K → 读 patch(SimTx1) → 基于 v1 执行 → K=v2
SimTx(crN+3) → 读 v2 → ...
```

- SimTx 之间通过 **nonce 排序**保证区块内执行顺序
- 下游 SimTx 的 `GetState` 通过 `CXTSimulationState.ChainPatch` 读到上游 SimTx 的 WriteSet
- 区块执行时 stateDB 按 nonce 递增依次写入，下游自然看到上游执行结果

### 2.2 DAG 执行模型

单笔 SimTx 提交后，可以 chain 出**多个**互不冲突的下游 SimTx：

```
SimTx1(crN+1) 写 {K, L}
    ├── SimTx2(crN+2) 依赖 K 写 {K, M}    ← chain 分支 1
    └── SimTx3(crN+3) 依赖 L 写 {L, N}    ← chain 分支 2
```

- SimTx2 和 SimTx3 的 ReadSet/WriteSet 无交集 → 可并行执行
- 依赖判断仅看 **key 交集**（见 §3.2）

### 2.3 原子回滚

乐观锁语义：链中任何一笔 SimTx 模拟/验证失败，**整条依赖链回滚**：

```
SimTx1 成功 → SimTx2 成功 → SimTx3 失败
                         ↓
全部回滚（SimTx1、SimTx2、SimTx3 均不生效）
```

- 失败触发与现有 `RetryCancel` 一致的清理路径
- 所有 SimTx 持有的锁由 `TempLockView` 统一释放

---

## 3. 依赖链构建（DAG 拓扑）

### 3.1 触发时机

`chainNextSim` 在 **SimTx 提交后**触发，替代原来的 `chainHotKeyCR`（CR 提交后触发）：

```go
// 原有：CR 提交 → chainHotKeyCR
// 改为：SimTx 提交 → chainNextSim

// 触发位置：SubmitSimulationTx 成功返回后
func (s *sscService) afterSubmitSimulation(txHash common.Hash, simReq *SimulationRequest) {
    // 提取当前 SimTx 的 WriteSet
    writeSet := extractWriteSet(simReq)

    // 链式构建下游
    s.retryScheduler.chainNextSim(txHash, writeSet)
}
```

### 3.2 依赖定义

一笔 retry tx **依赖**一笔已提交的 SimTx，当且仅当：

```
(retryTx.ReadSet ∪ retryTx.WriteSet) ∩ SimTx.WriteSet ≠ ∅
```

即：retryTx 的读集或写集与 SimTx 的写集存在交集。

| retryTx 用例 | SimTx 写集 | 是否依赖 | 解释 |
|-------------|-----------|:-------:|------|
| ReadSet={K}, WriteSet={} | {K} | ✅ | retryTx 要读 K 的新值 |
| ReadSet={}, WriteSet={K} | {K} | ✅ | retryTx 要写 K，需要前序状态 |
| ReadSet={M}, WriteSet={N} | {K, L} | ❌ | 无交集，互不依赖 |
| ReadSet={K, M}, WriteSet={K} | {K, L} | ✅ | ReadSet 和 WriteSet 都含 K |

### 3.3 chainNextSim 算法

```go
// chainNextSim: 在 SimTx 提交后构建下游依赖
//
// 输入:
//   - upstreamTxHash: 刚提交的 SimTx 的 txHash
//   - writeSet: 该 SimTx 对所有 key 的写结果
//
// 输出:
//   - 为每个匹配的 retry tx 发 ReSimulationSignal（带 ChainPatch）
//
func (rs *retryScheduler) chainNextSim(upstreamTxHash common.Hash, writeSet *api.RWSet) {
    // 1. 提取上游 SimTx 产生的 key→value 映射
    patch := &api.RWSet{
        WriteState: cloneWriteState(writeSet.WriteState),
    }
    if len(patch.WriteState.State) == 0 {
        return  // 空写集，不可能有下游依赖
    }

    // 2. 收集所有匹配的 retry 交易（RLock 下完成，避免阻塞 RPC）
    var matchedTxs []*matchedRetry
    rs.mu.RLock()
    for txHash, retryTx := range rs.retryPool {
        if bytes.Equal(txHash.Bytes(), upstreamTxHash.Bytes()) {
            continue  // 跳过自己
        }
        if !dependsOn(retryTx, patch) {
            continue  // 无交集，不依赖
        }
        // 检查 TempLockView：确保 key 当前可锁
        // 注意：chain 场景下上游 SimTx 刚提交但锁未释放，
        //       但我们可以用乐观假设——这条链是 leader 调度的，
        //       下游的锁一定在上游释放后获取
        matchedTxs = append(matchedTxs, &matchedRetry{
            txHash: retryTx.TxHash,
            retryTx: retryTx,
        })
    }
    rs.mu.RUnlock()

    // 3. 对每个匹配的交易发送信号（释放锁后做 RPC，避免 RwMutex 饥饿）
    for _, m := range matchedTxs {
        rs.sendChainSignal(m.txHash, m.retryTx, patch)
    }
}

// dependsOn: 判断 retryTx 是否依赖 patch 中的任意 key
func dependsOn(retryTx *RetryTxState, patch *api.RWSet) bool {
    // 检查 ReadSet
    for _, key := range retryTx.ReadSet {
        if patchContainsKey(patch, key) {
            return true
        }
    }
    // 检查 WriteSet
    for _, key := range retryTx.WriteSet {
        if patchContainsKey(patch, key) {
            return true
        }
    }
    return false
}

// sendChainSignal: 发送 chain 信号到 origin shard
func (rs *retryScheduler) sendChainSignal(txHash common.Hash, retryTx *RetryTxState, patch *api.RWSet) {
    signal := &api.ReSimulationSignal{
        TxHash:          txHash,
        FromShard:       rs.selfShard,
        Epoch:           retryTx.Epochs[retryTx.OriginShardID],
        SimulationNum:   retryTx.SimulationNum,
        Condition:       retryTx.Condition,
        Ready:           true,
        ChainPatch:      patch,  // renamed from CRHotWritePatch
    }

    originLeader := rs.sscService.GetLeader(
        retryTx.Epochs[retryTx.OriginShardID], retryTx.OriginShardID)
    err := rs.comm.Call(rs.ctx, nil, originLeader,
        api.Method_HandleChainSimSignal, signal)
    if err != nil {
        utils.SSCLogger().Error().Err(err).
            Str("txHash", txHash.Hex()).
            Msg("chainNextSim: failed to send signal")
    } else {
        utils.SSCLogger().Info().
            Str("txHash", txHash.Hex()).
            Int("patchKeys", countPatchKeys(patch)).
            Msg("chainNextSim: sent chain sim signal")
    }
}
```

### 3.4 DAG 排序过滤层

`txSubmitter` 增加 DAG 依赖排序，确保同一区块内 nonce 分配遵循拓扑序：

```go
// DAGOrderingFilter: 在 nonce 分配前确定 SimTx 的合法顺序
//
// 原则:
//   1. 根节点（不依赖本区块内任何未提交 SimTx）优先分配 nonce
//   2. 非根节点至少等到所有上游节点的 nonce 都分配完成
//   3. 同一层级（互不依赖的节点）按任意顺序分配 nonce
//
type DAGOrderingFilter struct {
    // pendingTxns: key=txHash → DAGNode
    pendingTxns map[common.Hash]*DAGNode

    // assignedNonces: 已经分配了 nonce 但未提交的 SimTx
    assignedNonces []uint64
}

type DAGNode struct {
    TxHash     common.Hash
    DependsOn  []common.Hash  // 上游 txHash 列表
    ReadSet    []api.LockKey
    WriteSet   []api.LockKey
    Nonce      uint64         // 暂定的 nonce
}

// assignNonce: 为待分配 nonce 的 SimTx 分配 nonce
func (f *DAGOrderingFilter) assignNonce(txHash common.Hash) (uint64, error) {
    node, ok := f.pendingTxns[txHash]
    if !ok {
        return 0, fmt.Errorf("tx not in DAG filter: %s", txHash.Hex())
    }

    // 检查所有上游是否已分配
    for _, dep := range node.DependsOn {
        depNode, ok := f.pendingTxns[dep]
        if !ok {
            continue  // 上游不在 filter 中（可能是 CR 或其他已上链交易）
        }
        if depNode.Nonce == 0 {
            return 0, fmt.Errorf("dependency not assigned: %s", dep.Hex())
        }
        if depNode.Nonce >= node.NonceCandidate() {
            return 0, fmt.Errorf("nonce ordering violation")
        }
    }

    // 分配下一个可用的连续 nonce
    nextNonce := nextAvailableNonce(f.assignedNonces)
    node.Nonce = nextNonce
    f.assignedNonces = append(f.assignedNonces, nextNonce)
    return nextNonce, nil
}
```

### 3.5 链的终止条件

依赖链自然终止：

- chainNextSim 扫描 retryPool，没有 retry tx 依赖当前 SimTx 的 WriteSet → 终止
- 当前 SimTx 的 WriteSet 为空 → 终止
- 达到区块 SimTx 数量上限（可配置，如 `ChainMaxPerBlock`）→ 终止
- 下游 retry tx 的 key 在 `TempLockView` 中被更早的 SimTx 锁住且无法通过 chain 解决 → 终止

---

## 4. 数据流端到端

### 4.1 完整流程

```
Phase 1: 链下构建与提交
────────────────────────
1. Leader 发现 retryTx1 可以执行（正常信号或区块边界触发）
2. Leader 链下模拟 retryTx1 → 成功
3. txSubmitter 为 SimTx1 分配 nonce=crN+1
4. SubmitSimulationTx(crSigner, nonce=crN+1)
5. afterSubmitSimulation → chainNextSim(SimTx1.hash, writeSet1)
6. chainNextSim 扫描 retryPool:
   ├─ 找到 retryTx2（依赖 K）→ 发 HandleChainSimSignal(ChainPatch=writeSet1)
   └─ 找到 retryTx3（依赖 L）→ 发 HandleChainSimSignal(ChainPatch=writeSet1)

Phase 2: Origin shard 响应
───────────────────────────
7. Shard B 收到 HandleChainSimSignal:
   ├─ stateLock → state.ChainPatch = signal.ChainPatch
   └─ rs.setSignal(fromShard, signal) → readyCnt check
       └─ readyCnt == len(RelatedShards)?
           ├─ Yes → tryToReSimulation(Tx2)
           │          ├─ 链下模拟（GetState 走 ChainPatch）
           │          ├─ 成功 → txSubmitter 分配 nonce=crN+2
           │          └─ afterSubmitSimulation → chainNextSim(SimTx2)
           └─ No  → 等待其他 shard 信号

Phase 3: 下一个 SimTx 链式触发
───────────────────────────────
8. SimTx2 提交 → chainNextSim(SimTx2.hash, writeSet2)
   ├─ 找到 retryTx4（依赖 M）→ 发 HandleChainSimSignal
   └─ 未找到其他匹配 → 终止

Phase 4: 区块执行与验证
────────────────────────
9. Block N 打包:
   ├─ SimTx1(crN+1) → VerifySimulation → 锁 {K, L} → K=v1, L=w1
   ├─ SimTx2(crN+2) → VerifySimulation → 读 K=v1（stateDB）
   │                                   → 锁 {K, M} → K=v2, M=m1
   ├─ SimTx3(crN+3) → VerifySimulation → 读 L=w1（stateDB）
   │                                   → 锁 {L, N} → L=w2, N=n1
   └─ ...（后续非 SSC 交易）

Phase 5: CR (跨分片提交)
─────────────────────────
10. Block N+1: SimTx1-3 全部提交成功
    ├─ SimTx1 CR(crN+1) 提交 → K 最终值 v1
    ├─ SimTx2 CR(crN+2) 提交 → K 最终值 v2
    └─ SimTx3 CR(crN+3) 提交 → K 最终值 v2（从 SimTx2 继承）

Phase 6: 失败回滚
─────────────────
11. 若 SimTx2 VerifySimulation 失败（K 实际值 ≠ patch 预期值）:
    ├─ SimTx1, SimTx2, SimTx3 全部回滚
    ├─ TempLockView 释放所有锁
    └─ retryPool 中重新放回各 tx（可重试）
```

### 4.2 区块内 nonce 排序示例

```
Block N 交易列表（按 nonce 升序）:

nonce    txType    crSigner    ReadSet   WriteSet   Patch来源
─────────────────────────────────────────────────────────────
crN+1    SimTx1    ✅           {K,L}     {K,L}     stateDB（根节点）
crN+2    SimTx2    ✅           {K,M}     {K,M}     SimTx1.WriteSet
crN+3    SimTx3    ✅           {L,N}     {L,N}     SimTx1.WriteSet
crN+4    SimTx4    ✅           {M,O}     {M,O}     SimTx2.WriteSet
```

执行过程（节点侧 `VerifySimulation` 或 `ExecuteSimulation`）：

```
SimTx1: stateDB.GetState(K) = v0 → 模拟 → K=v1, L=w1
SimTx2: stateDB.GetState(K) = v1（SimTx1 已写入）→ 模拟 → K=v2, M=m1
SimTx3: stateDB.GetState(L) = w1（SimTx1 已写入）→ 模拟 → L=w2, N=n1
SimTx4: stateDB.GetState(M) = m1（SimTx2 已写入）→ 模拟 → M=m2, O=o1
```

**注意**：在区块执行时，stateDB 在每笔 SimTx 执行时已包含前序 SimTx 的修改。`ChainPatch` 的用途是在**链下模拟阶段**（SimTx 构建时）替代不可见的 stateDB，而非区块执行阶段——因为在区块执行阶段，前序 SimTx 已经真正写入 stateDB。

---

## 5. 数据结构变更

### 5.1 `CXTSimulationState`

```go
type CXTSimulationState struct {
    // ... 现有字段 ...

    // ChainPatch stores state values produced by the upstream SimTx
    // that defined this retry's dependency chain.
    // When set during chain-optimized simulation, GetState() checks
    // this FIRST before querying stateDB.
    // Renamed from CRHotWritePatch to reflect broader semantics.
    ChainPatch *RWSet `json:"chain_patch,omitempty"`
}
```

### 5.2 `ReSimulationSignal`

```go
type ReSimulationSignal struct {
    TxHash        common.Hash
    FromShard     uint32
    Epoch         Epoch
    SimulationNum int
    Condition     ConflictCondition
    Ready         bool

    // ChainPatch carries the WriteSet from the upstream SimTx.
    // Only set in the chain-optimized pathway (HandleChainSimSignal).
    ChainPatch *RWSet `json:"chain_patch,omitempty"`
}
```

### 5.3 `retryScheduler`

```go
type retryScheduler struct {
    // ... 现有字段 ...

    // 以下字段可移除（原 HotKey 方案不再需要）:
    // hotKeySet    map[api.LockKey]struct{}
    // hotKeyMu     sync.RWMutex

    // 新增：ChainPatch 映射表（可选，用于 VerifySimulation 的跨 SimTx 验证）
    // 用于在区块执行时追踪本区块内 SimTx 的依赖关系
    blockChainPatch map[common.Hash]*api.RWSet  // txHash → upstream's WriteSet
}

// 新方法（替代 chainHotKeyCR）:
func (rs *retryScheduler) chainNextSim(upstreamTxHash common.Hash, writeSet *api.RWSet)

// 新方法（替代 HandleHotKeyRetrySignal）:
func (rs *retryScheduler) HandleChainSimSignal(signal *api.ReSimulationSignal)
```

### 5.4 RPC 方法名

```go
// 重命名
Method_HandleChainSimSignal = "ssc_handleChainSimSignal"  // was Method_HandleHotKeyRetrySignal
```

---

## 6. 接口定义

### 6.1 `HandleChainSimSignal`

Origin shard 接收 chain signal 的处理入口：

```go
// ssc/impl.go
func (s *sscService) HandleChainSimSignal(signal *api.ReSimulationSignal) {
    // 委托给 retryScheduler
    s.retryScheduler.HandleChainSimSignal(signal)
}
```

### 6.2 `sscs.go` 接口声明

```go
// ssc/api/sscs.go
SSCServiceServer interface {
    // ... 现有接口 ...

    // HandleChainSimSignal receives a chain optimization signal
    // from the shard where an upstream SimTx was submitted.
    // The signal carries a ChainPatch (upstream SimTx's WriteSet)
    // that allows the downstream retry simulation to read the
    // upstream's produced state values.
    HandleChainSimSignal(signal *ReSimulationSignal)
}
```

---

## 7. 关键实现细节

### 7.1 `GetState` 的 patch 检查路径

```go
// ssc/state_impl.go
func (s *sscService) GetState(db api.StateDB, txHash common.Hash,
    address common.Address, key common.Hash) (common.Hash, error) {

    // Step 0: 检查 ChainPatch（重命名自 CRHotWritePatch）
    s.stateLock.RLock()
    state, err := s.getState(txHash)
    s.stateLock.RUnlock()
    if err == nil && state != nil && state.ChainPatch != nil {
        if addrState, ok := state.ChainPatch.WriteState.State[address]; ok {
            if val, exists := addrState[key]; exists {
                // Patch 命中：使用上游 SimTx 产生的值
                // 同时缓存到 callState 的 RWSet 中
                if callState := s.GetCallState(txHash); callState != nil {
                    callState.StateLock.Lock()
                    callState.RWSet.ReadState.State[address][key] = val
                    callState.RWSet.CurrentState.State[address][key] = val
                    callState.StateLock.Unlock()
                }
                utils.SSCLogger().Debug().
                    Str("txHash", txHash.Hex()).
                    Hex("addr", address.Bytes()).
                    Hex("key", key.Bytes()).
                    Msg("GetState: ChainPatch hit")
                return val, nil
            }
        }
    }

    // Step 1: 现有缓存逻辑（不变）
    callState := s.GetCallState(txHash)
    // ... rest of existing GetState ...
}
```

### 7.2 SimTx 提交钩子

```go
// ssc/impl.go — 在 SubmitSimulationTx 成功提交后
// （具体位置取决于 implementation，与 CR 提交后的 chainHotKeyCR 类似）

func (s *sscService) afterSubmitSimulationTx(txHash common.Hash, req *SimulationRequest) {
    if !s.IsLeader(req.Epochs[s.SelfShard]) {
        return  // 仅 leader 触发 chain 优化
    }

    // 提取 WriteSet（需要确保模拟结果中的 WriteState 可用）
    writeSet := extractWriteSetFromRequest(req)
    if writeSet == nil || len(writeSet.WriteState.State) == 0 {
        return
    }

    // 触发链式下游构建
    s.retryScheduler.chainNextSim(txHash, writeSet)
}
```

### 7.3 失败回滚路径

```go
// 链中任何 SimTx 模拟/验证失败时的回调
func (rs *retryScheduler) onChainFailure(txHash common.Hash, chainRootHash common.Hash) {
    utils.SSCLogger().Warn().
        Str("failedTx", txHash.Hex()).
        Str("chainRoot", chainRootHash.Hex()).
        Msg("onChainFailure: rolling back entire dependency chain")

    // 1. 清理所有链成员的状态
    chainMembers := rs.resolveChainMembers(chainRootHash)
    for _, member := range chainMembers {
        rs.RetryCancel(member.TxHash)
    }

    // 2. 更新 retryPool 状态
    // 链中的所有 tx 重新进入等待状态（状态未变，仅模拟失败）
}
```

---

## 8. 配置

替换原 `HotKeyConfig`，新增 `ChainConfig`：

```go
// ssc/api/types.go
type ChainConfig struct {
    Enabled         bool  `json:"enabled" yaml:"enabled"`              // 总开关
    MaxPerBlock     int   `json:"max_per_block" yaml:"max_per_block"`  // 每区块最大链式 SimTx 数
    MaxDepth        int   `json:"max_depth" yaml:"max_depth"`          // 依赖链最大深度（0=不限）
}
```

放置在 `ShardSimulateCommitteeConfig` 中：

```go
type ShardSimulateCommitteeConfig struct {
    Committees []*ShardSimulateCommittee
    Timeout    *TimeoutConfig
    Chain      *ChainConfig        // NEW（替换 HotKeyConfig）
    Reputation *ReputationConfig
}
```

默认值建议：

| 参数 | 默认值 | 说明 |
|------|:-----:|------|
| `Enabled` | `true` | 默认开启 |
| `MaxPerBlock` | `50` | 每区块最多 50 笔链式 SimTx |
| `MaxDepth` | `10` | 最多链入深度 10 层，防止无限 chain |

---

## 9. 变更文件清单

| 文件 | 变更内容 | 优先级 |
|------|---------|:-----:|
| `ssc/api/types.go` | `CRHotWritePatch` → `ChainPatch`（两处）；移除 `HotKeyConfig`，新增 `ChainConfig` | P0 |
| `ssc/api/sscs.go` | `HandleHotKeyRetrySignal` → `HandleChainSimSignal`；接口声明 | P0 |
| `ssc/retry_scheduler.go` | `chainHotKeyCR` → `chainNextSim`；`HandleHotKeyRetrySignal` → `HandleChainSimSignal`；移除 `hotKeySet`/`hotKeyMu` | P0 |
| `ssc/impl.go` | 重写 `HandleHotKeyRetrySignal` → `HandleChainSimSignal`；触发点从 CR 提交移到 SimTx 提交 | P0 |
| `ssc/state_impl.go` | `GetState` 中 patch 字段名 `CRHotWritePatch` → `ChainPatch` | P0 |
| `ssc/state_impl.go` | `RetryCancel` 中 cleanup 字段名更新 | P0 |
| `ssc/statistics.go` | 移除 `HotKeyConflicts` 相关代码（如不再需要） | P1 |
| `txSubmitter.go` | 新增 `DAGOrderingFilter` | P0 |

---

## 10. 日志与观测

| 事件 | 级别 | 位置 | 关键数据 |
|------|:----:|------|---------|
| SimTx 提交，触发 chainNextSim | INFO | `chainNextSim` | txHash, writeSet size |
| 找到下游依赖 | INFO | `chainNextSim` | downstream txHash, patch key count |
| 未找到依赖（链终止） | DEBUG | `chainNextSim` | upstream txHash |
| 发 chain signal | INFO | `sendChainSignal` | target txHash, patch key count |
| Origin shard 收到 chain signal | INFO | `HandleChainSimSignal` | txHash, patch size |
| ChainPatch 命中 | DEBUG | `GetState` | txHash, addr, key |
| ChainPatch miss | DEBUG | `GetState` | txHash（patch 存在但 key 不在） |
| 整链回滚 | WARN | `onChainFailure` | failedTx, chainRoot |
| DAG nonce 分配 | INFO | `DAGOrderingFilter.assignNonce` | txHash, assigned nonce |

---

## 11. 成功指标

| 指标 | 优化前 | 目标 |
|------|:------:|:----:|
| 热点 key 每区块修改次数 | 1 | ≥3（链式执行） |
| 整体提交率（RATE=100） | ~45% | >65% |
| 回滚数 | 0（TempLockView） | 0（维持） |
| 链式 SimTx 成功率 | N/A | >80%（链内原子性） |
| DAG 分支并行度（平均） | N/A | ≥2 |

---

## 12. 实现顺序

1. **P0 基础结构变更** — `ChainPatch` 重命名（types.go），更新 `GetState` 检查路径（state_impl.go）
2. **P0 触发点迁移** — 从 CR 提交移到 SimTx 提交（impl.go `afterSubmitSimulationTx`）
3. **P0 chainNextSim** — 核心链式构建逻辑（retry_scheduler.go）
4. **P0 HandleChainSimSignal** — 信号接收与 signal aggregation 集成（retry_scheduler.go）
5. **P0 DAGOrderingFilter** — txSubmitter 排序过滤层
6. **P0 失败回滚** — 整链原子回滚逻辑
7. **P0 RPC 接口更新** — sscs.go 接口重命名
8. **P1 清理** — 移除 HotKeyConfig、hotKeySet 等废弃代码
9. **P2 实验验证** — RATE=100, delay=20, before/after 对比

---

## 13. 开放问题

### Q1: 跨 shard SimTx 的 chain 信号延迟

chainNextSim 从 A shard 的 leader 发信号到 B shard 的 leader，B shard 模拟成功后提交 SimTx。这个 RPC 往返时间能否在一个区块时间内完成？如果延迟过高，SimTx 可能赶不上当前区块，退化为"区块间 chain"（每个区块一个 SimTx）。

**潜在解决**：延迟容忍——即使 SimTx 赶不上当前区块，nonce 连续性仍然保证顺序执行。

### Q2: chain 触发时的 `TempLockView` 状态

chainNextSim 触发时，上游 SimTx 刚提交，锁还在 `StateLockManager` 中。下游 retry tx 的 `TempLockView.CanLock` 检查可能返回 false。是否应该跳过 `CanLock` 检查（因为 chain 是由 leader 调度的，锁的释放时机由 nonce 顺序保证）？

**建议**：chain 路径跳过 `CanLock` 检查，依赖区块执行时的 nonce 排序和 VerifySimulation 保证正确性。

### Q3: 链长度与区块大小

如果一条链深度达到 10 层，且每层并行度低，一个区块可能被链式 SimTx 占满。`ChainConfig.MaxPerBlock` 和 `ChainConfig.MaxDepth` 应该动态调整还是固定配置？

**建议**：先使用静态配置，实验后根据实际吞吐调整。
