# Bug 修复记录：A05 / A06 / A07 代码实现问题

> **评估日期**：2026-06-21
> **涉及文档**：`docs/A05-hotkey-retry-design.md` v4-v6、`docs/A06-lock-priority-coordination.md` v5、`docs/A07-force-simulation.md` v1
> **范围**：本轮开发（A07 实现 → onChainPatches v6 实现）中发现并修复的 bug

---

## 目录

1. **[Critical] Bug #1：ReadSet Wound-Wait 逻辑缺失**
2. **[Medium] Bug #2：CommitSimulation 被 Wound 后无 clean closure**
3. **[Medium] Bug #3：ConflictKeys 为无效 placeholder**
4. **[Critical] Bug #4：CommitSimulation 中 UpstreamTxHash 始终为空**
5. **[Critical] Bug #5：sendChainSignal 未设置 ChainNode.UpstreamTxHash**
6. **[Medium] Bug #6：ForceSimulation 导致链上验证失败**
7. **[Minor] 偏差 #7：closeTransaction 只清链下 patches/PatchPool**
8. **[Minor] 偏差 #8：Priority 文档字段顺序不一致**

---

## [Critical] Bug #4：CommitSimulation 中 UpstreamTxHash 始终为空

### 状态 ✅ 已修复（2026-06-21）

### 位置
`ssc/impl.go` `CommitSimulation()` — SimTx 构建代码

### 现象
链式交易的 SimTx 在链上验证时，所有节点看到的 `hasUpstream=false`，意味着链式依赖信息没有传递到 SimTx 中。`ReadOnChainPatch` 从未被调用，一致性检查退化到只能由 leader 通过 `readPatchChain` 完成。

### 日志证据
```
AddOnChainPatch: 6010（写入正常）
但 hasUpstream=true: 0（全部错误）
ReadOnChainPatch: 0（从未被调用）
```

### 根因
`CommitSimulation` 构建 SimTx 时使用 `GetChainPatchRef(txHash)` 来获取上游信息：

```go
// 错误代码（impl.go）
if chRef := s.retryScheduler.GetChainPatchRef(txHash); chRef != nil {
    upstreamTxHash = chRef.TxHash      // ← 这是当前 tx 自己的 hash，不是上游的！
    upstreamSimNum = chRef.SimulationNum
}
```

`GetChainPatchRef` 的实现返回 `node.TxHash`，即**当前 tx 自己的 hash**，不是上游的 `UpstreamTxHash`。

```go
// retry_scheduler.go
func (rs *retryScheduler) GetChainPatchRef(txHash common.Hash) *api.TxSimKey {
    for _, node := range rs.patches[txHash] {
        if node != nil {
            return &api.TxSimKey{
                TxHash:        node.TxHash,    // ← 自己的 hash
                SimulationNum: node.SimulationNum,
            }
        }
    }
}
```

### 修复
新增 `GetUpstreamTxRef(txHash)` 方法，返回真实的 `(UpstreamTxHash, UpstreamSimNum)`：

```go
// retry_scheduler.go
func (rs *retryScheduler) GetUpstreamTxRef(txHash common.Hash) (common.Hash, int) {
    for _, node := range rs.patches[txHash] {
        if node != nil && node.UpstreamTxHash != (common.Hash{}) {
            return node.UpstreamTxHash, node.UpstreamSimNum
        }
    }
    return common.Hash{}, 0
}
```

`CommitSimulation` 改用此方法。

### 文件
- `ssc/retry_scheduler.go` — 新增 `GetUpstreamTxRef`
- `ssc/impl.go` — 调用 `GetUpstreamTxRef` 替代 `GetChainPatchRef`

---

## [Critical] Bug #5：sendChainSignal 未设置 ChainNode.UpstreamTxHash

### 状态 ✅ 已修复（2026-06-21）

### 位置
`ssc/retry_scheduler.go` `sendChainSignal()`

### 现象
Bug #4 修复后仍然 `hasUpstream=true: 0`，因为 `patches[txHash][simNum].UpstreamTxHash` 恒为零值。`GetUpstreamTxRef` 读不到任何上游信息。

### 根因
`sendChainSignal` 创建 ChainNode 时，`UpstreamTxHash` 和 `UpstreamSimNum` 被注释为"将在 HandleRetrySignal 中设置"，但实际上**从未在任何地方被设置**：

```go
// 错误代码（retry_scheduler.go）
chainNode := &api.ChainNode{
    TxHash:        txHash,
    SimulationNum: retryTx.SimulationNum,
    Patch:         writeSet,
    // UpstreamTxHash 和 UpstreamSimNum 将在 HandleRetrySignal 中设置 ← 从未实现！
}
```

### 修复
1. `chainNextSim` 加 `upstreamSimNum` 参数
2. `sendChainSignal` 加 `upstreamTxHash`/`upstreamSimNum` 参数
3. ChainNode 创建时直接填入

```go
// 修复后
chainNode := &api.ChainNode{
    TxHash:         txHash,
    SimulationNum:  retryTx.SimulationNum,
    Patch:          writeSet,
    UpstreamTxHash: upstreamTxHash,   // ✅
    UpstreamSimNum: upstreamSimNum,   // ✅
}
```

### 调用链修复
`impl.go` 中 `chainNextSim(txHash, writeSet)` → `chainNextSim(txHash, commit.SimulationNum, writeSet)`

### 文件
- `ssc/retry_scheduler.go` — `chainNextSim` 加参数、`sendChainSignal` 加参数、`OnPatchPoolUpdated` 调用修复
- `ssc/impl.go` — `chainNextSim` 调用传 `commit.SimulationNum`

---

## [Medium] Bug #6：ForceSimulation 导致链上验证失败

### 状态 ⏸️ 已暂关（`ForceSimulation=false`），等待 onChainPatches 完善后重新评估

### 位置
`core/vm/sscis_simulation_call.go`、`core/vm/sscis_simulation_recall.go`、`core/ssc_state_transition.go`

### 现象
`ForceSimulation=true` 时，链上 `VerifySimulation` 报大量 `simulation result is not equal to execution result` 错误。日志：

```
error: simulation result is not equal to execution result, expected: [], got: [0 0 0 0 ... 0 0 1]
```

实验数据：`failed to verify execution for call state: 1905` 次。

### 根因
`ForceSimulation` 在锁冲突时从 stateDB 读当前值继续执行，但该值可能已被更高优先级的交易修改（Wound-Wait 场景下常见），导致模拟结果与链上实际执行结果不同。模拟基于过期状态执行，产生了错误的返回结果。

ForceSimulation 的核心假设"每个 key 只被一笔交易最终写入"在 Wound-Wait 优先级机制下被打破。

### 修复
设置 `ForceSimulation=false`（默认关闭），等待 `onChainPatches` v6 完善后重新评估。

### 文件
- `cmd/build_keys/build_keys.go` — `ForceSimulation: false`
- `docs/A07-force-simulation.md` — 顶部标注已暂关 + 根因说明

---

## [Minor] Bug #7：VerifySimulation 一致性检查仍用 leader 侧 patches

### 状态 ✅ 已修复（2026-06-21）

### 位置
`ssc/verify.go`

### 现象
非 líder 节点在 VerifySimulation 的 Patch vs stateDB 一致性检查中，使用的是 `readPatchChain`（读 leader 侧 `patches` 缓存），而非所有节点共享的 `onChainPatches`。

### 修复
将 `readPatchChain` 替换为 `ReadOnChainPatch`：

```go
// 修复前
expectedVal, found := v.retrySchd.readPatchChain(
    chainPatchRef.TxHash, chainPatchRef.SimulationNum, address, key)

// 修复后
expectedVal, found := v.retrySchd.ReadOnChainPatch(
    simulation.UpstreamTxHash, simulation.UpstreamSimNum, address, key)
```

此外，`isChainTx` 判断也从 `GetChainPatchRef` 改为直接从 `simulation.ChainPatch != nil` 判断。

### 文件
- `ssc/verify.go` — `isChainTx` 判断 + 一致性检查路径

---

## [Minor] 偏差 #8：closeTransaction 只清链下 patches/PatchPool

### 状态 ✅ 已验证（no change needed）

### 位置
`ssc/impl.go` `closeTransaction()`

### 说明
`closeTransaction` 清理 `patches`（通过 `StaleTx`）和 `PatchPool`（通过 `Remove`），但不清理 `onChainPatches`。这是正确的设计——`onChainPatches` 的链上清理由 `Committer.CommitOrRollbackWithProof` 完成，链下清理由超时/失败路径处理。

### 当前清理归属

| 数据 | 清理时机 | 清理方法 |
|------|----------|----------|
| `patches`（链下，仅 leader） | 交易终结 | `closeTransaction` → `StaleTx` |
| `PatchPool`（链下，仅 leader） | 交易终结 | `closeTransaction` → `patchPool.Remove` |
| `onChainPatches`（所有节点） | CR 完成后（链上） | `Committer.CommitOrRollbackWithProof` → `RemoveOnChainPatch` |

---

## 总结

| 严重程度 | Bug ID | 描述 | 文件 | 状态 |
|:--------:|:------:|------|------|:----:|
| 🔴 Critical | #1 | ReadSet Wound-Wait 逻辑缺失 | `ssc/temp_lock_view.go` | ✅ 已修 |
| 🟡 Medium | #2 | CommitSimulation 被 Wound 后无 clean closure | `ssc/impl.go` | ✅ 已修 |
| 🟡 Medium | #3 | ConflictKeys 为无效 placeholder | `ssc/simulator.go`, `ssc/simulator_member.go` | ✅ 已修 |
| 🔴 Critical | #4 | CommitSimulation 中 UpstreamTxHash 始终为空 | `ssc/impl.go`, `ssc/retry_scheduler.go` | ✅ 已修 |
| 🔴 Critical | #5 | sendChainSignal 未设置 ChainNode.UpstreamTxHash | `ssc/retry_scheduler.go`, `ssc/impl.go` | ✅ 已修 |
| 🟡 Medium | #6 | ForceSimulation 导致链上验证失败 | `cmd/build_keys/build_keys.go` | ⏸️ 已暂关 |
| 🟢 Minor | #7 | VerifySimulation 一致性检查仍用 leader 侧 patches | `ssc/verify.go` | ✅ 已修 |
| 🟢 Minor | #8 | closeTransaction 清理归属设计正确 | `ssc/impl.go` | ✅ 确认无需改动 |
