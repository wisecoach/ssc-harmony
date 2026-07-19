# DSN-35: 跨 SimTx 批量并行验证

> **状态**：草案
> **对应**：HANDOFF-20260718-parallel-simtx-verify

## 1. 动机

### 1.1 现状瓶颈

单 SimTx 耗时 P50=2.31ms，足够快，但 `CommitSSCTransactions` 逐个处理：

```
for each SimTx in pool:
    w.commitTransaction(tx)  // → ApplyTransaction → precompile → VerifySimulation
    ↑                                   ↑
   串行处理                              每笔 2.31ms
```

在 rate=150 下一个块内可能有 50+ 笔 SimTx，串行耗时 115ms+，锁冲突率极低（1/78,641）。

### 1.2 目标

- 跨 SimTx 并行 execVerify，消除串行瓶颈
- Validator 侧 `state_processor.go` 的 SSC CR tx 并行处理
- 开关控制并行模式，`lockStateWithExecution` 与并行互斥

## 2. 架构设计

### 2.1 Leader 端改动

**配置开关**：`EnableParallelBatch bool`，同时互斥 `EnableLockOnConflict = false`

**`CommitTransactions` 分支逻辑：**

```
CommitTransactions:
  // 1. CR tx → 同现有（串行，极少量）
  w.CommitSSCTransactions(crTxns, ..., 0)  // maxTime=0 无预算限制

  // 2. SSC SimTx → 根据开关走不同路径
  if config.EnableParallelBatch {
      processSSCBatchParallel(w, sscTxns, remaining)
  } else {
      w.CommitSSCTransactions(sscTxns, ..., 1*time.Second)
  }

  // 3. 普通 tx → 同现有
  w.CommitSortedTransactions(normalTxns, ..., 200*time.Millisecond)
```

**`processSSCBatchParallel` 函数：**

```
function processSSCBatchParallel(w, txs, remaining):
  // Phase 0: 收集待处理 SimTxs
  batch = collectSimTxs(txs, remaining, budget)  // 按预算收集
  
  // Phase 1: 批量验证
  results = BatchVerifySimulations(batch, w.current.state, w.current.header)
  
  // Phase 2: 逐笔写入区块结果（不调 precompile verify）
  for each result in results:
      if result.passed:
          writeSimTxToBlock(tx, result)
          remaining--
  
  // Phase 3: 未通过的 SimTx 退回池
```

### 2.2 `BatchVerifySimulations` 新接口

**接口定义（`ssc/api/sscs.go`）：**

```go
type BatchVerifyResult struct {
    TxHash   common.Hash
    Passed   bool
    Error    error
}

BatchVerifySimulations(results []BatchVerifyResult, simulations [][]byte, statedb api.StateDB, header *block.Header) error
```

**实现（`ssc/verify.go`）：**

```
BatchVerifySimulations(simulations, statedb, header):
  // Phase 0: 跨 SimTx RWSet 冲突检测（O(N²) 纯内存）
  //           返回 conflictGroups: [][]int（组内无冲突，组间可并行）
  
  // Phase 1: 串行 lockCheck × 所有 SimTxs 的 CallStates
  for each SimTx in all:
      for each CallState:
          LockCheck

  // Phase 1.5: stateDB.Copy() × totalCallStates
  copies := make([]*corestate.DB, totalCallStates)
  tCopy0 := time.Now()
  for i := range totalCallStates:
      copies[i] = statedb.Copy()
  log(Copy timing)

  // Phase 2: 并行 execVerify（goroutine pool × totalCallStates）
  goroutine pool:
      for each conflictGroup:
          for each CallState in group (组内串行, 组间并行):
              execVerify(simTx, callState, copies[callStateIndex])

  // Phase 3: 串行 lockStateWithRWSet × 所有 CallStates
  for each SimTx in all:
      for each CallState:
          lockStateWithRWSet(txHash, callState, statedb)

  // 清理
  for each SimTx:
      cleanup(...)
```

### 2.3 Precompile 并行模式检测

**`ssc_contracts_write.go` 改动：**

```go
func (s *simulationCommit) RunWriteCapable(vm *SSCVM, contract *Contract, input []byte) ([]byte, error) {
    if vm.SSCService.IsParallelBatchEnabled() {
        // 并行模式：跳过 VerifySimulation，用已计算的结果
        // VerifySimulation 在 BatchVerifySimulations 中统一完成
        return nil, nil
    }
    vm.SSCService.VerifySimulation(input, vm.StateDB, vm.Context.Header)
    return nil, nil
}
```

### 2.4 state_processor.go Validator 并行

**问题**：Validator 处理区块时，SSC CR tx 逐个 `ApplyTransaction` → 修改 shared `statedb`。

**方案**：识别区块中的 SSC tx，按 SimTx 分组，组内串行、组间并行。

```
Process:
  Phase A: 遍历区块 tx，筛出 SSC tx 索引
           sscIndices = [i | tx.To() is SSC address]
  
  Phase B: 按 SimTx state key 分组
           group[txHash] → [ssc tx indices for this tx]
           不同 txHash 的组 → 可并行
  
  Phase C: 串行处理非 SSC tx（普通 tx、CX tx）
           for i not in sscIndices:
               ApplyTransaction(tx[i])
  
  Phase D: 并行处理 SSC tx
           for each group in parallel:
               copy = statedb.Copy()
               for each txIdx in group (串行):
                   ApplyTransaction(tx[txIdx], copy)
               merge(copy → statedb)
```

**`statedb.Merge` 辅助方法：**

```go
func (db *DB) Merge(other *DB) {
    for addr := range other.stateObjectsDirty {
        obj := other.stateObjects[addr]
        obj.db = db  // 重新绑定 to main statedb
        db.stateObjects[addr] = obj
        db.stateObjectsDirty[addr] = struct{}{}
    }
    // Merge logs
    for hash, logs := range other.logs {
        db.logs[hash] = logs
    }
}
```

## 3. 预测试

- `CommitTransactions` 并行分支仅影响 Leader（产块时有效）
- Validator `state_processor.go` 并行不影响账本一致性（合并策略正确）
- `lockStateWithExecution` 被禁用时不产生脏写
- `stateDB.Copy()` 的 GC 压力需要实验监控

## 4. 接口变更

| 文件 | 变更 |
|:----|:-----|
| `ssc/api/sscs.go` | 新增 `BatchVerifySimulations` 和 `IsParallelBatchEnabled` |
| `ssc/verify.go` | 新增 `BatchVerifySimulations` 实现 |
| `ssc/api/types.go` | `ShardSimulateCommitteeConfig` 新增 `EnableParallelBatch` |
| `node/worker/worker.go` | `CommitTransactions` 新增并行分支 `processSSCBatchParallel` |
| `core/vm/ssc_contracts_write.go` | `simulationCommit.RunWriteCapable` 检测并行模式 |
| `core/vm/sscvm.go` | 保留已导出的 `Run` 方法 |
| `core/state/statedb.go` | 新增 `Merge` 方法 |
| `core/state_processor.go` | `Process` 新增 SSC tx 并行路径 |

## 5. 开放问题

| 问题 | 分析 |
|:----|:-----|
| `commitTransaction` 写入区块时是否需要重新 ApplyTransaction？ | Batch 模式下，`BatchVerifySimulations` 已完成 verify + lock。写入区块只需要调用轻量的 `writeSimTxToBlock`，跳过 precompile。 |
| Validator 如何知道哪些 SimTxs 通过了 Leader 的验证？ | Leader 产出块时，只有验证通过的 SimTxs 被写入区块。Validator 看到的都是已通过的。 |
| 并行后 gas 计算是否需要调整？ | SimTx 的 gas 在提交时已预扣，验证阶段不涉及 gas 变更。 |
