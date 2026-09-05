# DSN-37: StateProcessor 支持 BatchVerify

## 1. 问题

Batch 模式下，只有 Leader 节点在 `worker.go` 打包阶段调用了 `BatchVerifySimulations` 并发送 Commit vote。Validator 节点在 `state_processor.go:Process` 重新执行区块交易时，SimTx 走 `ApplyCXTTransaction` → `simulationCommit.RunWriteCapable`，在 batch 模式下直接 `return nil, nil` 跳过，**不执行任何验证，也不发送 Commit vote**。

结果：Commit vote 只有 Leader 的 1 票，永远达不到 threshold（如 3/3），交易永远无法 commit。

## 2. 方案

### 2.1 设计思路

将 `StateProcessor.Process` 的交易处理逻辑改为与 `Worker.CommitTransactions` 相同的分类分批模式：

1. **先分类**：遍历 `block.Transactions()`，按 `To()` 地址将交易分到不同桶
2. **按优先级分批处理**：CR → SimTx → SL → NE → Normal
3. **SimTx 批处理完后**：调用 `BatchVerifySimulations`

### 2.2 关键约束

**Receipts 顺序**：区块交易顺序已经是 Leader 按优先级排好的（CR → Sim → SL → NE → Normal），分类后批次内保持原始顺序，各批按处理顺序 append 到 receipts 数组，顺序与 `block.Transactions()` 完全一致。无需记录索引回填。

**OtherSSC 省略**：Worker 中的 "其他 SSC Tx" 分类（sender 在 `sscAddrSet`）仅用于 mempool 选交易时的静态分类。Validator 重新执行时，这类交易只需正常走 `ApplyTransaction`，不需要特殊处理。因此 `StateProcessor.Process` 不做此分类，只按 `To()` 地址分五类：CR / Sim / SL / NE / Normal。

**sscAddrSet 不需要**：`WriteCapablePrecompiledSSCContracts` 是全局 map，包含了所有 SSC 合约地址（SimulationCommitAddr, CxtCommitOrRollbackAddr, NewEpochAddr, SLOpinionAddr, EmptyAddr），不是 Worker 中的 `sscAddrSet`（动态 SSC submitter）。由于省略了 OtherSSC 分类，`sscAddrSet` 对于 `Process` 不需要。

## 3. 详细实现

### 3.1 txBucket 类型

```go
type txBucket struct {
    txs []*types.Transaction
}
```

各桶的定义：

```
crBucket:   To() == CxtCommitOrRollbackAddr
simBucket:  To() == SimulationCommitAddr
slBucket:   To() == SLOpinionAddr
neBucket:   To() == NewEpochAddr
normalBucket: 所有其他交易（包括 non-cross-shard、其他 SSC precompile 等）
```

### 3.2 Process 方法改造

```
// 原：
//   for i, tx := range block.Transactions() { ... 逐笔处理 ... }

// 改为：
// 第一轮：分类
buckets := classifyByToAddr(block.Transactions())
//   结果：crBucket, simBucket, slBucket, neBucket, normalBucket

// 第二轮：按优先级分批处理
//   1. CR Tx → 调 readApplyCXTTransaction
processBucket(crBucket, ...)
//   2. SimTx → 调 readApplyCXTTransaction，然后 BatchVerifySimulations
processBucket(simBucket, ...)
if p.sscService.IsParallelBatchEnabled() {
    p.doBatchVerify(simBucket, statedb, header)
}
//   3. SL Upload → 调 readApplyCXTTransaction
processBucket(slBucket, ...)
//   4. NewEpoch → 调 readApplyCXTTransaction
processBucket(neBucket, ...)
//   5. Normal → 调 readApplyTransaction（原 ApplyTransaction）
processNormalBucket(normalBucket, ...)
```

### 3.3 processBucket 函数

对桶内交易逐一执行，生成 receipts，记录 cxReceipts 和 stakeMsgs：

```go
func (p *StateProcessor) processBucket(
    bucket *txBucket,
    statedb *state.DB, beneficiary *common.Address,
    gp *GasPool, header *block.Header, cfg vm.Config,
    usedGas *uint64, crossGasUsed *uint64,
) ([]*types.Receipt, types.CXReceipts, []staking.StakeMsg) {
    // 对桶内每笔交易：
    //   if tx.CrossShard() → ApplyCXTTransaction(p.sscService, ...)
    //   else → ApplyTransaction(...)
    //   收集 receipts, cxReceipts, stakeMsgs
}
```

### 3.4 doBatchVerify 函数

SimTx 桶处理完后调用。从每个 SimTx 的 `tx.Data()` 反序列化 `CXTSimulation`，调 `BatchVerifySimulations`：

```go
func (p *StateProcessor) doBatchVerify(
    bucket *txBucket,
    statedb *state.DB, header *block.Header,
) {
    sims := make([]api.CXTSimulation, 0, len(bucket.txs))
    for _, tx := range bucket.txs {
        var sim api.CXTSimulation
        if err := json.Unmarshal(tx.Data(), &sim); err != nil {
            utils.SSCLogger().Error().Err(err).
                Str("txHash", tx.Hash().Hex()).
                Msg("doBatchVerify: failed to unmarshal SimTx")
            continue
        }
        sims = append(sims, sim)
    }
    p.sscService.BatchVerifySimulations(sims, statedb, header)
}
```

### 3.5 Normal 交易的处理

Normal 桶包括：non-cross-shard 交易 + `SSCAddrsApplyOnChain` 中的非五类地址（只有 `EmptyAddr`） + 其他跨分片交易（不修改状态、仅更新 nonce 的 cross shard tx）。

对 Normal 桶沿用原始代码的逐笔 `tx.CrossShard()` 判断路径（`ApplyCXTTransaction` / `ApplyTransaction`）：

```go
for _, tx := range bucket.txs {
    if tx.CrossShard() {
        receipt, cxReceipt, stakeMsgs, _, err = ApplyCXTTransaction(...)
    } else {
        receipt, cxReceipt, stakeMsgs, _, err = ApplyTransaction(...)
    }
}
```

### 3.6 CrossShard 区分

注意 `tx.CrossShard()` 对所有 SSC precompile 地址的交易都返回 true（它们是跨分片交易），包括 CR/Sim/SL/NE。但分类后，这些交易在对应桶中用 `ApplyCXTTransaction` 处理。Normal 桶中 `tx.CrossShard()` 为 true 的交易只有那些普通跨分片交易（`SubtractionOnly` 等），不需要特殊处理。

## 4. 与 Worker 的对齐

| 环节 | Worker.CommitTransactions | StateProcessor.Process |
|------|--------------------------|----------------------|
| 分类方式 | 遍历 mempool，按 To()+sender 分类 | 遍历 block.Transactions()，按 To() 分类 |
| 分类类型 | CR / Sim / SL / NE / OtherSSC / Normal | CR / Sim / SL / NE / Normal（OtherSSC 省略） |
| 优先级 | CR → Sim → SL → NE → OtherSSC → Normal | 同左 |
| SimTx 后处理 | 调 `BatchVerifySimulations` | 同左 |
| 执行函数 | `CommitSSCTransactions` | `ApplyCXTTransaction`（CR/Sim/SL/NE），`ApplyTransaction`（Normal） |
| 是否需要 sscAddrSet | 是（区分 OtherSSC） | 否（省略 OtherSSC） |

## 5. 变动文件清单

| 文件 | 改动 |
|------|------|
| `core/state_processor.go` | `Process` 方法重构：分类→分批处理；新增 `processBucket`，`doBatchVerify` 辅助函数；新增 `encoding/json` import |
| `core/state_processor.go` | 无接口/结构体变更；`Process` 签名不变 |
