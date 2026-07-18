# DSN-31: CrossShardTx 不入池，直启模拟

## 1. 动机

### 1.1 问题

当前跨分片交易从客户端到模拟的路径：

```
Python 客户端 → hmy_sendRawTransaction → addPendingTransactions
                                              ↓
    poolTxs = append(poolTxs, tx)              ← 入池
    ↓
    txErrs := AddRemotes(poolTxs)              ← 占用交易池 + 块空间
    ↓
    go node.SSCService.SimulateCXTransaction   ← 异步模拟（真正工作）
    ↓
    commitTransaction → SetNonce +1            ← CrossShardTx 唯一作用
```

CrossShardTx（`to=0xff0000...`）入池后在 `commitTransaction` 中仅执行 `SetNonce +1`，模拟工作已在异步 goroutine 中完成。

**代价：**

| 指标 | rate=100 | rate=500（预计） |
|:-----|:--------:|:----------------:|
| CrossShardTx 笔数 | ~10,000 | ~50,000 |
| 块内垃圾数据 | ~5MB | ~25MB |
| 块传播延迟 | 增加 | 显著增加 |

### 1.2 根因

CrossShardTx 最初设计为"在共识上下文中调用一次 SSC 预编译"。但模拟不依赖 CrossShardTx 的任何状态修改，SimTx 已提供共识 ordering，模拟本身异步执行不需要同步等待。

---

## 2. 方案

### 2.1 核心设计

**CrossShardTx trigger 不入交易池，在 `addPendingTransactions` 中拦截并直启模拟。**

```
当前：hmy_sendRawTransaction → addPendingTransactions → 入池 → block → commitTransaction → SetNonce +1
                                                              ↓
                                                        异步模拟（goroutine）

优化：hmy_sendRawTransaction → addPendingTransactions ─┬─ 不入池
                                                       └─ 异步模拟（goroutine）
                              CRTx commit/rollback ── SetNonce +1
```

### 2.2 改动点

#### 2.2.1 `node/node.go:addPendingTransactions`

CrossShardTx trigger 在入池前过滤：

```go
// 构建 poolTxs 时过滤 trigger tx
poolTxs := types.PoolTransactions{}
for _, tx := range newTxs {
    // ...原有验证逻辑...
    if tx.CrossShard() && !vm.IsSSCAddrApplyOnChain(*tx.To()) {
        // CrossShardTx trigger：不入池，仅触发模拟
        triggerPoolTxs = append(triggerPoolTxs, tx)
        continue
    }
    poolTxs = append(poolTxs, tx)
}
txErrs := registry.GetTxPool().AddRemotes(poolTxs)

// trigger tx 的模拟触发与原流程一致
if node.Consensus != nil && node.Consensus.IsLeader() {
    for i, tx := range triggerPoolTxs {
        go node.SSCService.SimulateCXTransaction(req)
    }
}
```

需额外变量 `triggerPoolTxs []types.PoolTransaction` 记录被过滤的 tx，用于后续模拟触发。

#### 2.2.2 `core/vm/ssc_contracts_write.go`

CRTx 执行时补上 nonce 递增：

```go
func (c *cxtCommitOrRollback) RunWriteCapable(vm *SSCVM, contract *Contract, input []byte) ([]byte, error) {
    err := vm.SSCService.CommitOrRollbackWithProof(input, vm.StateDB, vm.Context.Header.Number().Uint64())
    if err != nil {
        return nil, err
    }
    // nonce 递增：补偿 CrossShardTx 不入池后缺失的 SetNonce
    if vm.Context.Tx != nil {
        sender, _ := vm.Context.Tx.SenderAddress()
        vm.StateDB.SetNonce(sender, vm.StateDB.GetNonce(sender)+1)
    }
    return nil, nil
}
```

#### 2.2.3 `ssc/impl.go:CommitOrRollbackWithProof`

确认 `CommitOrRollbackWithProof` 接口签名已接受 `StateDB` 参数（当前已有）。

### 2.3 与原链路的兼容

```
         ┌─ trigger tx 过滤不入池 ─→ 异步模拟
客户端 ──┤
         └─ 非 trigger tx（NormalTx/Staking）→ 正常入池
```

**入口统一**：客户端仍走 `hmy_sendRawTransaction`，改动仅在 `addPendingTransactions` 内部。

**后续 SimTx/CRTx 不变**：SimTx 通过 `tx_submitter.go` 入池上链，`VerifySimulation` 在 `commitTransaction` 中执行，CRTx 在 `commitTransaction` 中落盘 → 新增 `SetNonce +1`。

### 2.4 安全性分析

| 顾虑 | 分析 |
|:------|:------|
| **共识 ordering** | 模拟异步，不改变 SimTx 上链顺序 → 锁仲裁不变 |
| **双花** | nonce 由 CRTx 统一管理，同一 address 串行 |
| **实验场景** | 每笔 tx 不同地址，nonce=0，无影响 |
| **断电恢复** | 模拟结果在 SimTx 中上链，重启后重放 |
| **回退** | 原 `hmy_sendRawTransaction` 路径代码不变，trigger tx 入池路径保留作 fallback |

---

## 3. 改动清单

| 文件 | 改动 | 复杂度 |
|:-----|:------|:------:|
| `node/node.go:280-320` | `addPendingTransactions` 过滤 trigger tx 不入池 | 简单（~5 行） |
| `core/vm/ssc_contracts_write.go:66-73` | `cxtCommitOrRollback.RunWriteCapable` 加 `SetNonce +1` | 简单（~3 行） |
| `ssc/impl.go` 或 `ssc/committer.go` | 确认 CRTx 落盘时 nonce 递增不遗漏 | 验证即可 |

---

## 4. 实验验证

### 4.1 验证标准

| 指标 | 当前（rate=100） | 预期 |
|:-----|:--------------:|:-----|
| 块内 CrossShardTx | ~10,000 笔 | 0 笔 |
| SimTx + CRTx 数量 | 不变 | 不变 |
| TPS | ~95 | ≥95 |
| 时延 P50 | ~6.1s | 无退化 |
| unfinished | 111 | 不增加 |

### 4.2 验证方法

同参数跑一次实验（delay=10, rate=100, 4shards），对比日志中 `commit transaction` 各 txType 计数。
