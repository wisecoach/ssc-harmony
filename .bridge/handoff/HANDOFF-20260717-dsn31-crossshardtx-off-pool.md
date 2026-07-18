# HANDOFF-20260717-dsn31-crossshardtx-off-pool

> from_session: current
> from_role: Designer
> to_role: Designer
> focus: DSN-31 实现 + 实验验证待跑

## 完成内容

| 事项 | 状态 |
|:-----|:------|
| DSN-31 设计文档 | ✅ 已写入 `docs/designs/active/DSN-31-skip-crossshardtx-pool.md` |
| `node/node.go:addPendingTransactions` — CrossShardTx 过滤不入池 | ✅ 已实现，编译通过 |
| `node/node.go` — CrossShardTx 不入池后仍触发异步模拟 | ✅ 已实现 |
| `data_handler.py` — 识别 `Pre-Simulating cross shard transaction` 日志为 `add` 事件 | ✅ 已更新（远程） |
| nonce 在 CRTx 时递增 | ⏳ 待处理（实验场景 nonce=0 无影响） |

### 代码改动

**`node/node.go:258-320` — `addPendingTransactions`**

```go
// AddRemotes 前过滤普通跨分片交易
if tx.CrossShard() && !vm.IsSSCAddrApplyOnChain(*tx.To()) {
    normalCrossShardTxs = append(normalCrossShardTxs, tx)
    continue  // ← 不入池
}
poolTxs = append(poolTxs, tx)
```

模拟触发遍历 `normalCrossShardTxs` 而非 `poolTxs`，不再依赖 `txErrs` 下标：

```go
for _, tx := range normalCrossShardTxs {
    // ... go node.SSCService.SimulateCXTransaction(req)
}
```

**日志：** 新加 Info 行 `"Pre-Simulating cross shard transaction (off-pool)"`，`data_handler.py` 已更新以识别此日志。

### 未完成

- **nonce 递增**：当前 CrossShardTx 不入池后 `SetNonce +1` 缺失。应在 CRTx `CommitOrRollbackWithProof` 中补充。实验场景每笔 tx 不同地址 nonce=0 无影响，可推迟。
- **`ssc_contracts_write.go`**：`cxtCommitOrRollback.RunWriteCapable` 需在 commit/rollback 后调用 `vm.StateDB.SetNonce(sender, nonce+1)`。因 `vm.Context` 中无 `Tx` 对象获取 sender，需在 `CXTCommitProof` 中加 `SenderAddr` 字段。

## 当前实验状态

### 最新实验（rate=100, delay=10, 4shards）

| 指标 | 值 |
|:-----|:----:|
| TPS | ~95 |
| 时延 P50 | ~6.1s |
| 时延 P90 | ~23s |
| CrossShardTx | 9,996 笔（将降为 0） |

### DSN-31 预期效果

| 指标 | 当前 | DSN-31 后 |
|:-----|:----:|:----------:|
| 块内 tx | SimTx+CRTx+CrossShardTx | SimTx+CRTx 仅 |
| 块垃圾 | ~5MB | ~0 |
| TPS | ~95 | ≥95（等幅略升） |
| 时延 | — | 无退化 |

## 下一步

1. rsync 代码到远程（包含 `node/node.go` 改动）
2. 远程编译验证
3. 跑实验对比日志
4. 如需处理 nonce，继续 `ssc_contracts_write.go` + `CXTCommitProof` 改动

## 参考文档

| 路径 | 说明 |
|:-----|:------|
| `docs/designs/active/DSN-31-skip-crossshardtx-pool.md` | 设计文档 |
| `node/node.go:258-320` | `addPendingTransactions` 改动 |
| `ssc-cli/data_process/sscc_log_handle/data_handler.py:663-667` | data_handler 新增日志识别 |

## 已知坑

- `make test` 需要 Docker TTY，本地环境不支持
- 本地编译需要先 `source scripts/setup_local_build.sh`（BLS/MCL 库路径）
- 远程 BLS 头文件在 `~/go/pkg/mod/github.com/harmony-one/bls@v0.0.6/`，已预编译
- rsync 后注意二进制新鲜度检查
