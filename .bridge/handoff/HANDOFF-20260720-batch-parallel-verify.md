# HANDOFF-20260720-batch-parallel-verify (续)

> from_session: 当前 Designer session
> from_role: Designer
> to_role: Designer (新 session)
> 焦点: Batch 并行验证实现完成，进入测试验证阶段

## 代码状态

**已完成并编译通过（`go build ./ssc/... ./node/worker/... ./core/vm/... ./consensus/...` ✅）：**

### DSN-35: Batch 并行验证核心

| 文件 | 改动 |
|:-----|:------|
| `ssc/impl.go` | 新增 `newStateSetFromRead`、`pendingBatchSims`、`PendingBatchSimulations()` |
| `ssc/impl.go` | `CommitSimulation` 中 batch 模式收集 SimTxs |
| `ssc/verify.go` | `batchVerifyPassed` 重写：逐笔冲突/失败追踪 + `newStateSetFromRead` 统一初始化 |
| `ssc/verify.go` | 串行路径 `verifySimulationParsed` 也统一用 `newStateSetFromRead` |
| `ssc/api/types.go` | `EnableParallelBatch` 配置字段（已有） |
| `ssc/api/sscs.go` | `BatchVerifySimulations` + `IsParallelBatchEnabled` 接口（已有） |

### 日志增强

| 文件 | 改动 |
|:-----|:------|
| `core/vm/ssc_contracts_write.go` | precompile `verify simulation` 改为 Info 级别；batch skip 标记 Info |
| `node/worker/worker.go` | `SSC tx commit timing` 新增 `Str("txType", ...)` 字段（CR/SimTx/SL/NewEpoch/normal） |

### 交易分类（CommitTransactions）

| 文件 | 改动 |
|:-----|:------|
| `node/worker/worker.go` | `CommitTransactions` 改为接收 `pendingPoolTxs` + `sscAddrSet`，内部分类 CR/SimTx/SL/NewEpoch/OtherSSC/Normal 按序执行 |
| `node/worker/worker.go` | SimTx 处理后立即调用 `BatchVerifySimulations` |
| `consensus/consensus_block_proposing.go` | 移除外部分类逻辑，直接传 `pendingPoolTxs` + `sscAddrSet` |

### 配置

| 文件 | 改动 |
|:-----|:------|
| `cmd/build_keys/build_keys.go` | `EnableParallelBatch: true`（已启用） |

### 分析脚本

| 文件 | 位置 |
|:-----|:------|
| `scripts/analyze-tx-type-timing.py` | 本地 + 远程 `logs/harmony-sscc/` |

## Batch 并行验证架构

```
precompile (batch=on → skip VerifySimulation)
  ↓
CommitSimulation → 收集 simulation → pendingBatchSims[]
  ↓ (SimTx 上链完成后)
CommitTransactions → SimTx 处理 → CommitSSCTransactions(simNetTxns)
  ↓
PendingBatchSimulations() → 取出 pendingBatchSims
  ↓
BatchVerifySimulations(sims, stateDB, header)
  ├─ Phase 0:   提取 RWSet
  ├─ Phase 0.5: 同块冲突仲裁（串行）
  ├─ Setup:     链式补丁、定时器、上下文
  ├─ Phase 1:   跨 SimTx 并行 lockCheck（goroutine pool）
  ├─ Phase 1.5: 子上下文预分配（currentState 从 ReadState 初始化）
  ├─ Phase 2:   stateDB.Copy() × N
  ├─ Phase 3:   全并行 execVerify（goroutine pool）
  ├─ Phase 4:   串行 lockStateWithRWSet（仅成功 CallStates）
  ├─ Phase 4.5: 冲突/失败的 SimTx 发 retry/rollback
  └─ Phase 5:   成功的 SimTx 发 commit vote
```

## 实验数据（旧 baseline，batch=off）

| 参数 | rate=150 | rate=200 |
|:-----|:--------:|:--------:|
| VerifySimulation 次数 | 70,428 | 75,312 |
| VS_total avg | 3,675ms | 3,801ms |
| VS_total P50 | 1,920ms | 1,875ms |
| VS_lockCheck avg | 155ms | 176ms |
| VS_execVerify avg | 3,481ms | 3,588ms |
| VS_lockState avg | 3,543ms | 3,654ms |

→ execVerify（并行）≈ total 的 95%，lockCheck（串行）≈ 5%。batch 预期 shard 3 的 681 unfinished 应该能大幅降低。

## 待办

- [ ] **同步远程 199 + make + 跑实验**（已配置 `EnableParallelBatch: true`）
- [ ] 对比 rate=100/150/200 的 TPS / unfinished / VerifySimulation 变化
- [ ] 确认 `batch mode: skip single VerifySimulation` marker 出现
- [ ] 确认 `BatchVerifySimulations: start / done / passed phase done` marker 出现
- [ ] 分析 batch 后各 shard 的 unfinished 数量变化

## 已知风险

| 风险 | 缓解 |
|:-----|:------|
| Shard 3 节点被 kill（之前实验出现 `148606 已杀死`） | 检查远程节点状态后再跑 |
| `EnableParallelBatch` 未生效（`IsParallelBatchEnabled` 返回 false） | 已确认 `build_keys.go` 配置；检查日志中 `batch_skip_verify` marker |
| batch 模式下 precompile 跳过验证，但若 `PendingBatchSimulations()` 返回空则 SimTx 不会验证 | 检查 `CommitSimulation` 日志确认收集是否正常 |
| 日志级别 Info，`SSC tx commit timing` 和 `verify simulation` 现在可抓取 | ✅ 已改为 Info |
