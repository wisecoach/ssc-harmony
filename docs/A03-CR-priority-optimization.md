# [A03] CR 提交优先级优化

> **阅读顺序**（`[前缀]` 表示阅读顺序）：
> 1. 📄 `docs/A01-sscc-refactor-plan.md` [A01] — SSCC 模块化重构总览
> 2. 📄 `docs/A02-lock-retry-mechanism.md` [A02] — 锁与重试机制
> 3. 📄 `docs/A03-CR-priority-optimization.md` [A03] **（本文）** — CR 提交优先级优化
> 4. 📄 `docs/A04-onchain-retry-limit.md` [A04] — 链上重试次数限制
> 5. 📄 `docs/A05-hotkey-retry-design.md` [A05] — HotKey 链式重试（数据层）
> 6. 📄 `docs/A06-lock-priority-coordination.md` [A06] — 锁优先级协调（协调层）
>
> **运维辅助**（与主线并行的独立文档）：
> - 📄 `docs/B01-log-query-guide.md` [B01] — 日志查询指南
> - 📄 `docs/B02-log-lifecycle.md` [B02] — 交易生命周期日志大全

> 日期：2026-06-01
> 代码库：harmony-sscc
> 状态：✅ 已部署远程，已编译运行

---

## 1. 问题背景

### 1.1 核心问题：nonce 阻塞导致 CR tx 执行延迟

- **SimulationTx** 和 **CommitOrRollbackTx** 共用同一签名账户（SSC submitter address）
- nonce 序列共享导致 CR tx 无法插队，必须等待同 nonce 序列的 SimulationTx 全部完成
- 跨分片场景下，origin shard 在 `Sp1=10` 超时后才会触发回滚（wait 10 blocks）
- nonce 间隙 + 区块等待 导致 simCommit→rollbackSubmit 延迟达 11 个块

### 1.2 次要问题

| 问题 | 描述 |
|------|------|
| **retry 上限检查仅 leader 可执行** | `CXTSimulationState.OnChainLockedSimulationNum` 依赖 state，非 leader 节点无法判断 retry 是否超限 |
| **单块无 tx 分类限制** | CR tx 和普通 SSC tx 混排，gas 竞争下 CR tx 可能被打包失败 |
| **旧上限方案** | CR 限 25/块 + SSC 限 250/块 + 普通 100/块，相互独立不共享 |

---

## 2. 优化方案

### 2.1 账户分离 — 双签名器 + 双 nonce

**改动文件：** `ssc/tx_submitter.go`, `cmd/harmony/main.go`, `cmd/build_keys/build_keys.go`, `core/genesis.go`

```
txSubmitter
├── simSigner  / simNonce   → SimulationTx / 其他所有 tx
└── crSigner   / crNonce    → CommitOrRollbackTx
```

- `tx_submitter.go`：结构体拆出 `simSigner`/`crSigner`、`simNonce`/`crNonce`
- `processTask()`：按 `txType` 分流 signer 和 nonce
- `submitWithRetry()`：成功时 `incNonce`（可传 signer 参数）
- `GetNonce()`/`SetNonce()`：统一加 `txType` 参数
- `NewTxSubmitter()`：签名改为 `(simSigner, crSigner)`
- `main.go`：从 `--ssc.commit-rollback-key-path` 加载 CR key 创建 `crSigner`
- `build_keys.go`：新增 `generate_commit_rollback_keys()`，读 `validators.json` 生成 CR 密钥对
- `genesis.go`：启动时自动从 `commit_rollback_ecdsa/` 读 `.key` 文件并配 `InitFreeFund`（100M ONE）

**效果：** CR tx 拥有独立 nonce 序列，不会被 SimulationTx 的 nonce 间隙阻塞。所有非 CR tx 仍用 `simSigner`/`simNonce`。

### 2.2 pending tx 分类 — 按 To() 识别 CR tx

**改动文件：** `consensus/consensus_block_proposing.go`

```
for addr, poolTxs := range pendingPoolTxs {
    _, isSSCAddr := sscAddrSet[addr]
    for _, tx := range poolTxs {
        if t.To() != nil && bytes.Equal(t.To().Bytes(), vm.CxtCommitOrRollbackAddr.Bytes()) {
            → pendingCRTxs    // CommitOrRollbackTx（不依赖 sender 地址）
        } else if isSSCAddr {
            → pendingSSCTxs   // 其他 SSC 交易
        } else {
            → pendingPlainTxs // 普通交易
        }
    }
}
```

- **单次遍历** pendingPoolTxs，不重复
- 使用 `To() == CxtCommitOrRollbackAddr` 判断，不依赖 sender 地址或节点持有额外 key 文件
- 同时输出分类日志：`[cr, ssc, plain] = [N, N, N]`

### 2.3 三段式提交 — 总上限 300 笔/块

**改动文件：** `node/worker/worker.go`

```
remaining := 300      // 单块总上限

// 1. CR tx 最高优先级（使用 remaining 中的配额）
if len(pendingCRTxs) > 0 {
    w.CommitSSCTransactions(crTxns, coinbase, remaining)
    remaining -= (len(w.current.txs) - before)
}

// 2. 其他 SSC tx
if remaining > 0 {
    w.CommitSSCTransactions(sscTxns, coinbase, remaining)
    remaining -= (len(w.current.txs) - before)
}

// 3. 普通 tx（剩余配额）
if remaining > 0 {
    w.CommitSortedTransactions(normalTxns, coinbase, remaining)
}
```

- **所有类型共享 300 配额**，不再是固定的 25/250/100 硬上限
- **CR tx 最高优先级**，确保跨分片事务能抢先出块
- 每个阶段 `after - before` 准确递减

### 2.4 retry 上限重构 — 全局 txLockedSimNum

**改动文件：** `ssc/impl.go`

- 新增 `txLockedSimNum map[common.Hash]int` 存储每个 txHash 的锁定模拟次数
- `newBaseService()` 中初始化
- `VerifySimulation()` 中写入 `txLockedSimNum[txHash] = lockedSimNum`
- retry 判断从 `CXTSimulationState`（leader 独占）改为 `txLockedSimNum`（所有节点可用）
- 抽成 helper 方法：`checkRetryLimitExceeded()`, `sendRollbackVoteForRetry()`
- `HandleCommitVote()` / `HandleCXTCommitSSCVote()`：添加 nil guard
- `MaxOnChainRetries=0`：第一次冲突即触发 rollback（`>=` 判断）

---

## 3. 改动文件清单

| 文件 | 改动 | 优先级 |
|------|------|--------|
| `ssc/impl.go` | retry 检查重构、nil guard、helper 方法 | 高 |
| `ssc/tx_submitter.go` | 双签名器 + 双 nonce，helper 方法分流 | 高 |
| `cmd/harmony/main.go` | 加载 CR key 创建 crSigner，传双签名器 | 高 |
| `consensus/consensus_block_proposing.go` | 单次遍历按 To() 识别 CR tx | 高 |
| `node/worker/worker.go` | 三段式 CR→SSC→普通，总上限 300 | 高 |
| `cmd/build_keys/build_keys.go` | generate_commit_rollback_keys() | 中 |
| `core/genesis.go` | 启动自动读 commit_rollback_ecdsa/ 分配余额 | 中 |
| `cmd/harmony/flags.go` | commit-rollback-key-path flag（已有） | — |
| `internal/configs/harmony/harmony.go` | CommitRollbackKeyPath 配置字段（已有） | — |
| 测试文件（x7） | 加 nil 参数适配新签名 | 低 |

---

## 4. 部署与验证

### 4.1 部署步骤

1. 本地生成 CR 密钥对：`cd cmd/build_keys && go run build_keys.go`
2. 同步代码到远程：`scp -P 10022 -r ... zjnu@10.7.95.199:~/go/src/github.com/harmony-one/harmony-sscc/`
3. 远程编译：`cd ~/go/src/github.com/harmony-one/harmony-sscc && make harmony`
4. 同步配置文件到各节点
5. 启动实验

### 4.2 验证指标

- [ ] `Leader apply CommitOrRollback txns first` 日志出现
- [ ] `[ProposeNewBlock] begin to commit transaction, [cr, ssc, plain] = [N, N, N]` 分类日志出现
- [ ] simCommit→rollbackSubmit 间隔显著缩短（期望 < 5 blocks 而非 11 blocks）
- [ ] retry 检查日志在所有节点可见（非仅 leader）
- [ ] `no response from other shards` 错误减少或消失

---

## 5. 已知问题与后续

### 5.1 已修复
- Sp1 超时后的 nil panic（HandleCXTCommitSSCVote nil guard）
- nonce too low 的广播污染（由 BroadcastInvalidTx 引起）

### 5.2 后续关注
- BLS 签名聚合失败的根本原因（aggregateSSCCommitVote 返回 nil）
- 跨分片执行时间差异（单块 gasUsed=79M 执行 66 秒的极端案例）
- SSDC/PSDC 二进制缓存不一致问题
