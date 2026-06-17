# SSCC 模块化重构与 Sim DAG Chaining 设计总结

## 1. 设计目标

### 1.1 性能优化
- 从"每个区块只能修改同一个 key 一次"升级为"区块内链式多次修改"
- 利用 DAG 依赖关系并行构建多条 chain 分支
- 减少热点 key 的锁竞争瓶颈

### 1.2 代码架构优化
- `impl.go` 从 4187 行瘦身到 ~3500 行（继续缩减中）
- 按模块职责拆分，每个模块自管理存储、通过 `StateAccessor` 回调访问共享状态
- `sscService` 内嵌模块结构体，方法自动提升

---

## 2. Sim DAG Chaining 设计

### 2.1 核心思路
- **触发时机**：SimTx 提交后触发 `chainNextSim`（替代原 `chainHotKeyCR` 在 CR 提交后触发）
- **依赖定义**：`(retryTx.ReadSet ∪ retryTx.WriteSet) ∩ SimTx.WriteSet ≠ ∅`
- **DAG 结构**：树形依赖——每个下游 tx 只有一个上游来源，多个下游可并行
- **ChainPatch 累积**：逐级累积上游 WriteSet，下游 GetState 走递归回溯
- **ChainPatchRef**：每个 tx 存一级 WriteSet + UpstreamTxHash 指针（O(n) 存储，递归查询）
- **nonce 排序**：由 txSubmitter 串行 processQueue 天然保证拓扑序

### 2.2 数据结构变更

**`CXTSimulationState`：**
- `CRHotWritePatch *RWSet` → `ChainPatch *RWSet`（后续改为 `ChainPatchRef`）
- `OnChainLockedSimulationNum` — 已移入 Verifier.txLockedSimNum，从 CXTSimulationState 删除

**`ReSimulationSignal`：**
- `CRHotWritePatch` → `ChainPatch`（后续改为 `RetrySignal`）

**`retryScheduler`：**
- `chainHotKeyCR` → `chainNextSim`（SimTx 提交后触发）
- `HandleHotKeyRetrySignal` → `HandleChainSimSignal`
- `hotKeySet`/`hotKeyMu`/`updateHotKeys`/`isHotKey` — 已废弃移除
- 新增：`dependsOn`、`sendChainSignal`、`HandleChainSimSignal`

### 2.3 树形 vs DAG 结论

**结论**：采用**树形**结构，不采用 DAG。
- 树形：每个下游只被一个上游 chain 出来，ChainPatch 无需合并
- DAG：多路并发查找同一 tx 时需要依赖计数和 patch 累积，复杂度过高
- DAG 交汇节点由正常 retry 路径在下一区块处理

---

## 3. 模块化拆分现状

### 3.1 已完成的拆分

| 模块 | 文件 | 函数数 | 职责 | 自管理存储 |
|------|------|:------:|------|-----------|
| **Verifier** | `verify.go` | 17 | VerifySimulation、锁冲突检查、验证执行、结果读取 | `executionVerifyContexts`、`txLockedSimNum` |
| **Committer** | `committer.go` | 2 | CommitOrRollbackWithProof | 无（通过 accessor 回调） |

### 3.2 sscService 内嵌结构

```go
type sscService struct {
    *CommitteeMechanism  // 节点身份
    *Verifier            // 验证模块
    *Committer           // 提交/回滚模块
    // ... 其余字段 ...
}
```

### 3.3 StateAccessor 模式

每个模块通过 `XxxStateAccessor` 回调访问 `simulationState`，锁永远在 `sscService` 内部：

```go
type VerifierStateAccessor struct {
    GetState            func(txHash) (*CXTSimulationState, error)
    SetStatus           func(txHash, CXTStatus)
    IsTxFinished        func(txHash) bool
    SetWaitingForResimu func(txHash)
}

type CommitterStateAccessor struct {
    IsTxFinished func(txHash) bool
    SetStatus    func(txHash, CXTStatus)
    CloseTx      func(txHash, success bool, reason string)
}
```

**Verifier 暴露的清理接口**：
- `Verifier.Cleanup(txHash)` — 清理 `executionVerifyContexts` + `txLockedSimNum`
- `Verifier.HasVerifyContext(txHash)` — 判断是否有验证上下文

---

## 4. 下一步拆分计划

### 4.1 Simulator 模块拆分（最重要）

按 **Leader/Member 角色**物理分文件，共享同一个 `Simulator` 结构体。

**`simulator.go`** — 结构体定义 + 共用方法：
```go
type SimulatorStateAccessor struct {
    GetState        func(txHash) (*CXTSimulationState, error)
    SetStatus       func(txHash, CXTStatus)
    IsTxFinished    func(txHash) bool
    // ...
}

type Simulator struct {
    committee   *CommitteeMechanism
    bc          core.BlockChain
    comm        *Comm
    config      *api.Config
    timerMgr    *CXTTimerManager
    retrySchd   *retryScheduler
    state       SimulatorStateAccessor

    // 自管理字段（从 sscService 移入）
    callStatesInWaiting map[common.Hash][]*api.SimulationCallState
    simuWaitingChs      map[common.Hash][]chan *api.CXTSimulationSSCResult
    simuResultCh        map[common.Hash]chan *api.CXTSimulationSSCResult
    pendingRequests     map[common.Hash][]*pendingCXTRequest
}
```

**`simulator_leader.go`** — Leader 专属函数：
| 函数 | 说明 |
|------|------|
| `SimulateCXTransaction` | 发起模拟请求 |
| `StartSimulateCXTransaction` | leader 发起 |
| `thresholdSignSimulationCommit` | 收集签名 |
| `aggregateSimulationResults` | 聚合结果 |
| `CommitSimulation` | 提交模拟 |
| `buildSignaturesForSimulation` | 构建签名 |
| `HandleCXTRecallProof` | 处理 recall |
| `HandleCommitVote` | 收集投票 |
| `aggregateSSCCommitVote` | 聚合投票 |

**`simulator_member.go`** — Member 专属函数：
| 函数 | 说明 |
|------|------|
| `HandleSimulateRequest` | 接收模拟请求 |
| `HandleCXTCall` | 跨分片调用处理 |
| `RequestCallCXT` | 执行调用 |
| `startCall` / `endCall` | 调用管理 |
| `HandleCXTSSCCall` | 跨分片 SSC 调用 |
| `aggregateCXSSCCallResult` | 聚合调用结果 |

### 4.2 信号归属重命名

| 信号 | 归属 | 变更 |
|------|------|------|
| `HandleCommitVote` | **simulator_leader.go** | 不改名，Leader 收集投票 |
| `aggregateSSCCommitVote` | **simulator_leader.go** | 不改名 |
| `ReSimulationSignal` | **retry_scheduler.go** | 改名为 `RetrySignal` |

`RetrySignal` 重命名影响范围：
- `ssc/api/types.go` — 结构体定义 `ReSimulationSignal` → `RetrySignal`
- `ssc/api/types.go` — `ReSimulationSignals` → `RetrySignals`
- `ssc/api/sscs.go` — `Method_HandleReSimulationSignal` → `Method_HandleRetrySignal`
- `ssc/retry_scheduler.go` — `HandleReSimulationSignal` → `HandleRetrySignal`，`ReSimulationSignal` 引用
- `ssc/impl.go` — `SignalReSimulation`、`HandleReSimulationSignal` 委托

### 4.3 WorkerPool（可选，独立拆分）

- 独立结构体，~150 行，零耦合
- `simulationReqPriorityQueue`、`startWorkers`、`worker`、`processSimulationTask`

---

## 5. 不拆分的模块

**Simulator + CrossShard + VoteAggregator**：
- 三者是同一条流水线上的三个阶段，共享 VM 执行器、simulationState、CallForest、锁管理
- 强行拆成 3 个结构体会导致每个都需要注入大量相同依赖
- 通过 Simulator 结构体 + Leader/Member 物理分文件来改善可读性

---

## 6. 当前函数分布

| 文件 | 函数数 | 内容 |
|------|:------:|------|
| `impl.go` | 72 | 数据结构、构造函数、CXT 生命周期、信号处理、WorkerPool、辅助函数 |
| `verify.go` | 17 | Verifier 验证模块 |
| `committer.go` | 2 | Committer 提交/回滚模块 |
| `retry_scheduler.go` | ~15 | RetryScheduler（含 chainNextSim、RetrySignal） |
| **合计** | ~106 | |

---

## 7. Git 操作参考

```bash
# 当前状态（已 commit）
git log -1   # 确认最新 commit

# 开发中随时查看改动
git diff --stat

# 需要回滚时
git checkout -- <file>     # 单个文件
git stash                  # 临时保存改动
git reset --hard HEAD      # 全部回滚到最新 commit
```
