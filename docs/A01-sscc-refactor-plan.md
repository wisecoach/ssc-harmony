# [A01] SSCC 模块化重构总览

> **阅读顺序**：本文是 SSCC 架构系列的第一篇，建议顺序阅读：
> 1. 📄 **`docs/A01-sscc-refactor-plan.md`（本文）** — 架构总览：模块拆分、文件分布、设计原则
> 2. 📄 `docs/A02-lock-retry-mechanism.md` [A02] — 锁与重试机制
> 3. 📄 `docs/A03-CR-priority-optimization.md` [A03] — CR 优先级优化
> 4. 📄 `docs/A04-onchain-retry-limit.md` [A04] — 链上重试限制
> 5. 📄 `docs/A05-hotkey-retry-design.md` [A05] — HotKey 链式重试（数据层）
> 6. 📄 `docs/A06-lock-priority-coordination.md` [A06] — 锁优先级协调（协调层）
>
> **运维辅助**（与主线并行的独立文档）：
> - 📄 `docs/B01-log-query-guide.md` [B01] — 日志查询指南
> - 📄 `docs/B02-log-lifecycle.md` [B02] — 交易生命周期日志大全

## 1. 设计目标

### 1.1 性能优化
- 从"每个区块只能修改同一个 key 一次"升级为"区块内链式多次修改"
- 利用线性链减少热点 key 的锁竞争瓶颈
- 减少 2-3 个区块的等待时间：SimTx 提交后立即 chain，不等 CR 上链

### 1.2 代码架构优化
- `impl.go` 从 4187 行瘦身到 ~1200 行（已完成）
- 按模块职责拆分，每个模块自管理存储
- `sscService` 内嵌 `*Simulator`、`*Verifier`、`*Committer`、`*CommitteeMechanism`，方法通过内嵌自动提升
- `state_impl.go` 已删除，所有 state 方法归入 `simulator.go` 或 `verify.go`

---

## 2. Sim DAG Chaining 设计

### 2.1 核心思路
- **触发时机**：SimTx 提交后触发 `chainNextSim`（替代原 `chainHotKeyCR` 在 CR 提交后触发）
- **依赖定义**：`(retryTx.ReadSet ∪ retryTx.WriteSet) ∩ SimTx.WriteSet ≠ ∅`
- **链结构**：线性链——每次只取第一个匹配的 retry tx（非 DAG/树）
- **ChainPatch 存储**：`patches[txHash][simNum]` 链式指针，每节点只存自己的 WriteSet + 上游指针（O(n) 存储）
- **读取路径**：`readPatchChain` 递归查 patches 链，读到就停
- **VerifySimulation**：链式交易跳过锁冲突检查 + Patch vs stateDB 一致性检查
- **nonce 排序**：由 txSubmitter 串行 processQueue 天然保证拓扑序

### 2.2 数据结构变更

**`CXTSimulationState`：**
- `CRHotWritePatch *RWSet` → `ChainPatch *RWSet`（后续改为 `ChainPatchRef`）
- `OnChainLockedSimulationNum` — 已移入 Verifier.txLockedSimNum，从 CXTSimulationState 删除

**`RetrySignal`（重命名自 `ReSimulationSignal`）：**
- `CRHotWritePatch` → `ChainPatch`
- `Ready` bool 字段保留
- `ReSimulationSignal` → `RetrySignal`；`ReSimulationSignals` → `RetrySignals`

**`retryScheduler`：**
- `chainHotKeyCR` → `chainNextSim`（SimTx 提交后触发）
- `HandleHotKeyRetrySignal` / `HandleChainSimSignal` → `HandleRetrySignal`
- `hotKeySet`/`hotKeyMu`/`updateHotKeys`/`isHotKey` — 已废弃移除
- 新增：`chainNextSim`、`dependsOn`、`sendChainSignal`、`HandleRetrySignal`、`readPatchChain`、`GetChainPatchRef`
- `AddToRetry` 内化 RWSet 提取（通过 `GetSimState` accessor 回调）
- `signals` map 从 `*ReSimulationSignal` 改为 `*RetrySignal`

### 2.3 树形 vs DAG 结论

**结论**：采用**树形**结构，不采用 DAG。
- 树形：每个下游只被一个上游 chain 出来，ChainPatch 无需合并
- DAG：多路并发查找同一 tx 时需要依赖计数和 patch 累积，复杂度过高
- DAG 交汇节点由正常 retry 路径在下一区块处理

---

## 3. 模块化拆分现状

### 3.1 已完成的结构

| 模块 | 文件 | 函数数 | 职责 | 自管理存储 |
|------|------|:------:|------|-----------|
| **Simulator** | `simulator.go` + `simulator_leader.go` + `simulator_member.go` | 45+ | 跨分片模拟执行（Leader/Member 分角色） | `simStates`、`callStatesInWaiting`、`pendingRequests`、`simuResultCh`、`simuWaitingChs` |
| **Verifier** | `verify.go` | 17+ | VerifySimulation、锁冲突检查、验证期 state 操作 | `executionVerifyContexts`、`txLockedSimNum` |
| **Committer** | `committer.go` | 2 | CommitOrRollbackWithProof | 无（通过 accessor 回调） |
| **RetryScheduler** | `retry_scheduler.go` | 15+ | 重试池管理、链式依赖 chain、信号聚合 | `retryPool`、`signals`、`patches` |

### 3.2 模块内嵌结构

```go
type sscService struct {
    *CommitteeMechanism  // 节点身份
    *Verifier            // 验证模块
    *Committer           // 提交/回滚模块
    // 字段（非内嵌）
    Simulator     *Simulator         // 模拟模块（通过引用而非内嵌，因 ssCService 引用 Simulator 的 sscService 需要）
    retryScheduler *retryScheduler   // 重试调度器
    // ... 其余字段 ...
}
```

### 3.3 state_impl.go 已删除

所有 `api.Service` 接口方法已按职责分配到：

- **`simulator.go`** — 模拟执行期：`GetCallState`、`GetRWSet`、`GetState`、`SetState`、`GetBalance`、`AddBalance`、`SubBalance`、`CreateAccount`、`EndCTX`
- **`verify.go`** — 链上验证期：`SubSimuBalance`、`AddSimuBalance`、`GetSimuBalance`、`GetSimuState`、`SetSimuState`、`GetResult`

通过 `sscService` 内嵌 `*Simulator` 和 `*Verifier`，这些方法自动提升到 `sscService`，实现 `api.Service` 接口。

---

## 4. 剩余拆分计划

### 4.1 RetrySignal → HandleRetrySignal 命名重命名 ✅ 已完成

| 信号 | 归属 | 变更 |
|------|------|------|
| `ReSimulationSignal` | **retry_scheduler.go** | ✅ `RetrySignal`（已完成） |
| `ReSimulationSignals` | **retry_scheduler.go** | ✅ `RetrySignals`（已完成） |
| `HandleChainSimSignal` | **retry_scheduler.go** | ✅ `HandleRetrySignal`（已完成） |
| `Method_HandleChainSimSignal` | `ssc/api/sscs.go` | ✅ `Method_HandleRetrySignal`（已完成） |

### 4.2 `getState` 清理 ✅ 已完成

- 9 处改为直接访问 `getTxStateLocked` 或字段
- 3 处迁入 Simulator/RetryScheduler
- `state_impl.go` 已删除

### 4.3 `aggregateCXSSCCallResult` BLS 签名 ✅ 已完成

- 添加 `GetSSCSigner().Aggregate(results)`
- 填入 `Signatures` + `BLSBitMap`
- 修复 `empty bitmap` 导致的 `ExecutionFailed`

### 4.4 后续待做

- `impl.go` 中 `commitSimulation` 函数进一步拆分（当前 ~120 行）
- `buildSignaturesForSimulation` 可否迁入 Simulator 的讨论
- `getState` 兼容函数本身是否可删除（当前零引用，可删）

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
| `impl.go` | ~35 | 数据结构、构造函数、CXT 生命周期、信号处理、提交/签名 |
| `simulator.go` | 25+ | 模拟执行（含迁入的 api.Service state 方法）、worker pool、callStates |
| `simulator_leader.go` | 12 | Leader 协调函数 |
| `simulator_member.go` | 10+ | Member 执行函数 |
| `verify.go` | 20+ | Verifier 验证 + 验证期 state 方法 |
| `committer.go` | 2 | Committer 提交/回滚 |
| `retry_scheduler.go` | 15+ | RetryScheduler（chainNextSim、RetrySignal、patches、AddToRetry） |
| — | — | — |
| **总量** | ~120 | 比重构前 ~160 函数更清晰分模块 |

---

## 7. Git 操作参考

## 文档清单

| 文档 | 内容 | 状态 |
|------|------|:----:|
| `docs/A05-hotkey-retry-design.md` | HotKey Retry 设计方案（线性链） | ✅ 当前核心 |
| `docs/A01-sscc-refactor-plan.md` | SSCC 模块重构总览 | ✅ 当前核心 |
| `docs/A02-lock-retry-mechanism.md` | 锁与重试机制详解 | ✅ 当前 |
| `docs/A04-onchain-retry-limit.md` | 链上重试限制 | ✅ 当前 |
| `docs/A03-CR-priority-optimization.md` | CR 优先级优化 | ✅ 当前 |
| `docs/B02-log-lifecycle.md` | 日志生命周期 | ✅ 当前 |
| `docs/B01-log-query-guide.md` | 日志查询指南 | ✅ 当前 |
| `docs/archived/Z01-cr-hotkey-chaining-design.md` | CR 触发+HotKey 分类（v1） | 🗂️ 已归档 |
| `docs/archived/Z02-sim-dag-chaining-design.md` | DAG 树形链（v2） | 🗂️ 已归档 |
| `docs/archived/Z04-lock.md` | 旧锁设计 | 🗂️ 已归档 |
