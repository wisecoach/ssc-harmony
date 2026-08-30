---
id: DSN-48
title: 简单 SSC 内部交易池（存储/提取/提交后清理）+ tx_submitter 接入
type: DSN
status: implemented
priority: P1
author: Designer
created: 2026-08-27
updated: 2026-08-28
scope: [ssc/internal_pool.go(新), ssc/tx_submitter.go, ssc/impl.go, ssc/api/types.go, ssc/api/sscs.go, cmd/harmony/main.go, node/worker/worker.go, core/state_processor.go]
refs: [DSN-46, DSN-47, DSN-45]
---

> **状态**：已实现（2026-08-28 落地并回归）
> **背景**：DSN-47 已定义 `SSCInternalTx`（区块承载、SSCVM.ProcessInternal、worker/processor 出块与验证链路）。但当前 SimTx/CRTx 仍由 `tx_submitter.go` 转成普通 `types.Transaction`（precompile 地址 + JSON）提交到普通交易池。本 DSN 落地**最简内部交易池**：存储、提取、区块提交后清理，并把 `tx_submitter` 改为提交到内部池，作为 DSN-46 完整版（进池仲裁分组 + DAG）的最小可用第一步。

## 1. 概述

实现一个**只做三件事**的简单 `SSCInternalPool`：

1. **存储**：内部交易（`*SSCInternalTx`，来自 DSN-47）按类型入池；
2. **提取**：出块时按确定顺序（CRTx → SimTx → Other）取出一批；
3. **清理**：区块提交后（`OnBlockCommitted`），把已进块的内部交易从池中删除。

并把 `tx_submitter.go` 的 `SubmitSimulationTx / SubmitCommitOrRollbackTx / SubmitNewEpoch / SubmitUploadOpinions / SubmitEmptyTx` 改为**构造 `SSCInternalTx` 提交到内部池**，不再塞进普通交易池。

> 本 DSN **不**做 DSN-46 的进池仲裁分组、冲突图、DAG 就绪——那些是后续增强。此处先把“存储/提取/清理 + 提交入口替换”的闭环打通。**不设开关**：内部交易已不适配普通 `types.Transaction`（无 nonce/签名/gas），因此**无条件走内部池**，旧的“塞普通交易池”路径直接移除。

## 2. 现状与问题

- `tx_submitter.go`：原先 `SubmitSimulationTx` 等把载荷 `json.Marshal` 进 `types.Transaction.Data()`，`To=SimulationCommitAddr/CxtCommitOrRollbackAddr`，经 `nodeAPI.AddPendingTransaction` 进普通交易池（`(sender,nonce)` 排序）——该路径已不适配内部交易，本 DSN 移除。
- 无独立内部交易池：内部交易与用户交易混在一起，靠 precompile 地址 + JSON hack 区分。
- DSN-47 已把结构（`SSCInternalTx`）、区块承载（`BodyV2.SSCTransactions`）、执行入口（`SSCVM.ProcessInternal`）、出块/验证链路（worker + state_processor）打通，但**没有交易从哪来**——需要一个内部池承接提交。

### 关键代码点
| 位置 | 现状 |
|---|---|
| `ssc/tx_submitter.go` | 把内部载荷塞进普通 `types.Transaction` 提交 |
| `ssc/impl.go BlockCommitted` | 现成的每块提交钩子（可挂清理） |
| `cmd/harmony/main.go:940` | `NewTxSubmitter(...)` 注入点 |
| `node/worker/worker.go` | 出块时 `FinalizeNewBlock` 已传 `w.current.sscTxns`（当前为空） |
| `core/state_processor.go` | `Process` 已按 SSC→普通→staking 执行 `block.SSCTransactions()` |

## 3. 设计目标

1. 一个**最简**内部池：存储 / 提取 / 提交后清理。
2. `tx_submitter` 提交到内部池（构造 `SSCInternalTx`），不再进普通池。
3. 与 DSN-47 区块承载链路衔接：出块方从内部池提取 → 写入 `w.current.sscTxns` → 进块。
4. **不设开关**：内部交易无条件走内部池，移除旧的普通交易池提交路径。
5. 为 DSN-46（分组/DAG）预留演进空间：池结构上不堵死后续扩展。

## 4. 方案设计

### 4.1 无开关：无条件走内部池

内部交易（`SSCInternalTx`）已与普通 `types.Transaction` 完全分离（DSN-47），因此**不设开关**：

- `tx_submitter` 的 `Submit*` 一律构造 `SSCInternalTx` 提交到内部池并广播；
- `cmd/harmony/main.go` 恒创建共享 `SSCInternalPool`，注入 txSubmitter 与 sscService；
- worker 出块恒从内部池 `ExtractSSCTransactions` 提取。

旧的非内部池路径（nonce/签名/`AddPendingTransaction`）从 `tx_submitter.go` 中移除。

### 4.2 池结构（`ssc/internal_pool.go` 新增）

```go
// SSCInternalPool 最简内部交易池。
type SSCInternalPool struct {
    mu   sync.Mutex
    // 按类型分桶存储（第一维下标=InternalTxType，与 DSN-47 一致）
    txs  map[types.InternalTxType][]*types.SSCInternalTx
    // hash 索引：用于幂等去重 + 提交后清理
    byHash map[common.Hash]*types.SSCInternalTx
}

func NewSSCInternalPool() *SSCInternalPool
```

- **存储**：`Add(tx *types.SSCInternalTx)` 按 `tx.Type` 追加到对应桶，同时记入 `byHash`（相同 `tx.Hash()` 幂等去重）。
- **提取**：`Extract(maxTotal int) [][]*types.SSCInternalTx`——按类型顺序（`CRTx → SimTx → NewEpoch/UploadOpinions/Empty`）取，返回二维（第一维=类型桶），供 worker 写入 `w.current.sscTxns`；`maxTotal` 用于单块上限。
- **清理**：`OnBlockCommitted(block *types.Block)`——遍历 `block.SSCTransactions()`，按 `tx.Hash()` 从池中删除；未进块的保留。
- **查询**：`Len() int` / `PendingOfType(t) []*SSCInternalTx`（可选，供 debug）。

> 分组/冲突/DAG 不在本 DSN：`Extract` 直接按桶顺序取即可（最简）。DSN-46 会在 `Extract` 前插入「进池仲裁分组」得到互不冲突批次，接口保持兼容。

### 4.3 tx_submitter 改造（`ssc/tx_submitter.go`）

`txSubmitter` 简化为纯内部池提交器：

```go
type txSubmitter struct {
    selfShard    uint32
    internalPool *SSCInternalPool
    broadcaster  SSCInternalTxBroadcaster
}
```

各 `Submit*` 方法**无条件**构造 `SSCInternalTx` 提交到池并广播；旧的非内部池机制（队列/优先级/nonce/signer/retry/`AddPendingTransaction`）已删除。

```go
// SubmitSimulationTx（内部池开启时）
func (t *txSubmitter) SubmitSimulationTx(sim *api.CXTSimulation) error {
    if t.enableInternalPool {
        return t.internalPool.Add(&types.SSCInternalTx{
            Type:    types.InternalTxTypeSimTx,
            Shard:   sim.ShardId,
            Payload: sim.Bytes(), // 当前仍为 JSON；protobuf 迁移见 DSN-47 R9
        })
    }
    // ...旧逻辑不变
}
```

类型映射：
| 旧 TxType | SSCInternalTx.Type |
|---|---|
| `SimulationTx` | `InternalTxTypeSimTx` |
| `CommitOrRollbackTx` | `InternalTxTypeCRTx` |
| `NewEpochTx` | `InternalTxTypeNewEpoch` |
| `UploadOpinionsTx` | `InternalTxTypeUploadOpinions` |
| `EmptyTx` | `InternalTxTypeEmpty` |

> 注意：内部池路径不再做 nonce 分配 / 签名 / `AddPendingTransaction`——内部交易无需 `(sender,nonce)` 排序与签名，由区块数组顺序定序（DSN-46/47 语义）。`Submit*` 返回值语义保持（入池成功返回 nil）。

### 4.3.1 广播交易设计（关键）

> **为什么必须考虑广播**：当前内部交易能到达全网，靠的是 `tx_submitter → nodeAPI.AddPendingTransaction(signedTx)` → `node.AddPendingTransaction` 在入普通交易池成功后调用 `tryBroadcast(tx)`，把交易经 P2P 发到本分片 group（`api/proto/node` 的 `ConstructTransactionListMessageAccount`）。**一旦改走内部池（不进普通交易池），这条广播链路就断掉了**——只有本地节点知道这笔内部交易，validator 无法收到、无法出块/验证。因此内部池必须自带广播。

**方案：为 `SSCInternalTx` 新增独立 P2P 广播**

1. **消息载体**（`api/proto/node`）：参照 `ConstructTransactionListMessageAccount`，新增
   ```go
   // 新 header，如 sscInternalTxListH = []byte{nodeB, sscB, sendB}
   func ConstructSSCInternalTransactionListMessage(txs []*types.SSCInternalTx) []byte
   ```
   内容 = header + RLP(`[]*types.SSCInternalTx`)。接收侧按 header 分派到 SSC 内部交易处理函数。

2. **广播能力注入**：`txSubmitter` 目前只依赖 `hmy.NodeAPI`。为内部池路径新增一个窄接口（或复用 node 的能力）：
   ```go
   // txSubmitter 持有的广播器
   type SSCInternalTxBroadcaster interface {
       BroadcastSSCInternalTx(tx *types.SSCInternalTx) error
   }
   ```
   `node` 实现它，**整体实现参考普通交易广播 `node.tryBroadcast`**：构造消息 → `host.SendMessageToGroups(本分片 group, ...)` → 失败按 `NumTryBroadCast` 重试。仅消息头/载荷换成 SSC 内部交易版本。

3. **触发点**：`tx_submitter.Submit*` 在**内部池开启**时，先 `internalPool.Add(tx)` 入池，再 `broadcaster.BroadcastSSCInternalTx(tx)` 广播；接收方（本分片其他节点）收到后反序列化并 `internalPool.Add` 同一笔（`byHash` 幂等去重，重复广播无害）。

4. **广播目标**：最简版先广播到**本分片 group**（`tx.Shard` 所在分片）。SimTx 涉及跨分片（related shards）的广播暂不处理，留给 DSN-46/后续（最简版先保证本分片 validator 一致）。

5. **与区块承载衔接**：广播的是内存态“待进块”的内部交易；一旦进块，靠区块本身（已含 `SSCTransactions`）传播，广播仅是让交易尽早到达各节点。`OnBlockCommitted` 清理后不再重复广播。

### 4.4 装配与生命周期（`ssc/impl.go` / `cmd/harmony/main.go`）

- `cmd/harmony/main.go`：创建 `internalPool := ssc.NewSSCInternalPool()`，传入 `NewTxSubmitter(...)`；同时把 pool 挂到 `sscService`（供 `BlockCommitted` 调用）。
- `sscService` 持有 `internalPool *SSCInternalPool`（由 main.go 注入，与 txSubmitter 共享同一实例）。
- `sscService.BlockCommitted`：在现有各模块回调后追加 `if s.internalPool != nil { s.internalPool.OnBlockCommitted(block) }`，实现“区块提交后清理池中交易”。

### 4.5 出块衔接（`node/worker/worker.go`）

- worker 出块前**无条件**调用 `sscService.ExtractSSCTransactions(maxTotal)`，把结果写入 `w.current.sscTxns`（并执行产生 receipts），随后 `FinalizeNewBlock` 会把 `sscTxns` 进块。
- `api.Service` 增加 `ExtractSSCTransactions(max int) [][]*types.SSCInternalTx`（或等价方法），供 worker 取数。

> 最简版：`Extract` 直接取按序批次；DSN-46 会在该方法内接入“进池仲裁分组”。

### 4.6 validator 侧

- `core/state_processor.go` **无需改动**：它已按 `block.SSCTransactions()` 顺序执行（SSC→普通→staking，见 DSN-47 集成）。只要 leader 出块按相同顺序写 `sscTxns`，validator 复算即一致。

### 4.7 一致性保证

| 项 | 保证 |
|---|---|
| 存储 | 按类型分桶 + `byHash` 幂等 |
| 提取 | 按类型顺序（CR→Sim→Other），`maxTotal` 限流 |
| 清理 | 按 `block.SSCTransactions()` 的 `tx.Hash()` 精确删除进块交易 |
| 顺序 | 以区块数组顺序为唯一序号（leader 写、validator 跟） |

## 5. 变更文件清单

| 文件 | 改动 |
|:-----|:-----|
| `ssc/internal_pool.go`（新） | `SSCInternalPool`：Add / Extract / OnBlockCommitted / Len |
| `ssc/api/sscs.go` | `ExtractSSCTransactions(max)` |
| `ssc/tx_submitter.go` | 各 `Submit*` 无条件构造 `SSCInternalTx` 提交到池并广播；删除旧 nonce/普通交易池机制 |
| `api/proto/node/node.go` | 新增 `ConstructSSCInternalTransactionListMessage`（SSC 内部交易 P2P 消息） |
| `node/node.go` | 实现 `BroadcastSSCInternalTx`（发到本分片 group）；接收侧分派入内部池 |
| `ssc/impl.go` | 持有 `internalPool`；`BlockCommitted` 调 `OnBlockCommitted` 清理 |
| `cmd/harmony/main.go` | 创建 internalPool、注入 txSubmitter（含广播器）与 sscService |
| `node/worker/worker.go` | 出块时 `ExtractSSCTransactions` 写入 `w.current.sscTxns` |

## 6. 验证计划

1. 内部交易经 `tx_submitter.Submit*` **无条件**进入内部池（`Len()` 增长）。
2. 广播：入池后经 `BroadcastSSCInternalTx` 发到本分片 group，其他节点收到并入本节点内部池（`byHash` 幂等，无重复）。
3. 出块时 `Extract` 取到交易并写入 `block.SSCTransactions()`（worker 已执行产生 receipts）。
4. 区块提交后 `OnBlockCommitted` 清理已进块交易（`Len()` 回落）。
5. validator 复算 state root 与 leader 一致、无分叉。
6. 回归：SimTx/CRTx 生命周期（验证→commit/rollback→retry）不受影响。

## 7. 风险与开放问题

| 问题 | 说明 | 处理 |
|---|---|---|
| 最简版无进池仲裁 | 冲突检测仍堆到出块（DSN-45/46 阶段处理） | 本 DSN 只打通链路，仲裁留给 DSN-46 |
| 提取顺序与分组 | 当前直接按桶序取，未做冲突分组 | `Extract` 接口预留；DSN-46 在提取前分组 |
| payload 仍 JSON | `SSCInternalTx.Payload` 暂为 `sim.Bytes()`（JSON） | protobuf 迁移见 DSN-47 §4.2/R9 |
| ~~广播范围~~（已修） | ~~最简版只广播本分片 group~~。实现中曾因 `CommitSimulation` 把 SimTx 的 `ShardId` 写成 origin 分片，导致每个节点把 SimTx 广播到**所有**分片 group 并收进本节点池 → 验证时 `failed to get header` / `InvalidSimulation` 大量回滚。已修：`ShardId=s.SelfShard` + 接收/提交侧按 `tx.Shard==selfShard` 过滤。 | 已修复（见 §8） |
| 广播与进块竞态 | 广播是“尽早到达”，最终一致性靠区块承载 | 进块后 `OnBlockCommitted` 清理，不再重复广播 |

---

## 8. 实现落地与回归修复（2026-08-28）

> 本节记录 DSN-48 落地后实测发现并修复的问题，供后续维护参考。

### 8.1 已完成（对照 §5 变更清单）

- `ssc/internal_pool.go`（新）：`SSCInternalPool`（Add / Extract / Remove / OnBlockCommitted / Len）。
- `ssc/tx_submitter.go`：`Submit*` 全部构造 `SSCInternalTx` 提交到内部池并广播；删除旧 nonce/签名/普通交易池机制。
- `ssc/impl.go`：持有 `internalPool`；`BlockCommitted` 调 `OnBlockCommitted` 清理；新增 `ExtractSSCTransactions` / `DiscardSSCInternalTx`。
- `cmd/harmony/main.go`：创建共享 `internalPool`，注入 txSubmitter（含广播器）与 sscService；`SetSSCInternalTxSink` 接收侧回调。
- `node/node.go` / `api/proto/node`：`BroadcastSSCInternalTx` + `ConstructSSCInternalTransactionListMessage`（P2P 广播）。
- `node/node_handler.go`：`SSCInternalSend` 反序列化 → 入内部池 sink。
- `node/worker/worker.go`：出块时 `ExtractSSCTransactions` 写入 `w.current.sscTxns` 并执行产生 receipts。
- `core/state_processor.go`：validator 复算已按 `block.SSCTransactions()` 顺序执行（DSN-47 集成）。

### 8.2 严重回归：跨分片 SimTx 泄漏（每分片 SimTx 边界被打破）

**现象**：`rate=200` 实验回滚率极高，回滚 reason 几乎全是 `InvalidSimulation`，细分 `failed to get header for block hash 0x...`（7.7 万+）。

**根因**：
- `CommitSimulation`（`ssc/impl.go`）构造 `CXTSimulation` 时用了 `ShardId: commit.ShardId`（= **origin** 分片，来自 `BaseBLSSignedMessage.ShardId`）。
- 于是**每个节点**都会为所有 origin（0/1/2/3）各建一条 SimTx，全部：
  - 塞进**本节点自己的**内部池（`submitInternal` 无条件 `internalPool.Add`）；
  - 广播到对应 origin 的**全部分片 group**（`BroadcastSSCInternalTx` 按 `tx.Shard` 发）。
- 节点出块把“别的分片的 SimTx”也提取进块 → 本节点验证时对**非本分片 BlockHash** 调 `GetHeaderByHash` → nil → `InvalidSimulation` 回滚。

**证据**：shard 0（port 9000）的 `tryBroadcastSSCInternalTx` 出现 `node/shard/1`、`node/shard/2`、`node/shard/3` 的 group（本应只有 `node/beacon`）；单笔交易 `0x1df82d...` 在 shard 0 上先验证自己 SimTx 成功、又验证 shard 1 的 SimTx（callIndex [0]）失败。

**修复**（三处）：
1. 主修 `ssc/impl.go`：`ShardId: commit.ShardId` → `ShardId: s.SelfShard`——每个节点只建/广播自己分片的 SimTx。
2. 接收侧兜底 `cmd/harmony/main.go`：`SetSSCInternalTxSink` 里 `if tx.Shard != nodeConfig.ShardID { return }`。
3. 提交侧兜底 `ssc/tx_submitter.go`：`submitInternal` 里 `if shard != t.selfShard { return nil }`。

**效果**：SimTx 广播只发本分片 group；池里只有本分片 SimTx；不再出现“验证别的分片 BlockHash”导致的 `InvalidSimulation` 回滚。

### 8.3 出块时间预算：SSC 内部交易也受 1s 限制

**现象**：`Block gas limit and usage info` 日志 `duration` 可达 3.2s+，出块超时。

**根因**：SSC 内部交易循环（`node/worker/worker.go`）在 DSN-48 接入时**没有走 1s 时间预算**，一次性把整批提取的 SSC 交易跑完，把出块拖超时；普通交易已有 `CommitSortedTransactions` 的预算。

**修复**：
- SSC 循环加 `remainingTime`（默认 1000ms）检查，超预算即 `break sscBatchLoop` 停止提取/执行；
- SSC 消耗时间从总预算扣除（`remainingTime -= time.Since(sscStart)`），普通交易继续用剩余预算；
- `Block gas limit and usage info` 日志新增 `sscTxn` 字段，正确显示本块应用的 SSC 内部交易数。

**效果**：整块（SSC + 普通 + staking）被约束在 1s 内，与之前行为一致；日志可观测 SSC 数量。

### 8.4 遗留 / 后续
- `CHAIN_RETRY_STATS` 仍是 Debug 级，实验日志默认抓不到（AGENTS 规则：统计应 Info）——如需量化 DAG 使用，需提升日志级别或把关键计数提到 Info。
- DSN-46 的进池仲裁分组 / DAG 就绪仍未做，冲突仍堆到出块阶段。
