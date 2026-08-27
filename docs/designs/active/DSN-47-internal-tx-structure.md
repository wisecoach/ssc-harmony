---
id: DSN-47
title: 专用内部交易结构（SSCInternalTx）+ 区块承载 + SSCVM 原生处理
type: DSN
status: planned
priority: P0
author: Designer
created: 2026-08-27
updated: 2026-08-27
scope: [core/types, core/vm, node/worker, core/state_processor, ssc]
refs: [DSN-45, DSN-46, EXP-07]
---

> **状态**：设计阶段
> **背景**：SimTx/CRTx 目前被硬塞进普通 `types.Transaction`（特殊 precompile 地址 + JSON-in-Data + 假 nonce），导致 JSON 开销、单账户 nonce 串行、语义不清。本 DSN 定义**独立的内部交易结构**，并作为 DSN-46（内部池）和 DSN-45（批量并行）的承载基础。

## 1. 概述

定义 `SSCInternalTx`（RLP 外壳 + protobuf 内部载荷），作为区块/交易池/网络中内部交易的统一实体；在区块 `BodyV2` 中新增**独立内部交易队列（二维、按类型分桶）**；给 SSCVM 提供**原生处理入口**，不再走「特殊地址 + precompile + JSON」的 hack。

## 2. 现状与问题

- `To = SimulationCommitAddr/CxtCommitOrRollbackAddr` + `Data = JSON(...)`：每次 marshal/unmarshal，是火焰图 JSON 开销来源。
- `nonce = simNonce/crNonce`：内部计数器却是假 nonce，被普通池 `(sender, nonce)` 排序 → 单账户串行。
- 分类靠 `To()` switch，重复且易错。
- 区块里 SimTx/CRTx 混在普通 `Transactions` 里，与用户交易无区分。

## 3. 设计目标

1. 独立的内部交易结构，类型化、去 JSON。
2. 区块里独立承载内部交易，按类型分桶（二维数组）。
3. SSCVM 原生识别/处理内部交易。
4. 排序以「区块数组顺序」为唯一权威（无 seq 字段）。
5. leader/validator 确定性一致。

## 4. 方案设计

### 4.1 SSCInternalTx 结构（定稿）

```go
// 外壳：RLP 编码（进入区块 / 交易池 / 网络传输）
type SSCInternalTx struct {
    Type    InternalTxType
    Shard   uint32
    Payload []byte   // protobuf 编码的内部载荷（CXTSimulation / CXTCommitProof / ...）
}
```

**派生方法**（内存中已反序列化为类型化对象时使用）：
```go
func (t *SSCInternalTx) Hash() common.Hash        // 由 Type+Shard+Payload(字节) 派生 Keccak
func (t *SSCInternalTx) RefTxHash() common.Hash   // 解 Payload 后取；非外部(NewEpoch等)返回 zero
```

### 4.2 设计决策（与用户对齐）

| # | 决策 |
|---|---|
| 1 | **无 `seq` 字段**：区块数组顺序 = 唯一序号；池内先后是内存态（进池先后/优先级），不落盘 |
| 2 | **单一 `Payload []byte`（protobuf）+ `Type` 标签**，不用双字段 |
| 3 | **`Upstream`/`RWSet` 不入结构**：在 SimTx protobuf 内（`UpstreamTxList`/`ChainPatch`），池内用 `rwSetCache` 缓存 |
| 4 | **`Hash()` 由内容派生**，不存字段，避免“两节点各存一份对不上” |
| 5 | **不加 `RefTxHash` 字段**：使用场景均已反序列化，取 `Payload.TxHash` 即可 |
| 6 | **外壳 RLP，内部载荷 protobuf**（混合序列化） |
| 7 | **去掉 `IsExternal()`**：是否外部直接用 `Type` 判断 |

### 4.3 区块承载：BodyV2 新增独立内部交易队列（二维）

参照 `StakingTransactions`（独立队列的先例）：

```go
type bodyFieldsV2 struct {
    Transactions          []*Transaction
    StakingTransactions   []*staking.StakingTransaction
    SSCTransactions       [][]*SSCInternalTx   // ← 新增：二维，行=类型
    Uncles                []*block.Header
    IncomingReceipts      CXReceiptsProofs
}
```

**二维数组行约定（必须确定、leader/validator 一致）**：
```go
// 行索引 = 类型（O(1) 分类，不用 switch To()）
const (
    SSCRowCR        = 0   // CRTx        （先）
    SSCRowSim       = 1   // SimTx       （后，批量并行）
    SSCRowOther     = 2   // NewEpoch/UploadOpinions/Empty（最后）
)
```

**收益**：
- 分类 O(1)、天然确定顺序（CR 行在 Sim 行前）；
- 每行可配不同处理策略（CR 串行、Sim 批量并行、其余串行）；
- validator 直接读区块顺序复算，不再重复分类。

> 注意：真正的吞吐提升来自 Sim 行内部的并行 execVerify（DSN-45）和去 nonce 串行（DSN-46）；二维数组解决的是“组织/分类 + 确定性顺序”。

### 4.4 SSCVM 原生处理入口

```go
// 不再走“普通 tx → precompile 分发 → JSON”
func (v *SSCVM) ProcessInternal(tx *SSCInternalTx, stateDB api.StateDB, header *block.Header) {
    switch tx.Type {
    case SimTx:
        sim := decodeSim(tx.Payload)      // protobuf 解码
        v.verifyExec(sim, stateDBCopy)     // 并行（DSN-45）
        // 写路径按区块顺序串行
    case CRTx:
        proof := decodeProof(tx.Payload)
        v.CommitOrRollbackWithProof(proof, stateDB, header.Number().Uint64())
    default:
        // NewEpoch / UploadOpinions / Empty
    }
}
```

### 4.5 RLP / 区块 / 集成影响

| 文件 | 改动 |
|:-----|:-----|
| `core/types/` | `SSCInternalTx` 类型 + RLP 编解码 |
| `core/types/bodyv2.go` | `SSCTransactions [][]*SSCInternalTx` 字段 + getter/setter + RLP |
| `core/types/block.go` | `Block` 增加 `sscTransactions` 字段、`extblockV2` 序列化 |
| `core/vm/sscvm.go` | `ProcessInternal` 原生入口 |
| `node/worker/worker.go` | 出块从内部池取 `batches[0]` 写入 `SSCTransactions[Sim]` |
| `core/state_processor.go` | validator 复算走同一内部交易顺序 |
| `ssc/tx_submitter.go` | 构造 `SSCInternalTx` 提交到内部池（DSN-46） |
| `ssc/` | protobuf 定义 `CXTSimulation` / `CXTCommitProof`（替换 JSON） |

### 4.6 receipt / gas 语义（待定，见开放问题）

## 5. 与 DSN-45 / DSN-46 的关系

| DSN | 职责 |
|---|---|
| DSN-47 | 内部交易结构 + 区块承载（二维）+ SSCVM 原生入口 —— **基础** |
| DSN-46 | 内部池（进池仲裁、分组、DAG 就绪）—— 消费 DSN-47 的结构 |
| DSN-45 | 拿到批次后并行 execVerify —— 消费 DSN-46 的批次 |

## 6. 验证计划

1. 默认关（`EnableInternalTx`/`EnableInternalPool`）→ 无回归。
2. 打开后 A/B：JSON 是否消除、CPU 是否从 ~4 核提升、submitToCommit 是否下降。
3. leader/validator 对同一区块 state root 一致、无分叉。

## 7. 开放问题

| 问题 | 说明 |
|---|---|
| receipt / gas 语义 | 内部交易要不要 receipt？gas 怎么算？（可参照 staking） |
| 二维数组行内顺序 | Sim 行内顺序：冲突分组后如何定？（由内部池 `batches[0]` 决定） |
| protobuf 迁移范围 | 现有 `CXTSimulation` 等从 JSON→protobuf 的工作量与兼容 |
| RLP 向后兼容 | 实验可接受区块格式变更，但需全节点同版本 |
