---
id: DSN-47
title: 专用内部交易结构（SSCInternalTx）+ 区块承载 + SSCVM 原生处理
type: DSN
status: implemented
priority: P0
author: Designer
created: 2026-08-27
updated: 2026-08-28
scope: [core/types, core/vm, node/worker, core/state_processor, ssc]
refs: [DSN-45, DSN-46, EXP-07]
---

> **状态**：已实现（2026-08-28 落地并回归）
> **背景**：SimTx/CRTx 目前被硬塞进普通 `types.Transaction`（特殊 precompile 地址 + JSON-in-Data + 假 nonce），导致 JSON 开销、单账户 nonce 串行、语义不清。本 DSN 定义**独立的内部交易结构**，并作为 DSN-46（内部池）和 DSN-45（批量并行）的承载基础。

## 1. 概述

定义 `SSCInternalTx`（RLP 外壳 + protobuf 内部载荷），作为区块/交易池/网络中内部交易的统一实体；在区块 `BodyV2` 中新增**独立内部交易队列（一维 `[]*SSCInternalTx`，按类型在内存分桶处理）**；给 SSCVM 提供**原生处理入口**，不再走「特殊地址 + precompile + JSON」的 hack。

## 2. 现状与问题

- `To = SimulationCommitAddr/CxtCommitOrRollbackAddr` + `Data = JSON(...)`：每次 marshal/unmarshal，是火焰图 JSON 开销来源。
- `nonce = simNonce/crNonce`：内部计数器却是假 nonce，被普通池 `(sender, nonce)` 排序 → 单账户串行。
- 分类靠 `To()` switch，重复且易错。
- 区块里 SimTx/CRTx 混在普通 `Transactions` 里，与用户交易无区分。

## 3. 设计目标

1. 独立的内部交易结构，类型化、去 JSON。
2. 区块里独立承载内部交易（一维队列），执行时按类型分桶处理。
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

### 4.3 区块承载：BodyV2 新增独立内部交易队列

参照 `StakingTransactions`（独立队列的先例）。

> **R11 决策：RLP 用一维 `[]*SSCInternalTx`**（数组顺序 = 执行顺序，与 `Transactions` 语义一致，最简单、最不易出错）。**类型分桶只在内存执行时做**（worker 已有 `pendingCRTxs/pendingSSCTxs` 分类）；二维 `[][]*SSCInternalTx` 列为可选（若要在 body 层就固化顺序）。
>
> **实现落地（2026-08-28）**：实际采用了 R11 的“可选二维”分支——`bodyFieldsV2.SSCTransactions [][]*SSCInternalTx`（第一维下标=InternalTxType 类型桶，第二维=行内顺序），并在 `bodyv1/bodyv0`、`extblockV1/extblockV2`、`Block.EncodeRLP/DecodeRLP` 同步补齐（含修复 v1 body 曾丢失 SSC 的 BAD BLOCK，见 BRF-06）。

```go
type bodyFieldsV2 struct {
    Transactions          []*Transaction
    StakingTransactions   []*staking.StakingTransaction
    SSCTransactions       []*SSCInternalTx   // ← 新增：一维，顺序=执行顺序
    Uncles                []*block.Header
    IncomingReceipts      CXReceiptsProofs
}
```

**执行时分类（内存态）**：
```go
// worker / state_processor 执行时按 Type 分桶，行顺序固定：
//   CRTx(先) → SimTx(后,批量并行) → NewEpoch/UploadOpinions/Empty(最后)
const (
    SSCRowCR    = 0
    SSCRowSim   = 1
    SSCRowOther = 2
)
```

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

> **R5/R10 语义**：`SSCInternalTx` 走标准 ApplyTransaction 机制（产生 receipt、累计 gas、更新 nonce、并入 state root），仅执行入口换成 `ProcessInternal`（详见 DSN-45 §4.4.1）。收据/gas/root 全部复用，不特判。

### 4.5 RLP / 区块 / 集成影响

| 文件 | 改动 |
|:-----|:-----|
| `core/types/` | `SSCInternalTx` 类型 + RLP 编解码 |
| `core/types/bodyv2.go` | `SSCTransactions []*SSCInternalTx` 字段 + getter/setter + RLP |
| `core/types/block.go` | `Block` 增加 `sscTransactions` 字段、`extblockV2` 序列化 |
| `core/vm/sscvm.go` | `ProcessInternal` 原生入口 |
| `node/worker/worker.go` | 出块从内部池取 `batches[0]` 写入 `SSCTransactions` |
| `core/state_processor.go` | validator 复算走同一内部交易顺序 |
| `ssc/tx_submitter.go` | 构造 `SSCInternalTx` 提交到内部池（DSN-46） |
| `ssc/` | protobuf 定义 `CXTSimulation` / `CXTCommitProof`（替换 JSON） |

**R8 影响面清单（全节点协调升级）**：改动区块格式会连锁影响——
1. block hash / state root（header）
2. `extblockV2` RLP 序列化/反序列化
3. P2P gossip / 区块广播
4. DB 存储 / 存档 / 快照同步
5. RPC 返回区块结构
6. 跨分片 header / 收据
7. `StateProcessor.Process` 复算路径

每项都要在实现时同步更新，否则分叉。

**R9 签名与编码解耦**：protobuf 只用于线上/区块载荷；**签名仍用独立规范字节**（沿用现有 `CXTSimulation.Bytes()` 语义），与线上编码解耦，避免 protobuf 迁移导致签名字节不稳定。

### 4.6 receipt / gas 语义

**已定（R5/R10）**：`SSCInternalTx` 作为一类交易走标准 ApplyTransaction 机制——产生 receipt、累计 gas、更新 nonce、并入 state root，与普通交易一致；仅执行入口换成 `ProcessInternal`。详见 DSN-45 §4.4.1。

## 5. 与 DSN-45 / DSN-46 的关系

| DSN | 职责 |
|---|---|
| DSN-47 | 内部交易结构 + 区块承载（一维）+ 内存分桶 + SSCVM 原生入口 —— **基础** |
| DSN-46 | 内部池（进池仲裁、分组、DAG 就绪）—— 消费 DSN-47 的结构 |
| DSN-45 | 拿到批次后并行 execVerify —— 消费 DSN-46 的批次 |

## 6. 验证计划

1. 默认关（`EnableInternalTx`/`EnableInternalPool`）→ 无回归。
2. 打开后 A/B：JSON 是否消除、CPU 是否从 ~4 核提升、submitToCommit 是否下降。
3. leader/validator 对同一区块 state root 一致、无分叉。

## 7. 开放问题

| 问题 | 说明 | 处理 |
|---|---|---|
| receipt / gas 语义 | 内部交易要不要 receipt | 已定：走标准 ApplyTransaction，产生 receipt（R5/R10） |
| 行内顺序 | Sim 行内顺序 | 已定：由内部池 `batches[0]`（DSN-46）决定 |
| protobuf 迁移范围 | JSON→protobuf 工作量 | 已定：只换线上/区块载荷，签名仍用独立规范字节（R9） |
| RLP 向后兼容 | 区块格式变更 | 全节点协调升级；按 R8 清单逐项同步 |

---

## 8. 实现落地与回归（2026-08-28）

> 本 DSN 已实现完毕。核心交付物与实测确认：

### 8.1 已落地（对照 §4.5 / §6）
- `core/types/ssc_internal_tx.go`（新）：`SSCInternalTx`（Type/Shard/Payload，RLP 外壳）+ `InternalTxType` 枚举 + `Hash()`/`Copy()`/`String()` + 二维视图 `SSCTransactions`（实现 `DerivableBase`，并入 TxHash）。
- 区块承载：`BodyV2.SSCTransactions [][]*SSCInternalTx`（2D，第一维=类型桶）；`BodyV1`/`BodyV0`、`extblockV1/extblockV2`、`Block.EncodeRLP/DecodeRLP` 同步补齐。
- `SSCVM.ProcessInternal`（`core/vm/sscvm.go`）：原生处理入口，SimTx/CRTx/NewEpoch/UploadOpinions/Empty 按 `Type` 分派。
- 出块/验证：`node/worker/worker.go` 从内部池提取写 `w.current.sscTxns`；`core/state_processor.go` `ApplySSCInternalTransaction`/`Process` 按 SSC→普通→staking 复算。
- 载荷 protobuf 化：`CXTSimulation`/`CXTCommitProof`/`NewEpoch`/`SelfOpinions` 走 protobuf（BRF-05），签名仍用独立规范字节（R9）。

### 8.2 回归修复（与本 DSN 相关）
- **v1 body 丢 SSC（BAD BLOCK）**：`BodyV1`/`extblockV1` 曾未带 SSC，导致 v1 header 区块 RLP 传输后 SSC 丢失 → TxHash mismatch。已修（BRF-06）。`core/block_validator.go` 的 `ValidateBody` 补 `types.SSCTransactions(block.SSCTransactions())`，与 `NewBlock` 三元组对齐。
- **跨分片 SimTx 泄漏**：`CommitSimulation` 曾用 origin 的 ShardId 建 SimTx，导致每节点把所有分片 SimTx 收进自己池并广播到所有分片 group → `InvalidSimulation` 回滚。已修：`ShardId=s.SelfShard` + 接收/提交侧 shard 过滤（详见 DSN-48 §8.2）。
- **出块 1s 预算**：SSC 内部交易纳入 worker 1s 时间预算（DSN-48 §8.3）。

### 8.3 遗留
- DSN-46（内部池进池仲裁/分组/DAG）仍未做；`CHAIN_RETRY_STATS` 仍 Debug 级（统计应 Info）。

---

## 9. 迁移遗留 Gap：`TempLockView.OnBlockCommitted` 未随内部交易迁移（2026-08-31 发现）

### 9.1 问题

`ssc/temp_lock_view.go` 的 `TempLockView.OnBlockCommitted(block)` 只扫描**普通交易** `block.Transactions()` 来释放 TLV 临时锁并产出 `releasedKeys`：

```go
blockTxHashes := make([]common.Hash, 0, len(block.Transactions()))
for _, tx := range block.Transactions() {
    blockTxHashes = append(blockTxHashes, tx.Hash())
}
```

在 DSN-31 / DSN-46 / DSN-47 / DSN-48 之后，跨分片交易的提交产物已经**全部迁移到 SSC 内部交易**（SimTx/CRTx，承载于 `block.SSCTransactions()`），普通 `block.Transactions()` 里不再有跨分片提交。但本函数没有同步迁移，导致：

- `releasedKeys` 恒为 0（所有 121 个区块实测均如此）；
- `retryScheduler.OnBlockCommitted` 的 `querySubscribers(releasedKeys)` 永远返回 0 → **增量晋升通路彻底停摆**；
- retryPool 涨到 ~4000 后冻结（实测 `retryPool=4156`、`globalFinished` 不再增长）；
- 大量依赖「链上 key 释放」唤醒的重试交易永久卡死（实测 3914 笔，采样 59/80 为等链上锁释放）。

### 9.2 根因

`OnBlockCommitted` 隐含的旧假设「**已上链交易 = `block.Transactions()`**」在内部交易迁移后失效。真正的上链提交在 `block.SSCTransactions()`（SimTx/CRTx）里，且：

- `txReadWriteSets`（TLV 锁登记表）**只可能被 `RetryCommit` 写入**（`TryLockWithPriority`），普通交易从不登记 → 扫 `block.Transactions()` 是无用功；
- 因此 `OnBlockCommitted` **只需处理内部交易**，普通交易那段可以移除。

### 9.3 锁生命周期（判定依据）

| 事件 | 动作 | 锁状态 |
|---|---|---|
| `RetryCommit` | `TryLockWithPriority` 登记 TLV 临时锁（key=原始 tx hash） | TLV 锁获得 |
| **SimTx 上链** | `VerifySimulation` → `SetAndLockState` | 链上锁（SLM）获得；TLV 临时锁作废 |
| **CRTx 上链** | `CommitOrRollbackWithProof` → `CommitTx/RollbackTx` | 链上锁释放 |

结论：
- **SimTx** 负责「清 TLV 临时锁」（修 TLV 读锁泄漏，次要）；
- **CRTx** 负责「上报被释放的链上 key → 唤醒等链上锁的重试交易」（主因，实测 59/80）。

两者不能混在一个 `releasedKeys` 里，也不能只做其一。

### 9.4 修改方案

1. `TempLockView.OnBlockCommitted` **去掉** `block.Transactions()` 扫描，改为遍历 `block.SSCTransactions()`；
2. **SimTx 桶**：反序列化 payload（`sscpb.CXTSimulationFromProto`）取 `simulation.TxHash`（原始跨分片交易 hash，即 `txReadWriteSets` 的 key）→ 清理该 tx 在 TLV 的读写锁（修泄漏）；
3. **CRTx 桶**：反序列化 payload（`sscpb.CXTCommitProofFromProto`）取 `proof.TxHash` → 从 `stateLockManager` 拿到该交易实际释放的链上 key → 并入 retry 唤醒的 `releasedKeys`（或走独立的「链上释放→唤醒」通道）；
4. 注意：SimTx/CRTx 的 `Hash()` 是内部交易自身的 hash，**不是**原始跨分片交易 hash，**不能**直接拿去查 `txReadWriteSets`，必须从 payload 解出原始 txHash。

### 9.5 影响面

- `ssc/temp_lock_view.go`：`OnBlockCommitted` 改造；
- `ssc/retry_scheduler.go`：`OnBlockCommitted` 消费 `releasedKeys` 的语义需与「SimTx 清 TLV / CRTx 唤醒」对齐；
- `ssc/state_lock_impl.go` / `ssc/state_locker.go`：确认 CRTx `CommitTx/RollbackTx` 能精确给出该交易释放的 key 集合；
- 需要新增/复用 protobuf 解码（`CXTSimulationFromProto` / `CXTCommitProofFromProto`）。

### 9.6 同类需迁移清单（原本扫普通交易 → 需改为处理内部交易）

系统性排查 `block.Transactions()` 用法后发现，除 `TempLockView.OnBlockCommitted` 外还有同源遗留。以下清单用于后续逐项整改：

| # | 位置 | 现状（旧模型） | 影响 | 优先级 |
|---|---|---|---|---|
| 1 | `ssc/temp_lock_view.go` `OnBlockCommitted` | 只扫 `block.Transactions()` 释放 TLV 锁 | retry 晋升通路停摆（本 DSN §9 主因） | **P0** |
| 2 | `ssc/committee.go` `StateDataParser.ParseBlock` | 遍历 `block.Transactions()`，找 `To=SimulationCommitAddr` + `json.Unmarshal(tx.Data())` | SimTx 迁移后解析不到 → `MemberTxCounts/MemberSignedCounts/TxNum` 恒 0，**信誉/参与统计失效** | P1 |
| 3 | `core/rawdb/accessors_indexes.go` `WriteBlockTxLookUpEntries` / `WriteTxLookupEntriesByBlock` | 只给 `block.Transactions()` 建 hash 索引 | 若 SimTx/CRTx 需要按 hash 可查（RPC/调试）则缺失；否则可忽略 | P2 |

**已确认无需改动（已正确迁移）**：
- `core/state_processor.go`：已按 SSC→普通→staking 处理 `block.SSCTransactions()` ✅
- `core/block_validator.go`：`ValidateBody` 已补 `types.SSCTransactions(block.SSCTransactions())` ✅
- `node/worker/worker.go`：出块已用 `sscTxns` / `ExtractSSCTransactions` ✅
- `ssc/internal_pool.go`：清理已用 `block.SSCTransactions()` ✅

> 整改 2、3 时同样注意：内部交易自身的 `Hash()` ≠ 原始跨分片交易 hash，需从 payload 解出原始 txHash（SimTx→`CXTSimulation.TxHash`，CRTx→`CXTCommitProof.TxHash`）。
