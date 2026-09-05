# RSH-03: Priority-Aware Reverse Chandy-Misra-Haas for Cross-Shard Deadlock Detection

> **版本**：v3（2026-09-02，与 DSN-53 定稿对齐：锁语义、触发主次、BLS 可信证明、消息结构、探针路由/重验证/生命周期）
>
> ⚠️ **本文锁语义/触发部分已由 DSN-53 v2 修正**
> ⚠️ **Wait-Die 弃用（v3）**：DSN-52 的"低优先链上主动 die"已废弃；唯一 die 机制是 CMH 环 victim 终局回滚。
>：链上锁 = global ∪ pending（等价，同时查两张 map）；
> TLV 只是链下预约/调度（无自身 finalized、非持久锁）；CMH 持久 waitEdge 只来自链上锁(global+pending)、方向不限、
> 记边与探测解耦、转发沿完整 WFG 无剪枝。与本文冲突处，以 `docs/designs/active/DSN-53-crossshard-deadlock-detection.md`(v2) 为准。
> **范围**：论文向研究文档 —— 跨分片区块链交易死锁检测
> **对应**：DSN-53（代码落地向，已含 §10 实现里程碑）

---

## 1. Problem Statement

**背景**：跨分片交易（cross-shard transaction）需要所有相关分片（related shards）确认才能提交，
但锁是**逐个分片**获取的。这产生「部分赢家」（partial winner）：一笔交易在一个分片上锁成功、
在另一分片被阻塞。多个部分赢家可形成**跨分片死锁环（ring）**：

```
Tx1 锁 shard0，卡在 shard1（被 Tx2）
Tx2 锁 shard1，卡在 shard0（被 Tx1）
→ 互相等待，任何一个都无法凑齐所有分片确认
```

**难点**：结合 wait-die 后，低优先级者理论上应「重新验证时 die」，但全分片 Ready 聚合门
（`readyCnt == len(RelatedShards)`）把「重新验证」卡死——低优先者被更高优先挡在某一分片，
该分片不发 Ready → 永不重新验证 → 永不 die → 环无法自行打破。

**目标**：在**链下**、**无超时**、**有界回滚**的前提下检测并打破跨分片死锁环。

---

## 2. Related Work

### 2.1 Chandy-Misra-Haas (CMH)
- 经典分布式死锁检测：进程维护局部等待图（WFG），通过**探针** `(i,j,k)` 沿等待边传播，
  探针回到发起者 `i` 即检测到环，选 victim 中止。
- 假设 **AND 等待模型**（须同时持有所有等待资源），与我们跨分片「须所有分片确认」一致。
- 原版触发：**等待者（低优先/被卡者）** 发现自己在等待时发起。

### 2.2 Wound-Wait / Wait-Die
- 基于全局优先级全序（`Nonce > OriginShardID > TxHash`）打破环。
- **Wound-Wait**：高优先可 wound（抢占）低优先。**Wait-Die**：低优先请求者 die，高优先 wait。
- 我们采用 **Wait-Die**（避免 wound 破坏投票协议）。

### 2.3 我们的定位
结合两者，提出**优先级感知反向 CMH**：由**高优先被低优先卡住**的交易发起探针（反向于原版 CMH），
并用全局全序做传播剪枝。既解决 CMH「探针多」和「与 wait-die 不耦合」的缺点，又补足
wait-die「Ready 门卡死低优先者 die」的盲区。

---

## 3. Proposed Method: Priority-Aware Reverse CMH

### 3.1 核心思想
- **等待边（反向）**：`A ⇢ B` = 高优先 A 在分片 X 被低优先 B 的锁阻塞。
- **触发（高优先发起）**：仅当「高优先 A 被**更低优先 B 的链上/已 Finalized（不可 wound）锁**卡住」时发起探针。
  - **主力在链下**：严格门（`RetryCommit` Phase 2 `CheckLock`）在构建 SimTx **之前**就识别该边；
  - **链上为兜底安全网**：仅 leader 轮换导致 TLV 未正常协调 / TOCTOU 等罕见场景才在 `VerifySimulation` 触发；
  - **两处都要判断**是否触发 CMH。
- **传播（优先级定向剪枝）**：探针沿 `高优先被低优先` 的链走，多数冲突提前停。
- **victim（环内最低优先）**：探针带路径回到发起者 → 选环内全局全序最低者 die。

### 3.2 算法（伪码）

```
// 每分片维护局部等待边表 waitEdges
// 只有满足 shouldTriggerCMH 的边才记录（否则正常 wait-die，不触发）

shouldTriggerCMH(A, key, B):
  if key 被 DAG patch 覆盖: return false          // 依赖边，不算阻塞
  if !(pri(B) < pri(A)):   return false           // A 不是更高优先 → 低优先 wait，不触发
  if B 的锁不是链上/已 Finalized: return false    // 可 wound 的 TLV 边 → 会被 wound，不构成持久边
  return true                                     // A 高优先 且 被低优先链上锁卡住 → 触发

// 触发点：
//   链下（主力）：RetryCommit Phase1(持有者 Finalized) / Phase2(OnChainLockConflict)
//   链上（兜底）：VerifySimulation 冲突且 dsn52ShouldYield==false

probe(i, j, k, path):
  if k == i:                       // 回到发起者 → 成环
      victim = min_priority(path)  // 环内全局全序最低者
      abort(victim)                // 终局回滚，释放 victim 全部锁
      return
  for m in waitEdges[k]:           // k 在分片 X 等 m（已满足 shouldTriggerCMH）
      if priority(m) < priority(k):   // 剪枝：仅沿「高优先被低优先」链
          probe(i, k, m, path + [m])
      // 否则丢弃（不构成高优先被低优先链）
```

### 3.3 与 wait-die 的分层
- 高优先被低优先卡 → 探针检测环（本方法）；不成环则高优先 wait。
- 低优先被高优先卡 → 低优先正常 wait；若其能重新验证（未被 Ready 门卡死）→ die（wait-die 已有路径）。
- 检测到环 → 只 die 环内最低优先者，其余继续。

### 3.4 锁语义：链上 / 链下 / 补丁的「被锁住」精确定义

跨分片系统有两套锁 + 一套补丁，等待边必须绑定到明确层，否则探针会沿错误边传播：

| 层 | 载体 | 是否持久 | 「被锁住」判定 | 是否构成死锁环 |
|---|---|---|---|---|
| **链上锁（SLM）** | `stateLockManager`（`globalLockedStates`/`globalRLockedStates`，随链） | 是 | `stateDB.CheckLock(key)` 返回 `ErrLockConflict_OnChain`（覆盖跨块 + 同块 pending） | **是（主目标）** |
| **链下锁（TLV）** | `TempLockView`（leader 本地，不进链） | 否（可 wound/释放） | `TryLockWithPriority` 返回 `locked=false`（`wounded` 恒为 false，需再查持有者区分两种情况） | 仅当持有者已 Finalized |
| **补丁（DAG patch）** | `offChainDAG`（上游 SimTx WriteSet 覆盖） | 依赖边 | 被 `findCoveringSet`+`isFullyCovered` 覆盖 → **不算阻塞** | 否 |

**TLV `locked=false` 的两种子情况（必须区分）**：
1. **持有者 B 优先级更高**（或未知）→ A 是低优先，正常 wait-die，**不触发 CMH**；
2. **持有者 B 优先级更低 且 B 的 Patch 已 Finalized**（链上/将上链，无法 wound）→ **A 高优先被低优先链上锁卡住 → 触发 CMH**（主力路径之一）。

**触发主次**：
- **主力 = 链下 `RetryCommit`**：Phase 1（TLV 持有者已 Finalized）+ Phase 2（`OnChainLockConflict`，
  A 高优先被低优先 B 的链上锁卡住且 DAG patch 补救失败）——正常流程严格门在此处拦住冲突，**这是解决死锁的主力**。
- **兜底 = 链上 `VerifySimulation`**：仅 leader 轮换导致 TLV 未协调 / TOCTOU 等罕见场景才走到，
  作为安全网。
- **两处都要判断** `shouldTriggerCMH`（只有「A 高优先 且 被低优先链上/已 Finalized 锁阻塞」才触发）。

**结论**：waitEdge 只记录**未被 DAG patch 覆盖**的、真正阻塞的等待边；主目标是链上 SLM 边
（只有它随链持久、构成打不破的赢家环），TLV 边仅当其持有者已 Finalized 才构成持久边。

### 3.5 通信方式与安全（阈值 BLS 可信证明）

**通信拓扑**：每个分片有独立 SSC 委员会（leader + members）。反向 CMH 探针是**跨分片消息**，
走各分片 leader 间的 gRPC（`SSCCrossService`），逐跳沿「高优先被低优先」链转发，回到发起者即成环。

**安全模型**（论文需陈述）：victim die 是终局、不可逆的 Rollback CRTx。为避免单个（恶意/故障）
leader 伪造 waitEdge 诱导误杀，每个转发 hop 的探针必须携带**发送分片 SSC 委员会 ≥threshold 成员的
聚合 BLS 签名**（`BaseBLSSignedMessage`：`ShardId + Signatures + BLSBitMap + Epochs`）。接收方用该分片
公钥组 `Verify`，不通过即丢弃。这提供：

1. **来源/完整性**：只有多数诚实的委员会共同背书 waitEdge/路径才生效；
2. **防伪造/防篡改**：单节点签名或篡改 Path 均无法通过阈值验证；
3. **防重放/跨 epoch**：消息带 `Epoch + Nonce + BlockNum`，跨纪元或重复 nonce 失效；
4. **与投票协议一致（防御纵深）**：探针只选 victim，真正的 die 仍走 `sendRollbackVoteForDie`
   → 正常投票聚合 → 非 ReleaseOnly 终局 Rollback CRTx。即使探针被注入，回滚仍需满足既有投票协议。

> 信任假设：每分片 SSC 委员会 ≥threshold 诚实（与共识一致）；链上锁状态以链为真，BLS 只用于链下探针。

### 3.6 CMH 消息结构

仅需一个新消息 **`DeadlockProbe`**（逐跳转发 + 阈值 BLS），外加可选 **`DeadlockProbeAck`** 回执；
victim die 复用现有 `CXTCommitProof`（Rollback, 非 ReleaseOnly），**无新增消息**。

```
DeadlockProbe {
  init, sender, current : 交易哈希   // CMH (i,j,k)
  path[]                : 已访问路径  // N-环选 victim（2-环可省）
  layer                 : onchain | tlv  // 上一跳等待边来自哪层（§3.4）
  wait_key              : 冲突 key    // 精确剪枝 / 去重
  epoch, block_num, nonce: 防重放/防 stale/去重
  shard                 : 当前分片
  base(BLS)             : 发送分片 SSC ≥threshold 聚合签名（可信证明）
}
```

结构评估：原 CMH `(i,j,k)` 只够 2-环成环判定；为支持 **N-环 + victim 选择 + 可信性 + 防重放**，
必须扩展为上述字段。逐跳 `Bytes()` 序列化除 BLS 外的全部字段后签名，保证签名绑定完整内容。

---

## 4. Differences from Original CMH

| 维度 | 原版 CMH | 我们的变种 |
|---|---|---|
| 触发者 | 等待者（低优先/被卡者）发起 | **高优先被低优先卡者**发起（反向） |
| 等待边方向 | `A 等 B`（低优先→高优先） | `A 被 B 卡`（高优先→低优先），反向 |
| 传播是否看优先级 | 不看，沿所有等待边 | **用全局全序剪枝定向**，多数提前停 |
| 触发频率/开销 | 每个等待边都可能触发 → 探针多 | 仅高优先被卡触发 → 探针少 |
| 与事务调度 | 独立 | 与 wait-die 分层绑定（低优先 wait 不触发） |
| 完备性 | 任意环（通用） | **场景性**：高优先被低优先构成的环；纯低优先环靠 wait-die die 解决 |

---

## 5. Correctness / Complexity

### 5.1 正确性论证（sketch）
- **无超时**：触发条件是「高优先被低优先卡」这一确定性事实（全局全序下所有节点一致），非时间。
- **环检测**：探针沿「被卡」链传播，回到发起者 ⇔ 存在由这些边构成的环。
- **终止性**：探针只沿优先级严格下降（低优先）的边走，且发起者唯一 → 有界、无无限传播。
- **与 wait-die 不冲突**：victim 是环内最低优先，die 是 wait-die 允许的（低优先 die）。

### 5.2 复杂度
- **检测**：本地冲突信息（免费）+ 探针 RPC。
- **消息**：O(被阻塞的 (A,B) 对 × 分片数)，仅检测环时发送，可批量。
- **可信背书**：每 hop 需 O(committee) 次签名收集 + 1 次聚合（发送方），接收方 1 次阈值验证；
  可对同 (waiter,holder) 缓存签名、或批量探针共用一次聚合摊销。
- **回滚**：每 victim 一次 Rollback CRTx（与普通失败相同），无额外 O(分片数)。
- **victim 选择**：环内线性扫描（O(环长)），可带路径或只报告存在环。

---

## 6. Evaluation Plan（论文实验）

1. **收敛性**：rate=200 下 `unfinished→0`、`globalLocked` 归零。
2. **回滚开销**：对比「宽松 wait-die（大量 die）」与本方法，验证 rollback 显著下降且只 die 环内成员。
3. **探针开销**：`probe sent/dropped/ring detected` 计数，验证剪枝带来的消息减少。
4. **环覆盖**：构造 2-环/3-环/N-环单元测试，验证检测正确、victim 为环内最低优先。
5. **安全/可信**：伪造探针（错 BLS / 单节点签名 / 篡改 Path / 重放）必须被 `Verify` 拒绝；合法委员会签名通过。
6. **无超时**：证明无超时参数依赖，性能不随超时上限退化。

---

## 7. Contribution（拟论文贡献点）

1. **首次将 CMH 死锁检测适配到跨分片区块链交易场景**，并结合 wait-die 分层。
2. **提出反向触发（高优先被低优先卡）**，比原版 CMH 探针更少、更贴合 wait-die 语义。
3. **利用区块链已有的全局优先级全序做传播剪枝**，无超时、有界回滚。
4. **以分片 SSC 阈值 BLS 作为跨分片探针的可信证明**：终局回滚只由委员会背书的探针触发，
   与投票协议形成防御纵深——这是本方法在区块链场景下相对通用 CMH 的关键安全性增量。
5. **与「宽松 wait-die」对比**，展示本方法在保证收敛的同时显著降低回滚率。

---

## 8. Open Questions / Risks

- **场景性完备**：非「高优先被低优先」构成的环（如纯低优先环）是否会被漏检？
  论证：纯低优先环中最低优先者终会被更高优先者挡住而重新验证 die → 由 wait-die 兜底。
- **优先级不可比边**：若存在非全序边，剪枝可能断；需声明全局全序假设。
- **探针跨分片延迟**：对收敛时延的影响；是否需批量/合并。
- **BLS 背书延迟**：每 hop 聚合签名增加探针收敛时延；量化其影响并评估缓存/批量优化。
- **TLV 边噪声**：链下 TLV 是瞬态且可 wound，误记录会放大探针；默认仅记录链上边，TLV 边作为可选开关。
- **与链下 wound 的关系**：链下 TLV wound 是否能先化解部分冲突，减少需探针的环。
