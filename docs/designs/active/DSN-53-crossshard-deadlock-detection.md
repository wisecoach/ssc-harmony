---
id: DSN-53
title: 跨分片死锁检测：优先级感知反向 CMH 探针
type: DSN
status: active
priority: P0
author: Designer
created: 2026-09-02
updated: 2026-09-02
scope: [ssc/deadlock_detector.go, ssc/verify.go, ssc/retry_scheduler.go, ssc/state_lock_impl.go, ssc/comm.go, ssc/signer.go, ssc/api, ssc/api/proto]
refs: [DSN-52, RSH-03]
---

# DSN-53：跨分片死锁检测 —— 优先级感知反向 CMH 探针

> 一份**独立、自洽**的设计方案。与论文向文档 RSH-03 对应，本文为代码落地向。
> 涉及的分层收敛基底见 DSN-52；本方案在其之上加入"死锁环检测 + 定向 victim"层。
>
> **v3 声明（Wait-Die 完全弃用）**：链上锁冲突**不再按优先级主动 die**。DSN-52 的 Wait-Die die 侧已废弃，
> **唯一的 die 机制是 CMH**：检测到环后只对环内最低优先 victim 终局回滚。非环链上冲突一律记 waitEdge 等 CMH 判环。

---

## 1. 摘要

本方案解决跨分片交易的**赢家环死锁**：一笔跨分片交易需在全部 related 分片持锁并确认才能提交，
但锁是**逐个分片**获取的，因此可能成为"部分赢家"——在 A 分片已持真实锁、在 B 分片被另一交易卡住。
多个部分赢家可互等成环；叠加"全分片 Ready 聚合门"后，环内**低优先者永不重新验证、永不 die**，
环无法自行打破。

方案用**优先级感知的反向 CMH（Chandy-Misra-Haas）探针**：由被卡的**高优先**交易沿真实等待图发起探针，
探针跨分片逐跳传播、带回完整环路径，回到发起者即判环，然后**只对环内全局全序最低优先者**做终局回滚（die）。
它是有界的（探针沿边传播、带去重）、无超时的、且只 die 环内最低者（不引入大量随机回滚）。

**核心立场（与锁模型一致）**：只有"真实链上锁"才能构成死锁等待边；链下 TLV 预约与 DAG 补丁只是
帮助交易构建 SimTx 的调度/编排层，**不是** CMH 的持锁来源（详见 §5）。

---

## 2. 背景：锁、提交与死锁成因

### 2.1 一次跨分片提交如何发生

跨分片交易（cross-shard transaction，CXT）需要**所有 related 分片**确认才能 commit：

1. **模拟/重试（链下）**：`RetryScheduler` 用 `TempLockView`(TLV) 在 leader 本地预约交易读写的 key，
   决定 retry 交易谁先走、谁被高优先 wound 掉；并用 `offChainDAG`(patch) 让依赖上游的交易能
   **提前构建 SimTx**（对着上游 patch 值读，不必等上游 commit）。
2. **验证与上锁（链上）**：`VerifySimulation` 通过后 `lockStateWithRWSet` 把交易写进 stateDB 的
   **pendingStates（同块未提交锁）**，区块提交时 merge 进 SLM 的 **globalLockedStates**。
3. **聚合确认**：各 related 分片都对同一笔交易发 commit vote，origin 聚合到阈值后广播
   `CommitOrRollbackWithProof`（CRTx）；**锁只能由 CRTx 释放**（commit→解锁，或 rollback→释放+终局）。
4. **Ready 聚合门**：交易能否"重新验证/重新走流程"由 `readyCnt == len(RelatedShards)` 控制。

### 2.2 死锁成因（部分赢家 + Ready 门）

```
跨分片交易需所有 related 分片确认 → 逐个上锁 → 出现部分赢家：
   B 在 shard0 被更高优先 A 卡住（持锁不成），却在 shard1 已持真实锁挡住 A
Wait-Die：A 高优先 → 只 wait（不 die）；B 低优先 → 本该 die，但要靠"重新验证"才会 die
但 Ready 聚合门要求 readyCnt==len(RelatedShards)：B 被 A 挡在 shard0，
   shard0 不发 Ready → B 永不重新验证 → 永不 die → 锁永在
⇒ 互等成环，没有任何一方能自行打破
```

需要强调：**死锁边是"真实链上锁"之间的互等**；低优先 B 卡住的本质是它**已经或将要持有真实链上锁**，
而不是它还在 TLV 预约阶段（详见 §5）。

---

## 3. 设计原则

1. **只检测真实死锁，不做暴力破解**：不用超时 kill、不做全量随机 rollback；只沿真实等待边找环，
   只 die 环内最低优先者。
2. **判定以链上锁为真**：waitEdge 只建立在链上锁（global ∪ pending）上，可被探针传播的边是"打不破的"。
3. **等待图完整**：低被高卡的回程边也记录，使环能闭合、victim 能选准；优先级方向只决定"谁发起探测"。
4. **可信传播**：每个转发 hop 都由发送分片委员会阈值 BLS 背书，防单点伪造；die 仍走正常投票协议。
5. **有界、无超时**：探针沿边传播、`(init,nonce)` 去重，探针数量有界；不引入新超时。

---

## 4. 目标与非目标

**目标**
- 检测并打破跨分片赢家环（2-环/3-环/N-环），只回滚环内全局最低者。
- 在有界、无超时、委员会背书下完成；与 Wait-Die / TLV wound / DAG 分层收敛共存。

**非目标 / 明确边界**
- 不做"TLV 预约饥饿"的判定：若死锁实际发生在"低优先交易永远没有机会构建 SimTx"（调度层），
  不属于链上死锁环，需另行解决（见 §5.4、§10 风险）。
- 不解决非单调优先级（不可比边）——假设优先级为确定性全局全序 `Nonce > OriginShardID > TxHash`。

---

## 5. 锁模型（本方案的地基）

### 5.1 只有两类对象与锁打交道：真实锁（链上）与预约/编排

| | 链上锁 | 链下预约/编排 |
|---|---|---|
| 代表 | **SLM globalLockedStates（跨块）** + **stateDB pendingStates（同块未提交）** | **TLV tempLockView** + **offChainDAG/patchpool** |
| 本质 | 交易"真正拿到、只能 CRTx 释放"的锁 | leader 本地调度用的预约 / 依赖 patch |
| 对后续交易 | **链上事实** | 非持锁事实 |
| 高优先能否抢 | **不能**（锁只能 CRTx 释放） | 未 finalize 可 wound |
| 能否成为 CMH waitEdge | ✅ | ❌ |

### 5.2 链上锁 = global ∪ pending（两者等价）

- **globalLockedStates（SLM）**：跨块、随链持久；`GetLockHolderMeta`(写) / `GetRLockHolder`(读) 查。
- **pendingStates（stateDB）**：同块未提交；`FindPendingLockHolder` 查。
- **对后续交易两者等价**：都是"这个 key 已被某交易链上锁定"。pending 只是"还没 commit merge 进 global"，
  但**已经挡住后续交易**。所以找链上持有者 = **同时查这两张 map**。
- 日志注意：pending 冲突的错误串被写成 `[TempLockView] locked by tx ...`（历史命名误导），
  它**不是** TLV 预约，而是 stateDB 的 pending 真实锁。

### 5.3 TLV(tempLockView)：链下预约/调度，不是持久锁

- 结构：`tempWriteLocks/tempReadLocks`，条目 `{Holder, Priority}`——**没有 finalized 字段**。
- 作用：RetryCommit Phase1 用它预约 retry 交易的读写 key，做 wound 调度（高优先抢低优先未 finalize 的预约）。
- 生命周期：OnBlockCommitted / GarbageCollect / wound 即释放。
- **TLV 不构成 CMH waitEdge**：它可被 wound、瞬态；"finalized-TLV" 一词不成立。
  只有"holder 已定稿要上链（DAG finalize）"的过渡才可能不可 wound，而那时 holder 即将落到
  global/pending——CMH 在链上锁里找它即可。

### 5.4 offChainDAG / patch：帮助构建 SimTx，不是持锁层

- 节点 = SimTx（WriteSet + UpstreamTxList），`finalizePatch` 置 `PatchFinalized`（不可 wound 标记）。
- 作用：让依赖上游的交易对着上游 patch 值**提前构建 SimTx**（不必直接持冲突 key）；表达上游依赖顺序。
- **DAG 不登记"当前谁占 key"**，所以不能作为 waitEdge 的 holder 来源。
- **"finalized"是 DAG 节点状态，不是 TLV 属性**，作用仅是 wound 闸门（holder 被 finalize → 不可 wound）。
- DAG patch 覆盖某 key 属于执行语义（让交易能构建 SimTx），**不是** CMH"不算阻塞"的判定依据（见 §7 讨论）。

### 5.5 "被卡住"（blocked）的精确定义

> **A 在分片 S 被真实链上锁卡住** ⇔ A 尝试获取 key K 的锁失败，且 K 的链上持有者 B 位于
> `globalLockedStates` 或 `pendingStates`（`B ≠ A`）。

- **不构成 waitEdge**：A 卡在 TLV 预约（可被 wound / 只是调度等待）；A 卡在被 DAG patch 覆盖的 key（那是执行语义）。
- 一次 waitEdge 记录必须落到一个**真实的、不可 wound 的链上持有者 B**；找不到这样的 B，就不记。

---

## 6. 核心数据结构

### 6.1 waitEdge（等待边）

```
waitEdge {
    waiter     common.Hash    // 被卡的交易 A
    holder     common.Hash    // 真实链上持有者 B（global/pending）
    shard      uint32         // 发生阻塞的分片
    key        LockKey        // 冲突 key
    layer      LockLayer      // 一律 OnChain（waitEdge 只来自链上锁）
    waiterPri  Priority       // A 优先级
    holderPri  Priority       // B 优先级（用于探测门与 victim 选择）
}
```

- 存储：`deadlockDetector.waitEdges`，按 `waiter` 索引的 `[]waitEdge`，`sync.Map` + 去重。
- **记边方向不限**：A 高被 B 低、或 A 低被 B 高，都记（补全 WFG）。
- `layer` 恒为 OnChain；TLV/DAG 不进入 waitEdge。

### 6.2 DeadlockProbe（探针）

```
DeadlockProbe {
    Init      Hash        // 发起者（回到 Init 即成环）
    Sender    Hash        // 上一跳节点
    Current   Hash        // 当前节点（等待者，正在查它的出边）
    Path      []Hash      // 环内已经过的节点（init + 每个 holder），用于选 victim
    Layer     LockLayer   // 上一跳边的层（OnChain）
    WaitKey   LockKey     // 上一跳冲突 key
    Epoch     uint64      // 防跨 epoch 重放
    BlockNum  uint64
    Nonce     uint64      // 发起者唯一编号，去重/防重放
    Shard     uint32      // 本跳发送分片
    // + BaseBLSSignedMessage（阈值 BLS 背书）
}
```

### 6.3 其它状态

- `seenProbes`：`(Init, Nonce) → 存在`，用于转发去重。
- 原子计数：`probeSent / probeDropped / probeInvalid / ringDetected / victimDied`，供监控。
- 干净封装：waitEdges 只服务探针，不参与上锁/调度，避免反向影响 SLM/TLV/DAG（无循环依赖）。

---

## 7. 记边 vs 探测（两层判定）

**两层解耦是核心**：记边 ≠ 探测。

| 动作 | 触发条件 | 目的 |
|---|---|---|
| **记边** | A 在 S 被真实链上锁 B 卡住（global/pending，方向不限） | 补全完整 WFG，使探针能回程成环 |
| **探测** | **高优先 A 被更低优先 B** 卡（`A.pri < B.pri`）且 B 不可 wound（链上） | 沿图找环、选 victim；低被高不探测 |

- **低被高回程边**（如 2-环里的 `B→A`）必须记，但**不发起探测**——否则会把探针导向不该 die 的高优先 A。
- 探测门 `shouldTriggerProbe`：`A.pri < B.pri` 且 B 优先级已知、B 不可 wound → true。
- 记边门只要求"找到真实链上持有者 B（global/pending）"。

**holder 统一查询（三处共用：verify 兜底 / retry Phase2 / edgeValid）**
```
findChainHolder(key) =
    global 写  GetLockHolderMeta(key)
    → global 读 GetRLockHolder(key)
    → pending   stateDB.FindPendingLockHolder(key)
（不含 TLV、不含 DAG）
```
若找不到真实链上持有者 → 说明冲突来自可 wound 的 TLV 预约/调度 → **不记边**。

> **TLV 触发 & 临时边（v4，见 附录 D）**：链下 `TryLockWithPriority` cannot-wound 时也触发 CMH。
> 能定位到真实链上 holder → 记【持久边】；定位不到（B 只是不可 wound 的 TLV 预约）→ 记【临时边】
> （不进持久 waitEdges，仅供探针发起/遍历判环，阻塞解除即清）。这样不丢"链下先被挡、还没上链"的卡死，
> 也不把 TLV 预约刷成持久假边。

---

## 8. 探针协议（传播、成环、选 victim）

### 8.1 发起

高优先 A 被低优先 B 的链上锁卡住（过 shouldTriggerProbe）→ 本分片 leader 记下 `A→B`，
并向本分片委员会收集对 probe 的阈值 BLS 签名，然后把探针发往 B 的 related 分片（找 B 的出边）。
`Path = [A, B]`（init + 首个 holder，保证环成员完整）。

### 8.2 收到探针（HandleProbe）的判定顺序

```
0) BLS 验签：签名/分片/epoch 不合法 → 丢弃
1) 【成环】若 Current == Init → 已回到发起者 → 判环 → 选 victim → die
2) 去重：seenProbes[(Init,Nonce)] 已存在 → 丢弃（只用于转发去重，绝不能挡成环）
3) 重验证上一跳：若 Sender != Init（非首跳）才用当前链上锁事实重验
   （首跳刚记下，跳过防"新建边立刻 stale 自掐"）
4) 查 Current 的 waitEdges，沿【完整 WFG】转发下一跳（无优先级剪枝）
5) 无出边 → 丢弃
```

关键点：
- **成环判定在去重之前**：否则发起者自处理初始探针会把 `(init,nonce)` 记入 seen，
  探针真正绕回时被去重丢弃，永远判不出环。
- **转发不做优先级剪枝**：低被高的回程边也必须走，否则 2-环闭合不了。"高被低"只决定发起，不决定转发。
- **首跳（Sender==Init）不重验证**：A→B 刚记下时，B 可能在 pending（未 merge 进 global），
  立即用仅 global 的重验会误判 stale。

### 8.3 路由

- 探针发往 `holder` 的 related 分片（由 `GetTxMeta(holder).RelatedShards` 得到），
  只有"存有 holder 出边 waitEdge"的那个分片会继续，其余丢弃。
- 实现用多播 + 各分片 leader 上的 `HandleProbe` 判定；可后续优化为 holder→blocking-shard 反查索引。

### 8.4 victim 选择与终局 die

- 环成员 = `unique(Path) ∪ {Init}`。
- victim = 环内**全局全序最低优先**者（`Nonce > OriginShardID > TxHash` 升序即高优先，最小者优先最高，
  最低优先 = 数值最大者）。
- 触发 victim 的 origin 广播**终局 Rollback CRTx**（非 ReleaseOnly）到 victim.RelatedShards；
  各分片 CommitOrRollbackWithProof → RollbackTx + CloseTx → 释放 victim 全部真实锁。
- die 走**正常投票/证明协议**——探针只负责"选谁"，不直接杀死（防御纵深）。

### 8.5 清理（waitEdge 生命周期）

- 建立：过 §7 记边门时。
- 清理：holder 的 CRTx（commit 或 die）上链时，各分片 `CleanupHolder(holder)` 删除 holder 相关所有边。
- 重验证：转发跳用当前 global+pending 事实校验边仍成立，stale 即丢。

---

## 9. 与其他模块的关系

```
                         ┌────────────────────────────────────────────┐
                         │  RetryScheduler  (链下主路径)              │
                         │   - RetryCommit Phase1: TLV 预约(触发观测)   │
                         │   - RetryCommit Phase2: 链上锁冲突 → 记边     │
                         └──────────────┬─────────────────────────────┘
                                        │ 触发记边(找到真链上持有者)
                                        ▼
   ┌──────────────── deadlockDetector（本方案核心）────────────────┐
   │  waitEdges / seenProbes / HandleProbe / CleanupHolder / Stats │
   │  记边(方向不限) + shouldTriggerProbe(仅高被低) + 探针传播/成环  │
   └──┬───────────┬──────────────┬─────────────┬──────────────────┘
      │           │              │             │
      ▼           ▼              ▼             ▼
 GetLockHolderMeta  FindPending    signerMgr      comm / state(GetLeader,
 GetRLockHolder     LockHolder     (BLS 背书)     GetCommittee,RelatedShards)
 (SLM global)      (stateDB pending)
      │                              │              
      ▼                              ▼
   VerifySimulation(链上兜底触发)   tempLockView / offChainDAG
   （只作"触发/观测"，不作为 waitEdge 来源）
```

模块职责边界：
- **stateLockManager（global）**：提供链上写/读持有者与优先级（`GetLockHolderMeta/GetRLockHolder/GetTxPriority/GetTxMeta`）——CMH holder 来源①。
- **corestate.DB（pending）**：`FindPendingLockHolder`——CMH holder 来源②（同块真实锁）。
- **TempLockView / offChainDAG**：只用于"触发观测/构建 SimTx/调度"，**不**向 waitEdge 提供 holder。
- **RetryScheduler**：持有 detector；Phase2 触发记边；OnBlockCommitted 见 CRTx 调 CleanupHolder。
- **VerifySimulation**：链上兜底触发记边（leader）。
- **signerMgr / comm / state(accessor)**：探针 BLS 背书、跨分片路由、leader/committee 查找。
- **committer**：victim 终局 Rollback CRTx（复用，无需新消息）。

---

## 10. 触发点与伪码

### 10.1 统一 holder 查询 helper

```
findChainHolder(key):
  # global 写
  if meta := GetLockHolderMeta(key); meta != nil && meta.TxHash != A: return meta.TxHash
  # global 读
  if h := GetRLockHolder(key); h != A: return h
  # pending(同块)
  if h := stateDB.FindPendingLockHolder(key); h != A: return h
  return nil
```

### 10.2 RetryCommit Phase2（链下主力）

```
Phase2 CheckLock 失败得到 conflictKeys（全部来自真实链上锁）：
  for key in conflictKeys:
    B := findChainHolder(key)                 # global∪pending
    if B == nil: continue                     # 查不到真实 holder → 不记
    detector.OnBlocked(A, B, key, OnChain, priA, priB)   # 内部: 记边(必) + shouldTriggerProbe→探测(若高被低)
```

### 10.3 VerifySimulation 冲突分支（链上兜底，leader）

```
冲突分支 dsn52ShouldYield==false（A 高优先 wait）：
  for key in conflictLockKeys:
    B := findChainHolder(key)                 # global∪pending（不再查 TLV）
    if B == nil: continue
    detector.OnBlocked(A, B, key, OnChain, priA, priB)
```

### 10.4 deadlockDetector.OnBlocked（记边 + 可选探测）

```
OnBlocked(A, B, shard, key, layer, priA, priB):
  if A==0 || B==0 || A==B: return
  edge := {A,B,shard,key,OnChain,priA,priB}
  if !addWaitEdge(edge): return               # 去重
  if shouldTriggerProbe(edge):                 # 仅高被低 且 B 已知 且 不可 wound
     triggerProbe(init=A, holder=B)           # BLS + 路由 + Path=[A,B]
```

---

## 11. 安全与可信模型

1. **阈值 BLS 背书**：每个转发 hop 带发送分片 ≥threshold 成员的聚合签名；接收方 `Verify` 不过则丢弃。
   保证单点/故障 leader 无法伪造 waitEdge 或探针路径诱导误杀。
2. **防重放/跨 epoch**：`Epoch + Nonce + BlockNum` 校验；`seenProbes` 去重。
3. **victim die 走投票**：探针只选 victim，最终回滚仍要满足 CXT 投票协议——即使探针被注入也不直接致死。
4. **不误杀高优先**：victim 取环内最低优先；即便成环判定偶发，也只会 die 最低者，符合 Wait-Die 语义。
5. **成环在去重前**：保证"回到发起者"这一终局信号不会被去重误吞。

---

## 12. 正确性论证（要点）

- **只沿真实链上锁传播**：waitEdge 全部来自 global∪pending，不夹带可 wound 的 TLV 预约 → 减少假边/噪声。
- **环可闭合**：低被高回程边也记录 + 转发无剪枝 → 探针能沿完整 WFG 回到 Init → 不漏真环。
- **终止/有界**：`(init,nonce)` 去重 + 边数有限 → 每笔发起最多 O(边数×分片) 的探针，无无限循环。
- **victim 安全**：环成员来自完整 Path，取全局最低者 → 高优先绝不成为 victim。
- **die 幂等/无双重处理**：复用 CRTx 投票，重复探针不重复 die（有投票去重）。

---

## 13. 配置与实验开关（非语义）

- `TimeoutConfig.EnableDAG bool`（默认 false）：消融期**去掉 DAG patch-救援**让冲突暴露成边、便于实现/验证 CMH。
  它**不改变** §5 锁模型语义——CMH 仍只看链上锁(global+pending)，TLV/DAG 仍不进 waitEdge。
- 恢复 DAG（true）时 patch-救援恢复，但 `isPatchFinalized` 只作 wound 闸门，不引入 TLV waitEdge。

---

## 14. 验证计划

### 14.1 单元 / 构造场景
1. 2-环：A(shard1) 被 B、B(shard0) 被 A（互持真实链上锁）→ 探针回 Init、victim=低优先者。
2. 3-环 / N-环：Path 完整、victim 为全局最低者。
3. 低被高只记边不探测：B→A 记入 waitEdges、probeSent 不增。
4. 成环不被去重吞：发起者自处理后探针绕回仍能判环。
5. 清理：holder commit/die 后各分片 CleanupHolder 删除对应边，再探测不误判假环。

### 14.2 安全
6. 伪造探针（错 BLS / 单节点签名 / 篡改 Path）→ 被 Verify 拒绝。
7. 重放/跨 epoch：重复 (init,nonce) 丢弃。

### 14.3 集成 / 压测
8. 复跑 rate=200：`unfinished→0`、`rollback` 有界、`deadlockRingDetected/VictimDied>0`、leader waitEdge 稳定非 0。
9. 与「宽松大量 die」对比 rollback 更低。
10. 对照 EnableDAG=true 验证 DAG 恢复后行为不回退。

---

## 15. 边界 / 风险 / 待确认

- **TLV 预约饥饿 ≠ 链上死锁**：若 unfinished 主体是"低优先永远构建不了 SimTx"（调度），CMH 链上环看不到，
  需单独机制（保证低优先能重新验证/构建）。实现期需用日志区分：
  (a) 真实链上锁 partial-winner 互等（本方案处理）vs (b) TLV 预约饥饿（另处理）。
- **非单调优先级 / 不可比边**：假设全局全序；不可比会导致定向传播断（声明为场景假设）。
- **路由多播成本**：v1 向 RelatedShards 多播，O(相关分片数)/跳；可优化 holder→blocking-shard 反查。
- **edgeValid 统一**：转发跳重验证须用 findChainHolder（global+pending）；当前实现只查 global，待收口。
- **清理遗漏**：CleanupHolder 须挂在各分片 CRTx；遗漏会残留旧边，靠转发跳重验证 + BLS + 投票兜底。
- **DAG patch 覆盖**是否仍作为"不算阻塞"前置，需用日志验证其必要性后定夺（§7 暂不作为 CMH 依据）。

---

## 16. 实现范围 / 里程碑

**M0 结构 & RPC**：`api` DeadlockProbe/Ack、LockLayer(OnChain)、proto+grpc(`DetectDeadlockProbe`)、comm 注册。
**M1 detector 纯逻辑**：waitEdges/seenProbes、OnBlocked(记边+探测解耦)、HandleProbe(成环先去重)、
edgeValid(统一 holder)、resolveRing/victim、CleanupHolder、Stats。构造单测。
**M2 触发接线**：RetryCommit Phase2、VerifySimulation 兜底（findChainHolder + OnBlocked）；OnBlockCommitted→CleanupHolder。
**M3 跨分片闭环**：BLS 背书/验证、多播路由；§14 单测/安全/压测。

文件落点：`ssc/deadlock_detector.go`（新）、`ssc/api`、`ssc/api/proto`、`ssc/comm.go`、`ssc/retry_scheduler.go`、
`ssc/verify.go`、`ssc/state_lock_impl.go`（复用 holder 查询）。

---

## 17. 相关文档

- DSN-52：链上 Wait-Die + TLV wound 分层收敛（resolution 基底）。
- RSH-03：论文向（锁语义/触发以本文 §5-§8 为准）。
- 实现推进：`.bridge/handoff/HANDOFF-20260902-crossshard-cmh-dag-switch-tlv-slm.md`。

---

## 附录 A：接口与方法签名清单（代码向）

### A.1 deadlockDetector（`ssc/deadlock_detector.go`）

构造
```go
func newDeadlockDetector(
    selfShard uint32,
    state     *RetrySchedulerStateAccessor, // GetLeader/GetCommittee/ShardNum/GetTxMeta 等
    comm      *Comm,
    tempLockView *TempLockView,             // 仅"触发观测/IsWounded"，不作 waitEdge 来源
    dag       *offChainDAG,                 // 仅 isPatchFinalized(wound 闸门)/isKeyPatchCovered
    slm       *stateLockManager,            // GetLockHolderMeta/GetRLockHolder/GetTxPriority/GetTxMeta
    signerMgr api.BLSSignerMgr,
) *deadlockDetector
```

记边 / 触发（两层解耦）
```go
// 记边（方向不限）: 找到真实链上持有者 B(global/pending) 后调用
func (d *deadlockDetector) OnBlocked(
    waiter, holder common.Hash,
    shard uint32,
    key api.LockKey,
    layer api.LockLayer,             // 一律 LockLayerOnChain
    waiterPri, holderPri api.Priority,
) error

// OnBlockedAt 显式携带 epoch/blockNum（防重放），内部同 OnBlocked
func (d *deadlockDetector) OnBlockedAt(
    waiter, holder common.Hash, shard uint32, key api.LockKey, layer api.LockLayer,
    waiterPri, holderPri api.Priority, epoch api.Epoch, blockNum uint64,
) error

// 探测门：仅"高被低 + holder 已知 + 不可 wound"返回 true（记边在 OnBlocked 内无条件完成）
func (d *deadlockDetector) shouldTriggerProbe(e waitEdge) bool
```

waitEdge 内部
```go
func (d *deadlockDetector) addWaitEdge(edge waitEdge) bool                 // 去重后记入
func (d *deadlockDetector) getEdges(waiter common.Hash) []waitEdge         // waiter 出边快照
func (d *deadlockDetector) hasLocalWaitEdge(waiter, holder common.Hash,
    key api.LockKey, layer api.LockLayer) bool                            // 本分片是否存在该边
func (d *deadlockDetector) isKeyPatchCovered(key api.LockKey, exclude common.Hash) bool // 待 §15 重估
```

探针接收 / 成环 / 清理 / 统计
```go
func (d *deadlockDetector) HandleProbe(p *api.DeadlockProbe) *api.DeadlockProbeAck
    // 顺序：BLS 验签 → 成环(Current==Init，先去重) → 去重 → 重验(仅非首跳) → 沿 WFG 转发(无剪枝)
func (d *deadlockDetector) edgeValid(e waitEdge) bool                     // 用 findChainHolder(global+pending) 重验
func (d *deadlockDetector) CleanupHolder(txHash common.Hash)              // holder commit/die 时清相关边
func (d *deadlockDetector) OnBlockCommitted(block *types.Block)           // 见 CRTx→CleanupHolder
func (d *deadlockDetector) Stats() DeadlockDetectorStatus
```

发送侧
```go
func (d *deadlockDetector) triggerProbe(init, sender, current common.Hash,
    shard uint32, key api.LockKey, layer api.LockLayer, epoch api.Epoch, blockNum uint64) error
func (d *deadlockDetector) signProbe(probe *api.DeadlockProbe) error       // 委员会阈值 BLS
func (d *deadlockDetector) dispatch(p *api.DeadlockProbe)                  // 单测缝优先，否则 routeProbe
func (d *deadlockDetector) routeProbe(probe *api.DeadlockProbe)            // 多播 holder.RelatedShards
func (d *deadlockDetector) relatedShardsOf(txHash common.Hash) []uint32
```

victim 选择
```go
func (d *deadlockDetector) resolveRing(p *api.DeadlockProbe)
func (d *deadlockDetector) uniquePath(path []common.Hash, init common.Hash) []common.Hash
func (d *deadlockDetector) lowestPriority(members []common.Hash) common.Hash
func (d *deadlockDetector) priorityOf(txHash common.Hash) (api.Priority, bool)
```

监控状态
```go
type DeadlockDetectorStatus struct {
    WaitEdges       int   // 当前 waitEdge 条数
    SeenProbes      int   // seenProbes 数量
    ProbeSent       int64
    ProbeDropped    int64 // 无效签名/无出边/重放丢弃
    ProbeInvalidSig int64 // BLS 验签失败
    RingDetected    int64
    VictimDied      int64
}
```

### A.2 关键依赖（供接线）

- `RetrySchedulerStateAccessor.GetLeader(epoch, shard) *api.Member`：路由目标。
- `RetrySchedulerStateAccessor.GetCommittee(epoch, shard)` / `ShardNum()`：BLS 委员会。
- `stateLockManager.GetLockHolderMeta(key)`（global 写）/ `GetRLockHolder(key)`（global 读）
  / `GetTxPriority(tx)` / `GetTxMeta(tx).RelatedShards`：holder 来源① 与路由。
- `corestate.DB.FindPendingLockHolder(key)`：holder 来源②（pending）。
- `signerMgr.GetSSCSigner().Verify(p)` / `.Aggregate(...)`：探针背书/验证。

### A.3 单测缝（仅测试用，生产为 nil）
```go
verifyOverride   func(p *api.DeadlockProbe) error   // 代替 BLS 验签
routeOverride    func(p *api.DeadlockProbe)          // 捕获/代替跨分片转发
edgeValidOverride func(e waitEdge) bool              // 代替 findChainHolder 重验
rollbackVictim   func(txHash common.Hash)            // 注入 victim 终局回滚
```

---

## 附录 B：A/B 2-环时序（ASCII）

设定：Tx A（高优先，需 Shard1·K1 + Shard2·K2），Tx B（低优先，同需两把锁）。
形成部分赢家环：A 已在 Shard1 持 K1、想拿 Shard2·K2 被 B 卡；B 已在 Shard2 持 K2、想拿 Shard1·K1 被 A 卡。

```
Shard1 (A 持 K1; B 想拿 K1 被 A 卡)          Shard2 (B 持 K2; A 想拿 K2 被 B 卡)
┌───────────────────────────────┐        ┌───────────────────────────────┐
│ waitEdge:  B → A  (低被高,只记) │        │ waitEdge:  A → B  (高被低,记+探) │
└───────────────────────────────┘        └───────────────────────────────┘
                                         ① A 被 B 卡(链上锁) → 记 A→B
                                           OnBlocked 记边
                                           shouldTriggerProbe(A<B)=true
                                           triggerProbe: Path=[A,B], 路由→B
                                                     │
             ② 探针到达 B (Current=B, Sender=A=Init) │
               成环? Current(B)!=Init(A) → 否
               去重 (A,nonce)
               首跳 Sender==Init → 不重验
               查 B 出边 → 找到 B→A
               转发: Current=A, Path=[A,B,A], 无剪枝
                                                     │
             ③ 探针回到 A (Current=A==Init)  ◄────────┘
               成环判定(先去重) → ringDetected++
               resolveRing: 环成员 uniquePath([A,B,A])={A,B}
               victim = 全局最低优先 = B
               rollbackVictim(B) → Shard1+Shard2 终局 Rollback CRTx
```

要点：
- ② 是"回程边 B→A 能走通"的关键——它只被**记录**（不探测），靠**转发无剪枝**让探针走回 Init。
- 若没有低被高的 B→A 边或转发被剪枝，探针停在 B，永远到不了 ③ 判环。
- 成环在 ③ 于去重之前判定，发起者在 ① 自处理时写入的 `(A,nonce)` 不会吞掉回程信号。

---

## 附录 C：waitEdge / 探针生命周期状态机

```
  A 被真实链上锁 B 卡(global/pending)
        │ findChainHolder 找到 B
        ▼
   [记边 A→B] ──────────── 仅记录(低被高/无可破) → 静默等待，直到 holder 释放
        │ shouldTriggerProbe(高被低) → true
        ▼
   [发起探针 Path=[A,B], nonce] ──BLS──> [路由到 B 的 related]
        │
        ▼
   [收到探针]
        ├─ Current==Init ──────▶ [成环] → [选 victim(最低)] → [终局 Rollback CRTx]
        ├─ seenProbes 已有 ────▶ 丢弃(转发去重)
        ├─ 非首跳重验失败 ──────▶ 丢弃(stale)
        ├─ 无出边 ─────────────▶ 丢弃(不成环)
        └─ 有出边(完整 WFG,无剪枝) ─▶ 转发下一跳 Path+[holder]

  holder B 释放(commit/die CRTx) ──▶ 各分片 CleanupHolder(B): 删 holder==B 及 B 作为 waiter 的边
```

---

## 附录 D：链下 TLV 触发 + 临时边（不持久）—— v4 增补

### D.1 背景：大量卡死先在"链下请求锁"被暴露

实证：相当多 unfinished 是**高优先 A 在链下请求锁时发现被低优先 B 的真实链上锁挡住**（B 已在某分片持真实锁、是 partial winner）。这个"被挡"最早是在 **`TempLockView.TryLockWithPriority`**（cannot-wound）这一层观察到的——B 的预约不可 wound（等价于已定稿/将上链），所以 A 抢不过。

若只有链上 `Lockable` 一个入口，会漏掉大量在链下这一层就被挡、还来不及走到链上 CheckLock 的卡死交易。因此 **TLV 也是必需的探测触发入口**。

### D.2 为什么"只发起探测、不给可遍历边"判不出环

CMH 判环 = 探针沿"B 被谁卡 → C 被谁卡 → …"逐跳走回 A。每跳都要能回答"当前节点被谁卡住"。
该答案来自已记的持久 waitEdge，或来自"当时的活/临时阻塞状态"。
- 若 A→B 这条链下阻塞**既不记持久边、探测时也不提供活状态** → 探针到 B 时 `getEdges(B)` 为空 → 断链 → 回不到 A → **判不出环**。

**结论**：只发起探测、不提供任何可遍历边，无法判环。

### D.3 方案：把链下 cannot-wound 阻塞作为"临时边"

把"本分片这次链下（cannot-wound）阻塞 A→B"当作**临时边**，**只用于探针遍历（并可发起 A 的探测）**，
**不写入持久 waitEdges**（不进 map、不做 CleanupHolder 语义），随阻塞解除即消失。

两种边的分工：

| 边种类 | 来源 | 是否持久 | 用途 | 清理 |
|---|---|---|---|---|
| **持久边** | 链上锁(global∪pending)：`Lockable` 钩子 / Verify / Retry Phase2 | 是（waitEdges） | 环遍历 + CleanupHolder 语义 | holder CRTx 上链时 CleanupHolder |
| **临时边** | 链下 TLV cannot-wound（holder 不可 wound / 等价链上但尚未在 global+pending） | **否**（独立瞬态结构） | (a) 发起 A 的探测；(b) 探针遍历时补上 B 的出边让环闭合 | 阻塞解除(A 拿到锁 / B 被 wound/commit/rollback / OnBlockCommitted)即清 |

### D.4 为什么临时边不写持久 waitEdges

- 链下 TLV 预约本身**不是**真实链上锁；若全部写进 waitEdges 会刷大量瞬态假边、污染 CleanupHolder/持久图。
- 但判环又需要它作"这一跳"，所以用**独立的瞬态临时边**承接遍历，寿命短、随释放消失。
- 当 B 真正上链（global/pending）后，同一关系会以**持久边**形式由链上 Lockable 重新登记（去重兜底，不重复）。

### D.5 触发规则（TLV 只触发，边来源区分）

```
TryLockWithPriority(A, …) 返回 locked=false（cannot-wound）：
  1) 定位 B = 该 key 的 TLV 预约 holder（GetTempLockHolder）
  2) 回查 B 是否持真实链上锁（global∪pending，findChainHolder）
       ├─ 找到真实链上 holder H → 记【持久边】A→H + shouldTriggerProbe→探测（与链上一致）
       └─ 未找到（B 只是 TLV 预约但不可 wound）→ 记【临时边】A→B（仅供探测遍历/发起）+ 探测
  3) 若 B 可 wound（holder 优先级更低且未 finalize）→ 直接 wound，不触发（非 cannot-wound）
```

- 记边（持久或临时）方向不限（含低被高回程），补全完整 WFG。
- 探测：仅高被低才发起（shouldTriggerProbe）；victim 为环内最低优先。
- **临时边去重/寿命**：同 waiter 短存，阻塞解除即清；不参与 CleanupHolder 持久语义。

### D.6 落点与待定（实现前已定口径）

1. 临时边**同时**用于"发起 A 的探测"和"探针遍历补出边"（它们代表不可 wound、值得探测的阻塞）。
2. 临时边存于 `deadlockDetector` 内**独立的瞬态表**（waiter 索引），由"阻塞解除"（A 拿到锁 / B 被 wound / OnBlockCommitted 释放 / B commit-rollback）清除。
3. 与链上持久边共存：先查持久 waitEdges，查不到再查临时边补遍历；成环/去重/选 victim 逻辑共用。

> 说明：本附录先把机制写清楚。实现时 TLV 钩子接在 `TryLockWithPriority` 的 cannot-wound 分支，链上统一入口仍是 `Lockable` 钩子（持久边）；TLV 钩子负责补临时边并触发探测。
