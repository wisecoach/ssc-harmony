---
id: DSN-55
title: "DAG 救援交易：得锁后优先级提升保锁（抢锁保持低优先）+ 跨分片 DAG 标记下传"
type: DSN
status: draft
priority: P0
author: Designer
created: 2026-09-04
updated: 2026-09-05
scope: [ssc/api, ssc/api/proto, ssc/retry_scheduler.go, ssc/temp_lock_view.go, ssc/impl.go]
refs: [DSN-49, DSN-50, DSN-51, DSN-52, DSN-53, DSN-54, REV-DSN-54]
---

> **状态**：draft（rev2）
>
> **rev2 说明**：撤下 rev1 的“本分片 reservation/admission 前插”方案（实测 61%→61% 无改善，因为跨分片不对称：A 分片判 chain-ready、B 分片仍按低优先处理并被 wound）。
>
> **一句话（rev2）**：DAG 救援交易**抢锁时保持普通(低)全局优先级**，公平竞争；**一旦在 RetryCommit 成功拿到锁（Locked:true）**，就把这笔**已持有的锁的优先级提升（admission 提权）**，使 `canWound` 因“持有者优先级更高”而不再放行后来者 wound 抢走——直到它走上链(变真链上锁)或失败被 RetryCancel 释放。为让所有 related shard 同判，“这是 DAG 救援 attempt”由 origin 用 `isChainTxMarked` 判定并**随 RetryCommit 下传给各分片**。抢锁阶段仍按普通全局 Priority，不破坏跨分片一致性。

---

## 1. 背景与问题（rate=200, ssc=1, 2026-09-04/05）

| 项 | rev1 前 | rev1(admission 前插)后 |
|---|---|---|
| DAG-consumed tx | 3723 | 3861 |
| → simulation committed OK | 90% | 91% |
| → close commit:true | 61% | **61%（无改善）** |
| DAG-stuck（committed 未 close） | 1084 | 1155 |
| └ 卡 reservation-skip | 99% | 99% |
| DAG-consumed ∩ wound-holder | 1823 | 1868 |

结论：
- 模拟关 OK（~91%）；真正问题是 **DAG 救起的 tx 拿不到/守不住锁 → 上不了链 → hot key 不释放 → 后面一直 etc**；
- rev1 的“局部 admission 前插”只在**单个分片本地**起作用，治不了**跨分片**里“A 分片判 DAG 高、B 分片仍当低优先并被 wound/不 admission”的不对称。

---

## 2. 机制分析：为什么 rev1（局部 admission 前插）不够

rev1 判据 `isChainReadyLocal` = 读本 shard `consumedPatches`（DSN-55 rev1 代码）。问题：
- DAG patch 消费是**各分片 leader 本地瞬态**；一个跨分片 tx 可能在 shard A 消费了 DAG、shard B 没有；
- 于是 A 把它当 chain-ready 前插放行，B 仍按普通低优先处理——B 上它既不被 admission，也**可以被更高优先 wound**；
- 结果整体仍失败 → 正是 rev1 后 DAG-stuck 无改善的原因。

需要一个**全分片一致的**“这是 DAG 救援 attempt”标记，并且只作用于“已得锁后保锁”，不影响“抢锁”的公平性。

---

## 3. 约束与不变量（rev2，勿破坏）

| # | 不变量 | 说明 |
|---|---|---|
| I1 | **抢锁阶段全局 Priority 不动、不加 DAG 提权** | DAG tx 抢锁时按普通 `Nonce>Origin>TxHash` 公平竞争，不插队、不 wound 别人 → 跨分片 wound 判定一致 |
| I2 | **得锁后优先级提升保锁** | 一旦 RetryCommit 拿到锁(Locked:true)，提升该 tx **已持有的锁**的优先级，使 `canWound` 不再放行后来者 wound（等价“即将上链的锁”，与链上锁不可 wound 一致） |
| I3 | **链上不可 wound、链下可 wound 的逃生口保留** | 抢锁阶段仍按普通优先级可被 wound；仅得锁后的优先级提升保锁，且随 RetryCancel 释放，不造成永久钉死 |
| I4 | **DAG 标记跨分片一致** | “是 DAG 救援 attempt”由 origin 判 `isChainTxMarked`，随 RetryCommit 下传，所有 related shard 用同一份事实 |
| I5 | **RetryCommit / RetryCancel 兜底** | 任一分片拿不到锁 → Locked:false → 整体 RetryCancel（释放已得锁 + 撤销优先级提升）→ 无死锁 |

---

## 4. 方案（rev2）：得锁后优先级提升保锁 + 跨分片 DAG 标记下传

### 4.1 语义分界（核心）

| 阶段 | 优先级/行为 |
|---|---|
| **抢锁**（TryLockWithPriority / RetryCommit Phase1/2） | 保持普通(低)优先级，公平竞争；抢不到就 Locked:false |
| **得锁后**（RetryCommit 返回 Locked:true，锁已到手） | 提升该 tx 已持有锁的优先级（admission 提权）：后来的更高优先 tx **不能 wound 抢走**，直到上链或 RetryCancel |

### 4.2 DAG 标记跨分片下传

- origin 在 `tryToReSimulation` 判定 `isDAGAttempt := isChainTxMarked(txHash)`（HandleRetrySignal 时已在 origin 置位）；
- 把 `isDAGAttempt` 随 **RetryCommit** 请求传给每个 related shard（当前 RPC 只带 `txHash`，需加一个 flag 字段）→ 各分片用同一份事实，无不对称。

### 4.3 得锁后优先级提升（即“用 admission 提权保锁”）

保持“提高优先级”这一既有保锁语义（而非另设布尔闸）。DAG attempt 在 RetryCommit 得锁成功后，把**它已持有的每把 TLV 写锁的 `Priority` 提升到最高**（等价不可 wound 的准 Finalized）：

```go
// RetryCommit 得锁成功 && isDAGAttempt → 提升已持锁的 priority
func (v *TempLockView) protectHeldLocks(txHash common.Hash) {
    // 遍历该 tx 持有的 tempWriteLocks，把 entry.Priority 升到最高，
    // 使 canWound 比较 requesterPri.Less(holderPri)=false → 不被 wound
}
```

- 这样 `canWound` 不需新增逻辑：因为持有者优先级已被抬高，`requesterPri.Less(entry.Priority)` 天然返回 false → 后来者无法 wound；
- **抢锁阶段**（TryLockWithPriority 内）仍用普通全局 Priority 写入，未提升 → 公平竞争不受影响；
- 提升只发生在“得锁后”，且随 RetryCancel / GC 释放该锁一并清除（锁都没了，优先级自然作废）。

### 4.4 生命周期

| 时机 | 动作 |
|---|---|
| RetryCommit 抢锁成功(TLV 已拿) 且 `isDAGAttempt` | `protectHeldLocks(txHash)`：提升其已持锁 priority |
| 全分片成功 → 构建 SimTx 上链 | 上链后变真链上锁（天然不可 wound）；随该锁/交易终局自然清理 |
| RetryCancel / 任一分片失败 / GC / close | 释放锁（锁删除 → 优先级提升自然作废） |
| 抢锁失败(未得锁) | 不提升优先级，不产生副作用 |

要点：优先级提升只作用于**已到手且正被使用的那把锁**，生命周期受 RetryCommit/RetryCancel 判决周期约束，不会形成跨分片永久不对称。

---

## 5. 安全性论证

1. **抢锁一致性不被破坏**：DAG tx 抢锁仍按普通全局 Priority（I1），不做 DAG 提权 → 不引入各分片抢锁判定不一致。
2. **解决跨分片 wound**：A 得锁后提升优先级保锁；B 也收到同一 `isDAGAttempt`，得锁后同样提升保锁 → 各分片一致，不再出现“A 保、B 抢”的撕裂。
3. **无死锁**：优先级提升仅在有界窗口（本分片 RetryCommit 得锁 → 整体判决）内生效；若 B 拿不到锁 → Locked:false → RetryCancel 把 A 已得锁及其优先级提升一起撤销 → 无永久占锁。
4. **保留逃生口**：抢锁阶段仍可被 wound；只有“已得锁的 DAG-attempt”被保护，且可回退（RetryCancel），不做“永久禁 wound”。

---

## 6. 与既有机制关系

| 机制 | 关系 |
|---|---|
| 全局 Priority（Wound-Wait） | **不改**；抢锁仍按其判定 |
| canWound | 无需新闸：得锁后持有者 priority 已被抬高 → `requesterPri.Less(holderPri)=false` 自然不可 wound |
| isPatchFinalized | 平行，不冲突；DAG-attempt 得锁后近似“准 Finalized” |
| RetryCommit / RetryCancel | 核心依赖：得锁置位、失败回滚清除，兜底无死锁 |
| isChainTxMarked（origin） | 作为跨分片 DAG-attempt 判据，随 RetryCommit 下传 |
| DSN-55 rev1（admission 前插） | **撤销**，不再作为主方案（可留作可选的缓解） |

---

## 7. 变更清单（已实现）

1. `ssc/api` + `ssc/api/proto`：新增 **additive RPC `RetryCommitDAG(Hash)`**（不改现有 `RetryCommit` 签名）；常量 `Method_RetryCommitDAG`、api.Service 方法、rpc handler、comm case `retryCommitDAG`；已重新生成 `ssc.pb.go`/`ssc_grpc.pb.go`。
2. `ssc/retry_scheduler.go`：
   - 主体重构为 `retryCommit(txHash, dagAttempt bool)`；`RetryCommit`→false、`RetryCommitDAG`→true；
   - `tryToReSimulation`：`isChainTxMarked(txHash)` 为真时对全部 related shard 改调 `Method_RetryCommitDAG`；
   - 得锁成功路径（early-consumed / Phase2 普通成功 / Phase2b DAG 成功）在 `dagAttempt`(或 DAG-consume) 时调 `protectHeldLocks` 提权保锁。
3. `ssc/temp_lock_view.go`：新增 `protectHeldLocks(txHash)`（把该 tx 已持有的 TLV 写锁 priority 提到最高，使 `canWound` 拒绝 wound）；`IsHeldProtected` 诊断；RetryCancel/GC 释放锁时随锁删除自然作废。
4. `ssc/impl.go`：新增 `sscService.RetryCommitDAG`。
5. 打点：`SigDAGHoldProtected`（得锁后优先级提升次数）。

---

## 8. 观测 / 验证

- DAG-consumed ∩ wound-holder：1868 → 应显著下降（得锁后被 wound 消失）；
- DAG-consumed → close commit:true 转换率：61% → 应上升；
- DAG-stuck（committed 未 close）1155 → 应下降，且不再集中在“得锁后又被 wound / 跨分片被抢”；
- 计数：新增 `SigDAGHoldProtected` / 保留 `SigRetryCommitWoundedPre`、`SigRetryCommitTryLockWounded` 作对照。

---

## 9. 待确认 / 风险

1. RetryCommit 请求加 flag：是改 RPC 结构，还是复用既有可空字段（如 Condition/一个 bit）——需权衡兼容与改动面。
2. 优先级提升何时精确施加：只在“确实持有 TLV 锁”的返回路径（如 Phase2b 成功）施加，还是所有 Locked:true 都施加？需与“是否真正持锁”对齐，避免误保未持锁交易。
3. 若 DAG attempt 的某分片得锁后提升优先级保锁、但长时间凑不齐其它分片（锁被更高优先非 DAG tx 占住）→ 该分片锁被暂时挡着——需确认不会造成新的短时饥饿（应受 RetryCommit 周期约束）。
