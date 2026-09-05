# DSN-58: 跨分片 ready 聚合门活性修复（分母正确化 + reservation 反饥饿）

> 状态：**draft（设计先行，实现待确认点拍板后落地）**
> 关联：DSN-52/53/54/55/56/57。DSN-57 已 REVERTED；本文是继其后的第二轮收敛设计。
>
> **一句话**：跨分片交易卡死(unfinished/冻结)的直接表现是 **origin 的 ready 聚合门 `readyCnt == len(RelatedShards)` 永不闭合**。
> **根因（§5b 已定稿）＝ H2（reservation 饿死）**：某条（或全部）需重放的腿一直在 retryPool 里不被
> OnBlockCommitted 的 reservation admission（同 key 每块只放行最高优先一笔）→ 从不 ready → 门永不闭。
> **H1（分母把已放好腿的分片算进去等）非主导**，干净基线样本里“已放好的腿”往往不存在。
>
> 修复 = **保留 ready 前置门**（§3 教训证明不能拆，它是跨分片原子性/节流），只做 **§4.3 reservation 反饥饿**：
> 被跳过 N 块的交易当块强制提权放行一次 → 能 ready → 门闭合 → tryToReSimulation。**不做分母修复（§4.2 撤下）**。

---

## 1. 背景与症状

### 1.1 触发场景（rate=200, ssc=1, 2026-09-05）

基线（15:38 result）：
```
total 19999, committed 11115, rollback 2261, unfinished 6623
per-shard unfinished: S0 1089 / S1 2867 / S2 672 / S3 1995
```
日志持续 68 分钟、链出到 **block 2037**（~2s/块，链健康），但从约 block 218(14:10) 起 **整链冻结**：
- `OnBlockCommitted stats`：`retryPool=5215 / globalLocked=157 / signals=1828 / consumedPatches=333 / deadlockRingDetected=4` 从 14:10 到 15:10 **逐字节不变**（1800+ 块零进展）。
- 冻结期刷出 ~800 万行 `reservation skipped candidate`（累计 8,879,141）。
- 尾段 250 万行统计：`set signal ready:true` = 53 万、`tryToReSimulation / retry commit success / commit or rollback with proof` = **全 0**。
- `OnBlockCommitted: reservation completed`：`candidateCount=5215, onChainFreeAdded=5215`（锁全空）→ 说明**不是锁互斥挡路**。
- ready 信号来源分析（尾段 1219 笔 ready:true）：**93%(1140) 只被 1 个分片标记 ready**。

### 1.2 结构性证据：已放置腿的分片不再发 ready

抽样冻结交易 `dcfac5c4…`（related=[3 1]）：
- Shard1：4 validator 均 `VerifySimulation success` → 投 Commit vote → 聚合。
  ⇒ **Shard1 的腿已上链(simNum0)，已不在 retryPool，永远不会再发 re-sim ready**。
- Shard3(origin)：`tempWriteLock conflict` → `added to retry pool`，只收到 shard1 `ssc vote [1/2]`，
  自己的腿一直没放好、被 reservation 反复跳过。
- 结果：origin 等 `readyCnt==2`，shard1 永不出现 → **永不 tryToReSimulation** → Shard1 已上链的腿也永不 CR（孤儿）。

## 2. 根因

### 2.1 ready 前置门（保留，它对）

`HandleReSimulationSignal`（origin 侧）：
```go
readyCnt := 统计各 fromShard 的 Ready 信号数
if retryTx != nil && readyCnt == len(retryTx.RelatedShards) {
    go rs.tryToReSimulation(retryTx)
}
```
`tryToReSimulation` 再并行 RetryCommit 到所有 related 分片、全 Locked 才把 SimTx 上链。

这个门不是多余开销，它起的是**跨分片原子性/节流**作用：只有“所有相关分片都认为轮到它了”时才去拿链上锁并上链，避免高争用下太多交易进入“已上链、等跨分片 CR 聚合”的半途占锁状态（见 §3 教训）。

### 2.2 已排除的候选 H1：“分母”把已满足的分片算进去等（§5b 后不实现）

> ⚠️ **§5b 干净基线诊断后排除**：冻结样本里“已放好的腿”往往根本不存在（全腿都还只在池里待放），
> H1 不是根因。本文保留作记录，不实现分母修复（§4.2 撤下）。

原设想：若某分片在当前重试 simNum 上已放好腿、不再发 ready，则 `readyCnt == len(RelatedShards)` 会把它也
计入待等集合 → 永不闭合。实测冻结样本多为其反面（全部腿都在池里、无一被放行）→ 见 §2.3 H2。

### 2.3 根因 H2（§5b 已确认）：某条腿被 reservation 饿死 → 从不 ready

`OnBlockCommitted` 的 reservation 每个 key 每块只放行最高优先一笔：
```
candidateCount=5215  onChainFreeAdded=5215(锁全空)  selected≈780  reservedKeys≈770
reservation skipped candidate 累计 8.8M 次
```
- 冻结跨分片交易的某条（或全部）腿在 retryPool@simNum0 里，因同 key 有更高优先者而**每块被跳过**，
  从不被 admission → 从不发 ready；
- origin 的 `readyCnt == len(RelatedShards)` 永不闭合 → 永不 tryToReSimulation → 永不重放/上链 → 冻结。
- 锁其实全空（onChainFreeAdded==candidateCount），所以既不是锁死锁（CMH 无环可断），也不是“对端已放好不 ready”（H1），
  纯粹是 **admission 层饿死**（DSN-53 §10 明确列为 CMH 非目标）。

## 3. 失败尝试与教训（必须保留记录）

### 3.1 尝试：去掉前置门（“单分片 ready 即 tryToReSimulation”）
实现于 2026-09-05 20:05 实验，**回归**：
```
committed 11115→7304, rollback 2261→1666, unfinished 6623→11029
verify_success 行数 5999→12251（SimTx 上链尝试翻倍）
verify 成功却永不 CR-exec 260→2007（孤儿锁暴涨）
CMH ring_detected 4→55, victim 4→55（清不完）
```
**结论**：门不能拆。去掉门 = 去掉跨分片节流 → 大量交易在“只有部分分片真 ready”时就把 SimTx 打上链占锁 → 进入等跨分片 CR 的半途态 → 互相等 CR 票 → 死锁环暴涨。**RetryCommit 只保证“不重复提交”的互斥(原子性)，不负责“别让太多交易同时半途占锁”的活性(节流)——活性靠的就是这扇前置门。**

> 已回滚（`HandleReSimulationSignal` 恢复 `readyCnt == len(RelatedShards)`），基线 15:38 状态。

## 4. 设计

### 4.1 设计目标
保留 ready 前置门（跨分片原子性/节流），只消除“某条腿被 reservation 饿死、从不 ready”的活性问题。
**不做分母修复（§4.2 已撤，非根因）。**

### 4.2 reservation 反饥饿（唯一实现项）
给 retryScheduler 增加**每笔交易的 reservation 跳过计数**，在 `OnBlockCommitted` 的 reservation 段：
1. 交易被 reservation 跳过（同一块内因 key 已被更高优先预留而放弃）时，累加 `skipCnt[txHash]`；
2. 当 `skipCnt[txHash] >= N`（默认如 50 块）时，该交易在**当块强制提权放行一次**：即使它的 key 已被更高优先者
   预留，也放它通过、标记 ready（走既有的 Ready 门 → 发信号 → 让 origin 的前置门闭合 → tryToReSimulation）；
3. 放行后清 `skipCnt[txHash]`；若该轮仍未终局（回 retryPool），下轮重新累计。

要点：
- 只改 `OnBlockCommitted` 的 reservation/selected 段，不动 TLV/CMH/verify/ready 前置门。
- 反饥饿只影响“被饿死到阈值”的交易，正常高吞吐下多数交易在阈值前已放行，不影响主路径。
- 强制放行后它能正常走终局：要么跨分片凑齐(ready 门闭→tryToReSimulation→RetryCommit→上链/CR)，要么
  确定性回滚——两条都是终局，不会再无限滞留。

### 4.3 只等“仍需放行”的分片：ready 门分母 = 无 sscVote 的分片，扇出也只发给它们
关键修正（避免扇出到已放好腿分片导致 Locked:false）：**已放好腿(有 sscVote)的分片不算“待重放”，既不进分母，也不进 RetryCommit 扇出目标。**
```
placed(shard)   = origin 已收到该分片 sscVote(commitStates[simNum][shard] 存在, 腿已放好)
needing(T,simNum) = { s∈RelatedShards : !placed(s) }        // 仍需放行
readyCnt(T,simNum) = #{ s∈needing : 该分片已发 ready retrySignal }
当 readyCnt == |needing| 且 |needing|>0 → tryToReSimulation(T, 目标=needing)
```
- **只广播给 `needing`（有 retrySignal / 无 sscVote）的分片 leader**；已放好腿(有 sscVote)的分片不参与扇出。
- `HandleReSimulationSignal`：分母由 `len(RelatedShards)` 改为 `|needing|`，分子只数 `needing` 里已 ready 的。
- 收到 sscVote 后 `needing` 缩小（该分片从 needing 移入 placed），需**重算一次**：若仍为 0（全部放好）
  走原有 CR 全票路径；若仍 >0 且 readyCnt 已覆盖 → 触发。

**双触发源**（缺一不可）：
1. **收到 retrySignal**（`HandleReSimulationSignal`）后重算；
2. **收到 sscVote**（origin `HandleCXTCommitSSCVote` 记录后）**也重算一次**——某分片刚放好腿从 needing 移出，
   若其余 needing 早已 ready，则此刻 readyCnt==|needing| 就触发。

### 4.4 reservation 反饥饿（§4.2）与 4.3 的关系
- §4.2 反饥饿解决“needing 里的腿被 reservation 饿死、从不发 ready”；
- §4.3 解决“needing 的集合随 sscVote 收缩 + 收到 sscVote 时也触发” —— 两者合起来保证“needing 全 ready”能真正达成。
- 都保留 ready 前置门语义（不拆门）；§4.2 已实现，§4.3 待按 §4.5 落地。

### 4.5 实现要点（§4.3）
- 给 `RetrySchedulerStateAccessor` 增只读谓词（impl.go 接线到 `commitStates`）：
  `LegSscVoted func(txHash common.Hash, shard uint32, simNum int) bool`
  // origin 侧：`CommitSSCVotes[simNum][shard]` 是否已存在（该分片已放好当前 simNum 的腿）
- `retryScheduler` 提供一个统一判定/触发入口 `MaybeTriggerReSim(retryTx)`：
  1. `needing = { s∈retryTx.RelatedShards : !LegSscVoted(retryTx, s, retryTx.SimulationNum) }`；
  2. `readyCnt = #{ s∈needing : rs.signals[simNum][s].Ready }`；
  3. 若 `readyCnt == |needing| && |needing|>0` → `tryToReSimulation(retryTx, needing)`。
- `tryToReSimulation` 增加参数 `targets []uint32`（=needing），**只对 targets 扇出 RetryCommit**，不再对全部 related。
  （已放好腿的分片不在 targets → 不会因不在其 retryPool 而 Locked:false。）
- `HandleReSimulationSignal` 收到 retrySignal 后、`HandleCXTCommitSSCVote` 收到 sscVote 后都调 `MaybeTriggerReSim`。
- 干净日志同 simNum：`LegSscVoted(tx, s, N)` 直接可用；若日志出现抬 N+1 再补旧腿作废判定。

### 4.6 不做的
- **不拆 ready 前置门**（§3 教训）。
- 不按“上游已上链才放行”在 internal_pool 扣留（DSN-57 已 REVERTED，会饿死跨分片下游）。

## 5. 语义点（对 §4.3 sscVote 侧仍要按 (a)/(b) 确认）

> §5b 定稿后，**§4.2 reservation 反饥饿不依赖 (a)/(b)**；
> 但 **§4.3 的 sscVote 计入分子**是否成立，取决于“已放好 simNum=N 腿的分片，在其它分片把重试抬到 N+1 时，
> 它算不算已满足(N+1)”。
- (a) 同 simNum 补齐：其余分片重试仍放 N → 已放好 N 的分片算满足当前 simNum → `LegSscVoted(tx,shard,N)` 直接可用。
- (b) simNum 抬 N+1 重放：已放好 N 的分片需重放到 N+1 → 它**不算**满足 N+1，§4.3 不能把它的旧 N sscVote 算进分子；
  此时需它跟进 retryPool@N+1 发 ready（可能又回到反饥饿问题）。
- 干净基线的 sscVote 都是同 simNum（未抬升），§4.3 按当前 simNum 的 `CommitSSCVotes[simNum][shard]` 判定即可；
  若后续日志出现抬升 N+1，再补“旧腿作废→按 N+1 判定/重放”。

## 5b. 诊断结论（2026-09-05 干净基线，已定稿）

> 用**回滚后干净基线**（前置门恢复版，21:16 result，结果回到 committed~11000/rollback~2285/unfinished~6714）
> 抽冻结跨分片交易逐分片追踪，确认根因：

**样本 1：`5a8a0a27…`（related 覆盖 shard1/2/3）**
- 三腿都只是 simNum0 `added to retry pool`，**无任何分片 verify success/放腿**；
- 之后各自被 `reservation skipped` 47–51 次直到结束，**从不被 admission → 从不 ready**。
- ⇒ 纯 **H2 饿死**：所有腿都仍需放(在池)、却没一条能被放行。这里根本没有“已放好的腿”可豁免 → **H1 分母修复不适用**。

**样本 2：`10eb4ad2…`（related 覆盖 shard1/3）**
- Shard1：leader `DSN-57 deterministic rollback`，9042/9046 `verify success simNum0`（腿已放）；
- Shard3：`mu conflict(write)@0` + `Wound×8` + `reservation skipped 49`，腿没放、在池里被饿死；
- 整体仍停 **simNum0**（无 `simNum+1` 推进）。
- ⇒ 也落在 **H2**：需要重放的那条腿(shard3)饿死 → 永不 ready。

**两样本均未出现干净基线下的 `simNum+1` 抬升**（此前 20:05 拆门版看到的 `added to retry pool (simNum=1)` 是拆门造成的
异常 churn，不代表正常基线语义）。

**定稿结论**：
- **根因 = H2（reservation 饿死）**：冻结交易某条（或全部）需重放的腿一直在 retryPool 里不被 admission，
  从不 ready → origin `readyCnt==len(related)` 永不闭。
- **H1（分母把已放好腿的分片算进去等）不适用**：核对了 origin 身份与腿状态——
  - TX 10eb4ad2（origin=shard1, related=[1,3]）：shard1 即便有 validator verify_success，**origin 自己仍把交易留在 retryPool**(skip46)，shard3 也在 retryPool(skip49) → 两腿都在池等放行；
  - TX 5a8a0a27：三腿都在 retryPool、都无 verify success、都饿死；
  - 没有任何“origin 已放好腿→已移出 retryPool→不再发 ready”的样本 ⇒ H1 前提不成立。
- **rs.signals[tx][simNum][shard] 跨块累积**（setSignal 存住、readyCnt 计累计）：只要每条腿**被放行过一次发过 ready** 门就会闭 → 卡死纯因“某条腿从未被 admission、一次 ready 都没发”。
- ⇒ **只做 §4.2 reservation 反饥饿**（让饿死到阈值的腿被放行一次去发 ready）。**分母修复(H1)不实现**——origin 仍把交易留在 retryPool 会发 ready，用它做分母反而会错误剔除本要 ready 的腿。

## 6. 实现位置（§4.2 + §4.3 均已落地，`go build ./ssc/... ./rpc/...` 通过）
- `ssc/retry_scheduler.go`：
  - **§4.2 反饥饿**：`reservationSkipCnt`（txHash→连续跳过块数，常量为 `reservationAntiStarvationBlocks=50`）；
    `OnBlockCommitted` reservation 段被跳过时累加、达阈值当块强制放行一次、放行/选中清零；
    计数 `SigAntiStarvationAdmit`（`CHAIN_RETRY_STATS` 的 `antiStarvationAdmit`）+ Info 日志。
  - **§4.3 needing/sscVote**：
    - `RetrySchedulerStateAccessor` 增 `LegSscVoted func(txHash,shard,simNum) bool`；
    - `needingShards(retryTx)` = related 中无 sscVote(未放好腿)的分片；
    - `readyAmongNeeding` + `MaybeTriggerReSim(txHash)` 统一触发：needing 全 ready（且 needing>0）→ tryToReSimulation；
    - `HandleReSimulationSignal` 收到 retrySignal → `MaybeTriggerReSim`（不再用 `readyCnt==len(related)`）；
    - `tryToReSimulation` 只对 needing 扇出 RetryCommit（不再全 related）；needing 空则 return（走 CR 全票路径）。
- `ssc/impl.go`：
  - accessor 接线 `LegSscVoted`：读 `commitStates[txHash].CommitSSCVotes[simNum][shard]`（RLock）；
  - `HandleCXTCommitSSCVote` 记录 Commit sscVote 后调 `s.retryScheduler.MaybeTriggerReSim(txHash)`。
- （不做）把已放好腿分片从分母/扇出里硬砍成“全部 related”外的别的东西——needing 即已实现该语义。

## 7. 验收 / 监控
- rate=200：
  - `unfinished` 大幅下降（目标 < 基线 6623），整轮不再出现“block ~65 后 retryPool 冻结不变”；
  - `tryToReSimulation / triggerReSim / retry commit success / retryCommitCalled` 整轮有量、不中途归零；
  - 冻结尾段“reservation skipped”不再是主占日志（反饥饿让长期被跳过的交易能被放行）；
  - 不回归 §3（不出现 verify_success 翻倍/孤儿暴涨——门未拆，反饥饿只在饿死到阈值时才放行）。
- 监控：`CHAIN_RETRY_STATS` 的 `antiStarvationAdmit`（N=50 块阈值强制放行次数）+ Info 日志
  `reservation anti-starvation: force-admit tx`、被放行后最终 commit/rollback 比例。

## 8. 风险
- **反饥饿提权若过激进**：可能增加同 key 争用 / 短时多放行。缓解：N 默认较大（如 50）、只对“连续饿死到阈值”的交易放行、每次放行后清零、且放行仅到“发 ready/走终局”而非抢占锁。
- **强制放行后仍不终局**：若该腿所在跨分片交易仍凑不齐（其它分片也饿死），可能需每个分片都各自反饥饿到位；本设计是每分片本地做，配合 ready 前置门即可各腿都放行闭合。
- 不再有 H1 分母/`LegPlaced` 相关风险（该路径已撤）。
