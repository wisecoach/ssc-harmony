# BUG-13 时延高分析 — 交接文档（新 session 接手）

> **状态**：open · 待分析
> **创建**：2026-08-10（交接给新 session）
> **分支**：`ssc_master_58aae5802`
> **HEAD**：`f662453cc`（修复 txSubmitter flight 卡死 + BUG-12 锁泄漏 + gRPC 迁移整合）
> **工作区**：干净（全部已提交）

---

## 0. 背景：已解决的前置问题（别重复排查）

- **BUG-12 锁泄漏**：已修复并入 HEAD。`committer.go` IsTxFinished 分支释放残留锁 + `state_locker.go` applyTo 诊断。
- **txSubmitter flight 卡死**（signedTx hash 匹配 bug）：已修复。`submittedTxHashes[signedTx.Hash()]`（曾用 raw `tx.Hash()` → matched 恒 0 → inFlight 涨满 1000 → worker 停摆）。详见 `tx_submitter.go` submitWithRetry。

**验证数据（8/10 实验 `20260810_185643`）**：

| 指标 | 修复前 | 修复后 |
|------|--------|--------|
| 完成率 | 0.95% | **97%** |
| globalLocked 终态 | 11,668 | **0** ✅ |
| 吞吐 | 1.4 tps | **147/s** ✅ |
| 时延 | — | **P50=31.8s / P90=75s** ❌ |

## 1. 目标：解释并降低 P50=31.8s 的时延

**完成率、吞吐、锁都已正常，唯一残留问题是时延高。** 日志：`zjnu@10.7.95.199` → `~/go/src/github.com/harmony-one/logs/harmony-sscc/shard=4_validator=4_ssc=1_delay=10_rate=150_vpn=4/`（最新实验 `20260810_185643_result.txt`）。

## 2. 已挖到的关键证据（新 session 在此基础上继续，不必重挖）

### 2.1 链上生命周期其实不快（txLife，shard1 leader 9040）
```
simToSubmit  : p50=  24ms  (模拟执行本身很快)
crSubmitToCR : p50=2710ms  (CR 提交→上链，≈1 区块，正常)
totalLife    : p50=7830ms p90=9955ms (链上从 simulate 到 CR 完成，≈4-5 块)
totalGap     : p50=4 块 p90=5 块 (块 gap，4-5 块 ≈ 8.4-10.5s)
```
**txLife totalLife p50=7.8s，但 result 时延 P50=31.8s → 多出的 ~24s 不在 txLife 覆盖范围内。**

### 2.2 关键矛盾：`submitToCommit = 0`（埋点缺失/未记录）
```
submitToCommit    : p50=0 (恒 0，未正确记录)
commitToCRSubmit  : p50=0 (恒 0)
```
skill §7.24 说 submitToCommit（SimTx 在 txpool 排队到上链）通常是主瓶颈。此字段在这套代码里始终 0，**可能需要补埋点才能测出真实排队时间**。

### 2.3 SimTx 放大（重点怀疑对象）
```
shard1 leader 9040:
  handle simulate request   : 57,750  (模拟执行)
  startCXT for tx           : 63,694  (交易模拟开始)
  AddPatch                  : 63,337  (每个成功 SimTx 注册 ChainPatch)
  submitting SimulationTx   : 63,337  (SimTx 提交)
  commit:true (leader close): 63,336
  close 唯一 txHash         : 63,557
result 的 shard1 committed  : 28,665
```
**close 唯一 txHash 63,557 vs result committed 28,665 ≈ 2.2× 放大。** 每个真实交易被 SimTx 处理约 2 次（作为 origin SimTx commit + 作为相关分片 CR 处理）。此放大直接对应时延 31.8/6.9(历史正常) ≈ 4.5×——但 2.2× 的 SimTx 处理解释不了 4.5× 时延差，需要更精确定位。

### 2.4 全部交易走 chain tx / ChainPatch 路径
```
VerifySimulation: chain tx detected (from SimTx ChainPatch), skipping lock conflict check  × 63,336 (全部命中!)
ChainPatch : 317,315  (远多于 AddPatch 63,337)
AddPatch   : 63,337
hotkey/HotKey: 426 (少量)
```
**所有 SimTx 都被标记为 chain tx 并跳过锁冲突检查**。ChainPatch 累计 31.7 万（5× AddPatch）。怀疑 PatchPool DAG / 链式检测在当前逻辑下或 DSN-44 txSubmitter 排序下过度触发，把普通交易都变链式 → 每交易多轮模拟 → 时延放大。**这是最值得深挖的方向。**

### 2.5 区块打包正常
```
shard1L: 397 blocks, 块间隔 p50=2.1s, 每块 simTxn+crTxn avg=319.7, 326 块 simTxn>50
early: blk1 newTxns=64, blk4=64, 其余 blk2-20 newTxns=0 (早期略空, 但提交均匀)
submitting 时间分布均匀 (18:34-18:46 每分 ~4500-6000)
```
出块正常、提交均匀，非块级瓶颈。`newTxns` 早期 block 1-20 几乎 0 值得留意（可能早期有预热延迟）。

### 2.6 Verify 结果
```
commit: true  = 63,336 (几乎全成功)
commit: false = 221
simulationNum>0 (resimulation) = 1,641 (少量)
callForRetry/StartReSimulation = 207
rollback with proof = 63,574 (shard1 作为 target 收到大量回滚, 来自其他分片发起的交易)
```
**重试不多（1641 resimulation / 6.3万）。** 所以时延不是简单重试风暴。

## 3. 下一步建议（待验证方向，按优先级）

1. **确认 result 时延统计口径**：result 的 latency 是从「客户端交易生成」还是「节点接收」还是「SimTx 提交」算起？txLife 从 simulate 起算。二者差值 (~24s) 到底落在哪段（客户端→节点 / txpool 入池 / 触发 simulate）。
2. **补 submitToCommit 埋点**：它在当前代码恒 0，无法看 SimTx 排队时间。若 txLife 统计脚本/埋点未覆盖起源节点到提交，补上后再测。
3. **深挖 chain tx 全命中**（2.4）：为什么 100% 交易走 ChainPatch 跳过锁检查。对比历史正常实验（8/6 P50=6.9s）是否也全 chain。若历史不全是、这次全→DSN-44 或 PatchPool 逻辑引入。
4. **2.2× vs 4.5× 矛盾**：核实真实放大。可能需要看 result 到底怎么算唯一交易 vs 链上 SimTx。

## 4. 相关文件
- `ssc/tx_submitter.go`（DSN-44 heap/flight 重写，时延相关热点）
- `ssc/committer.go`、`ssc/state_locker.go`（BUG-12 已修复，勿动除非需要）
- `ssc/patchpool.go`、`ssc/retry_scheduler.go`（ChainPatch/DAG 链式机制）
- 分析脚本：远程 `logs/harmony-sscc/analyze-tx-life.py --leader`
- 实验数据：`RATE=150/HMY-SSCC/20260810_185643_result.txt`

## 5. 历史对照
- **8/6 实验**（历史正常基线）：P50=6.9s，P90=97s（那时锁泄漏在 shard1，尾部拖长）
- **8/10 185643**（本次）：P50=31.8s，P90=75s，globalLocked=0（锁已修，时延整体更平滑但 P50 更高，无慢尾杆）
- 注意 8/10 的 `total=10000` 与更早实验（total=100000）不同，对比口径要一致
