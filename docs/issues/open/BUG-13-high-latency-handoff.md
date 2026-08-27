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


---

## 6. 2026-08-11 新增：交易各阶段耗时分解结论（已用数据定型，不用重挖）

> 数据来源：`20260810_185643` 全量 99,662 笔 committed 交易，逐笔 join 客户端 txs.csv + 各 shard leader `[txLife]` 埋点。

### 6.1 阶段口径（先钉死测量边界）

| 阶段 | 起点 | 终点 | 是否被 `[txLife]` 覆盖 | 埋点 |
|---|---|---|---|---|
| ① pool→simulate | tx 入池 "Pooled" (zero log) | 模拟开始 (simulator_leader.go:103) | ❌ 未覆盖 | startCXT / "simulate cx transaction, start" (debug) |
| ② simToSubmit | 模拟开始 | SimTx 提交 | ✅ | `stage=0` → `[simTxSubmit]` |
| ③ submit→commit | SimTx 提交 | VerifySimulation 上链 | ⚠️ 恒0 | `[simTxSubmit]` → `[vsCommit]` |
| ④ commit→CRSubmit | SimTx 上链 | CR 提交 | ⚠️ 恒0 | `[vsCommit]` → CR submit |
| ⑤ crSubmitToCR | CR 提交 | CR 上链 | ✅ | CR submit → `[txLife]` |
| ⑥ totalLife | 模拟开始 | closeTransaction | ✅ | `[txLife]` |

**`[txLife]` 只覆盖 simulate→close；result 时延 = pool(add) → leader close。二者差 = ① 未被覆盖的 pool→simulate 段。**

**为什么 submitToCommit/commitToCRSubmit 恒 0（已定位根因）**：`ssc/tx_trace.go` 定义了 5 个 TraceStage，但 `recordTraceBlock` 在代码里只有 3 个调用点（`simulator_leader.go:103`=StageSimulateCX、`impl.go:910`=StageSimulationTxSubmit、`impl.go:1175`=StageCommitOrRollbackTxSubmit）。**StageSimulationTxCommit 和 StageCommitOrRollback 从未被调用** → `SimulationTxCommitTime`/`CommitOrRollbackTime`(via stage) 恒 0 → ③④ 恒 0。要补（`impl.go` close 处 `CommitOrRollbackTime` 是 `time.Now()` 直接设的，与 stage 无关，所以 totalLife 仍完整）。

### 6.2 全量对账结果（99,662 笔）

```
             result P50    txLife P50   pool→simulate 缺口 P50
shard 0        5.60s         4.36s           1.25s   ✅ 健康
shard 2        5.64s         4.25s           1.41s   ✅ 健康
shard 3       42.78s         4.29s          37.85s   ❌ 病态
shard 1       68.39s         7.83s          61.27s   ❌ 病态
```

**一句话结论**：时延瓶颈不在已埋点的任何阶段（模拟/上链/CR 全分片都正常，txLife 4.3-7.8s），而在 **① pool→simulate 这段完全未被 `[txLife]` 覆盖的服务端等待**。shard0/2 只等 1.3s（健康），shard1/3 等 38-61s（病态），占了各自总时延的 ~90%。

### 6.3 关键排除（已证伪的方向）

- **不是 cross_cnt/cross 深度**：shard1 即使 cross=1（单分片交易）也 约 61s；shard0 即使 cross=33 也只 0.5s。瓶颈是分片整体性，非单笔交易属性。
- **不是重试风暴**：simNum≥1 的仅 ~140 笔，且 CSV simulationNum 99.6% 为 0。
- **不是完成率/吞吐**：完成率 99.67%，各 leader vsCommit/simTxSubmit 全量正常推进（shard1 63k/63k 等）。
- **不是日志过滤**：debug 埋点齐全（"simulate cx transaction, start"=446k、"StartSimulateCXTransaction timing breakdown"=57k、"handle simulate request"=1.15M），无需补埋点即可定位 ①。

### 6.4 指向的根因（下一 session 验证方向）

**shard1/3 的 origin leader（9040/9120）在"从 txpool 捞交易 → 开始模拟"这一步被卡住/排队。** 与 §2.4 证据吻合：ChainPatch=317,315 远多于 AddPatch=63,337（5×），100% 交易被标 chain tx 跳过锁检查。怀疑 shard1/3 leader 的 chain-retry / DAG(ChainPatch) 调度使 leader 串行化，导致池内交易长期等不到模拟启动。

**验证方法（现有 debug 埋点即可，不用补）**：批量抽 shard1 vs shard0 每笔 tx 的 `Pooled`(zero log) 时间 + `simulate cx transaction, start`(leader log) 时间，本地 join 算 ① 分布；再对比 9040 vs 9000 的"开始模拟"事件到达节奏与耗散。

### 6.5 数据/文件
- 客户端 txs.csv 时延口径：`commit(add) 事件时间 − Pooled(add) 事件时间`（见 sscc_log_handle/data_handler.py:453-469）
- `[txLife]` 解析：远程 `logs/harmony-sscc/analyze-tx-life.py --leader`
- 本次分析临时文件：本地 `/tmp/afyg/txs.csv`(15MB,100k tx)、`/tmp/afyg/txlife_all.jsonl`(151MB,199k txLife 行)
