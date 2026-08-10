# BUG-12 Shard 1 锁泄漏导致跨分片时序放大（RATE=150）

> **状态**：待修复（open）
> **创建**：2026-08-08
> **涉及文件**：`ssc/verify.go`、`ssc/committer.go`、`ssc/state_lock_impl.go`（待定位）
> **关联**：`BUG-08`（TempLockView Recall 锁泄漏）、DSN-27（TLV→sync.Map）

---

## 现象

实验：`shard=4_validator=4_ssc=1_delay=10_rate=150_vpn=4`，`zjnu@10.7.95.199`，2026-08-06 21:12–21:26。

result（`.../RATE=150/HMY-SSCC/20260806_212827_result.txt`）：

```json
"committed": 69691, "timeout": 29530, "rollback": 1, "unfinished": 773
"latency": { "avg_ms": 30453.7, "p50_ms": 6883.99, "p90_ms": 97373.64,
             "p95_ms": 113488.49, "p99_ms": 116802.35, "max_ms": 123136.5 }
"per_shard_status": {
    "0": {"commit": 13622, "timeout": 9834},
    "1": {"commit": 27822, "timeout": 157, "unfinished": 773},
    "2": {"commit": 10446, "timeout": 7899, "rollback": 1},
    "3": {"commit": 17801, "timeout": 11640}
}
```

关键矛盾：**Shard 1 完成率 99.4%（27822/280），其他分片仅 58–60%**，同时整体 P90=97s、avg=30s。

---

## 根因（一句话）

**Shard 1 存在真实状态锁泄漏**：其 `globalLocked`（SLM 未释放 key 锁数）从 block 120 起无限增长，实验结束仍 **11,668** 个 key 锁死不放；`LOCK_STALE` 告警 **1,578,428 条（其他分片 6.6万–11.6万，约 14 倍）**，锁持有时间（`heldBlocks`）从 38 block 无限涨到 **307 block**（~10 分钟）不释放。

## 机制（时延高 + 仅 Shard 1 完成率高 = 同根因两面）

| 现象 | 机制（均有日志证据） |
|------|---------------------|
| Shard 1 完成率极高 | 它自己交易只是被堵在 **~9,800 个 SimTx 的池后排长队**（PoolTimeout≈100000 不超时），最终挤出提交。`totalGap` p50=35 / p90=58 blocks ≈ 70–120s/笔。 |
| Shard 0/2/3 完成率低 | 它们是受害者：发往 Shard 1 的跨分片交易验证时撞到死锁 key（`state is locked`），在 SP1 预算内过不了 → 超时回滚 `ReasonCxtTimeoutForSp1`。Shard 1 leader 处理 `rollback with proof origin:[false,0/2/3]` **29,131 条**，与 0/2/3 的 timeout 总数 **29,373** 几乎一一对应。 |
| 整体 P50=6.9s / P90=97s（双峰） | 快分片 0/2/3 提交仅 3–4 blocks（~6s）构成 P50；Shard 1 慢尾 35–58 blocks（~70–120s）构成 P90/P99/avg。 |

## 证据链

### E1. Shard 1 锁数单调增长、永不放空（核心）

`[retryScheduler] OnBlockCommitted stats` 的 `globalLocked`（每 40 块采样）：

```
Shard1(9040): blk40=226  120=1599 160=3565 200=5101 240=6386
              280=7733  320=9115 360=10203 400=11502  → 终态 11,668
Shard0(9000): blk40=648  120=1852 160=2421 200=2453 240=2919
              280=2734  320=2928 360=2880 400=887(回落)  → 终态 707
```

终态对比（`globalLocked`）：**Shard 0=707 / Shard 2=289 / Shard 3=132 / Shard 1=11,668**（40–90× 差异）。Shard 1 锁只加不减。

### E2. LOCK_STALE 随时间加速（正反馈）

`state_lock_impl.go:387` 的 `LOCK_STALE`（锁持有 >10 blocks 告警）按分钟统计（Shard 1 leader）：

| 时间 | 条数 | heldBlocks p50 | p90 | max |
|------|------|----------------|-----|-----|
| 21:17 | 23,195 | 38 | 48 | 58 |
| 21:18 | 71,574 | 52 | 72 | 88 |
| 21:19 | 108,053 | 70 | 98 | 118 |
| 21:20 | 143,063 | 86 | 125 | 148 |
| 21:21 | 171,717 | 103 | 152 | 178 |
| 21:22 | 200,548 | 118 | 180 | 208 |
| 21:23 | 230,498 | 133 | 207 | 238 |
| 21:24 | 256,957 | 150 | 235 | 268 |
| 21:25 | 283,220 | 164 | 263 | 298 |
| 21:26 | 89,603 | 173 | 282 | **307** |

锁持有时间无限增长 = **真实泄漏**（非瞬时 in-flight），且逐块恶化，是典型正反馈：锁死 key↑ → 冲突↑ → SimTx 积压↑ → 锁持更久↑ → 又锁死更多 key。

### E3. SimTx 池无界积压（泄漏后果）

`Block gas limit` 日志的 `pending`（SSC SimTx 排队数）：

- **Shard 1 leader**：blk30=358 → blk100=2318 → blk200=5750 → blk300=8363 → blk350=9826（峰值），至注入停止（~blk360）后才下降。
- Shard 0/2/3 leader：稳定在 **100–350**（注入≈提交，稳态）。

每个 Shard 1 SimTx 平均等 **35 blocks（p50）/ 58（p90）**（txLife `totalGap`）→ 对应 Shard 1 `totalLife` p50=70s。

### E4. 排除干扰假设

- **非 Shard 1 计算慢**：`commitTransaction timing breakdown` avg（p50）—— Shard1=882ms(301) 反而 4 分片最快，最慢是 Shard3=1356ms(347)。
- **非 origin 负载倾斜**：`handle simulate request, start` 各分片均衡（18378–29480），Shard1(29035) 非最高。
- 全部 1,578,428 条 stale 锁都落在跨分片 SSC 合约 **`0x4030A3D06cDB4237f8F3De615D1Ce41A5247f1cb`**，分布在 **15,142 个不同 txHash**（broad，非单热点 key）。Top txHash（如 `0x61b4...`）有 4,888 条 stale 标记但仅 ~3 条 `leader close` 事件 → 锁与完成事件严重不配平。

## 数据文件

- result：`/home/zjnu/go/src/github.com/harmony-one/ssc-cli/data_process/output/throughput/RATE=150/HMY-SSCC/20260806_212827_result.txt`
- 日志：`/home/zjnu/go/src/github.com/harmony-one/logs/harmony-sscc/shard=4_validator=4_ssc=1_delay=10_rate=150_vpn=4/`
- 分析 key：`[txLife]`、`LOCK_STALE`、`Block gas limit and usage info`、`[retryScheduler] OnBlockCommitted stats`、`commit with proof / rollback with proof origin:[*]`

---

## 已确认的根因（代码路径核实 2026-08-08）

对照 `docs/research/EXP-05-chain-lock-leak-analysis.md`，该文档已把锁释放链的脆弱点列为 P0/P1，但**这些修复至今未落地**，BUG-12 正是这些未修缺口的实际爆发。核实结果：

### ⚠️ 先回答：锁最终是否被清理？——没有，且确认为真泄漏（非"在途正常持有"）

**8/6 实验验证**（采样 60 个持有 stale 锁的 txHash 逐笔验证时序，shard1 leader 9040 日志）：

```
checked=60
  tx 从未有 CR/close: 0
  最终 CR/close 晚于最后 stale (锁已释放): 0
  CR/close 早于最后 stale (终结后仍锁着=泄漏): 60   ← 100%
```

单笔例证（tx `0x61b4903ec835`）：`rollback with proof` + `leader close` 均于 **21:17:10** 发生（交易已回滚终结），但 `LOCK_STALE` 自 **21:18:32 起**持续检测该锁 stale 到实验结束 **21:26:17**（4888 次）。

**8/8 实验完整复现（20260808_122218）**：

result：`committed=69712, timeout=29524`，Shard1 `commit=27860, timeout=133`（其余分片 timeout≈29k），`P90=101406ms`（比 8/6 的 97373 更高）。

Shard1 leader（9040）终态（blk 910-915 完全静止）：`globalLocked=12113` 恒定、`globalFinished=62671` 恒定、**`keyIndex=0`**。keyIndex=0 说明所有 lock→tx 关联已清空，但 globalLocked 仍有 12,113 个锁残留 → **纯死锁，无任何在途交易关联**。

LOCK_STALE 数量对比（8/8）：

```
shard_leader_9040: 7,697,090   (Shard 1)
shard_leader_9000:   138,765
shard_leader_9080:   109,350
shard_leader_9120:    73,589
```

Shard 1 比其他分片高 **55–100×**。再次采样 40 个 stale txHash：**40/40 = 终结后仍 stale（100% 泄漏）**，无一释放。

**结论**：持有 `globalLocked` ~12,113 锁的交易**绝大多数已 rollback/commit 终结**，锁却未随之释放，并持续被 LOCK_STALE 标记到实验结束 → **真泄漏，且 8/6、8/8 两轮稳定复现**。用户判断「交易长期未提交/回滚时锁该持有」在语义上正确，但本 case 交易已终结，锁不应再留存；确有一部分属"尾部在途"正常锁，但泄漏为主体（采样 100%）且持续放大（LOCK_STALE heldBlocks 单调涨）。

### 根因 1（P0 · 主因）— target 分片 RollbackTx→applyTo 跨块全局释放断点（8/8 深挖修正）

**⚠️ 修正先前判断**：最初怀疑 `committer.go:57 IsTxFinished` early-return 是唯一断点。8/8 实验逐笔因果链核实后，**主断点在"target 分片 rollback 的全局锁释放"**——超时交易的 rollback CR 正常处理了（rollback with proof + close），但锁未随回滚从全局移除。

**单笔证据链（tx `0xc69aba26c171`，Shard 1 target，origin shard2）**：

```
12:09:33  [rollback with proof] origin:[false,2]   ← 交易超时，Shard1 作为 target 收到回滚
12:09:33  [leader close] commit:false              ← 交易终结
12:09:35  [VerifySimulation] SUCCESS               ← 但 verify 仍报成功
12:10:39  [LOCK_STALE] 锁开始 stale                ← 回滚后锁仍在全局
12:36:44  LOCK_STALE 持续到实验结束                 ← 锁永久残留（7830 次）
```

**统计数据支撑**：泄漏的 40 个 stale txHash **全部是 `close:false + rollback` 的超时回滚交易**（非 commit 交易），且 rollback CR 均正常落链——确凿指向"回滚完成但锁释放失效"。

#### 代码断点精确定位（2026-08-08 深挖）

释放链跨块，两条腿：

```
verify 块(B):   Lock() → pendingStates.callIndex2lockedState[txHash][callIndex][key]
              块尾 locker.Commit() → merge 反向索引 → manager.lockedStates.callIndex2lockedState[txHash]
rollback块(B+K): RollbackTx(txHash) → 当前 locker.pendingStates 空（锁在 B 块已 merge）→ unlock 循环不删
              但 addTx(txHash) 执行 → pendingUnlocks 记录
              块尾 locker.Commit() → p.applyTo(mgr)
```

关键代码事实：

| 环节 | 位置 | 行为 |
|------|------|------|
| `unlock()` | state_locker.go:215-229 | **只从当前 locker 的 `pendingStates` 删锁**；跨块时 pendingStates 空 → 静默不删 |
| `RollbackTx` | state_locker.go:343-360 | 遍历 `s.pendingStates.getCallIndex2LockedStates(txHash)`（空）→ 循环不执行；**无条件 `addTx`**（line 372）|
| `applyTo` | state_locker.go:475-516 | line 483 `if c2ls := mgr.lockedStates.getCallIndex2LockedStates(txHash); c2ls != nil { ... Delete(lockKey) }` — **若反向索引为 nil，静默跳过删除全局锁** |

**断点**：`RollbackTx` 的 `unlock` 因跨块 pendingStates 为空而不删锁，全局释放完全押注在 **`applyTo` 依赖的 `manager.lockedStates.callIndex2lockedState[txHash]` 反向索引**。若该索引在 rollback 块时不可用（nil 或被 `state_lock_impl.go:488 delete` / `clear()` 提前清理），`applyTo` 的 `if c2ls != nil` 静默跳过 → **锁永久残留**，且无任何告警。

**EXPORT-05 未覆盖此点**（EXP-05 line 97 假设反向索引存在），属全新的 P0 断点。

**数据驱动验证缺口**：`applyTo` 无观测日志，无法从现有日志确认 rollback 时反向索引是否为 nil。需加一行诊断日志：`applyTo: txHash=X unlockedTxs=Y reverseIndexKeys=M`，下次实验观测 M==0 即确认。

### 根因 2（P0/1 并存）— `committer.go:57` `IsTxFinished` 提前 return 跳过释放（补充断点）

```go
if c.state.IsTxFinished(txHash) {
    return nil   // ← 跳过 CommitTx/RollbackTx
}
```

若交易已因其他路径提前 `tx.Closed=true`（如 PoolTimeout/ExecutionFailed），后续 CR 命中此分支跳过释放，**叠加断点**。此缺口本可被根因 1 的 applyTo 正确释放兜底，但根因 1 已断。

### 根因 3 — `verify.go:911` `lockStateWithRWSet` 静默吞错误（部分锁残留）

`SetAndLockState` error 被丢弃 → 部分 key 上锁成功残留。

### 根因 4 — `state_lock_impl.go:347-388` `LOCK_STALE` 只告警不兜底

一旦泄漏则永久留存、逐块累积 → Shard 1 `globalLocked` 涨到 12,113、`LOCK_STALE` heldBlocks 无限涨。

### 触发放大机制（Shard 1 独有）

三条未修缺口叠加 + Shard 1 作为跨分片 SSC 合约 `0x4030A3...` 的集中 target，形成正反馈：
```
SimTx 池积压 → CR 生产慢/target 等待超时 → 超时交易走 rollback CR（正常落链）
  → 但 target 分片 RollbackTx→applyTo 全局释放断（根因1）
  + IsTxFinished early-return 叠加断（根因2）
  + 冲突上锁部分成功残留（根因3）
  → globalLocked 累积、LOCK_STALE 无限增长（根因4）
  → 锁死 key 越多 → 0/2/3 target 交易超时回滚、Shard 1 自身 SimTx 排队更久 → 正反馈
```

## 修复建议（按优先级，待验证）

> ⚠️ **设计约束**：SSCC 链上锁只能通过**链上回滚/提交（CR）释放**，不能链下直接解锁（会破坏状态一致性）。因此修复必须**打通链上 rollback 的锁释放路径**，而非 ForceUnlockTx 之类链下强删。

1. **P0 · 修复 target 分片 `RollbackTx → applyTo` 跨块全局释放断点**：核实 why `applyTo` 没删掉已回滚交易的全局锁。嫌疑：`RollbackTx` 往当前块 `pendingUnlocks` 加 `txHash` 后，块尾 `stateDB.Commit()→locker.Commit()→applyTo` 依赖 `mgr.lockedStates.getCallIndex2LockedStates(txHash)` 反向索引删除 `globalLockedStates`，但该反向索引**在 verify 块 merge 后可能被 `clear()`/版本清理提前移错**。修复方向：
   - 让 `RollbackTx` 释放不依赖跨块反向索引——在 CR 处理当下即从 `globalLockedStates` 删除该 tx 的所有锁（`unlock` 直接操作全局），或
   - 保证 verify merge 写入的 `callIndex2lockedState` 与 applyTo 读取同源且不被提前清理。
   **这是阻断 12,113 死锁累积的关键，且完全符合"链上回滚释放"约束。**
2. **P0/1 · `committer.go IsTxFinished` 分支补链上回滚兜底**：命中 `IsTxFinished` 时仍需对已回滚交易执行全局锁 release（走链上状态，不链下强删）。
3. **P1 · verify.go `lockStateWithRWSet` 返回 error**（EXP-05 Fix 2）：成功路径检查 error，部分失败即 `RollbackTx(txHash)` + 回滚已上锁 key。
4. **P2 · LOCK_STALE 兜底**：`handleLockCommit` 对 `staleLocks` 超阈值做告警 + 触发链上回滚复核（确认该交易是否已终结），切断正反馈。
5. 对 target 分片 CR 处理路径做 **lock acquire/release 配平断言**（每 lock 必有且仅有一次释放）。
