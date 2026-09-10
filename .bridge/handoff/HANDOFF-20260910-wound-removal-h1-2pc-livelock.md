# HANDOFF-20260910: 彻底移除 wound + H1 全分片 2PC 上锁/构建 + 死锁已消、剩活锁

> 承接 HANDOFF-20260909-nowound-ab-and-onchain-lock-deadlock.md。
> 本 session：① 彻底移除 wound（含模拟层链下 wound）；② 定位并修复 H1（构建侧“半单上链”）——
> 改成「上锁唯一/提前/由 origin 统一协调，Phase B 只构建不抢锁」；③ 跑 RATE=200 实测：
> **链上锁泄漏(LOCK_STALE) 从十万级 → 0，死锁链被切断；剩余未完成以“预约层活锁/长尾”为主**。
> ④ 明确下一步：分析并发 tryToReSimulation 之间互相抢 TLV 锁（预约层活锁）问题。

---

## 0. 一句话结论
- **wound 已彻底移除**（TLV 抢锁 + 模拟层“忽略低优先锁”都关掉；日志 wound=0）。
- **H1 已修复**：构建前必须全分片 TLV 预留成功（Phase A，origin 统一 ACK），Phase B 只构建、不再抢锁；
  首建(`StartSimulateCXTransaction`)与重试(`StartReSimulation`)两条路径统一。`first-sim failed to acquire TLV lock`
  从 **9779 → 0**。
- **死锁/链上锁泄漏已消除**：4 leader `LOCK_STALE = 0`（此前十万级）。
- **剩余未完成 = 活锁/预约层饥饿 + 长尾**：settled(38min) 提交 **85.87%**、未完成 **2823**、回滚 3；
  未完成平均 `simulationNum=4.20`（已提交仅 0.46），`reservation-skip` 高达 107 万~326 万次/leader。

---

## 1. 本 session 做了什么

### 1.1 彻底移除 wound
- `ssc/temp_lock_view.go`：`canWound` 恒 `false`（TLV 永不抢占）。
- `ssc/simulator.go`：删除 `isLowerPriorityLock` / `lowerPriorityLock`，并移除 `GetState/SetState/IsKeyAvailable`
  里“撞到更低优先级持有者的锁 → 忽略该锁继续模拟/无锁读”的分支。
  原因：模拟层“忽略锁”的前提是“之后 TLV 会 wound 抢到锁”；wound 关掉后若仍忽略，会产生幻影 RWSet
  （高优反复模拟出抢不到锁的写集）→ 反复重试。
- 证明（本轮日志）：`Wound: high priority tx took lock` = 0；`retryCommitWounded*`/`onChainWound` = 0。

### 1.2 H1 修复：全分片 2PC（上锁唯一、提前、origin 统一协调）
H1 = 一笔跨分片交易部分分片建了 SimTx 并上链、另一些分片没建 → 已上链腿成了 partial winner。
根因（实测）：`CommitSimulation` 在 Phase B **又独立抢了一次 TLV**，抢不到就静默 `CallForRetry`，
origin 不知道 → 兄弟腿照上链。**实测 9779 次** `first-sim failed to acquire TLV lock`。

改动：
- **新增 RPC `ReserveLegCommit`**（`ssc/api/sscs.go` / `ssc/comm.go` / `rpc/ssc_grpc.go` /
  `ssc/api/proto/ssc_grpc.pb.go` 手写补 stub —— 入参复用 `SimulationCommit`、出参复用 `RetryCommitResp`；
  `ssc/impl.go` 实现）：每分片 leader 用本腿 RWSet `TryLockWithPriority(TLV)`，**只预留、不建单**，返回 `Locked`。
- **origin 统一 Phase A** `reserveAllLegsCommit`（`ssc/simulator_leader.go`）：并发让全 related 分片预留，
  全部 `locked` 才放行；任一失败 → 对已预留分片发 `RetryCancel` 释放 + 整单回 `SimulationNum+1`。
- **Phase B `CommitSimulation` 只构建不抢锁**（`ssc/impl.go`）：只校验本腿 TLV 已被本 tx 持有；
  持有 → 构建提交；未持有 → 拒绝构建（不再二次抢锁、不再静默 CallForRetry）。
- **两条路径统一**：`StartSimulateCXTransaction`（首建）与 `StartReSimulation`（重试）都先走
  `reserveAllLegsCommit` 再构建。
- Phase A 与 Phase B 用**同一套 `BuildCallStates` 推导 key**，保证“锁的 key 集 = 构建 key 集”。

### 1.3 代码清单
```
M rpc/ssc_grpc.go              ReserveLegCommit gRPC server adapter
M ssc/api/proto/ssc_grpc.pb.go 手写补 ReserveLegCommit stub（未跑 protoc）
M ssc/api/sscs.go              Method_ReserveLegCommit + ShardService 接口
M ssc/comm.go                  reserveLegCommit 路由
M ssc/impl.go                  ReserveLegCommit 实现 + Phase B 只构建不抢锁
M ssc/simulator.go             删除模拟层链下 wound
M ssc/simulator_leader.go      reserveAllLegsCommit + 首建/重试统一 Phase A 门控
M ssc/temp_lock_view.go        canWound=false
```

---

## 2. 实验与证据（RATE=200 / TX=20000 / shard4 / vpn4 / delay10）

| 版本 | 提交 | 未完成 | 回滚 | 说明 |
|---|---|---|---|---|
| 仅去 wound（in-loop 快照） | 12580 (62.9%) | 7405 | 14 | wound 非病根 |
| H1 2PC v1（仅首建、Phase B 仍抢锁） | 12700 (63.5%) | 7288 | 12 | `first-sim failed to acquire TLV lock`=9779 |
| **H1 2PC v2（本 session，统一+只构建）** | **17174 (85.87%)** | **2823** | **3** | 见下：settled 38min |

v2 关键验证（4 leader 合计）：
- `first-sim failed to acquire TLV lock` = **0**（v1 为 9779）
- `2PC PhaseB: leg TLV verified held` = 19260；`leg TLV not held` = 0
- `reserveAllLegsCommit: all legs reserved` = 12926；`not all legs locked` = 4912（整单回滚不半单）
- **`LOCK_STALE` = 0**（4 leader 全部，38 分钟后重拉仍为 0）→ 无链上锁泄漏/partial-winner 挂死
- `reservation-skip` = 177 万 / 326 万 / 108 万 / 127 万（预约层饥饿）
- 未完成平均 `simulationNum=4.20`（已提交 0.46），长尾到 19+

抽查一笔“热 key 持有者”（`0x0279daca…:0x64…`，holder `0x5d8ca3c3…`）：
- 该 tx 三条腿(shard0/1/2)均 `[vsCommit] success @simNum=1`，origin 收齐 3 票（`receive all related ssc votes=1`），
  且 `sscType=CRTx`、`closeTransaction commit=true` → **实际已提交**；只是 data_handler 快照(15:16:55)
  早于其 commit(15:17:12) 17 秒，被记成 unfinished。
- 说明 in-loop 的 unfinished 大量是**长尾晚到提交**，不是永久卡死。

---

## 3. 当前结论
- **死锁/链上锁泄漏已消除**：`LOCK_STALE=0`；v2 的“Phase A 不齐就整单不建”切断了
  “部分腿上链 → 锁永不释放(CRTx 不来) → 挡死下游”的死锁链。
- **剩余未完成 = 活锁/预约层饥饿 + 长尾**：交易在 `reservation skipped` 上反复让位/重试
  （skip 随重试涨、simNum 增），拿不到热 key 的 TLV 预留 → 永不推进；但**没有谁占锁不放**（不是死锁）。
  其中 simNum≤2 的约 2440 笔可能只是慢，simNum≥3 的约 380 笔是硬活锁。

---

## 4. 新 session 任务（建议顺序）

### 4.1 首要：并发 tryToReSimulation 之间互相抢 TLV 锁
- 现象：`reservation-skip` 百万级、未完成 simNum 高企；多笔交易（或同一交易的多次重试）
  并发走 `tryToReSimulation → RetryCommit/ReserveLegCommit`，在热 key 上互相抢 TLV、
  抢输者整单回退再抢 → 活锁。
- 要查：
  1. 是否存在**同一 tx 的并发 `tryToReSimulation`**（`reSimInFlight` 是否真正防住所有入口）；
  2. 多笔 tx 并发 Phase A 预留同一热 key 时，是否**先 lock 全分片成功、又因某分片锁输而整单 RetryCancel**，
     导致“锁了又放、放了再抢”的抖动（thundering herd）；
  3. `reservationAntiStarvationBlocks` 的 force-admit 是否真的能打破饥饿，还是反而制造更多并发抢锁。
- 方向：预约层**公平性/反饥饿**（按等待时长提优先级、同热 key 强制排队、或把同一热 key 的 Phase A 串行化），
  而不是再动锁释放。

### 4.2 遗留：H1 的“强保证”还差 Phase C（build-ACK + 准入闸）
- 现状：origin 只掌握 Phase A `locked`，Phase B（CommitSimulation）是 fire-and-forget、**没有 built ACK**。
  本轮 `Multicast failed=0`、`leg TLV not held=0`，所以没观测到 H1；但 leader 切换 / 广播丢失 /
  未处理构建异常时，仍可能出现“某腿没建、兄弟腿上链”。
- 若要“H1 代码级保证不发生”：加 Phase C —— 各分片回报 `built=true`，origin 收齐才放行上链；
  任一未 built → 整单释放。

### 4.3 遗留：partial winner 逃生/池超时被禁用（死锁若复发要用）
- `SignCXTSimulation` 一签 SimTx 就 `removePoolTx`（`impl.go:764`，全仓唯一调用点）；
- `handleTxPoolTimeout` 见 `HasVerifyContext`（有腿已上链）直接 return，不回滚。
- 结果：partial winner 被排除在超时/回滚之外 → 若再出现链上锁泄漏，没有释放路径。
  本轮 v2 已不再产生 partial winner，故未触发；但这是“死锁兜底”的已知缺口。

---

## 5. 风险 / 注意
- `ReserveLegCommit` 的 gRPC stub 是**手写补进 `ssc_grpc.pb.go`** 的，未跑 protoc；
  下次正规重生成 proto 时需把 `ReserveLegCommit` 补进 `ssc.proto`，否则会被覆盖。
- `ssc/api/types.go.bak` 是临时备份，**不要提交**。
- 实验快照口径：`test_single` 在 send-end +60s 就 download+data_handler，unfinished 会被长尾高估；
  判断真实终局需**固定 settle 窗口（如 +20~30min）后重拉重算**（本 session 用 38min → 85.87%）。

## 6. 运行 / 复现
- 远程：10.7.95.199（controller），worker 200-203；实验用 `run_nowound_round.sh`（= `source conda cli-py; . auto_test.sh; test_single`）。
- 日志：`/home/zjnu/go/src/github.com/harmony-one/harmony-sscc/tmp_log/shard=4_..._rate=200_vpn=4/`；
  下载副本在 `/home/zjnu/go/src/github.com/harmony-one/logs/harmony-sscc/...rate=200_vpn=4/`。
- 关键统计脚本：`ssc/scripts/remote_ssc_retry_stats.py`（注意大文件较慢）；主要埋点：
  `LOCK_STALE` / `reservation skipped` / `first-sim failed to acquire TLV lock` /
  `2PC PhaseB: leg TLV verified held` / `reserveAllLegsCommit: all legs reserved`。

## 7. 代码状态
- 本 session 的改动 = 上述 8 个文件（见 §1.3）+ 本 handoff。
- 未提交：`ssc/api/types.go.bak`（忽略/删除）。
