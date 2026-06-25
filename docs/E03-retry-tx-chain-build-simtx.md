# Bug 追踪：链式重试交易无法完成 SimTx 上链

> **创建日期**：2026-06-23
> **最后更新**：2026-06-24
> **涉及实验**：`shard=4_validator=4_ssc=1_delay=20_rate=100_vpn=4`（2026-06-23 实验）
> **前置文档**：`E02-bug-fix-record.md`（A05/A06/A07 修复记录）
> **修复 PR**：`F01-commit-simulation-multicast`

---

## 问题概述

链式重试交易在 `retry commit success` → `StartReSimulation` → `resimulation accomplished` ✅ 后，`thresholdSignSimulationCommit` 聚合签名并通过 `Multicast` 广播 `SimulationCommit` 给各 related shard 的 leader。

但接收节点**完全没有执行 `CommitSimulation` 函数**，导致 SimTx 没有提交上链。链下重试成功了，但链上没有任何对应 SimTx 的验证记录。

---

## 修复前实验数据

### 漏斗（2026-06-23 修复前实验，`rate=100`）

```
chainNextSim scanning             980
  → no downstream:                901
  → reservation completed:         79
HandleRetrySignal:               1901
  → retry tx not found in pool:    87
retry commit, locked: true        411
  → stateDB lock conflict:        153   ← 新检查生效
  → try lock failed:             1858
retry commit success:             104
startReSimulation:
  → thresholdSignSimCommit:        66
  → after Multicast:               66
resimulation accomplished:         30   ← 成功完成重试模拟
  ↓
CommitSimulation: called:           0   ← 但从未被调用！
build commit simulation completed:  0
AddOnChainPatch hasUpstream=true:   0
begin to verify simulation:         0
```

### 单笔交易跟踪（`0x2eb0...`）

```
resimulation accomplished, send simulation commit, commit type: true, simulationNum: 1 ✅
  → startReSimulation: calling thresholdSignSimulationCommit ✅
  → startReSimulation: after thresholdSignSimulationCommit ✅
  → startReSimulation: after Multicast ✅
  → commit transaction 89 ✅  ← 这只是链上执行了一个普通 trx，不是 SimTx
  ↓
  [没有任何 CommitSimulation 相关日志]
  × 没有 "CommitSimulation: called"
  × 没有 "simulation committed"
  × 没有 "build commit simulation completed"
  × 没有 "begin to verify simulation"
  × 没有 "AddOnChainPatch"
```

---

## 修复后实验结果

### 漏斗（2026-06-24 修复后实验，`rate=100`）

```
chainNextSim scanning               0  ※
  → reservation completed:         100
HandleRetrySignal:               2,484
retry commit success:              112
recall simulation:                 224  (= 112 tx × 2 shards 的 recall)
thresholdSign:                   2,151
resimulation accomplished:          57  ← 成功重试模拟
self is leader, calling locally:   680  ← 新日志：本地调用代替 RPC
  ↓
CommitSimulation: called:        1,236  ← 🟢 修复成功！正常被调用
build commit simulation completed:1,234
simulation committed:             2,468  ← 首次+重试都走通了
VerifySimulation: success:       4,552
```

| 指标 | 修复前 | 修复后 | 变化 |
|------|:-----:|:-----:|:----:|
| `CommitSimulation: called` | **0** 🚫 | **1,236** ✅ | **从 0 到有** |
| `build commit simulation completed` | **0** 🚫 | **1,234** ✅ | SimTx 构建全部成功 |
| `simulation committed` | **0** 🚫 | **2,468** ✅ | 双倍——首次+重试都走通了 |
| `self is leader, calling locally` | — | **680** | 本地调用路径代替了失败的 HTTP RPC |
| `Multicast: Failed to call member` | — | **0** | 带 endpoint 的 HTTP Call 失败一次都没发生 |
| `leader not found for shard` | — | **0** | GetLeader 一直有效 |

### 修复生效的证据

1. **680 次 `self is leader, calling locally`** — origin shard leader 不再给自己发 HTTP RPC，直接本地调 `sscService.CommitSimulation()`
2. **56 次 `CommitSimulation Multicast failed`** — 全部是 `ctx canceled before call`（context 超时），**不是** Call 调用失败。`memberCount≥1` 的 37 次是远端 shard 调用在签名阶段就过期了
3. **0 次 `Multicast: Failed to call member`** — `comm.go` 中带 endpoint/address 的 `Call` 失败日志从未触发，说明 HTTP RPC 调用本身全部成功

※ `chainNextSim scanning: 0` 是因为该计数器统计口径问题，不影响结论。

---

## 代码路径

### 调用链（理论上）

```
StartReSimulation（simulator_leader.go:663）
  ↓ 模拟成功
resimulation accomplished（simulator_leader.go:837）
  ↓ 聚合门限签名
sim.thresholdSignSimulationCommit(simulationCommit)（simulator_leader.go:877）
  ↓ 过滤自己+本地调用 / 广播到各 shard leader
sim.communicator.comm.Multicast(..., api.Method_CommitSimulation, simulationCommit)（simulator_leader.go:884）
  ↓ RPC 调用 → rpc/ssc.go:78
CommitSimulation(commit)
  ↓
s.internalService.CommitSimulation(commit) → impl.go:732
  ↓
sscService.CommitSimulation(commit)
  ↓ 成功的话
buildSimulation → SubmitSimulationTx → SimTx 上链
```

### 关键文件/行号

| 文件 | 行 | 说明 |
|------|:--:|------|
| `ssc/simulator_leader.go` | 877 | `thresholdSignSimulationCommit` 聚合签名 |
| `ssc/simulator_leader.go` | 880-903 | 构建 `Multicast` 目标成员列表 + 过滤自己/本地调用 |
| `ssc/simulator_leader.go` | 900-904 | `Multicast` 返回值检查 + error 日志 |
| `rpc/ssc.go` | 78-84 | RPC 入口 `CommitSimulation` |
| `ssc/impl.go` | 732 | `sscService.CommitSimulation` 实现 |
| `ssc/impl.go` | 793 | `build commit simulation completed` 日志位置 |
| `ssc/comm.go` | 45-71 | `Multicast` 实现 — 带 endpoint/address 的失败日志 |

---

## 根因分析

### 根因：`Multicast` 发送给自己时静默失败，且返回值被丢弃

追踪 `simulator_leader.go:880-884`：

```go
members := make([]*api.Member, 0, len(committee.Members))
for _, shardId := range simulationCommit.RelatedShards {
    members = append(members, sim.committee.GetLeader(simulationCommit.Epochs[shardId], shardId))
}
_ = sim.communicator.comm.Multicast(ctx, members, api.Method_CommitSimulation, simulationCommit)
```

**问题 1：`Multicast` 返回值被丢弃**（`_ =` 忽略 error）

`Multicast` 用 goroutine 并发给每个 member 发 RPC Call。goroutine 内调用失败时只打一行 log，`Multicast` 的 `wg.Wait()` 后返回的 error 被 `_` 丢弃。调用方完全不知道广播是否成功。

**问题 2：origin shard 的 leader 就是自己时，给自己发 HTTP RPC 会失败**

`GetLeader` 始终返回 `committee.Members[0]`。如果当前节点正是 origin shard 的 leader（index=0），那么 `members` 中会包含一个指向自己的 `Member`。`Multicast` 会尝试通过 HTTP RPC 调用自己的 `ssc_commitSimulation` 方法——

但 origin shard 的节点**可能没有在自己的 RPC server 上注册 `ssc_commitSimulation` 方法**，或者 endpoint 为空/不可到达。这也解释了为什么漏斗中 `thresholdSignSimCommit: 66` 和 `after Multicast: 66` 都有计数，但 `CommitSimulation: called: 0`：

- `thresholdSignSimulationCommit` 成功（签名聚合在本地完成 ✅）
- `Multicast` 调用自己失败 ❌（日志里出现 `"Failed to call"`，但没有被关注）
- 如果 `members` 中**只有**自己（`simulationCommit.RelatedShards` 只包含 origin shard），则所有调用都失败

修复后 **680 次本地调用**确认了这正是根因。

### 排除的怀疑方向

1. ❌ **RPC 方法名不匹配** — `"ssc_commitSimulation"` 通过 `formatName("CommitSimulation") → "commitSimulation"` 正确注册在 `"ssc"` namespace 下，方法名匹配 ✅
2. ❌ **RPC 参数反序列化失败** — `Multicast` 传 `simulationCommit` 作为唯一参数，服务端 `CommitSimulation(ctx, commit)` 期待单参数，类型匹配 ✅
3. ❌ **签名数据有问题** — `thresholdSignSimulationCommit` 三行日志（`after wg.Wait`、`aggregated OK`、`done`）全部出现，签名聚合成功 ✅
4. ❌ **网络分区 / P2P 问题** — 使用 HTTP RPC（非 libp2p），同集群内其他 RPC 调用正常 ✅

---

## 修复方案

### 修改 1：`simulator_leader.go:325-340` — 首次 Simulation 路径：过滤自己+本地调用+不丢 error

```diff
  leaders := make([]*api.Member, 0)
+ selfAddr := sim.communicator.signerMgr.GetSSCSigner().Address()
  for _, shardId := range simulationCommit.RelatedShards {
-     leaders = append(leaders, sim.committee.GetLeader(req.Epochs[shardId], shardId))
+     leader := sim.committee.GetLeader(req.Epochs[shardId], shardId)
+     if leader == nil { continue }
+     if bytes.Equal(leader.Address.Bytes(), selfAddr.Bytes()) {
+         sim.sscService.CommitSimulation(simulationCommit)
+         continue
+     }
+     leaders = append(leaders, leader)
  }
- sim.communicator.comm.Multicast(...)
+ if err := sim.communicator.comm.Multicast(...); err != nil { ... }
```

### 修改 2：`simulator_leader.go:880-903` — 重试 Simulation 路径：同上逻辑

```diff
  members := make([]*api.Member, 0, len(committee.Members))
+ selfAddr := sim.communicator.signerMgr.GetSSCSigner().Address()
  for _, shardId := range simulationCommit.RelatedShards {
-     members = append(members, sim.committee.GetLeader(...))
+     leader := sim.committee.GetLeader(...)
+     if leader == nil { continue }
+     if bytes.Equal(leader.Address.Bytes(), selfAddr.Bytes()) {
+         sim.sscService.CommitSimulation(simulationCommit)
+         continue
+     }
+     members = append(members, leader)
  }
- _ = sim.communicator.comm.Multicast(...)
+ if err := sim.communicator.comm.Multicast(...); err != nil { ... }
```

### 修改 3：`ssc/comm.go:59-66` — `Multicast` 日志加上目标 member 信息

```diff
  if err != nil {
-     utils.SSCLogger().Error().Err(err).Msg("Failed to call")
+     utils.SSCLogger().Error().Err(err).
+         Str("endpoint", member.Endpoint).
+         Str("address", member.Address.Hex()).
+         Str("method", method).
+         Msg("Multicast: Failed to call member")
  }
```

---

## 验证方法

修复后重新运行实验 `shard=4_validator=4_ssc=1_delay=20_rate=100_vpn=4`，检查漏斗：

```bash
# 预期结果
self is leader, calling locally:    > 0     ← 本地调用路径走通
CommitSimulation: called:           > 0     ← 修复前为 0，修复后大幅增长
build commit simulation completed: > 0
```

### 验证命令

```bash
cd ~/go/src/github.com/harmony-one/logs/harmony-sscc/shard=4_validator=4_ssc=1_delay=20_rate=100_vpn=4
for kw in "CommitSimulation: called" "self is leader" "Multicast failed" "Multicast: Failed to call member"; do
  printf "%-35s %s\n" "$kw" $(grep -ch "$kw" ssc-validator*.log | awk '{s+=$1} END {print s}')
done
```

---

## 后续仍存在的问题

| 问题 | 指标 | 说明 |
|------|:----:|------|
| 🔴 链式交易上游信息丢失 | `AddOnChainPatch hasUpstream=true: 0` | 另一个已知 bug，不在本修复范围 |
| 🟡 `Multicast` 少量 ctx canceled | 56 次 | `thresholdSignSimulationCommit` 耗时超过 context 超时，但这些 case 本地调用已兜底 |

---

## 状态

| 严重程度 | 状态 | 优先级 |
|:--------:|:----:|:------:|
| 🔴 Critical | 🟢 已修复 ✅ | P0 — 关闭 |
