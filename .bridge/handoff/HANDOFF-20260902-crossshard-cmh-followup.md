# HANDOFF-20260902-crossshard-cmh-followup

> from_session: 20260902（DSN-53 反向 CMH v1 已落地，rate=200 仍不收敛 → 交接排查）
> from_role: Developer
> to_role: Developer (next session)
> 状态：**代码已实现 + leader 门控已修，但 deadlockDetector 仍未检测到任何环，需继续排查。**

---

## 0. 一句话现状

DSN-53 的反向 CMH v1（`deadlockDetector` + 探针 RPC + BLS + 触发点）已经能编译、能记录少量 waitEdge、
能发送/转发探针；但 rate=200 下 `deadlockRingDetected=0 / deadlockVictimDied=0`，`unfinished` 依旧很高，
说明**检测覆盖率或 waitEdge 生命周期仍不匹配真实死锁**。

## 1. 最近一次实验（rate=200，2026-09-02 ~18:08，shard=4_validator=4_ssc=1_delay=10）

per-shard 结果：

```
Shard 0: {'rollback': 288, 'commit': 2967, 'unfinished': 1162}
Shard 1: {'commit': 2590, 'rollback': 516, 'unfinished': 2745}
Shard 2: {'commit': 2509, 'rollback': 260, 'unfinished': 608}
Shard 3: {'commit': 3887, 'rollback': 598, 'unfinished': 1868}
```

日志 dir：
`~/go/src/github.com/harmony-one/logs/harmony-sscc/shard=4_validator=4_ssc=1_delay=10_rate=200_vpn=4/`

关键计数（全 shard 汇总）：

| 计数 | 值 |
|---|---|
| `waitEdge recorded` | 11 |
| `previous waitEdge stale, dropped` | 8 |
| `no valid outgoing waitEdge, dropped` | 17 |
| `deadlock ring detected` | 0 |
| `ring victim chosen` | 0 |
| `deadlockProbeInvalidSig` | 0 |
| `retryCommit failed: stateDB lock conflict` (Phase2) | ~5 |
| `retryCommit failed: try lock failed` (Phase1) | ~530 |

## 2. 已经做对 / 已修

1. **DSN-53 v1 完整实现**（M0–M3）已落地并编译通过：
   - `ssc/deadlock_detector.go`（新）
   - `ssc/api/*`（LockLayer / DeadlockProbe / Ack + proto/convert）
   - `ssc/comm.go`、`rpc/ssc_grpc.go`（新 RPC）
   - `ssc/retry_scheduler.go`、`ssc/verify.go`、`ssc/impl.go`（触发点 + victim rollback）
2. **已修 leader 门控**（关键）：原来 `verify.go` 兜底触发在**每个 SSC member** 上都记 waitEdge，
   而跨分片探针只发给 **leader**，leader 的 waitEdges 为 0 → 探针全被丢。已改成 leader-only。

   ```
   go build ./ssc/... ./core/... ./cmd/... ./node/...
   go vet ./ssc/... ./rpc/...
   ```

## 3. 当前卡点 / 观察（下个 session 重点）

### 3.1 触发覆盖太窄
- 修复后 leader 上总共才 **11 条 waitEdge**（9040/9080/9120 各几条，9000 = 0）。
- Phase2 真实 on-chain 冲突日志只有 ~5 条，`OnChainLockConflict` 返回路径 0 次。
- Phase1 TLV fail ~530 次，但其中绝大多数是“低优先 wait / 持有者未 Finalized”，按设计**不应触发**。

→ 说明当前严格触发（仅链上/已 Finalized + 高优先）能捕捉到的“确认被卡”边很少；
  真实剩下的 unfinished 死锁可能主要是 **低优先级永不重新验证 / TLV→链上过渡 / winner-ring 变种**，
  严格门没有覆盖到，或者根本没有走到 Phase2 判定死锁的那一步。

### 3.2 `previous waitEdge stale, dropped` 立即发生
- leader（如 9040）在 `waitEdge recorded` 同一毫秒内就出现：
  ```
  [deadlockDetector] previous waitEdge stale, dropped
  ```
- 说明 `HandleProbe` 里的 `edgeValid` 对刚记录的本地边**立刻判 stale**。
- 怀疑点：
  - `edgeValid` 只查 **global** SLM（`GetLockHolderMeta` / `GetRLockHolder`），
    看不到 **pending/同块** 持有者 → 对 pending 冲突的边会立刻判失效。
  - 或该边本来就基于瞬时/即将被 CRTx 释放的锁，被 Cleanup / release 抢先。
  - `edgeValid` 对“同一 shard 初始 hop”可能过于严格，把本应能前进的探针直接掐断。

### 3.3 主触发点没有发挥主力作用
- `RetryCommit` Phase2 只在**冲突 key 能查到 global holder**时才记 waitEdge，
  没有像 `dsn52ShouldYield` 那样回退查 **pending holder**（`FindPendingLockHolder`），
  也没有读锁 pending 回退。
- 结果：很多链上冲突（尤其同块 pending）记不出 waitEdge → 触发面很窄。

## 4. 下个 session 建议排查顺序

1. **统一 holder 查询 helper**（最关键）
   - 新写（或复用）一个“找链上/pending 写/读持有者”的 helper：
     `global write → global read → pending write → pending read`，返回 `(holder, pri, finalized)`。
   - 三处都用它：
     - `retry_scheduler.go` Phase2 触发
     - `verify.go` 兜底触发
     - `deadlock_detector.edgeValid` / 记录前的持有者判定
   - 对齐 `dsn52ShouldYield` / `woundLowerPriorityHolders` 的既有查询逻辑。
2. **搞清 `previous waitEdge stale` 为什么立刻发生**
   - 给 `edgeValid` 失败加 reason 日志（哪个 key / holder 不在 global? pending? patch covered? wounded?）。
   - 对“本地刚记录且是同 shard 初始 hop”放宽/跳过重验证，先让探针能前进，避免自掐。
3. **补触发覆盖的日志埋点**
   - `shouldTriggerCMH` 拒绝时打 debug：低优先 wait / patch covered / TLV 未 Finalized / holder 未知。
   - `RetryCommit` Phase1 失败时打 holder 是否 Finalized、priority 比较结果。
   - Phase2 冲突时打“查得到 holder? global or pending?”。
   - 用这些判断“为什么 530 次 TLV fail 都没进 waitEdge / 为什么 Phase2 只有 5 次”。
4. **评估放宽触发范围**
   - 若验证后确认剩余死锁在 TLV 低优先/未 Finalized 一侧，考虑 DSN-53 §9 提到的
     “TLV 普通低优先边作为可选预警开关”（默认严格，实验期打开观察），或把“低优先永不重新验证”也纳入。
5. 重新压测后按 §1 指标对比：
   - `unfinished → 0`、`rollback` 有界
   - `deadlockRingDetected / deadlockVictimDied > 0`
   - leader 上 `deadlockWaitEdges` 稳定非 0、`deadlockProbeSent > 0`

## 5. 环境 / 复现 / 构建

- 远程：`zjnu@10.7.95.199 -p 10022`（必须 `-F /dev/null`）
- 日志：上述 rate=200 目录，`ssc-validator-*.log` 才是 SSC 日志。
- 本地验证：`GOCACHE=/tmp/gocache GOFLAGS=-mod=mod go build ./ssc/... ./core/... ./cmd/... ./node/...`
- `go vet ./ssc/... ./rpc/...`

## 6. 参考
- `docs/designs/active/DSN-53-crossshard-deadlock-detection.md`（实现向主文档）
- `.bridge/handoff/HANDOFF-20260902-crossshard-cmh-impl.md`（v1 落地交接）
- `docs/research/RSH-03-priority-aware-reverse-cmh.md`（论文向）
