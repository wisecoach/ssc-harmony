# HANDOFF-20260902-crossshard-cmh-impl

> from_session: 20260902（跨分片死锁：反向 CMH 方案**设计已定稿**，交给实现）
> from_role: Designer
> to_role: Developer (next session —— 落地实现)
> 状态：**设计定稿，待实现**。本文档是新 session 的**实现交接**，按 DSN-53 §10 里程碑 M0→M3 执行。

---

## 0. 一句话现状

残留「赢家环」死锁的解决方案——**优先级感知反向 CMH 探针**（DSN-53 代码向 / RSH-03 论文向）
已完成设计定稿：锁语义（SLM/TLV/DAG-patch）、触发主次、BLS 可信通信、消息结构、
waitEdge 生命周期（路由/重验证/清理）全部敲定。**下一步：按 DSN-53 §10 落地实现。**

---

## 1. 设计已定稿的关键点（实现前必读）

### 1.1 触发主次（用户定稿）
- **主力 = 链下 `RetryCommit`**：Phase 1（TLV 持有者已 Finalized）/ Phase 2（OnChainLockConflict，
  严格门在构建 SimTx 前识别「A 高优先被低优先 B 链上锁卡住」）。
- **兜底 = 链上 `VerifySimulation`**：仅 leader 轮换导致 TLV 未协调 / TOCTOU 等罕见场景。
- **两处都要判断**是否触发 CMH。
- **触发条件（严格）**：仅「A 高优先 且 被更低优先 B 的链上/已 Finalized（不可 wound）锁阻塞」才触发。
  低优先 wait、可 wound 的普通 TLV 边、被 DAG patch 覆盖的 key 均**不触发**。

### 1.2 锁语义（三套 + 一层）
- **SLM**（链上，随链持久）：`CheckLock`=被锁；写锁 `GetLockHolderMeta` / 读锁 `GetRLockHolder`。
- **TLV**（链下 leader 本地）：`TryLockWithPriority` 失败时 `wounded` **恒为 false**；
  `locked=false` 必须再查持有者区分“低优先 wait（不触发）” vs “高优先被低优先 Finalized 卡（触发）”。
- **DAG patch**：被覆盖的 key **不算阻塞**（依赖边，非等待边）。
- **waitEdges**：把 SLM/TLV/DAG 翻译成「确认被卡的等待图」的轻量派生视图（非冗余，见 DSN-53 §3.3.5）。

### 1.3 通信与安全
- 探针走 `SSCCrossService` gRPC，逐跳由**发送分片 SSC ≥threshold 成员聚合 BLS 签名**背书；
  接收方 `GetSSCSigner().Verify`，失败丢弃。`Epoch+Nonce+BlockNum` 防重放。
- victim die 复用 `sendRollbackVoteForDie` → 正常投票聚合 → 非 ReleaseOnly 终局 Rollback CRTx（不新增消息）。

### 1.4 waitEdge 生命周期（正确性要求）
- **建立**：`shouldTriggerCMH` 通过且 patch 补救失败后记录。
- **路由**：发到 holder 的 `RelatedShards` **多播**，只有持有出边 waitEdge 的分片继续，其余丢弃。
- **重验证**：每跳/成环前用当前 SLM/TLV/DAG 事实确认边仍成立，stale 即丢。
- **清理**：holder commit/die（CRTx）时，所有记录过“B 是 holder”的分片 `CleanupHolder(B)`（挂 `OnBlockCommitted`）。

---

## 2. 参考文档
| 文档 | 用途 |
|---|---|
| `docs/designs/active/DSN-53-crossshard-deadlock-detection.md` | **主实现文档**：结构、流程、改动点、验证、§10 里程碑 |
| `docs/research/RSH-03-priority-aware-reverse-cmh.md` | 论文向（v3，与定稿对齐） |
| `.bridge/handoff/HANDOFF-20260902-crossshard-deadlock-detection.md` | 上一份交接（背景/根因/环境） |
| `docs/designs/active/DSN-52-crossshard-deadlock-resolution.md` | 前置分层（Wait-Die + TLV + DAG）背景 |

---

## 3. 实现任务（新 session，按 DSN-53 §10）

### M0 —— 数据结构与 RPC 骨架
- [ ] `ssc/api/types.go`：`LockLayer` + `DeadlockProbe` / `DeadlockProbeAck` + `Bytes()` 确定性序列化。
- [ ] `ssc/api/proto/ssc.proto`：`DeadlockProbe` / `DeadlockProbeAck` + `DetectDeadlockProbe` RPC（SSCCrossService）。
- [ ] `ssc/api/proto/*.go`：重新生成或手写 convert + grpc stub。
- [ ] `ssc/comm.go`：注册 `Method_DetectDeadlockProbe` → `rs.deadlockDetector.HandleProbe`。

### M1 —— `deadlockDetector` 组件（先本地闭环、可单测）
- [ ] 新建 `ssc/deadlock_detector.go`：`waitEdges` / `seenProbes` 表。
- [ ] 方法：`OnBlocked` / `HandleProbe` / `CleanupHolder` / `Stats`。
- [ ] 实现 `shouldTriggerCMH`（写锁 `GetLockHolderMeta` + 读锁 `GetRLockHolder` + patch 覆盖排除）。
- [ ] 实现本地传播：Current→m 查表、优先级剪枝、stale 重验证、成环选 victim。
- [ ] `retryScheduler` 持有 `deadlockDetector`，注入依赖（state/comm/tempLockView/offChainDAG/stateLockManager）。

### M2 —— 触发点接线
- [ ] `ssc/retry_scheduler.go`：Phase 1（TLV Finalized）/ Phase 2（OnChainLockConflict）失败 → `OnBlocked`。
- [ ] `ssc/verify.go`：链上兜底（`dsn52ShouldYield==false` 高优先侧）→ `OnBlocked`。
- [ ] `OnBlockCommitted`：见 CRTx 调 `CleanupHolder`（跨分片清理）。

### M3 —— 跨分片闭环 + 验证
- [ ] 多播路由（按 `GetTxMeta(B).RelatedShards`）+ BLS 背书/验证（复用 signer）。
- [ ] 跑 DSN-53 §8.1 构造场景（2-环/3-环/路由/stale/读锁）+ §8.2 安全测试。
- [ ] rate=200 压测 + 监控计数 + 与宽松 wait-die 对比（§8.3）。

---

## 4. 代码改动点 / 复用速查

| 文件 | 动作 |
|---|---|
| `ssc/deadlock_detector.go` | **新增**：独立组件 |
| `ssc/api/types.go` | 新增消息类型 + `Bytes()` |
| `ssc/api/proto/ssc.proto` / `*.go` | 新增 RPC / 消息 |
| `ssc/comm.go` | 注册探针 RPC |
| `ssc/retry_scheduler.go` | 主力触发 + 持有 detector + `OnBlockCommitted` 清理 |
| `ssc/verify.go` | 兜底触发 |
| `ssc/signer.go` | 复用 `GetSSCSigner().Aggregate/Verify`（无需改） |
| `ssc/state_lock_impl.go` | 复用 `GetLockHolderMeta` / `GetRLockHolder` / `GetTxMeta`（无需改） |
| `ssc/committer.go` | 复用终局 Rollback CRTx（无需改） |

**复用已有**：`GetLockHolderMeta`（写锁）、`GetRLockHolder`（读锁）、`GetTxMeta`（RelatedShards）、
`GetTxPriority` / `api.Priority.Less`（全序）、`GetSSCSigner().Aggregate/Verify`（BLS）、
`sendRollbackVoteForDie`（die）、`GetTempLockHolder` / `IsWounded`（TLV）、
`offChainDAG.isPatchFinalized` / `isFullyCovered`（patch）。

---

## 5. 验证（DSN-53 §8 / RSH-03 §6）
- [ ] rate=200：`unfinished→0`、`rollback` 有界（只 die 环内成员）。
- [ ] 监控：`probe sent`、`probe dropped`、`probe invalid-sig`、`ring detected`、`victim died`。
- [ ] 安全：伪造探针（错 BLS / 单节点签名 / 篡改 Path / 重放）必须被拒；合法委员会签名通过。
- [ ] 构造：2-环 / 3-环 / 路由多播 / stale 清理 / 读锁冲突，验证闭环 + victim 为环内最低优先。
- [ ] 与「宽松 wait-die」对比 rollback。

---

## 6. 环境 / 复现
- 远程 199：`ssh -F /dev/null -p 10022 -i ~/.ssh/id_ed25519 zjnu@10.7.95.199`（必须 `-F /dev/null`）。
- 日志：`/home/zjnu/go/src/github.com/harmony-one/logs/harmony-sscc/shard=4_validator=4_ssc=1_delay=10_rate=200_vpn=4/`
  （`ssc-validator-*.log` 才是 SSC 日志）。
- 聚合：`scripts/local-ssc-retry-stats.py`、`scripts/local-txstages.py`。

## 7. 构建 / 坑
- 构建：`GOCACHE=/tmp/gocache GOFLAGS=-mod=mod go build ./ssc/... ./core/... ./cmd/... ./node/...`。
- `go test ./ssc/` 预编译失败（simulation_test.go 旧 API，无关）。
- **实现硬约束**：Phase 2 `OnChainLockConflict` 必须先按 `shouldTriggerCMH` 区分高低优先，**不能见冲突就触发**。
- **读锁**也要查 holder（别只查写锁 `GetLockHolderMeta`）。
- **TLV `wounded` 返回值恒为 false**，两种 `locked=false` 要再查持有者区分。
- 提交点参考：`7ffa092d9`（上一 session 前段）；DSN-53/RSH-03/本 handoff 尚未提交。

## 8. 超出本期 / 后续优化
- holder→blocking-shard 反查索引（替代 RelatedShards 多播）。
- TLV 普通低优先边作为可选预警开关（默认仅链上边）。
- 同 (waiter,holder) 探针签名缓存 / 批量共用一次 BLS 聚合，降低背书延迟。
