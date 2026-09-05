# HANDOFF-20260902-crossshard-deadlock-detection

> from_session: 20260902（跨分片死锁：定位「赢家环」→ 提出优先级感知反向 CMH 探针方案）
> from_role: Designer
> to_role: Designer (next session)
> 状态：**进行中** —— 方案设计已成型（DSN-53 + RSH-03），**尚未实现/验证**。新 session 优化设计并落地实现。

---

## 0. 一句话现状

rate=200 跨分片交易：Wait-Die 分层已解决大部分死锁（globalLocked 500→~14、unfinished 大幅下降），
但仍有**残留「赢家环」**：~14 个最高优先部分赢家互相等，挡住 ~1400 笔（因「全分片 Ready 聚合门」
卡死低优先者 die）。已提出**优先级感知反向 CMH 探针**方案（DSN-53 代码向 / RSH-03 论文向），
新 session 需优化设计并落地实现。

---

## 1. 问题根因（已定位，结论）

```
跨分片交易需所有 related 分片确认 → 逐个上锁 → 部分赢家（某分片锁成功、另一分片被卡）
多个部分赢家形成跨分片死锁环（A 等 B 在 shardX，B 等 A 在 shardY）
wait-die：低优先者本应「重新验证时 die」，但 HandleReSimulationSignal 要求
  readyCnt == len(RelatedShards)（全分片 Ready）才重新验证
→ 低优先者被更高优先挡在某分片 → 该分片不发 Ready → 永不重新验证 → 永不 die → 环破不掉
```

**关键**：不是超时/性能问题，是「全分片 Ready 门」把 wait-die 的 die 触发点卡死了。
所以方案不能用超时（论文不友好），要用确定性触发。

---

## 2. 已确认的设计方向：优先级感知反向 CMH

### 核心（与用户对齐的关键决策）
- **等待边（反向）**：`A 被 B 卡` = 高优先 A 在分片 X 被低优先 B 锁阻塞。
- **触发（高优先发起）**：仅「高优先被低优先卡」时发起探针（低优先 wait 是正常行为，不触发，避免探针风暴）。
- **传播（优先级定向剪枝）**：探针沿「高优先被低优先」链走，用全局全序剪枝，多数提前停。
- **victim（环内最低优先）**：探针带路径回发起者 → 环内选全局全序最低者 die（终局 Rollback CRTx）。

### 与原版 CMH 的区别（论文要写清）
1. 触发者反向：高优先被卡者（非低优先等待者）发起。
2. 等待边方向反向：`A 被 B 卡`（非 `A 等 B`）。
3. 传播用全局全序剪枝：非无差别沿等待边。
4. 与 wait-die 分层绑定。
5. 场景性完备：检测「高优先被低优先」环；纯低优先环靠 wait-die die 兜底。

### 为什么不用超时
- 触发条件是「存在更高优先直接阻塞者」这一确定性事实（全局全序下所有节点一致），非时间。
- 超时方案有性能上限、论文不友好（用户明确要求避免）。

### 为什么不是宽松放行
- 宽松放行（任何低优先被挡就 die）会导致大量回滚（之前 Wait-Die 版本 rollback 2000-3500）。
- 严格（只 die 环内成员）回滚有界。CMH 探针能精确选环内 victim。

---

## 3. 已交付文档

| 文档 | 类型 | 内容 |
|---|---|---|
| `docs/designs/active/DSN-53-crossshard-deadlock-detection.md` | 代码向设计 | 数据结构（waitEdge/DeadlockProbe，含 BLS）、锁语义定义（SLM/TLV/DAG-patch）、通信与安全、消息结构、流程、改动点表、验证计划 |
| `docs/research/RSH-03-priority-aware-reverse-cmh.md` | 论文向研究 | Problem/Related Work/方法/锁语义/通信与 BLS 可信证明/消息结构/与原版差异/正确性/复杂度/实验/Contribution（v2） |
| 本 handoff | 交接 | 现状 + 方向 + 下一步 |

---

## 4. 下一步（新 session）

### 4.1 设计决策（本轮已优化定稿，直接进入实现）
- [x] **探针消息格式 + RPC 通道**：`DeadlockProbe`（+可选 `DeadlockProbeAck`）走 `SSCCrossService`
      gRPC（`comm.Call`）；`comm.go` 新增 `Method_DetectDeadlockProbe`。结构见 DSN-53 §4.2。
- [x] **安全通信**：每个转发 hop 由发送分片 SSC ≥threshold 成员聚合 BLS 签名（`BaseBLSSignedMessage`），
      接收方 `GetSSCSigner().Verify`；`Epoch+Nonce+BlockNum` 防重放。victim die 仍走正常投票/证明协议（防御纵深）。
- [x] **锁语义厘清（链上 vs 链下）**：链上 `stateLockManager`（SLM，随链持久，`CheckLock`=被锁）、
      链下 `TempLockView`（TLV，leader 本地瞬态，`TryLockWithPriority locked=false`=被锁，`wounded` 恒为 false 需再查持有者）、
      DAG patch（被覆盖的 key **不算阻塞**）。waitEdge 只记录未被 patch 覆盖的等待边，主目标是链上边。见 DSN-53 §3.3。
- [x] **触发点主次（用户定稿）**：**主力 = 链下 `RetryCommit`**（Phase 1 持有者已 Finalized / Phase 2
      `OnChainLockConflict`，严格门在构建 SimTx 前识别「A 高优先被低优先 B 的链上锁卡住」）；
      **兜底 = 链上 `VerifySimulation`**（仅 leader 轮换/TOCTOU 罕见场景）。**两处都要判断**是否触发。
      触发条件严格：仅「A 高优先 且 被更低优先 B 的链上/已 Finalized 锁阻塞」才触发；低优先 wait / 可 wound
      的 TLV 边 / 被 patch 覆盖的 key 均不触发。见 DSN-53 §3.2/§3.3.4/§5.1。
- [x] **N-环 vs 2-环**：探针带 `Path`，直接支持 N-环（victim = 环内全局全序最低者）；2-环为特例。
- [x] **victim die 落地**：victim 的 origin 广播非 ReleaseOnly 终局 Rollback CRTx（复用 `sendRollbackVoteForDie` /
      `CXTCommitProof`），不破坏投票协议；不是单节点改锁表。
- [x] **完备性声明**：场景性完备（高优先被低优先构成的环）+ 纯低优先环靠 wait-die 兜底。
- [x] **waitEdge 生命周期 / stale**：建立 = `shouldTriggerCMH` 通过且 patch 失败；每跳/成环前重验证；
      清理 = holder commit/die 时各分片 `CleanupHolder`（挂在 `OnBlockCommitted`）。见 DSN-53 §5.4。
- [x] **探针路由**：发到 holder 的 `RelatedShards` 多播（只有持有出边 waitEdge 的分片继续）；可优化为反查索引。见 DSN-53 §5.1。
- [x] **读锁 holder**：Phase 2 / `shouldTriggerCMH` 同时覆盖写锁 `GetLockHolderMeta` 与读锁 `GetRLockHolder`。
- [x] **模块归属**：新组件 `ssc/deadlock_detector.go`，由 `retryScheduler` 持有。见 DSN-53 §4.3。
- **设计已定稿**：锁语义 / 触发主次 / BLS 通信 / 消息结构 / waitEdge 生命周期均完成，进入落地实现（DSN-53 §10）。

### 4.2 落地实现（新 session，按 DSN-53 §10 里程碑 M0→M3）
- **M0 数据结构与 RPC 骨架**
  - [ ] `ssc/api/types.go`：`LockLayer` + `DeadlockProbe`/`DeadlockProbeAck` + `Bytes()`。
  - [ ] `ssc/api/proto/ssc.proto` + convert/grpc：新增消息 + `DetectDeadlockProbe` RPC。
  - [ ] `ssc/comm.go`：注册 `Method_DetectDeadlockProbe` → `deadlockDetector.HandleProbe`。
- **M1 `deadlockDetector` 组件（先本地闭环）**
  - [ ] 新建 `ssc/deadlock_detector.go`：`waitEdges`/`seenProbes` + `OnBlocked`/`HandleProbe`/`CleanupHolder`/`Stats`。
  - [ ] 实现 `shouldTriggerCMH`（写锁+读锁+patch 覆盖排除）+ 探针本地传播（剪枝/重验证/选 victim）。
  - [ ] `retryScheduler` 持有并注入依赖（state/comm/tempLockView/offChainDAG/stateLockManager）。
- **M2 触发点接线**
  - [ ] `retry_scheduler.go`：Phase 1（TLV Finalized）/ Phase 2（OnChainLockConflict）失败调 `OnBlocked`。
  - [ ] `verify.go`：链上兜底冲突分支（`dsn52ShouldYield==false`）调 `OnBlocked`。
  - [ ] `OnBlockCommitted`：见 CRTx 调 `CleanupHolder`（跨分片清理）。
- **M3 跨分片闭环 + 验证**
  - [ ] 多播路由 + BLS 背书/验证（复用 signer）。
  - [ ] 跑 §8.1 构造场景（2-环/3-环/路由/stale/读锁）+ §8.2 安全测试。
  - [ ] rate=200 压测 + 监控计数 + 与宽松 wait-die 对比（§8.3）。

### 4.3 验证（DSN-53 §8 / RSH-03 §6）
- [ ] rate=200：`unfinished→0`、`rollback` 有界（只 die 环内成员）。
- [ ] 监控：`probe sent/dropped/invalid-sig/ring detected/victim died`。
- [ ] 安全测试：伪造探针（错 BLS / 单节点签名 / 篡改 Path / 重放）必须被拒；合法委员会签名通过。
- [ ] 构造 2-环/3-环/读锁/路由/stale 单元场景，验证探针闭环 + victim 为环内最低优先。
- [ ] 与「宽松 wait-die」对比 rollback。

---

## 5. 环境 / 复现
- 远程 199：`ssh -F /dev/null -p 10022 -i ~/.ssh/id_ed25519 zjnu@10.7.95.199`（必须 `-F /dev/null`）。
- 日志：`/home/zjnu/go/src/github.com/harmony-one/logs/harmony-sscc/shard=4_validator=4_ssc=1_delay=10_rate=200_vpn=4/`
  （`ssc-validator-*.log` 才是 SSC 日志）。
- 聚合：`scripts/local-ssc-retry-stats.py`、`scripts/local-txstages.py`。
- 关键日志：`OnBlockCommitted stats`（retryPool/globalLocked/globalFinished）、`mu conflict`（conflictSource）、
  `VerifySimulation success`、`LOCK_STALE`、`DSN-52 ...`。

## 6. 构建 / 坑
- 构建：`GOCACHE=/tmp/gocache GOFLAGS=-mod=mod go build ./ssc/... ./core/... ./cmd/... ./node/...`。
- `go test ./ssc/` 预编译失败（simulation_test.go 旧 API，无关）。
- 当前代码已演进到「链上 Wait-Die + 链下 TLV wound + 严格门 + DAG 排序」（§17 分层），
  方案 1（CommitSimulation 占 TLV 锁）**已证明无效**（lock-fail=0），可考虑回退或保留。
- 工作区已有大量改动，提交点：`7ffa092d9`（本 session 前段）；方案 1 与文档尚未提交。

## 7. 关键文件速查
- `ssc/verify.go`：`dsn52ShouldYield`(738)、`sendRollbackVoteForDie`(915)、冲突分支。
- `ssc/retry_scheduler.go`：`RetryCommit`、`tryToReSimulation`、`HandleReSimulationSignal`（Ready 门）、`OnBlockCommitted`。
- `ssc/state_lock_impl.go`：`GetLockHolderMeta`/`GetTxPriority`（优先级查询，已具备）。
- `ssc/impl.go`：`CommitSimulation`（方案 1 所在）。
- 设计：`DSN-52`（§17 分层）、`DSN-53`、`RSH-03`。
