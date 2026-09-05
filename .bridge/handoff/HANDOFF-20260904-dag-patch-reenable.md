# HANDOFF-20260904-DAG-patch 重新启用：根因交接 + 新 session 聚焦

> 上一份：HANDOFF-20260903-cmh-chainonly-waitdie-off-summary.md
> 本 session 完成：VictimTx 实现、unfinished 根因定位（热 key 饥饿）、诊断打点、EnableDAG 默认开启、offChainDAG 单测修复。
> 现状：**unfinished 主因已定 = 低优先交易在同一热 key 上被 reservation「最高优先通吃」饿死；决定重新启用 DAG-patch（同 key 块内多次更新）来解。**
> 状态：**EnableDAG 默认开 + 配置已同步 199，实验待跑；新 session 聚焦评估与完善 DAG-patch 模块。**

---

## 0. 一句话交接
unfinished（~6-7k）**不是死锁、不是回滚失效**，而是：
**约 80% 低优先交易在同一把超高热度 on-chain key 上，被每块只放行最高优先一笔的 reservation 策略饿死（Wound-Wait/活锁）；约 17% 是跨分片半提交——某 related shard 一直抢不到（同上热 key 饥饿），导致 origin 凑不齐 commit。**
对策：**重新启用 off-chain DAG-patch**，让一个块内对同一 key 允许经 patch 链式多次更新，化解热 key 串行化/饥饿。

---

## 1. 根因证据链（本 session 实测，rate=200 shard=4 delay=10）
- 回滚链路通：VictimTx 上线后被判中 victim 都能 CXT_ROLLBACKED（5/5）。→ 不是回滚无效。
- CMH 覆盖极低：全 run 只 ~5 distinct victim；~90% unfinished 从没进 CMH waitEdge。
- 最远阶段直方图（6601 笔 unfinished）：
  - `respawn / call for retry` 5274（79.9%）= (b) retry 活锁
  - `send SSC commit vote` 1093（16.6%）= (c) 跨分片 commit 聚合卡死
  - 链上冲突层 ≈ 0（真死锁几乎不存在）
- 逐笔追（0x000b9e59… / 0x00124559…）：被 shard 上的热 key 挡住（合约 0x4030A3D0…/0xc2Ee93E2…），wound 管不到 on-chain 锁；进 retry 后只 `set signal` 不重跑。
- 诊断打点确认：reservation-skip 计数爆炸（shard1 37 万次）→ **低优先候选每块都被更高优先抢掉同 key 而跳过**。

---

## 2. 本 session 代码改动

### VictimTx（CMH 回滚，代码完成、未上运行验证）
`CRTx > VictimTx > SimTx` 类型层 + `DeadlockProbe.Proof` 证据链 + `VictimTx` payload/proto + submitter + `resolveRing` 改造成最后一跳 shard 提交 VictimTx + sscvm 执行让委员会投票回滚 + 桶内排序。本地编译通过（详见上一份交接/本仓库改动）。

### 诊断打点（已并入，可留可撤）
- `retry_scheduler.go AddToRetry`：`added to retry pool` 带 simulationNum + writeKeys/readKeys（前12）。
- `retry_scheduler.go OnBlockCommitted` reservation：`reservation skipped candidate (key already reserved by higher-prio)`。
- 新增 `previewLockKeys` 辅助（strings import）。

### EnableDAG 默认开启（本 session 收尾）
- `cmd/build_keys/build_keys.go` 默认 TimeoutConfig `EnableDAG: true`。
- 实验配置加 `enable_dag: true`：`test/configs/dev|local/shard=4_validator=4_ssc=1_delay=10_vpn=4/genesis_config_*.yaml`。
- 已 scp 到 199。⚠️ 若 run 用其它配置（不同 delay/shard/vpn），需各自补 `enable_dag: true`。

### offChainDAG 单测修复（重要！）
- `patchpool.go` 重构后 DAG **默认关闭**（`enabled=false` 零值），所有方法被 `isOn()` 短路成 no-op。
- 但 `offchain_dag_test.go` 全用 `&offChainDAG{}` 且从不 `SetEnabled(true)` → **7/8 单测全挂**（AddNode/MarkReady/findCoveringSet/深度 全空转）。
- 修复：加 `newTestDAG()`（构造即 `SetEnabled(true)`）并替换全部构造点 → 8/8 通过（199 上验证，需先移走 simulation_test.go / simulation_params_test.go 两个历史坏测试）。

---

## 3. DAG-patch 当前实现评估（新 session 起点）
`offChainDAG` 实现在 **`ssc/patchpool.go`**（不是独立文件），机制完整但默认关：
- 节点 = SimTx WriteSet patch + 上游依赖(DAG 边) + Depth + status(Free/Consumed)
- 索引：keyIndex / subscriber / txSubKeys
- 覆盖集选择：`findCoveringSet` / `isFullyCovered`，**严格 `<`（同轮不允许 chaining）** + 链深上限 `defaultMaxChainDepth`(=5，DSN-49/50 收敛用，防无限 churn/PoolTimeout)
- `readPatchChain` / `sim.IsKeyAvailable`：下游 SimTx 读上游已写值、不触发 on-chain 冲突 → **这正是「同 key 块内多次更新」的机制**
- `subscribeRetryTx/querySubscribers`：retry 池按 key 订阅、OnBlockCommitted 择优提升

### 既有测试（已修好全绿）
`TestOffChainDAGFindCoveringSet / RemoveCleansAll / MultiUpstreamConsume / MarkReadyTriggersScan / DepthComputation / IsFullyCovered / ChainDepthOf / DefaultMaxChainDepth`

---

## 4. 新 session 聚焦：评估与完善 DAG-patch

### 4.1 先跑 EnableDAG 实验，建立基线
- 199：`sync_code.sh uploadCode`（或已 scp 关键文件）→ 起一次 4-shard rate=200。
- 观察（关键判据）：
  - `PatchHit` 是否 >0（此前 DAG 关 PatchHit=0）→ DAG 真的在救；
  - `chainDepthDist` / `chainDepthCapped` / `retryAdd` / `sp1Started` / `PoolTimeout`——**千万别重蹈 DSN-49 的无限 churn / 链深过深**；
  - unfinished 是否下降、热 key shard 吞吐是否改善。
- 对照本 session 数据（DAG 关）：commit ~12k、unfinished ~6.6k、rollback ~8。

### 4.2 代码层要审的点
1. `sim.IsKeyAvailable` 的 patch 覆盖顺序 vs `retryCommit Phase2` 冲突门 —— DAG 开时冲突是否都先被 patch 覆盖再判死。
2. `findCoveringSet` 严格 `<` + `maxChainDepth`：确认同轮/链深限制真的挡住无限 chaining（对比 DSN-49 回归）。
3. **reservation 与 DAG 的衔接**：现在热 key 每块只放行最高优先一笔；DAG 让同 key 链式 SimTx 经 patch 同块推进后，reservation/subscriber 是否配合、会不会仍卡在「先拿到 key 的那一笔才放行」——**这是解热 key 的关键**。
4. `AddToRetry` 从 `SimulationCallStates[SimulationNum-1]` 提取订阅 key 的逻辑在「首轮成功但建 SimTx 时 TLV 冲突」与「多轮」下是否都把 LockedKeys 正确并入订阅（防“只 set signal 不重跑”复发）。
5. DAG 节点生命周期（AddNode→MarkReady→consume→release/Remove）与 closeTransaction 的清理是否无泄漏。

### 4.3 建议测试
- 单测：给 `Simulator.IsKeyAvailable` patch 链式读取补单测（覆盖 findCoveringSet → readPatchChain）；给「同 key 多 SimTx 同块」构造端到端小场景。
- 实验：rate 梯度（100/200/300）+ delay 梯度，重点看热 key 单点上 unfinished 降幅与链深是否受控。

---

## 5. 构建/运行
- 远程：`zjnu@10.7.95.199 -p 10022`（`-F /dev/null`）
- 编译：`export TOP=~/go/src/github.com/harmony-one && export CGO_CFLAGS="-I$TOP/bls/include -I$TOP/mcl/include" && export CGO_LDFLAGS="-L$TOP/bls/lib" && GOCACHE=/tmp/gocache GOFLAGS="-mod=mod -buildvcs=false" go build ./ssc/...`
- 单测：先移走 `ssc/simulation_test.go`、`ssc/simulation_params_test.go`（历史坏测试），跑 `go test ./ssc/ -run 'TestOffChainDAG|TestDetector_' -v`，跑完恢复。
- 本机 cache 只读，需 `GOCACHE=/tmp/gocache`。

## 6. 相关文档
- 本会话根因分析详见对话记录（热 key 饥饿结论）。
- `docs/designs/active/DSN-49-unify-offchain-patch-pool.md`、`DSN-50-ssc-dag-rescue-coverage-and-depth-cap.md`、`DSN-53-crossshard-deadlock-detection.md`（DSN-53 附录 D 临时边与当前代码不符，需修正/否决）。
- `docs/research/RSH-03-priority-aware-reverse-cmh.md`、`docs/research/analyze_unfinished.py`、`scripts/remote_ssc_retry_stats.py`。
