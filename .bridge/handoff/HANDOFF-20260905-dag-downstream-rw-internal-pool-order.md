# HANDOFF-20260905: DAG 下游读写上游 + Internal Pool 保证“上游先于下游”序

> 交接给新 session。目标两项（先设计文档后实现）：
> **A. 确保允许“下游 SimTx 修改上游锁住/写出的状态”**（下游可读写上游）。
> **B. 在 Internal Pool 里保证 SimTx 顺序满足“上游先于下游”，降低因找不到 Patch 而 Rollback 的交易。**

## 0. 一句话背景
从 rate=200 排查 unfinished 至今的链条：
- DSN-56(去 ChainPatch) + DSN-57(上游就绪门，**已 REVERTED**，因饿死跨分片下游回归) + DSN-58(ready 聚合 needed/sscVote + reservation 反饥饿)。
- DSN-58 已消除跨分片 ready 死锁/冻结：实测 `committed 11916 / rollback 2631 / unfinished 5453`，CMH `ring_detected=0`、retryPool 单调下降（不再一字不变）。
- 残留 unfinished 主要是一大批写同一热 key 的交易，链只建到 depth 2–3（`chainDepthCapped=0`，非 depth cap 瓶颈），多数块 `dagPerBlock sameKey=1` → 退化成同 key 串行 reservation，慢。
- DSN-59（draft）曾设想“积极成批续链”，**被否**：交易进来无读写集，只能模拟/撞锁后才知道依赖；把一堆同 key tx 绑成长链，任一环失败整链崩，负收益放大。→ 改走下面 A+B 两条更稳的路。

## 1. 待办任务（本次 session 定稿，交给新 session 完成）

### Task A：确保“下游可修改上游锁住/写出的状态”（Part A of DSN-59）
- 目标：链式下游 SimTx 应可**读上游写出的值**，并且**继续写**（含写上游也写过的 key）。基于上游 patch 值构建自身 SimTx，verify `isChainTx` 跳过锁冲突、靠 DAG 全序 + 块内顺序保证“上游先执行，下游读其写再写”。
- 现状代码点（`ssc/verify.go`）：链式一致性只校验下游 **ReadState** 里被上游写过的 key（`ReadOnChainDAGPatch`）；下游 **WriteState** 未做显式“接着上游写同一 key”的续链判定，只靠 DAG 顺序。
- 要做：
  1. **先判据**（可选项，但建议先做）：抽 Shard1 头号热 key 那 ~667 笔，判断第 n+1 笔是否把第 n 笔(该 key 上一写者)放进 `UpstreamTxList`、且其 Read/Write 覆盖第 n 笔 Write 的 key → 确认这批是不是真“读-写依赖”。
  2. 让 DAG 救援/匹配把下游 **Read∪Write** 都作为“需被前序上游覆盖/续接”的 key（不只 Read）。
  3. verify 对下游 **Write** 命中上游的 key 也做一致/记录；不变量：同 key 写按 DAG 全序可**同块连续落多笔**，由块内顺序保证正确。
- 参考：`docs/designs/active/DSN-59-dag-downstream-rw-and-eager-chainmatch.md` §3。

### Task B：Internal Pool 保证 SimTx 序“上游先于下游”，减少“找不到 Patch→Rollback”
- 目标：让同一 key/依赖链的下游 SimTx 在**上链/验证时上游已存在(patch 可命中)**，从而减少 DSN-57 verify fail-closed 式的“upstream patch not on-chain → rollback”。
- ⚠️ **必须避开 DSN-57 的坑**：DSN-57 曾用“上游已上链才放行”在 internal_pool 扣留 not-ready 下游，实测**饿死跨分片下游→回归**并 REVERTED（见 `DSN-57-simtx-upstream-ready-admission.md` 状态）。新 session 做序时须：只保证**能确定的顺序**（同批/同 key/真依赖），绝不把跨分片下游长期扣死。
- 现状：`ssc/internal_pool.go` 已有 `orderSimTxsByDAG`（同批内被依赖者先于依赖者）；不足是跨批/跨块、跨分片时下游可能在“上游 patch 未注册”时被放行。
- 要做（方向待新 session 细化）：在 Internal Pool 的放行/Extract 阶段，对**能确定前序依赖**的 SimTx 保证其上游已注册/已入批，控制 not-ready 的扣留范围与超时，避免跨分片下游饿死；量化“因找不到 patch 而 rollback”是否下降。
- 参考：`DSN-57-simtx-upstream-ready-admission.md`（为何被回滚）、`DSN-59` §4/§5 讨论。

## 2. 现有设计/代码状态（工作区已含）
- `docs/designs/active/DSN-58-crossshard-ready-quorum-liveness.md`：ready 聚合 needed/sscVote + reservation 反饥饿（已实现，见下）。
- `docs/designs/active/DSN-59-dag-downstream-rw-and-eager-chainmatch.md`：Task A/B 的草案底稿（含“积极链被否”的原因）。
- 代码改动（已 `go build ./ssc/... ./rpc/...` 通过，位于工作区，未确认是否已 commit，新 session 先 `git status` / `git diff` 确认）：
  - `ssc/retry_scheduler.go`：DSN-58 —— `reservationSkipCnt`+50 块反饥饿；`needingShards`/`readyAmongNeeding`/`MaybeTriggerReSim`；`tryToReSimulation` 只扇出 needing；`LegSscVoted` accessor；`antiStarvationAdmit` 计数。
  - `ssc/impl.go`：`LegSscVoted` 接线（读 `commitStates.CommitSSCVotes`）；`HandleCXTCommitSSCVote` 收到 Commit sscVote 后调 `MaybeTriggerReSim`。
  - `ssc/verify.go`：DSN-57 确定性 fail-closed（只查 on-chain `onChainDAGPatches`，不依赖 leader 内存 internal_pool）。
  - `ssc/internal_pool.go`：注释同步（同批 DAG 序职责）。
- 分支：`ssc_master_58aae5802`。

## 3. 验证环境/基线（远端 199）
- 远程：`zjnu@10.7.95.199 -p 10022`（key `~/.ssh/id_ed25519`，须 `-F /dev/null -i ... -o IdentitiesOnly=yes`），见 AGENTS.md §6.1。
- 结果：`~/go/src/github.com/harmony-one/ssc-cli/data_process/output/throughput/RATE=200/HMY-SSCC/20260905_225441_result.txt`（DSN-58 后基线：11916/2631/5453）。
- 日志：`~/go/src/github.com/harmony-one/logs/harmony-sscc/shard=4_validator=4_ssc=1_delay=10_rate=200_vpn=4/`
- 本地脚本：`scripts/remote_ssc_retry_stats.py`、`scripts/local-ssc-retry-stats.py`；分析临时脚本在 `/tmp/rem_*.py`（本机沙箱 /tmp 可能有残留，但远程环境临时文件在远端 /tmp，可能已被清理，需重传）。
- 诊断要点（Task A 判据 / Task B 效果）：
  - `dagPerBlock maxSameKeyWrites`（同 key 一块被写几次）、`chainDepthDist`、`chainDepthCapped`；
  - `VerifySimulation: upstream patch not on-chain yet -> rollback (DSN-57, deterministic)` 计数（Task B 要压这个）；
  - `CHAIN_RETRY_STATS`：`chainReadyAdmission`、`retryCommitPatchHit/Miss`、`antiStarvationAdmit`；
  - `reservation skipped` 的 conflictKey 分布 / 头号热 key 上 distinct tx。

## 4. 工作方式
- 按仓库规范：先更新/新建 `docs/designs/active/DSN-*.md`（设计文档）再实现；每步 `go build ./ssc/... ./rpc/...`。
- 编译/部署/跑实验按 AGENTS.md（`sync_code.sh`、`go_executable_build.sh`、`test_single`/`auto_test.sh`）；部署前校验二进制新鲜度。
- 分析日志勿裸 grep，用脚本聚合（见 §3）。
- 每个实验后把 `*_result.txt` 的 per-shard committed/rollback/unfinished 与上基线对比，判断是否改善/回归。

## 5. 关键风险/约束提醒（避免重蹈）
1. **DSN-57 已 REVERTED**：Task B 做“上游先于下游”时若在 internal_pool 长期扣留跨分片下游会饿死(unfinished↑)，必须先想清“确定序 vs 不饿死”的边界与超时。
2. **积极长链被否**：Task A 是“允许下游读写上游”，不是把一堆同 key tx 绑成长链成批并行——任一环失败整链崩。A 的价值是让“真依赖的短链”能被救起、少 rollback，不是无脑并行同 key。
3. 读写集只在模拟/撞锁后才有，方案不要在“无读写集”的入队阶段假设依赖。
