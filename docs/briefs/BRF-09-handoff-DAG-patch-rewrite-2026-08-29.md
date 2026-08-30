# HANDOFF — DAG Patch 方案总结：DSN-49/50 已实施但未解决超时，决定重写整个 DAG patch 方案

> **日期**：2026-08-29
> **项目**：`/mnt/E/gowork/src/github.com/wisecoach/harmony-sscc`
> **远程**：`zjnu@10.7.95.199 -p 10022` → `~/go/src/github.com/harmony-one/harmony-sscc`
> **远程日志**：`~/go/src/github.com/harmony-one/logs/harmony-sscc/`
> **基于**：`docs/briefs/BRF-08-handoff-DAG-monitor-fixes-DSN49-2026-08-28.md`
> **本次提交**：`53a5b5232`（checkpoint，含 DSN-49/50 实现 + BRF-08 全量 SSC 重构）
> **分支**：`ssc_master_58aae5802`

---

## 一、一句话总结

本 session 实现了 **DSN-49（合并 localPatches+patches 为单一 offChainDAG）** 和 **DSN-50（救援前覆盖完整性 + 链深上限）**，但两轮 rate=300 实验表明**成功率不升反微降（57.77%→55.47%）、churn 不降（retryAdd 10.4万→10.6万）**。核心结论：**当前 DAG patch 方案不是超时的解药，问题根源是“高锁竞争 + 重试机制无节制”，而不是 DAG 本身。用户决定：新 session 直接重写整个 DAG patch 方案。**

---

## 二、本次已实现并提交（commit 53a5b5232）

### ① DSN-49：链下 Patch 池统一为单一 `offChainDAG`
- `ssc/patchpool.go`：新增 `OffChainPatchNode`（= 原 ChainPatchNode + UpstreamTxList）与 `offChainDAG`（nodes/keyIndex/subscriber/txSubKeys）。
- 删除 `retryScheduler.localPatches` 与 `patches`，替换为 `offChainDAG` 一个字段；`onChainPatches` 保持独立。
- 写入合并：impl.go `AddPatch`+`addPatch`/`OnPatchPoolUpdated` → `AddNode`（无条件）+ `MarkReady(txHash, onReady)`（leader 建 keyIndex + 通过回调触发 `scanPatchSubscribers`）。
- 读取统一：`readPatchChain`/`GetChainPatchRef`/`GetUpstreamTxRef` 全读 `offChainDAG.nodes`；`sendChainSignal` 走 `AddNode`。
- 清理收敛：`closeTransaction` 一处 `offChainDAG.Remove(txHash)` 原子删 node+keyIndex+subscriber+txSubKeys。
- 无环：`findCoveringSet` 增加 `exclude`（排除自己）+ 严格更早轮次（`SimulationNum < maxSimNum`）。

### ② DSN-50：救援前覆盖完整性 + 链深上限
- `offChainDAG.isFullyCovered(needKeys, patches)`：校验每个 key 都被选中 Patch 写过。
- `OffChainPatchNode.Depth` + `chainDepthOfUpstreams` / `chainDepthOf`；`TimeoutConfig.MaxChainDepth`（默认 5）。
- 三条救援路径（`scanPatchSubscribers`、`RetryCommit` Phase 1b/2b）都加“覆盖完整 + 深度 ≤ max”闸门，不满足则退回 `OnChainLockConflict`（等锁释放）。
- `RetryCommit` Phase 2b 从“仅 conflictKeys + maxPatches=1”改为“完整 needKeys + isFullyCovered”，移除 maxPatches=1。
- 新增差分统计 `SigChainDepthCapped`（进 CHAIN_RETRY_STATS）。

### ③ 附带
- BRF-08 的全量 SSC 重构（模块监控 gRPC/`ssc_getModuleStatus`、isChainTx 修复、global_finished_txs 清理尝试、SSCInternalPool、proto 迁移等）。
- 新增/更新单测：`ssc/offchain_dag_test.go`（覆盖完整性、深度、MarkReady 触发、Remove 清空、多上游消费等）。
- `.gitignore` 排除 `.hermes`。

---

## 三、两轮实验数据（rate=300，各 20000 笔）

| 指标 | DSN-49 状态 | +DSN-50 状态 | 变化 |
|---|---:|---:|---|
| 成功提交 | 11554 (57.77%) | 11095 (55.47%) | ↓ |
| 超时 | 8324 | 8789 | ↑ |
| 平均时延 | 9131ms | 9007ms | ≈ |
| P50 | 7905ms | 7528ms | ↓一点 |
| `retryAdd` | 103845 | 106321 | 仍 ~5.3 次/笔 |
| `sp1Started` | 372930 | 393777 | 仍 ~19.7 次/笔 |
| `poolStarted` | 217596 | 219376 | 仍 ~11 次/笔 |
| `retryCommitPatchHit` | 5956 | 5341 | ≈ |
| `PatchMiss` | 0 | **0** | 仍 0 |
| `chainLengthDist` 最大 | 37 | **66** | 反而更高 |
| `tempLockTryFail` / 冲突率 | — | 8514 / 34.6% | 高锁竞争 |
| `PoolTimeout` close | 10022 | 10204 | ≈ |
| `retry_limit_exceeded` | 459 | 500 | ≈ |

`local-ssc-retry-stats.py` 输出的 DAG 判定：`TryLockOk=3001 TryLockFail=1589 lockConflictRate=34.6% PatchHit=5341 PatchMiss=0 DAG-rescue-share=77.1%`。

---

## 四、关键诊断结论（新 session 必读）

### ① DAG 不是 churn 的来源，它被锁竞争拖垮
- DAG rescue-share 77%+、PatchMiss=0 → DAG 机制本身“能救的都救了”。
- 但 churn 依旧爆炸（每笔重试 5 次、sp1 启动 19 次）——因为**大量交易在真实竞争同一批 key（热 key）**，重试也抢不到锁，直到 PoolTimeout。
- 把 DAG 收窄（DSN-50）让交易退回“等锁”，但成功率没回升 → 说明**等锁也等不到（锁迟迟不释放）**，瓶颈是**串行化/吞吐不足**，不是 DAG。

### ② DSN-50 的覆盖完整性闸门是“冗余空转”
- `findCoveringSet` 本身逻辑就是“必须覆盖完所有传入 key 才返回非空”，所以 `isFullyCovered` 对同一批 key 恒为 true，实测 `PatchMiss=0` 证实它没拒绝任何救援。
- 结论：覆盖闸门对当前实现无实际效果，可保留作安全网，但**不是解药**。

### ③ `chainLengthDist` 与 DAG 深度无关——是重试次数
- 代码 `chainRetryStats.ChainLengthCnt[retryTx.SimulationNum]++`，而 `SimulationNum` 每次重模拟 +1（`req.SimulationNum + 1`）。
- 所以 66 层“链长” = **同一笔交易被重模拟 66 次**，不是 DAG 依赖深度。
- `MaxChainDepth` 只限制 DAG `Depth`，**约束不了 `SimulationNum`（重试次数）** → 拦不住 churn。这是 DSN-50 失效的根本原因之一。

### ④ 真正的问题：高锁竞争下的“失败→重试→再失败”无节制循环
- `tempLockTryFail=8514`、`lockConflictRate≈34.6%`、`retry_limit_exceeded` 到 simNum 46+。
- 每次锁冲突 → `CallForRetry` → 重模拟（SimulationNum+1）→ 又冲突 → 又重试，直到 MAX_TOTAL/sp1/pool 超时。
- 需要的是**让冲突交易在一个块内按确定顺序串行/批处理完成**，而不是靠重试循环。

---

## 五、决定：新 session 重写整个 DAG patch 方案

### 为什么重写而不是继续修
- 现有“链下 offChainDAG + 重试 chaining”设计在**高锁竞争**下表现不佳：救得了一部分，但大量交易仍靠重试硬扛，超时率下不来。
- DSN-49/50 只是把现有结构“合并/收窄”，没有改变“冲突→重试→churn”的机制本质。
- 用户判断需要**重新设计整个 DAG patch 方案**，而不是在这个基础上继续打补丁。

### 新方案应回答的核心问题（建议方向）
1. **如何让冲突交易确定性串行/批处理**（而不是重试）——这正是 **DSN-46（SimTx 层排序/批处理 + 进池仲裁分组 + 并行验证）** 的范畴，且与 DSN-49/50 正交。
2. **是否还需要“链下 DAG + ChainPatch 提前读状态”**，还是改为“等锁释放 + 块内排序”即可。
3. **如何限制重试次数 / 避免单笔被重模拟 60+ 次**（SimulationNum 失控）。
4. **热 key 仲裁**：先验证 `tempLockTryFail` 是否集中在少数 key（用日志里 lock conflict 的 key 分布确认）。
5. 新方案要保留的：`onChainPatches` 作为验证事实、`isChainTx`（len(UpstreamTxList)>0）判定、模块监控埋点。

### 参考既有设计文档
- `docs/designs/active/DSN-46-ssc-internal-pool-dag.md`（去 nonce + 进池仲裁分组 + 并行验证）
- `docs/designs/active/DSN-49-unify-offchain-patch-pool.md`（已实现，存储合并）
- `docs/designs/active/DSN-50-ssc-dag-rescue-coverage-and-depth-cap.md`（已实现，但被证实非解药）
- `docs/designs/active/DSN-23/26/29`（patchpool DAG 早期设计）

---

## 六、构建 / 环境注意事项（同 BRF-08，新 session 必读）

- **本地 build**：`export GOCACHE=/tmp/gocache && mkdir -p /tmp/gocache`（默认缓存目录只读）。
- **proto 重新生成**（改 `ssc/api/proto/ssc.proto` 后）：
  ```bash
  cd ssc/api/proto
  export PATH="$PATH:/home/wisecoach/go/bin"
  protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ssc.proto
  ```
- **SSH 到 199**：必须 `-F /dev/null -i ~/.ssh/id_ed25519 -o IdentitiesOnly=yes`；本环境无外网连不到 199，需用户执行或拷回日志。
- **实验日志在远程 199**；本地 `tmp_log/` 有拉回的 `txstages_*.csv`。
- **一键统计**：`python3 scripts/local-ssc-retry-stats.py`（本地会自动 scp + SSH 跑聚合）。

---

## 七、本 session 变更文件清单（相对 BRF-08）

**新增**
- `docs/designs/active/DSN-50-ssc-dag-rescue-coverage-and-depth-cap.md`
- `ssc/offchain_dag_test.go`
- `docs/briefs/BRF-09-handoff-...`（本文）

**修改**
- `ssc/patchpool.go`（offChainDAG、Depth、isFullyCovered、chainDepthOf、defaultMaxChainDepth、扫描闸门）
- `ssc/retry_scheduler.go`（maxChainDepth、SigChainDepthCapped、RetryCommit Phase1b/2b 闸门）
- `ssc/api/types.go`（TimeoutConfig.MaxChainDepth）
- `docs/designs/active/DSN-49-unify-offchain-patch-pool.md`（状态 implemented + 回归记录）
- `.gitignore`（排除 .hermes）

**已提交**：`53a5b5232`（含 BRF-08 全量 SSC 重构 + DSN-49/50 + 本文之前的所有进度）。

---

## 八、验证状态
- `go build ./ssc/... ./rpc/... ./node/...` 通过（exit=0）。
- DAG 单测（offchain_dag_test.go）全部 PASS。
- 实验（rate=300）：DSN-49 57.77% → +DSN-50 55.47%，均未解决超时；结论见 §四。
- **下一步**：新 session 按 §五 重写整个 DAG patch 方案（建议优先落 DSN-46 的确定性串行/批处理）。
