# HANDOFF — SSC 模块监控接口 + DAG 泄漏/环/误判修复 + DSN-49 合并方案（待实现）

> **日期**：2026-08-28
> **项目**：`/mnt/E/gowork/src/github.com/wisecoach/harmony-sscc`
> **远程**：`zjnu@10.7.95.199 -p 10022` → `~/go/src/github.com/harmony-one/harmony-sscc`
> **远程日志**：`~/go/src/github.com/harmony-one/logs/harmony-sscc/`
> **基于**：`docs/briefs/BRF-07-...-2026-08-28.md`
> **新增设计**：`docs/designs/active/DSN-49-unify-offchain-patch-pool.md`（status: planned，待实现）

---

## 一、本 session 一句话总结

为 SSC 增加了「模块状态监控」（gRPC `SSCMonitorService` + JSON-RPC `ssc_getModuleStatus`），并用它定位到 DAG 层的**资源泄漏**、**`isChainTx` 误判（锁检测失效 → 回滚-重模拟循环 → 高时延）**和 **Patch DAG 成环导致 stack overflow**；已完成 4 处最小修复，并写出了 DSN-49（合并 `localPatches`+`patches` 为单一链下 DAG）的设计文档，**尚未实现**。

---

## 二、新增能力：SSC 模块监控

### ① gRPC 服务 `SSCMonitorService`
- `ssc/api/proto/ssc.proto` 新增 `ModuleStatus` + 子消息（`RetrySchedulerStatus`/`StateLockStatus`/`TempLockViewStatus`/`SimulatorStatus`/`VerifierStatus`/`TimerStatus`/`DAGStatus`）+ `SSCMonitorService/GetModuleStatus`。
- 服务端：`rpc/ssc_grpc.go` 新增 `sscGrpcMonitorService`，在 `RegisterSSCGrpcServer` 注册（`node/api.go StartSSCGrpc` 自动带上，端口 = P2P 端口 − 500）。
- 调用：`grpcurl -plaintext -d '{}' localhost:<grpc端口> sscpb.SSCMonitorService/GetModuleStatus`（JSON 输出）。

### ② JSON-RPC `ssc_getModuleStatus`（用户更常用）
- `rpc/ssc.go` 新增 `NewPublicSSCMonitorAPI` + `PublicSSCMonitorService`；`rpc/rpc.go getAPIs()` 在 `GetSSCService()!=nil` 时注册（`ssc` 命名空间已在 HTTPModules 白名单）。
- 调用：`curl -X POST -H 'Content-Type: application/json' -d '{"jsonrpc":"2.0","method":"ssc_getModuleStatus","params":[],"id":1}' http://<host>:<rpc端口>`
- 输出为 snake_case JSON，含各模块条目数 + `dag` 块（`chain_tx_detected` / `retry_commit_patch_hit` / `retry_commit_patch_miss`，**累计值**）。

### ③ 配套 Stats 方法
- `ssc/simulator.go` `Simulator.Stats()`、`ssc/verify.go` `Verifier.Stats()`、`ssc/retry_scheduler.go` `retryScheduler.Stats()`、`ssc/cxt_timer.go` `CXTTimerManager.Stats()`、`ssc/impl.go` `simulationReqPriorityQueue.Len()`、`ssc/api/monitor.go` `api.ModuleStatus`。

---

## 三、定位到的问题与已做修复（4 处，均已 `go build ./ssc/... ./rpc/... ./node/...` 通过）

### ① `isChainTx` 误判 → 锁检测整体失效（高时延主因）
- **现象**：`chain tx detected` ~14.6 万（几乎每笔都被判链式）；`①pool→sim≈1ms` 但 `②sim→close≈16s`（P50），P99≈24s、max≈42s；回滚-重模拟循环 ~15 万次空原因回滚。
- **根因**：`CommitSimulation` 给每笔 SimTx 都填非 nil `ChainPatch`，而 `VerifySimulation` 用 `ChainPatch != nil` 判 `isChainTx` → 全 true → `if !isChainTx { CheckLock }` 整体跳过锁冲突检查 → 冲突无法被正确串行化 → 全部掉进超时兜底。
- **修复**（`ssc/verify.go`）：`isChainTx := len(simulation.UpstreamTxList) > 0`（只有真正有上游依赖才算链式）。

### ② `retryScheduler.patches` 泄漏
- **根因**：`patches`（leader 侧 ChainPatch 缓存）只写不删；`localPatches` 有 `removePatch` 清理、`onChainPatches` 有 `RemoveOnChainPatch` 清理，唯独 `patches` 没有 → 实测残留 ~9828。
- **修复**（`ssc/impl.go closeTransaction`）：补 `s.retryScheduler.patches.Delete(txHash)`。

### ③ `stateLock.global_finished_txs` 泄漏（⚠ 未完全解决，见待办）
- **修复尝试**：`closeTransaction` 补 `s.lockStateMgr.globalFinishedTxs.Delete(txHash)`。
- **结果**：`patches` 已从 9828→1，但 `global_finished_txs` 仍 ~9824。**原因待查**——怀疑 `globalFinishedTxs` 在块提交 `applyTo()` 写入，而 `closeTransaction` 的删除发生在其之前/另一条路径（异常回滚没走正常 close）。见 §六待办①。

### ④ Patch DAG 成环 → stack overflow
- **现象**：`fatal error: stack overflow`，栈全为 `retryScheduler.ReadOnChainPatch`（retry_scheduler.go:1726）自我递归。
- **根因**：`ReadOnChainPatch` / `readPatchChain` 沿 `UpstreamTxList` 递归，无环检测；而 `UpstreamTxList` 由 `findCoveringSet` 从“当前 Free 且覆盖冲突 key 的 patch”推导，**不含全局顺序、也不排除自己** → 可自环/互环。
- **修复**（`ssc/retry_scheduler.go`）：两处递归加 `visited`（按 `(TxHash,SimulationNum)`）防环。这是**止血**，根因（构造时可能成环）留给 DSN-49 解决。

### ⑤ DAG 使用情况可观测（配合本次定位）
- `chainRetryStats` 新增 `SigChainTxDetected`（差分，进 `CHAIN_RETRY_STATS`）+ `MonitorChainTxDetected/PatchHit/PatchMiss`（**累计**，进 `ssc_getModuleStatus` 的 `dag` 块）。
- `retryScheduler.cycle()` 原为空循环（统计从不输出）→ 改为**每 60s + 退出时 dump `CHAIN_RETRY_STATS`**。
- `scripts/remote_ssc_retry_stats.py` `key_order` 加 `chainTxDetected`。

---

## 四、重要诊断结论（供新 session 参考）

- **DAG 本身是好的**：`retryCommitPatchHit=40, PatchMiss=0, rescue-share=100%`；`chainCommitDist={'1':72,'2':4}` → 真正走 RetryCommit 的全被 DAG 救回。
- **高时延不是 DAG 慢**，而是 `isChainTx` 误判导致锁检测被关 → 冲突交易反复回滚-重模拟直到超时。① 修复后预期：`chain tx detected` 大幅下降、`②sim→close` 显著回落。
- **DSN-49 与 DSN-46 分层**（用户明确，重要）：
  - DSN-49 = **链下模拟期 key 冲突**：模拟时提前读“锁持有者的 patch 状态”，不必等锁释放（状态可见性/并发）。
  - DSN-46 = **SimTx 与 SimTx 之间**的提交顺序/批处理（排序/区块构建层）。
  - 两者**正交、非前后置**。DSN-49 不依赖也不前置 DSN-46。

---

## 五、待办（新 session 实施顺序）

### ① 先查 `global_finished_txs` 为什么没清干净（可和②一起）
- 它和时延可能同源：大量交易没走正常 `closeTransaction`（异常回滚路径）。修复后若回滚大幅下降，此残留大概率自行好转；否则要追 `applyTo()` 写入 vs close 删除的时序。
- `tx_traces` 按用户要求**保留**（分析数据），不清理。

### ② 重跑一次实验验证 4 处修复
- 部署到 199，重跑（如 `rate=200/300`），用 `scripts/local-ssc-retry-stats.py` 复查：
  - `chain tx detected` 应从 ~14.6 万 掉到几百/几千；
  - `retry_scheduler.patches`、`global_finished_txs` 应接近 0；
  - `②sim→close` 时延应明显下降（不再全靠超时兜底）；
  - 不再 stack overflow。

### ③ 实现 DSN-49（合并 `localPatches` + `patches` → 单一链下 `offChainDAG`）
- 完整方案见 `docs/designs/active/DSN-49-unify-offchain-patch-pool.md`（planned）。核心：
  1. 新类型 `OffChainPatchNode` = 原 `ChainPatchNode`（调度状态）+ `api.ChainNode` 的 `UpstreamTxList`（读构）合并；
  2. 删 `retryScheduler.localPatches` 与 `patches` 两个字段，换 `offChainDAG` 一个字段；`onChainPatches` 保持独立；
  3. 写入合并：impl.go:1011 `AddPatch` + impl.go:1044 `addPatch` → 一次 `AddNode` + `MarkReady`；
  4. 读取统一：`readPatchChain`/`findCoveringSet`/`GetChainPatchRef`/`GetUpstreamTxRef` 全读 `offChainDAG.nodes`；
  5. 清理收敛：`closeTransaction` 一处 `dag.Remove(txHash)` 原子清 node+keyIndex+subscriber；
  6. 无环由构造保证：`findCoveringSet`/`scanPatchSubscribers` **排除自己** + 只选严格更早/更小顺序（配合 visited 兜底）；
  7. 补单测：无自环、无环、findCoveringSet 正确性、close 后 offChainDAG 清空、多上游消费。

### ④ DSN-46（独立线，可后做）
- SimTx 层排序/批处理（去 nonce + 进池仲裁分组 + 并行验证），与 DSN-49 正交，见 `docs/designs/active/DSN-46-...md`。

---

## 六、构建 / 环境注意事项（新 session 必读）

- **本地 build 需要设可写 GOCACHE**（默认缓存目录只读会报 `read-only file system`）：
  ```bash
  export GOCACHE=/tmp/gocache && mkdir -p /tmp/gocache
  ```
- **proto 重新生成**（改动 `ssc/api/proto/ssc.proto` 后）：
  ```bash
  cd ssc/api/proto
  export PATH="$PATH:/home/wisecoach/go/bin"   # protoc-gen-go / protoc-gen-go-grpc
  protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ssc.proto
  ```
- **SSH 到 199**：必须 `-F /dev/null -i ~/.ssh/id_ed25519 -o IdentitiesOnly=yes`（见 BRF-07 §6.1；id_rsa_new 未授权）。本机此环境无外网，连不到 199，需用户在 199 上执行或把日志拷回本地。
- **实验日志在远程 199**，本地 `tmp_log/` 只有拉回的 `txstages_*.csv`。

---

## 七、本 session 变更文件清单

**新增**
- `ssc/api/monitor.go`（`api.ModuleStatus` + 各子状态 + `DAGStatus`）
- `docs/designs/active/DSN-49-unify-offchain-patch-pool.md`
- `docs/briefs/BRF-08-...`（本文）

**修改**
- `ssc/api/sscs.go`（`Service` 接口加 `GetModuleStatus()`）
- `ssc/api/proto/ssc.proto` / `ssc.pb.go` / `ssc_grpc.pb.go`（ModuleStatus/DAGStatus/SSCMonitorService）
- `ssc/api/proto/convert.go`（ModuleStatus/DAGStatus 转换）
- `ssc/impl.go`（GetModuleStatus、patches/globalFinished 清理、DAG 累计值、队列 Len）
- `ssc/verify.go`（isChainTx 修复、chain tx 计数）
- `ssc/retry_scheduler.go`（Stats、cycle 定期 dump、SigChainTxDetected、Monitor 累计、visited 防环）
- `ssc/simulator.go` / `ssc/cxt_timer.go`（Stats）
- `rpc/ssc_grpc.go` / `rpc/ssc.go` / `rpc/rpc.go`（监控服务 + 注册）
- `scripts/remote_ssc_retry_stats.py`（chainTxDetected）

---

## 八、验证状态
- 本地 `go build ./ssc/... ./rpc/... ./node/...` 通过（exit=0）。
- 尚未重新跑实验验证时延/回滚/栈溢出是否解决——**下一步是部署 + 重跑**（见 §五②）。
