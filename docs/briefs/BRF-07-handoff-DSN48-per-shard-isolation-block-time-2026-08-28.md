# HANDOFF — DSN-48 落地回归：跨分片 SimTx 隔离 + 出块 1s 预算 + 日志 SSC 计数

> **日期**：2026-08-28
> **项目**：`/mnt/E/gowork/src/github.com/wisecoach/harmony-sscc`
> **远程**：`zjnu@10.7.95.199 -p 10022` → `~/go/src/github.com/harmony-one/harmony-sscc`
> **基于**：`docs/briefs/BRF-06-...-2026-08-27.md`、`docs/designs/active/DSN-48-simple-ssc-internal-pool.md`
> **远程日志**：`~/go/src/github.com/harmony-one/logs/harmony-sscc/shard=4_validator=4_ssc=1_delay=10_rate=200_vpn=4/`

---

## 一、本 session 一句话总结

DSN-48 内部池落地后，定位并修复了**跨分片 SimTx 泄漏（每分片 SimTx 边界被打破）**导致的 `InvalidSimulation` 大量回滚，并给 SSC 内部交易补上 **1s 出块时间预算**、让块级日志正确显示 **SSC 内部交易数**。

## 二、关键结论 / 修复清单

### ① 跨分片 SimTx 泄漏（主因：回滚率高）
- **现象**：`rate=200` 回滚率极高，回滚 reason 几乎全 `InvalidSimulation`，细分 `failed to get header for block hash`（7.7 万+）。
- **根因**：`ssc/impl.go` 的 `CommitSimulation` 用 `ShardId: commit.ShardId`（origin），导致每个节点为所有 origin 建 SimTx，全部塞进**本节点池**并广播到**所有分片 group**；出块时把别的分片 SimTx 也进块 → 验证对非本分片 BlockHash 调 `GetHeaderByHash` → nil → 回滚。
- **修复**（三处）：
  1. `ssc/impl.go`：`ShardId: commit.ShardId` → `ShardId: s.SelfShard`（主修）
  2. `cmd/harmony/main.go`：sink 按 `tx.Shard != nodeConfig.ShardID` 丢弃（接收侧兜底）
  3. `ssc/tx_submitter.go`：`submitInternal` 按 `shard != t.selfShard` 拒绝（提交侧兜底）

### ② 出块 1s 时间预算（SSC 内部交易纳入）
- **现象**：`Block gas limit and usage info` 的 `duration` 可达 3.2s+。
- **根因**：SSC 内部交易循环（`node/worker/worker.go`）没走 1s 预算，整批跑完拖超时。
- **修复**：SSC 循环加 `remainingTime`（1000ms）检查，超预算 `break sscBatchLoop`；SSC 耗时从总预算扣除；普通交易继续用剩余预算。

### ③ 块级日志正确显示 SSC 数量
- `Block gas limit and usage info` 新增 `sscTxn` 字段（本块实际应用的 SSC 内部交易数）。

### ④ 分析工具与连接规范
- 新增 `scripts/local-ssc-retry-stats.py` + `scripts/remote_ssc_retry_stats.py`：一键向 199 拉取 CHAIN_RETRY_STATS / STATS DUMP / 超时 / 回滚 reason / `failed to get header` 分布。
- `AGENTS.md` 新增 §6.1 SSH 连接规范：必须 `-F /dev/null -i ~/.ssh/id_ed25519 -o IdentitiesOnly=yes`（199 只授权 id_ed25519；id_rsa_new 未授权）。

## 三、验证状态

- 本地 `go build ./...` 通过（exit=0），`go vet` 通过。
- 尚未重新跑实验验证回滚率/时延是否回落——**下一步是部署 + 重跑 rate=200 复查**。

## 四、部署必读
- **行为变更**：SimTx 广播范围从“所有分片”收敛到“本分片”；出块时间预算把 SSC 纳入 1s。需全节点同版本、同时升级。
- 重跑后复查：
  1. `tryBroadcastSSCInternalTx` 的 `shardGroupID` 应只剩本分片（不再出现 `node/shard/1/2/3` 在别的节点上）。
  2. `failed to get header` / `InvalidSimulation` 回滚应大幅下降。
  3. `Block gas limit and usage info` 出现 `sscTxn` 且 `duration` 基本 ≤1s。
  4. 用 `scripts/local-ssc-retry-stats.py --rate 200` 复查 DAG/超时/回滚 reason。

## 五、保留未动 / 已知项
- `CHAIN_RETRY_STATS` 仍是 Debug 级，默认日志抓不到（统计应 Info，需提升级别才能量化 DAG）。
- DSN-46（内部池进池仲裁分组 / DAG 就绪）仍未做，冲突仍堆到出块阶段。
- 详细设计变更与风险已写回 `docs/designs/active/DSN-48-...md` §8。
