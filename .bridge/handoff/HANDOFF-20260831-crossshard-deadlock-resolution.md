# HANDOFF-20260831-crossshard-deadlock-resolution

> from_session: 20260831 (分析跨分片冲突/重试 + 死锁/活锁根治)
> from_role: Designer
> to_role: Designer (next session)
> 状态：**进行中** —— 死锁已修复并验证；活锁修复（§4.2）已实现、**尚未部署验证**

---

## 0. 背景与目标

rate=200 压测下跨分片交易大量 unfinished（~6900），最终 block 66 后**彻底死锁**（30 分钟零活动）。目标是：理清冲突/重试机制 → 修复 bug → 根治死锁 → 让所有交易能收敛提交。

## 1. 关键结论（根因链）

1. **死锁（已修）**：跨分片交易锁「先 Commit、后确认」——某个分片验证成功即上链锁，但整笔需要所有 related shard 验证通过才发 CRTx 释放。任一 shard 失败 → 其它已上锁 shard 锁成孤儿（LOCK_STALE）→ 级联死锁。
   - 迁移前被 `ChainPatch != nil` 跳过锁检测掩盖（`isChainTx` bug）；迁移修锁检测后暴露。**不是回归，是暴露**。
2. **活锁（§4.2，已实现待验证）**：锁释放后，冲突时 `VerifySimulation` 仍一律失败重试，无优先级判定 → A/B 双方都让位、无限重试 churn → 负载停后冻结 retryPool（~4200-5300）。

## 2. 已完成改动（ssc/）

| 文件 | 改动 |
|---|---|
| `simulator_leader.go` | ① RelatedShards 不再硬编码 self（错误/冲突分支用完整集合）；② 冲突/聚合失败分支也 notifyWaiters+closeResultCh（修并发死锁） |
| `simulator_member.go` | 删除 `ret.ConflictKeys` 赋值（重试从 states 的 LockedKeys 读） |
| `ssc/api/types.go` + `proto/ssc.proto` + `ssc.pb.go` + `proto/convert.go` | 删除 `ConflictKeys`；`CXTCommitProof` 新增 `ReleaseOnly` 字段 |
| `temp_lock_view.go` | `OnBlockCommitted` 改为处理内部交易（SimTx 清 TLV + CRTx 唤醒），加 `onChainKeys` 桥接表 |
| `state_locker.go` | BUG-12：`applyTo` 反向索引缺失时按持有者扫描释放孤儿锁（写+读） |
| `state_lock_impl.go` | 加 `txPriority` 注册表 + `RegisterTxPriority/GetTxPriority/GetLockHolder`（§4.2） |
| `verify.go` | ① `MulticastRollbackProof`（ReleaseOnly 跨分片释放锁）；② 冲突分支按优先级决定是否让位（§4.2） |
| `committer.go` | `CommitOrRollbackWithProof` Rollback 分支：`ReleaseOnly=true` 只释放锁、不 CloseTx |

**文档**：新增 `docs/designs/active/DSN-52-crossshard-deadlock-resolution.md`（设计+落地+实测）；更新 `DSN-47`（§9 迁移遗留 gap + 同类清单）。

## 3. 验证结果（rate=200，第 1 步 ReleaseOnly 部署后）

```
死锁修复前: LOCK_STALE 数万, globalLocked ~1000+, unfinished ~6900
ReleaseOnly 后: LOCK_STALE shard0=197/shard1=0, globalLocked ~0,
              unfinished 降到 ~463/1757/275/674, 但出现 rollback ~822/1203/495/1455
```

✅ 孤儿锁死锁已修复。
❌ 仍剩活锁：retryPool ~4200-5300 冻结，冲突交易反复重试仍撞锁。

## 4. 下一步（新 session 继续）

### 4.1 首要：部署验证 §4.2（优先级 Wound-Wait）
- §4.2 已实现（state_lock_impl.go + verify.go），**尚未部署跑**。
- 预期：低优先级让位（ReleaseOnly 释放）、高优先级保留锁 → 打破活锁 → retryPool 排空、unfinished 归零、不再每秒重试撞锁 churn。

### 4.2 若 §4.2 验证仍有问题，重点排查
- `dsn52ShouldYield` 里 `v.retrySchd.tempLockView.stateLockManager` 是否非 nil；持有者优先级是否成功注册。
- `GetLockHolder` 只查 on-chain 写锁；off-chain（同块 pending）冲突不走优先级（需确认是否要处理）。

### 4.3 后续实现（DSN-52 §9.3 / §4.6）
- ✅ **§4.6**：`SSCInternalPool.Extract` 按优先级排序（辅助，降低冲突频率）—— `internal_pool.go`：SimTx 桶按 `simPriority`（Nonce>OriginShardID>TxHash）稳定排序。
- ✅ **P1**：`StateDataParser.ParseBlock`（committee.go:901）改扫 `block.SSCTransactions()[InternalTxTypeSimTx]`（protobuf 载荷），旧路径（`block.Transactions()`）保留为无 SimTx 时的历史回退。
- **P2**：`core/rawdb` 交易索引是否补 SSC（若 SimTx/CRTx 需按 hash 可查）。

### 4.4 环境 / 复现
- 远程 199：`ssh -p 10022 zjnu@10.7.95.199`（key `~/.ssh/id_ed25519`）。
- 日志目录：`/home/zjnu/go/src/github.com/harmony-one/logs/harmony-sscc/shard=4_validator=4_ssc=1_delay=10_rate=200_vpn=4/`（当前 live run）。
- 分析脚本：`scripts/remote_ssc_retry_stats.py`（重试统计）、`scripts/local-txstages.py`、`scripts/analyze-tx-stages.py`。
- 关键日志：`[TempLockView] OnBlockCommitted timing breakdown`（releasedKeys）、`[retryScheduler] OnBlockCommitted stats`（retryPool/globalLocked）、`LOCK_STALE`、`MulticastRollbackProof`、`rollback with proof (ReleaseOnly)`、`DSN-52: higher priority...`。

## 5. 注意事项 / 坑

- **构建**：`go build ./ssc/...` 需 `GOCACHE=/tmp/gocache GOFLAGS=-mod=mod`（本机 go-build 缓存只读）。
- **测试编译失败是预先存在**：`simulation_test.go`/`simulation_params_test.go` 引用旧 Simulator API，与本改动无关。
- **proto 重新生成**：改 `ssc.proto` 后用 `protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc-opt=paths=source_relative ssc.proto`（版本匹配 v1.36.11）。
- **别混淆预先存在的工作区改动**：`test/configs/*.yaml`、`cmd/build_keys.go`、`harmony` 二进制、`retry_scheduler.go` 的 LockedKeys 合并、`verify.go` 的 Nonce 行等是**之前就有的**，非本 session 改动。
- **rollback vs 让位**：ReleaseOnly 是「释放锁、不判死、回重试池」；普通 Rollback CRTx 会 CloseTx（判死）。两者语义不同，别混用。
