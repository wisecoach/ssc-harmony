# HANDOFF-20260715-block-execution-analysis

> from_session: current
> from_role: Designer
> to_role: Designer
> focus: 区块执行耗时分析 — 定位 commitTxs 高耗时的根因

## 完成内容

| 事项 | 状态 |
|:-----|:------|
| DSN-29 PatchPool 重构 + subscriber 索引 | ✅ 已实现，实验验证通过 |
| DSN-30 PatchPool 合并入 RetryScheduler | ✅ 已实现，编译通过 |
| 7月15日实验 | ✅ 新代码跑完，日志就绪 |
| 实验结论 | ✅ retry 路径完美（16/16 success via consumedPatches） |

## 最新实验数据（7月15日, delay=10, rate=100, 4shards）

```
TPS: ~95, 总耗时: ~104s
分片: S0=2063/1/1, S1=2798/0/74, S2=1689/1/0, S3=3337/0/36
unfinished: 111 (S1=74 热点)
```

### 交易处理时间（commitTransaction timing breakdown）

| txType | 数量 | avg | P50 | P90 | 说明 |
|:-------|:----:|:---:|:---:|:---:|:------|
| SimTx | 19,870 | **6.87ms** | **5.08ms** | **12.92ms** | 🔴 主瓶颈（同步 VerifySimulation） |
| CRTx | 19,853 | 0.56ms | 0.47ms | 0.83ms | 快 |
| CrossShardTx | 9,996 | 0.07ms | 0.05ms | 0.10ms | 几乎免费 |
| NormalTx | 512 | 0.12ms | 0.11ms | 0.15ms | 几乎免费 |

### Retry 路径状态（PatchPool + consumedPatches）

| 指标 | 值 |
|:-----|:-----|
| total retry attempts | 16 → 全部成功 |
| consumedPatches bypass | 16/16（绕过 TLV TryLock） |
| retry commit failed | **0** |
| TLV try lock failed | **0** |
| scanPatchSubscribers matched | 16 |
| added to retry pool | 76 |

✅ **retry 路径已无优化必要。**

## 下一个 session 的聚焦方向

### 为什么 commitTxs 耗时那么高

日志中有 `[BlockTiming] ProposeNewBlock breakdown`（`/mnt/E/gowork/src/github.com/wisecoach/harmony-sscc/consensus/consensus_block_proposing.go:44`），`commitTxs` 字段记录了从 ProposeNewBlock 入口到 `CommitTransactions()` 结束的时间。

已知信息：
- SimTx（VerifySimulation）平均 6.87ms/笔，同步执行
- 块 28 有 50+ 笔 commitTransaction timing breakdown（大量 SimTx 集中在一块）
- `commitTransaction timing breakdown` 日志在 `/mnt/E/gowork/src/github.com/wisecoach/harmony-sscc/node/worker/worker.go:491`
- 远程日志在 `~/go/src/github.com/harmony-one/logs/harmony-sscc/shard=4_validator=4_ssc=1_delay=10_rate=100_vpn=4/ssc-validator*.log`

需要进一步分析：
1. ProposeNewBlock breakdown 的 `total` vs `commitTxs` vs `waitForSignatures` 细分
2. 跨 shard 的 block timing 对比（为什么 S1 有 74 unfinished 而其他 shard 不到 36）
3. VerifySimulation 的细分耗时（verify.go 中各阶段的时间分布）
4. StartSimulateCXTransaction breakdown（`total` 是 130ms avg，但异步不阻塞出块）

### 相关文件路径

| 路径 | 说明 |
|:-----|:------|
| `/mnt/E/gowork/src/github.com/wisecoach/harmony-sscc/consensus/consensus_block_proposing.go` | ProposeNewBlock + BlockTiming |
| `/mnt/E/gowork/src/github.com/wisecoach/harmony-sscc/node/worker/worker.go` | CommitTransactions + timing |
| `/mnt/E/gowork/src/github.com/wisecoach/harmony-sscc/ssc/verify.go` | VerifySimulation |
| `/mnt/E/gowork/src/github.com/wisecoach/harmony-sscc/ssc/simulator.go` | StartSimulateCXTransaction |
| `/mnt/E/gowork/src/github.com/wisecoach/harmony-sscc/ssc/simulator_leader.go` | Leader 端模拟调度 |
| `docs/designs/active/DSN-29-patchpool-refactor-subscriber-index.md` | DSN-29 设计文档 |
| `docs/designs/active/DSN-30-patchpool-merge-into-retryscheduler.md` | DSN-30 设计文档 |

## 已知坑

- `commitTxs` 是 cumulative 时间（从 ProposeNewBlock 入口到 CommitTransactions 结束），包括了块间等待（BLS 签名）时间，**不是纯交易处理时间**
- SimTx 的 timing `total` 是用 ms 单位，ProposeNewBlock 的 `total` 是用 seconds 单位
- 远程实验日志覆盖同一目录，记得跑新实验前备份旧日志
- 确认二进制新鲜度后再分析（`stat --format="%y" bin/harmony` vs `find . -name "*.go" -newer bin/harmony`）
