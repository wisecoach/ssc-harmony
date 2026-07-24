# HANDOFF-20260722-partial-block-timing-analysis

> from_session: 20260721_224402
> from_role: Designer
> to_role: Designer (新 session)
> 焦点: 少量交易的 block 为什么耗时异常（~1200ms），而非系统容量问题

## 背景

RATE=150 实验中 Shard 3 的 block 25 只处理了 13 CR + 10 SimTx（23 笔），但 `commitTxs: 1205ms`，远超正常值（Shard 2 类似场景只要 4.3ms）。

当前分析认为 problem is pool backlog，但用户认为分析有误，需要聚焦单 block 的耗时分布。

## 当前代码状态

在 `ssc_master_58aae5802` 分支上，包含以下改动：

| 改动 | 文件 | 说明 |
|------|------|------|
| AddOnChainPatch 时序修复 | ssc/verify.go, ssc/impl.go | 验证成功后才存 onChainPatches |
| AddPatch 方法 | ssc/retry_scheduler.go | 链下 patches |
| callForRetry 异步 | ssc/verify.go:646 | go 异步执行 |
| remainingTime 保护 | node/worker/worker.go | 后一批次在 remainingTime<=0 时跳过 |
| Fake BLS 签名 | ssc/signer.go | 定位 CPU 瓶颈 |

## 关键日志

Shard 3 block 25:
- `begin to propose` → `Leader apply CommitOrRollback: txn=1`
- 1 CR tx 用了 ~242ms（从 01:54:33.93 到 34.17）
- `Leader apply ssctxs: txn=1, duration=242ms` — 这是从 startTime 开始的累计时间
- `Leader apply txs: txn=0, duration=1200ms` — 同上，累计时间
- `ProposeNewBlock breakdown`: commitTxs=1205ms, total=1653ms

`[cr, ssc, plain]` 在 `begin to commit transaction` 中显示的是 **pool 中的 pending 数**：
- Block 24 commit: `[13, 1788, 0]` — pool 还有 1788 笔 SimTx
- Block 25 commit: `[9, 1941, 0]` — pool 还有 1941 笔（还在涨）

## 遗留问题

1. `remainingTime=1000ms` 是否太保守？建议调到 2000ms
2. block 25 的 1 CR + 0 SimTx 耗时 242ms 是不是正常？
3. Shard 3 是否因为跨分片 SimTx 验证的异步等待导致产块延迟？
