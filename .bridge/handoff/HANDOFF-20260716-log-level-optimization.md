# HANDOFF-20260716-log-level-optimization

> from_session: current
> from_role: Designer
> to_role: Designer
> focus: 日志等级规范化 + 实验验证

## 完成内容

| 事项 | 状态 |
|:-----|:------|
| DSN-10 日志查询指南更新 | ✅ 已更新至 2026-07-16 版本 |
| 所有 SSC timing breakdown → Debug | ✅ verifiy.go / simulator.go / committer.go / retry_scheduler.go / impl.go |
| `lockStateWithRWSet timing` → Debug | ✅ verify.go:800 |
| `HandleCommitVote timing` → Debug | ✅ impl.go:631 |
| `Simulator.Cleanup timing` → Debug | ✅ simulator.go:206 |
| `VerifySimulation timing breakdown` → Debug | ✅ verify.go:267 |
| 已恢复用户误改的 Info 日志 | ✅ 恢复了 `ssc/committee.go`, `impl.go`, `cxt_timer.go` 等 12 个文件的 Info |

## data_handler 所需的 Info 日志（精确清单）

data_handler.py 解析的 5 个日志模式，**必须保持 Info**：

| # | 日志 message 包含 | 位置 | 当前等级 |
|:-:|:------------------|:-----|:--------:|
| 1 | `"commit or rollback with proof"` | `ssc/committer.go:62` | Info ✅ |
| 2 | `"commit with proof, origin"` | `ssc/committer.go:68` | Info ✅ |
| 3 | `"leader close transaction, commit:"` | `ssc/impl.go:1167` | Info ✅ |
| 4 | `"simulation is pool timeout, status=PoolTimeout"` | `ssc/impl.go:488` | Info ✅ |
| 5 | `"simulation is pool timeout, status="` | `ssc/impl.go:779` | Error ✅ |

> 注：`"handle simulate request, start"`（simulator_member.go:935）本就在 Debug，data_handler 从 validator 日志解析模拟事件，不影响。

## 当前 SSCC 远程状态

| 资源 | 状态 |
|:-----|:------|
| 远程服务器 | `zjnu@10.7.95.199 -p 10022` |
| 代码目录 | `~/go/src/github.com/harmony-one/harmony-sscc` |
| 日志目录 | `~/go/src/github.com/harmony-one/logs/harmony-sscc/shard=4_validator=4_ssc=1_delay=10_rate=100_vpn=4/` |
| 二进制 | 最新编译时间 `2026-07-16 16:42`（含所有 Debug 改动） |
| 剩余 git diff | 7 个文件变更（DSN-29/DSN-30 重构 + Debug 改动） |

## 待办清单

- [ ] 跑实验验证日志量是否降低（预期从 3.8M Info 大幅下降）
- [ ] 如需进一步压低，可以排查 `aggregate sigs`（133K Info 行，在 ssc/signer.go 或 committee.go，由共识委员会签名触发）
- [ ] 定期回顾 DSN-10，确保新加的 Info 日志不会被遗忘

## 参考文档

| 路径 | 说明 |
|:-----|:------|
| `docs/designs/active/DSN-10-log-query-guide.md` | 更新后的日志查询指南 |
| `docs/designs/active/DSN-29-patchpool-refactor-subscriber-index.md` | DSN-29 PatchPool 重构 |
| `docs/designs/active/DSN-30-patchpool-merge-into-retryscheduler.md` | DSN-30 PatchPool 合并入 RS |
| `../ssc-cli/data_process/sscc_log_handle/data_handler.py` | data_handler 源码（精确依赖 5 个 SSC 日志模式） |
| `.bridge/handoff/HANDOFF-20260715-block-execution-analysis.md` | 上一份交接文档 |

## 已知坑

- 本地无法编译 BLS 依赖（`bls/bls.h` 不存在），必须 rsync 到远程后编译
- `make test` 需要 Docker TTY，本环境不支持
- `aggregate sigs`（~133K 行/实验）是共识签名日志，来自 `singer.go`，不在 SSCLogger 路径下。要降低它需要调 `Logger()` 的全局等级
- 不要批量改所有 `.Info()` → `.Debug()` —— data_handler 依赖 5 个精确模式
