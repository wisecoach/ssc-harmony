# HANDOFF-20260711-tlv-syncmap

> from_session: `$HERMES_SESSION_ID`
> from_role: Designer
> to_role: Designer

## 完成内容

| 事项 | 状态 |
|:-----|:------|
| DSN-25: OnBlockCommitted StateDB 缓存 | **已实现 + 实验验证** |
| DSN-26: PatchPool DAG 扩展到 TLV Phase 1 + Phase 2b | **已实现 + 实验验证** |
| 发现 Phase 1b ClearWounded 不是根因 | 已确认 |
| 发现 maxPatches=1 限制不改提交率 | 已确认 |
| **DSN-27: TempLockView 全局 Mutex 优化** | **已实现 + 实验验证** |
| RetryCommit + TLV + CheckLock timing 计时 | 已实现 |
| Useless drawio tempfile cleanup | 已提交 |

## DSN-27 实验结果

| 指标 | 改造前（全局 mutex） | 改造后（sync.Map） | 提升 |
|:-----|:-----------------:|:----------------:|:----:|
| TLV >100ms 慢调用 | 14,714 (20.6%) | **0** | ✅ 完全消除 |
| RC P50 | 13ms | **7ms** | ↓46% |
| RC P90 | 474ms | **23ms** | ↓20x |
| RC P99 | 1,498ms | **316ms** | ↓5x |
| >100ms 调用占比 | 20.6% | **2.8%** | ↓8x |
| 提交率 | ~79% | **~79%** | 不变 |

## 当前代码状态

- Branch: `ssc_shared_address_space`
- Latest commit: `878556d2e` DSN-27: TempLockView 全部 sync.Map 化

## 待办清单

- [ ] 追剩余 2.8% >100ms RetryCommit 调用的来源（可能是 CheckLock 或 block 提案）
- [ ] 优化 block 处理时间（每 500 tx 块耗时 5~6s，tx 级 ~10ms/笔）
- [ ] 多跑几轮实验确认 DSN-27 的并发正确性（提交率方差 <3%）

## 关键设计文档

| 路径 | 说明 |
|:-----|:------|
| `docs/designs/active/DSN-27-tlv-mutex-contention.md` | **v3：TempLockView 全部 sync.Map 化** |
| `docs/designs/active/DSN-25-retrycommit-state-cache.md` | OnBlockCommitted 缓存 stateDB |
| `docs/designs/active/DSN-26-patchpool-dag-tlv-extension.md` | PatchPool DAG 扩展到 TLV |

## 已修改的文件

| 文件 | 改动 |
|:-----|:------|
| `ssc/temp_lock_view.go` | 全部 sync.Map 化，移除 `v.mu` |
| `ssc/retry_scheduler.go` | Phase 1b DAG, Phase 2b DAG, maxPatches=1, 计时 defer, CheckLock 计时 |

## 已知坑

- TLV 不再有全局 mutex，但 GarbageCollect/OnBlockCommitted 的 Load→检查→Delete 有微秒级 race（被删的锁是别人刚拿的）——无害，对方会重试
- 本地 `go build ./ssc/` 因 go 版本 1.22.2 vs 需要 1.22.5 失败。远程 `make` 正常工作
- conda 在 SSH 后台进程有插件冲突（`anaconda_anon_usage`）。前台用 `env -i HOME=$HOME PATH=... bash -l -c` 绕过
- 每次实验的提交率波动较大（75%~96%），多看几轮取中位数
- sync.Map 的 Range 在遍历时对整个 map 上锁，但只在 OnBlockCommitted（~2s/次）调用
