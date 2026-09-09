# HANDOFF-20260909: DSN-65(P1/P2) 已提交 checkpoint + 移除wound A/B 结果（未解决停摆）+ 下一步定位链上锁泄漏

> 承接 HANDOFF-20260907-dsn-unfinished-drain-analysis.md（4711 平台期根因）。
> 本 session 完成：① P1 符合性修复并提交 checkpoint `69cd2ec07`；
> ② 跑 P1 版 5h 长窗口 → 确认 unfinished≈4.2k 平台（比旧 4711 略好）；根因收敛到
> **partial winner 的链上锁泄漏（LOCK_STALE 千万级）**，CMH 因“sink 而非环 + 只在高被低卡才发探针”从不杀 victim；
> ③ 做「移除 wound」A/B（`canWound→false`，**未提交**）→ 实测**同样 ~1h 停摆**，证伪“wound 导致系统失活”。
> 下一步应在**新 session** 继续（见 §6）。

---

## 0. 一句话结论
P1/P2（DAG producer 自保护 + 整单 DAG 标志）是有效的地基（unfinished 平台 4711→~4.2k），但**不是解药**。
真正的卡死 = **跨分片异步上链的 partial winner：某腿已链上锁(LOCK_STALE)、因其它腿 verify 撞链上锁/被饿而永不 commit、
CRTx 永不发 → 链上锁永不释放 → 挡死下游**。CMH 设计只杀“环”且只由“高被低卡”发探针，对这种 **sink(无出边)** 与
“低被高卡”边**从不触发**（triggerProbe=0）。**移除 wound A/B 未避免停摆 → wound 不是病根**。

---

## 1. 本 session 做了什么
- **P1 符合性修复**（短窗口复测 P1 violations 260→0）：producer 自保护从 `markChainProtected` 拆成独立
  `markProducerProtected`（patchpool.go `producerProtected` 位），`releasePatch/unmarkChainProtected` 只撤下游那份，
  不再剥离 producer 自身的构建保护（修复“producer 构建后仍被 wound”，根因=与 DSN-62 下游保护共用同一 flag）。
- **E/P2**：`HandleRetrySignal` 去掉 origin 本地过滤，携带上游的 chain signal 一律整单 `markChainTx`（短窗口触发 1374 次）。
- **提交 checkpoint** `69cd2ec07`（含上一 session 在途的 RetryRollback/Fix-B/StaleTx 对称撤销改动 + DSN-65 + handoff）。
- **P1 版 5h 长窗口**（网络 9/8 19:33→9/9 00:59）：committed≈**15528(77.6%)**、unfinished≈**4.2-4.3k 平台**、
  **20:31 后零 commit**（跨 4 leader 去重 `MarkCommittedOnChain`/commit-close 两信号一致）。
- **移除 wound A/B**：`temp_lock_view.go canWound → return false`（**未提交**）；网络 9/9 14:22 启动，
  send-end commit 12515/62.58%/unfinished 7419；但 **commit 也在 ~15:25（约 1h）停摆** → 证伪“wound→失活”。

---

## 2. 长窗口根因（P1 版，证据最硬）
- 具体交易：`0x9458cd…`(unfinished DAG, shard0 origin cross2)：origin shard0 5547 `set signal`，shard1 腿 8999
  reservation-skip 卡在热 key `0xe1512`/`0xfea728`，无 vote、无终局。
- 热 key `0xe1512` 的链上持有者完整 txHash = **`0xf3fc4c5a3e7279bc1f160a9235c068ba1c232c2e3f3871239a1d9e0a4e075bf9`**
  （shard1 origin cross8，unfinished）：shard1 腿先 verify 成功锁住 `e1512`(AddOnChainDAGPatch)，shard3 腿 verify 撞
  `0xc2Ee93` 已被另一交易链上锁(mu conflict write)→不投票→整单 quorum 不齐→`e1512` 链上锁永不释放(LOCK_STALE~8959块)。
- **规模**：LOCK_STALE（锁泄漏告警）跨 leader：9000=479 万 / 9040=166 万 / 9080=985 万 / 9120=**1256 万** 次；
  shard1 内 ~496 把不同 key 被链上锁泄漏。rollback≈103（几乎无 partial winner 被整单回滚）。
- **CMH 为何抓不到**：deadlock_detector `shouldTriggerProbe` 只对「高优先 waiter 被低优先 holder 的链上锁卡」
  发探针；这批是**低被高卡 + holder 是 sink(等自己 quorum、无出边)**；waitEdge 记了几百条但 **triggerProbe/die/victim=0**
  （4 leader 全 0）。即 CMH 只杀“环”、不杀“以 sink 结尾的链”。

## 3. 机制澄清（对 Q2 的修正，勿再犯）
- key 是分片的，单 leader 下 **TLV 能串行本分片所有 key**（TLV 管得住构建层）。
- TLV 只管到“构建 SimTx”，真正锁在**链上 verify** 才产生；跨分片多腿**异步逐个上链**，先落块者拿链上锁。
- 高优先在 TLV 抢到 / 或同 key 多个在途 SimTx 由 InternalPool 按优先级排序 → 高优先 SimTx 先上链锁住 → 低者 verify 撞锁。
- **wound 只作用于链下 TLV**，对已上链锁无效；放弃 wound 对链上泄漏是中性（A/B 证实）。

## 4. 移除 wound A/B 的观察（未提交）
- 改法：`canWound` 恒 false（TryLock 永不抢占，谁先拿 TLV 谁保留）。**未提交**（git status 仅 M temp_lock_view.go）。
- 结果：send-end commit 12515/62.58%、unfinished 7419；**commit 也在 ~15:25(约1h) 停摆**，与 P1 几乎同时。
  → **wound 不是那次 ~1h“失活”的根因**（A/B 证伪）。~1h 点更像“可提交的耗尽、剩真卡死核心”。
- 环境现况：199 可达、磁盘 ~67G 空闲（旧大日志已清）；logs 目录含 nowound **短窗口**日志(4.9G)；
  worker 4 leader 现网日志 3.7G/4.4G/2.7G/3.3G（已跑至停摆 15:25，未下载）；`/home/zjnu/nowound_monitor.log` 有采样记录。

## 5. 已讨论的设计方向（下一步候选，未定稿）
1. **对链上已锁 partial winner 做有界整单 abort**（origin 见“已放好腿 + 某腿 N 块不进”即整单 Rollback CRTx，
   释放全部已上链腿）——非超时/有界，非 CMH 判环。
2. **verify 链上锁冲突 → 立即整单 die（`sendRollbackVoteForDie`）** 而非软重试——消灭 partial winner；代价=rollback 上升。
3. **CMH 扩展覆盖 sink/“低被高卡”**：把“prepared 后迟迟无 quorum”建模成可判等待边。
4. **移除 wound 的并发代价量化**（见 §6.2，A/B 已能对比）。

## 6. 新 session 任务（建议顺序）
### 6.1 先收 nowound A/B 平台期（数据已在 worker）
- **务必选择性下载 4 个 leader**（勿跑全量 `download_log`，磁盘紧张会打满 199）：
  `.200-9000 / .201-9040 / .202-9080 / .203-9120` 的 `ssc-validator-*.log`（现网 3-4.5G each）到 logs 目录。
- 算 nowound 真实 committed（跨 4 leader 去重 `MarkCommittedOnChain`/`leader close commit:true`）→ unfinished 平台，
  与 P1(15528/~4.2k) 对比 → 确认“移除 wound 是否改变平台期”。
### 6.2 量化移除 wound 代价
- 对比 nowound vs P1：`retryCommitTryLockFail` / `retryCommit failed` / `reservation skipped` / `set signal` /
  `wound`(=0) 等计数，估计并发 tryToReSimulation 因无抢占导致的额外失败/重试。
### 6.3 回到根因：链上锁泄漏的整单逃生
- 以 §2/§3 证据为准，设计“partial winner 整单 abort”（origin 无进展判定），非 wound、非纯超时。
- 建议先写 DSN 评审再改码；改动经 `go build ./ssc/...`（GOCACHE=/tmp/gocache）验证，远程实验前确认 worker 空闲。

## 7. 代码状态与回滚
- **已提交** `69cd2ec07` = DSN-65(P1/P2)+上一 session 在途对称撤销改动+DSN-65/handoff 文档。
- **未提交**：`ssc/temp_lock_view.go` 的 `canWound→false`（移除 wound A/B 开关）。
  - 想保留基线干净：`git checkout -- ssc/temp_lock_view.go` 回退；
  - 想保留 A/B：作为独立 commit 记录，勿混入其它改动。
- `ssc/api/types.go.bak` 是临时备份，勿提交（可忽略/删除）。

## 8. 实用提醒
- SSH：`ssh -F /dev/null -i ~/.ssh/id_ed25519 -p 10022 -o ... zjnu@10.7.95.199`；worker 200-203 从 199 再跳。
- **199 曾因跑 `test_single` 内置全量 `download_log` 写满磁盘而失联**；恢复=控制台清盘。今后分析一律**选择性下载 leader**。
- 实验启动：199 上 `source conda.sh && conda activate cli-py && source auto_test.sh && test_single`（nohup）。
- 计数用 Info 级日志；日志勿裸 grep（用 ssc_grep.sh / remote_ssc_retry_stats.py，注意其对大文件较慢）。
