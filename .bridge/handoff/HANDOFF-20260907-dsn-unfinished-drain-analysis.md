# HANDOFF-20260907-2: 重新跑一轮实验 → unfinished 收敛慢的根因定位（吞吐 vs 热点 vs DAG 覆盖）

> 承接 HANDOFF-20260907-dsn60-64-…-next-unfinished.md 的“下一 session 任务”。
> 本轮：**代码回退 DSN-63（保留 DSN-64/61/62B′CD）**后，在 rate=200 / shard=4 / delay=10 /
> vpn=4 重新 clean 跑一轮（结果 20260907_124157），并对 fresh ssc-validator logs 跑
> `remote_ssc_retry_stats.py` + txs.csv 逐 tx 时序分析。结论：**unfinished 大头不是“均匀吞吐不够”，
> 而是深跨分片(高 cross_cnt)长尾集中在一个热分片，DAG 只救浅热点(≤depth2)，深链基本走串行/锁冲突慢路径。**

## 0. 一句话总结
submit 192 tps 期间 commit 稳定 ~108 tps → 发送期堆 ~8700 backlog（结束即 ~7850 unfinished）；
发送一停，commit 不是保持 108 继续清，而是**塌到 ~5-20 tps**。剩余 unfinished 高度偏斜：
- 跨分片深度 cross_cnt=4~10 占大头（unfinished 里 cross_cnt=1 仅 9%，committed 里占 54%）；
- 集中在 shard1（u3256，commit:u≈0.79），shard2 却几乎空闲（u906，c:u≈2.7）；
- 多数 unfinished 还停在 simulationNum 0-1（排队/在途，不是重试风暴）。
DAG 确实在工作（chainTxDetected 6956、depth2 11898、patchHit 8584 救 69% 冲突），但只覆盖浅热点；
深跨分片靠 simDAGPatch/链式救援几乎不命中（hit 90 vs miss 38505，genuinely-uncovered 38868），
于是大量深链 tx 走串行锁/retry 慢路径 → 收敛慢。

## 1. 本轮做了什么
### 1.1 代码 / 环境
- 本地 HEAD `421243b85` = 在 DSN-64 `6ee80436f` 上 **Revert DSN-63** + handoff 文档。
- rsync（带 -F /dev/null -i key，排除 .git/bin/db/tmp_log/logs/output）→ 199；校验
  `ssc/internal_pool.go` 与本地一致（34 行删除 = DSN-63 回退生效）。
- 199 上 `source conda.sh; conda activate cli-py; . auto_test.sh; test_single`（nohup，12:35 启动，12:41:57 rc=0）。
  preset 强制重编译 → 本轮跑的是 DSN-63 已回退、DSN-64 保留的代码。
- 先清理了上一轮遗留的空转 client（simulate_sscc.py）/ pprof viewer。

### 1.2 结果文件
- result: `data_process/output/throughput/RATE=200/HMY-SSCC/20260907_124157_result.txt`
- txs:  `.../20260907_124157_txs.csv`（19999 行）
- logs: `~/go/src/github.com/harmony-one/logs/harmony-sscc/shard=4_validator=4_ssc=1_delay=10_rate=200_vpn=4/`（16 个 ssc-validator，fresh）
- 聚合输出: `/home/zjnu/retry_stats_124157.out`

## 2. 结果
| 指标 | 值 |
|---|---|
| total / commit / unfinished / rollback | 19998 / **12042 (60.2%)** / **7850** / 106 |
| timeout / retry_limit_exceeded | 0 / 0 |
| TPS / submit_tps / duration | 116.06 / 192.74 / 103.8s |
| per shard | s0 c2998/u1397; **s1 c2562/u3256**; s2 c2447/u906; s3 c4035/u2291 |

**commit 时序（10s 桶，自 send 起）**：发送期 +10~+100s 每桶稳定 ~1050-1100（≈108 tps）；
+110s=638（send 结束），随后 **+120~+130s 塌到 52-66**，再慢慢爬回 101/115/139/150/156/172/220（至 +210s）。
到 +210s 累计 commit=12042，剩 7850。

**unfinished vs committed 的 cross_cnt 分布**（决定性证据）：
- committed：cross_cnt=1 占 6511/12042≈54%，随深度衰减；
- unfinished：cross_cnt=1 仅 722/7850≈9%，而 4=954、5=550、6=1164、7=514、8=535、9=670、10=466…→ 深跨分片链占绝对主导。
- simulationNum：unfinished 里 0=5796、1=1579、2=284…（长尾到 27）→ 大多仍在一稿/排队，非重试风暴。

**DAG / retry 聚合（remote_ssc_retry_stats.py, 16 logs）**：
- chainTxDetected 6956；chainDepthDist depth1=27136 / depth2=11898 / depth3=631 / depth4=55 / depth5=10；
- retryCommitPatchHit 8584 vs PatchMiss 6 → DAG 救起 69.2% 同 key 锁冲突；dagHoldProtected 9960；
- **simDAGPatchHit 90 vs simDAGPatchMiss 38505**；genuinely-uncovered 38868；
- chainReadyAdmission 37789 / chainReadyCandidate 97430；dagPerBlockMaxSameKey=8（>1 的块 340）；
- leader 口径 chainTx 去向：chainTxCRCommitted 0、no-commit close 139、≈仍在途/未终局 7507 ≈ unfinished 7850。

## 3. 分析：为什么 unfinished 消化这么慢
1. **不是“量太大、均匀吞吐不够就慢慢清完”**：若只是吞吐天花板，send 结束后应保持 ~108 tps 清空；
   实际 send 一停 commit 塌到 ~5-20 tps，说明剩的是“难啃子集”，不是普通排队量。
2. **剩的是深跨分片链 + 单个热分片**：unfinished 被 cross_cnt 4-10 的深链主导，且集中在 shard1
   （饱和、堆 3256），shard2 空闲（906）→ 各分片负载不均，整体被最热分片/最深链拖住。
   跨分片多跳 tx 必须逐跳等上游 commit/就绪，天然慢。
3. **DAG 在工作但不覆盖深链**：链确实形成（depth2 11898、最深 5、340 个块同 key>1、patch 救 69% 同 key
   冲突），说明 DAG 不是失效；但它的救援主要落在浅热点(depth≤2)。对深跨分片，simDAGPatch 几乎不命中
   （90 vs 38505），genuinely-uncovered 38868 → 大量深链冲突只能串行锁/retry，这才是收敛慢的主因。
4. **多数 unfinished 停在 sim 0-1**：不是 CPU 空转/重试风暴，而是“等上游/跨分片就绪/reservation”，
   瓶颈是跨分片协调延迟 + 单热分片，不是单机吞吐。


## 3.1 ⚠️ 修正（用户指出 + BUG-13 佐证）：cross_cnt ≠ 慢的“因”
**用户问题**：多跳不是也通过 ≤4 个 SimTx（每 related 分片一条）就完成提交了吗，为什么多跳就慢？
**确认**：是的——提交条数由 related 分片数(≤4)决定，与跳数无关（DSN-53：CXT 需所有 related 分片 verify+锁+
commit 投票，origin 凑齐才发 CRTx）。`cross_cnt` 是 call_tree 里跨分片 frame(hop) 数（见 call_txs_data.csv 的
tx_args：每 frame 带 shard_id/parent_index），不是提交条数。
**那深(多跳)为何显得慢**：
1. 模拟阶段要**串行走完整棵 call tree**：每 hop 是 leader→leader 嵌套跨分片调用，父 frame state/返回值须先算完
   再喂下一跳 → 因果依赖不可并行；tree 越深/越宽，模拟期串行跨分片 RTT 越多（提交条数却不变）。
2. 提交是**全分片 all-or-nothing**：端到端 ≈ 最慢的 related 分片；多分片 tx 落到更多分片 → 更易踩到热分片(shard1)。
3. 深链最易被**就绪/DAG 门扣住**（DSN-54/55/57/60/64）：下游要等上游 onchain/ready-quorum → 停在 waiting（sim=0/1）。
**勿把 cross_cnt 当因**（BUG-13 §6.3 已证伪）：热分片连 cross=1 都慢、冷分片 cross=33 也快；瓶颈是**分片整体负载/
leader pool→simulate 调度**，cross_cnt 只是“需等最慢分片 + 阶段多”的标记/放大器。故上表“深跨分片 + 热分片”中，
**热分片才是主因**，深跳是它在 unfinished 里占比高的放大器。下一步应查热分片(shard1)调度/吞吐与深链在热分片的就绪等待。


## 3.2 具体卡死点（逐 tx 追踪，124157 结果）——比 §3/§3.1 更精确
对 3 笔 unfinished（shard3 深 cross=15、shard1 深 cross=8、shard1 单 cross=1）逐 hash 追踪 16 个 ssc-validator log，
三笔**卡法完全一致**：
1. **首败**：跨分片 call 在某分片(9080/9120/9040) 命中 `IsKeyAvailable: genuinely-uncovered lock conflict (simDAGPatch miss)`
   → `state is locked by other tx` → `failed to simulate transaction` → `added to retry pool`（仅 1~2 轮，sim=1/2）。
2. **卡死/不收敛**：进 retryScheduler 后，其 RW key **每轮都被更高优先级候先预约**，
   持续打印 `[retryScheduler] reservation skipped candidate (key already reserved by higher-prio)`（约每 1.5~2s 一次，
   与 `set signal` 交替），**永远轮不到重跑** → 无任何终局（无 leader close/commit/rollback）。偶发
   `anti-starvation force-admit (starved>=N)` 也救不回来（仍被更高优先压过）。
3. 时间跨度 12:37:01 → 12:40:2x（~3.5min，到抓取窗口结束仍在 retry），无终局 → 记为 unfinished。

**规模（全 16 log 去重 txHash）**：added_retry=11219、reservation_skip(**debug 级**)=10427、
uncovered=9420、state_locked=7459、**uncovered ∩ reservation_skip=8824**。即 ~10k 笔都进过“被更高优先抢走
预约”的死循环；与 ~7850 unfinished 同源。
**结论（比“深跨分片/热分片”更根本）**：收敛慢的**直接机制 = 未覆盖锁冲突(genuinely-uncovered) → retryScheduler
里被更高优先级 reservation 持续饿死**（Wait-Die/优先级预约造成）。深/浅、热/冷分片同一种死法；深 tx 只因碰到的
key/分片多而更易命中，单分片在热分片(shard1)同样被饿（例3）。下一步应聚焦 retryScheduler 预约的优先级公平/
反饿死（为何更高优先 tx 长期占住 key，force-admit 阈值是否过低/过高），而非跳数或分片数。


## 3.3 4.5 小时后复检：不是“纯饿死”，确有真卡死(liveness)分量（回应用户“等30分钟还不清”）
fresh 网络从 12:35 一直跑到 17:05+，对 §3.2 那 3 笔 unfinished 用当前 worker log 复检（同一交易 4.5h 后）：
- **tx2 (shard1 深 cross=8)**：12:40:35 出现 `leader close, commit:true, CXT_COMMITTED, SUCCESS` → **最终提交了**
  （是“慢但会清”的 transient 分量，仅比 12:40 快照晚 ~35s）。
- **tx1 (shard3 深 cross=15)**：最后事件 **17:05:38 仍是 `set signal`**，4.5h 无任何 close/rollback → 真卡。
- **tx3 (shard1 单 cross=1)**：最后事件 **17:05:54 仍是 `set signal`**，4.5h 无终局；期间 reserv_skip=7971、
  **anti-starvation force-admit=159** 次仍不完结 → 真卡（被 force-admit 159 次也收敛不了 = liveness 失败）。
**结论**：unfinished 里两类并存——(a) transient：慢但最终 commit/rollback（如 tx2，解释了 7929→4731 的下降）；
(b) **真卡死/活性失败**：4.5h 仍 `set signal` 循环、无终局，且从不被选为 CMH/rollback victim（rollback=0）
（如 tx1/tx3，对应 ~4731 平台期）。所以用户“等30分钟没清掉”是因为存在真正的 stuck 子集，不是单纯饿死。
下一步：查这些 stuck tx 为何 force-admit/anti-starvation 也不收敛、为何 deadlock detector 从不把它们选为 victim
（是否在 CMH 环外 / 环判定漏掉 / ready-quorum 卡在别分片）。


## 3.4 长窗口确认(17:09 result)：剩余 4749 基本全部卡死 + 卡死模式分类
同一轮 fresh 跑到 17:09 落盘 `20260907_170923_result`：commit 15036(75.2%) / **unfinished 4749** /
rollback 213——与 4.5h 逐笔判断一致，unfinished 稳定在 ~4750（平台期）。4749 构成：shard1=1978(热)、
shard3=1402、shard0=851、shard2=518；sim 分布 sim=1:2866、sim=0:1055、sim=2:452；cross 仍偏深(6:785,4:522…)；
全部在 104s send 窗口内提交、已在途 4.5h。对当前-unfinished 再抽 4 笔(.201/.200/.203 各自 leader)复检：
**close=0 / rollback=0（4.5h 无终局）**，且仍在打转——两类卡法：
1. **热 key 同组活锁(livelock)**：s1a/s1b/s0a 的 reserv_skip≈8216/8226、anti_starv≈164(**三笔几乎同数**)
   → 它们同属一个在**同一批热 key** 上互相争抢的群体；anti-starvation 每轮把整组(≈164)一起 force-admit，
   放行后又立刻互相冲突被更高优先抢走 → 永不收敛（活锁陷阱，不是简单饿死）。
2. **跨分片 ready-quorum 卡死**：s3a(shard3 origin) 在本 shard `ready:true, sim=2`、reserv_skip=0，
   但 remote related shard 永不确认 → origin 永远凑不齐 commit 票 → 无终局。这与 DSN-53 ready-quorum/
   死锁环对应，且这些 stuck tx 从不被选为 rollback/CMH victim(rollback=0)。
**结论（回应用户“剩下的应该全部是卡死”）**：成立。4749 在 4.5h 内一个都不再终局，全在活锁/ready-quorum 等待里。
修复方向：(a) 热 key 同组活锁——force-admit 应保证“放行后有且仅有一个真正推进/其余让位或终局回滚”，否则整组
再碰撞；(b) ready-quorum 跨分片等待——为何远端 related shard 永不 ready/永不投 commit 票、CMH 为何不挑它们做 victim。


## 3.5 聚焦：DAG 标记(isChainTx)的交易是否也卡在 unfinished？（用户重点）
定义：tx 在 VerifySimulation 判定 `isChainTx=UpstreamTxList>0` 即被 DAG/chain 标记
（log "VerifySimulation: chain tx detected" + retryScheduler.chainTxMarks，见 verify.go:399、retry_scheduler markChainTx）。
对 170923 全量 4.5h log 扫描 "chain tx detected" 去重后与 4749 unfinished 求交：
- 去重 chain 标记 tx = **2077**；其中已 finish(commit/rollback)=1491(≈72%)，unfinished=586。
- **unfinished 4749 里只有 586(≈12%) 是 DAG 标记**；**4163(≈88%) 从未被 DAG 标记**。
分片：DAG-stuck 集中在 shard0(169/851) 与 shard3(210/1402)，shard1 只有 91/1978；
non-DAG-stuck 全分片主导（shard1=1887 最重）。cross：DAG-stuck 偏深(cross8=79,6=62,4=61…)，
non-DAG-stuck 也深(6=723,4=461,9=403…) 但含不少浅(1=259)。
**结论**：永久卡死的 4749 绝大多数(88%)是**从未被 DAG 覆盖的普通冲突交易**（genuinely-uncovered →
reservation 饿死）；DAG 标记本身与“能终局”正相关（72% 标记 tx 已结束）。那 586 笔 DAG-stuck 主要是
shard0/3 的深跨分片(ready-quorum/远端永不确认)型——DAG 救起某条腿也救不了“origin 凑不齐跨分片 commit 票”。


## 3.6 DAG-stuck 确切机制（逐腿实证，210109 patch 后仍卡）
目标：DAG-stuck 必须归零。对 DAG-stuck 交易 0x6e43c94b(shard0 origin, cross2, sim1) 逐分片腿状态：
- **shard0(origin 9000) + members 9002/9004/9006**：每分片都有 `DAG_RESOLVED` + `COMMIT_VOTE`(20:34:13 发 CXTCommitVote)
  → **origin 本分片腿已放好并投 commit 票**；但 leader 9000 仍在 775 次 `set signal`+RETRYCOMMIT_OK，等别腿。
- **shard1(9040, 另一 related 分片)**：`UNCOVERED=2, STATE_LOCKED=2, ADDED_RETRY=1, RESERV_SKIP=589` → **这条腿是
  非 DAG 的真未覆盖冲突**，进 retry 后一直 `reservation skipped (higher-prio)`，**从不投 commit 票** → origin 凑不齐
  commit quorum → 整单卡死。
**实证结论**：DAG-stuck = **跨分片 ready-quorum 卡死**：DAG 覆盖到的那几条腿（含 origin 本分片）都已 vote，
但至少一条 **related 分片是“非 DAG、真未覆盖”的腿**，它永远锁不上/不投票；因整单被 origin 标为 DAG（chain 保护），
正常 wound/victim 逃生被禁用 → 永久卡死。我的“RetryRollback 撤保护”patch 没救回：origin 每次失败仍被重新标 DAG
(HandleRetrySignal 重新 markChainTx) → 继续对那条腿 RetryCommitDAG → 那条腿继续 RESERV_SKIP → 死循环。
**归零方向**：需要“整单跨分片 quorum 的有界逃生”——origin 本分片已 vote、却等某条腿持续 UNCOVERED/OnChainConflict
超过 N 块/轮时，**整单终局回滚（含已 vote 的 DAG 腿、释放全部保护）**，把“永久卡”变成“有界内 commit 或整单 rollback”，
使 DAG-stuck=0；而非靠逐腿撤本地保护（无效）。


## 3.7 决定性 A/B：全套 DAG 撤销/Fix-B/StaleTx patch 未消除平台期（实测）
对 234148（含：RetryRollback 撤免wound + Fix-B 得锁即保 + StaleTx 终局撤销）跑 ~3.5h 长窗口(030312/031330)：
- **unfinished = 4711**（030312 与 031330 完全相同 = 已平台期）；基线 170923(无 patch, 4.5h) = **4749** → **几乎没动(-38，噪声)**。
- commit 15064 vs 基线 15036(+28)；rollback 225 vs 213。
**结论**：用户提出的“正确的 DAG 撤销机制能避免 stuck-DAG 锁死其他交易”已被实测否定——无论怎么在 RetryRollback/终局
撤销 DAG 的免wound 标记(protectHeldLocks/chainProtected/chainTxMark)，长窗口平台期都停在 ~4710，不下降。
**根因不在“DAG 残留的免wound 标记”**：stuck 主体是 (a) 撞链上锁 holder 永不放 + (b) reservation 饿死的 non-DAG
(88%)；这些 holder 不是靠“撤我们自己的 DAG 免wound”能释放的。这 3 个 patch 均无收益 → 建议整体回退保持基线干净，
把火力转向“那永不释放的 holder 是谁/为何不动 + 跨分片 quorum 为何凑不齐”。

## 4. 结论（回应用户问题）
unfinished 收敛慢 = **深跨分片(cross_cnt 4-10)多跳链交易的长尾**，集中在 shard1；DAG 已生效但只覆盖
浅热点(≤depth2)，深链多跳大部分走未被 DAG 覆盖的串行锁/retry 路径（simDAGPatchHit≈0、genuinely-
uncovered 高）。所以单纯“加吞吐/减量”不是重点，重点在：(a) 深链跨分片的就绪/救援覆盖，(b) 热分片均衡，
(c) 长尾是否有真卡死(平台期)。

## 5. 建议下一步
- **先做长窗口复测**：worker 网络仍在跑（下载后 ~350 块继续消化）；间隔后再 `download_log` + `DATA_HANDLER`
  重算，看 7850 是否继续降、是否有平台期（区分 transient vs 真卡）。复用“长窗口法”。
- **为什么 simDAGPatchHit≈0 / genuinely-uncovered 38868**：查 DAG 子图广播/到达时序（imported 15110 vs
  sent 7555，可能成员先 verify 后 patch 才到）与“uncovered”冲突到底是同分片还是跨分片 RW。
- **热分片 shard1 归因**：按分片看 hot-key / cross_cnt / leader 是否饱和；为何 s1 堆而 s2 空。
- 统计口径修正：rollback '?'=82950 是聚合脚本 reason 正则没匹配上（result 实际 rollback=106），勿当真。

## 6. 提醒
- 结果/logs/聚合输出位置见 §1.2。改码仍按 AGENTS 规范：先设计→实现→`go build ./ssc/...`(GOCACHE=/tmp/gocache)
  →同步→核验 bin→跑实验。计数用 Info，日志勿裸 grep（用 ssc_grep.sh / zero_grep.sh）。
- 本轮 rsync 排除了 .git；199 的 git HEAD 仍是 6ee80436f（工作区 internal_pool.go 已是回退态）。
