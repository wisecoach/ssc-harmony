# HANDOFF-20260907: 从“上游未上链回滚”根因到 DSN-64，下一 session 聚焦 unfinished 收敛慢

> 交接给新 session。本 session 把“为什么还有大量 rollback / upstream-not-onchain”彻底定位并修到
> rollback 大幅下降；剩余重点转向 **unfinished 交易为什么收敛这么慢**（交易量 vs 单热点，DAG 是否真生效）。
>
> ⚠️ 建议先读本文件 §0/§5，再读对应 DSN 文档（active/DSN-60/61/62/63/64）。

## 0. 一句话总结
- 根因链：TaskB(DSN-60) 已把 internal_pool 的“上游先于下游”做进池就绪门，但 `upstream-not-onchain`
  rollback 仍高。逐层排查 wound/保锁/排序都不够 → 单 tx 轨迹找到真根因：**U(上游)上链很快，但
  CR commit 后 `RemoveOnChainDAGPatch` 把临时 patch 清掉；一批下游 D 是 U 还在 in-flight 时被救援
  创建、引用 U，真正 verify 时 U 早已 commit+被移除 → DSN-57 误回滚**。
- 修复 DSN-64：持久记录“已 CR Commit 的上游”(`committedOnChain`)，`upstreamOnChain` 对已 commit 的 U
  也判就绪。**rate=200：upstream-not-onchain 2652→416，终局 rollback ~606→88，commit_rate ~60% 不劣化**。
- 已确认无“真缺口”（U 同分片、事件前已 commit 仍回滚 = 0）。残留 416 = U 在本分片没 commit
  （reservation 饿死 / 跨分片腿不同步）+ 少量 D 抢跑在 U commit 前。
- **未实现**（用户决定先不做）：把 DSN-57 终局回滚改 retry。**先保持现状彻底回滚。**

## 1. 本次 session 做了什么（流水）
### 1.1 实验/环境
- 连 `zjnu@10.7.95.199`（-p10022，-F /dev/null -i key -o IdentitiesOnly），worker 节点 .200–.203，
  客户端 .196–.199；`bin/` 需 preset 重编译后 rsync 到 worker。
- 实验入口：`. sync_code.sh && uploadCode zjnu@10.7.95.199`（rsync 需带 -F /dev/null -i key 参数，
  裸 ssh 会 Bad owner/permissions）；远端 `conda activate cli-py && . auto_test.sh && test_single`。
- `.env` 控制参数：RATE/SHARD_NUM/VALIDATOR/DELAY/VALIDATOR_PER_NODE/TX_NUM/EXPERIMENT_TYPE。
  结果输出目录按 .env RATE 分（RATE=200/HMY-SSCC/…）。**跑前确认 .env RATE 与目标一致**。
- 客户端卡死修复（ssc-cli，独立于 harmony-sscc，需单独同步到 .196–.199）：done 文件原靠进程退出，
  因线程池 executor shutdown 卡住永不退出 → 改 simulate_sscc.py 在发送完成后主动写 done + os._exit
  （见 ssc-cli/cli-py）。**实验时长不可控 → 不用固定超时**。

### 1.2 根因调查（数据/代码证据）
- 分类探针（日志 `ssc-validator-*.log`，勿裸 grep）：
  - `upstream patch not on-chain yet` 事件里，93% 的 U 后来确实上链过（只是 >50 块晚）→ 不是“没上链”。
  - submit→on-chain 块间隔 p50=2 → **U 注册很快**。
  - 单 tx 轨迹（0x9b54e406…）：U commit 后被 RemoveOnChainDAGPatch，多笔 D 在其后 verify → 误回滚。
- 因而方向从“wound/保锁/排序”切到“生命周期错配”。

### 1.3 改动（均已 commit，见 §4）
- DSN-64（有效核心）：`committedOnChain` + `MarkCommittedOnChain` + `upstreamOnChain` 双判。
- DSN-61/62(B′)(C)(D)：上游保锁/被 wound 移除/被动 chainProtected —— 部分有效（2652 前的小幅），
  DSN-63（上游前置）**无收益且有重排隐患**（建议回退，见 §4）。
- ssc-cli 客户端完成通知修复。

## 2. 结果汇总（rate=200 / shard=4 / delay=10 / vpn=4）
| 运行(结果文件) | 代码 | upstream-not-onchain | rollback | commit率 |
|---|---|---|---|---|
| 145305 | DSN-60 TaskB | 4372 | 849 | 57% |
| 173648 | DSN-61 | 2978 | 627 | 57% |
| 191734 | DSN-62(B)(C) | 2702 | 626 | 57% |
| 200600 | DSN-62(B′)(D) | 2894 | 644 | 59% |
| 215102 | DSN-63(污染/无效) | — | — | 13%(不可信) |
| 012529 | DSN-64 | 416 | 88 | 59.9% |
| 020132 | DSN-64 长窗口(+30min) | — | 221 | 75.2%（unfinished 7929→4731）|

- 长窗口表明 unfinished 大头 transient（多跑 30 分钟 ~3200 流向 commit），但仍有 ~4731 挂着。

## 3. 上一 session 交接要点（沿用）
- 见 HANDOFF-20260905-…（TaskB/DSN-60 定义）与 docs DSN-52~60。DSN-57 已 REVERT，勿重蹈“跨分片扣留饿死”。

## 4. 代码状态与建议
- HEAD `6ee80436f`（DSN-64）。commit 链：DSN-60 `75d43d106` → DSN-61/62A/C `a8836de65` →
  DSN-62B `f39e7eccc` → 回退(A) `784597724` → DSN-63 `ca7f86da7` → DSN-64 `6ee80436f`。
- 建议：**回退 DSN-63 `ca7f86da7`**（clean 跑无收益，且“无脑上游前置”有饿死普通交易/放大冲突隐患）；
  保留 DSN-64 与 DSN-61/62(B′)(C)(D)。
- 工作区残留：`AGENTS.md`（中文版重写，pre-existing 未提交）、`ssc/api/types.go.bak`(未跟踪)。勿误提交。
- ⚠️ **曾踩坑**：`test/dev_deploy.sh` 曾被写坏（行36 `function upload_for_servers() {as`），导致
  test_single 的 clean/preset/deploy 全失效、节点起不来；已修复。同步/改码后 `bash -n` 校验关键脚本。

## 5. 下一 session 任务：unfinished 为什么收敛这么慢
**目标**：判断 unfinished(长窗口后仍 ~4731/rate=200) 到底卡在“交易量太多(吞吐不足)”还是“单一热点重
(DAG 消化热点能力不足)”，以及 **DAG 是否真在正确生效**。

### 建议切入点（用户提示）
1. 理论：要么交易数量多（性能足够则不是瓶颈），要么单热点重（DAG 应能消化热点）→ 到底什么卡住？
2. **看最新区块的交易分布**：每块 SimTx/CRTx 数、hot-key(同 key 写多次)分布、chainTx 占比、
   是否成链（depth≥2）。
3. **DAG 是否真生效**：用聚合器 `remote_ssc_retry_stats.py` 看
   `chainTxDetected / chainDepthDist / chainDepthCommitDist / dagHoldProtected / chainReadyAdmission /
   retryCommitPatchHit / simDAGPatchHit / reservation skipped / genuinely-uncovered`。
   若链深都=1 / dagHoldProtected 低 → DAG 链式救援没真正形成，热点靠纯串行 → 慢。
4. 区分 transient vs 真卡死：对未终局 tx 做 stage 分类（retryPool 排队 / in-flight / 跨分片 ready /
   reservation 饿死 / 锁泄漏）。可再等一段重算看 unfinished 是否继续降（平台期=真卡）。

### 参考日志/结果位置
- 结果：`~/go/src/github.com/harmony-one/ssc-cli/data_process/output/throughput/RATE=200/HMY-SSCC/20260907_020132_result.txt`
- 日志：`~/go/src/github.com/harmony-one/logs/harmony-sscc/shard=4_validator=4_ssc=1_delay=10_rate=200_vpn_4/`（16 个 ssc-validator log）
- 本地分析脚本（/tmp，远程也有）：`rem_eval.py`(upstream-not-onchain)、`rem_gap*.py`(生命周期)、
  `remote_ssc_retry_stats.py`(DAG/retry 聚合)。按需重传。
- 复用长窗口法：跑完 test_single 后节点仍跑，间隔后 `download_log` + `DATA_HANDLER` 重算即可得长窗口结果。

## 6. 风险/提醒
- 改共识关键路径必须：先设计文档→实现→`go build ./ssc/...`(用可写 GOCACHE=/tmp/gocache)→同步→
  **核验 worker bin 新鲜(nm 查新符号)+节点起来**→跑实验→对比。勿手动删 worker db/杀节点越权干预。
- 计数用 Info 级；日志勿裸 grep。
