# HANDOFF-20260903-CMH 链上-only + Wait-Die 弃用：工作小结与新 session 聚焦

> from_session: 20260903 延续 HANDOFF-20260902-crossshard-cmh-dag-switch-tlv-slm.md
> 本 session 完成：DAG 开关、CMH 语义 v2/v3、Wait-Die die 弃用、链上统一锁判定点注入、TLV 探测钩子(后撤)、victim 回滚正确性验证。
> 现状：**CMH 能判环、victim 能正确回滚，但只 die 十几个；去掉 Wait-Die 后交易几乎无法回滚，unfinished 不降反升。**
> 状态：**待新 session 分析日志，回答"为什么 CMH 没把交易救出来"**

---

## 0. 一句话现状

CMH（链上锁 only + 记边/探测解耦 + 只 die 环内 victim）在功能上已通：waitEdge ~700-900/leader、每轮能判出 ~18 个环且 victim 都被正确终局回滚（CXT_ROLLBACKED、跨全部 related shard）。**但**：
- 之前 Wait-Die 存在时：海量回滚(~10万)但 unfinished ~5k 不动（震荡）。
- 现在去掉 Wait-Die（CMH 唯一 die）：**rollback 骤降到 1-2/shard**（几乎无法回滚），unfinished 不降反略升(~7.5k)。

→ CMH 的"少量 victim"远不足以排空几千笔 unfinished，需新 session 查日志定位根因。

---

## 1. 本 session 关键改动（代码）

| 文件 | 改动 |
|---|---|
| `ssc/api/types.go` | `TimeoutConfig.EnableDAG bool`(默认 false) |
| `ssc/patchpool.go` | `offChainDAG.enabled`(默认关)+全方法短路；`isPatchFinalized` DAG 关时返回 false（**不禁 wound**） |
| `ssc/retry_scheduler.go` | DAG 开关/链读取门控；`onChainBlocked`(leader,记持久边+高被低探测)；Phase2 pending 兜底；注入 `deadlockDetector.pendingHolder` |
| `ssc/state_locker.go` | `Lockable` 三个真实链上锁冲突分支 → `maybeRecordChainBlocked` → `onChainBlocked`（**链上统一触发点**） |
| `ssc/verify.go` | **Wait-Die die 弃用**：`dsn52ShouldYield==true→sendRollbackVoteForDie` 删除；链上冲突一律记边交 CMH |
| `ssc/deadlock_detector.go` | 记边/探测解耦；成环先去重；转发去优先级剪枝(沿完整 WFG)；Path 含 holder；首跳不重验；edgeValid=global+pending；单测缝 |
| `ssc/temp_lock_view.go` | 曾加 TLV 探测钩子 → **本轮已撤**（TLV 只 wound，不 probe） |

> 说明：本轮一度按"TLV 触发 + 临时边"实现（DSN-53 附录 D），但最终定稿为 **链上锁才 probe、链下锁只管 wound**，故 TLV 钩子与临时边已全部回退。**DSN-53 附录 D 描述与当前代码不符，建议新 session 删掉或改标"已否决"。**

---

## 2. 关键实验数据（rate=200, shard=4, validator=4, delay=10）

### 2.1 上一轮（链上 Lockable 钩子已上、Wait-Die 仍在）
- waitEdge recorded：9000:739 / 9040:939 / 9080:704 / 9120:670（stale=0）
- deadlock ring detected / victim chosen：9000:7/7, 9040:4/4, 9080:0/0, 9120:7/7（共 ~18 环）
- victim 回滚：验证正确（跨 related shard 全部 CXT_ROLLBACKED）
- 但 unfinished 几乎不动（~5k）

### 2.2 本轮（Wait-Die die 弃用，CMH 唯一 die）
```
Shard 0: commit 3125  unfinished 1291  rollback 1
Shard 1: commit 2619  unfinished 3230  rollback 2
Shard 2: commit 2678  unfinished  699  rollback 1
Shard 3: commit 3992  unfinished 2360  rollback 1
```
- **rollback ≈ 1-2/shard（几乎无法回滚）**，unfinished ~7.5k（不降反升）。
- commit ~2.6-4k/shard（有前进，说明不是完全死锁，更多像 backlog/无法 die）。

### 2.3 结论性观察
- CMH 只 die 环内 victim（十来个），对几千 unfinished 杯水车薪。
- 去掉 Wait-Die 后低优先不再批量 die → 只剩 CMH 少数 victim → rollback 趋 0。
- 大量卡死交易既不前进也不 die，堆积成 unfinished。

---

## 3. 已确认 / 已否
- ✅ victim 被正确终局回滚（回滚链路通：rollbackVictim→Verifier.RollbackVictim→sendRollbackVoteForDie→CRTx）。
- ✅ 链上锁冲突能触发 CMH（Lockable 钩子）。
- ✅ Wait-Die die 侧已从 verify 移除（CMH 唯一 die）。
- ❌ 临时边 / TLV 探测钩子：实现过又回退（定稿为链上才 probe）。
- ❓ 为什么 CMH 只判出 ~18 个环、救不了几千笔——**未解，新 session 重点**。

---

## 4. 新 session 聚焦问题（建议的日志分析方向）

1. **CMH 为何覆盖不到大多数 unfinished？**
   - unfinished 交易里，有多少被记成 waitEdge / 成为环成员 / 被选为 victim？（应占极少数）
   - 大多数 unfinished 到底处在什么状态：
     (a) 已在某分片持真实链上锁(partial winner)却未被判环/被 die；还是
     (b) 从没拿到真实锁、只在 retry 池打转（调度/吞吐/Ready 门）；还是
     (c) 拿到锁后卡在"等其它分片确认"(聚合层)，没有可被 CMH 记的链上锁冲突。
2. **为什么 rollback 趋 0**：是不是低优先该 die 的交易现在既不被 Wait-Die 杀、也不在 CMH 环里 → 永远不死？
3. 关键判定：(a) vs (b) vs (c)。抽几笔 unfinished 跨 4 shard 追状态（是否有 commit vote / 是否持锁 / 最后日志）。
   - 若 (b)/(c) 为主 → CMH(链上锁环) 方向可能不对，需回吞吐/Ready/回收机制。
   - 若 (a) 为主 → 需"成规模 victim/回收"，而非逐个环。

---

## 5. 参考工具 / 脚本
- `~/. .../logs/harmony-sscc/<dir>/ssc-validator-*.log`（leader: 9000/9040/9080/9120）
- `scripts/remote_ssc_retry_stats.py`：聚合 CHAIN_RETRY_STATS / close reason / rollback reason
- `docs/research/analyze_unfinished.py` 思路（added vs closed → unfinished）
- deadlockDetector 计数：`waitEdge recorded` / `deadlock ring detected` / `ring victim chosen`（按 message 精确 grep）
- victim 回滚：`leader close transaction, commit:false, CXT_ROLLBACKED`

---

## 6. 构建 / 运行
- 远程：`zjnu@10.7.95.199 -p 10022`（`-F /dev/null`）
- 编译：`export TOP=~/go/src/github.com/harmony-one && export CGO_CFLAGS="-I$TOP/bls/include -I$TOP/mcl/include" && export CGO_LDFLAGS="-L$TOP/bls/lib" && GOCACHE=/tmp/gocache GOFLAGS="-mod=mod -buildvcs=false" go build ./ssc/... ./core/... ./cmd/... ./node/...`
- 单测：先临时移走 `ssc/simulation_test.go`、`ssc/simulation_params_test.go`（历史坏测试），跑 `go test ./ssc/ -run TestDetector_ -v`，跑完恢复。

---

## 7. 相关文档
- `docs/designs/active/DSN-53-crossshard-deadlock-detection.md`（v2/v3 + 附录 D——**附录 D 临时边与当前代码不符，需修正/否决**）
- `docs/designs/active/DSN-52-crossshard-deadlock-resolution.md`（Wait-Die 弃用声明）
- `docs/research/RSH-03-priority-aware-reverse-cmh.md`（Wait-Die 弃用提示）
- 上份交接：`HANDOFF-20260902-crossshard-cmh-dag-switch-tlv-slm.md`
