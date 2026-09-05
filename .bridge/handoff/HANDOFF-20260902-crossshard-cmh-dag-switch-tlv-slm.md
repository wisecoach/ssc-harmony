# HANDOFF-20260902-crossshard-cmh-dag-switch-tlv-slm

> from_session: 20260902 延续 HANDOFF-20260902-crossshard-cmh-followup.md
> 本轮目标：给 DAG 加开关做消融 → 明确 TLV/SLM 下 CMH 的记边/探测语义 → 用单测验证"2-环能判出环、victim 选最低优先"
> 状态：**已落地可编译；两个单测 PASS；但尚未跑真实压测，仍有 TODO**

---

## 0. 一句话现状

给整个 off-chain DAG/PatchPool 加了 `enabled` 开关（**默认关**），并修正了 CMH 的核心语义：
**"被卡就记边（含低被高回程边），只有高被低才探测"**。
用一个不依赖 BLS/网络的构造单测证明了 **2-环 A→B→A 能判出环、victim=B**（此前判不出）。单测还揪出并修了 3 个真实 bug。

---

## 1. 本轮改动（文件清单）

| 文件 | 改动 |
|---|---|
| `ssc/api/types.go` | `TimeoutConfig` 新增 `EnableDAG bool`（默认 false） |
| `ssc/patchpool.go` | `offChainDAG` 加 `enabled bool`(零值=false=默认关) + `isOn()`/`SetEnabled()`；所有方法自短路；**`isPatchFinalized` 关时恒返回 true** |
| `ssc/retry_scheduler.go` | `newRetryScheduler` 按 `config.EnableDAG` 设开关；`onChainPatches`/链读取/signal 门控；**Phase1 & Phase2 改"一律记边"**（去掉方向/finalize 预过滤，探测交 detector）；**Phase2 记边时 global + pending 两张 map 同时查** |
| `ssc/verify.go` | 兜底触发改**统一 holder 查询**：global 写 → global 读 → **pending `FindPendingLockHolder`** → TLV |
| `ssc/deadlock_detector.go` | **记边/探测解耦**；成环判定提前到去重前；去掉转发优先级剪枝；`triggerProbe.Path` 含 init+holder；若干日志 nil 保护；单测缝 |
| `ssc/deadlock_detector_test.go` | 新增两个单测（见 §3） |

> 注：`ssc/simulation_test.go`、`ssc/simulation_params_test.go` 是**历史遗留、与本次无关、本就编译不过**的测试文件；跑单测时临时移走、跑完已恢复。
>
> **模型澄清**：pendingStates 对后续交易与 globalLockedStates **等价**（都是"链上锁"），只是同块未 commit 时存在 stateDB 的 pending map、尚未 merge 进 SLM。所以找链上持有者就是**同时查这两张 map**（日志里 pending 冲突误标成 "[TempLockView] locked by tx"）。

---

## 2. 语义定稿（TLV / SLM 的 CMH 统一规则）

**记边（无条件，方向不限）**：只要 waiter 在 shard S 被 holder 卡住就记 `waitEdge`，
补全完整 WFG —— 这是环能闭合的前提（含低被高的"回程"边）。

**探测（仅高被低）**：只有"高优先 A 被更低优先 B、且 B 不可 wound（链上 / TLV 已 finalize）"才发起探针找 victim。

| 持有者所在层 | 找 holder 的方式 | 记边 | 探测 |
|---|---|---|---|
| SLM global 写/读 | `GetLockHolderMeta` / `GetRLockHolder` | 一律 | 高被低才探 |
| pending(同块，日志误标"[TempLockView]") | `FindPendingLockHolder` | 一律 | 高被低才探 |
| TLV(tempLockView) | `GetTempLockHolder` | 一律 | 仅已 finalize(恒true, DAG关) 且 高被低 |

DAG 关闭后（本轮默认）：
- `isPatchFinalized` 恒 true → TLV `canWound` 失效 → wound 整体关闭 → 所有冲突都落到 CMH（纯 CMH 消融）。
- `isKeyPatchCovered` 恒 false → 无 patch 覆盖豁免。
- patch 救援（Phase1b/2b）关闭 → 冲突不再被自动消解，全落成 waitEdge。

---

## 3. 单测（如何复现）

```bash
# 需先临时移走两个编译不过的历史测试文件：
cd ssc && mv simulation_test.go /tmp/ && mv simulation_params_test.go /tmp/
cd .. && export TOP=~/go/src/github.com/harmony-one
export CGO_CFLAGS="-I$TOP/bls/include -I$TOP/mcl/include"
export CGO_LDFLAGS="-L$TOP/bls/lib"
go test ./ssc/ -run "TestDetector_" -count=1 -v
# 跑完恢复：mv /tmp/simulation_test.go ssc/ ...
```

- `TestDetector_LowPriWaitAlsoRecordsEdge`：A→B 与 B→A 都记边；`shouldTriggerProbe` 仅高被低为 true。
- `TestDetector_TwoRingClosureAndVictim`：驱动 `A→B→A`，断言 `ringDetected==1 && victimDied==1 && victim==B`。

结果：**两个都 PASS**；`go build ./ssc/...` EXIT=0。

---

## 4. 单测揪出并修掉的 3 个真实 bug

1. **成环判定被 seenProbes 去重挡住**：去重在 `Current==Init` 之前；发起者自处理初始探针把 `(init,nonce)` 记入 seen，探针绕回时被去重丢弃 → 环判不出。**改**：成环判定提前到去重前。
2. **转发仍按优先级剪枝**：低被高回程边虽已记录，但 `HandleProbe` 只沿"高被低"转发，探针回不到 Init，2-环闭合不了。**改**：去掉转发剪枝（"高被低"只属发起探测 `shouldTriggerProbe`，不阻探针沿完整 WFG 回程）。
3. **`triggerProbe.Path` 漏记 holder**：Path 只 `[init]`，中间节点丢失 → victim 曾错选 A。**改**：`Path=[init,current]`，之后每跳追加下一跳 holder。

---

## 5. 待办 / 下一步

1. ~~`retry_scheduler.go` Phase2 的 pending 兜底~~ **已做**（20260902 补充）：Phase2 记边时 global + pending 两 map 同时查（见 `retry_scheduler.go` 冲突循环）。
2. **`edgeValid` 转发跳统一到 pending/TLV**：目前转发跳重验证仍只用 global（`edgeValidOverride` 只是单测缝）。要把 §2 的 holder 查询逻辑也接进 `edgeValid`，避免转发跳把真实 pending/TLV 边误判 stale。
3. **真实压测复跑 rate=200**：验证 `unfinished→0`、`rollback` 有界、`deadlockRingDetected/VictimDied > 0`、leader waitEdge 稳定非 0。
4. 压测前建议先开启 `deadlockDetector` 的 reason 日志（每个 `shouldTriggerProbe` 拒绝 / `edgeValid` 失败原因），确认边覆盖率确实上升、stale 是否消失。
5. 若需对照 DAG 行为，把 `TimeoutConfig.EnableDAG` 置 true 再跑一次（本轮默认 false）。

---

## 6. 构建 / 环境

- 远程：`zjnu@10.7.95.199 -p 10022`（`-F /dev/null`）
- 编译：`export TOP=~/go/src/github.com/harmony-one && export CGO_CFLAGS="-I$TOP/bls/include -I$TOP/mcl/include" && export CGO_LDFLAGS="-L$TOP/bls/lib" && GOCACHE=/tmp/gocache GOFLAGS=-mod=mod go build ./ssc/... ./core/... ./cmd/... ./node/...`
- 参考：`HANDOFF-20260902-crossshard-cmh-followup.md`、`docs/designs/active/DSN-53-crossshard-deadlock-detection.md`
