# RSH-03: Harmony-SSCC 全模块性能埋点方案

> **版本**：v1（2026-07-23）
> **范围**：ssc/ 目录下全部核心模块
> **目标**：所有模块的关键阶段加上统一格式的耗时埋点，支撑分析脚本量化瓶颈

---

## 1. 现有埋点总览

| 模块 | 文件 | 已有埋点数 | 埋点位置 |
|:-----|:-----|:---------:|:---------|
| **verify** | verify.go | 5 | Verifier.Cleanup / Phase 1.5.5 Copy / sscvm.Call timing / VerifySimulation |
| **simulator_leader** | simulator_leader.go | 2 | startSimulateCXTransaction / handleCXTSSCCall |
| **simulator_member** | simulator_member.go | 6 | handleCXTCall / startReSimulation / aggregateSSCCallRequest / handleSimulateRequest |
| **simulator** | simulator.go | 2 | Simulator.Cleanup timing / SSCCAdapter.invoke |
| **signer** | signer.go | 3 | BLS sign / aggregate / verify |
| **retry_scheduler** | retry_scheduler.go | 4 | RemoveFromPassivePool / StaleTx / RetryCommit / RemoveOnChainPatch |
| **patchpool** | patchpool.go | 1 | PatchPool.Remove |
| **impl** | impl.go | 1 | new epoch broadcast |
| **committee** | committee.go | 2 | UploadSLOpinion |
| **cxt_timer** | cxt_timer.go | 1 | CXTTimerManager.RemoveTx |
| **state_lock_impl** | state_lock_impl.go | 0 | ❌ 无 |
| **state_locker** | state_locker.go | 0 | ❌ 无 |
| **temp_lock_view** | temp_lock_view.go | 0 | ❌ 无 |
| **committer** | committer.go | 0 | ❌ 无 |
| **tx_submitter** | tx_submitter.go | 0 | ❌ 无 |
| **comm** | comm.go | 0 | ❌ 无 |
| **statistics** | statistics.go | 0 | ❌ 无 |

**核心高频路径很多没埋。** 最缺的是 state_lock / temp_lock_view / comm / tx_submitter。

---

## 2. 模块-建议埋点清单

### 2.1 retry_scheduler.go（4 → 15）

```
函数                   新增埋点                      阶段/位置
────────────────────────────────────────────────────────────────
OnBlockCommitted       staleCleanup                   rs:500-506
                       querySubscribers               rs:538
                       sortCandidates                 rs:558-560
                       reservation                    rs:562-593
                       stateCheckLock                 rs:617-634
                       woundedRecovery                rs:672-687
                       hotKeyStats                    rs:690-718
                       lockWaitScan                   rs:722-747
                       statsRange                     rs:769-787

RetryCommit            tlvTryLock                     rs:1223
                       dagSearchPhase1b               rs:1244
                       dagConsumePhase1b              rs:1249-1257
                       dagSearchPhase2b               rs:1340
                       dagConsumePhase2b              rs:1346-1354

tryToReSimulation      parallelRetryCommit            rs:987-1001
                       woundHandling                  rs:1026-1034
                       releaseConsumedPatches         rs:1094-1108

HandleReSimulationSignal processSignals               rs:907-930

sendChainSignal        storePatch                     rs:1424-1428
```

### 2.2 patchpool.go（1 → 8）

```
函数                   新增埋点
────────────────────────────────
findCoveringSet        collectCandidates              pp:336-368
                       greedySelect                   pp:372-395

scanPatchSubscribers   querySubscribers               pp:407
                       findCoveringSetPerCandidate    pp:431
                       reservation                    pp:448-513

tryConsumePatch        tryConsume                     pp:127-138
releasePatch           release                        pp:141-154

AddPatch               addPatch                       pp:1545-1562 (在 retry_scheduler.go)
```

### 2.3 verify.go（5 → 12）

```
函数                   新增埋点
────────────────────────────────
verifySimulationParsed setup                           vf: 开头
                       lockCheck                       vf: 写集+读集
                       subCtxAlloc                     vf: Phase 1.5
                       execVerify                      vf: Phase 3 (已有 sscvm.Call)
                       lockState                       vf: Phase 4
                       handleSuccess                   vf: Phase 5 commit

BatchVerifySimulations phase0_5_arbitration            vf: 仲裁
                       phase1_parallel_exec            vf: 并行执行
                       phase4_serial_lock              vf: 串行锁
```

### 2.4 simulator_leader.go（2 → 8）

```
函数                   新增埋点
────────────────────────────────
CommitTransactions     classifyTxs                     sim_leader.go: 分类
                       simTxProcess                    simTx 子阶段
                       crTxProcess                     CR 子阶段
                       normalTxProcess                 普通 tx 子阶段
                       budgetRemaining                 remainingTime 衰减

StartSimulate          firstSimDuration                首次模拟耗时
chainNextSim           chainBuild                      链式 NextSim 构建
onSimTxCommitted       chainSignalSend                 链式信号发送
```

### 2.5 simulator_member.go（6 → 10）

```
函数                   新增埋点
────────────────────────────────
startReSimulation      preCheck/statePrep              sim_member.go: 预检
                       vmExecute                       VM 执行
                       lockState                       锁状态
                       sendVote                        发送投票

HandleMemberRequest    processRequest                  处理 RPC 请求

aggregateResults       aggregateVotes                  聚合投票
```

### 2.6 state_lock_impl.go（0 → 6）

```
函数                   新增埋点
────────────────────────────────
SetAndLockState        setLock                         LockState map 写操作
LockExecution          lockExec                        CheckLock 检查
CommitTx               commitTx                        提交状态锁
RollbackTx             rollbackTx                      回滚锁
CheckLock              checkLock                       读锁检查耗时
CheckRLock             checkRLock                      读–读锁检查
```

### 2.7 temp_lock_view.go（0 → 5）

```
函数                   新增埋点
────────────────────────────────
TryLockWithPriority    tryLock                         TLV 尝试
GarbageCollect         gc                              清理
OnBlockCommitted       onBlockCommitted                区块提交
ClearWounded           clearWounded                    清理 Wound
IsWounded              isWounded                       检查
```

### 2.8 committer.go（0 → 4）

```
函数                   新增埋点
────────────────────────────────
handleCommittedMessage handleMsg                       处理 CR 消息
doCommit               doCommit                        执行提交
doRollback             doRollback                      执行回滚
processRecallVote      processRecall                   处理 Recall 投票
```

### 2.9 tx_submitter.go（0 → 4）

```
函数                   新增埋点
────────────────────────────────
SubmitCXTTransaction   submitSimTx                     提交 SimTx
SubmitCRTransaction    submitCR                        提交 CR Tx
SubmitNormalTransaction submitNormal                   提交普通 tx
submitRetrySignal      submitSignal                    提交重试信号
```

### 2.10 comm.go（0 → 2）

```
函数                   新增埋点
────────────────────────────────
Call                   callRPC                         RPC 调用
Broadcast              broadcast                       RPC 广播
```

### 2.11 statistics.go（0 → 3）

```
函数                   新增埋点
────────────────────────────────
EmitStats              emitStats                       统计输出
snapshotStats           snapshot                        快照采集
resetStats             reset                            重置
```

---

## 3. 统一埋点格式

### 3.1 代码模板

```go
t0 := time.Now()
// ... 操作 ...
utils.SSCLogger().Debug().
    Str("cat", "retryScheduler").         // 模块分类
    Str("func", "OnBlockCommitted").      // 外层函数
    Str("phase", "staleCleanup").         // 阶段名
    Dur("dur", time.Since(t0)).           // 耗时
    Int("ctx1", val1).                    // 上下文（可选，按阶段定义）
    Msg("[perf] retryScheduler perf")     // 搜索前缀
```

### 3.2 cat（模块分类）

| cat | 对应模块 |
|:----|:---------|
| `retryScheduler` | retry_scheduler.go |
| `patchpool` | patchpool.go |
| `verify` | verify.go |
| `simLeader` | simulator_leader.go |
| `simMember` | simulator_member.go |
| `simulator` | simulator.go |
| `stateLock` | state_lock_impl.go |
| `stateLocker` | state_locker.go |
| `tlv` | temp_lock_view.go |
| `committer` | committer.go |
| `txSubmitter` | tx_submitter.go |
| `comm` | comm.go |
| `signer` | signer.go |
| `statistics` | statistics.go |
| `impl` | impl.go |

### 3.3 phase 命名规范

- 小写 camelCase
- 字母数字（方便 grep）
- 约定：`<操作>` 或 `<阶段>`：`staleCleanup`、`tlvTryLock`、`lockCheck`

### 3.4 日志搜索前缀

所有性能埋点用 `[perf]` 前缀，grep 一次拉全：

```bash
grep "\[perf\]" harmony*.log > perf_data.txt
```

---

## 4. 分析脚本

### 4.1 脚本接口

```bash
python3 scripts/analyze_sscc_perf.py <logfile> [--cat retryScheduler] [--top 10]
```

### 4.2 输出

每个 cat 模块一张 p50/p90/p99/max 表：

```
=== retryScheduler (cat=retryScheduler) ===
total events: 12345
┌────────────────────┬──────────┬──────────┬──────────┬──────────┐
│ phase              │ p50(ms)  │ p90(ms)  │ p99(ms)  │ max(ms)  │
├────────────────────┼──────────┼──────────┼──────────┼──────────┤
│ staleCleanup       │    0.02  │    0.05  │    0.10  │    0.50  │
│ querySubscribers   │    0.01  │    0.03  │    0.08  │    0.30  │
│ sortCandidates     │    0.50  │    2.00  │    5.00  │   15.00  │
│ ...                │          │          │          │          │
└────────────────────┴──────────┴──────────┴──────────┴──────────┘

=== Top 10 最慢阶段 (p99) ===
#1  verify:execVerify             45.2ms
#2  stateLock:checkLock           12.1ms
#3  retryScheduler:sortCandidates  5.0ms
#4  ...

=== 各模块事件数 ===
retryScheduler          12345  events
verify                  9876   events
stateLock               5678   events
simLeader               3456   events
tlv                     2345   events
...
```

### 4.3 数据提取格式

日志行格式举例：
```
{"level":"debug","time":"2026-07-23T10:00:00.123","caller":"ssc/retry_scheduler.go:502","message":"[perf] retryScheduler perf","cat":"retryScheduler","func":"OnBlockCommitted","phase":"staleCleanup","dur":"1.234ms","staleN":5}
{"level":"debug","time":"2026-07-23T10:00:00.125","caller":"ssc/verify.go:454","message":"[perf] verify perf","cat":"verify","func":"verifySimulationParsed","phase":"stateDBCopy","dur":"0.045ms"}
```

脚本用 JSON 解析，`dur` 字段统一解析为 ms。

---

## 5. 实施步骤

```
Phase 1: 新增 unified wrapper（1 个 util 函数）
  写一个 perfLog() 工具函数，统一格式，减少重复代码

Phase 2: retryScheduler + patchpool（最高优先级，P0–P4 输入数据）
  新增 ~20 个埋点

Phase 3: verify.go（SSCC 核心验证路径）
  新增 ~8 个埋点

Phase 4: state_lock_impl + temp_lock_view（锁核心）
  新增 ~11 个埋点

Phase 5: simulator_leader + simulator_member（模拟执行路径）
  新增 ~12 个埋点

Phase 6: committer + tx_submitter + comm
  新增 ~10 个埋点

Phase 7: 编写 analyze_sscc_perf.py
Phase 8: 跑实验验证埋点输出
```
