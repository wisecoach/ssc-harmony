# Sim DAG Chaining — 实现进度表

## 总览

| 阶段 | 预计工时 | 状态 |
|:----:|:--------:|:----:|
| P0 基础结构变更 | 1h | ⏳ 进行中 |
| P0 触发点迁移 | 1h | ⏳ |
| P0 chainNextSim | 2h | ⏳ |
| P0 HandleChainSimSignal | 1h | ⏳ |
| P0 DAGOrderingFilter | 2h | ⏳ |
| P0 失败回滚 | 1h | ⏳ |
| P0 RPC 接口更新 | 0.5h | ⏳ |
| P1 清理废弃代码 | 0.5h | ⏳ |
| P2 实验验证 | 1h | ⏳ |
| **合计** | **~10h** | |

---

## 阶段分解

### Phase 1: P0 基础结构变更（~1h）

**1a. `ssc/api/types.go` — 字段重命名 + NewChainConfig**
- [ ] `CRHotWritePatch` → `ChainPatch`（CXTSimulationState + ReSimulationSignal 两处）
- [ ] 移除 `HotKeyConfig` 结构体
- [ ] 新增 `ChainConfig` 结构体
- [ ] `ShardSimulateCommitteeConfig.HotKey` → `Chain`

**1b. `ssc/state_impl.go` — GetState patch 路径更新**
- [ ] `state.CRHotWritePatch` → `state.ChainPatch`
- [ ] 日志消息更新

**1c. `ssc/state_impl.go` — RetryCancel cleanup**
- [ ] `state.CRHotWritePatch = nil` → `state.ChainPatch = nil`
- [ ] 日志消息更新

---

### Phase 2: P0 触发点迁移（~1h）

**2a. `ssc/impl.go` — 触发钩子**
- [ ] 在 `SubmitSimulationTx` 成功提交路径上加 `afterSubmitSimulationTx` 钩子
- [ ] 钩子内容：提取 WriteSet → `rs.retryScheduler.chainNextSim()`
- [ ] 仅在 leader 节点触发

**2b. 移除旧的 CR 触发链**
- [ ] 删除/注释 CR 提交处的 `chainHotKeyCR` 调用

---

### Phase 3: P0 chainNextSim（~2h）

**3a. `ssc/retry_scheduler.go` — 新方法**
- [ ] `chainNextSim(upstreamTxHash, writeSet *api.RWSet)` 主体
- [ ] `dependsOn(retryTx, patch) bool` 辅助函数
- [ ] `sendChainSignal(txHash, retryTx, patch)` 辅助函数
- [ ] RLock 下收集匹配 → 解锁 → 逐个 RPC 发送

**3b. 清理旧代码**
- [ ] 移除 `chainHotKeyCR` 方法
- [ ] 移除 `updateHotKeys()`、`isHotKey()`
- [ ] 移除 `hotKeySet`、`hotKeyMu` 字段

---

### Phase 4: P0 HandleChainSimSignal（~1h）

**4a. `ssc/retry_scheduler.go` — 新方法**
- [ ] `HandleChainSimSignal(signal)` 主体（复用现有 signal aggregation 逻辑）
- [ ] 存 `state.ChainPatch = signal.ChainPatch`
- [ ] 走 `setSignal` + `readyCnt` → `tryToReSimulation`

**4b. 清理旧代码**
- [ ] 移除 `HandleHotKeyRetrySignal`

---

### Phase 5: P0 DAGOrderingFilter（~2h）

**5a. `txSubmitter.go` / 新文件 `dag_ordering.go`**
- [ ] `DAGNode` 结构体（TxHash, DependsOn, ReadSet, WriteSet, Nonce）
- [ ] `DAGOrderingFilter` 结构体
- [ ] `assignNonce(txHash) (uint64, error)` — 拓扑序 nonce 分配
- [ ] 集成到 txSubmitter nonce 分配流程

---

### Phase 6: P0 失败回滚（~1h）

**6a. `ssc/retry_scheduler.go` — 回滚逻辑**
- [ ] `onChainFailure(failedTxHash, chainRootHash)` — 整链回滚
- [ ] `resolveChainMembers(rootHash)` — 解析链成员列表
- [ ] 回调接入：VerifySimulation 失败时触发

---

### Phase 7: P0 RPC 接口更新（~0.5h）

**7a. `ssc/api/sscs.go`**
- [ ] `Method_HandleHotKeyRetrySignal` → `Method_HandleChainSimSignal`
- [ ] `HandleHotKeyRetrySignal(signal)` → `HandleChainSimSignal(signal)`
- [ ] 注释更新

**7b. `ssc/impl.go`**
- [ ] `HandleHotKeyRetrySignal` 委托 → `HandleChainSimSignal` 委托
- [ ] 方法名更新

---

### Phase 8: P1 清理废弃代码（~0.5h）

- [ ] `ssc/statistics.go` — 确认 `HotKeyConflicts` 是否还要保留（如果其他部分引用就留着，否则清理）
- [ ] `ssc/api/types.go` — `HotKeyConfig` 引用清理
- [ ] 编译通过，零未使用变量/字段

---

### Phase 9: P2 实验验证（~1h）

- [ ] RATE=100, delay=20 跑一轮 before（当前代码）
- [ ] git stash → 应用 chain 代码 → 跑一轮 after
- [ ] 对比指标：commit rate > 65%, chain SimTx 成功率 > 80%

---

## 当前进度

```
Phase 1: [          ] 0%
Phase 2: [          ] 0%
Phase 3: [          ] 0%
Phase 4: [          ] 0%
Phase 5: [          ] 0%
Phase 6: [          ] 0%
Phase 7: [          ] 0%
Phase 8: [          ] 0%
Phase 9: [          ] 0%
```

---

## Git 操作参考

```bash
# 当前状态（已 commit）：
git log -1   # 确认最新 commit

# 开发中随时查看改动：
git diff --stat

# 需要回滚时：
git checkout -- <file>     # 单个文件
git stash                  # 临时保存改动
git reset --hard HEAD      # 全部回滚到最新 commit
```
