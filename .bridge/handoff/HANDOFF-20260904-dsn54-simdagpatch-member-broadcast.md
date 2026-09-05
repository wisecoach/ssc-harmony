# HANDOFF-20260904-DSN54-模拟期链下DAG patch 分发（成员侧 simDAGPatches）—— 交接给新 session

> 续接目标：DSN-54（`docs/designs/active/DSN-54-sim-time-dag-patch-subgraph.md`，rev3 为最终模型）
> 评审：`docs/reviews/DSN-54-sim-time-dag-patch-subgraph-review.md`（REV-DSN-54）
> 本 session 过长，在“可编译骨架”检查点停手，剩余 7 步接线请新 session 完成。

---

## 0. 一句话交接

**问题**：rate=200（2026-09-04）DAG 调度层大量生效（PatchHit≈10671、深度≤5、同 key 同块 7-11），但 `failed to simulate transaction: state is locked by other tx on chain ≈ 21282`、链上 Verify 拒绝≈0、unfinished 居高。

**根因（多轮收敛定论）**：leader 已在 offChainDAG 里分配好链下 patch（UpstreamTxList 已知），但**执行离链模拟的成员没有去读这份链下 DAG patch** → 读被锁 key 全报 `state is locked` → SimTx 建不出来 → 到不了 Verify。

**最终方案（DSN-54 rev3，用户拍板）**：
- leader 在 RetryCommit 选好上游后，从 offChainDAG 构建该交易本轮模拟所需 patch 子图，**广播给本分片所有成员**；成员导入本地 `simDAGPatches`（per-(tx,simNum)）。
- `CXTSimulationRequest` 带 `UpstreamTxList`，成员据此从 simDAGPatches 反查对应上游节点 RWSet。
- 成员模拟读被锁 key 时，撞锁→先查 simDAGPatches（覆盖则用之，不撞锁）；未覆盖才保留为真冲突（genuinely-uncovered）。
- **明确不做**：不携带扁平 SimPatch（rev2 已撤）；不改 ForceSimulation 的锁冲突 `ret.Err`（保留，作为救援信号）。

---

## 1. 关键语义/已确认决策（勿再翻案）

| # | 决策 |
|---|---|
| D1 | simDAGPatches 来自 leader 从 offChainDAG 构建的“该交易模拟所需 patch 子图”，RetryCommit 时广播给所有成员，成员此时导入 |
| D2 | 模拟读被锁 key 时，从链下锁转向 simDAGPatches 反查取值 |
| D3 | CXTSimulationRequest 增加 UpstreamTxList（成员据此定位 simDAGPatches 节点） |
| D4 | 不引入扁平 SimPatch 字段（rev2 实现已回退干净） |
| D5 | ForceSimulation 锁冲突 `ret.Err` 保留（**不要修** simulator_member.go:1080 那处——注释与代码看似矛盾但那是设计语义） |
| D6 | 分片私有：每个分片 leader 只广播本分片子图给本分片成员，**不跨分片提供 patch** |

rev3 §8 待确认点已在会话中拍板（写进文档 rev3 §8）：
1. 广播含“被消费上游 + 传递闭包”子图；
2. 生命周期 = 按 (tx,simNum) 导入；更大 simNum 覆盖；tx close/stale 整组清（不做“每次模拟返回即清”）；
3. simDAGPatches 放 retryScheduler；
4. 广播通道 = **B：独立 gRPC RPC `StoreSimDAGPatch`**（非随请求携带）。

---

## 2. 当前代码状态（本次 session 已落地，可编译）

`go build ./ssc/... ./rpc/...` 通过。

### 已改/新增
1. **proto** `ssc/api/proto/ssc.proto`：
   - `CXTSimulationRequest` 加 `repeated TxSimKey upstream_tx_list = 10;`
   - 新增 `SimPatchNode / SimPatchSubgraph / StoreSimDAGPatchRequest`
   - `SSCShardService` 加 `rpc StoreSimDAGPatch(StoreSimDAGPatchRequest) returns (Empty);`
2. **重新生成** `ssc/api/proto/ssc.pb.go` + `ssc_grpc.pb.go`（protoc-gen-go v1.36.11 / protoc-gen-go-grpc v1.2.0 已离线装到 `/tmp/pbgen/`）。
3. **api 类型** `ssc/api/types.go`：`CXTSimulationRequest.UpstreamTxList []TxSimKey`；新增 `SimPatchNode/SimPatchSubgraph/StoreSimDAGPatchRequest`。
4. **convert** `ssc/api/proto/convert.go`：request UpstreamTxList 双向；子图/存储请求双向（文件尾新增块）。
5. **comm** `ssc/comm.go`：客户端 `callOnConn` 加 `case "storeSimDAGPatch"`。

### 已回退干净（rev2 遗留，勿再引入）
- `SimPatch` 字段/类型/convert/leader 填充/member 消费 全部移除。`grep -rn SimPatch ssc/` 应无代码残留（仅 DSN 文档 rev2 历史文字）。

### 工具备忘
- 离线 protoc 插件：`protoc`=/usr/bin/protoc；`protoc-gen-go` 与 `protoc-gen-go-grpc` 在 `/tmp/pbgen/`（从模块缓存构建）。若 /tmp 被清，重建：
  - `cd /home/wisecoach/go/pkg/mod/google.golang.org/protobuf@v1.36.11 && go build -o /tmp/pbgen/protoc-gen-go ./cmd/protoc-gen-go`
  - `cd /home/wisecoach/go/pkg/mod/google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0 && go build -o /tmp/pbgen/protoc-gen-go-grpc .`
  - 环境：`GOCACHE=/tmp/gocache GOPROXY=off GOFLAGS="-mod=mod -buildvcs=false"`
  - 生成：在 `ssc/api/proto/` 下 `protoc -I . --plugin=protoc-gen-go=/tmp/pbgen/protoc-gen-go --go_out=. --plugin=protoc-gen-go-grpc=/tmp/pbgen/protoc-gen-go-grpc --go-grpc_out=. ssc.proto`

---

## 3. 剩余工作（新 session 必做 7 步）

> 顺序建议：先 1→4（存储+接口+server），再 5→7（广播+读路径）。每步后 `go build ./ssc/... ./rpc/...`。

1. **api.Service / ShardService**（`ssc/api/sscs.go`）：加 `StoreSimDAGPatch(req *StoreSimDAGPatchRequest)`。
2. **rpc/ssc_grpc.go**：给 `sscGrpcShardService` 实现 `StoreSimDAGPatch`（否则现在走 `Unimplemented`，广播会失败）。注意 `SSCShardService` 现在还要求该 method 必须可编译（已满足，因嵌 Unimplemented；但要做真实现）。
3. **sscService 实现**（`ssc/impl.go`）：`func (s *sscService) StoreSimDAGPatch(req *api.StoreSimDAGPatchRequest)` → 写入 `retryScheduler` 的 simDAGPatches。
4. **retryScheduler 存储**（`ssc/retry_scheduler.go`）：
   - `simDAGPatches sync.Map // (txHash,simulationNum) → *api.SimPatchSubgraph`（建议 key 用 `api.TxSimKey` 或复合）。
   - `StoreSimDAGPatch(subgraph)`：更大 simNum 覆盖旧轮；同一 simNum 幂等覆盖。
   - `GetSimDAGPatch(txHash, simNum) *api.SimPatchSubgraph`（读路径用）。
   - 清理：`RemoveSimDAGPatches(txHash)` 在 tx close/stale（对齐 offChainDAG.Remove / closeTransaction）时调用；`OnBlockCommitted`/GC 里可选按 (tx,simNum) 淘汰。
   - 这些方法都要 `isOn()`（DAG 关时 no-op）。
5. **RetryCommit 广播**：成功消费上游后（consumedPatches 早退 / Phase1b / Phase2b 命中处，`retry_scheduler.go`），从 offChainDAG 取“被消费上游+传递闭包”构建 `api.SimPatchSubgraph`，向**本分片 committee 成员** `comm.Call(..., api.Method_StoreSimDAGPatch, req)`（先查 `Method_StoreSimDAGPatch` 是否已定义——本次只加了 proto/comm case，**常量还没加**，需在 `ssc/api/sscs.go` 常量表加 `Method_StoreSimDAGPatch = "ssc_storeSimDAGPatch"`）。
   - 参考现有 leader→成员广播写法（HandleSimulateRequest 到 committee、AddRetryTx 到成员等）。
   - 子图构建辅助可放 `patchpool.go`（把 offChainDAG 节点 + 上游序列化成 `api.SimPatchSubgraph`，复用 `readPatchChainVisited` 的反查思路）。
6. **StartReSimulation**（`simulator_leader.go`）：填 `req.UpstreamTxList`（取 `simState`/consumed 的 `GetUpstreamTxRef` 或 leader 已选上游）。
7. **成员读路径**（`simulator.go` readChainPatch / IsKeyAvailable + `simulator_member.go`）：
   - 撞锁时，用 req.UpstreamTxList 在 `simDAGPatches` 里找“写了该 key 的上游节点 RWSet”取值（含传递上游，visited 防环，复用 readPatchChainVisited 模式）。
   - 命中 → available（不撞锁）；未命中 → 保留 ErrLockConflict（genuinely-uncovered）。
   - 注意：这是门限签名一致性敏感路径——所有成员对同一 (tx,simNum) 必须反推出相同值；广播子图同一 leader 构造应保证确定性。

### 别忘
- `ssc/api/sscs.go` 常量表加 `Method_StoreSimDAGPatch`（本次漏了）。
- grep `SimPatch` 确认无残留代码（应该只剩文档 rev2 历史文字，可顺手清掉 rev3 里不再用的旧表述）。

---

## 4. 验证方式（新 session）

- **编译**：`GOCACHE=/tmp/gocache GOFLAGS="-mod=mod -buildvcs=false" go build ./ssc/... ./rpc/...`
- **单测**（DAG，先移走历史坏测试）：`mv ssc/simulation_test.go /tmp/ ; mv ssc/simulation_params_test.go /tmp/` → `go test ./ssc/ -run 'TestOffChainDAG' -count=1 -v` → 跑完 mv 回。
- **打点建议**（验证口径 M3）：在模拟失败点记录 key 是否能在 simDAGPatches 反查到 → 分 `covered-but-invisible`（修复目标，应消失）与 `genuinely-uncovered`（预期残留）。
- **端到端**（需部署 199 + 真机跑）：`state is locked ~21k` 中“可被 simDAGPatches 覆盖的”应消失；`triggerReSim→chainTxCRCommitted` 转换率（成熟 run ~16%）应上升。

---

## 5. 实验基线（rate=200，2026-09-04，供对照）

| 指标 | 值 |
|---|---|
| retryCommitPatchHit | ≈10671（PatchMiss 0） |
| 真实 DAG 深度 | ≤5 |
| dagPerBlockMaxSameKey | 7-11（236 块同 key>1） |
| failed to simulate: state is locked | ≈21282 |
| Verify 拒绝 | ≈0 |
| triggerReSim | ≈8254 |
| chainTxCRCommitted（成熟 run） | ≈1328 |

---

## 6. 相关文档
- 设计：`docs/designs/active/DSN-54-sim-time-dag-patch-subgraph.md`（rev3 最终模型）
- 评审：`docs/reviews/DSN-54-sim-time-dag-patch-subgraph-review.md`（REV-DSN-54，需按 M1-M3/R1-R4 已部分落实）
- 前置：DSN-46/49/50/51/52/53
- 根因数据/去向记账：本 session 会话记录（DAG 去向、ForceSimulation 语义讨论）

---

## 7. 留给新 session 的第一件事
1. 读 DSN-54 rev3 + 本交接；
2. 在“可编译骨架”上按 §3 顺序做 7 步；
3. 每步 build；完成后跑 DAG 单测；
4. 补 `covered-but-invisible` 打点；
5. 部署 199 跑一轮 rate=200 对照。
