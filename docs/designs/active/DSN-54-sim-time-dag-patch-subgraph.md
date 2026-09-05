---
id: DSN-54
title: 模拟期链下 DAG patch 子图：RetryCommit 广播 + 成员侧按 UpstreamTxList 反查
type: DSN
status: draft
priority: P0
author: Designer
created: 2026-09-04
updated: 2026-09-05
scope: [ssc/api, ssc/api/proto, ssc/retry_scheduler.go, ssc/impl.go, ssc/simulator.go, ssc/simulator_member.go, ssc/simulator_leader.go, ssc/comm.go]
refs: [DSN-46, DSN-49, DSN-50, DSN-51, DSN-52, DSN-53, REV-DSN-54]
---

> **状态**：draft（rev3，最终模型，已撤 rev2 的 SimPatch）
>
> **背景**：rate=200 实验（2026-09-04）显示 DAG 在调度层大量生效（`retryCommitPatchHit≈10671`、`PatchMiss=0`、真实深度≤5、`dagPerBlockMaxSameKey≈7-11`），但 `failed to simulate transaction: state is locked by other tx on chain ≈ 21282`、链上 Verify 拒绝≈0、unfinished 居高。
>
> **结论（经多轮收敛）**：这批 `state is locked` 的根因是——**leader 已经在 offChainDAG 里把链下 patch 分配好了（`UpstreamTxList` 已知），但执行链下模拟的成员没有去读这份链下 DAG patch**，于是读被锁 key 时全部报错。ForceSimulation 的锁冲突报错是**设计内、需保留**（它是“本轮未干净通行、需救援成 chain SimTx”的信号）；真正缺的是“成员能按 `UpstreamTxList` 从本地的模拟 patch 池里反查到上游 `RWSet` 来取值”。

---

## 1. 概述

给成员侧补一份“模拟期链下 DAG patch 池”（`simDAGPatches`），让链下模拟在撞锁时能读到 leader 已分配的上游值，而不是报 `state is locked`。

**三个动作**：
1. **RetryCommit 广播子图**：leader 在 RetryCommit 选好上游（消费 patch）后，从 offChainDAG 构建“该交易本轮模拟所需的 patch 子图”，广播给本分片所有成员；成员收到后导入本地 `simDAGPatches`。
2. **请求带 UpstreamTxList**：`CXTSimulationRequest` 增加 `UpstreamTxList`，成员据此定位 `simDAGPatches` 里对应的上游节点。
3. **读路径转向**：成员模拟读被锁 key 时，先按 `UpstreamTxList` 在 `simDAGPatches` 反查上游 `RWSet`，命中则用其值（不撞锁）；未命中才保留为真冲突（genuinely-uncovered）。

> 明确**不做**：不携带扁平 `SimPatch`（rev2 已撤）；不在成员侧持久化长期 patch；不改 ForceSimulation 的锁冲突报错（保留）。

---

## 2. 根因（最终口径）

| 层 | 状态 |
|---|---|
| leader offChainDAG | ✅ 已分配上游、`UpstreamTxList` 已知 |
| 成员离链模拟 | ❌ 没去读这份链下 patch → 读被锁 key 报 `state is locked` |
| 链上 Verify | 几乎不触发（因为 SimTx 在离链模拟这关就没建出来） |

---

## 3. 已确认决策（用户拍板）

| # | 决策 |
|---|---|
| D1 | `simDAGPatches` 来自 leader 从 offChainDAG 构建的“该交易模拟所需 patch 子图”，在 **RetryCommit 时广播给所有成员**，成员此时导入 |
| D2 | 模拟读被锁 key 时，**从链下锁转向 simDAGPatches** 反查取值 |
| D3 | `CXTSimulationRequest` 增加 `UpstreamTxList`，成员据此从 simDAGPatches 取对应 patch.RWSet |
| D4 | **不引入 SimPatch 扁平字段**（rev2 已撤） |
| D5 | ForceSimulation 的锁冲突 `ret.Err` **保留**（按设计） |
| D6 | 分片私有：每个分片 leader 只广播本分片子图给本分片成员，不跨分片提供 patch |

---

## 4. 设计方案

### 4.1 成员侧存储：`simDAGPatches`

成员 `retryScheduler`/`Simulator` 增加 per-round 只读 patch 池：

```go
// simDAGPatches — (txHash, simulationNum) → 该交易本轮模拟所需的上游 patch 子图
simDAGPatches sync.Map // key: api.TxSimKey → *SimPatchSubgraph
```

`SimPatchSubgraph`（每节点自带写集 + 上游边，供反查与传递依赖）：
```go
type SimPatchNode struct {
    TxSim   api.TxSimKey
    Writes  map[api.LockKey]common.Hash
    Upstream []api.TxSimKey   // 上游边
}
type SimPatchSubgraph struct {
    nodes map[api.TxSimKey]*SimPatchNode
}
```

### 4.2 RetryCommit 广播子图（D1）

在 RetryCommit 成功消费上游、准备返回 `Locked:true` 前：
1. 从 offChainDAG 取“本交易被消费的上游及其传递上游”，构建 `SimPatchSubgraph`；
2. 通过 RPC 广播给**本分片 committee 的其它成员**（新增 `Method_StoreSimDAGPatch` 之类）；
3. 成员 `StoreSimDAGPatch(txHash, simNum, subgraph)` → 写入 `simDAGPatches[(tx,simNum)]`。

（跨分片：各分片各自在各自 RetryCommit 里做同样动作，只发本分片子图给本分片成员——D6。）

### 4.3 请求带 UpstreamTxList（D3）

`CXTSimulationRequest` 增加 `UpstreamTxList []TxSimKey`（leader 已消费的上游引用）。成员 `HandleSimulateRequest` 用它从 `simDAGPatches[(tx,simNum)]` 定位节点。

### 4.4 读路径转向（D2）

`readChainPatch` / `IsKeyAvailable` 增加一步：
1. 读 key K 撞锁时，取本轮 `UpstreamTxList`；
2. 在 `simDAGPatches` 里，从这些上游节点出发（含其传递上游，visited 防环）找哪个 `Writes` 写了 K → 用其值；
3. 命中 → `available=true`（不撞锁）；未命中 → 保留 `ErrLockConflict`（genuinely-uncovered，继续走救援信号）。

### 4.5 生命周期

- 导入：RetryCommit 广播时（按 (tx,simNum)）；
- 用：一次模拟轮次内（`HandleSimulateRequest`）；
- 清理：本轮模拟结束 / 收到更大 simulationNum 覆盖 / 交易终结兜底清（与 offChainDAG.Remove 对齐）。为防泄漏与跨轮脏读，生命周期**严格绑定 (tx,simNum)**。

### 4.6 链上 Verify / 依赖

- B 的 `UpstreamTxList` 就是 B 的 SimTx 的 `UpstreamTxList`（现有字段），链上 Verify 据其判 `isChainTx` 并做一致性检查；
- B 自己能提供的 patch 由 B 链上真实执行生成（`ChainPatch` + `AddOnChainPatch`），不走成员分发。

---

## 5. 与现有模块关系

| 模块 | 角色 | 改动 |
|---|---|---|
| offChainDAG | leader 调度/选上游 | 读：构建子图用；不直接下放 |
| onChainPatches | Verify 一致性/长期上游 | 不动 |
| `CXTSimulationRequest` | 请求 | + `UpstreamTxList`（不加 SimPatch） |
| `simDAGPatches`（新增，成员） | 模拟期上游反查 | 新增 + 生命周期 |
| ForceSimulation `ret.Err` | 锁冲突信号 | **不动**（保留） |

---

## 6. 验证与观测

- 打点：失败点记录 key 是否在本轮 `simDAGPatches`（按 UpstreamTxList 可反查到）：
  - `covered-but-invisible`（修复目标，应消失）
  - `genuinely-uncovered`（预期残留，单独呈现）
- 端到端：`state is locked ~21k` 中“可被 simDAGPatches 反查到的”部分应消失；`triggerReSim→chainTxCRCommitted` 转换率应上升。

---

## 7. 变更清单（rev3）

1. `ssc/api` / proto：`CXTSimulationRequest` + `UpstreamTxList`（`TxSimKey` 列表）；新增 `Method_StoreSimDAGPatch` 广播 RPC（含子图 proto）。
2. `ssc/retry_scheduler.go`：RetryCommit 命中后构建子图并广播；成员侧 `StoreSimDAGPatch` + `simDAGPatches` 生命周期。
3. `ssc/simulator_leader.go`：`StartReSimulation` 填 `req.UpstreamTxList`。
4. `ssc/simulator_member.go`：`HandleSimulateRequest` 用 `UpstreamTxList` 定位 simDAGPatches。
5. `ssc/simulator.go`：`readChainPatch`/`IsKeyAvailable` 增加 simDAGPatches 反查。
6. 打点：covered-but-invisible vs genuinely-uncovered。

---

## 8. 已拍板决策（2026-09-05 会话确认，取代原"待确认点"）

1. **传递上游范围**：广播**含传递闭包**的子图（“被消费上游 + 整条传递上游”），以满足深读；成员按 (tx,simNum) 反查整个子图，无需仅看直接上游。
2. **生命周期**：按 (tx,simNum) 导入（StoreSimDAGPatch 到达即导入）；更大 simNum 覆盖/新增；tx close/stale 整组清（`RemoveSimDAGPatches`），**不做**“每次模拟返回即清”。
3. **存放位置**：`simDAGPatches` 放 **retryScheduler**（与 offChainDAG/consumedPatches 同模块，leader 广播与成员导入都走同一处）。
4. **广播通道**：独立 gRPC RPC **`StoreSimDAGPatch`**（`SSCShardService`，`Method_StoreSimDAGPatch="ssc_storeSimDAGPatch"`），不随既有请求携带。
