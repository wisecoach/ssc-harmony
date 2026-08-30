# HANDOFF — DSN-47/48 内部交易结构 + 简单内部池 + JSON→protobuf 迁移

> **日期**：2026-08-27
> **项目**：`/mnt/E/gowork/src/github.com/wisecoach/harmony-sscc`
> **远程**：`zjnu@10.7.95.199 -p 10022` → `~/go/src/github.com/harmony-one/harmony-sscc`
> **远程日志**：`~/go/src/github.com/harmony-one/logs/harmony-sscc/shard=4_validator=4_ssc=1_delay=10_rate=200_vpn=4/`
> **连接 SSH 注意**：本机需 `ssh -F /dev/null -p 10022 zjnu@10.7.95.199`（默认 ssh 会因 `/etc/ssh/ssh_config.d/20-systemd-ssh-proxy.conf` 权限报错）

---

## 一、背景与目标

内部交易（SimTx/CRTx/NewEpoch/UploadOpinions/Empty）原本被硬塞进普通 `types.Transaction`
（precompile 地址 + JSON-in-Data + 假 nonce），导致 JSON 开销、单账户 nonce 串行、语义不清。

已落地两条设计，把内部交易**从普通交易中彻底独立**：

| 设计 | 内容 | 状态 |
|:-----|:-----|:----:|
| **DSN-47**（结构） | `SSCInternalTx`（RLP 外壳 + 不透明 payload）、区块承载（BodyV2 二维 `[][]*SSCInternalTx`，第一维下标=`InternalTxType`）、`SSCVM.ProcessInternal` 原生入口、worker/processor 出块与验证链路 | ✅ 已实现 |
| **DSN-48**（简单内部池） | `SSCInternalPool`（存储/提取/提交后清理）+ `tx_submitter` 改走内部池 + 广播（参考普通交易广播）+ **无条件走内部池（不设开关）** | ✅ 已实现 |

> 设计文档：`docs/designs/active/DSN-47-internal-tx-structure.md`、`docs/designs/active/DSN-48-simple-ssc-internal-pool.md`

---

## 二、已完成并确认（保留不动）

### 2.1 DSN-47 结构层
- `core/types/ssc_internal_tx.go`（新）：`SSCInternalTx{Type, Shard, Payload}`、`InternalTxType`（CRTx=0→SimTx=1→NewEpoch/UploadOpinions/Empty=2/3/4，**iota 即执行顺序**）、`Hash()`（内容派生）、`SSCTransactions`（二维 DerivableBase，并入 TxHash）
- `core/types/bodyv2.go`：`bodyFieldsV2.SSCTransactions [][]*SSCInternalTx` + getter/setter + RLP
- `core/types/block.go`：`Block.sscTransactions`、`extblockV2.SSC`、`NewBlock(...)` 接收 `sscTxns`、`Body()`/`WithBody(...)`、BodyV0/V1 stub
- `core/vm/sscvm.go`：`ProcessInternal(tx, stateDB, header)` 按类型分派到既有 Service 方法
- `core/state_processor.go`：`ApplySSCInternalTransaction`（执行入口=ProcessInternal，复用 receipt/gas/root）；`Process` 顺序 **SSC→普通→staking**
- `internal/chain/engine.go` + `consensus/engine/consensus_engine.go`：`Finalize` 增加 `sscTxns` 参数
- `node/worker/worker.go`：`environment.sscTxns`；`FinalizeNewBlock` 传 sscTxns；`CommitTransactions` 开头提取并执行内部交易

### 2.2 DSN-48 内部池 + 提交 + 广播
- `ssc/internal_pool.go`（新）：`SSCInternalPool`（Add 幂等 / Extract 按类型桶 / OnBlockCommitted 清理 / Len）
- `ssc/tx_submitter.go`（重写为纯内部池提交器）：删除旧 nonce/签名/普通交易池机制；`Submit*` 一律 `submitInternal`（入池 + 广播）
- `api/proto/node/node.go`：`SSCInternalSend` 子类型 + `ConstructSSCInternalTransactionListMessage`
- `node/node.go`：`BroadcastSSCInternalTx`（参考 `tryBroadcast`，本分片 group）+ `SetSSCInternalTxSink`
- `node/node_handler.go`：`transactionMessageHandler` 加 `SSCInternalSend` 分支 → 反序列化 → 入内部池
- `ssc/impl.go`：`internalPool` 字段 + `BlockCommitted` 清理；`ExtractSSCTransactions`
- `cmd/harmony/main.go`：创建共享 `internalPool`，注入 txSubmitter 与 sscService，设 sink

### 2.3 本次测试发现并已修复的 bug（重要）
**现象**：全部失败（TPS 0），节点 panic。
**根因**：`tx_submitter` 用了 `simulation.Bytes()` 作 payload，而 `Bytes()` 的 `WithoutSignature` 结构**不含 `Epochs`** →
`VerifySimulation` 里 `simulation.Epochs[SelfShard]` 越界（`index out of range [0] with length 0`）→ panic → 节点崩 → RPC 拒连。
**修复**：`ssc/tx_submitter.go` 的 SimTx/CRTx payload 改用**完整 `json.Marshal(...)`**（保留 Epochs/签名）。
> ⚠️ 这个 bug 正是「JSON 载荷字段不全」的典型——迁移到 protobuf 时务必确保字段完整。

---

## 三、待办 1（重点）：把 JSON 替换为 protobuf

### 3.1 现状
`SSCInternalTx.Payload` 目前是 **JSON**（`json.Marshal` 写入，`json.Unmarshal` 读出）。
虽然 DSN-47 R9 说「protobuf 只用于线上/区块载荷，签名仍用独立规范字节」，但**尚未实施**。

### 3.2 好消息：protobuf 基础已存在，直接复用
- `ssc/api/proto/ssc.proto` 已定义 `CXTSimulation`(L168)、`CXTCommitProof`(L299)、`NewEpoch`(L374)、`RWSet`(L107)、`CXTCallState`(L182)
- `ssc/api/proto/convert.go` 已有双向转换：
  - `CXTSimulationFromProto` / `CXTSimulationToProto`
  - `CXTCommitProofFromProto` / `CXTCommitProofToProto`
- 序列化用 `proto.Marshal` / `proto.Unmarshal`（`google.golang.org/protobuf`）

### 3.3 需要替换的 JSON 点位
**写入侧（tx_submitter → Payload）**：
| 文件:行 | 现在（JSON） | 改为 |
|---|---|---|
| `ssc/tx_submitter.go` SubmitSimulationTx / WithSigner | `json.Marshal(simulation)` | `proto.Marshal(CXTSimulationToProto(simulation))` |
| `ssc/tx_submitter.go` SubmitCommitOrRollbackTx | `json.Marshal(proof)` | `proto.Marshal(CXTCommitProofToProto(proof))` |
| `ssc/tx_submitter.go` SubmitNewEpoch | `json.Marshal(newEpoch)` | `proto.Marshal(NewEpochToProto(newEpoch))`（已有 `NewEpochToProto`，convert.go:1124） |
| `ssc/tx_submitter.go` SubmitUploadOpinions | `json.Marshal(uploadOpinions)` | `SelfOpinions` **暂无** proto/convert，需先补定义 |

**读取侧（执行/验证 → Unmarshal）**：
| 文件:行 | 现在（JSON） | 改为 |
|---|---|---|
| `ssc/verify.go:237` VerifySimulation | `json.Unmarshal(simulationBytes, simulation)` | `proto.Unmarshal` → `CXTSimulationFromProto` |
| `ssc/committer.go:51` CommitOrRollbackWithProof | `json.Unmarshal(commitProofBytes, commitProof)` | `proto.Unmarshal` → `CXTCommitProofFromProto` |
| `ssc/impl.go:586` NewEpoch | `json.Unmarshal(newEpochBytes, newEpoch)` | proto 版本 |
| `ssc/committee.go:912,944` | `json.Unmarshal(tx.Data(), ...)` / opinions | 旧普通交易路径；若普通路径废弃可不动，但内部池路径必须切 |
| `ssc/mischief/proxy.go:101` | `json.Unmarshal(simulationBytes, ...)` | 与 VerifySimulation 一致 |

> 说明：`ssc/verify.go:243/499/536/663`、`ssc/impl.go:503` 的 `json.Marshal(payload)` 是**投票/错误 payload**（`CXTInvalidSimulationPayload` 等），与内部交易载荷无关，**不要动**。

### 3.4 关键约束（R9）
- **签名仍用独立规范字节**：`CXTSimulation.Bytes()` / `CXTCommitProof.Bytes()` 继续用于签名/哈希，**不与 protobuf 编码耦合**。迁移只改「区块/网络载荷」编码，不改签名语义。
- `SSCInternalTx.Hash()` 基于 `Type+Shard+Payload` 的 RLP，payload 编码变化会改变 hash → **属 R8 区块格式变更**，需全节点统一升级。
- 迁移后建议在 payload 里带一个版本/类型标记，或直接依赖 `InternalTxType` 决定用哪个 proto 结构反序列化（更清晰）。

### 3.5 验证
- 单测/临时程序：`CXTSimulation` round-trip（proto.Marshal → Unmarshal → FromProto）字段完整，尤其 **Epochs / BaseBLSSignedMessage**。
- 重跑 rate=200 实验：不再 panic，SimTx 正常提交/验证/commit。

---

## 四、待办 2：37 秒 SimTx 延迟（本次测试观察到，未定位）

- 日志显示 worker 从 block1 正常出块、普通交易正常提交（1408 条），但**跨分片模拟（HandleSimulateRequest）直到 14:52:37 才出现**，随后 SimTx 一次性批量提交（86 个，无 CRTx）。
- 可能原因待查：跨分片交易是否在 37s 前未触发模拟？是否与某个 shard 提前 panic/崩溃有关？还是 rate=200 下跨分片协调本身延迟？
- **建议**：修复 protobuf + 本次 panic 后，先重跑看延迟是否消失；若仍在，单独排查跨分片模拟启动时机（`HandleSimulateRequest` / `StartSimulateCXTransaction` 的触发链路）。

---

## 五、待办 3：后续增强（DSN-46 等，本轮未做）

| 项 | 说明 |
|:---|:-----|
| 广播跨分片 | 当前 `BroadcastSSCInternalTx` 只发本分片 group；SimTx 需广播 related shards |
| 进池仲裁分组 / DAG | DSN-46：`Extract` 前做冲突分组 + ChainPatch/DAG 就绪 |
| worker 出块语义 | `ProcessInternal` 对 SimTx 目前走 `VerifySimulation`（验证投票逻辑），leader 出块时调用是否合适需评审（是否要拆「执行」与「验证」入口） |
| 区块格式升级 | `SSCTransactions` 并入 TxHash，R8 影响面（block hash / P2P / DB / RPC）需全节点协调 |

---

## 六、新 session 启动指引

```bash
# 本地构建验证（AGENTS 规则：不做 go test）
cd /mnt/E/gowork/src/github.com/wisecoach/harmony-sscc
export GOCACHE=/tmp/gocache && unset GOMODCACHE
go build ./internal/... ./consensus/... ./core/... ./node/... ./ssc/... ./cmd/... ./api/... ./test/...

# 远程查看日志（关键点）
ssh -F /dev/null -p 10022 zjnu@10.7.95.199 \
  'cd ~/go/src/github.com/harmony-one/logs/harmony-sscc/shard=4_validator=4_ssc=1_delay=10_rate=200_vpn=4 && \
   ls | grep ssc-validator | xargs cat | grep -a "SSCInternalPool\|TxSubmitter\|SSCInternalTx\|panic"'
```

**优先顺序建议**：
1. 完成 **JSON→protobuf**（第三节）——这是明确方向，基础已具备。
2. 重跑实验验证 protobuf + panic 修复。
3. 排查 37 秒 SimTx 延迟。
4. 再做 DSN-46 分组/DAG 与跨分片广播。

---

## 七、风险与注意
- **不要 git checkout 回退**：DSN-47/48 的结构与链路改动是当前工作基线。
- 迁移 protobuf 是**区块格式变更**，务必全节点同版本，否则分叉。
- `json.Marshal(payload)`（投票/错误载荷）与内部交易载荷区分开，别误改。
- 连接远程 SSH 必须 `-F /dev/null`。
