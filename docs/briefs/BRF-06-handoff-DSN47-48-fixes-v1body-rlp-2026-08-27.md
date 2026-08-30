# HANDOFF — DSN-47/48 执行流适配修复 + JSON→protobuf 迁移 + v1 区块体 SSC 丢失 BUG

> **日期**：2026-08-27
> **项目**：`/mnt/E/gowork/src/github.com/wisecoach/harmony-sscc`
> **远程**：`zjnu@10.7.95.199 -p 10022` → `~/go/src/github.com/harmony-one/harmony-sscc`
> **远程日志**：`~/go/src/github.com/harmony-one/logs/harmony-sscc/shard=4_validator=4_ssc=1_delay=10_rate=200_vpn=4/`
> **连接 SSH 注意**：需 `ssh -F /dev/null -p 10022 zjnu@10.7.95.199`（默认 ssh 会因 `/etc/ssh/ssh_config.d/20-systemd-ssh-proxy.conf` 权限报错）
> **基于**：`docs/briefs/BRF-05-handoff-DSN47-48-json2protobuf-2026-08-27.md`

---

## 一、本 session 做了什么（一句话）

在 DSN-47/48（内部交易结构 + 简单内部池）基础上，完成：
**执行流适配性修复（Prepare 对齐 + 失败清理 + 清理双轨）、JSON→protobuf 迁移、以及定位并修复了导致 BAD BLOCK 的「v1 区块体 RLP 丢失 SSCTransactions」严重 bug。**

---

## 二、执行流梳理结论（关键认知）

> 先澄清：**DSN-47/48 只是换了交易的数据结构（普通 `types.Transaction` → `SSCInternalTx`），执行逻辑（`VerifySimulation`/`CommitOrRollbackWithProof`/`NewEpoch`/`UploadSLOpinion`）保持原样**。
> 因此「Leader 出块时走 `VerifySimulation`」是**原有语义**，不是回归，无需改（BRF-05 待办3 里那条“出块语义需评审”属于设计遗留，非 bug）。

### 内部交易全链路
```
产生/提交 (ssc/tx_submitter.go)  Submit* → submitInternal(): internalPool.Add + BroadcastSSCInternalTx
网络接收 (node/node_handler.go)   SSCInternalSend → rlp decode → sink → internalPool.Add
Leader 出块 (consensus_block_proposing.go + worker.CommitTransactions)
    ① 提取并执行内部交易 (ExtractSSCTransactions → ApplySSCInternalTransaction → ProcessInternal)
    ② 普通/跨分片交易 (CommitSortedTransactions)
FinalizeNewBlock → Engine().Finalize(..., sscTxns) → types.NewBlock(...sscTxns) → Body 2D SSC → 并入 TxHash
Validator 复算 (core/state_processor.go Process)   SSC → 普通 → staking → incoming → Finalize
提交后清理 (ssc/impl.go BlockCommitted)             internalPool.OnBlockCommitted
```

---

## 三、本次修复清单

### ③ 失败内部交易不清理 → 每块无限重试（已修）
- `ssc/internal_pool.go`：新增 `SSCInternalPool.Remove(tx)`（按 hash 幂等删除 + Warn 日志）
- `ssc/api/sscs.go`：`api.Service` 新增 `DiscardSSCInternalTx(tx)`（`MischiefProxy` 内嵌接口自动满足）
- `ssc/impl.go`：`sscService.DiscardSSCInternalTx` 委托 `internalPool.Remove`
- `node/worker/worker.go`：内部交易执行失败时调用 `DiscardSSCInternalTx` 并从池中移除（日志由 skipping 改 discarding）

### ② worker 与 Process 的 `statedb.Prepare` 上下文对齐（已修）
- `node/worker/worker.go`：
  - 内部交易循环执行前补 `w.current.state.Prepare(sscTx.Hash(), common.Hash{}, len(receipts))`
  - 普通交易两处 Prepare 的 index 由 `len(w.current.txs)` 改为 `len(w.current.receipts)`，与 validator 的 `sscTotal+i` 语义一致

### ④ 清理遗留普通交易 SSC 双轨路径（已修）
- `consensus/consensus_block_proposing.go`：删除 `sscAddrSet`、`pendingCRTxs`/`pendingSSCTxs` 分类；普通交易一律进 `pendingPlainTxs`；`CommitTransactions` 改 3 参；移除 `bytes`/`vm` import
- `node/worker/worker.go`：`CommitTransactions` 签名去掉 `pendingSSCTxs`/`pendingCRTxs`；删除两个遗留分支；删除死代码 `CommitSSCTransactions`
- `consensus/consensus.go`：删除 `GetOnChainSSCAddrs` 类型/字段/默认值
- `cmd/harmony/main.go`：删除 `GetOnChainSSCAddrs` 赋值
- 调用点同步：`test/chain/main.go`、`node/worker/worker_test.go`、`node/node_handler_test.go`、`node/node_newblock_test.go`

### ⑥ JSON→protobuf 迁移（已修）
- **proto**：`ssc/api/proto/ssc.proto` 新增 `SLOpinion`、`SelfOpinions`（用本机 `protoc v3.21.12` + 模块缓存构建的 `protoc-gen-go v1.36.11` 重新生成 `ssc.pb.go`，diff 仅新增两消息）
- **convert**（`ssc/api/proto/convert.go`）：
  - 新增 `SLOpinionFromProto/ToProto`、`SelfOpinionsFromProto/ToProto`
  - **修正 `NewEpochFromProto/ToProto`**：原来 `Committee` 直接置 nil（丢数据）；现在真正 JSON 序列化/反序列化进 proto 的 `bytes committee`。顺带修好 gRPC `HandleNewEpoch`（`rpc/ssc_grpc.go`、`ssc/comm.go`）committee 一直为空的问题
- **写入侧**（`ssc/tx_submitter.go`）：全部 `json.Marshal` → `proto.Marshal(ToProto(...))`（SimTx/CRTx/NewEpoch/UploadOpinions）
- **读取侧**：`ssc/verify.go` VerifySimulation、`ssc/committer.go` CommitOrRollbackWithProof、`ssc/impl.go` NewEpoch、`ssc/mischief/proxy.go` VerifySimulation → `proto.Unmarshal` + `FromProto`
- **验证**（临时程序已删）：SimTx 的 `Epochs`/`BaseBLSSignedMessage`、`SelfOpinions`、`NewEpoch` committee 均 round-trip 完整（原 panic 根因 Epochs 越界在 proto 下安全）

---

## 四、严重 BUG：BAD BLOCK — v1 区块体 RLP 丢失 SSCTransactions（已修，重点）

### 现象
```
Number: 10 ... Hash: 0xfa4c...
Error: transaction root hash mismatch: have 56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421, want 1c1e...
```
- `have` = **EmptyRootHash**（空 trie 根），`want` = 非空
- 即：proposer 在内存里把 SSC 内部交易算进 TxHash，但块体经 RLP 传输后 SSC 被丢，validator 重算得空根 → mismatch
- 同根因还出现在：shard 3 / block 7（`consensus_v2.go:640 [TryCatchup]`）——说明**节点跑的是旧二进制，修复未部署**

### 根因
`core/types/block.go` 的 `EncodeRLP/DecodeRLP` 只有 `v3.Header` 走 `extblockV2`（带 SSC）；`v1/v2` 走 `extblockV1`、`v0` 走 `extblock`，**都不带 SSC**。而本链配置 `Staking: 2, CrossLink: 2`，epoch 1（block 10）走 `HasCrossTxFields` → **v1 header → BodyV1/extblockV1**，恰好 DSN-47 只给 v3/BodyV2 加了 SSC，v1/v0 留了 stub → 序列化丢字段。

### 修复
1. `core/types/bodyv1.go`：`bodyFieldsV1` 增加 `SSCTransactions [][]*SSCInternalTx`；实现真实的 `SSCTransactions()`/`SSCTransactionAt()`/`SetSSCTransactions()`（与 BodyV2 同归一化）
2. `core/types/block.go`：`extblockV1` 增加 `SSC [][]*SSCInternalTx`；`EncodeRLP` 的 `v1/v2` 分支带上 `b.sscTransactions`；`DecodeRLP` 的 `extblockV1` 分支读回 `eb.SSC`

### 验证（临时程序已删）
v1 header + 1 条 SimTx + 0 普通交易（复现场景）做 RLP 往返：
```
original txHash: 0x6162c6b8...
full-block RLP: ssc rows=5, simtx=1, txHash match=true   ← 修复前 rows=0, mismatch
body RLP:       ssc rows=5, simtx=1
```
全量 `go build ./...` 通过（exit=0）。

### 追加修复（新 session 发现：RLP 修复后 BAD BLOCK 仍存在）
现象（block 9 / shard 3，`consensus_v2.go commitBlock` → `InsertChain`）：
```
transaction root hash mismatch: have 56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421 (EmptyRootHash), want 2fb1336f...
```
- 上一轮只修了**序列化**（body/extblockV1 携带 SSC），但 validator 端**重算 TxHash 的校验点没跟上**：
  `core/block_validator.go` 的 `ValidateBody` 仍只 `DeriveSha(block.Transactions(), block.StakingTransactions())`，
  **漏了 `block.SSCTransactions()`** → 对「仅含 SSC、0 普通/staking」的块算出 EmptyRootHash，而 header.TxHash（proposer 用 `NewBlock` 计算）含 SSC → 恒 mismatch。
- 修复：`ValidateBody` 的 `DeriveSha` 增加 `types.SSCTransactions(block.SSCTransactions())`（`Block.SSCTransactions()` 返回匿名 `[][]*SSCInternalTx`，需转成命名类型 `SSCTransactions` 才实现 `DerivableBase`），与 `NewBlock` 的三元组完全对齐。
- 全量 `go build ./...` 通过（exit=0）。
- 部署注意不变：仍需全节点同版本、同时升级，并从干净链重起。

### ⚠️ 部署必读
- **所有节点必须同版本、同时升级**——这是区块格式变更（v1 body / extblockV1 新增字段），新旧不能混跑
- **很可能需要重置测试链**：修复生效前已有坏块/分叉（如 block 7、10），建议清掉从干净状态用新二进制重新起链，再跑 `rate=200` 实验

---

## 五、序列化路径核对（确认无其他丢失点）

共识/存储所有块序列化都走 `types.Block` RLP / `Body`：
- Leader 提案 `consensus/leader.go:24`、Validator `validator.go:98/113`、ViewChange `view_change_*.go`、Catchup `consensus_v2.go:503` → `rlp.Encode/DecodeBytes(block)`
- 落库 `core/rawdb/accessors_chain.go` → `block.Body()`（BodyV1）
全部收敛到 `Block.EncodeRLP/DecodeRLP` 与 `BodyV1`——即本次修复点。

---

## 六、保留未动 / 已知项
- **`ssc/committee.go:912/944`**（`StateDataParser.ParseBlock/ParseOpinions`）：旧普通交易路径解析**历史区块**数据的，不影响内部池路径，保留（如需兼容老链数据可后续清）
- **`BroadcastSSCInternalTx`** 只发本分片 group —— **用户明确不需要跨分片广播**，保持现状（BRF-05 待办3 划掉）
- **投票/错误载荷**（`CXTInvalidSimulationPayload` 等）仍是 JSON，未动（BRF-05 明确不要动）
- 新 session 暂未做的：DSN-46（内部池冲突分组/DAG 就绪）、worker 出块语义拆「执行/验证」入口（设计遗留）

---

## 七、新 session 启动指引
```bash
# 本地构建验证（AGENTS 规则：不做 go test）
cd /mnt/E/gowork/src/github.com/wisecoach/harmony-sscc
export GOCACHE=/tmp/gocache && unset GOMODCACHE
go build ./...

# 远程查看日志（关键点）
ssh -F /dev/null -p 10022 zjnu@10.7.95.199 \
  'cd ~/go/src/github.com/harmony-one/logs/harmony-sscc/shard=4_validator=4_ssc=1_delay=10_rate=200_vpn=4 && \
   ls | grep ssc-validator | xargs cat | grep -a "SSCInternalPool\|TxSubmitter\|SSCInternalTx\|BAD BLOCK\|transaction root hash\|panic"'
```

**优先顺序建议**：
1. **部署本 session 修复**（重编 + 全节点统一升级 + 重置链）——这是继续实验的前提
2. 重跑 `rate=200` 实验：确认不再 panic、不再 BAD BLOCK、SimTx 正常提交/验证/commit
3. 若仍存在 37s SimTx 延迟，再单独排查跨分片模拟启动时机（`HandleSimulateRequest` / `StartSimulateCXTransaction`）

---

## 八、风险与注意
- **不要 git checkout 回退**：DSN-47/48 结构链路是当前工作基线
- v1 body 新增字段是**区块格式变更**，务必全节点同版本，否则分叉
- `json.Marshal(payload)`（投票/错误载荷）与内部交易载荷区分开，别误改
