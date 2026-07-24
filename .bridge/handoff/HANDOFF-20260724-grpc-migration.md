# HANDOFF-20260724-grpc-migration

> from_session: 20260724_171857_e2cc18 (hentai_coder-designer)
> from_role: Designer
> to_role: Designer (next session)

## 完成内容

| 事项 | 状态 | 位置 |
|:-----|:------|:------|
| pprof 性能数据分析 | ✅ 完成 | 见下文"瓶颈分析" |
| perf aggregator 加 Enabled 开关 | ✅ | `ssc/perf/aggregator.go` — `perf.Enabled = true` |
| auto_test.sh 移除 CPU 监控 | ✅ | `auto_test.sh` — `test_single()` 精简 |
| test/test.sh 增加 pprof 自动采样 | ✅ | 后台启动客户端 → sleep 10s → 4 节点 16 进程并发 `curl /debug/pprof/profile?seconds=60` |
| P2P 密钥 RSA 2048 → ECDSA 128 | ✅ | `internal/utils/utils.go` — `GenKeyP2P` / `GenKeyP2PRand` |
| impl.go 修复 `sync.NewRWMutex` | ✅ | `ssc/impl.go:171` → `sync.RWMutex{}` |
| tx_submitter.go 修复 `lm.Mutex` → `sync.Mutex` | ✅ | `ssc/tx_submitter.go` — 删除 `ssc/lm` 依赖 |
| commitLock 封装（GetCommitStates/GetCommitLock → InitCommitState/GetCommitState） | ✅ | `ssc/simulator.go` 接口 + `ssc/impl.go` 实现 |

## 性能瓶颈分析（pprof, 60s, 226.73s total, 376.92%）

### Flat Top（函数自身 CPU）

| 函数 | Flat | 说明 |
|:-----|:----:|:-----|
| `syscall.Syscall6` | 16.96s (7.48%) | 系统调用 I/O |
| `runtime.memmove` | 6.93s (3.06%) | 内存拷贝 |
| `runtime.memclrNoHeapPointers` | 6.73s (2.97%) | GC 内存清零 |
| `SSCVMInterpreter.Run` | 5.12s (2.26%) | VM 解释器自身 |
| `runtime.mapaccess1` | 4.06s (1.79%) | map 查找 |

### Cum Top（含子调用）

| 函数 | Cum | 说明 |
|:-----|:---:|:-----|
| `rpc handler → node → SSC` | 76.63s (33.8%) | 主 RPC 入口 |
| `SSCVM.run/Call` | 60-62s (26-27%) | VM 执行 |
| `RWMutex.RUnlock` | 35.43s (15.6%) | **锁竞争** |
| `TransitionDb → SSCVM.Call` | 34.15s (15.1%) | 验证执行 |
| `HandleSimulateRequest` | 29.78s (13.1%) | Member 模拟 |
| `txSubmitter.processQueue` | 26.79s (11.8%) | 交易提交器 |
| `Publish (pubsub)` | 22.32s (9.8%) | P2P 广播 |
| **`json.Decode/Unmarshal/Marshal`** | **~22.6s (10%)** | **RPC 序列化瓶颈** |
| `zerolog 系列` | ~40s (18%) | 日志开销 |
| `bigmod.addMulVVW1024 + montgomeryMul` | ~27s (12%) | P2P 加密（RSA → ECDSA 已改，待验证） |

### 关键结论

1. **🔥 JSON 序列化/反序列化占 ~10% CPU** — `comm.go` 使用 go-ethereum `rpc.Client` 走 JSON-RPC over HTTP
2. **🔥 日志 ~18%** — Debug 级别日志过多
3. **🔥 锁竞争 ~15.6%** — RWMutex.RUnlock 累加高
4. **✅ P2P RSA→ECDSA 已改** — 下次跑实验验证效果

## 待办：gRPC + Protobuf 迁移

**目标**：将 `ssc/comm.go` 从 JSON-RPC over HTTP 替换为 gRPC + Protobuf

### 当前通信层架构

```
ssc/comm.go (97行)
  └── eth/rpc.Client.CallContext()
        ├── HTTP POST
        └── encoding/json 序列化/反序列化
```

### 现有 RPC 方法清单（18个）

定义在 `ssc/api/sscs.go:16-34`：

1. `ssc_startSimulateCXTransaction` — leader 启动模拟
2. `ssc_handleSimulateRequest` — member 处理模拟请求
3. `ssc_requestCallCXT` — 请求跨分片调用
4. `ssc_handleCXTCall` — member 处理 CALL
5. `ssc_signSimulationCommit` — BLS 签名模拟提交
6. `ssc_signCXTSimulation` — BLS 签名 CX 模拟
7. `ssc_handleCommitVote` — 处理提交投票
8. `ssc_handleCXTSSCCall` — leader 处理 SSC 调用
9. `ssc_commitSimulation` — 提交模拟结果
10. `ssc_handleCXTCommitSSCVote` — 处理 SSC 提交投票
11. `ssc_handleCXTCommitProof` — 处理提交证明
12. `ssc_signalReSimulation` — 触发重试信号
13. `ssc_addRetryTx` — 添加重试交易
14. `ssc_retryCommit` — 重试提交
15. `ssc_retryCancel` — 取消重试
16. `ssc_handleRetrySignal` — 处理重试信号
17. `ssc_sLTest` — 状态锁测试
18. `ssc_handleNewEpoch` — 处理新区块

### 调用方分布（8个文件）

| 文件 | 调用的方法 |
|:-----|:-----------|
| `simulator_leader.go` | HandleSimulateRequest, CommitSimulation, SignSimulationCommit, SignCXTSimulation, HandleCXTCall |
| `simulator_member.go` | HandleCXTSSCCall, RequestCallCXT |
| `simulator.go` | StartSimulateCXTransaction |
| `verify.go` | HandleCommitVote |
| `impl.go` | HandleCommitVote, HandleNewEpoch, HandleCXTCommitSSCVote, SignCXTSimulation, HandleCXTCommitProof |
| `retry_scheduler.go` | AddRetryTx, SignalReSimulation, RetryCommit, RetryCancel, AddToPassivePool, HandleRetrySignal |
| `committee.go` | SLTest |

### 数据结构（ssc/api/types.go）

大量消息结构体（CXTSimulation, CXTCallSSCRequest, CXTCallSSCResult, CXTCommitVote 等），需要逐个转为 `.proto` message。

### 迁移步骤建议

1. 创建 `ssc/api/proto/` 目录
2. 定义 `.proto` 文件（按业务分组：simulator.proto, vote.proto, retry.proto）
3. `protoc` 生成 Go 代码
4. 替换 `comm.go` 为 gRPC client/server
5. 逐个替换 18 个 handler 和调用方
6. 删除旧的 JSON-RPC handler 注册代码

## 当前环境

| 资源 | 详情 |
|:-----|:------|
| 工作目录 | `/mnt/E/gowork/src/github.com/wisecoach/harmony-sscc` |
| 远程编译 | `zjnu@10.7.95.199 -p 10022` → `~/go/src/github.com/harmony-one/harmony-sscc/` |
| 实验节点 | 10.7.95.200-203（4 节点，每节点 4 validator） |
| pprof 端口 | HTTP Port - 2500（200:6500-6506, 201:6540-6546, 202:6580-6586, 203:6620-6626） |
| P2P key | 已改 ECDSA 128（下次实验生效） |
| test shell | `source auto_test.sh && test_single` |
| 代码同步 | `source sync_code.sh && uploadCode zjnu@10.7.95.199` |
