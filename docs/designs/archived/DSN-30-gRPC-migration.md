# DSN-30 — JSON-RPC → gRPC + Protobuf 迁移

| 字段 | 值 |
|:-----|:----|
| Author | Designer |
| Date | 2026-07-24 |
| Status | Draft |
| Supersedes | — |

## 1. 动机

pprof 数据显示 **JSON 序列化/反序列化占 ~10% CPU**（`json.Decode/Unmarshal/Marshal` ~22.6s in 226s total）。

当前 `ssc/comm.go` 使用 go-ethereum `rpc.Client` 走 JSON-RPC over HTTP。每个 RPC 调用：
1. 客户端 json.Marshal 参数
2. HTTP POST 发送
3. 服务端 json.Unmarshal 请求 → 处理 → json.Marshal 响应
4. HTTP 响应返回
5. 客户端 json.Unmarshal 响应

迁移到 gRPC + Protobuf 的好处：
- **免序列化**：Protobuf 二进制编码，CPU 开销 ~JSON 的 1/5~1/10
- **强类型**：`.proto` 定义即契约，编译时类型检查
- **流式传输**：少数路径（如阈值签名聚合）可受益于 streaming
- **零新增依赖**：`google.golang.org/grpc` 和 `google.golang.org/protobuf` 已在 go.mod

## 2. 现状

### 通信架构

```
ssc/comm.go (97行)
  └── eth/rpc.Client.CallContext()
        ├── HTTP POST → endpoint
        └── encoding/json 序列化/反序列化
        └── 连接池: http.Transport{MaxIdleConnsPerHost:32, MaxIdleConns:128}
```

### RPC 方法清单（18 个）

按通信模式分为三组：

**Group A — Request/Response（一对一，需返回值）**
| 方法 | 参数 | 返回值 | 调用方 |
|:-----|:-----|:-------|:-------|
| `HandleSimulateRequest` | `CXTSimulationRequest` | `CXTSimulationResult` | simulator_leader |
| `RequestCallCXT` | `CXTCallRequest` | `CXTCallSSCResult` | simulator_member |
| `HandleCXTCall` | `CXTCallSSCRequest` | `CXTCallResult` | simulator_leader |
| `HandleCXTSSCCall` | `CXTCallSSCRequest` | `CXTCallSSCResult` | simulator_member |
| `SignSimulationCommit` | `SimulationCommit` | `[]byte` | simulator_leader |
| `SignCXTSimulation` | `CXTSimulation` | `[]byte` | impl.go |
| `StartSimulateCXTransaction` | `CXTSimulationRequest` | `CXTSimulationSSCResult` | simulator.go |
| `RetryCommit` | `common.Hash` | `RetryCommitResp` | retry_scheduler |
| `SLTest` | `SLTestRequest` | `SLTestResult` | committee |
| `AddRetryTx` | — | void | — |
| `RetryCancel` | — | void | — |
| `AddToPassivePool` | — | void | — |

**Group B — Fire-and-forget（无返回值）**
| 方法 | 参数 | 调用方 |
|:-----|:-----|:-------|
| `CommitSimulation` | `SimulationCommit` | simulator_leader |
| `HandleCommitVote` | `CXTCommitVote` | impl.go, verify.go |
| `HandleCXTCommitSSCVote` | `CXTCommitSSCVote` | impl.go |
| `HandleCXTCommitProof` | `CXTCommitProof` | impl.go |
| `HandleNewEpoch` | `NewEpoch, uint64` | impl.go |
| `HandleRetrySignal` | `RetrySignal` | retry_scheduler |

**Group C — Fire-and-forget with retry semantics**
| 方法 | 参数 | 调用方 |
|:-----|:-----|:-------|
| `SignalReSimulation` | `RetrySignals` | retry_scheduler |

### 消息结构体（当前）

~21 个主要消息类型定义在 `ssc/api/types.go`，全部手动实现 `Bytes() []byte` 做 json.Marshal。

关键类型及体积特征：
- `CXTSimulationRequest` — 含 `*types.Transaction`（~200B–2KB）
- `CXTSimulation` — 含 `[]*CXTCallState` 数组 + `*RWSet`（ChainPatch）
- `CXTCallRequest` — 含 `*big.Int`×2（GasPrice, Value）
- `CXTCommitProof` — 含 `[]*CXTCommitSSCVote` 数组
- `RWSet` — 含 `StateSet` map（L2 map: `map[common.Address]map[common.Hash]common.Hash`）
- `types.Receipt` — 外部模块，>100字段

### 服务端 handler（当前）

`rpc/ssc.go` — 两个 stub 类实现 go-ethereum rpc 接口：
- `PublicSSCShardService` — `ShardService` 方法
- `PublicSSCCrossService` — `CrossService` 方法

通过 `rpc2.API{Namespace, Version, Service, Public: true}` 注册到 go-ethereum JSON-RPC server。

## 3. 设计

### 原则

1. **渐进替换，每一步可编译**：先定义 proto + 生成代码，新代码与旧代码共存，逐个方法替换
2. **不修改业务逻辑**：gRPC handler 仅做参数解析 → 调用现有 service 接口
3. **保持 comm.go 接口兼容**：`Comm.Call()` 签名不变，内部切换 gRPC 调用

### 3.1 Protobuf 定义

```
ssc/api/proto/
├── ssc.proto         # 所有消息类型 + rpc service 定义
└── gen/              # protoc 生成代码（go_package）
    └── ssc/
        └── ssc.pb.go
```

**消息转换策略：**
- `common.Hash` → `bytes`（固定32字节原型）
- `common.Address` → `bytes`（20字节）
- `*big.Int` → `bytes`（big-endian 编码 + string 编码可选，选 bytes 以保持精确）
- `CallIndex` ([]int) → `repeated int32`
- `Epoch` (uint64) → `uint64`
- `time.Duration` → `int64` (nanos)
- `types.Transaction` / `types.Receipt` — **保持 json 编码为 bytes**（避免反序列化外部模块的复杂结构）
- `[]byte` → `bytes`
- maps → 使用 field 数组或 `map<k,v>`

> **关键决策：** `types.Transaction` 和 `types.Receipt` 是 go-ethereum 的外部结构体，无 protobuf 定义。将其序列化为 `bytes`（使用现有的 `rlp.EncodeToBytes` 或 `json.Marshal`），在 gRPC layer 传输 bytes，服务端/客户端各自反序列化。不做 proto 版本的外部类型定义，避免与 go-ethereum 版本耦合。

### 3.2 gRPC Service 定义

按语义分 2 个 service：

```protobuf
service SSCShardService {
  // Shard-local RPC (同分片内部)
  rpc StartSimulateCXTransaction(CXTSimulationRequest) returns (CXTSimulationSSCResult);
  rpc HandleSimulateRequest(CXTSimulationRequest) returns (CXTSimulationResult);
  rpc RequestCallCXT(CXTCallRequest) returns (CXTCallSSCResult);
  rpc HandleCXTCall(CXTCallSSCRequest) returns (CXTCallResult);
  rpc SignSimulationCommit(SimulationCommit) returns (Bytes);
  rpc SignCXTSimulation(CXTSimulation) returns (Bytes);
  rpc HandleCommitVote(CXTCommitVote) returns (Empty);
  rpc SLTest(SLTestRequest) returns (SLTestResult);
  rpc AddRetryTx(RetryTx) returns (Empty);
  rpc AddToPassivePool(Hash) returns (Empty);
  rpc RetryCommit(Hash) returns (RetryCommitResp);
  rpc RetryCancel(Hash) returns (Empty);
  rpc HandleNewEpoch(HandleNewEpochRequest) returns (Empty);
}

service SSCCrossService {
  // Inter-shard RPC
  rpc HandleCXTSSCCall(CXTCallSSCRequest) returns (CXTCallSSCResult);
  rpc CommitSimulation(SimulationCommit) returns (Empty);
  rpc HandleCXTCommitSSCVote(CXTCommitSSCVote) returns (Empty);
  rpc HandleCXTCommitProof(CXTCommitProof) returns (Empty);
  rpc SignalReSimulation(RetrySignals) returns (Empty);
  rpc HandleRetrySignal(RetrySignal) returns (Empty);
}
```

> Note: 不合并为一个 service，以匹配现有 `ShardService` / `CrossService` 接口拆分。client 端通过统一 gRPC 连接调用，不影响。

### 3.4 转换层（convert.go）

**设计：** 在 `ssc/api/proto/` 下加 `convert.go`，包含 proto ↔ api 结构体双向转换函数。转换层是纯映射逻辑，零外部状态。

**为什么需要转换层：**
- 业务代码所有接口（`sscs.go`）、消息结构体（`types.go`）、service 实现完全不感知 gRPC
- proto 生成的结构体是 gRPC 层的内部细节，不污染业务层
- 迁移时可增量替换，JSON-RPC 方法与 gRPC 方法并存

**目录结构：**
```
ssc/api/proto/
├── ssc.proto           # 消息 + service 定义
├── convert.go          # proto ↔ api 类型转换
└── gen/                # protoc 生成代码
    └── ssc.pb.go
```

**转换模式示例：**

```go
// CXTSimulationRequest: proto → api
func CXTSimulationRequestFromProto(p *sscpb.CXTSimulationRequest) *api.CXTSimulationRequest {
    tx := new(types.Transaction)
    if len(p.GetTx().GetData()) > 0 {
        rlp.DecodeBytes(p.GetTx().GetData(), tx)
    }
    return &api.CXTSimulationRequest{
        BlockNum:      p.GetBlockNum(),
        Epochs:        p.GetEpochs(),
        TxHash:        bytesToHash(p.GetTxHash()),
        SimulationNum: int(p.GetSimulationNum()),
        Author:        bytesToAddrPtr(p.GetAuthor()),
        BlockHash:     bytesToHash(p.GetBlockHash()),
        Tx:            tx,
        From:          bytesToAddr(p.GetFrom()),
        GasPool:       p.GetGasPool(),
    }
}

// CXTSimulationRequest: api → proto
func CXTSimulationRequestToProto(a *api.CXTSimulationRequest) *sscpb.CXTSimulationRequest {
    var txBytes []byte
    if a.Tx != nil {
        txBytes, _ = rlp.EncodeToBytes(a.Tx)
    }
    return &sscpb.CXTSimulationRequest{
        BlockNum:      a.BlockNum,
        Epochs:        a.Epochs,
        TxHash:        hashToProto(a.TxHash),
        SimulationNum: int32(a.SimulationNum),
        Author:        addrPtrToBytes(a.Author),
        BlockHash:     hashToProto(a.BlockHash),
        Tx:            &sscpb.RLPBytes{Data: txBytes},
        From:          addrToProto(a.From),
        GasPool:       a.GasPool,
    }
}
```

**性能：** 转换层只有纯内存 field copy + 少量 RLP 编解码（仅 `types.Transaction` 和 `types.Receipt` 两个外部类型走 RLP）。预计每调用额外 ~0.5μs，远低于 JSON 序列化的 ~13μs/调用。

### 3.5 客户端替换 — comm.go 重写

**目标：** 保持 `Comm` 结构体完全对外兼容：

```go
type Comm struct {
    // 旧：map[string]*rpc.Client
    // 新：map[string]*grpc.ClientConn
    clients map[string]*grpc.ClientConn  
    // 新：每个 service 一个 client stub
    shardServiceClient  sscpb.SSCShardServiceClient
    crossServiceClient  sscpb.SSCCrossServiceClient
}
```

**接口保持不变：**
```go
func (c *Comm) Call(ctx context.Context, ret interface{}, member *api.Member, method string, args ...interface{}) error
```

内部实现根据 `method` 字符串路由到对应的 gRPC call：
- 去掉 `"ssc_"` 前缀 → 匹配 rpc method name
- switch → 调用 `shardServiceClient.Xxx()` 或 `crossServiceClient.Xxx()`
- `ret interface{}` 接收结果（现有调用方用 `new(api.CXTSimulationSSCResult)` 模式，需要匹配）

**兼容层设计：** 新旧共存的过渡期，`Comm.Call()` 先尝试 gRPC，若 `not implemented` 回退 JSON-RPC（通过可选的旧 client）。但简化方案是直接切换，因为所有 handler 在同一个节点进程中。

### 3.4 服务端替换 — rpc/ssc.go 重写

**目标：** 保留现有的 `rpc/ssc.go` handler 实现（参数校验 + 调用 service 接口），添加 gRPC 注册方式。

```go
// 新增：gRPC server 注册
func RegisterSSCGrpcServer(grpcServer *grpc.Server, internalService api.Service) {
    sscpb.RegisterSSCShardServiceServer(grpcServer, &sscGrpcShardService{...})
    sscpb.RegisterSSCCrossServiceServer(grpcServer, &sscGrpcCrossService{...})
}
```

gRPC handler 与现有 JSON-RPC handler 共享同一个 `internalService` 实例。两者可同时运行。

### 3.5 `big.Int` 序列化

`CXTCallRequest` 中有 `GasPrice *big.Int` 和 `Value *big.Int`。Protobuf 没有 `big.Int` 类型。

**方案：** 定义为 `bytes` 字段，用 `big.Int.Bytes()`（big-endian unsigned）编码。反序列化用 `new(big.Int).SetBytes(v)`。

对于 `GasPrice` 和 `Value`，这个编码是精确的。注意补零处理：`big.Int.Bytes()` 去掉高位零，`SetBytes` 恢复。这对 `Value=0` 需要兼容空 bytes（表示 0）。

### 3.6 `RWSet` 序列化

`RWSet` 包含 `*StateSet`，而 `StateSet` 是 `map[common.Address]map[common.Hash]common.Hash`。

**方案：** `StateSet` 平坦化为 `StateEntry` + `StateKVPair`：

```protobuf
message StateKVPair {
    bytes key = 1;
    bytes value = 2;
}
message StateEntry {
    bytes address = 1;
    repeated StateKVPair state = 2;  // raw bytes key
    bytes balance = 3;
}
```

`RWSet` = `ReadState + WriteState + CurrentState` 各一个 `repeated StateEntry`。

> 注：不直接用 `map<bytes, bytes>` 因为 proto3 不支持 bytes 作为 map key 类型。`repeated StateKVPair` 等价且保持 raw bytes 无 hex 额外开销。

### 3.7 `types.Transaction` / `types.Receipt` 传输

**方案：** 使用 `bytes` 字段 + RLP 编码。服务端/客户端各自用 `rlp.DecodeBytes` 恢复。

```protobuf
message CXTSimulationRequest {
    uint64 block_num = 1;
    repeated uint64 epochs = 2;  // Epoch[]
    bytes tx_hash = 3;
    int32 simulation_num = 4;
    bytes author = 5;           // *common.Address — nil 时为空 bytes
    bytes block_hash = 6;
    bytes tx_bytes = 7;         // RLP-encoded *types.Transaction
    bytes from = 8;
    uint64 gas_pool = 9;
}
```

## 4. 迁移计划

### Phase 0 — 基础设施（1 人日）

1. 安装 protoc + protoc-gen-go：
   ```bash
   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
   ```
2. 创建 `ssc/api/proto/` 目录 + `ssc.proto`
3. 编写 `.proto` 文件（所有消息 + 2 个 service）
4. `protoc --go_out=. --go-grpc_out=. ssc.proto`
5. 验证生成代码可编译

### Phase 1 — 客户端+服务端 stubs 共存（1 人日）

1. 新增 `ssc/comm_grpc.go` — 新的 gRPC 版 `Comm`（保留旧的 `comm.go` 不变）
2. 新增 `rpc/ssc_grpc.go` — gRPC handler 实现（重用现有 handler 逻辑，通过 convert.go 做 proto ↔ api 转换）
3. `node` 层：在 `StartRPC()` 中同时启动 gRPC server

**gRPC 启动时机：** 与 JSON-RPC 共用同一个 `api.Service` 实例，在 `node.StartRPC()` 中一并启动：

```go
// node/api.go
func (node *Node) StartRPC() error {
    harmony := hmy.New(...)
    apis := node.APIs(harmony)

    // 启动 JSON-RPC（不变）
    hmy_rpc.StartServers(harmony, apis, ...)

    // 启动 gRPC
    grpcPort := nodeconfig.GetSSCGrpcPortFromBase(node.NodeConfig.P2PPort)
    node.StartSSCGrpc(grpcPort)

    return nil
}

func (node *Node) StartSSCGrpc(port int) {
    lis, _ := net.Listen("tcp", fmt.Sprintf(":%d", port))
    grpcServer := grpc.NewServer()
    rpc.RegisterSSCGrpcServer(grpcServer, node.SSCService)  // 传入 api.Service
    go grpcServer.Serve(lis)
}
```

### Phase 2 — 逐个方法迁移（2-3 人日）

按调用频率由高到低逐步切换：
1. **HandleSimulateRequest** — 最热的路径，交互模拟核心
2. **HandleCommitVote + CommitSimulation** — 提交路径
3. **SignSimulationCommit + SignCXTSimulation** — 签名路径
4. **HandleCXTCall + HandleCXTSSCCall + RequestCallCXT** — CXT 交互
5. **HandleCXTCommitSSCVote + HandleCXTCommitProof** — 跨分片提交
6. **AddRetryTx + RetryCommit + RetryCancel + SignalReSimulation** — 重试路径
7. **SLTest + HandleNewEpoch + AddToPassivePool + HandleRetrySignal** — 低频方法
8. **StartSimulateCXTransaction** — 入口方法

每个方法迁移步骤：
a. 添加该方法的 gRPC call 到 `comm_grpc.go` 的 switch
b. 验证 handler 在 `rpc/ssc_grpc.go` 中已实现
c. 跑实验/测试验证功能正常

### Phase 3 — 清理（0.5 人日）

1. 删除 `ssc/comm.go`（旧 JSON-RPC client）
2. 删除 `rpc/ssc.go`（旧 JSON-RPC handler）
3. 删除 `ssc/api/sscs.go` 中的 Method_XXX 常量
4. 从 go-ethereum rpc server 注册中移除 SSC namespace

## 5. 风险与注意事项

### 5.1 端口管理
当前 JSON-RPC 与外部 API（hmy/eth/debug 等）共享 HTTP 端口。SSC 内部 RPC 走独立 gRPC 端口。

**决策：独立端口，基准 = `HTTPPort - 1000`**（等价于 `P2P_Port - 500`）。

端口继承关系（以 4 节点 4 shard 为例）：

| 机器 | Validator | P2P Port | HTTP/RPC Port (+500) | gRPC Port (-1000) |
|:----|:----------|:---------|:--------------------|:-----------------|
| 10.7.95.200 | #0 | 9000 | 9500 | 8500 |
| 10.7.95.200 | #1 | 9002 | 9502 | 8502 |
| 10.7.95.201 | #0 | 9040 | 9540 | 8540 |
| 10.7.95.201 | #1 | 9042 | 9542 | 8542 |

即 **不随 launch config 改**，gRPC 端口在代码中自动计算：`P2P_Port - 500`。

实现：在 `nodeconfig/network.go` 中加 offset 常量：

```go
// 新增：
rpcSSCGrpcPortOffset = -500

func GetSSCGrpcPortFromBase(basePort int) int {
    return basePort + rpcSSCGrpcPortOffset
}
```

P2P 端口 9000 → gRPC = 9000 - 500 = 8500。不新增配置项，代码自动计算。

**为什么不用端口复用：**
- 现有 HTTP server 由 go-ethereum 持有，SSC 只是其上的一个 namespace
- 端口复用需要修改 `rpc.StartHTTPEndpoint()` 启动链，插入 gRPC mux，影响所有模块
- 独立端口隔离 SSC 和外部 API 的启动/停止/日志，改动范围小

### 5.2 连接管理与 HTTP/2
gRPC 使用 HTTP/2 多路复用。一个连接支持多个并发流，不需要旧 comm.go 的每 endpoint 一个连接。

**简化方案：** 每个 endpoint 保持一个 `*grpc.ClientConn`，但不再需要 MaxIdleConns 配置。

### 5.3 超时机制
现有代码通过 `context.WithTimeout(ctx, config.CallTimeout)` 设置超时，gRPC 原生支持 context 传播。迁移后超时机制不变。

### 5.4 日志兼容性
当前 `comm.go` 中无日志（日志在调用方）。gRPC 层保持同样模式，不增加额外日志。

### 5.5 序列化性能对比
| 指标 | JSON | Protobuf |
|:-----|:----:|:---------|
| Marshal 10KB 结构 | ~5μs | ~1μs |
| Unmarshal 10KB 结构 | ~8μs | ~1μs |
| 编码体积 | 10-15KB | 4-6KB |
| 零拷贝读取 | ❌ | ✅ (字段直接取 bytes slice) |

## 6. 测试计划

1. **单元测试**：`ssc/api/proto/` 下 proto 消息的 marshal/unmarshal roundtrip（含 `big.Int`、`common.Hash` 等边 case）
2. **功能测试**：`test_single` 验证相同功能
3. **性能基准**：替换前后对比 pprof JSON 序列化 CPU 占比

## 7. 已定决策（备忘录）

| 问题 | 决策 |
|:-----|:-----|
| 端口分配 | 独立端口，`P2P_Port - 500`（HTTP - 1000），代码自动计算 |
| gRPC 启动时机 | `node.StartRPC()` 中一并启动，与 JSON-RPC 共享 `api.Service` 实例 |
| `types.Transaction`/`Receipt` 传输 | RLP 编码为 `bytes` 传输 |
| `StateSet` 序列化 | `repeated StateEntry` + `repeated StateKVPair`，不 map |
| 转换层 | 需要，`ssc/api/proto/convert.go`，业务代码零感知 |
| 字段编号优化 | 暂不优化，保持顺序编号 |
