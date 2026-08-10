# HANDOFF-20260725-grpc-activation

> from_session: 20260724_230000 (hentai_coder-designer)
> from_role: Designer
> to_role: Designer (next session)

## 已完成

1. **Proto 定义 + 生成代码**
2. **convert.go** — proto ↔ api 转换（Receipt 用 JSON bytes）
3. **rpc/ssc_grpc.go** — gRPC handler stubs + `RegisterSSCGrpcServer()`
4. **comm.go** — 替换为 gRPC client（Call/Multicast/CallToEndpoint）
5. **nodeconfig/network.go** — `sscGrpcPortOffset = -500`
6. **node/api.go** — `StartSSCGrpc()` 在 `StartRPC()` 中启动
7. **node/node.go** — `sscGrpcServer` 字段
8. **types.go** — `Member.SSCEndpoint` + JSON tag
9. **build_keys.go** — `SSCEndpoint` 生成

## 问题：火焰图显示仍是 JSON-RPC

`rpc.(*PublicSSCShardService).HandleSimulateRequest` 被调用，说明 gRPC 没走通。

**怀疑**：`Member.SSCEndpoint` 没有被 committee JSON 反序列化填充。配置文件中已有 `sscendpoint: 10.7.95.X:85XX`，但运行时 `SSCEndpoint` 为空 → fallback → grpc dial 到 HTTP 端口 9500 失败 → 回退？

**配置加载链路**：genesis YAML → `Genesis.SSCConfig` → `json.Marshal` 写入 stateDB → `json.Unmarshal` 运行时读取。

## 下一步

1. 确认 `SSCEndpoint` 是否被填充：在 `comm.Call()` 里加日志打印 endpoint
2. 给 `ShardSimulateCommittee.Members` 和 `Validators` 加 JSON tag
3. 不需要 fallback 时删除 fallback 逻辑
4. 编译同步远程，跑实验 + 火焰图验证
