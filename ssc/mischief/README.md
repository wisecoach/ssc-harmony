# Mischief Mode - 作恶行为测试框架

## 概述

通过**代理/装饰器模式**实现零侵入的作恶行为注入，用于测试跨分片交易模拟系统的容错能力。

## 设计原则

1. **零侵入**：原服务代码无需任何 `if` 判断，作恶逻辑完全隔离在代理层
2. **配置化**：通过配置文件或代码灵活控制行为类型、概率、目标交易
3. **可观测**：所有作恶行为都有 `[MISCHIEF]` 日志标记
4. **零开销**：禁用模式下直接返回原服务，无性能损失

## 快速开始

### 1. 基础用法 - 恶意延迟 3 秒

```go
import "github.com/harmony-one/harmony/ssc/mischief"

// 创建作恶配置：100% 概率延迟 3 秒
config := mischief.NewMaliciousDelayConfig(3000, 1.0)

// 创建服务（带作恶行为）
service := ssc.NewServiceWithMischief(ctx, config, cm, sscConfig, signerMgr, bc, txSigner, comm, config)
```

### 2. 组合行为 - 延迟 + 拒绝服务

```go
// 创建组合配置：
// - 100% 概率延迟 3 秒
// - 30% 概率拒绝服务
config := mischief.NewCombinedConfig(
    3000,   // 延迟毫秒数
    1.0,    // 延迟概率
    0.3,    // 拒绝服务概率
)

service := ssc.NewServiceWithMischief(ctx, config, cm, sscConfig, signerMgr, bc, txSigner, comm, config)
```

### 3. 针对特定交易

```go
config := &mischief.MischiefConfig{
    Enabled: true,
    TargetTxHashes: []common.Hash{
        common.HexToHash("0x1234567890abcdef..."),
    },
    Behaviors: []mischief.MischiefBehavior{
        {
            Type:        mischief.BehaviorMaliciousDelay,
            Probability: 1.0,
            DelayMs:     3000,
        },
    },
}

service := ssc.NewServiceWithMischief(ctx, config, cm, sscConfig, signerMgr, bc, txSigner, comm, config)
```

### 4. 禁用模式（生产环境）

```go
// 方式 1：传入 nil
service := ssc.NewServiceWithMischief(ctx, config, cm, sscConfig, signerMgr, bc, txSigner, comm, nil)

// 方式 2：使用默认配置（Enabled=false）
service := ssc.NewServiceWithMischief(ctx, config, cm, sscConfig, signerMgr, bc, txSigner, comm, mischief.DefaultMischiefConfig())

// 方式 3：使用原有 API（向后兼容）
service := ssc.NewService(ctx, config, cm, sscConfig, signerMgr, bc, txSigner, comm)
```

## 作恶行为类型

| 类型 | 常量 | 说明 | 注入点 |
|------|------|------|--------|
| 恶意延迟 | `BehaviorMaliciousDelay` | 延迟指定毫秒数 | `SimulateCXTransaction`, `HandleSimulateRequest` |
| 拒绝服务 | `BehaviorDenyService` | 直接返回错误 | `HandleSimulateRequest`, `RequestCallCXT` |
| 随机超时 | `BehaviorRandomTimeout` | 模拟上下文超时 | `HandleSimulateRequest` |
| 签名失败 | `BehaviorSignatureFail` | 模拟签名验证失败 | `RequestCallCXT` |
| 状态不一致 | `BehaviorStateInconsistent` | 跳过验证导致超时 | `VerifySimulation` |
| 消息丢弃 | `BehaviorMessageDrop` | 不发送投票消息 | `sendCXTCommitVote` |
| 错误分片 | `BehaviorWrongRelatedShards` | 返回错误分片列表 | `CommitSimulation` |

## 配置结构

```go
type MischiefConfig struct {
    Enabled        bool                // 总开关
    TargetTxHashes []common.Hash       // 目标交易（空=全部）
    Behaviors      []MischiefBehavior  // 行为列表
}

type MischiefBehavior struct {
    Type        MischiefBehaviorType  // 行为类型
    Probability float64               // 触发概率 (0.0-1.0)
    DelayMs     int64                 // 延迟毫秒数
    ErrorCode   string                // 错误码
    ErrorMessage string               // 错误消息
}
```

## 日志示例

```
[WARN][MISCHIEF] mischief mode enabled, wrapping service with proxy config={...}
[WARN][MISCHIEF] injecting malicious delay txHash=0x... delayMs=3000
[WARN][MISCHIEF] denying service txHash=0x...
```

## 目录结构

```
ssc/
├── api/
│   └── service.go          # SSCService 接口定义
├── impl.go                 # sscService 实现（零修改）
├── factory.go              # 工厂方法
└── mischief/
    ├── config.go           # 配置结构
    ├── proxy.go            # 代理实现
    ├── README.md           # 本文档
    └── proxy_test.go       # 单元测试（待添加）
```

## 测试建议

1. **单元测试**：为 `MischiefProxy` 编写独立测试，验证各行为正确触发
2. **集成测试**：在测试网部署，观察系统对作恶行为的容错能力
3. **压力测试**：高概率作恶模式下测试系统恢复能力
4. **生产验证**：确保 `Enabled=false` 时零性能开销

## 移除作恶代码

测试完成后，只需删除 `mischief/` 目录和 `factory.go` 中的相关代码，`impl.go` 无需任何修改。

## 扩展新行为

1. 在 `config.go` 添加新的行为类型常量
2. 在 `proxy.go` 添加对应的注入方法
3. 在对应接口方法中调用 `shouldInject` 检查

```go
// 示例：添加新行为
const BehaviorNewType MischiefBehaviorType = iota

func (p *MischiefProxy) HandleSimulateRequest(...) *api.CXTSimulationResult {
    if p.shouldInject(req.TxHash, BehaviorNewType) {
        return p.injectNewType(req)
    }
    return p.real.HandleSimulateRequest(ctx, req)
}
```
