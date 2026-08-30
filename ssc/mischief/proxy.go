package mischief

import (
	"context"
	"math/rand"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/block"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/harmony-one/harmony/ssc/api/proto"
	"google.golang.org/protobuf/proto"
)

// MischiefProxy 作恶行为代理（装饰器模式）
// 零侵入原服务代码，通过代理层注入作恶行为
type MischiefProxy struct {
	api.Service // 被代理的真实服务
	config      *api.MischiefConfig
	rng         *rand.Rand
}

// NewMischiefProxy 创建作恶代理
func NewMischiefProxy(real api.Service, config *api.MischiefConfig) *MischiefProxy {
	return &MischiefProxy{
		Service: real,
		config:  config,
		rng:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// SimulateCXTransaction 代理交易模拟请求
func (p *MischiefProxy) SimulateCXTransaction(req *api.CXTSimulationRequest) {
	// 作恶行为：恶意延迟
	if p.shouldInject(req.Tx.Hash(), api.BehaviorMaliciousDelay) {
		p.injectDelay(req.Tx.Hash(), 3000) // 默认延迟 3s
	}

	// 委托给真实服务
	p.Service.SimulateCXTransaction(req)
}

// HandleSimulateRequest 代理处理模拟请求
func (p *MischiefProxy) HandleSimulateRequest(ctx context.Context, req *api.CXTSimulationRequest) *api.CXTSimulationResult {
	txHash := req.TxHash

	// 作恶行为：恶意延迟
	if p.shouldInject(txHash, api.BehaviorMaliciousDelay) {
		behavior := p.getBehavior(api.BehaviorMaliciousDelay)
		p.injectDelay(txHash, behavior.DelayMs)
	}

	// 作恶行为：拒绝服务
	if p.shouldInject(txHash, api.BehaviorDenyService) {
		return p.injectDenyService(req)
	}

	// 作恶行为：随机超时
	if p.shouldInject(txHash, api.BehaviorRandomTimeout) {
		return p.injectRandomTimeout(req)
	}

	// 正常委托给真实服务
	return p.Service.HandleSimulateRequest(ctx, req)
}

// RequestCallCXT 代理合约调用请求
func (p *MischiefProxy) RequestCallCXT(req *api.CXTCallRequest) *api.CXTCallSSCResult {
	txHash := req.TxHash

	// 作恶行为：拒绝服务
	if p.shouldInject(txHash, api.BehaviorDenyService) {
		return p.injectDenyServiceCall(req)
	}

	// 作恶行为：签名失败
	if p.shouldInject(txHash, api.BehaviorSignatureFail) {
		return p.injectSignatureFail(req)
	}

	// 委托给真实服务
	return p.Service.RequestCallCXT(req)
}

// CallCXTContract 代理合约调用
func (p *MischiefProxy) CallCXTContract(req *api.CXTCallRequest) *api.CXTCallSSCResult {
	txHash := req.TxHash

	// 作恶行为：拒绝服务
	if p.shouldInject(txHash, api.BehaviorDenyService) {
		return p.injectDenyServiceCall(req)
	}

	// 委托给真实服务
	return p.Service.CallCXTContract(req)
}

// VerifySimulation 代理验证模拟
func (p *MischiefProxy) VerifySimulation(simulationBytes []byte, stateDB api.StateDB, header *block.Header) {
	simulationProto := &sscpb.CXTSimulation{}
	err := proto.Unmarshal(simulationBytes, simulationProto)
	if err != nil {
		return
	}
	simulation := sscpb.CXTSimulationFromProto(simulationProto)
	// 作恶行为：状态不一致
	if p.shouldInject(simulation.TxHash, api.BehaviorStateInconsistent) {
		p.injectStateInconsistent(simulationBytes, stateDB, header)
		return
	}

	// 委托给真实服务
	p.Service.VerifySimulation(simulationBytes, stateDB, header)
}

// ========== 辅助方法 ==========

// shouldInject 检查是否应该注入作恶行为
func (p *MischiefProxy) shouldInject(txHash common.Hash, behaviorType api.MischiefBehaviorType) bool {
	if p.config == nil || !p.config.Enabled {
		return false
	}

	// 检查目标交易哈希
	if len(p.config.TargetTxHashes) > 0 {
		matched := false
		for _, target := range p.config.TargetTxHashes {
			if target == txHash {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	// 检查行为类型和概率
	for _, behavior := range p.config.Behaviors {
		if behavior.Type == behaviorType {
			if behavior.Probability >= 1.0 || p.rng.Float64() <= behavior.Probability {
				return true
			}
		}
	}

	return false
}

// getBehavior 获取指定类型的行为配置
func (p *MischiefProxy) getBehavior(behaviorType api.MischiefBehaviorType) api.MischiefBehavior {
	for _, behavior := range p.config.Behaviors {
		if behavior.Type == behaviorType {
			return behavior
		}
	}
	return api.MischiefBehavior{}
}

// injectDelay 注入恶意延迟
func (p *MischiefProxy) injectDelay(txHash common.Hash, delayMs int64) {
	utils.SSCLogger().Warn().
		Str("txHash", txHash.Hex()).
		Int64("delayMs", delayMs).
		Msg("[MISCHIEF] injecting malicious delay")
	time.Sleep(time.Duration(delayMs) * time.Millisecond)
}

// injectDenyService 注入拒绝服务（模拟请求）
func (p *MischiefProxy) injectDenyService(req *api.CXTSimulationRequest) *api.CXTSimulationResult {
	behavior := p.getBehavior(api.BehaviorDenyService)
	errMsg := behavior.ErrorMessage
	if errMsg == "" {
		errMsg = "service unavailable (mischief mode)"
	}

	utils.SSCLogger().Warn().
		Str("txHash", req.TxHash.Hex()).
		Msg("[MISCHIEF] denying service")

	return &api.CXTSimulationResult{
		Err: errMsg,
	}
}

// injectDenyServiceCall 注入拒绝服务（调用请求）
func (p *MischiefProxy) injectDenyServiceCall(req *api.CXTCallRequest) *api.CXTCallSSCResult {
	behavior := p.getBehavior(api.BehaviorDenyService)
	errMsg := behavior.ErrorMessage
	if errMsg == "" {
		errMsg = "service unavailable (mischief mode)"
	}

	utils.SSCLogger().Warn().
		Str("txHash", req.TxHash.Hex()).
		Msg("[MISCHIEF] denying service (call)")

	return &api.CXTCallSSCResult{
		Err: errMsg,
		BaseBLSSignedMessage: api.BaseBLSSignedMessage{
			Epochs: req.Epochs,
		},
	}
}

// injectRandomTimeout 注入随机超时
func (p *MischiefProxy) injectRandomTimeout(req *api.CXTSimulationRequest) *api.CXTSimulationResult {
	utils.SSCLogger().Warn().
		Str("txHash", req.TxHash.Hex()).
		Msg("[MISCHIEF] injecting random timeout")

	return &api.CXTSimulationResult{
		Err: "context deadline exceeded (mischief timeout)",
	}
}

// injectSignatureFail 注入签名失败
func (p *MischiefProxy) injectSignatureFail(req *api.CXTCallRequest) *api.CXTCallSSCResult {
	utils.SSCLogger().Warn().
		Str("txHash", req.TxHash.Hex()).
		Msg("[MISCHIEF] injecting signature failure")

	return &api.CXTCallSSCResult{
		Err: "signature verification failed (mischief mode)",
		BaseBLSSignedMessage: api.BaseBLSSignedMessage{
			Epochs: req.Epochs,
		},
	}
}

// injectStateInconsistent 注入状态不一致
func (p *MischiefProxy) injectStateInconsistent(simulationBytes []byte, stateDB api.StateDB, header *block.Header) {
	utils.SSCLogger().Warn().
		Msg("[MISCHIEF] injecting state inconsistency")
	// 故意不调用真实服务，让验证超时或失败
}
