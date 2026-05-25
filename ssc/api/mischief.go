package api

import "github.com/ethereum/go-ethereum/common"

// MischiefBehaviorType 作恶行为类型
type MischiefBehaviorType int

const (
	// BehaviorMaliciousDelay 恶意延迟指定时间
	BehaviorMaliciousDelay MischiefBehaviorType = iota
	// BehaviorDenyService 拒绝服务，直接返回错误
	BehaviorDenyService
	// BehaviorRandomTimeout 随机超时
	BehaviorRandomTimeout
	// BehaviorSignatureFail 签名验证失败
	BehaviorSignatureFail
	// BehaviorStateInconsistent 返回不一致的状态
	BehaviorStateInconsistent
	// BehaviorMessageDrop 丢弃消息不发送
	BehaviorMessageDrop
	// BehaviorWrongRelatedShards 返回错误的分片列表
	BehaviorWrongRelatedShards
)

// MischiefBehavior 单个作恶行为配置
type MischiefBehavior struct {
	// Type 行为类型
	Type MischiefBehaviorType
	// Probability 触发概率 (0.0-1.0)，1.0 表示必定触发
	Probability float64
	// DelayMs 延迟毫秒数（用于 BehaviorMaliciousDelay）
	DelayMs int64
	// ErrorCode 错误码（用于 BehaviorDenyService）
	ErrorCode string
	// ErrorMessage 错误消息
	ErrorMessage string
}

// MischiefConfig 作恶行为配置
type MischiefConfig struct {
	// Enabled 总开关
	Enabled bool
	// TargetTxHashes 目标交易哈希列表，空表示对所有交易生效
	TargetTxHashes []common.Hash
	// Behaviors 作恶行为列表
	Behaviors []MischiefBehavior
}

// DefaultMischiefConfig 返回默认配置（禁用状态）
func DefaultMischiefConfig() *MischiefConfig {
	return &MischiefConfig{
		Enabled:   false,
		Behaviors: make([]MischiefBehavior, 0),
	}
}

// NewMaliciousDelayConfig 创建恶意延迟配置
func NewMaliciousDelayConfig(delayMs int64, probability float64) *MischiefConfig {
	return &MischiefConfig{
		Enabled: true,
		Behaviors: []MischiefBehavior{
			{
				Type:        BehaviorMaliciousDelay,
				Probability: probability,
				DelayMs:     delayMs,
			},
		},
	}
}

// NewDenyServiceConfig 创建拒绝服务配置
func NewDenyServiceConfig(probability float64, errorCode, errorMessage string) *MischiefConfig {
	return &MischiefConfig{
		Enabled: true,
		Behaviors: []MischiefBehavior{
			{
				Type:         BehaviorDenyService,
				Probability:  probability,
				ErrorCode:    errorCode,
				ErrorMessage: errorMessage,
			},
		},
	}
}

// NewCombinedConfig 创建组合配置（延迟 + 拒绝服务）
func NewCombinedConfig(delayMs int64, delayProb float64, denyProb float64) *MischiefConfig {
	return &MischiefConfig{
		Enabled: true,
		Behaviors: []MischiefBehavior{
			{
				Type:        BehaviorMaliciousDelay,
				Probability: delayProb,
				DelayMs:     delayMs,
			},
			{
				Type:         BehaviorDenyService,
				Probability:  denyProb,
				ErrorCode:    "SERVICE_UNAVAILABLE",
				ErrorMessage: "service unavailable (mischief mode)",
			},
		},
	}
}
