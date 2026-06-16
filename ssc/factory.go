package ssc

import (
	"context"

	"github.com/harmony-one/harmony/core"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/harmony-one/harmony/ssc/mischief"
)

// ServiceConfig 服务配置（包含可选的作恶配置）
type ServiceConfig struct {
	*api.Config
	MischiefConfig *api.MischiefConfig // 作恶行为配置（测试用）
}

// NewService 创建 SSC 服务
// 如果 mischiefConfig 不为空且 Enabled=true，则返回带作恶行为的代理
// 否则返回原始服务（零性能开销）
func NewService(ctx context.Context, config *api.Config, cm *CommitteeMechanism,
	sscConfig *api.ShardSimulateCommitteeConfig, signerMgr api.BLSSignerMgr,
	bc core.BlockChain, txSigner api.TxSigner, comm *Comm) api.Service {

	// 创建基础服务（原有逻辑完全不变）
	baseService := newBaseService(ctx, config, cm, sscConfig, signerMgr, bc, txSigner, comm)

	// 如果启用作恶模式，返回代理；否则直接返回原服务（零开销）
	if config.MischiefConfig != nil && config.MischiefConfig.Enabled {
		utils.SSCLogger().Warn().
			Interface("config", config.MischiefConfig).
			Msg("[MISCHIEF] mischief mode enabled, wrapping service with proxy")
		return mischief.NewMischiefProxy(baseService, config.MischiefConfig)
	}

	return baseService
}
