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
	bc core.BlockChain, txSigner api.TxSigner, comm *Comm,
	internalPool *SSCInternalPool) api.Service {

	// 创建基础服务（原有逻辑完全不变）
	baseService := newBaseService(ctx, config, cm, sscConfig, signerMgr, bc, txSigner, comm)
	// DSN-48：注入与 txSubmitter 共享的内部池（main.go 创建）
	if internalPool != nil {
		baseService.internalPool = internalPool
		// DSN-57 探针：给 retryScheduler 接上“某上游是否在本分片 internalPool 排队(未上链)”的判别，
		// 供 verify fail-closed 区分“本分片上游没上链(queued)”与“上游在别的分片(absent→fail-open)”。
		if baseService.retryScheduler != nil {
			baseService.retryScheduler.SetQueuedSimCheck(internalPool.Has)
			// DSN-60 Task B：internalPool 接上“某上游是否已 on-chain 注册”的就绪谓词（闭包到
			// retryScheduler.upstreamOnChain）。供 Extract 对**本地链式** SimTx 做有界“上游先于下游”门，
			// 减少“上游 patch 未上链 → verify rollback”。口径只查本分片 onChainDAGPatches（本地依赖）。
			internalPool.SetSimUpstreamOnChain(baseService.retryScheduler.upstreamOnChain)
		}
	}

	// 如果启用作恶模式，返回代理；否则直接返回原服务（零开销）
	if config.MischiefConfig != nil && config.MischiefConfig.Enabled {
		utils.SSCLogger().Warn().
			Interface("config", config.MischiefConfig).
			Msg("[MISCHIEF] mischief mode enabled, wrapping service with proxy")
		return mischief.NewMischiefProxy(baseService, config.MischiefConfig)
	}

	return baseService
}
