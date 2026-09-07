package ssc

import (
	"sort"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	sscpb "github.com/harmony-one/harmony/ssc/api/proto"
	"google.golang.org/protobuf/proto"
)

// simNotReadyHoldBlocks — DSN-60 Task B：链式下游 SimTx 因“本分片本地上游未就绪”被扣留的
// Extract 次数上限。达到后强制放行一次（若本地上游真缺失/已终局，则走 verify fail-closed 回滚传播），
// 避免把下游无限扣在池里饿死（DSN-57 的教训：不得长期扣留跨分片下游）。
const simNotReadyHoldBlocks = 50

// SSCInternalPool 最简内部交易池（DSN-48）。
// 只做三件事：存储、提取、区块提交后清理。分组/冲突/DAG 留给 DSN-46。
type SSCInternalPool struct {
	mu sync.Mutex
	// 按类型分桶存储（第一维下标 = InternalTxType，与 DSN-47 一致）
	txs map[types.InternalTxType][]*types.SSCInternalTx
	// hash 索引：幂等去重 + 提交后清理
	byHash map[common.Hash]*types.SSCInternalTx

	// simUpstreamOnChain — DSN-60 Task B 就绪谓词：给定上游 txHash 是否已在本分片
	// onChainDAGPatches 注册（已上链验证成功）。由 sscService/factory 接线（闭包到
	// retryScheduler.upstreamOnChain）。nil = 未接线 → 不做扣留，保持旧行为。
	//
	// 口径：只查 SimTx.UpstreamTxList 里**本分片本地**的上游（CommitSimulation 已用
	// LocalUpstreamTxRef 收敛），因此绝不扣留“上游只在别的分片”的跨分片下游
	// （DSN-57 饿死根因，这里刻意避开）。
	simUpstreamOnChain func(common.Hash) bool

	// notReadyHoldCnt — DSN-60 Task B 有界扣留计数：链式 SimTx 因“本分片本地上游既不在同批、
	// 也未 on-chain”而被扣留(未放入本块)的 Extract 次数。达到 simNotReadyHoldBlocks 阈值后
	// 强制放行一次（走 verify 确定性 fail-closed，若本地上游真缺失则回滚传播，避免无限饿死）。
	notReadyHoldCnt map[common.Hash]int64
}

// NewSSCInternalPool 创建一个空内部池。
func NewSSCInternalPool() *SSCInternalPool {
	return &SSCInternalPool{
		txs:             make(map[types.InternalTxType][]*types.SSCInternalTx),
		byHash:          make(map[common.Hash]*types.SSCInternalTx),
		notReadyHoldCnt: make(map[common.Hash]int64),
	}
}

// Add 把一条内部交易加入池中。相同 hash 幂等去重（重复广播/重复提交无害）。
func (p *SSCInternalPool) Add(tx *types.SSCInternalTx) error {
	if tx == nil {
		return nil
	}
	h := tx.Hash()
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.byHash[h]; ok {
		return nil // 已存在，去重
	}
	p.txs[tx.Type] = append(p.txs[tx.Type], tx.Copy())
	p.byHash[h] = tx.Copy()
	return nil
}

// Extract 按类型顺序（CRTx(0) → VictimTx(1) → SimTx(2) → NewEpoch/UploadOpinions/Empty(3/4/5)）
// 取出一批，返回二维（第一维=类型桶，第二维=行内顺序）。maxTotal<=0 表示不限制。
// 最简版：直接按桶序取，不做冲突分组（DSN-46 在此处接入进池仲裁）。
func (p *SSCInternalPool) Extract(maxTotal int) [][]*types.SSCInternalTx {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([][]*types.SSCInternalTx, types.InternalTxTypeCount())
	remaining := maxTotal
	for t := 0; t < types.InternalTxTypeCount(); t++ {
		tt := types.InternalTxType(t)
		row := p.txs[tt]
		if len(row) == 0 {
			continue
		}
		// VictimTx：按 victim 优先级（Nonce>OriginShardID>TxHash）确定性排序，
		// 保证各分片/各块内处理顺序一致（DSN-53 VictimTx 方案）。
		if tt == types.InternalTxTypeVictimTx {
			sort.SliceStable(row, func(i, j int) bool {
				pi, oki := p.victimPriority(row[i])
				pj, okj := p.victimPriority(row[j])
				if !oki {
					return false
				}
				if !okj {
					return true
				}
				return pi.Less(pj)
			})
		}
		// DSN-52 §4.6：同分片内 SimTx 按确定性优先级（Nonce>OriginShardID>TxHash）排序，
		// 让各分片处理顺序尽量一致，降低随机冲突频率（辅助，不替代冲突时的 Wound-Wait 判定）。
		// 解不出优先级的（损坏 payload）保持原相对顺序（放后面）。
		if tt == types.InternalTxTypeSimTx {
			// DSN-52 §4.6：SimTx 按确定性优先级（Nonce>OriginShardID>TxHash）排序。
			sort.SliceStable(row, func(i, j int) bool {
				pi, oki := p.simPriority(row[i])
				pj, okj := p.simPriority(row[j])
				if !oki {
					return false
				}
				if !okj {
					return true
				}
				return pi.Less(pj)
			})
			// DSN-52：DAG 依赖排序——被依赖的 SimTx 在依赖它的 SimTx 之前执行，
			// 保证链式交易的 Upstream 先落块，避免下游 SimTx 在链上验证时上游还没就绪。
			// 这是 internal_pool 保证“下游 SimTx 排在上游 SimTx 之后”的职责所在（同批内）。
			row = p.orderSimTxsByDAG(row)
			// DSN-60 Task B：有界“上游先于下游”门（同批 DAG 序之外，补“跨批/跨块本地依赖”缺口）。
			// 只对本分片**本地**上游（SimTx.UpstreamTxList 已经 LocalUpstreamTxRef 收敛）做就绪判定：
			//   - 上游同批 → orderSimTxsByDAG 已保证 U 在 D 前；
			//   - 上游已 on-chain（upstreamOnChain）→ 就绪；
			//   - 否则（本地上游既不在同批、也未 on-chain）→ 有界扣留，不放入本块，避免 verify
			//     提前 rollback；达 simNotReadyHoldBlocks 后强制放行（真缺失则走 fail-closed 回滚）。
			// 绝不对“上游只在别的分片”的跨分片下游扣留（DSN-57 饿死根因，本池只按本地口径判定）。
			row = p.filterReadySimTxs(row)
		}
		if remaining <= 0 {
			// 不限或已取满上限：整桶取
			for _, tx := range row {
				out[tt] = append(out[tt], tx.Copy())
			}
			if maxTotal > 0 {
				remaining -= len(row)
			}
			continue
		}
		// 还有剩余预算，最多取 remaining 笔
		n := len(row)
		if n > remaining {
			n = remaining
		}
		for i := 0; i < n; i++ {
			out[tt] = append(out[tt], row[i].Copy())
		}
		remaining -= n
	}
	return out
}

// simUpstreams 从 SimTx payload 解出其 DAG 上游依赖（UpstreamTxList 的 txHash 集合）。
// 解不出（非 SimTx / payload 损坏）返回空集。
func (p *SSCInternalPool) simUpstreams(tx *types.SSCInternalTx) []common.Hash {
	if tx == nil || tx.Type != types.InternalTxTypeSimTx {
		return nil
	}
	sim := &sscpb.CXTSimulation{}
	if err := proto.Unmarshal(tx.Payload, sim); err != nil {
		return nil
	}
	apiSim := sscpb.CXTSimulationFromProto(sim)
	if apiSim == nil {
		return nil
	}
	out := make([]common.Hash, 0, len(apiSim.UpstreamTxList))
	for _, up := range apiSim.UpstreamTxList {
		out = append(out, up.TxHash)
	}
	return out
}

// SetSimUpstreamOnChain — DSN-60 Task B：注入“某上游是否已在本分片 onChainDAGPatches 注册”的
// 就绪谓词（本地依赖口径）。由 sscService/factory 接线到 retryScheduler.upstreamOnChain。
// 未接线(nil)时本池不做扣留，仅保留同批 DAG 排序（旧行为）。
func (p *SSCInternalPool) SetSimUpstreamOnChain(f func(common.Hash) bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.simUpstreamOnChain = f
}

// filterReadySimTxs — DSN-60 Task B：对已按 DAG 排序的 SimTx 行做“就绪筛选”。
// 保持 DAG 顺序，只保留“本地依赖就绪”的 SimTx；not-ready 的下游留在池中（不放入本块、不占预算），
// 达 simNotReadyHoldBlocks 后强制放行一次。未接线就绪谓词时原样返回（旧行为）。
func (p *SSCInternalPool) filterReadySimTxs(row []*types.SSCInternalTx) []*types.SSCInternalTx {
	if p.simUpstreamOnChain == nil || len(row) < 2 {
		return row
	}
	inRow := make(map[common.Hash]struct{}, len(row))
	for _, tx := range row {
		inRow[tx.Hash()] = struct{}{}
	}
	out := make([]*types.SSCInternalTx, 0, len(row))
	for _, tx := range row {
		if p.simTxLocalDepsReady(tx, inRow) {
			delete(p.notReadyHoldCnt, tx.Hash())
			out = append(out, tx)
			continue
		}
		// 本地上游未就绪 → 有界扣留
		cnt := p.notReadyHoldCnt[tx.Hash()] + 1
		p.notReadyHoldCnt[tx.Hash()] = cnt
		if cnt >= simNotReadyHoldBlocks {
			// 达阈值：强制放行一次（真缺失则由 verify fail-closed 回滚传播，避免无限饿死）。
			delete(p.notReadyHoldCnt, tx.Hash())
			out = append(out, tx)
		}
		// 未达阈值：not-ready，不放入本块（留在池中）
	}
	return out
}

// simTxLocalDepsReady — DSN-60 Task B：判断 SimTx 的本分片本地依赖是否已就绪。
// 对每个上游 U：U 在本批(inRow) → 已由 orderSimTxsByDAG 保证 U 在 D 前（就绪）；
// 否则 U 已 on-chain（upstreamOnChain）→ 就绪；否则 not-ready。
func (p *SSCInternalPool) simTxLocalDepsReady(tx *types.SSCInternalTx, inRow map[common.Hash]struct{}) bool {
	if tx == nil {
		return true
	}
	for _, up := range p.simUpstreams(tx) {
		if _, ok := inRow[up]; ok {
			continue // 同批，DAG 序已保证 U 先于 D
		}
		if p.simUpstreamOnChain(up) {
			continue // 上游已上链
		}
		return false // 本地上游既不在同批、也未 on-chain → not-ready
	}
	return true
}

// orderSimTxsByDAG 把同批 SimTx 按 DAG 依赖排序：被依赖（Upstream）的 SimTx 先于依赖者。
// 入参 row 已按优先级排序；只考虑同在 batch 内的上游（batch 外上游不受本 batch 顺序约束）。
// 出现环（异常）时按剩余顺序兜底，不阻塞。
func (p *SSCInternalPool) orderSimTxsByDAG(row []*types.SSCInternalTx) []*types.SSCInternalTx {
	if len(row) < 2 {
		return row
	}
	inBatch := make(map[common.Hash]struct{}, len(row))
	for _, tx := range row {
		inBatch[tx.Hash()] = struct{}{}
	}
	// deps[h] = h 的、且在本 batch 内的上游集合
	deps := make(map[common.Hash]map[common.Hash]struct{}, len(row))
	for _, tx := range row {
		h := tx.Hash()
		deps[h] = make(map[common.Hash]struct{})
		for _, up := range p.simUpstreams(tx) {
			if _, ok := inBatch[up]; ok {
				deps[h][up] = struct{}{}
			}
		}
	}
	out := make([]*types.SSCInternalTx, 0, len(row))
	done := make(map[common.Hash]struct{}, len(row))
	for len(out) < len(row) {
		progress := false
		for _, tx := range row {
			h := tx.Hash()
			if _, ok := done[h]; ok {
				continue
			}
			ready := true
			for up := range deps[h] {
				if _, ok := done[up]; !ok {
					ready = false
					break
				}
			}
			if ready {
				out = append(out, tx)
				done[h] = struct{}{}
				progress = true
			}
		}
		if !progress {
			// 环（异常）：把剩余未排的按原顺序追加，避免死循环
			for _, tx := range row {
				if _, ok := done[tx.Hash()]; !ok {
					out = append(out, tx)
					done[tx.Hash()] = struct{}{}
				}
			}
			break
		}
	}
	return out
}

// simPriority 从 SimTx payload 解出原始跨分片交易的确定性优先级（DSN-52 §4.6）。
// 优先级 `Nonce > OriginShardID > TxHash` 与 VerifySimulation 冲突判定（§4.2）一致，
// 用于同分片内 SimTx 处理顺序排序。解不出（非 SimTx / payload 损坏）返回 (zero,false)。
func (p *SSCInternalPool) simPriority(tx *types.SSCInternalTx) (api.Priority, bool) {
	if tx == nil || tx.Type != types.InternalTxTypeSimTx {
		return api.Priority{}, false
	}
	sim := &sscpb.CXTSimulation{}
	if err := proto.Unmarshal(tx.Payload, sim); err != nil {
		return api.Priority{}, false
	}
	apiSim := sscpb.CXTSimulationFromProto(sim)
	if apiSim == nil {
		return api.Priority{}, false
	}
	return api.Priority{
		Nonce:         apiSim.Nonce,
		OriginShardID: apiSim.OriginShardId,
		TxHash:        apiSim.TxHash,
	}, true
}

// victimPriority 从 VictimTx payload 解出 victim 的确定性优先级（Nonce>OriginShardID>TxHash），
// 用于同分片内 VictimTx 桶内排序。解不出（非 VictimTx / payload 损坏）返回 (zero,false)。
func (p *SSCInternalPool) victimPriority(tx *types.SSCInternalTx) (api.Priority, bool) {
	if tx == nil || tx.Type != types.InternalTxTypeVictimTx {
		return api.Priority{}, false
	}
	vt := &sscpb.VictimTx{}
	if err := proto.Unmarshal(tx.Payload, vt); err != nil {
		return api.Priority{}, false
	}
	apiVt := sscpb.VictimTxFromProto(vt)
	if apiVt == nil {
		return api.Priority{}, false
	}
	return api.Priority{
		Nonce:         apiVt.Nonce,
		OriginShardID: apiVt.OriginShardId,
		TxHash:        apiVt.TxHash,
	}, true
}

// Remove 把一条内部交易从池中删除（按 hash 匹配）。
// 用于执行失败的内部交易：不清理会每块被重复提取/重试，浪费出块预算（DSN-48 修复）。
// 对不存在/已删除的条目幂等（no-op）。
func (p *SSCInternalPool) Remove(tx *types.SSCInternalTx) {
	if tx == nil {
		return
	}
	h := tx.Hash()
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.byHash[h]; !ok {
		return
	}
	delete(p.byHash, h)
	delete(p.notReadyHoldCnt, h) // DSN-60 Task B：移除时清掉其有界扣留计数
	tt := tx.Type
	slice := p.txs[tt]
	for i, e := range slice {
		if e.Hash() == h {
			p.txs[tt] = append(slice[:i], slice[i+1:]...)
			break
		}
	}
	utils.SSCLogger().Warn().
		Str("sscType", tt.String()).
		Str("sscHash", h.Hex()).
		Msg("[SSCInternalPool] removed failed internal tx")
}

// OnBlockCommitted 区块提交后，把已进块的内部交易从池中删除（按 block.SSCTransactions 的 hash）。
func (p *SSCInternalPool) OnBlockCommitted(block *types.Block) {
	if block == nil {
		return
	}
	removed := 0
	remaining := 0
	p.mu.Lock()
	for _, row := range block.SSCTransactions() {
		for _, tx := range row {
			h := tx.Hash()
			if _, ok := p.byHash[h]; !ok {
				continue
			}
			delete(p.byHash, h)
			delete(p.notReadyHoldCnt, h) // DSN-60 Task B：落块后清掉其有界扣留计数
			// 从对应类型桶移除
			tt := tx.Type
			slice := p.txs[tt]
			for i, e := range slice {
				if e.Hash() == h {
					p.txs[tt] = append(slice[:i], slice[i+1:]...)
					break
				}
			}
			removed++
		}
	}
	for _, row := range p.txs {
		remaining += len(row)
	}
	p.mu.Unlock()
	utils.SSCLogger().Info().
		Uint64("blockNum", block.NumberU64()).
		Int("removed", removed).
		Int("remaining", remaining).
		Msg("[SSCInternalPool] OnBlockCommitted cleanup")
}

// Len 返回池中内部交易总数（不锁内调用注意）。
func (p *SSCInternalPool) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, row := range p.txs {
		n += len(row)
	}
	return n
}

// Has 判断某条内部交易是否仍在池中（含 SimTx——用于 DSN-57 判别“上游是否在本分片池里排队”）。
func (p *SSCInternalPool) Has(h common.Hash) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.byHash[h]
	return ok
}
