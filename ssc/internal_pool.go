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

// SSCInternalPool 最简内部交易池（DSN-48）。
// 只做三件事：存储、提取、区块提交后清理。分组/冲突/DAG 留给 DSN-46。
type SSCInternalPool struct {
	mu sync.Mutex
	// 按类型分桶存储（第一维下标 = InternalTxType，与 DSN-47 一致）
	txs map[types.InternalTxType][]*types.SSCInternalTx
	// hash 索引：幂等去重 + 提交后清理
	byHash map[common.Hash]*types.SSCInternalTx
}

// NewSSCInternalPool 创建一个空内部池。
func NewSSCInternalPool() *SSCInternalPool {
	return &SSCInternalPool{
		txs:    make(map[types.InternalTxType][]*types.SSCInternalTx),
		byHash: make(map[common.Hash]*types.SSCInternalTx),
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
			// 注：跨批/跨分片的上游就绪不在本池扣留（那会卡死跨分片下游）；verify 已改为
			// **确定性**判定——只查链上 onChainDAGPatches 有无该上游 patch，无则直接回滚，
			// 且不再依赖本池/offChainDAG 做 fail-open（见 verify.go DSN-57 说明）。
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
