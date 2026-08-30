package ssc

import (
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/internal/utils"
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

// Extract 按类型顺序（CRTx(0) → SimTx(1) → NewEpoch/UploadOpinions/Empty(2/3/4)）取出一批，
// 返回二维（第一维=类型桶，第二维=行内顺序）。maxTotal<=0 表示不限制。
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
