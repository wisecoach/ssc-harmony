package types

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/harmony-one/harmony/crypto/hash"
)

// InternalTxType 标识 SSC 内部交易类型（DSN-47），并直接作为区块内二维
// SSCTransactions 的第一维下标（sscTransactions[InternalTxTypeCRTx] = CR 桶，……）。
// SimTx / CRTx 等内部交易不再硬塞进普通 types.Transaction（特殊 precompile 地址 + JSON），
// 而是作为独立结构统一承载。
//
// 注意：iota 顺序即执行顺序（由小到大遍历下标）——CRTx(0) 先 → VictimTx(1) →
// SimTx(2) → NewEpoch/UploadOpinions/Empty(3/4/5) 最后，与 DSN-47 §4.3 的行序约定一致。
//
// 执行顺序说明：
//   - CRTx 先：释放已提交/已回滚交易的链上锁；
//   - VictimTx 其次：CMH 判出的环内 victim 解锁（不碰状态，只触发委员会回滚投票），
//     必须排在 SimTx 之前，以便尽快解锁、避免被普通 SimTx 堵在块尾；
//   - SimTx 再后：此时上游 CRTx/VictimTx 已释放的锁才可被重模拟的 SimTx 抢到。
//
// ⚠️ 变更提醒（论文实验可重来，故允许重编号）：
//   - 插入 VictimTx 会让其后的 SimTx/NewEpoch/... 数值整体 +1，属于跨节点共识格式变更，
//     重放此前的旧块会分叉；若需保留历史重放兼容，请不要重编号。
//   - 新增类型时若要求“执行顺序在已有某类型之前”，必须插入而非末尾追加，并同步重编号。
type InternalTxType uint8

const (
	InternalTxTypeCRTx InternalTxType = iota
	InternalTxTypeVictimTx
	InternalTxTypeSimTx
	InternalTxTypeNewEpoch
	InternalTxTypeUploadOpinions
	InternalTxTypeEmpty
)

func (t InternalTxType) String() string {
	switch t {
	case InternalTxTypeCRTx:
		return "CRTx"
	case InternalTxTypeVictimTx:
		return "VictimTx"
	case InternalTxTypeSimTx:
		return "SimTx"
	case InternalTxTypeNewEpoch:
		return "NewEpoch"
	case InternalTxTypeUploadOpinions:
		return "UploadOpinions"
	case InternalTxTypeEmpty:
		return "Empty"
	default:
		return "Unknown"
	}
}

// InternalTxTypeCount 返回已定义的内部交易类型数量，用作二维 SSCTransactions 第一维长度。
func InternalTxTypeCount() int {
	return int(InternalTxTypeEmpty) + 1
}

// SSCInternalTx 是 SSC 内部交易的统一实体（RLP 外壳 + 不透明 payload）。
// 进入区块 / 交易池 / 网络统一使用。区块内以二维 [][]*SSCInternalTx 承载，
// 第一维下标 = InternalTxType（类型桶），第二维 = 该类型行内交易（顺序 = 行内执行顺序）。
// 执行时按下标（类型）从小到大遍历：CRTx(0) → VictimTx(1) → SimTx(2) → NewEpoch/UploadOpinions/Empty(3/4/5)。
type SSCInternalTx struct {
	Type    InternalTxType
	Shard   uint32
	Payload []byte // 内部载荷：protobuf（未来，DSN-47 §4.2/R9）；当前迁移前为既有 JSON 字节
}

// Hash 由内容派生（Type+Shard+Payload 的 RLP），不落盘字段，避免两节点各存一份对不上。
func (t *SSCInternalTx) Hash() common.Hash {
	return hash.FromRLP(t)
}

// Copy 深拷贝。
func (t *SSCInternalTx) Copy() *SSCInternalTx {
	if t == nil {
		return nil
	}
	cpy := *t
	if t.Payload != nil {
		cpy.Payload = make([]byte, len(t.Payload))
		copy(cpy.Payload, t.Payload)
	}
	return &cpy
}

// String 返回可读描述。
func (t *SSCInternalTx) String() string {
	if t == nil {
		return "SSCInternalTx<nil>"
	}
	return "SSCInternalTx{type=" + t.Type.String() + ", shard=" + fmt.Sprint(t.Shard) + "}"
}

// SSCTransactions 是二维 SSC 内部交易列表的扁平化视图，实现 DerivableBase，
// 用于把 SSC 内部交易并入区块 TxHash（DeriveSha）。扁平化顺序 = 按类型下标
// （执行顺序）从小到大遍历，与执行语义一致。
type SSCTransactions [][]*SSCInternalTx

// Len 返回所有类型桶内交易总数。
func (s SSCTransactions) Len() int {
	n := 0
	for _, row := range s {
		n += len(row)
	}
	return n
}

// GetRlp 返回扁平化后第 i 个内部交易的 RLP。
func (s SSCTransactions) GetRlp(i int) []byte {
	for _, row := range s {
		if i < len(row) {
			enc, _ := rlp.EncodeToBytes(row[i])
			return enc
		}
		i -= len(row)
	}
	return nil
}
