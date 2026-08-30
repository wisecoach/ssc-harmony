package types

import (
	"io"

	"github.com/ethereum/go-ethereum/rlp"

	"github.com/harmony-one/harmony/block"
	staking "github.com/harmony-one/harmony/staking/types"
)

// BodyV2 is the V2 block body
type BodyV2 struct {
	f bodyFieldsV2
}

type bodyFieldsV2 struct {
	Transactions        []*Transaction
	StakingTransactions []*staking.StakingTransaction
	SSCTransactions     [][]*SSCInternalTx // 新增（DSN-47）：二维，第一维下标=InternalTxType（类型桶），第二维=行内交易（顺序=行内执行顺序）
	Uncles              []*block.Header
	IncomingReceipts    CXReceiptsProofs
}

// Transactions returns the list of transactions.
//
// The returned list is a deep copy; the caller may do anything with it without
// affecting the original.
func (b *BodyV2) Transactions() (txs []*Transaction) {
	for _, tx := range b.f.Transactions {
		txs = append(txs, tx.Copy())
	}
	return txs
}

// StakingTransactions returns the list of staking transactions.
// The returned list is a deep copy; the caller may do anything with it without
// affecting the original.
func (b *BodyV2) StakingTransactions() (txs []*staking.StakingTransaction) {
	for _, tx := range b.f.StakingTransactions {
		txs = append(txs, tx.Copy())
	}
	return txs
}

// TransactionAt returns the transaction at the given index in this block.
// It returns nil if index is out of bounds.
func (b *BodyV2) TransactionAt(index int) *Transaction {
	if index < 0 || index >= len(b.f.Transactions) {
		return nil
	}
	return b.f.Transactions[index].Copy()
}

// StakingTransactionAt returns the staking transaction at the given index in this block.
// It returns nil if index is out of bounds.
func (b *BodyV2) StakingTransactionAt(index int) *staking.StakingTransaction {
	if index < 0 || index >= len(b.f.StakingTransactions) {
		return nil
	}
	return b.f.StakingTransactions[index].Copy()
}

// NewSSCTransactions 返回一个长度 = InternalTxTypeCount() 的二维切片，
// 第一维可直接用 InternalTxType 做下标（例如 ssc[InternalTxTypeSimTx]）。
// 这是构造 SSCTransactions 的推荐入口。
func NewSSCTransactions() [][]*SSCInternalTx {
	return make([][]*SSCInternalTx, InternalTxTypeCount())
}

// SSCTransactions returns the SSC internal transactions as a 2-D slice
// (first dim index = InternalTxType bucket, second dim = tx within that type, in row order).
// The returned list is a deep copy; the caller may do anything with it without
// affecting the original.
func (b *BodyV2) SSCTransactions() (txs [][]*SSCInternalTx) {
	for _, row := range b.f.SSCTransactions {
		var rowCopy []*SSCInternalTx
		for _, tx := range row {
			rowCopy = append(rowCopy, tx.Copy())
		}
		txs = append(txs, rowCopy)
	}
	return txs
}

// SSCTransactionsByType returns a deep copy of the SSC internal transactions of the given type.
// It returns nil if t is out of range or that bucket is empty.
func (b *BodyV2) SSCTransactionsByType(t InternalTxType) []*SSCInternalTx {
	if int(t) < 0 || int(t) >= len(b.f.SSCTransactions) {
		return nil
	}
	var txs []*SSCInternalTx
	for _, tx := range b.f.SSCTransactions[t] {
		txs = append(txs, tx.Copy())
	}
	return txs
}

// SSCTransactionAt returns the SSC internal transaction at the given global index in this block,
// counting by type order (CRTx first, then SimTx, ...). It returns nil if index is out of bounds.
func (b *BodyV2) SSCTransactionAt(index int) *SSCInternalTx {
	if index < 0 {
		return nil
	}
	for _, row := range b.f.SSCTransactions {
		if index < len(row) {
			return row[index].Copy()
		}
		index -= len(row)
	}
	return nil
}

// CXReceiptAt returns the CXReceipt at given index in this block
// It returns nil if index is out of bounds
func (b *BodyV2) CXReceiptAt(index int) *CXReceipt {
	if index < 0 {
		return nil
	}
	for _, cxp := range b.f.IncomingReceipts {
		cxs := cxp.Receipts
		if index < len(cxs) {
			return cxs[index].Copy()
		}
		index -= len(cxs)
	}
	return nil
}

// SetTransactions sets the list of transactions with a deep copy of the given
// list.
func (b *BodyV2) SetTransactions(newTransactions []*Transaction) {
	var txs []*Transaction
	for _, tx := range newTransactions {
		txs = append(txs, tx.Copy())
	}
	b.f.Transactions = txs
}

// SetStakingTransactions sets the list of staking transactions with a deep copy of the given
// list.
func (b *BodyV2) SetStakingTransactions(newStakingTransactions []*staking.StakingTransaction) {
	var txs []*staking.StakingTransaction
	for _, tx := range newStakingTransactions {
		txs = append(txs, tx.Copy())
	}
	b.f.StakingTransactions = txs
}

// SetSSCTransactions sets the list of SSC internal transactions (2-D:
// first dim index = InternalTxType bucket, second dim = tx within that type) with a deep copy
// of the given list. The stored slice is normalized to length InternalTxTypeCount() so callers
// can always index by InternalTxType safely; missing buckets become empty (nil) rows.
func (b *BodyV2) SetSSCTransactions(newSSCTransactions [][]*SSCInternalTx) {
	txs := NewSSCTransactions() // length = InternalTxTypeCount()
	for i, row := range newSSCTransactions {
		if i >= len(txs) {
			break // ignore rows beyond defined types
		}
		for _, tx := range row {
			txs[i] = append(txs[i], tx.Copy())
		}
	}
	b.f.SSCTransactions = txs
}

// Uncles returns a deep copy of the list of uncle headers of this block.
func (b *BodyV2) Uncles() (uncles []*block.Header) {
	for _, uncle := range b.f.Uncles {
		uncles = append(uncles, CopyHeader(uncle))
	}
	return uncles
}

// SetUncles sets the list of uncle headers with a deep copy of the given list.
func (b *BodyV2) SetUncles(newUncle []*block.Header) {
	var uncles []*block.Header
	for _, uncle := range newUncle {
		uncles = append(uncles, CopyHeader(uncle))
	}
	b.f.Uncles = uncles
}

// IncomingReceipts returns a deep copy of the list of incoming cross-shard
// transaction receipts of this block.
func (b *BodyV2) IncomingReceipts() (incomingReceipts CXReceiptsProofs) {
	return b.f.IncomingReceipts.Copy()
}

// SetIncomingReceipts sets the list of incoming cross-shard transaction
// receipts of this block with a dep copy of the given list.
func (b *BodyV2) SetIncomingReceipts(newIncomingReceipts CXReceiptsProofs) {
	b.f.IncomingReceipts = newIncomingReceipts.Copy()
}

// EncodeRLP RLP-encodes the block body into the given writer.
func (b *BodyV2) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, &b.f)
}

// DecodeRLP RLP-decodes a block body from the given RLP stream into the
// receiver.
func (b *BodyV2) DecodeRLP(s *rlp.Stream) error {
	return s.Decode(&b.f)
}
