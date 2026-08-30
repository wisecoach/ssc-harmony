package types

import (
	"io"

	"github.com/ethereum/go-ethereum/rlp"

	"github.com/harmony-one/harmony/block"
	staking "github.com/harmony-one/harmony/staking/types"
)

// BodyV1 is the V1 block body
type BodyV1 struct {
	f bodyFieldsV1
}

type bodyFieldsV1 struct {
	Transactions     []*Transaction
	SSCTransactions  [][]*SSCInternalTx // 新增（DSN-47）：二维，第一维下标=InternalTxType，第二维=行内顺序
	Uncles           []*block.Header
	IncomingReceipts CXReceiptsProofs
}

// Transactions returns the list of transactions.
//
// The returned list is a deep copy; the caller may do anything with it without
// affecting the original.
func (b *BodyV1) Transactions() (txs []*Transaction) {
	for _, tx := range b.f.Transactions {
		txs = append(txs, tx.Copy())
	}
	return txs
}

// StakingTransactions returns the list of staking transactions.
// The returned list is a deep copy; the caller may do anything with it without
// affecting the original.
func (b *BodyV1) StakingTransactions() (txs []*staking.StakingTransaction) {
	return nil
}

// TransactionAt returns the transaction at the given index in this block.
// It returns nil if index is out of bounds.
func (b *BodyV1) TransactionAt(index int) *Transaction {
	if index < 0 || index >= len(b.f.Transactions) {
		return nil
	}
	return b.f.Transactions[index].Copy()
}

// CXReceiptAt returns the CXReceipt at given index in this block
// It returns nil if index is out of bounds
func (b *BodyV1) CXReceiptAt(index int) *CXReceipt {
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
func (b *BodyV1) SetTransactions(newTransactions []*Transaction) {
	var txs []*Transaction
	for _, tx := range newTransactions {
		txs = append(txs, tx.Copy())
	}
	b.f.Transactions = txs
}

// SetStakingTransactions sets the list of staking transactions with a deep copy of the given
// list. (not supported by Body V1)
func (b *BodyV1) SetStakingTransactions(newTransactions []*staking.StakingTransaction) {
	// not supported
}

// StakingTransactionAt returns the staking transaction at the given index in this block.
// It returns nil if index is out of bounds. (not supported by Body V1)
func (b *BodyV1) StakingTransactionAt(index int) *staking.StakingTransaction {
	// not supported
	return nil
}

// Uncles returns a deep copy of the list of uncle headers of this block.
func (b *BodyV1) Uncles() (uncles []*block.Header) {
	for _, uncle := range b.f.Uncles {
		uncles = append(uncles, CopyHeader(uncle))
	}
	return uncles
}

// SetUncles sets the list of uncle headers with a deep copy of the given list.
func (b *BodyV1) SetUncles(newUncle []*block.Header) {
	var uncles []*block.Header
	for _, uncle := range newUncle {
		uncles = append(uncles, CopyHeader(uncle))
	}
	b.f.Uncles = uncles
}

// SSCTransactions returns the list of SSC internal transactions as a 2-D slice
// (first dim index = InternalTxType bucket, second dim = tx within that type, in row order).
func (b *BodyV1) SSCTransactions() (txs [][]*SSCInternalTx) {
	for _, row := range b.f.SSCTransactions {
		var rowCopy []*SSCInternalTx
		for _, tx := range row {
			rowCopy = append(rowCopy, tx.Copy())
		}
		txs = append(txs, rowCopy)
	}
	return txs
}

// SSCTransactionAt returns the SSC internal transaction at the given global index in this block,
// counting by type order (CRTx first, then SimTx, ...). It returns nil if index is out of bounds.
func (b *BodyV1) SSCTransactionAt(index int) *SSCInternalTx {
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

// SetSSCTransactions sets the list of SSC internal transactions (2-D:
// first dim index = InternalTxType bucket, second dim = tx within that type) with a deep copy
// of the given list. The stored slice is normalized to length InternalTxTypeCount() so callers
// can always index by InternalTxType safely; missing buckets become empty (nil) rows.
func (b *BodyV1) SetSSCTransactions(newSSCTransactions [][]*SSCInternalTx) {
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

// IncomingReceipts returns a deep copy of the list of incoming cross-shard
// transaction receipts of this block.
func (b *BodyV1) IncomingReceipts() (incomingReceipts CXReceiptsProofs) {
	return b.f.IncomingReceipts.Copy()
}

// SetIncomingReceipts sets the list of incoming cross-shard transaction
// receipts of this block with a dep copy of the given list.
func (b *BodyV1) SetIncomingReceipts(newIncomingReceipts CXReceiptsProofs) {
	b.f.IncomingReceipts = newIncomingReceipts.Copy()
}

// EncodeRLP RLP-encodes the block body into the given writer.
func (b *BodyV1) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, &b.f)
}

// DecodeRLP RLP-decodes a block body from the given RLP stream into the
// receiver.
func (b *BodyV1) DecodeRLP(s *rlp.Stream) error {
	return s.Decode(&b.f)
}
