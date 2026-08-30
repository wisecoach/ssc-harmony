package ssc

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/hmy"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	sscpb "github.com/harmony-one/harmony/ssc/api/proto"
	"google.golang.org/protobuf/proto"
)

// SSCInternalTxBroadcaster 广播内部交易到网络的窄接口（DSN-48）。
// 由 node 实现，整体参考普通交易广播（node.tryBroadcast）。
type SSCInternalTxBroadcaster interface {
	BroadcastSSCInternalTx(tx *types.SSCInternalTx) error
}

// txSubmitter 是 SSC 内部交易的提交器（DSN-48）。
// 内部交易已不适配普通 types.Transaction（无 (sender,nonce)、无签名、无 gas），
// 因此所有 Submit* 一律构造 SSCInternalTx 提交到内部池并广播，不再进入普通交易池。
type txSubmitter struct {
	selfShard    uint32
	internalPool *SSCInternalPool
	broadcaster  SSCInternalTxBroadcaster
}

// NewTxSubmitter 创建内部交易提交器。
// 参数 simSigner/crSigner/nodeAPI/config 为历史签名保留（内部池路径不再使用）。
func NewTxSubmitter(selfShard uint32, simSigner, crSigner api.TxSigner, nodeAPI hmy.NodeAPI, config *api.Config, internalPool *SSCInternalPool, broadcaster SSCInternalTxBroadcaster) api.TxSubmitter {
	_ = simSigner
	_ = crSigner
	_ = nodeAPI
	_ = config
	return &txSubmitter{
		selfShard:    selfShard,
		internalPool: internalPool,
		broadcaster:  broadcaster,
	}
}

// submitInternal 构造 SSCInternalTx 提交到内部池并广播（DSN-48）。
func (t *txSubmitter) submitInternal(tt types.InternalTxType, shard uint32, payload []byte, origin common.Hash) error {
	// DSN-48 兜底：本节点只提交/广播本分片的内部交易，杜绝跨分片 SimTx 进自己池
	if shard != t.selfShard {
		utils.SSCLogger().Warn().
			Str("sscType", tt.String()).
			Uint32("txShard", shard).
			Uint32("selfShard", t.selfShard).
			Msg("[TxSubmitter] skipping internal tx for other shard")
		return nil
	}
	tx := &types.SSCInternalTx{Type: tt, Shard: shard, Payload: payload}
	utils.SSCLogger().Info().
		Str("sscType", tt.String()).
		Str("sscHash", tx.Hash().Hex()).
		Str("originTxHash", origin.Hex()).
		Msg("[TxSubmitter] submitting internal tx to internal pool")
	if t.internalPool != nil {
		if err := t.internalPool.Add(tx); err != nil {
			return err
		}
	}
	if t.broadcaster != nil {
		return t.broadcaster.BroadcastSSCInternalTx(tx)
	}
	return nil
}

// SubmitSimulationTx 提交 SimTx 到内部池。
// DSN-47 R9：区块/网络载荷用 protobuf 编码；签名仍走 CXTSimulation.Bytes()（独立规范字节）。
// payload = proto.Marshal(CXTSimulationToProto(simulation))，完整保留 Epochs/签名/BaseBLSSignedMessage。
func (t *txSubmitter) SubmitSimulationTx(simulation *api.CXTSimulation) error {
	payload, err := proto.Marshal(sscpb.CXTSimulationToProto(simulation))
	if err != nil {
		return err
	}
	return t.submitInternal(types.InternalTxTypeSimTx, simulation.ShardId, payload, simulation.TxHash)
}

// SubmitSimulationTxWithSigner 提交 SimTx 到内部池（signerType 仅历史兼容，内部池无 signer 概念）。
func (t *txSubmitter) SubmitSimulationTxWithSigner(simulation *api.CXTSimulation, signerType string) error {
	_ = signerType
	payload, err := proto.Marshal(sscpb.CXTSimulationToProto(simulation))
	if err != nil {
		return err
	}
	return t.submitInternal(types.InternalTxTypeSimTx, simulation.ShardId, payload, simulation.TxHash)
}

// SubmitCommitOrRollbackTx 提交 CRTx 到内部池。
func (t *txSubmitter) SubmitCommitOrRollbackTx(proof *api.CXTCommitProof) error {
	payload, err := proto.Marshal(sscpb.CXTCommitProofToProto(proof))
	if err != nil {
		return err
	}
	return t.submitInternal(types.InternalTxTypeCRTx, t.selfShard, payload, proof.TxHash)
}

// SubmitEmptyTx 提交空内部交易到内部池。
func (t *txSubmitter) SubmitEmptyTx() error {
	return t.submitInternal(types.InternalTxTypeEmpty, t.selfShard, []byte{}, common.Hash{})
}

// SubmitNewEpoch 提交 NewEpoch 内部交易到内部池。
func (t *txSubmitter) SubmitNewEpoch(newEpoch *api.NewEpoch) error {
	payload, err := proto.Marshal(sscpb.NewEpochToProto(newEpoch))
	if err != nil {
		return err
	}
	return t.submitInternal(types.InternalTxTypeNewEpoch, t.selfShard, payload, common.Hash{})
}

// SubmitUploadOpinions 提交 UploadOpinions 内部交易到内部池。
func (t *txSubmitter) SubmitUploadOpinions(uploadOpinions *api.SelfOpinions) error {
	payload, err := proto.Marshal(sscpb.SelfOpinionsToProto(uploadOpinions))
	if err != nil {
		return err
	}
	return t.submitInternal(types.InternalTxTypeUploadOpinions, t.selfShard, payload, common.Hash{})
}

// OnBlockCommitted 内部池清理由 sscService.BlockCommitted 负责，这里无需处理（no-op）。
func (t *txSubmitter) OnBlockCommitted(block *types.Block) {}
