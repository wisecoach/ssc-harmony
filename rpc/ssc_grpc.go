package rpc

import (
	"context"

	"github.com/harmony-one/harmony/ssc/api"
	sscpb "github.com/harmony-one/harmony/ssc/api/proto"
	"google.golang.org/grpc"
)

// RegisterSSCGrpcServer registers the SSC gRPC services on the given gRPC server.
// This includes the shard/cross services plus the monitor service used for
// post-experiment resource-release analysis.
func RegisterSSCGrpcServer(grpcServer *grpc.Server, svc api.Service) {
	sscpb.RegisterSSCShardServiceServer(grpcServer, &sscGrpcShardService{
		internalService: svc,
	})
	sscpb.RegisterSSCCrossServiceServer(grpcServer, &sscGrpcCrossService{
		internalService: svc,
	})
	sscpb.RegisterSSCMonitorServiceServer(grpcServer, &sscGrpcMonitorService{
		internalService: svc,
	})
}

// ============================================================================
// sscGrpcMonitorService — implements SSCMonitorServiceServer
// ============================================================================

type sscGrpcMonitorService struct {
	sscpb.UnimplementedSSCMonitorServiceServer
	internalService api.Service
}

// GetModuleStatus 返回 SSC 各模块当前维护的数据量快照。
// 供实验结束后调用，分析各模块是否正确释放资源。
func (s *sscGrpcMonitorService) GetModuleStatus(
	ctx context.Context, req *sscpb.Empty,
) (*sscpb.ModuleStatus, error) {
	status := s.internalService.GetModuleStatus()
	return sscpb.ModuleStatusToProto(status), nil
}

// ============================================================================
// sscGrpcShardService — implements SSCShardServiceServer
// ============================================================================

type sscGrpcShardService struct {
	sscpb.UnimplementedSSCShardServiceServer
	internalService api.Service
}

func (s *sscGrpcShardService) StartSimulateCXTransaction(
	ctx context.Context, req *sscpb.CXTSimulationRequest,
) (*sscpb.CXTSimulationSSCResult, error) {
	internalReq := sscpb.CXTSimulationRequestFromProto(req)
	ret := s.internalService.StartSimulateCXTransaction(internalReq)
	return sscpb.CXTSimulationSSCResultToProto(ret), nil
}

func (s *sscGrpcShardService) HandleSimulateRequest(
	ctx context.Context, req *sscpb.CXTSimulationRequest,
) (*sscpb.CXTSimulationResult, error) {
	internalReq := sscpb.CXTSimulationRequestFromProto(req)
	ret := s.internalService.HandleSimulateRequest(ctx, internalReq)
	return sscpb.CXTSimulationResultToProto(ret), nil
}

func (s *sscGrpcShardService) RequestCallCXT(
	ctx context.Context, req *sscpb.CXTCallRequest,
) (*sscpb.CXTCallSSCResult, error) {
	internalReq := sscpb.CXTCallRequestFromProto(req)
	ret := s.internalService.RequestCallCXT(internalReq)
	return sscpb.CXTCallSSCResultToProto(ret), nil
}

func (s *sscGrpcShardService) HandleCXTCall(
	ctx context.Context, req *sscpb.CXTCallSSCRequest,
) (*sscpb.CXTCallResult, error) {
	internalReq := sscpb.CXTCallSSCRequestFromProto(req)
	ret := s.internalService.HandleCXTCall(internalReq)
	return sscpb.CXTCallResultToProto(ret), nil
}

func (s *sscGrpcShardService) SignSimulationCommit(
	ctx context.Context, req *sscpb.SimulationCommit,
) (*sscpb.Bytes, error) {
	internalReq := sscpb.SimulationCommitFromProto(req)
	ret := s.internalService.SignSimulationCommit(internalReq)
	return &sscpb.Bytes{Val: ret}, nil
}

func (s *sscGrpcShardService) SignCXTSimulation(
	ctx context.Context, req *sscpb.CXTSimulation,
) (*sscpb.Bytes, error) {
	internalReq := sscpb.CXTSimulationFromProto(req)
	ret := s.internalService.SignCXTSimulation(internalReq)
	return &sscpb.Bytes{Val: ret}, nil
}

func (s *sscGrpcShardService) SignDeadlockProbe(
	ctx context.Context, req *sscpb.DeadlockProbe,
) (*sscpb.Bytes, error) {
	internalReq := sscpb.DeadlockProbeFromProto(req)
	ret := s.internalService.SignDeadlockProbe(internalReq)
	return &sscpb.Bytes{Val: ret}, nil
}

func (s *sscGrpcShardService) HandleCommitVote(
	ctx context.Context, req *sscpb.CXTCommitVote,
) (*sscpb.Empty, error) {
	internalReq := sscpb.CXTCommitVoteFromProto(req)
	s.internalService.HandleCommitVote(internalReq)
	return &sscpb.Empty{}, nil
}

func (s *sscGrpcShardService) SLTest(
	ctx context.Context, req *sscpb.SLTestRequest,
) (*sscpb.SLTestResult, error) {
	internalReq := sscpb.SLTestRequestFromProto(req)
	ret := s.internalService.SLTest(internalReq)
	return sscpb.SLTestResultToProto(ret), nil
}

func (s *sscGrpcShardService) AddRetryTx(
	ctx context.Context, req *sscpb.RetryTx,
) (*sscpb.Empty, error) {
	internalReq := sscpb.RetryTxFromProto(req)
	s.internalService.AddRetryTx(internalReq)
	return &sscpb.Empty{}, nil
}

func (s *sscGrpcShardService) AddToPassivePool(
	ctx context.Context, req *sscpb.Hash,
) (*sscpb.Empty, error) {
	txHash := sscpb.HashFromProto(req)
	s.internalService.AddToPassivePool(txHash)
	return &sscpb.Empty{}, nil
}

func (s *sscGrpcShardService) RetryCommit(
	ctx context.Context, req *sscpb.Hash,
) (*sscpb.RetryCommitResp, error) {
	txHash := sscpb.HashFromProto(req)
	ret := s.internalService.RetryCommit(txHash)
	return sscpb.RetryCommitRespToProto(ret), nil
}

func (s *sscGrpcShardService) RetryCommitDAG(
	ctx context.Context, req *sscpb.Hash,
) (*sscpb.RetryCommitResp, error) {
	txHash := sscpb.HashFromProto(req)
	ret := s.internalService.RetryCommitDAG(txHash)
	return sscpb.RetryCommitRespToProto(ret), nil
}

func (s *sscGrpcShardService) ReserveLegCommit(
	ctx context.Context, req *sscpb.SimulationCommit,
) (*sscpb.RetryCommitResp, error) {
	internalReq := sscpb.SimulationCommitFromProto(req)
	ret := s.internalService.ReserveLegCommit(internalReq)
	return sscpb.RetryCommitRespToProto(ret), nil
}

func (s *sscGrpcShardService) RetryCancel(
	ctx context.Context, req *sscpb.Hash,
) (*sscpb.Empty, error) {
	txHash := sscpb.HashFromProto(req)
	s.internalService.RetryCancel(txHash)
	return &sscpb.Empty{}, nil
}

func (s *sscGrpcShardService) HandleNewEpoch(
	ctx context.Context, req *sscpb.HandleNewEpochRequest,
) (*sscpb.Empty, error) {
	newEpoch := sscpb.NewEpochFromProto(req.GetNewEpoch())
	err := s.internalService.HandleNewEpoch(newEpoch, req.GetBlockNum())
	return &sscpb.Empty{}, err
}

func (s *sscGrpcShardService) StoreSimDAGPatch(
	ctx context.Context, req *sscpb.StoreSimDAGPatchRequest,
) (*sscpb.Empty, error) {
	internalReq := sscpb.StoreSimDAGPatchRequestFromProto(req)
	s.internalService.StoreSimDAGPatch(internalReq)
	return &sscpb.Empty{}, nil
}

// ============================================================================
// sscGrpcCrossService — implements SSCCrossServiceServer
// ============================================================================

type sscGrpcCrossService struct {
	sscpb.UnimplementedSSCCrossServiceServer
	internalService api.Service
}

func (s *sscGrpcCrossService) HandleCXTSSCCall(
	ctx context.Context, req *sscpb.CXTCallSSCRequest,
) (*sscpb.CXTCallSSCResult, error) {
	internalReq := sscpb.CXTCallSSCRequestFromProto(req)
	ret := s.internalService.HandleCXTSSCCall(internalReq)
	return sscpb.CXTCallSSCResultToProto(ret), nil
}

func (s *sscGrpcCrossService) CommitSimulation(
	ctx context.Context, req *sscpb.SimulationCommit,
) (*sscpb.Empty, error) {
	internalReq := sscpb.SimulationCommitFromProto(req)
	s.internalService.CommitSimulation(internalReq)
	return &sscpb.Empty{}, nil
}

func (s *sscGrpcCrossService) HandleCXTCommitSSCVote(
	ctx context.Context, req *sscpb.CXTCommitSSCVote,
) (*sscpb.Empty, error) {
	internalReq := sscpb.CXTCommitSSCVoteFromProto(req)
	s.internalService.HandleCXTCommitSSCVote(internalReq)
	return &sscpb.Empty{}, nil
}

func (s *sscGrpcCrossService) HandleCXTCommitProof(
	ctx context.Context, req *sscpb.CXTCommitProof,
) (*sscpb.Empty, error) {
	internalReq := sscpb.CXTCommitProofFromProto(req)
	s.internalService.HandleCXTCommitProof(internalReq)
	return &sscpb.Empty{}, nil
}

func (s *sscGrpcCrossService) SignalReSimulation(
	ctx context.Context, req *sscpb.RetrySignals,
) (*sscpb.Empty, error) {
	internalReq := sscpb.RetrySignalsFromProto(req)
	s.internalService.SignalReSimulation(internalReq)
	return &sscpb.Empty{}, nil
}

func (s *sscGrpcCrossService) HandleRetrySignal(
	ctx context.Context, req *sscpb.RetrySignal,
) (*sscpb.Empty, error) {
	internalReq := sscpb.RetrySignalFromProto(req)
	s.internalService.HandleRetrySignal(internalReq)
	return &sscpb.Empty{}, nil
}

func (s *sscGrpcCrossService) DetectDeadlockProbe(
	ctx context.Context, req *sscpb.DeadlockProbe,
) (*sscpb.DeadlockProbeAck, error) {
	internalReq := sscpb.DeadlockProbeFromProto(req)
	ret := s.internalService.DetectDeadlockProbe(internalReq)
	return sscpb.DeadlockProbeAckToProto(ret), nil
}
