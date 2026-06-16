package rpc

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	rpc2 "github.com/harmony-one/harmony/eth/rpc"
	"github.com/harmony-one/harmony/ssc/api"
	"golang.org/x/time/rate"
)

var lock = sync.Mutex{}
var requests = make([]interface{}, 0)

func NewPublicSSCShardAPI(
	internalService api.ShardService,
	version Version,
	limiterEnable bool,
	limit int) rpc2.API {
	var limiter *rate.Limiter
	if limiterEnable {
		limiter = rate.NewLimiter(rate.Limit(limit), limit)
	}

	return rpc2.API{
		Namespace: version.Namespace(),
		Version:   APIVersion,
		Service: &PublicSSCShardService{
			count:           &atomic.Int32{},
			internalService: internalService,
			version:         version,
			limiterCall:     limiter,
		},
		Public: true,
	}
}

func NewPublicSSCCrossAPI(
	internalService api.CrossService,
	version Version,
	limiterEnable bool,
	limit int) rpc2.API {
	var limiter *rate.Limiter
	if limiterEnable {
		limiter = rate.NewLimiter(rate.Limit(limit), limit)
	}

	return rpc2.API{
		Namespace: version.Namespace(),
		Version:   APIVersion,
		Service: &PublicSSCCrossService{
			count:           &atomic.Int32{},
			internalService: internalService,
			version:         version,
			limiterCall:     limiter,
		},
		Public: true,
	}
}

type PublicSSCCrossService struct {
	count           *atomic.Int32
	internalService api.CrossService
	version         Version
	limiterCall     *rate.Limiter
}

func (s *PublicSSCCrossService) HandleCXTSSCCall(
	ctx context.Context,
	req *api.CXTCallSSCRequest,
) (*api.CXTCallSSCResult, error) {
	ret := s.internalService.HandleCXTSSCCall(req)
	return ret, nil
}

func (s *PublicSSCCrossService) CommitSimulation(
	ctx context.Context,
	commit *api.SimulationCommit,
) error {
	s.internalService.CommitSimulation(commit)
	return nil
}

func (s *PublicSSCCrossService) HandleCXTCommitSSCVote(
	ctx context.Context,
	vote *api.CXTCommitSSCVote,
) error {
	s.internalService.HandleCXTCommitSSCVote(vote)
	return nil
}

func (s *PublicSSCCrossService) HandleCXTCommitProof(
	ctx context.Context,
	proof *api.CXTCommitProof,
) error {
	s.internalService.HandleCXTCommitProof(proof)
	return nil
}

func (s *PublicSSCCrossService) SignalReSimulation(
	ctx context.Context,
	signals *api.ReSimulationSignals) error {
	s.internalService.SignalReSimulation(signals)
	return nil
}

func (s *PublicSSCCrossService) HandleChainSimSignal(
	ctx context.Context,
	signal *api.ReSimulationSignal) error {
	s.internalService.HandleChainSimSignal(signal)
	return nil
}

type PublicSSCShardService struct {
	count           *atomic.Int32
	internalService api.ShardService
	version         Version
	limiterCall     *rate.Limiter
}

func (s *PublicSSCShardService) StartSimulateCXTransaction(
	ctx context.Context,
	req *api.CXTSimulationRequest,
) (*api.CXTSimulationSSCResult, error) {
	ret := s.internalService.StartSimulateCXTransaction(req)
	return ret, nil
}

func (s *PublicSSCShardService) HandleSimulateRequest(
	ctx context.Context,
	req *api.CXTSimulationRequest,
) (*api.CXTSimulationResult, error) {
	ret := s.internalService.HandleSimulateRequest(ctx, req)
	return ret, nil
}
func (s *PublicSSCShardService) RequestCallCXT(
	ctx context.Context,
	req *api.CXTCallRequest,
) (*api.CXTCallSSCResult, error) {
	ret := s.internalService.RequestCallCXT(req)
	return ret, nil
}

func (s *PublicSSCShardService) HandleCXTCall(
	ctx context.Context,
	req *api.CXTCallSSCRequest,
) (*api.CXTCallResult, error) {
	ret := s.internalService.HandleCXTCall(req)
	return ret, nil
}

func (s *PublicSSCShardService) SignSimulationCommit(
	ctx context.Context,
	req *api.SimulationCommit,
) ([]byte, error) {
	ret := s.internalService.SignSimulationCommit(req)
	return ret, nil
}

func (s *PublicSSCShardService) SignCXTSimulation(
	ctx context.Context,
	req *api.CXTSimulation,
) ([]byte, error) {
	ret := s.internalService.SignCXTSimulation(req)
	return ret, nil
}

func (s *PublicSSCShardService) HandleCommitVote(
	ctx context.Context,
	vote *api.CXTCommitVote,
) ([]byte, error) {
	s.internalService.HandleCommitVote(vote)
	return nil, nil
}

func (s *PublicSSCShardService) SLTest(ctx context.Context, req *api.SLTestRequest) (*api.SLTestResult, error) {
	return s.internalService.SLTest(req), nil
}

func (s *PublicSSCShardService) AddRetryTx(ctx context.Context, tx *api.RetryTx) error {
	s.internalService.AddRetryTx(tx)
	return nil
}
func (s *PublicSSCShardService) RetryCommit(ctx context.Context, txHash common.Hash) (*api.RetryCommitResp, error) {
	return s.internalService.RetryCommit(txHash), nil
}

func (s *PublicSSCShardService) RetryCancel(ctx context.Context, txHash common.Hash) error {
	s.internalService.RetryCancel(txHash)
	return nil
}

func (s *PublicSSCShardService) HandleNewEpoch(ctx context.Context, newEpoch *api.NewEpoch, blockNum uint64) error {
	return s.internalService.HandleNewEpoch(newEpoch, blockNum)
}
