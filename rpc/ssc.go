package rpc

import (
	"context"
	rpc2 "github.com/harmony-one/harmony/eth/rpc"
	"github.com/harmony-one/harmony/ssc/api"
	"golang.org/x/time/rate"
	"sync"
	"sync/atomic"
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
	ret := s.internalService.HandleSimulateRequest(req)
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
