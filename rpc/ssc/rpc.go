package ssc

import (
	"context"
	rpc2 "github.com/harmony-one/harmony/eth/rpc"
	"github.com/harmony-one/harmony/rpc"
	"github.com/harmony-one/harmony/ssc/api"
	"golang.org/x/time/rate"
)

type PublicSSCService struct {
	internalService api.Service
	version         rpc.Version
	limiterCall     *rate.Limiter
}

func NewPublicSSCAPI(
	internalService api.Service,
	version rpc.Version,
	limiterEnable bool,
	limit int) rpc2.API {
	var limiter *rate.Limiter
	if limiterEnable {
		limiter = rate.NewLimiter(rate.Limit(limit), limit)
	}

	return rpc2.API{
		Namespace: version.Namespace(),
		Version:   rpc.APIVersion,
		Service: &PublicSSCService{
			internalService: internalService,
			version:         version,
			limiterCall:     limiter,
		},
		Public: true,
	}
}

func (s *PublicSSCService) SimulateCXTransaction(
	ctx context.Context,
	req *api.CXTSimulationRequest,
) (*api.CXTSimulationSSCResult, error) {
	result := s.internalService.SimulateCXTransaction(ctx, req)
	return result, result.Err
}
