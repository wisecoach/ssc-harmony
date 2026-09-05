package ssc

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	sscpb "github.com/harmony-one/harmony/ssc/api/proto"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Comm provides gRPC-based communication to remote SSC endpoints.
// It replaces the old JSON-RPC over HTTP implementation.
// The public API (Call, CallToEndpoint, Multicast) is unchanged.
// All args are expected to be api.* types; comm.go does the api→proto conversion internally.
type Comm struct {
	clients map[string]*grpc.ClientConn
	rwLock  sync.RWMutex
}

func NewComm() *Comm {
	return &Comm{
		clients: make(map[string]*grpc.ClientConn),
		rwLock:  sync.RWMutex{},
	}
}

// CallToEndpoint dispatches a gRPC call to a raw endpoint string.
func (c *Comm) CallToEndpoint(ctx context.Context, ret interface{}, endpoint string, method string, args ...interface{}) error {
	client, err := c.getOrCreateClient(endpoint)
	if err != nil {
		return err
	}
	return c.callOnConn(ctx, ret, client, endpoint, method, args...)
}

// Call dispatches a gRPC call to a member identified by its SSCEndpoint.
// SSCEndpoint must be set; empty means the member config lacks the gRPC endpoint.
func (c *Comm) Call(ctx context.Context, ret interface{}, member *api.Member, method string, args ...interface{}) error {
	endpoint := member.SSCEndpoint
	if endpoint == "" {
		utils.SSCLogger().Error().
			Str("method", method).
			Str("member_addr", member.Address.Hex()).
			Str("http_endpoint", member.Endpoint).
			Msg("Comm: SSCEndpoint is empty, cannot dial gRPC")
		return fmt.Errorf("comm: SSCEndpoint empty for member %s", member.Address.Hex())
	}
	return c.CallToEndpoint(ctx, ret, endpoint, method, args...)
}

// Multicast sends a fire-and-forget gRPC call to all members in parallel.
func (c *Comm) Multicast(ctx context.Context, members []*api.Member, method string, args ...interface{}) error {
	var wg sync.WaitGroup
	wg.Add(len(members))

	select {
	case <-ctx.Done():
		return nil

	default:
	}

	for _, member := range members {
		go func(member *api.Member) {
			defer wg.Done()
			err := c.Call(ctx, nil, member, method, args...)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					utils.SSCLogger().Error().Err(err).
						Str("endpoint", member.Endpoint).
						Str("address", member.Address.Hex()).
						Str("method", method).
						Msg("Multicast: Failed to call member")
				}
			}
		}(member)
	}

	wg.Wait()
	return nil
}

// callOnConn dispatches to the appropriate gRPC method.
// args[0] is always the api type; it is converted to proto before sending.
// ret is a pointer to the api type result; it is filled from the proto response.
func (c *Comm) callOnConn(ctx context.Context, ret interface{}, conn *grpc.ClientConn, endpoint string, method string, args ...interface{}) error {
	methodName := strings.TrimPrefix(method, "ssc_")

	switch methodName {
	// --- SSCShardService ---
	case "startSimulateCXTransaction":
		a := args[0].(*api.CXTSimulationRequest)
		p := sscpb.CXTSimulationRequestToProto(a)
		resp, err := sscpb.NewSSCShardServiceClient(conn).StartSimulateCXTransaction(ctx, p)
		if err != nil {
			return err
		}
		if ret != nil {
			*(ret.(*api.CXTSimulationSSCResult)) = *sscpb.CXTSimulationSSCResultFromProto(resp)
		}
		return nil

	case "handleSimulateRequest":
		a := args[0].(*api.CXTSimulationRequest)
		p := sscpb.CXTSimulationRequestToProto(a)
		resp, err := sscpb.NewSSCShardServiceClient(conn).HandleSimulateRequest(ctx, p)
		if err != nil {
			return err
		}
		if ret != nil {
			*(ret.(*api.CXTSimulationResult)) = *sscpb.CXTSimulationResultFromProto(resp)
		}
		return nil

	case "requestCallCXT":
		a := args[0].(*api.CXTCallRequest)
		p := sscpb.CXTCallRequestToProto(a)
		resp, err := sscpb.NewSSCShardServiceClient(conn).RequestCallCXT(ctx, p)
		if err != nil {
			return err
		}
		if ret != nil {
			*(ret.(*api.CXTCallSSCResult)) = *sscpb.CXTCallSSCResultFromProto(resp)
		}
		return nil

	case "handleCXTCall":
		a := args[0].(*api.CXTCallSSCRequest)
		p := sscpb.CXTCallSSCRequestToProto(a)
		resp, err := sscpb.NewSSCShardServiceClient(conn).HandleCXTCall(ctx, p)
		if err != nil {
			return err
		}
		if ret != nil {
			*(ret.(*api.CXTCallResult)) = *sscpb.CXTCallResultFromProto(resp)
		}
		return nil

	case "signSimulationCommit":
		a := args[0].(*api.SimulationCommit)
		p := sscpb.SimulationCommitToProto(a)
		resp, err := sscpb.NewSSCShardServiceClient(conn).SignSimulationCommit(ctx, p)
		if err != nil {
			return err
		}
		if ret != nil {
			*(ret.(*[]byte)) = resp.GetVal()
		}
		return nil

	case "signCXTSimulation":
		a := args[0].(*api.CXTSimulation)
		p := sscpb.CXTSimulationToProto(a)
		resp, err := sscpb.NewSSCShardServiceClient(conn).SignCXTSimulation(ctx, p)
		if err != nil {
			return err
		}
		if ret != nil {
			*(ret.(*[]byte)) = resp.GetVal()
		}
		return nil

	case "signDeadlockProbe":
		a := args[0].(*api.DeadlockProbe)
		p := sscpb.DeadlockProbeToProto(a)
		resp, err := sscpb.NewSSCShardServiceClient(conn).SignDeadlockProbe(ctx, p)
		if err != nil {
			return err
		}
		if ret != nil {
			*(ret.(*[]byte)) = resp.GetVal()
		}
		return nil
	case "handleCommitVote":
		a := args[0].(*api.CXTCommitVote)
		p := sscpb.CXTCommitVoteToProto(a)
		_, err := sscpb.NewSSCShardServiceClient(conn).HandleCommitVote(ctx, p)
		return err

	case "sLTest":
		a := args[0].(*api.SLTestRequest)
		p := sscpb.SLTestRequestToProto(a)
		resp, err := sscpb.NewSSCShardServiceClient(conn).SLTest(ctx, p)
		if err != nil {
			return err
		}
		if ret != nil {
			*(ret.(*api.SLTestResult)) = *sscpb.SLTestResultFromProto(resp)
		}
		return nil

	case "addRetryTx":
		a := args[0].(*api.RetryTx)
		p := sscpb.RetryTxToProto(a)
		_, err := sscpb.NewSSCShardServiceClient(conn).AddRetryTx(ctx, p)
		return err

	case "addToPassivePool":
		txHash := args[0].(common.Hash)
		_, err := sscpb.NewSSCShardServiceClient(conn).AddToPassivePool(ctx, &sscpb.Hash{Val: txHash.Bytes()})
		return err

	case "storeSimDAGPatch":
		a := args[0].(*api.StoreSimDAGPatchRequest)
		p := sscpb.StoreSimDAGPatchRequestToProto(a)
		_, err := sscpb.NewSSCShardServiceClient(conn).StoreSimDAGPatch(ctx, p)
		return err

	case "retryCommit":
		txHash := args[0].(common.Hash)
		resp, err := sscpb.NewSSCShardServiceClient(conn).RetryCommit(ctx, &sscpb.Hash{Val: txHash.Bytes()})
		if err != nil {
			return err
		}
		if ret != nil {
			*(ret.(*api.RetryCommitResp)) = *sscpb.RetryCommitRespFromProto(resp)
		}
		return nil

	case "retryCommitDAG":
		txHash := args[0].(common.Hash)
		resp, err := sscpb.NewSSCShardServiceClient(conn).RetryCommitDAG(ctx, &sscpb.Hash{Val: txHash.Bytes()})
		if err != nil {
			return err
		}
		if ret != nil {
			*(ret.(*api.RetryCommitResp)) = *sscpb.RetryCommitRespFromProto(resp)
		}
		return nil

	case "retryCancel":
		txHash := args[0].(common.Hash)
		_, err := sscpb.NewSSCShardServiceClient(conn).RetryCancel(ctx, &sscpb.Hash{Val: txHash.Bytes()})
		return err

	case "handleNewEpoch":
		a := args[0].(*api.NewEpoch)
		blockNum := args[1].(uint64)
		p := &sscpb.HandleNewEpochRequest{
			NewEpoch: sscpb.NewEpochToProto(a),
			BlockNum: blockNum,
		}
		_, err := sscpb.NewSSCShardServiceClient(conn).HandleNewEpoch(ctx, p)
		return err

	// --- SSCCrossService ---
	case "handleCXTSSCCall":
		a := args[0].(*api.CXTCallSSCRequest)
		p := sscpb.CXTCallSSCRequestToProto(a)
		resp, err := sscpb.NewSSCCrossServiceClient(conn).HandleCXTSSCCall(ctx, p)
		if err != nil {
			return err
		}
		if ret != nil {
			*(ret.(*api.CXTCallSSCResult)) = *sscpb.CXTCallSSCResultFromProto(resp)
		}
		return nil

	case "commitSimulation":
		a := args[0].(*api.SimulationCommit)
		p := sscpb.SimulationCommitToProto(a)
		_, err := sscpb.NewSSCCrossServiceClient(conn).CommitSimulation(ctx, p)
		return err

	case "handleCXTCommitSSCVote":
		a := args[0].(*api.CXTCommitSSCVote)
		p := sscpb.CXTCommitSSCVoteToProto(a)
		_, err := sscpb.NewSSCCrossServiceClient(conn).HandleCXTCommitSSCVote(ctx, p)
		return err

	case "handleCXTCommitProof":
		a := args[0].(*api.CXTCommitProof)
		p := sscpb.CXTCommitProofToProto(a)
		_, err := sscpb.NewSSCCrossServiceClient(conn).HandleCXTCommitProof(ctx, p)
		return err

	case "signalReSimulation":
		a := args[0].(*api.RetrySignals)
		p := sscpb.RetrySignalsToProto(a)
		_, err := sscpb.NewSSCCrossServiceClient(conn).SignalReSimulation(ctx, p)
		return err

	case "handleRetrySignal":
		a := args[0].(*api.RetrySignal)
		p := sscpb.RetrySignalToProto(a)
		_, err := sscpb.NewSSCCrossServiceClient(conn).HandleRetrySignal(ctx, p)
		return err

	case "detectDeadlockProbe":
		a := args[0].(*api.DeadlockProbe)
		p := sscpb.DeadlockProbeToProto(a)
		resp, err := sscpb.NewSSCCrossServiceClient(conn).DetectDeadlockProbe(ctx, p)
		if err != nil {
			return err
		}
		if ret != nil {
			*(ret.(*api.DeadlockProbeAck)) = *sscpb.DeadlockProbeAckFromProto(resp)
		}
		return nil
	default:
		return fmt.Errorf("comm: unknown method %q", method)
	}
}

// getOrCreateClient returns a cached or new gRPC connection to an endpoint.
// The endpoint format is "host:HTTPPort". The gRPC port is HTTPPort - 1000.
func (c *Comm) getOrCreateClient(endpoint string) (*grpc.ClientConn, error) {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()

	if conn, ok := c.clients[endpoint]; ok {
		return conn, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		utils.SSCLogger().Error().Err(err).
			Str("grpc_addr", endpoint).
			Msg("Comm: failed to dial gRPC")
		return nil, err
	}

	c.clients[endpoint] = conn
	return conn, nil
}
