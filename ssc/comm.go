package ssc

import (
	"context"
	"fmt"
	"sync"

	"github.com/harmony-one/harmony/eth/rpc"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
)

type Comm struct {
	clients map[string]*rpc.Client

	rwLock sync.RWMutex
}

func NewComm() *Comm {
	return &Comm{
		clients: make(map[string]*rpc.Client),
		rwLock:  sync.RWMutex{},
	}
}

func (c *Comm) CallToEndpoint(ctx context.Context, ret interface{}, endpoint string, method string, args ...interface{}) error {
	client, err := c.getOrCreateClient(endpoint)
	if err != nil {
		return err
	}

	err = client.CallContext(ctx, &ret, method, args...)
	if err != nil {
		return err
	}
	return nil
}

func (c *Comm) Call(ctx context.Context, ret interface{}, member *api.Member, method string, args ...interface{}) error {
	return c.CallToEndpoint(ctx, ret, member.Endpoint, method, args...)
}

func (c *Comm) Multicast(ctx context.Context, members []*api.Member, method string, args ...interface{}) error {
	var wg sync.WaitGroup
	wg.Add(len(members))

	select {
	case <-ctx.Done():
		utils.SSCLogger().Error().Err(ctx.Err()).
			Msg("FATAL: ctx already canceled BEFORE Multicast!")
		return fmt.Errorf("ctx canceled before call")
	default:
		utils.SSCLogger().Debug().Msg("ctx is alive before Multicast")
	}

	for i, member := range members {
		go func(i int, member *api.Member) {
			defer wg.Done()

			err := c.Call(ctx, nil, member, method, args...)
			if err != nil {
				utils.SSCLogger().Error().Err(err).Msg("Failed to call")
				return
			}

		}(i, member)
	}

	wg.Wait()

	return nil
}

func (c *Comm) getOrCreateClient(endpoint string) (*rpc.Client, error) {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()

	if _, ok := c.clients[endpoint]; !ok {
		client, err := rpc.Dial(endpoint)
		if err != nil {
			return nil, err
		}
		c.clients[endpoint] = client
	}
	return c.clients[endpoint], nil
}
