package ssc

import (
	"context"
	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/eth/rpc"
	"github.com/harmony-one/harmony/ssc/api"
	"sync"
)

type Comm struct {
	clients map[common.Address]*rpc.Client

	rwLock sync.RWMutex
}

func NewComm() *Comm {
	return &Comm{
		clients: make(map[common.Address]*rpc.Client),
		rwLock:  sync.RWMutex{},
	}
}

func (c *Comm) Call(ctx context.Context, member *api.Member, method string, args ...interface{}) (interface{}, error) {
	client, err := c.getOrCreateClient(member)
	if err != nil {
		return nil, err
	}

	var ret interface{}

	err = client.CallContext(ctx, &ret, method, args...)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (c *Comm) Multicast(ctx context.Context, members []*api.Member, method string, args ...interface{}) error {
	var wg sync.WaitGroup
	wg.Add(len(members))

	for i, member := range members {
		go func(i int, member *api.Member) {
			defer wg.Done()

			_, err := c.Call(ctx, member, method, args...)
			if err != nil {
				return
			}

		}(i, member)
	}

	wg.Wait()

	return nil
}

func (c *Comm) getOrCreateClient(member *api.Member) (*rpc.Client, error) {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()

	if _, ok := c.clients[member.Address]; !ok {
		client, err := rpc.Dial(member.Endpoint)
		if err != nil {
			return nil, err
		}
		c.clients[member.Address] = client
	}
	return c.clients[member.Address], nil
}
