package ssc

import (
	"context"
	"github.com/harmony-one/harmony/eth/rpc"
	"github.com/harmony-one/harmony/ssc/api"
	"sync"
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

func (c *Comm) Call(ctx context.Context, ret interface{}, member *api.Member, method string, args ...interface{}) error {
	client, err := c.getOrCreateClient(member)
	if err != nil {
		return err
	}

	err = client.CallContext(ctx, &ret, method, args...)
	if err != nil {
		return err
	}

	return nil
}

func (c *Comm) Multicast(ctx context.Context, members []*api.Member, method string, args ...interface{}) error {
	var wg sync.WaitGroup
	wg.Add(len(members))

	for i, member := range members {
		go func(i int, member *api.Member) {
			defer wg.Done()

			err := c.Call(ctx, nil, member, method, args...)
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

	if _, ok := c.clients[member.Endpoint]; !ok {
		client, err := rpc.Dial(member.Endpoint)
		if err != nil {
			return nil, err
		}
		c.clients[member.Endpoint] = client
	}
	return c.clients[member.Endpoint], nil
}
