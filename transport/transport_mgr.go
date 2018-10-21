package raftgrp

import (
	"context"
	"sync"

	"github.com/EricYT/raftgrp/proto"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	grpc "google.golang.org/grpc"
)

type ClientConnManager interface {
	Start() error

	// WithClient checks out a client by specific address,
	// and executes a function with it, and check in it back to manager.
	WithClient(addr string, f func(ctx context.Context, c *Client) error) error

	Stop()
}

type clientConnManager struct {
	Logger *zap.Logger

	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	clients map[string]*Client

	stopc chan struct{}
}

func (c *clientConnManager) WithClient(addr string, f func(ctx context.Context, c *Client) error) error {
	c.mu.Lock()
	client, ok := c.clients[addr]
	if !ok {
		var err error
		client, err = newClient(addr)
		if err != nil {
			c.mu.Unlock()
			return errors.Wrapf(err, "[ClientConnManager] new client with addr %s error", addr)
		}
		c.clients[addr] = client
	}
	c.mu.Unlock()

	// FIXME: safe run or go attach ?
	return f(c.ctx, client)
}

type Client struct {
	addr string
	conn *grpc.ClientConn
	// stats
}

func newClient(addr string) (*Client, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return &Client{
		addr: addr,
		conn: conn,
	}
}

func (c *Client) RaftGroupService() proto.RaftGrouperClient {
	return proto.NewRaftGrouperClient(c.conn)
}

// TODO: snapshot file sync service

func (c *Client) close() {
	c.conn.Close()
}
