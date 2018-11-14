package transport

import (
	"context"
	"sync"

	"github.com/EricYT/raftgrp/proto"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	grpc "google.golang.org/grpc"
)

type ClientConnManager interface {
	// WithClient checks out a client by specific address,
	// and executes a function with it, and check in it back to manager.
	WithClient(addr string, f func(ctx context.Context, c *Client) error) error
}

type clientConnManager struct {
	Logger *zap.Logger

	ctx context.Context

	mu      sync.Mutex
	clients map[string]*Client

	// FIXME: channel to dispatch jobs ?

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
	err := f(c.ctx, client)
	if err != nil {
		// FIXME: remove the connection
		client.close()
		c.mu.Lock()
		delete(c.clients, addr)
		c.mu.Unlock()
	}
	return err
}

func (c *clientConnManager) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, client := range c.clients {
		client.close()
	}
	c.clients = nil
}

type Client struct {
	addr string
	conn *grpc.ClientConn
	// stats
}

func newClient(addr string) (*Client, error) {
	conn, err := grpc.Dial(addr,
		grpc.WithInsecure(),
		//grpc.WithBlock(), // The default connection will create non-blocking
		//grpc.WithTimeout(time.Second*1),
		// FIXME: writeBufferSize ?
	)
	if err != nil {
		return nil, err
	}
	return &Client{
		addr: addr,
		conn: conn,
	}, nil
}

func (c *Client) RaftGrouperClient() proto.RaftGrouperClient {
	return proto.NewRaftGrouperClient(c.conn)
}

// TODO: snapshot file sync service

func (c *Client) close() {
	c.conn.Close()
}
