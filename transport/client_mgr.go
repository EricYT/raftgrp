package transport

import (
	"sync"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	grpc "google.golang.org/grpc"
)

type clientManager struct {
	Logger *zap.Logger

	mu      sync.Mutex
	clients map[string]*Client
}

func newClientManager(lg *zap.Logger) *clientManager {
	return &clientManager{
		Logger:  lg,
		clients: make(map[string]*Client),
	}
}

func (cm *clientManager) pick(addr string) (c *Client, err error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	c, ok := cm.clients[addr]
	if ok {
		return c, nil
	}
	c, err = newClient(cm.Logger, addr)
	if err != nil {
		cm.Logger.Error("[clientManager] failed to new client",
			zap.String("addr", addr),
			zap.Error(err),
		)
		return nil, errors.Wrapf(err, "[clientManager] faield to new client(%s)", addr)
	}
	cm.clients[addr] = c
	return c, nil
}

func (cm *clientManager) close() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	for _, c := range cm.clients {
		c.stop()
	}
	cm.clients = make(map[string]*Client)
}

// Client is a thread-safe instance for messages sending,
// shared by a lot raft groups.
type Client struct {
	lg   *zap.Logger
	addr string

	writer *streamWriter

	// TODO:stats
}

func newClient(lg *zap.Logger, addr string) (*Client, error) {
	writer, err := startStreamWriter(lg, addr)
	if err != nil {
		lg.Error("[client] failed to start stream writer",
			zap.String("address", addr),
			zap.Error(err),
		)
		return nil, err
	}
	return &Client{
		lg:     lg,
		addr:   addr,
		writer: writer,
	}, nil
}

func (c *Client) writec() chan<- msgWrapper {
	return c.writer.writec()
}

func (c *Client) stop() {
	c.writer.stop()
}

// Raw client for snapshot
func (c *Client) rawRaftGrouperConn() (*grpc.ClientConn, error) {
	// Dial with special arguments
	conn, err := grpc.Dial(c.addr,
		grpc.WithInsecure(),
		// Buf size
	)
	if err != nil {
		return nil, err
	}
	return conn, err
}
