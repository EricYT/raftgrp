package transport

import (
	"sync"

	"github.com/pkg/errors"
	"go.etcd.io/etcd/pkg/types"
	"go.uber.org/zap"
)

var (
	ErrTransportNotFound error = errors.New("[TransportManager] transport not found")
)

type TransportManager struct {
	Logger *zap.Logger

	mu         sync.RWMutex
	transports map[uint64]Transport

	c *clientManager
	t *transportServer
}

func NewTransportManager(lg *zap.Logger, addr string) *TransportManager {
	tm := &TransportManager{
		Logger:     lg,
		transports: make(map[uint64]Transport),
	}

	tm.t = newTransportServer(lg, tm, addr)
	tm.c = newClientManager(lg)

	return tm
}

func (tm *TransportManager) CreateTransport(lg *zap.Logger, gid uint64, localID types.ID, r Raft) Transport {
	tm.Logger.Info("[TransportManager] create transport",
		zap.Uint64("group-id", gid),
		zap.String("local-id", localID.String()),
	)

	tm.mu.Lock()
	defer tm.mu.Unlock()
	t, ok := tm.transports[gid]
	if ok {
		return t
	}
	if lg == nil {
		lg = tm.Logger
	}
	t = newTransportV1(lg, gid, localID, tm.c, r)
	tm.transports[gid] = t
	return t
}

func (tm *TransportManager) RemoveTransport(gid uint64) (err error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	t, ok := tm.transports[gid]
	if !ok {
		return nil
	}
	t.Close()
	delete(tm.transports, gid)
	return nil
}

func (tm *TransportManager) GetTransport(gid uint64) (t Transport, ok bool) {
	tm.mu.RLock()
	t, ok = tm.transports[gid]
	tm.mu.RUnlock()
	return t, ok
}

func (tm *TransportManager) Close() {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	for _, t := range tm.transports {
		t.Close()
	}
	tm.transports = make(map[uint64]Transport)
	tm.t.close()
	tm.c.close()
}
