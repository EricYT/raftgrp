package transport

import (
	"context"
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

	ctx    context.Context
	cancel context.CancelFunc

	mu         sync.RWMutex
	transports map[uint64]Transport

	// for client view
	ccm *clientConnManager
	// for server view
	sm *serverManager
}

func NewTransportManager(lg *zap.Logger, addr string, hnd Handler) *TransportManager {
	ccm := &clientConnManager{
		Logger:  lg,
		clients: make(map[string]*Client),
		stopc:   make(chan struct{}),
	}

	sm := &serverManager{
		Logger: lg,
		Addr:   addr,
		rs: &raftServer{
			Logger: lg,
			hnd:    hnd,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())

	ccm.ctx = ctx

	tm := &TransportManager{
		Logger:     lg,
		ctx:        ctx,
		cancel:     cancel,
		ccm:        ccm,
		sm:         sm,
		transports: make(map[uint64]Transport),
	}

	sm.rs.tm = tm

	return tm
}

func (tm *TransportManager) CreateTransport(lg *zap.Logger, gid uint64) Transport {
	if lg == nil {
		lg = tm.Logger
	}
	t := &transportV1{
		Logger: lg,
		mgr:    tm.ccm,
		gid:    gid,
		peers:  make(map[types.ID]peer),
	}
	tm.mu.Lock()
	tm.transports[gid] = t
	tm.mu.Unlock()
	return t
}

func (tm *TransportManager) GetTransport(gid uint64) (t Transport, ok bool) {
	tm.mu.RLock()
	t, ok = tm.transports[gid]
	tm.mu.RUnlock()
	return t, ok
}

func (tm *TransportManager) Start() error {
	// FIXME: How to handle the transport manager crash?
	go tm.sm.Start()
	return nil
}

func (tm *TransportManager) Stop() {
	tm.sm.Stop()
	tm.ccm.Stop()
}
