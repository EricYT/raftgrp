package transport

import (
	"context"
	"sync"

	"go.etcd.io/etcd/pkg/types"
	"go.uber.org/zap"
)

type TransportManager struct {
	Logger *zap.Logger

	ctx    context.Context
	cancel context.CancelFunc

	mu sync.Mutex

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

	return &TransportManager{
		Logger: lg,
		ctx:    ctx,
		cancel: cancel,
		ccm:    ccm,
		sm:     sm,
	}
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
	return t
}

func (tm *TransportManager) Start() error {
	go tm.sm.Start()
	return nil
}

func (tm *TransportManager) Stop() {
	tm.sm.Stop()
	tm.ccm.Stop()
}
