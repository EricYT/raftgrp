package raftgrp

import (
	"context"
	"sync"

	etransport "github.com/EricYT/raftgrp/transport"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

var (
	ErrRaftGroupManagerNotFound error = errors.New("[RaftGroupManager] group not found")
	ErrRaftGroupManagerExist    error = errors.New("[RaftGroupManager] group already exist")
)

type RaftGroupManager struct {
	Logger *zap.Logger

	mu     sync.Mutex
	groups map[uint64]*RaftGroup

	tm *etransport.TransportManager
}

func NewRaftGroupManager(lg *zap.Logger, addr string) *RaftGroupManager {
	if lg == nil {
		lg = zap.NewExample()
	}

	rm := &RaftGroupManager{
		Logger: lg,
		groups: make(map[uint64]*RaftGroup),
	}
	rm.tm = etransport.NewTransportManager(lg, addr, rm)

	return rm
}

func (rm *RaftGroupManager) Start() error {
	rm.tm.Start()
	return nil
}

func (rm *RaftGroupManager) Stop() {
}

// raft process dispatcher
func (rm *RaftGroupManager) Process(ctx context.Context, gid uint64, m *raftpb.Message) error {
	rm.mu.Lock()
	g, ok := rm.groups[gid]
	rm.mu.Unlock()
	if ok {
		return g.ProcessExtra(ctx, m)
	}
	return ErrRaftGroupManagerNotFound
}

func (rm *RaftGroupManager) CreateTransport() etransport.Transport {
	return rm.tm.CreateTransport(rm.Logger)
}

func (rm *RaftGroupManager) NewRaftGroup(lg *zap.Logger, gid, id uint64, peers []string, datadir string) (*RaftGroup, error) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if g, ok := rm.groups[gid]; ok {
		return g, ErrRaftGroupManagerExist
	}

	if lg == nil {
		lg = rm.Logger
	}

	grp, err := NewRaftGroup(GroupConfig{
		Logger:        lg,
		ID:            id,
		Peers:         ParsePeers(peers),
		DataDir:       datadir,
		TickMs:        1000,
		ElectionTicks: 10,
		PreVote:       false,
		SnapshotCount: 100000,
	})
	if err != nil {
		return nil, errors.Wrap(err, "[RaftGroupManager] new raft group error")
	}

	rm.groups[gid] = grp

	return grp, nil
}
