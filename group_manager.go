package raftgrp

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"sync"

	etransport "github.com/EricYT/raftgrp/transport"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
)

var (
	ErrRaftGroupManagerNotFound error = errors.New("[RaftGroupManager] group not found")
	ErrRaftGroupManagerExist    error = errors.New("[RaftGroupManager] group already exist")
	ErrRaftGroupManagerLaunched error = errors.New("[RaftGroupManager] group already launched")
)

const (
	defaultTickMs                 int    = 1000
	defaultElectionTicks          int    = 10
	defaultSnapshotCount          uint64 = 10
	defaultSnapshotCatchUpEntries uint64 = 5
)

type RaftGroupManager struct {
	Logger *zap.Logger

	mu     sync.RWMutex
	logdir string
	// gRPC address for all raft groups
	addr   string
	groups map[uint64]*RaftGroup

	tm *etransport.TransportManager
}

func NewRaftGroupManager(lg *zap.Logger, logdir, addr string) *RaftGroupManager {
	if lg == nil {
		lg = zap.NewExample()
	}

	// initialize raft group log directory
	if terr := fileutil.TouchDirAll(logdir); terr != nil {
		lg.Fatal("[RaftGroupManager] create raft group log directory error",
			zap.String("directory", logdir),
			zap.Error(terr),
		)
	}

	rm := &RaftGroupManager{
		Logger: lg,
		logdir: logdir,
		addr:   addr,
		groups: make(map[uint64]*RaftGroup),
	}
	rm.tm = etransport.NewTransportManager(lg, addr, rm)

	return rm
}

func (rm *RaftGroupManager) Start() error {
	// FIXME: If we try to reload all raft groups from here.
	// It's a heavy operation maybe.

	rm.tm.Start()
	log.Println("[RaftGroupManager] start 4")
	return nil
}

func (rm *RaftGroupManager) Stop() {
	rm.tm.Stop()
}

// raft process dispatcher
func (rm *RaftGroupManager) Process(ctx context.Context, gid uint64, m *raftpb.Message) error {
	// FIXME: this operation will be bothered by raft group create and restart.
	rm.mu.RLock()
	g, ok := rm.groups[gid]
	rm.mu.RUnlock()
	if ok {
		return g.Process(ctx, m)
	}
	return ErrRaftGroupManagerNotFound
}

// snapshot methods
func (rm *RaftGroupManager) UnmarshalSnapshotReader(ctx context.Context, gid uint64, p []byte) (etransport.SnapshotReader, error) {
	rm.mu.RLock()
	g, ok := rm.groups[gid]
	rm.mu.RUnlock()
	if !ok {
		return nil, ErrRaftGroupManagerNotFound
	}
	return g.sm.UnmarshalSnapshotReader(p)
}

func (rm *RaftGroupManager) UnmarshalSnapshotParter(ctx context.Context, gid uint64, p []byte) (etransport.SnapshotParter, error) {
	rm.mu.RLock()
	g, ok := rm.groups[gid]
	rm.mu.RUnlock()
	if !ok {
		return nil, ErrRaftGroupManagerNotFound
	}
	return g.sm.UnmarshalSnapshotParter(p)
}

func (rm *RaftGroupManager) NewRaftGroup(lg *zap.Logger, gid, id uint64, peers []string, newCluster bool) (*RaftGroup, error) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if g, ok := rm.groups[gid]; ok {
		return g, ErrRaftGroupManagerExist
	}

	gdir := logDirByGId(rm.logdir, gid)
	if fileutil.Exist(gdir) {
		return rm.restartRaftGroup(lg, gid, id)
	}

	return rm.newRaftGroup(lg, gid, id, peers, newCluster)
}

func (rm *RaftGroupManager) newRaftGroup(lg *zap.Logger, gid, id uint64, peers []string, newCluster bool) (*RaftGroup, error) {
	if lg == nil {
		lg = rm.Logger
	}

	// initialize raft group log parent directory
	gdir := logDirByGId(rm.logdir, gid)
	if terr := fileutil.CreateDirAll(gdir); terr != nil {
		lg.Error("[RaftGroupManager] create raft group parent directory failed",
			zap.Uint64("gid", gid),
			zap.Uint64("id", id),
			zap.String("raft-group-parent-directory", gdir),
			zap.Error(terr),
		)
		return nil, errors.Wrap(terr, "[RaftGroupManager] create raft group parent directory error")
	}

	trans := rm.tm.CreateTransport(lg, gid)
	grp, err := NewRaftGroup(GroupConfig{
		Logger:                 lg,
		ID:                     id,
		GID:                    gid,
		Peers:                  ParsePeers(peers),
		NewCluster:             newCluster,
		DataDir:                gdir,
		TickMs:                 defaultTickMs,
		ElectionTicks:          defaultElectionTicks,
		PreVote:                false,
		SnapshotCount:          defaultSnapshotCount,
		SnapshotCatchUpEntries: defaultSnapshotCatchUpEntries,
	}, trans)
	if err != nil {
		lg.Error("[RaftGroupManager] create raft group failed",
			zap.Uint64("gid", gid),
			zap.Uint64("id", id),
			zap.Error(err),
		)
		return nil, errors.Wrap(err, "[RaftGroupManager] new raft group error")
	}
	lg.Info("[RaftGroupManager] create raft group success",
		zap.Uint64("group-id", gid),
		zap.Uint64("id", id),
	)
	rm.groups[gid] = grp
	return grp, nil
}

func (rm *RaftGroupManager) restartRaftGroup(lg *zap.Logger, gid, id uint64) (*RaftGroup, error) {
	if g, ok := rm.groups[gid]; ok {
		lg.Error("[RaftGroupManager] restart raft group already launched",
			zap.Uint64("raft-group-id", gid),
			zap.Uint64("id", id),
			zap.Error(ErrRaftGroupManagerLaunched),
		)
		return g, ErrRaftGroupManagerLaunched
	}

	trans := rm.tm.CreateTransport(lg, gid)
	grp, err := NewRaftGroup(GroupConfig{
		Logger:                 lg,
		ID:                     id,
		GID:                    gid,
		DataDir:                logDirByGId(rm.logdir, gid),
		TickMs:                 defaultTickMs,
		ElectionTicks:          defaultElectionTicks,
		PreVote:                false,
		SnapshotCount:          defaultSnapshotCount,
		SnapshotCatchUpEntries: defaultSnapshotCatchUpEntries,
	}, trans)
	if err != nil {
		lg.Error("[RaftGroupManager] restart raft group failed",
			zap.Uint64("gid", gid),
			zap.Uint64("id", id),
			zap.Error(err),
		)
		return nil, errors.Wrap(err, "[RaftGroupManager] restart raft group error")
	}
	lg.Info("[RaftGroupManager] restart raft group success",
		zap.Uint64("group-id", gid),
		zap.Uint64("id", id),
	)
	rm.groups[gid] = grp
	return grp, nil
}

func (rm *RaftGroupManager) AddPeer(ctx context.Context, gid, peerid uint64, addr string) error {
	panic("not implement")
}

func (rm *RaftGroupManager) RemovePeer(ctx context.Context, gid, peerid uint64) error {
	panic("not implement")
}

func logDirByGId(parent string, gid uint64) string {
	return filepath.Join(parent, fmt.Sprintf("raft-group-%d", gid))
}
