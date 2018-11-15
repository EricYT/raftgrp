package raftgrp

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"sync/atomic"
	"time"

	etransport "github.com/EricYT/raftgrp/transport"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/pkg/pbutil"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"

	"github.com/EricYT/raftgrp/store"
)

var (
	ErrRaftGroupShutdown error = errors.New("[RaftGroup] already shutdown")
	ErrRaftGroupRemoved  error = errors.New("[RaftGroup] the member has been permanently removed from the cluster")
)

const (
	//backendStorage store.StorageType = store.StorageTypeWAL
	backendStorage store.StorageType = store.StorageTypeLevelDB
)

type RaftGrouper interface {
	// Propose try to replicate a message to all followers.
	// We can't try to delete it when user already cancel
	// this one, because follower can't know whether it
	// was canceled by leader.
	// If this entry was commited, Grouper will callback
	// upstream to persist it.
	Propose(ctx context.Context, payload []byte) error

	// add a new member into the group
	AddMember(ctx context.Context, m *Member) error
	// remove a member from the group
	RemoveMember(ctx context.Context, id uint64) error
}

var _ RaftGrouper = (*RaftGroup)(nil)

type RaftGroup struct {
	groupID uint64
	Cfg     GroupConfig

	snapshottedIndex uint64 // must use atomic operations to access;
	appliedIndex     uint64 // must use atomic operations to access;
	committedIndex   uint64 // must use aotmic operations to access;
	term             uint64 // must use atomic operations to access;
	lead             uint64 // must use atomic operations to access;
	confState        raftpb.ConfState

	peerID   types.ID
	topology *RaftGroupTopology

	r raftNode

	lgMu *sync.RWMutex
	lg   *zap.Logger

	stop     chan struct{}
	stopping chan struct{}
	done     chan struct{}

	errorc chan error

	// wgMu blocks concurrent waitgroup mutation while server stopping
	wgMu sync.RWMutex
	wg   sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	leadTimeMu      sync.RWMutex
	leadElectedTime time.Time

	// user state machine for processing data before sending to peers and
	// cutting data when receiving message.
	usm UserStateMachine
}

func NewRaftGroup(cfg GroupConfig, tf func(g *RaftGroup) etransport.Transport) (grp *RaftGroup, err error) {
	var (
		n raft.Node

		snapshot raftpb.Snapshot

		storageBackend store.Storage

		peerID   uint64
		topology *RaftGroupTopology
	)

	haveStorage := store.HaveStorage(backendStorage, cfg.RaftDir())

	peerID = cfg.ID

	switch {
	case !haveStorage && !cfg.NewCluster:
		cfg.Logger.Info("[RaftGroup] joining a existing raft group",
			zap.Uint64("group-id", cfg.GID),
			zap.Uint64("id", cfg.ID),
		)

		topology = NewTopology(cfg.Logger)
		topology.SetGroupID(cfg.GID)
		topology.SetPeerID(types.ID(peerID))

		storageBackend = store.NewStorageByType(cfg.Logger, backendStorage, cfg.RaftDir())
		n = startNode(cfg, peerID, nil, storageBackend)
		// FIXME: Etcd use any one in the peers to discovery the cluster topology.
		// What should we do? Just using peers act as remote peers for now.

	case !haveStorage && cfg.NewCluster:
		cfg.Logger.Info("[RaftGroup] creating a new raft group",
			zap.Uint64("group-id", cfg.GID),
			zap.Uint64("id", cfg.ID),
		)

		topology, err = NewTopologyWithPeers(cfg.Logger, cfg.Peers)
		if err != nil {
			cfg.Logger.Error("[RaftGroup] new topology with peers failed",
				zap.Uint64("group-id", cfg.GID),
				zap.Uint64("id", cfg.ID),
				zap.Any("peers", cfg.Peers),
				zap.Error(err),
			)
			return nil, err
		}
		topology.SetGroupID(cfg.GID)
		topology.SetPeerID(types.ID(cfg.ID))

		storageBackend = store.NewStorageByType(cfg.Logger, backendStorage, cfg.RaftDir())
		n = startNode(cfg, peerID, topology.Members(), storageBackend)

	case haveStorage:
		cfg.Logger.Info("[RaftGroup] restart raft group",
			zap.Uint64("group-id", cfg.GID),
			zap.Uint64("id", cfg.ID),
		)

		if err = fileutil.IsDirWriteable(cfg.MemberDir()); err != nil {
			return nil, errors.Errorf("cannot write to member directory: %v", err)
		}

		storageBackend = store.NewStorageByType(cfg.Logger, backendStorage, cfg.RaftDir())

		snapshot, err = storageBackend.Snapshot()
		if err != nil && err != snap.ErrNoSnapshot {
			return nil, err
		}

		// FIXME: We use snapshot and entries after this snapshot to recover cluster
		// topology, because we don't set applied index directly. So last status
		// can be recovered from snapshot and entries.
		topology = NewTopology(cfg.Logger)
		if !raft.IsEmptySnap(snapshot) && snapshot.Data != nil {
			snapmeta := etransport.SnapshotMetadata{}
			if err := snapmeta.Unmarshal(snapshot.Data); err != nil {
				cfg.Logger.Error("[RaftGroup] unmarshal snapshot metadata failed.",
					zap.Uint64("group-id", cfg.GID),
					zap.Uint64("id", cfg.ID),
					zap.Error(err),
				)
				return nil, err
			}

			if err := topology.Recovery(snapmeta.TopologyMeta); err != nil {
				cfg.Logger.Error("[RaftGroup] recover topology from snapshot failed.",
					zap.Uint64("group-id", cfg.GID),
					zap.Uint64("id", cfg.ID),
					zap.Any("snapshot", snapshot),
					zap.Error(err),
				)
				return nil, err
			}
		}
		// TODO: we haven't writen the information below into storage.
		// But we have these information in our application.
		topology.SetGroupID(cfg.GID)
		topology.SetPeerID(types.ID(cfg.ID))

		n = restartNode(cfg, peerID, storageBackend)

	default:
		return nil, errors.Errorf("[RaftGroup] unknow raft group start config")
	}

	cfg.Logger.Info("[RaftGroup] topology",
		zap.String("topology", string(topology.Marshal())),
	)

	heartbeat := time.Duration(cfg.TickMs) * time.Millisecond

	grp = &RaftGroup{
		Cfg:    cfg,
		lgMu:   new(sync.RWMutex),
		lg:     cfg.Logger,
		errorc: make(chan error, 1),
		r: *newRaftNode(
			raftNodeConfig{
				lg:          cfg.Logger,
				isIDRemoved: func(id uint64) bool { return topology.IsIDRemoved(types.ID(id)) },
				Node:        n,
				heartbeat:   heartbeat,
				storage:     storageBackend,
			},
		),
		groupID:  cfg.GID,
		peerID:   types.ID(peerID),
		topology: topology,
	}

	// generate transport
	t := tf(grp)

	// initialize transport
	for _, m := range grp.topology.Members() {
		if grp.peerID != m.ID {
			cfg.Logger.Info("[RaftGroup] add peer ",
				zap.Uint64("peer-id", uint64(m.ID)),
				zap.String("addr", m.Addr))
			t.AddPeer(m.ID, m.Addr)
		}
	}

	// for those joining a existing cluster
	for _, r := range cfg.Peers {
		if r.ID != uint64(grp.peerID) {
			cfg.Logger.Info("[RaftGroup] add remote peer",
				zap.Uint64("peer-id", r.ID),
				zap.String("addr", r.Addr),
			)
			t.AddPeer(types.ID(r.ID), r.Addr)
		}
	}

	trs := NewTransport(t, grp.renderMessage)
	grp.r.transport = trs

	return grp, nil
}

func (g *RaftGroup) SetUserStateMachine(m UserStateMachine) {
	g.usm = m
}

func (g *RaftGroup) getLogger() *zap.Logger {
	g.lgMu.RLock()
	l := g.lg
	g.lgMu.RUnlock()
	return l
}

// Propose try to replicate a message to others by RaftGroup.
func (g *RaftGroup) Propose(ctx context.Context, payload []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.done:
		return ErrRaftGroupShutdown
	default:
	}

	lg := g.getLogger()
	lg.Info("[RaftGroup] propose a message: ", zap.String("payload", string(payload)))

	err := g.r.Propose(ctx, payload)

	return err
}

func (g *RaftGroup) AddMember(ctx context.Context, m *Member) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  uint64(m.ID),
		Context: b,
	}
	return g.configure(ctx, cc)
}

func (g *RaftGroup) RemoveMember(ctx context.Context, id uint64) error {
	// FIXME: balabala check

	cc := raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: id,
	}
	return g.configure(ctx, cc)
}

// rendering message
func (g *RaftGroup) renderMessage(ms []raftpb.Message) ([]raftpb.Message, error) {
	//FIXME: rewrite message before we send it
	lg := g.getLogger()
	for i := range ms {
		m := &ms[i]
		if m.Type != raftpb.MsgApp {
			continue
		}
		if m.Entries == nil || len(m.Entries) == 0 {
			continue
		}

		lg.Debug("[RaftGroup] rendering append message to", zap.Uint64("to", m.To))
		ents := make([]raftpb.Entry, 0, len(m.Entries))
		for j := range m.Entries {
			entry := m.Entries[j]
			if entry.Type != raftpb.EntryNormal {
				ents = append(ents, entry)
				continue
			}
			if entry.Data == nil || len(entry.Data) == 0 {
				ents = append(ents, entry)
				continue
			}

			// FIXME: redering message. Don't try to modify
			// m.Entris[j].Data, it's a pointer point to
			// the shared entry in 'g.r.storage'. But we can
			// use one data copy serve all same entry from
			// .RenderMessage function.
			data, err := g.usm.RenderMessage(entry.Data)
			if err != nil {
				lg.Warn("[RaftGroup] render message failed",
					zap.Error(err),
				)
				// FIXME: ignore this message or send it ?
				ents = append(ents, entry)
				continue
			}
			entry.Data = data
			ents = append(ents, entry)
		}
		m.Entries = ents
	}
	return ms, nil
}

// Process takes a raft message and applies it to the server's raft state.
func (g *RaftGroup) Process(ctx context.Context, m raftpb.Message) error {
	if g.topology.IsIDRemoved(types.ID(m.From)) {
		return ErrIDRemoved
	}

	lg := g.getLogger()
	if m.Type == raftpb.MsgApp {
		if m.Entries != nil {
			//FIXME: unloading payload and rewrite message
			lg.Info("[RaftGroup] Process message from", zap.Uint64("from", m.From))
			for i := range m.Entries {
				entry := m.Entries[i]
				if entry.Type != raftpb.EntryNormal {
					continue
				}
				if entry.Data == nil || len(entry.Data) == 0 {
					continue
				}

				// FIXME: No side effect will occur when we modify
				// m.Entries[i].Data from receiving messages.
				data, err := g.usm.ProcessMessage(entry.Data)
				if err != nil {
					lg.Warn("[RaftGroup] process message failed",
						zap.Error(err),
					)
					continue
				}
				m.Entries[i].Data = data
			}
		}
	}
	return g.r.Step(ctx, m)
}

func (g *RaftGroup) IsIDRemoved(id uint64) bool {
	return g.topology.IsIDRemoved(types.ID(id))
}

func (g *RaftGroup) ReportUnreachable(id uint64) { g.r.ReportUnreachable(id) }

// ReportSnapshot reports snapshot sent status to the raft state machine,
// and clears the used snapshot from the snapshot store.
func (g *RaftGroup) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	g.r.ReportSnapshot(id, status)
}

func (g *RaftGroup) Start() {
	g.start()
}

func (g *RaftGroup) start() {
	g.done = make(chan struct{})
	g.stop = make(chan struct{})
	g.stopping = make(chan struct{})
	g.ctx, g.cancel = context.WithCancel(context.Background())

	go g.run()
}

// RaftReadyHandler contains a set of RaftGroup operations to be called
// by raftNode, and helps decouple state machine logic from Raft algorithms.
type raftReadyHandler struct {
	updateSoftState func(*raft.SoftState)
	applyAll        func(raftpb.Snapshot, []raftpb.Entry) error
	onError         func(err error)
}

func (g *RaftGroup) run() {
	lg := g.getLogger()

	// raft group from snapshot
	sn, err := g.r.storage.Snapshot()
	if err != nil {
		lg.Panic("[RaftGroup] fetch snapshot from storage error",
			zap.Error(err),
		)
	}
	g.setAppliedIndex(sn.Metadata.Index)
	g.setTerm(sn.Metadata.Term)
	g.setSnapshottedIndex(sn.Metadata.Index)
	g.confState = sn.Metadata.ConfState

	rh := &raftReadyHandler{
		updateSoftState: g.updateSoftState,
		applyAll:        g.applyAll,
		onError:         func(err error) { g.errorc <- err },
	}
	g.r.start(rh)

	defer func() {
		g.wgMu.Lock()
		close(g.stopping)
		g.wgMu.Unlock()
		g.cancel()

		// wait for all goroutines before close raft node
		g.wg.Wait()

		// stop raftNode
		g.r.stop()

		close(g.done)
	}()

	// FIXME: Put applyAll into a FIFO scheduler queue to operate.

	for {
		select {
		case err := <-g.errorc:
			lg.Warn("[RaftGroup] server error", zap.Error(err))
			g.usm.OnError(err)
			return
		case <-g.stop:
			lg.Info("[RaftGroup] server shutdown")
			return
		}
	}
}

// core
func (g *RaftGroup) updateSoftState(s *raft.SoftState) {
	lg := g.getLogger()

	lg.Info("[RaftGroup] soft state update",
		zap.Uint64("group-id", g.groupID),
		zap.Uint64("peer-id", uint64(g.peerID)),
		zap.String("current-role", s.RaftState.String()),
		zap.Time("time", time.Now()),
	)

	if !g.IsLeader() && (s.Lead != raft.None && s.Lead == uint64(g.peerID)) {
		lg.Debug("[RaftGroup] I'm leader now",
			zap.Uint64("group-id", g.groupID),
			zap.Uint64("peer-id", uint64(g.peerID)),
			zap.Time("time", time.Now()),
		)
		g.usm.OnLeaderStart()
	}

	if g.IsLeader() && s.Lead != uint64(g.peerID) {
		lg.Debug("[RaftGroup] I'm not leader now",
			zap.Uint64("group-id", g.groupID),
			zap.Uint64("peer-id", uint64(g.peerID)),
			zap.String("current-role", s.RaftState.String()),
			zap.Time("time", time.Now()),
		)
		g.usm.OnLeaderStop()
	}

	// TODO: notify leader changed
	newLeader := s.Lead != raft.None && g.getLead() != s.Lead
	if newLeader {
		lg.Debug("[RaftGroup] Leader change",
			zap.Uint64("group-id", g.groupID),
			zap.Uint64("peer-id", uint64(g.peerID)),
			zap.String("current-role", s.RaftState.String()),
			zap.Time("time", time.Now()),
		)
		g.usm.OnLeaderChange()
	}

	if s.Lead == raft.None {
		lg.Info("[RaftGroup] no leader")
	} else {
		lg.Info("[RaftGroup] leader exist ", zap.Uint64("leader", s.Lead))
	}

	g.setLead(s.Lead)
}

func (g *RaftGroup) applyAll(snap raftpb.Snapshot, ents []raftpb.Entry) error {
	if err := g.applySnapshot(snap); err != nil {
		return err
	}
	if err := g.applyEntries(ents); err != nil {
		return err
	}
	g.triggerSnapshot()
	return nil
}

func (g *RaftGroup) applySnapshot(snap raftpb.Snapshot) error {
	if raft.IsEmptySnap(snap) {
		return nil
	}

	lg := g.getLogger()

	appliedi := g.getAppliedIndex()
	snapi := g.getSnapshottedIndex()
	if snap.Metadata.Index <= appliedi {
		lg.Fatal("[RaftGroup] unexpected leader snapshot from outdated index",
			zap.Uint64("current-snapshot-index", snapi),
			zap.Uint64("current-applied-index", appliedi),
			zap.Uint64("incoming-leader-index", snap.Metadata.Index),
			zap.Uint64("incoming-leader-term", snap.Metadata.Term),
		)
	}

	// TODO: recover group info by snapshot
	if snap.Data != nil {
		snapmeta := etransport.SnapshotMetadata{}
		if err := snapmeta.Unmarshal(snap.Data); err != nil {
			lg.Fatal("[RaftGroup] unmarshal snapshot metadata failed.",
				zap.Uint64("group-id", g.groupID),
				zap.String("id", g.peerID.String()),
				zap.Error(err),
			)
		}

		if err := g.topology.Recovery(snapmeta.TopologyMeta); err != nil {
			lg.Fatal("[RaftGroup] recovery from snapshot failed.",
				zap.Uint64("group-id", g.groupID),
				zap.String("id", g.peerID.String()),
				zap.Error(err),
			)
		}
		for _, m := range g.topology.Members() {
			if m.ID != g.peerID {
				g.r.transport.AddPeer(m.ID, m.Addr)
			}
		}

		// FIXME:
		// callback user sm to do something initialize
		sw, err := g.usm.UnmarshalSnapshotWriter(snapmeta.UserStateMachineMeta)
		if err != nil {
			lg.Fatal("[RaftGroup] unmarshal snapshot failed.",
				zap.Uint64("group-id", g.groupID),
				zap.String("id", g.peerID.String()),
				zap.Error(err),
			)
		}

		if err := g.usm.OnSnapshotLoad(sw); err != nil {
			lg.Fatal("[RaftGroup] on snapshot load failed",
				zap.Uint64("group-id", g.groupID),
				zap.String("peer-id", g.peerID.String()),
				zap.Error(err),
			)
		}

	}

	g.setAppliedIndex(snap.Metadata.Index)
	g.setTerm(snap.Metadata.Term)
	g.setSnapshottedIndex(snap.Metadata.Index)
	g.confState = snap.Metadata.ConfState

	return nil
}

func (g *RaftGroup) applyEntries(ents []raftpb.Entry) error {
	lg := g.getLogger()
	if len(ents) == 0 {
		return nil
	}
	firsti := ents[0].Index
	appliedi := g.getAppliedIndex()
	if firsti > appliedi+1 {
		lg.Panic("[RaftGroup] unexpected commited index",
			zap.Uint64("current-applied-index", appliedi),
			zap.Uint64("first-committed-entry-inex", firsti),
		)
	}
	var es []raftpb.Entry
	if appliedi+1-firsti < uint64(len(ents)) {
		es = ents[appliedi+1-firsti:]
	}
	if len(es) == 0 {
		return nil
	}

	if err := g.apply(es); err != nil {
		return err
	}

	return nil
}

func (g *RaftGroup) apply(ents []raftpb.Entry) error {
	lg := g.getLogger()
	shouldStop := false
	for i := range ents {
		e := ents[i]
		switch e.Type {
		case raftpb.EntryNormal:
			g.applyEntryNormal(&e)
			g.setAppliedIndex(e.Index)
			g.setTerm(e.Term)

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			pbutil.MustUnmarshal(&cc, e.Data)
			removeSelf := g.applyConfChange(cc)
			g.setAppliedIndex(e.Index)
			g.setTerm(e.Term)
			shouldStop = shouldStop || removeSelf

		default:
			lg.Fatal("[RaftGroup] unknow entry type, must be either EntryNormal or EntryConfChange",
				zap.String("type", e.Type.String()),
			)
		}
	}
	if shouldStop {
		return ErrRaftGroupRemoved
	}
	return nil
}

func (g *RaftGroup) applyEntryNormal(e *raftpb.Entry) {
	if e.Data == nil {
		return
	}
	lg := g.getLogger()
	lg.Debug("[RaftGroup] apply entry normal",
		zap.Uint64("index", e.Index),
		zap.Uint64("term", e.Term),
		zap.String("data", string(e.Data)),
	)
	// TODO: callback OnApply to use state machine
	if err := g.usm.OnApply(e.Data); err != nil {
		lg.Error("[RaftGroup] apply entry to user sm failed",
			zap.Uint64("index", e.Index),
			zap.Uint64("term", e.Term),
			zap.String("data", string(e.Data)),
		)
	}
}

func (g *RaftGroup) applyConfChange(cc raftpb.ConfChange) bool {
	lg := g.getLogger()

	// FIXME: all peers will be added first time, context is raft.None
	//if err := g.topology.ValidateConfigurationChange(cc); err != nil {
	//	lg.Error("[RaftGroup] validate configureation change error",
	//		zap.Error(err),
	//	)
	//	cc.NodeID = raft.None
	//	g.r.ApplyConfChange(cc)
	//	return false
	//}

	g.confState = *g.r.ApplyConfChange(cc)
	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		log.Printf("[RaftGroup] receive conf change add node context: %s msg: (%#v)", string(cc.Context), cc)
		m := new(Member)
		if err := json.Unmarshal(cc.Context, m); err != nil {
			lg.Panic("[RaftGroup] conf change add node unmarshal error")
		}
		if m.ID != types.ID(cc.NodeID) {
			lg.Panic("[RaftGroup] got different member ID",
				zap.String("member-id-from-config-change-entry", types.ID(cc.NodeID).String()),
				zap.String("member-id-from-message", m.ID.String()),
			)
		}
		g.topology.AddMember(m)

		if m.ID != g.peerID {
			g.r.transport.AddPeer(m.ID, m.Addr)
		}

	case raftpb.ConfChangeRemoveNode:
		id := types.ID(cc.NodeID)
		g.topology.RemoveMember(id)
		if id == g.peerID {
			return true
		}

		g.r.transport.RemovePeer(id)

	case raftpb.ConfChangeUpdateNode:
		// FIXME: maybe copy all data to another node and restart it.
		// So we just want to change member Address
		panic("not implement")
	}

	return false
}

func (g *RaftGroup) configure(ctx context.Context, cc raftpb.ConfChange) (err error) {
	if err := g.r.ProposeConfChange(ctx, cc); err != nil {
		return err
	}
	return nil
}

// snapshot
func (g *RaftGroup) triggerSnapshot() {
	lg := g.getLogger()

	appliedi := g.getAppliedIndex()
	snapi := g.getSnapshottedIndex()

	//	lg.Info("[RaftGroup] trigger snapshot",
	//		zap.Uint64("applied-index", appliedi),
	//		zap.Uint64("snapshot-index", snapi),
	//		zap.Uint64("snapshot-count", g.Cfg.SnapshotCount),
	//	)

	if appliedi-snapi <= g.Cfg.SnapshotCount {
		return
	}

	lg.Info("[RaftGroup] triggering snapshot",
		zap.Uint64("group-id", g.groupID),
		zap.String("peer-id", g.PeerID().String()),
		zap.Uint64("local-applied-index", appliedi),
		zap.Uint64("local-snapshot-index", snapi),
		zap.Uint64("local-snapshot-count", g.Cfg.SnapshotCount),
	)

	g.snapshot(appliedi, g.confState)
	g.setSnapshottedIndex(appliedi)
}

func (g *RaftGroup) snapshot(snapi uint64, cs raftpb.ConfState) {
	lg := g.getLogger()

	// TODO: callback user sm to gather snapshot information
	r, err := g.usm.OnSnapshotSave()
	if err != nil {
		lg.Fatal("[RaftGroup] on snapshot save failed",
			zap.Uint64("group-id", g.groupID),
			zap.String("peer-id", g.peerID.String()),
			zap.Uint64("snapshot-index", snapi),
			zap.Error(err),
		)
	}
	rp, err := r.Marshal()
	if err != nil {
		lg.Fatal("[RaftGroup] snapshot state machine metadata marshal",
			zap.Uint64("group-id", g.groupID),
			zap.String("peer-id", g.peerID.String()),
			zap.Uint64("snapshot-index", snapi),
			zap.Error(err),
		)
	}

	topologyMeta := g.topology.Marshal()

	metadata, err := (&etransport.SnapshotMetadata{
		UserStateMachineMeta: rp,
		TopologyMeta:         topologyMeta,
	}).Marshal()
	if err != nil {
		lg.Fatal("[RaftGroup] snapshot metadata marshal",
			zap.Uint64("group-id", g.groupID),
			zap.String("peer-id", g.peerID.String()),
			zap.Uint64("snapshot-index", snapi),
			zap.Error(err),
		)
	}

	lg.Info("[RaftGroup] snapshot",
		zap.Uint64("group-id", g.groupID),
		zap.String("peer-id", g.peerID.String()),
		zap.Uint64("snapshot-index", snapi),
		zap.String("metadata", string(metadata)),
	)

	snapshot, err := g.r.storage.CreateSnapshot(snapi, &cs, metadata)
	if err != nil {
		lg.Fatal("[RaftGroup] snapshot create failed",
			zap.Uint64("group-id", g.groupID),
			zap.String("peer-id", g.PeerID().String()),
			zap.Uint64("snapshot-index", snapi),
			zap.Error(err),
		)
	}

	if err := g.r.storage.SaveSnap(snapshot); err != nil {
		lg.Fatal("[RaftGroup] snapshot save failed",
			zap.Uint64("group-id", g.groupID),
			zap.String("peer-id", g.PeerID().String()),
			zap.Uint64("snapshot-index", snapi),
			zap.Error(err),
		)
	}

	// maybe trigger compact
	compacti := uint64(1)
	if snapi > g.Cfg.SnapshotCatchUpEntries {
		compacti = snapi - g.Cfg.SnapshotCatchUpEntries
	}

	firstIndex, _ := g.r.storage.FirstIndex()
	lg.Debug("[RaftGroup] before compacting",
		zap.Uint64("group-id", g.groupID),
		zap.String("peer-id", g.PeerID().String()),
		zap.Uint64("snapshot-index", snapi),
		zap.Uint64("compact-index", compacti),
		zap.Uint64("first-index", firstIndex),
	)

	err = g.r.storage.Compact(compacti)
	if err != nil {
		if err == raft.ErrCompacted {
			return
		}

		g.lg.Fatal("[RaftGroup] storage compact failed.",
			zap.Uint64("group-id", g.groupID),
			zap.String("id", g.peerID.String()),
			zap.Uint64("snapshot-index", snapi),
			zap.Error(err),
		)
	}

	firstIndex, _ = g.r.storage.FirstIndex()
	lg.Debug("[RaftGroup] after compacting",
		zap.Uint64("group-id", g.groupID),
		zap.String("peer-id", g.PeerID().String()),
		zap.Uint64("snapshot-index", snapi),
		zap.Uint64("compact-index", compacti),
		zap.Uint64("first-index", firstIndex),
	)
}

func (g *RaftGroup) HardStop() {
	select {
	case g.stop <- struct{}{}:
	case <-g.done:
		return
	}
	<-g.done
}

func (g *RaftGroup) Stop() {
	// FIXME: transfer leader ship
	g.HardStop()
}

func (g *RaftGroup) updateCommitedIndex(ci uint64) {
	i := g.getCommittedIndex()
	if ci > i {
		g.setCommittedIndex(ci)
	}
}

func (g *RaftGroup) UnmarshalSnapshotParter(d []byte) (parter etransport.SnapshotParter, err error) {
	return g.usm.UnmarshalSnapshotParter(d)
}

func (g *RaftGroup) UnmarshalSnapshotWriter(d []byte) (parter etransport.SnapshotWriter, err error) {
	return g.usm.UnmarshalSnapshotWriter(d)
}

func (g *RaftGroup) IsLeader() bool {
	return uint64(g.PeerID()) == g.Lead()
}

func (g *RaftGroup) setCommittedIndex(v uint64) {
	atomic.StoreUint64(&g.committedIndex, v)
}

func (g *RaftGroup) getCommittedIndex() uint64 {
	return atomic.LoadUint64(&g.committedIndex)
}

func (g *RaftGroup) setAppliedIndex(v uint64) {
	atomic.StoreUint64(&g.appliedIndex, v)
}

func (g *RaftGroup) getAppliedIndex() uint64 {
	return atomic.LoadUint64(&g.appliedIndex)
}

func (g *RaftGroup) setSnapshottedIndex(v uint64) {
	atomic.StoreUint64(&g.snapshottedIndex, v)
}

func (g *RaftGroup) getSnapshottedIndex() uint64 {
	return atomic.LoadUint64(&g.snapshottedIndex)
}

func (g *RaftGroup) setTerm(v uint64) {
	atomic.StoreUint64(&g.term, v)
}

func (g *RaftGroup) getTerm() uint64 {
	return atomic.LoadUint64(&g.term)
}

func (g *RaftGroup) setLead(v uint64) {
	atomic.StoreUint64(&g.lead, v)
}

func (g *RaftGroup) getLead() uint64 {
	return atomic.LoadUint64(&g.lead)
}

type RaftStatusGetter interface {
	GroupID() uint64
	PeerID() types.ID
	Leader() types.ID
	CommittedIndex() uint64
	AppliedIndex() uint64
	Term() uint64
}

func (g *RaftGroup) GroupID() uint64 { return g.groupID }

func (g *RaftGroup) PeerID() types.ID { return g.topology.PeerID() }

func (g *RaftGroup) Leader() types.ID { return types.ID(g.getLead()) }

func (g *RaftGroup) Lead() uint64 { return g.getLead() }

func (g *RaftGroup) CommittedIndex() uint64 { return g.getCommittedIndex() }

func (g *RaftGroup) AppliedIndex() uint64 { return g.getAppliedIndex() }

func (g *RaftGroup) Term() uint64 { return g.getTerm() }

func (g *RaftGroup) goAttach(f func()) {
	g.wgMu.Lock() // this blocks with ongoing close(s.stopping)
	defer g.wgMu.Unlock()
	lg := g.getLogger()
	select {
	case <-g.stopping:
		lg.Warn("[RaftGroup] go attach group already shutdown")
		return
	default:
	}

	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		f()
	}()
}
