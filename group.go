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

// user fsm
type FSM interface {
	RenderMessage(payload []byte) (p []byte, err error)
	ProcessMessage(payload []byte) (p []byte, err error)
	Apply(payload []byte) (err error)
}

type RaftGrouper interface {
	// Propose try to replicate a message to all followers.
	// We can't try to delete it when user already cancel
	// this one, because follower can't know whether it
	// was canceled by leader.
	// If this entry was commited, Grouper will callback
	// upstream to persist it.
	Propose(ctx context.Context, payload []byte) error
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

	// FIXME: not replace it right now
	snapshotter *snap.Snapshotter

	// wgMu blocks concurrent waitgroup mutation while server stopping
	wgMu sync.RWMutex
	wg   sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	leadTimeMu      sync.RWMutex
	leadElectedTime time.Time

	// user fms for processing data before sending to peers and
	// cutting data when receiving message.
	fsm FSM
}

func NewRaftGroup(cfg GroupConfig, t etransport.Transport) (grp *RaftGroup, err error) {
	var (
		n raft.Node

		// TODO: leave it alone right now
		snapshot *raftpb.Snapshot

		// FIXME:
		storageBackend store.Storage

		peerID   uint64
		topology *RaftGroupTopology
	)

	// TODO: snapshot interfaces
	if err = fileutil.TouchDirAll(cfg.SnapDir()); err != nil {
		cfg.Logger.Fatal("failed to create snapshot directory",
			zap.String("path", cfg.SnapDir()),
			zap.Error(err))
	}
	ss := snap.New(cfg.Logger, cfg.SnapDir())
	// end

	haveStorage := store.HaveStorage(cfg.LogDir())

	peerID = cfg.ID

	switch {
	case !haveStorage && !cfg.NewCluster:
		cfg.Logger.Info("[RaftGroup] joining a existing raft group",
			zap.Uint64("group-id", cfg.GID),
			zap.Uint64("id", cfg.ID),
		)

		topology = NewTopology(cfg.Logger)
		topology.SetGroupID(cfg.GID)
		topology.SetID(types.ID(peerID))

		storageBackend = store.NewStorage(cfg.Logger, cfg.LogDir(), ss)
		n = startNode(cfg, peerID, nil, storageBackend)

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
		topology.SetID(types.ID(cfg.ID))

		storageBackend = store.NewStorage(cfg.Logger, cfg.LogDir(), ss)
		n = startNode(cfg, peerID, topology.Members(), storageBackend)

	case haveStorage:
		cfg.Logger.Info("[RaftGroup] restart raft group",
			zap.Uint64("group-id", cfg.GID),
			zap.Uint64("id", cfg.ID),
		)

		if err = fileutil.IsDirWriteable(cfg.MemberDir()); err != nil {
			return nil, errors.Errorf("cannot write to member directory: %v", err)
		}

		snapshot, err = ss.Load()
		if err != nil && err != snap.ErrNoSnapshot {
			return nil, err
		}

		topology = NewTopology(cfg.Logger)
		if snapshot != nil {
			if err := topology.Recovery(snapshot.Data); err != nil {
				cfg.Logger.Error("[RaftGroup] recover topology from snapshot failed.",
					zap.Uint64("group-id", cfg.GID),
					zap.Uint64("id", cfg.ID),
					zap.Any("snapshot", snapshot),
					zap.Error(err),
				)
				return nil, err
			}
		}

		storageBackend = store.RestartStorage(cfg.Logger, cfg.LogDir(), snapshot, ss)
		n = restartNode(cfg, peerID, storageBackend)
	default:
		return nil, errors.Errorf("[RaftGroup] unknow raft group start config")
	}

	heartbeat := time.Duration(cfg.TickMs) * time.Millisecond

	grp = &RaftGroup{
		Cfg:         cfg,
		lgMu:        new(sync.RWMutex),
		lg:          cfg.Logger,
		errorc:      make(chan error, 1),
		snapshotter: ss,
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

	// initialize transport
	for _, m := range grp.topology.Members() {
		if grp.peerID != m.ID {
			cfg.Logger.Info("[RaftGroup] add peer ",
				zap.Uint64("peer-id", uint64(m.ID)),
				zap.String("addr", m.Addr))
			t.AddPeer(m.ID, []string{m.Addr})
		}
	}
	trs := NewTransport(t, grp.renderMessage)
	grp.r.transport = trs

	return grp, nil
}

func (g *RaftGroup) SetFSM(fsm FSM) {
	g.fsm = fsm
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
		m := ms[i]
		if m.Type != raftpb.MsgApp {
			continue
		}
		if m.Entries == nil || len(m.Entries) == 0 {
			continue
		}
		lg.Debug("[RaftGroup] rendering append message to", zap.Uint64("to", m.To))
		for j := range m.Entries {
			entry := m.Entries[j]
			if entry.Data == nil || len(entry.Data) == 0 {
				continue
			}
			// FIXME: redering message
			data, err := g.fsm.RenderMessage(entry.Data)
			if err != nil {
				lg.Warn("[RaftGroup] render message failed",
					zap.Error(err),
				)
				continue
			}
			m.Entries[j].Data = data
		}
	}
	return ms, nil
}

// Process takes a raft message and applies it to the server's raft state.
func (g *RaftGroup) Process(ctx context.Context, m *raftpb.Message) error {
	// FIXME: m.From is removed

	lg := g.getLogger()
	if m.Type == raftpb.MsgApp {
		if m.Entries != nil && len(m.Entries) != 0 {
			//FIXME: unloading payload and rewrite message
			lg.Info("[RaftGroup] Process message from", zap.Uint64("from", m.From))
			for i := range m.Entries {
				entry := m.Entries[i]
				if entry.Data == nil || len(entry.Data) == 0 {
					continue
				}
				data, err := g.fsm.ProcessMessage(entry.Data)
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
	return g.r.Step(ctx, *m)
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

	for {
		select {
		case err := <-g.errorc:
			lg.Warn("[RaftGroup] server error", zap.Error(err))
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
	newLeader := s.Lead != raft.None && g.getLead() != s.Lead
	if newLeader {
		lg.Info("[RaftGroup] ", zap.Uint64("new-leader", s.Lead))
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
		lg.Panic("[RaftGroup] unexpected leader snapshot from outdated index",
			zap.Uint64("current-snapshot-index", snapi),
			zap.Uint64("current-applied-index", appliedi),
			zap.Uint64("incoming-leader-index", snap.Metadata.Index),
			zap.Uint64("incoming-leader-term", snap.Metadata.Term),
		)
	}

	// FIXME:
	// callback user fsm to do something initialize

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
	if err := g.fsm.Apply(e.Data); err != nil {
		lg.Error("[RaftGroup] apply entry to user fsm failed",
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
	//	//cc.NodeID = raft.None
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
			g.r.transport.AddPeer(m.ID, []string{m.Addr})
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
		zap.String("local-id", g.ID().String()),
		zap.Uint64("local-applied-index", appliedi),
		zap.Uint64("local-snapshot-index", snapi),
		zap.Uint64("local-snapshot-count", g.Cfg.SnapshotCount),
	)

	g.snapshot(appliedi, g.confState)
	g.setSnapshottedIndex(appliedi)
}

func (g *RaftGroup) snapshot(snapi uint64, cs raftpb.ConfState) {
	// TODO: callback user fsm to gather snapshot information
	lg := g.getLogger()

	metadata := g.topology.Marshal()
	snapshot, err := g.r.storage.CreateSnapshot(snapi, &cs, metadata)
	if err != nil {
		lg.Fatal("[RaftGroup] snapshot create failed",
			zap.Uint64("group-id", g.groupID),
			zap.String("local-id", g.ID().String()),
			zap.Error(err),
		)
	}
	if err := g.r.storage.SaveSnap(snapshot); err != nil {
		lg.Fatal("[RaftGroup] snapshot save failed",
			zap.Uint64("group-id", g.groupID),
			zap.String("local-id", g.ID().String()),
			zap.Error(err),
		)
	}

	// maybe trigger compact
	compacti := uint64(1)
	if snapi > g.Cfg.SnapshotCatchUpEntries {
		compacti = snapi - g.Cfg.SnapshotCount
	}

	err = g.r.storage.Compact(compacti)
	if err != nil {
		if err == raft.ErrCompacted {
			return
		}

		g.lg.Fatal("[RaftGroup] storage compact failed.",
			zap.Uint64("group-id", g.groupID),
			zap.String("id", g.peerID.String()),
			zap.Error(err),
		)
	}
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

func (g *RaftGroup) isLeader() bool {
	return uint64(g.ID()) == g.Lead()
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

// RaftStatusGetter represents etcd server and Raft progress.
type RaftStatusGetter interface {
	ID() types.ID
	Leader() types.ID
	CommittedIndex() uint64
	AppliedIndex() uint64
	Term() uint64
}

func (g *RaftGroup) ID() types.ID { return g.topology.PeerID() }

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
