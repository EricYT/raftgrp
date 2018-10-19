package raftgrp

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/etcdserver/api/rafthttp"
	"github.com/coreos/etcd/etcdserver/api/snap"
	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/coreos/etcd/pkg/pbutil"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/EricYT/raftgrp/store"

	stats "github.com/coreos/etcd/etcdserver/api/v2stats"
)

var (
	ErrRaftGroupShutdown error = errors.New("[RaftGroup] already shutdown")
	ErrRaftGroupRemoved  error = errors.New("[RaftGroup] the member has been permanently removed from the cluster")
)

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
	appliedIndex   uint64 // must use atomic operations to access;
	committedIndex uint64 // must use aotmic operations to access;
	term           uint64 // must use atomic operations to access;
	lead           uint64 // must use atomic operations to access;
	confState      *raftpb.ConfState

	id       types.ID
	topology *RaftGroupTopology

	r raftNode

	Cfg GroupConfig

	lgMu *sync.RWMutex
	lg   *zap.Logger

	stop     chan struct{}
	stopping chan struct{}
	done     chan struct{}

	// http stop channel
	httpstopc chan struct{}
	httpdonec chan struct{}

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
}

func NewRaftGroup(cfg GroupConfig) (grp *RaftGroup, err error) {
	var (
		n raft.Node

		// TODO: leave it alone right now
		snapshot *raftpb.Snapshot

		// FIXME:
		storageBackend store.Storage

		id       uint64
		topology *RaftGroupTopology
	)

	if terr := fileutil.TouchDirAll(cfg.DataDir); terr != nil {
		return nil, errors.Errorf("cannot access data directory: %v", terr)
	}

	// TODO: snapshot interfaces
	if err = fileutil.TouchDirAll(cfg.SnapDir()); err != nil {
		cfg.Logger.Fatal("failed to create snapshot directory",
			zap.String("path", cfg.SnapDir()),
			zap.Error(err))
	}
	ss := snap.New(cfg.Logger, cfg.SnapDir())
	snapshot, err = ss.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		return nil, errors.Wrap(err, "new raft group error")
	}

	id = cfg.ID
	storageBackend, isNew := store.NewStorage(cfg.Logger, cfg.WALDir(), snapshot, ss)

	switch {
	case isNew:
		topology, err = NewRaftGroupTopology(cfg.Logger, cfg.Peers)
		topology.SetID(types.ID(id))
		// FIXME:
		n = startNode(cfg, id, topology.MemberIDs(), storageBackend)
	case !isNew:
		if err = fileutil.IsDirWriteable(cfg.MemberDir()); err != nil {
			return nil, errors.Errorf("cannot write to member directory: %v", err)
		}
		if snapshot != nil {
			// FIXME: Is necessary to recover something ?
		}
		n = restartNode(cfg, id, storageBackend)
		// FIXME: recover from store
		topology, err = NewRaftGroupTopology(cfg.Logger, cfg.Peers)
		topology.SetID(types.ID(id))
		//topology = NewTopology(cfg.Logger)
		//topology.SetID(types.ID(id))
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
		id:       types.ID(id),
		topology: topology,
	}

	// initialize transport
	tr := &rafthttp.Transport{
		Logger: cfg.Logger,
		//DialTimeout: cfg.peerDialTimeout(),
		ID:          grp.id,
		ClusterID:   types.ID(0x1000), // FIXME: volume id maybe
		Raft:        grp,
		Snapshotter: ss,
		ErrorC:      grp.errorc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(strconv.Itoa(int(id))),
	}
	if err = tr.Start(); err != nil {
		return nil, errors.Wrap(err, "raft group start transport error")
	}
	// add peers
	for _, m := range grp.topology.Members() {
		if grp.id != m.ID {
			cfg.Logger.Info("[RaftGroup] add peer ",
				zap.Uint64("peer-id", uint64(m.ID)),
				zap.String("addr", m.Addr))
			tr.AddPeer(m.ID, []string{m.Addr})
		}
	}
	trs := NewTransport(tr, grp.renderingMessage)
	grp.r.transport = trs

	return grp, nil
}

func (g *RaftGroup) getLogger() *zap.Logger {
	g.lgMu.RLock()
	l := g.lg
	g.lgMu.RUnlock()
	return l
}

func (g *RaftGroup) RaftHandler() http.Handler {
	return g.r.transport.Handler()
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

func (g *RaftGroup) RemovePeer(ctx context.Context, id uint64) error {
	// FIXME: balabala check

	cc := raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: id,
	}
	return g.configure(ctx, cc)
}

// rendering message
func (g *RaftGroup) renderingMessage(ms []raftpb.Message) ([]raftpb.Message, error) {
	//FIXME: rewrite message before we send it
	lg := g.getLogger()
	for i := range ms {
		m := ms[i]
		if m.Type == raftpb.MsgApp {
			if m.Entries == nil || len(m.Entries) == 0 {
				continue
			}
			for j := range m.Entries {
				entry := m.Entries[j]
				if entry.Data == nil || len(entry.Data) == 0 {
					continue
				}
				// FIXME: redering message
				lg.Debug("[RaftGroup] rendering append message ", zap.Any("append-entry", entry))
			}
		}
	}
	return ms, nil
}

// Process takes a raft message and applies it to the server's raft state.
func (g *RaftGroup) Process(ctx context.Context, m raftpb.Message) error {
	// FIXME: m.From is removed
	lg := g.getLogger()
	if m.Type == raftpb.MsgApp {
		if m.Entries != nil && len(m.Entries) != 0 {
			//FIXME: unloading payload and rewrite message
			log.Printf("[RaftGroup] Process message from (%#v) (%#v)", m.From, m)
			for i := range m.Entries {
				entry := m.Entries[i]
				if entry.Data == nil || len(entry.Data) == 0 {
					continue
				}
				lg.Debug("[RaftGroup] process append message ", zap.Any("append-entry", entry))
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

	// http
	g.httpstopc = make(chan struct{})
	g.httpdonec = make(chan struct{})

	// FIXME:raft http interface
	go g.serveRaft()

	go g.run()
}

// RaftReadyHandler contains a set of RaftGroup operations to be called
// by raftNode, and helps decouple state machine logic from Raft algorithms.
type raftReadyHandler struct {
	updateSoftState func(*raft.SoftState)
	applyAllEntries func([]raftpb.Entry) error
}

func (g *RaftGroup) run() {
	lg := g.getLogger()

	rh := &raftReadyHandler{
		updateSoftState: g.updateSoftState,
		applyAllEntries: g.applyAllEntries,
	}
	g.r.start(rh)

	defer func() {
		g.wgMu.Lock()
		close(g.stopping)
		g.wgMu.Unlock()
		g.cancel()

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

func (g *RaftGroup) applyAllEntries(ents []raftpb.Entry) error {
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

	return g.apply(es)
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

	g.confState = g.r.ApplyConfChange(cc)
	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		log.Printf("[RaftGroup] receive conf change add node context: %s msg: (%#v)", string(cc.Context), cc)

		//FIXME(Fatal): how to solve this
		if string(cc.Context) == "None" {
			break
		}

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
		if m.ID != g.id {
			g.r.transport.AddPeer(m.ID, []string{m.Addr})
		}

	case raftpb.ConfChangeRemoveNode:
		id := types.ID(cc.NodeID)
		g.topology.RemoveMember(id)
		if id == g.id {
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

func (g *RaftGroup) ID() types.ID { return g.topology.ID() }

func (g *RaftGroup) Leader() types.ID { return types.ID(g.getLead()) }

func (g *RaftGroup) Lead() uint64 { return g.getLead() }

func (g *RaftGroup) CommittedIndex() uint64 { return g.getCommittedIndex() }

func (g *RaftGroup) AppliedIndex() uint64 { return g.getAppliedIndex() }

func (g *RaftGroup) Term() uint64 { return g.getTerm() }

// FIXME: for http testing
func (g *RaftGroup) stopHttp() {
	close(g.httpstopc)
	<-g.httpdonec
}

func (g *RaftGroup) serveRaft() {
	self := g.topology.MemberByID(g.topology.ID())

	url, err := url.Parse(self.Addr)
	if err != nil {
		log.Fatalf("[RaftGroup] Failed parsing URL (%v)", err)
	}

	ln, err := newStoppableListener(url.Host, g.httpstopc)
	if err != nil {
		log.Fatalf("[RaftGroup] Failed to listen rafthttp (%v)", err)
	}

	err = (&http.Server{Handler: g.RaftHandler()}).Serve(ln)
	select {
	case <-g.httpstopc:
	default:
		log.Fatalf("[RaftGroup]: Failed to serve rafthttp (%v)", err)
	}
	close(g.httpdonec)
}
