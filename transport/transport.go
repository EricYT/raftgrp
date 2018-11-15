package transport

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
)

const (
	recvMsgSize int = 1024
	propMsgSize int = 100 // 4M payload
)

var (
	ErrTransportPeerNotFound error = errors.New("[transport] peer not found")
)

type Transport interface {
	// Send sends out the given message the remote peer.
	// Each message has a To field, if the id is not exist
	// in peers, ignore it.
	Send(m []raftpb.Message)
	Process(m raftpb.Message)

	AddPeer(id types.ID, addr string)
	RemovePeer(id types.ID)
	RemoveAllPeers()

	// snapshot
	UnmarshalSnapshotWriter(p []byte) (SnapshotWriter, error)
	UnmarshalSnapshotParter(p []byte) (SnapshotParter, error)

	//
	getPeerAddr(peerID types.ID) (addr string, err error)

	Close()
}

type Raft interface {
	// raft operations
	Process(ctx context.Context, m raftpb.Message) error

	// snapshot
	UnmarshalSnapshotWriter(p []byte) (SnapshotWriter, error)
	UnmarshalSnapshotParter(p []byte) (SnapshotParter, error)
}

var _ Transport = (*transportV1)(nil)

type transportV1 struct {
	Logger  *zap.Logger
	cm      *clientManager
	gid     uint64
	localID types.ID

	mu    sync.RWMutex
	peers map[types.ID]Peer
	r     Raft
}

func newTransportV1(lg *zap.Logger, gid uint64, localID types.ID, cm *clientManager, r Raft) *transportV1 {
	t := &transportV1{
		Logger:  lg,
		cm:      cm,
		peers:   make(map[types.ID]Peer),
		gid:     gid,
		localID: localID,
		r:       r,
	}
	return t
}

func (t *transportV1) getPeerAddr(peerID types.ID) (addr string, err error) {
	t.mu.RLock()
	p, ok := t.peers[peerID]
	t.mu.RUnlock()
	if ok {
		return p.getAddr(), nil
	}
	return "", ErrTransportPeerNotFound
}

func (t *transportV1) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, p := range t.peers {
		p.stop()
	}
	t.peers = make(map[types.ID]Peer)
}

func (t *transportV1) AddPeer(id types.ID, addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.peers[id]; ok {
		// alrady exist
		return
	}
	t.peers[id] = startPeer(t.Logger, t.gid, t.localID, id, addr, t.cm, t.r)

	t.Logger.Info("[transportV1] add remote peer",
		zap.Uint64("group-id", t.gid),
		zap.String("local-id", t.localID.String()),
		zap.String("peer-id", id.String()),
		zap.String("address", addr),
	)
}

func (t *transportV1) RemovePeer(id types.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.removePeer(id)
}

func (t *transportV1) RemoveAllPeers() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id := range t.peers {
		t.removePeer(id)
	}
}

func (t *transportV1) removePeer(id types.ID) {
	p, ok := t.peers[id]
	if !ok {
		return
	}
	p.stop()
	delete(t.peers, id)
}

func (t *transportV1) UnmarshalSnapshotWriter(p []byte) (SnapshotWriter, error) {
	return t.r.UnmarshalSnapshotWriter(p)
}

func (t *transportV1) UnmarshalSnapshotParter(p []byte) (SnapshotParter, error) {
	return t.r.UnmarshalSnapshotParter(p)
}

func (t *transportV1) Send(m []raftpb.Message) {
	for _, msg := range m {
		if msg.To == raft.None {
			continue
		}
		to := types.ID(msg.To)

		t.mu.RLock()
		p, ok := t.peers[to]
		t.mu.RUnlock()
		if !ok {
			t.Logger.Warn("[transportV1] failed to get peer from transport",
				zap.Uint64("group-id", t.gid),
				zap.String("local-id", t.localID.String()),
				zap.String("peer-id", to.String()),
			)
			continue
		}
		if err := p.send(msg); err != nil {
			t.Logger.Error("[transportV1] failed to send message",
				zap.Uint64("group-id", t.gid),
				zap.String("local-id", t.localID.String()),
				zap.String("peer-id", to.String()),
				zap.Error(err),
			)
		}
	}
}

func (t *transportV1) Process(m raftpb.Message) {
	from := types.ID(m.From)
	t.mu.RLock()
	p, ok := t.peers[from]
	t.mu.RUnlock()
	if !ok {
		t.Logger.Warn("[transportV1] failed to get peer from transport for processing message",
			zap.Uint64("group-id", t.gid),
			zap.String("local-id", t.localID.String()),
			zap.String("peer-id", from.String()),
		)
		return
	}
	p.process(m)
}

type Peer interface {
	send(m raftpb.Message) error
	process(m raftpb.Message) error
	stop()
	getAddr() string
}

type peer struct {
	lg      *zap.Logger
	gid     uint64
	localID types.ID
	peerID  types.ID // peer id
	addr    string
	r       Raft

	// for sending messages to others
	cm *clientManager

	// for receiving messages from others
	recvc chan raftpb.Message
	propc chan raftpb.Message

	mu     sync.Mutex
	paused bool

	ctx    context.Context
	cancel context.CancelFunc
	stopc  chan struct{}
}

func startPeer(lg *zap.Logger, gid uint64, localid, peerid types.ID, addr string, cm *clientManager, r Raft) *peer {
	p := &peer{
		lg:      lg,
		gid:     gid,
		localID: localid,
		peerID:  peerid,
		addr:    addr,
		r:       r,
		cm:      cm,

		recvc: make(chan raftpb.Message, recvMsgSize),
		propc: make(chan raftpb.Message, propMsgSize),
		stopc: make(chan struct{}),
	}

	p.ctx, p.cancel = context.WithCancel(context.Background())

	go func() {
		for {
			select {
			case mm := <-p.recvc:
				// handle messages without Proposal from others
				if err := p.r.Process(p.ctx, mm); err != nil {
					p.lg.Warn("[peer] failed to process raft message",
						zap.Error(err),
					)
				}
			case <-p.stopc:
				return
			}
		}
	}()

	// r.Process might block.
	go func() {
		for {
			select {
			case mm := <-p.propc:
				// handle proposal messages from others
				if err := p.r.Process(p.ctx, mm); err != nil {
					p.lg.Warn("[peer] failed to process proposal message",
						zap.Error(err),
					)
				}
			case <-p.stopc:
				return
			}
		}
	}()

	// FIXME: reader

	return p
}

func (p *peer) send(m raftpb.Message) error {
	p.mu.Lock()
	pause := p.paused
	p.mu.Unlock()

	if pause {
		return nil
	}

	c, err := p.cm.pick(p.addr)
	if err != nil {
		p.lg.Error("[peer] failed to pick a client for sending messages",
			zap.Uint64("group-id", p.gid),
			zap.String("local-id", p.localID.String()),
			zap.String("peer-id", p.peerID.String()),
			zap.String("address", p.addr),
			zap.Error(err),
		)
		return err
	}

	writec := c.writec()
	select {
	case writec <- msgWrapper{groupID: p.gid, peerID: p.peerID, msg: m}: // FIXME: ctx used
	default:
		p.lg.Warn("[peer] dropped internal raft message since sending buffer is full",
			zap.Uint64("group-id", p.gid),
			zap.String("local-id", p.localID.String()),
			zap.String("remote-id", p.peerID.String()),
		)
	}

	return nil
}

func (p *peer) process(m raftpb.Message) error {
	//p.lg.Info("[peer] process message from remote",
	//	zap.Uint64("group-id", p.gid),
	//	zap.String("peer-id", p.peerID.String()),
	//)

	recvc := p.recvc
	if m.Type == raftpb.MsgApp {
		recvc = p.propc
	}

	select {
	case recvc <- m:
	default:
		// FIXME:
		p.lg.Warn("[peer] dropped internal raft message since receiving buffer is full",
			zap.Uint64("group-id", p.gid),
			zap.Uint64("remote-id", m.From),
		)
	}

	return nil
}

func (p *peer) pause() {
	p.mu.Lock()
	p.paused = true
	p.mu.Unlock()
}

func (p *peer) unpause() {
	p.mu.Lock()
	p.paused = false
	p.mu.Unlock()
}

func (p *peer) stop() {
	close(p.stopc)
	p.cancel()
}

func (p *peer) getAddr() string {
	return p.addr
}
