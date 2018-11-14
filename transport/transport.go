package transport

import (
	"bytes"
	"context"
	"io"
	"sync"

	"github.com/EricYT/raftgrp/proto"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Transport interface {
	// Send sends out the given message the remote peer.
	// Each message has a To field, if the id is not exist
	// in peers, ignore it.
	Send(m []raftpb.Message)

	AddPeer(id types.ID, urls []string)
	RemovePeer(id types.ID)
	RemoveAllPeers()

	// Snapshot
	WithClient(to types.ID, hnd func(ctx context.Context, client *Client) error) error
}

type Raft interface {
	// raft operations
	Process(ctx context.Context, gid uint64, m *raftpb.Message) error

	// snapshot
	UnmarshalSnapshotWriter(ctx context.Context, gid uint64, p []byte) (SnapshotWriter, error)
	UnmarshalSnapshotParter(ctx context.Context, gid uint64, p []byte) (SnapshotParter, error)
}

var _ Transport = (*transportV1)(nil)

type transportV1 struct {
	Logger *zap.Logger

	cm ClientConnManager

	mu    sync.RWMutex
	peers map[types.ID]Peer
	gid   uint64
	r     Raft
}

func NewTransportV1(lg *zap.Logger, gid uint64, cm ClientConnManager, r Raft) *transportV1 {
	t := &transportV1{
		Logger: lg,
		cm:     cm,
		peers:  make(map[types.ID]Peer),
		gid:    gid,
		r:      r,
	}

	return t
}

func (t *transportV1) Close() error {
	panic("not implement")
}

func (t *transportV1) AddPeer(id types.ID, addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.peers[id]; ok {
		// alrady exist
		return
	}
	t.peers[id] = startPeer(t, t.gid, id, addr)

	t.Logger.Info("[transportV1] add remote peer",
		zap.Uint64("group-id", t.gid),
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
	t.peers = nil
}

func (t *transportV1) Send(m []raftpb.Message) {
	for _, msg := range m {
		if msg.To == raft.None {
			continue
		}
		to := types.ID(msg.To)

		t.mu.RLock()
		peer, ok := t.peers[to]
		t.mu.RUnlock()
		if !ok {
			continue
		}

		if err := t.mgr.WithClient(peer.addr, func(ctx context.Context, c *Client) error {
			return t.sendMessageToPeer(ctx, c, &msg)
		}); err != nil {
			t.Logger.Error("[transportV1] send msg to peer erorr",
				zap.Uint64("peer-id", uint64(to)),
				zap.String("peer-addr", peer.addr),
				zap.Error(err),
			)
		}
	}
}

func (t *transportV1) WithClient(to types.ID, hnd func(ctx context.Context, c *Client) error) error {
	t.mu.RLock()
	peer, ok := t.peers[to]
	t.mu.RUnlock()
	if !ok {
		return ErrTransportNotFound
	}
	return t.mgr.WithClient(peer.addr, hnd)
}

func (t *transportV1) Stop() {
}

func (t *transportV1) sendMessageToPeer(ctx context.Context, c *Client, msg *raftpb.Message) error {
	payload, err := msg.Marshal()
	if err != nil {
		t.Logger.Warn("[transportV1] marshal raft payload error",
			zap.Any("message", msg),
			zap.Error(err),
		)
		return err
	}
	payloadBuf := bytes.NewReader(payload)

	rc, err := c.RaftGrouperClient().Send(ctx, grpc.EmptyCallOption{})
	if err != nil {
		t.Logger.Warn("[transportV1] create a stream to peer failed",
			zap.String("peer-addr", c.addr),
			zap.Error(err),
		)
		return err
	}

	// send message matadata firstly
	meta := &proto.SendRequest{
		Msg: &proto.SendRequest_Meta{
			Meta: &proto.Metadata{
				GroupId: int64(t.gid),
			},
		},
	}
	if err := rc.Send(meta); err != nil {
		t.Logger.Warn("[transportV1] send message to peer failed",
			zap.String("peer-addr", c.addr),
			zap.Any("metadata", meta),
			zap.Error(err),
		)
		rc.CloseSend()
		return err
	}

	// TODO: send message payload 1m per slice
	// FIXME: 1Kb => 512Kb
	buf := make([]byte, 1*1024)
	for {
		n, err := payloadBuf.Read(buf)
		if n > 0 {
			message := &proto.SendRequest{
				Msg: &proto.SendRequest_Payload{
					Payload: &proto.Playload{
						Data: buf[:n],
					},
				},
			}
			if err := rc.Send(message); err != nil {
				t.Logger.Error("[transportV1] send message error",
					zap.Error(err),
				)
				rc.CloseSend()
				return err
			}
		}
		if err != nil {
			if err != io.EOF {
				t.Logger.Error("[transportV1] read payload buffer failed",
					zap.String("peer-addr", c.addr),
					zap.Error(err),
				)
				rc.CloseSend()
				return err
			}
			break
		}
	}

	reply, err := rc.CloseAndRecv()
	if err != nil {
		t.Logger.Error("[transportV1] close and recv failed",
			zap.String("peer-addr", c.addr),
			zap.Error(err),
		)
		return err
	}
	if reply.Ack.Code != 0 {
		t.Logger.Debug("[transportV1] send message failed",
			zap.String("peer-addr", c.addr),
			zap.Int64("code", reply.Ack.Code),
			zap.Any("reply", reply),
		)
	}
	return nil
}

type Peer interface {
	Send(m []raftpb.Message)
	Stop()
}

type peer struct {
	lg   *zap.Logger
	gid  uint64
	id   types.ID // remote peer id
	addr string
	r    Raft

	// streams
	writer *streamWriter
	reader *streamReader

	// for receiving messages from others
	recvc chan raftpb.Message
	propc chan raftpb.Message

	mu     sync.Mutex
	paused bool

	ctx    context.Context
	cancel context.CancelFunc
	stopc  chan struct{}
}

func startPeer(t *transportV1, gid uint64, id types.ID, addr string) *peer {
	p := &peer{
		lg:   t.Logger,
		gid:  gid,
		id:   id,
		addr: addr,

		recvc: make(chan raftpb.Message, recvMsgSize),
		propc: make(chan raftpb.Message, propMSgSize),
		stopc: make(chan struct{}),

		writer: startStreamWriter(t.Logger, gid, id, addr),
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

	p.reader = &streamReader{
		lg:    t.Logger,
		gid:   gid,
		id:    id,
		recvc: p.recvc,
		propc: p.propc,
		// FIXME: rate limitter
	}
	p.reader.Start()

	return p
}

func (p *peer) send(m raftpb.Message) {
	p.mu.Lock()
	pause := p.paused
	p.mu.Unlock()

	if pause {
		return
	}

	writec := p.writer.writec
	select {
	case writec <- m:
	default:
		p.lg.Warn("[peer] dropped internal raft message since sending buffer is full",
			zap.Uint64("group-id", p.gid),
			zap.String("remote-id", p.id.String()),
		)
	}
}

func (p *peer) process(m raftpb.Message) {
	recvc := p.reader.recvc
	if m.Type == raftpb.MsgApp {
		recvc = p.reader.propc
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
}

func (p *peer) Stop() {
	close(p.stopc)
	p.cancel()
	p.writer.Stop()
	p.reader.Stop()
}
