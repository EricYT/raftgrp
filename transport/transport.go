package transport

import (
	"bytes"
	"context"
	"sync"

	"github.com/EricYT/raftgrp/proto"
	"go.etcd.io/etcd/etcdserver/api/snap"
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

	// SendSnapshot sends a snpathost ot peer.
	SendSnapshot(m snap.Message)

	AddPeer(id types.ID, urls []string)
	RemovePeer(id types.ID)
	RemoveAllPeers()
}

var _ Transport = (*transportV1)(nil)

type transportV1 struct {
	Logger *zap.Logger

	mgr ClientConnManager

	mu    sync.RWMutex
	peers map[types.ID]peer
	gid   uint64
}

func (t *transportV1) Start() error {
	return nil
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
			t.Logger.Warn("[transportV1] send msg to peer erorr",
				zap.Uint64("peer-id", uint64(to)),
				zap.String("peer-addr", peer.addr),
			)
		}
	}
}

func (t *transportV1) SendSnapshot(m snap.Message) {
}

func (t *transportV1) AddPeer(id types.ID, urls []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.peers[id]; ok {
		// alrady exist
		return
	}
	t.peers[id] = peer{id: id, addr: urls[0]}
}

func (t *transportV1) RemovePeer(id types.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.peers, id)
}

func (t *transportV1) RemoveAllPeers() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers = nil
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
	buf := bytes.NewBuffer(payload)

	rc, err := c.RaftGrouperClient().Send(ctx, grpc.EmptyCallOption{})
	if err != nil {
		t.Logger.Warn("[transportV1] construct raft group send client failed",
			zap.Error(err),
		)
		return err
	}

	// TODO: send message 1m per slice
	var (
		n    int
		rerr error
	)
	slice := make([]byte, 1*1024)

	for rerr == nil {
		n, rerr = buf.Read(slice)
		message := &proto.SendRequest{
			GroupId: int64(t.gid),
			Msg: &proto.Message{
				Payload: slice[:n],
			},
		}
		if err := rc.Send(message); err != nil {
			t.Logger.Warn("[transportV1] send message error",
				zap.Error(err),
			)
			return err
		}
	}

	reply, err := rc.CloseAndRecv()
	if err != nil {
		t.Logger.Warn("[transportV1] close and recv failed",
			zap.Error(err),
		)
		return err
	}
	if reply.Ok != "done" {
		t.Logger.Debug("[transportV1] send message failed",
			zap.String("ok", reply.Ok),
			zap.Any("reply", reply),
		)
	}
	return nil
}

type Peer interface {
	Send(m []raftpb.Message)
	SendSnapshot(m snap.Message)
}

type peer struct {
	id   types.ID
	addr string
}
