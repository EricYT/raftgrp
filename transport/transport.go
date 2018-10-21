package transport

import (
	"context"
	"sync"

	"github.com/EricYT/raftgrp/proto"
	"github.com/coreos/etcd/etcdserver/api/snap"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"go.uber.org/zap"
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

	mu    sync.Mutex
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

		t.mu.Lock()
		peer, ok := t.peers[to]
		t.mu.Unlock()

		if ok {
			if err := t.mgr.WithClient(peer.addr, func(ctx context.Context, c *Client) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				rc := c.RaftGrouperClient()
				payload, err := msg.Marshal()
				if err != nil {
					t.Logger.Warn("[transportV1] marshal raft payload error",
						zap.Any("message", msg),
						zap.Error(err),
					)
					return err
				}
				message := &proto.SendRequest{
					GroupId: int64(t.gid),
					Msg: &proto.Message{
						Payload: payload,
					},
				}
				reply, err := rc.Send(ctx, message)
				if err != nil {
					t.Logger.Warn("[transportV1] send message error",
						zap.Error(err),
					)
					return err
				}
				if reply.Ok != "done" {
					t.Logger.Debug("[transportV1] send message success",
						zap.Uint64("peer-id", uint64(to)),
						zap.String("peer-addr", c.addr),
						zap.Any("reply", reply),
					)
				}
				return nil
			}); err != nil {
				t.Logger.Warn("[transportV1] send msg to peer erorr",
					zap.Uint64("peer-id", uint64(to)),
					zap.String("peer-addr", peer.addr),
				)
			}
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

type Peer interface {
	Send(m []raftpb.Message)
	SendSnapshot(m snap.Message)
}

type peer struct {
	id   types.ID
	addr string
}
