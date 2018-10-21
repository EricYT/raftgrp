package raftgrp

import (
	"sync"

	"github.com/coreos/etcd/etcdserver/api/snap"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"go.uber.org/zap"
)

type Transport interface {
	Start() error

	// Send sends out the given message the remote peer.
	// Each message has a To field, if the id is not exist
	// in peers, ignore it.
	Send(m []raftpb.Message)

	// SendSnapshot sends a snpathost ot peer.
	SendSnapshot(m snap.Message)

	AddPeer(id types.ID, urls []string)
	RemovePeer(id types.ID)
	RemoveAllPeers()

	Stop()
}

var _ Transport = (*transportV1)(nil)

type transportV1 struct {
	Logger *zap.Logger

	mu    sync.Mutex
	peers map[types.ID]peer
}

func (t *transportV1) Start() error {
	return nil
}

func (t *transportV1) Send(m []raftpb.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, msg := range m {
		if msg.To == raft.None {
			continue
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
