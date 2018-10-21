package raftgrp

import (
	"context"
	"sync"

	"github.com/coreos/etcd/etcdserver/api/rafthttp"
	"github.com/coreos/etcd/etcdserver/api/snap"
	"github.com/coreos/etcd/pkg/types"
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
}

func (t *transportV1) RemovePeer(id types.ID) {
}

func (t *transportV1) RemoveAllPeers() {
}

func (t *transportV1) Stop() {
}

type peer interface {
	start() error
	Send(m []raftpb.Message)
	SendSnapshot(m snap.Message)
	stop()
}

type Raft interface {
	Process(ctx context.Context, m []raftpb.Message) error
}

type transportPeer struct {
	ID   types.ID
	urls []string

	// for message callback
	r Raft

	mu     sync.Mutex
	cancel context.CancelFunc
	stopc  chan struct{}
}

// ETCD interface

var _ rafthttp.Transporter = (*transport)(nil)

type transport struct {
	*rafthttp.Transport
	r func([]raftpb.Message) ([]raftpb.Message, error)
}

func NewTransport(t *rafthttp.Transport, r func([]raftpb.Message) ([]raftpb.Message, error)) *transport {
	return &transport{
		Transport: t,
		r:         r,
	}
}

// Rendering payload
func (t *transport) Send(ms []raftpb.Message) {
	var err error
	if ms, err = t.r(ms); err != nil {
		t.Logger.Fatal("[transport] rendering messages error ", zap.Error(err))
	}
	t.Transport.Send(ms)
}
