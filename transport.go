package raftgrp

import (
	"log"

	etransport "github.com/EricYT/raftgrp/transport"
	"github.com/coreos/etcd/etcdserver/api/snap"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft/raftpb"
	"go.uber.org/zap"
)

// ETCD interface

type Transporter interface {
	Send(m []raftpb.Message)
	SendSnapshot(m snap.Message)
	AddPeer(id types.ID, urls []string)
	RemovePeer(id types.ID)
	RemoveAllPeers()
}

var _ Transporter = (*transport)(nil)

type transport struct {
	etransport.Transport
	r func([]raftpb.Message) ([]raftpb.Message, error)
}

func NewTransport(t etransport.Transport, r func([]raftpb.Message) ([]raftpb.Message, error)) *transport {
	return &transport{
		Transport: t,
		r:         r,
	}
}

// Rendering payload
func (t *transport) Send(ms []raftpb.Message) {
	var err error
	if ms, err = t.r(ms); err != nil {
		log.Fatal("[transport] rendering messages error ", zap.Error(err))
	}
	t.Transport.Send(ms)
}
