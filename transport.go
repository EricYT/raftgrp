package raftgrp

import (
	"github.com/coreos/etcd/etcdserver/api/rafthttp"
	"github.com/coreos/etcd/raft/raftpb"
	"go.uber.org/zap"
)

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
