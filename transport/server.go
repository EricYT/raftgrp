package transport

import (
	"context"
	"net"

	"github.com/EricYT/raftgrp/proto"
	"go.etcd.io/etcd/raft/raftpb"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	grpc "google.golang.org/grpc"
)

// rpc server manager
type serverManager struct {
	Logger *zap.Logger

	Addr string
	s    *grpc.Server

	rs *raftServer
	ss *snapshotServer
}

func (sm *serverManager) Start() error {
	lis, err := net.Listen("tcp", sm.Addr)
	if err != nil {
		return errors.Wrapf(err, "[serverManager] listen addrss %s error", sm.Addr)
	}
	sm.s = grpc.NewServer()

	// register service
	proto.RegisterRaftGrouperServer(sm.s, sm.rs)

	// blocking
	if err := sm.s.Serve(lis); err != nil {
		return errors.Wrap(err, "[serverManager] tcp serve error")
	}

	return nil
}

func (sm *serverManager) Stop() {
	sm.s.Stop()
}

type Handler interface {
	// raft operations
	Process(ctx context.Context, gid uint64, m *raftpb.Message) error

	// file oeprations
}

type raftServer struct {
	Logger *zap.Logger
	hnd    Handler
}

func (rs *raftServer) Send(ctx context.Context, req *proto.SendRequest) (*proto.SendReply, error) {
	reply := &proto.SendReply{}

	gid := uint64(req.GroupId)
	//msg := req.Msg

	// unmarshal message
	m := &raftpb.Message{}
	if err := m.Unmarshal(req.Msg.Payload); err != nil {
		reply.Ok = "unmarshal failed"
		return reply, err
	}

	if err := rs.hnd.Process(ctx, gid, m); err != nil {
		reply.Ok = "raft group process error"
		return reply, err
	}

	reply.Ok = "done"
	return reply, nil
}

type snapshotServer struct {
	Logger *zap.Logger
}
