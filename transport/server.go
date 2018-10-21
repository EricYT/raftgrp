package transport

import (
	"context"
	"net"

	"github.com/EricYT/raftgrp/proto"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	grpc "google.golang.org/grpc"
)

// rpc server manager
type serverManager struct {
	Logger *zap.Logger

	Addr string

	rs *raftServer
	ss *snapshotServer
}

func (sm *serverManager) start() error {
	lis, err := net.Listen("tpc", sm.Addr)
	if err != nil {
		return errors.Wrapf(err, "[serverManager] listen addrss %s error", sm.Addr)
	}
	// TODO: grpc server options
	s := grpc.NewServer()

	// register service
	proto.RegisterRaftGrouperServer(s, sm.rs)

	// blocking
	if err := s.Serve(lis); err != nil {
		return errors.Wrap(err, "[serverManager] tcp serve error")
	}

	return nil
}

type Handler struct {
	// raft operations
	Process func(ctx context.Context, gid uint64, m *raftpb.Message) error

	// file oeprations
}

type raftServer struct {
	Logger *zap.Logger
	hnd    Handler
}

func (rs *raftServer) Send(ctx context.Context, req *proto.SendRequest) (*proto.SendReply, error) {

	gid := uint64(req.GroupId)
	//msg := req.Msg

	// unmarshal message
	m := &raftpb.Message{}
	if err := m.Unmarshal(req.Msg.Payload); err != nil {
		return nil, err
	}

	if err := rs.hnd.Process(ctx, gid, m); err != nil {
		return nil, err
	}

	return nil, nil
}

type snapshotServer struct {
	Logger *zap.Logger
}
