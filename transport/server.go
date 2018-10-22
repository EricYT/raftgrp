package transport

import (
	"bytes"
	"context"
	"io"
	"net"

	"github.com/EricYT/raftgrp/proto"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/raftpb"
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

func (rs *raftServer) Send(stream proto.RaftGrouper_SendServer) error {

	var (
		gid uint64
		req *proto.SendRequest

		err error
	)

	reply := &proto.SendReply{Ok: "done"}
	defer func() {
		if err != nil {
			reply.Ok = err.Error()
		}
		stream.SendAndClose(reply)
	}()

	payload := new(bytes.Buffer)
readloop:
	for {
		req, err = stream.Recv()
		if err != nil {
			if err == io.EOF {
				break readloop
			}
			return err
		}
		// TODO: split metadata and payload
		gid = uint64(req.GetGroupId())
		payload.Write(req.GetMsg().GetPayload())
	}

	// unmarshal raft message
	m := &raftpb.Message{}
	if err = m.Unmarshal(payload.Bytes()); err != nil {
		return err
	}

	if err = rs.hnd.Process(stream.Context(), gid, m); err != nil {
		return err
	}

	return nil
}

type snapshotServer struct {
	Logger *zap.Logger
}
