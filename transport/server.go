package transport

import (
	"bytes"
	"context"
	"io"
	"net"

	"github.com/EricYT/raftgrp/proto"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
	grpc "google.golang.org/grpc"
)

var (
	ErrTransportUnknowMessageType error = errors.New("[transport] unknow message type")
)

const (
	bytesPerChunk int = 512 * 1025 // 512kb
)

const (
	snapshotReadSuccess int = iota
	snapshotReadFailed
)

// rpc server manager
type serverManager struct {
	Logger *zap.Logger

	Addr string
	s    *grpc.Server

	rs *raftServer
}

func (sm *serverManager) Start() error {
	lis, err := net.Listen("tcp", sm.Addr)
	if err != nil {
		sm.Logger.Error("[serverManager] listen failed",
			zap.String("address", sm.Addr),
			zap.Error(err),
		)
		return errors.Wrapf(err, "[serverManager] listen addrss %s error", sm.Addr)
	}
	sm.s = grpc.NewServer()

	// register service
	proto.RegisterRaftGrouperServer(sm.s, sm.rs)

	// blocking
	if err := sm.s.Serve(lis); err != nil {
		sm.Logger.Error("[serverManager] serve failed",
			zap.String("address", sm.Addr),
			zap.Error(err),
		)
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

	// snapshot
	UnmarshalSnapshotReader(ctx context.Context, gid uint64, p []byte) (SnapshotReader, error)
	UnmarshalSnapshotParter(ctx context.Context, gid uint64, p []byte) (SnapshotParter, error)
}

type raftServer struct {
	Logger *zap.Logger
	hnd    Handler
	tm     *TransportManager
}

func (rs *raftServer) Send(stream proto.RaftGrouper_SendServer) error {

	var (
		gid uint64
		req *proto.SendRequest

		err error
	)

	reply := &proto.SendReply{
		Ack: &proto.ACK{
			Code: 0,
		},
	}
	defer func() {
		if err != nil {
			reply.Ack.Code = 500
			reply.Ack.Detail = err.Error()
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

		switch req.Msg.(type) {
		case *proto.SendRequest_Meta:
			gid = uint64(req.GetMeta().GetGroupId())
		case *proto.SendRequest_Payload:
			payload.Write(req.GetPayload().GetData())
		default:
			return ErrTransportUnknowMessageType
		}
	}

	// unmarshal raft message
	m := &raftpb.Message{}
	if err = m.Unmarshal(payload.Bytes()); err != nil {
		return err
	}

	// TODO: snapshot sync snapshot here
	if m.Type == raftpb.MsgSnap && !raft.IsEmptySnap(m.Snapshot) {
		if err := rs.syncSnapshotFromLeader(gid, m); err != nil {
			rs.Logger.Error("[raftServer] sync snapshot from leader failed",
				zap.Uint64("group-id", gid),
				zap.Error(err),
			)
			return err
		}
	}

	if err = rs.hnd.Process(stream.Context(), gid, m); err != nil {
		return err
	}

	return nil
}

// leader handle this
func (rs *raftServer) SnapshotRead(req *proto.SnapshotReaderRequest, stream proto.RaftGrouper_SnapshotReadServer) (err error) {
	gid := uint64(req.GetGroupId())

	gidFields := []zap.Field{
		zap.Uint64("group-id", gid),
	}

	rs.Logger.Debug("[raftServer] receive snapshot read",
		gidFields...,
	)

	parter, err := rs.hnd.UnmarshalSnapshotParter(stream.Context(), gid, req.GetParter())
	if err != nil {
		rs.Logger.Error("[raftServer] unmarshal snapshot parter failed.",
			append(gidFields, zap.Error(err))...,
		)
		return err
	}

	rs.Logger.Debug("[raftServer] snapshot pater",
		append(gidFields, zap.Any("metadata", parter.Meta))...,
	)

	// create snapshot part reader
	reader, err := parter.CreateReader()
	if err != nil {
		rs.Logger.Error("[raftServer] create snpashot part reader failed.",
			append(gidFields, zap.Error(err))...,
		)
		return err
	}
	defer reader.Close()

	buf := make([]byte, bytesPerChunk)
	for {
		n, rerr := reader.Read(buf)
		if n > 0 {
			if err = stream.Send(&proto.SnapshotResponse{
				Chunk: &proto.Chunk{
					Chunk: buf[:n],
				},
			}); err != nil {
				rs.Logger.Error("[raftServer] send snapshot chunk failed.",
					append(gidFields, zap.Error(err))...,
				)
				return err
			}
		}

		if rerr != nil {
			if rerr != io.EOF {
				err = rerr
			}
			break
		}
	}

	return err
}

// sync snapshot from follower
func (rs *raftServer) syncSnapshotFromLeader(gid uint64, m *raftpb.Message) (err error) {
	gidFields := []zap.Field{
		zap.Uint64("group-id", gid),
	}

	rs.Logger.Debug("[raftServer] sync snapshot from leader",
		gidFields...,
	)

	// parse snapshot metadata
	snapMeta := &SnapshotMetadata{}
	if err := snapMeta.Unmarshal(m.Snapshot.Data); err != nil {
		rs.Logger.Error("[raftServer] unmarshal snapshot metadata failed.",
			append(gidFields, zap.Error(err))...)
		return err
	}

	snapReader, err := rs.hnd.UnmarshalSnapshotReader(context.TODO(), gid, snapMeta.StateMachineMeta)
	if err != nil {
		rs.Logger.Error("[raftServer] unmarshal snapshot reader failed.",
			append(gidFields, zap.Error(err))...)
		return err
	}

	rs.Logger.Debug("[raftServer] snapshot reader",
		zap.Uint64("group-id", gid),
		zap.String("snap-id", snapReader.GetID()),
		zap.Any("metadata", snapReader.Meta),
	)

	// read snapshot
	parters := snapReader.Next()
	for parter := range parters {
		parterPayload, err := parter.Marshal()
		if err != nil {
			rs.Logger.Error("[raftServer] marshal pater failed",
				zap.Uint64("group-id", gid),
				zap.Error(err),
			)
			return err
		}

		rs.Logger.Debug("[raftServer] snapshot pater",
			zap.Uint64("group-id", gid),
			zap.String("snap-id", snapReader.GetID()),
			zap.String("detail", string(parterPayload)),
		)

		partWriter, err := parter.CreateWriter()
		if err != nil {
			rs.Logger.Error("[raftServer] snapshot create part writer failed.",
				zap.Uint64("group-id", gid),
				zap.String("snap-id", snapReader.GetID()),
				zap.Error(err),
			)
			return err
		}
		defer partWriter.Close()

		// read data from remote
		t, ok := rs.tm.GetTransport(gid)
		if !ok {
			rs.Logger.Error("[raftServer] get transport instance failed.",
				zap.Uint64("group-id", gid),
			)
			return ErrTransportNotFound
		}

		hnd := func(ctx context.Context, c *Client) error {
			req := &proto.SnapshotReaderRequest{
				GroupId: int64(gid),
				Parter:  parterPayload,
			}

			client := c.RaftGrouperClient()
			stream, err := client.SnapshotRead(ctx, req, grpc.EmptyCallOption{})
			if err != nil {
				rs.Logger.Error("[raftServer] snapshot read failed.",
					zap.Uint64("group-id", gid),
					zap.String("detail", string(parterPayload)),
					zap.Error(err),
				)
				return err
			}

			// read loop
			for {
				response, err := stream.Recv()
				if err != nil {
					if err != io.EOF {
						rs.Logger.Error("[raftServer] snapshot recv parter failed.",
							zap.Uint64("group-id", gid),
							zap.String("detail", string(parterPayload)),
							zap.Error(err),
						)
						return err
					}
					break
				}

				_, err = partWriter.Write(response.Chunk.Chunk)
				if err != nil {
					rs.Logger.Error("[raftServer] snapshot write parter failed.",
						zap.Uint64("group-id", gid),
						zap.String("detail", string(parterPayload)),
						zap.Error(err),
					)
					return err
				}
			}

			return nil
		}

		if err := t.WithClient(types.ID(m.From), hnd); err != nil {
			rs.Logger.Error("[raftServer] snapshot parter sync failed",
				zap.Uint64("group-id", gid),
				zap.Error(err),
			)
			return err
		}

	}

	return nil
}
