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

// transport server
type transportServer struct {
	Logger *zap.Logger

	addr string
	s    *grpc.Server

	rs *raftServer // raft grpc service server
}

func newTransportServer(lg *zap.Logger, tm *TransportManager, addr string) *transportServer {
	ts := &transportServer{
		Logger: lg,
		addr:   addr,
		rs: &raftServer{
			Logger: lg,
			tm:     tm,
		},
	}
	go ts.run()
	return ts
}

func (ts *transportServer) run() {
	lis, err := net.Listen("tcp", ts.addr)
	if err != nil {
		ts.Logger.Fatal("[transportServer] listen failed",
			zap.String("address", ts.addr),
			zap.Error(err),
		)
	}
	ts.s = grpc.NewServer()

	// register raft group server
	proto.RegisterRaftGrouperServer(ts.s, ts.rs)

	// blocking
	if err := ts.s.Serve(lis); err != nil {
		ts.Logger.Error("[transportServer] serve failed",
			zap.String("address", ts.addr),
			zap.Error(err),
		)
	}
}

func (ts *transportServer) close() {
	ts.s.Stop()
}

type raftServer struct {
	Logger *zap.Logger

	tm *TransportManager
}

// receive message from others
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
	m := raftpb.Message{}
	if err = m.Unmarshal(payload.Bytes()); err != nil {
		return err
	}

	// dispatcher message to transport
	t, ok := rs.tm.GetTransport(gid)
	if !ok {
		rs.Logger.Error("[raftServer] fetch group transport not found",
			zap.Uint64("group-id", gid),
		)
		return nil
	}

	// TODO: snapshot sync snapshot here
	if m.Type == raftpb.MsgSnap && !raft.IsEmptySnap(m.Snapshot) {
		if err := rs.syncSnapshot(t, gid, m); err != nil {
			rs.Logger.Error("[raftServer] sync snapshot from leader failed",
				zap.Uint64("group-id", gid),
				zap.Error(err),
			)
			return err
		}
	}

	t.Process(m)

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

	t, ok := rs.tm.GetTransport(gid)
	if !ok {
		rs.Logger.Error("[raftServer] fetch group transport not found",
			zap.Uint64("group-id", gid),
		)
		return nil
	}

	parter, err := t.UnmarshalSnapshotParter(req.GetParter())
	if err != nil {
		rs.Logger.Error("[raftServer] unmarshal snapshot parter failed.",
			append(gidFields, zap.Error(err))...,
		)
		return err
	}

	rs.Logger.Debug("[raftServer] snapshot pater",
		append(gidFields, zap.Any("metadata", parter.Metadata()))...,
	)

	// create snapshot part reader
	reader, err := parter.OpenReader()
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
func (rs *raftServer) syncSnapshot(t Transport, gid uint64, m raftpb.Message) (err error) {
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

	snapWriter, err := t.UnmarshalSnapshotWriter(snapMeta.UserStateMachineMeta)
	if err != nil {
		rs.Logger.Error("[raftServer] unmarshal snapshot writer failed.",
			append(gidFields, zap.Error(err))...)
		return err
	}

	rs.Logger.Debug("[raftServer] snapshot reader",
		zap.Uint64("group-id", gid),
		zap.String("snap-id", snapWriter.GetID()),
		zap.Any("metadata", snapWriter.Metadata()),
	)

	// create sanp reader client
	addr, err := t.getPeerAddr(types.ID(m.From))
	if err != nil {
		rs.Logger.Error("[raftServer] get peer addr failed",
			zap.Uint64("peer-id", m.From),
			zap.Error(err),
		)
		return err
	}

	c, err := rs.tm.c.pick(addr)
	if err != nil {
		rs.Logger.Error("[raftServer] pick grpc conn failed",
			zap.String("addr", addr),
			zap.Error(err),
		)
		return err
	}

	cc, err := c.rawRaftGrouperConn()
	if err != nil {
		rs.Logger.Error("[raftServer] pick grpc conn failed",
			zap.String("addr", addr),
			zap.Error(err),
		)
		return err
	}
	defer cc.Close()
	client := proto.NewRaftGrouperClient(cc)

	// read snapshot
	parters := snapWriter.Next()
	for parter := range parters {
		if err := syncSnapshotParter(rs.Logger, client, gid, parter); err != nil {
			rs.Logger.Error("[raftServer] faield to sync snapshot parter",
				zap.Uint64("group-id", gid),
				zap.Error(err),
			)
			return err
		}
	}

	return nil
}

func syncSnapshotParter(lg *zap.Logger, client proto.RaftGrouperClient, gid uint64, parter SnapshotParter) error {
	// Encoding parter for send it back to sender
	parterPayload, err := parter.Marshal()
	if err != nil {
		lg.Error("[raftServer] marshal pater failed",
			zap.Uint64("group-id", gid),
			zap.Error(err),
		)
		return err
	}

	lg.Info("[raftServer] snapshot pater",
		zap.Uint64("group-id", gid),
		zap.String("detail", string(parterPayload)),
	)

	// Be careful to close the writer
	partWriter, err := parter.OpenWriter()
	if err != nil {
		lg.Error("[raftServer] snapshot create part writer failed.",
			zap.Uint64("group-id", gid),
			zap.Error(err),
		)
		return err
	}
	defer partWriter.Close()

	req := &proto.SnapshotReaderRequest{
		GroupId: int64(gid),
		Parter:  parterPayload,
	}

	stream, err := client.SnapshotRead(context.TODO(), req, grpc.EmptyCallOption{})
	if err != nil {
		lg.Error("[raftServer] snapshot read failed.",
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
				lg.Error("[raftServer] snapshot recv parter failed.",
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
			lg.Error("[raftServer] snapshot write parter failed.",
				zap.Uint64("group-id", gid),
				zap.String("detail", string(parterPayload)),
				zap.Error(err),
			)
			return err
		}
	}

	return nil
}
