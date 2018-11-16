package transport

import (
	"bytes"
	"context"
	"io"
	"sync"
	"time"

	"github.com/EricYT/raftgrp/proto"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	// FIXME: 4096 for Etcd, but we have a lot raft instances
	streamBufSize int = 4096

	// FIXME: 512Kb
	bufSize int = 1 * 1024
)

type msgWrapper struct {
	groupID uint64
	localID types.ID
	peerID  types.ID
	msg     raftpb.Message
}

type streamWriter struct {
	lg *zap.Logger

	addr    string // remote server grpc address
	ctx     context.Context
	cancel  context.CancelFunc
	conn    *grpc.ClientConn
	bufPool *sync.Pool

	msgc chan msgWrapper // for raft message

	stopc chan struct{}
	donec chan struct{}
}

func startStreamWriter(lg *zap.Logger, addr string) (*streamWriter, error) {
	// Dial remote peer server
	// This is non-blocking operation
	//grpc.WithBlock(), // The default connection will create non-blocking
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBackoffMaxDelay(time.Second*1))
	if err != nil {
		lg.Error("[streamWriter] failed to dial remote peer",
			zap.String("address", addr),
			zap.Error(err),
		)
		return nil, errors.Wrapf(err, "[streamWriter] failed to dial remote server(%s)", addr)
	}

	s := &streamWriter{
		lg:    lg,
		addr:  addr,
		conn:  conn,
		msgc:  make(chan msgWrapper, streamBufSize),
		stopc: make(chan struct{}),
		donec: make(chan struct{}),
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.bufPool = &sync.Pool{
		New: func() interface{} {
			return make([]byte, bufSize)
		},
	}
	go s.run()
	return s, nil
}

func (s *streamWriter) run() {
	s.lg.Info("[streamWriter] started stream writer with remote address",
		zap.String("remote-address", s.addr),
	)

	msgc := s.msgc
	for {
		select {
		case mm := <-msgc:
			s.send(&mm)
		case <-s.stopc:
			s.lg.Warn("[streamWriter] stream writer stop",
				zap.String("peer-addr", s.addr),
			)
			// TODO: stop connections if no one use it
			close(s.donec)
			return
		}
	}
}

func (s *streamWriter) send(mw *msgWrapper) {
	c := proto.NewRaftGrouperClient(s.conn)

	rc, err := c.Send(s.ctx, grpc.EmptyCallOption{})
	if err != nil {
		s.lg.Error("[streamWriter] create a stream client to peer failed",
			zap.String("peer-addr", s.addr),
			zap.Error(err),
		)
		return
	}

	data, err := mw.msg.Marshal()
	if err != nil {
		s.lg.Error("[streamWriter] marshal raft message error",
			zap.Error(err),
		)
		return
	}
	dataReader := bytes.NewReader(data)

	// send message matadata firstly
	meta := &proto.SendRequest{
		Msg: &proto.SendRequest_Meta{
			Meta: &proto.Metadata{
				GroupId: int64(mw.groupID),
			},
		},
	}
	if err := rc.Send(meta); err != nil {
		s.lg.Warn("[streamWriter] failed to send metadata message",
			zap.String("peer-addr", s.addr),
			zap.Error(err),
		)
		rc.CloseSend()
		return
	}

	// TODO: send message payload 1m per slice
	// FIXME: should we reset the buffer and put it backup to pool ? (no necessary, we got a 'n')
	buf := s.bufPool.Get().([]byte)
	defer s.bufPool.Put(buf)

	for {
		n, err := dataReader.Read(buf)
		if n > 0 {
			message := &proto.SendRequest{
				Msg: &proto.SendRequest_Payload{
					Payload: &proto.Playload{
						Data: buf[:n],
					},
				},
			}
			if err := rc.Send(message); err != nil {
				s.lg.Error("[streamWriter] failed to send data message",
					zap.String("peer-addr", s.addr),
					zap.Error(err),
				)
				rc.CloseSend()
				return
			}
		}
		if err != nil {
			if err != io.EOF {
				s.lg.Error("[streamWriter] read buffer failed",
					zap.Error(err),
				)
				rc.CloseSend()
				return
			}
			break
		}
	}

	// FIXME: we don't care about the result from remote.
	reply, err := rc.CloseAndRecv()
	if err != nil {
		s.lg.Error("[streamWriter] close and recv failed",
			zap.String("peer-addr", s.addr),
			zap.Error(err),
		)
		return
	}
	if reply.Ack.Code != 0 {
		s.lg.Error("[streamWriter] remote operate message failed",
			zap.String("peer-addr", s.addr),
			zap.Int64("code", reply.Ack.Code),
			zap.String("detail", reply.GetAck().GetDetail()),
		)
	}
}

func (s *streamWriter) writec() chan<- msgWrapper {
	return s.msgc
}

func (s *streamWriter) stop() {
	s.cancel()
	close(s.stopc)
}
