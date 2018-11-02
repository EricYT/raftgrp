package store

import (
	"github.com/EricYT/raftgrp/store/leveldb"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
)

var _ Storage = (*leveldbStoreage)(nil)

type leveldbStoreage struct {
	*leveldb.LeveldbBackend
}

func NewStorageLeveldb(lg *zap.Logger, logdir, snapdir string) *leveldbStoreage {
	s, err := leveldb.NewLevelDBBackend(lg, logdir, snapdir)
	if err != nil {
		lg.Fatal("[NewStorageLeveldb] start leveldb backend failed",
			zap.String("log-directory", logdir),
			zap.String("snap-directory", snapdir),
			zap.Error(err),
		)
	}
	ls := &leveldbStoreage{
		LeveldbBackend: s,
	}
	return ls
}

func (ls *leveldbStoreage) CreateSnapshot(snapi uint64, cs *raftpb.ConfState, data []byte) (snap raftpb.Snapshot, err error) {
	snap, err = ls.LeveldbBackend.CreateSnapshot(snapi, cs, data)
	if err != nil {
		if err == raft.ErrSnapOutOfDate {
			return
		}
	}
	return
}

// NOTICE: If there is a snapshot we should replay ents again.
func (ls *leveldbStoreage) Append(snap raftpb.Snapshot, ents []raftpb.Entry) (err error) {
	if raft.IsEmptySnap(snap) {
		return
	}
	return ls.LeveldbBackend.AppendEntries(ents)
}

func haveLevelDbBackend(dir string) bool {
	return leveldb.Exist(dir)
}
