package store

import (
	"github.com/EricYT/raftgrp/store/backend"
	"go.uber.org/zap"
)

var _ Storage = (*leveldbStoreage)(nil)

type leveldbStoreage struct {
	Storage
}

func NewStorageLeveldb(lg *zap.Logger, dir string) *leveldbStoreage {
	s, err := backend.NewLevelDBBackend(lg, dir)
	if err != nil {
		lg.Fatal("[NewStorageLeveldb] start leveldb backend failed",
			zap.String("directory", dir),
			zap.Error(err),
		)
	}
	ls := &leveldbStoreage{
		Storage: s,
	}
	return ls
}

func haveLevelDbBackend(dir string) bool {
	return backend.Exist(backend.BackendTypeLevelDB, dir)
}
