package store

import (
	"errors"
	"fmt"
	"path"

	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
)

var (
	snapDirPrefix = "snap"
	logDirPrefix  = "log"
)

var (
	ErrUnknowStorageType error = errors.New("[store] unknow storage type")
)

type Storage interface {
	// for raft instance
	raft.Storage

	// memory storage interfaces
	//ApplySnapshot overwrites the contents of this Storage object with
	//those of then given snapshot
	ApplySnapshot(snap raftpb.Snapshot) error
	CreateSnapshot(snapi uint64, cs *raftpb.ConfState, data []byte) (snap raftpb.Snapshot, err error)
	Compact(compacti uint64) error
	Append(snap raftpb.Snapshot, entries []raftpb.Entry) error

	// Save function saves ents and state to the underlying stable storage.
	// Save MUST block until st and ents are on stable storage.
	Save(st raftpb.HardState, ents []raftpb.Entry) error
	// SaveSnap function saves snapshot to the underlying stable storage.
	SaveSnap(snap raftpb.Snapshot) error
	// Close closes the Storage and performs finalization.
	Close() error
}

type StorageType int

func (s StorageType) String() string {
	switch s {
	case StorageTypeWAL:
		return "storage_wal"
	case StorageTypeLevelDB:
		return "storage_leveldb"
	default:
		return "unknow"
	}
}

func (s StorageType) Validate() (err error) {
	switch s {
	case StorageTypeWAL:
	case StorageTypeLevelDB:
	default:
		return ErrUnknowStorageType
	}
	return
}

const (
	StorageTypeWAL StorageType = iota
	StorageTypeLevelDB
)

func HaveStorage(typ StorageType, dir string) bool {
	switch typ {
	case StorageTypeWAL:
		return haveWALBackend(genLogDirPath(dir))
	case StorageTypeLevelDB:
		return haveLevelDbBackend(genLogDirPath(dir))
	default:
		panic(fmt.Sprintf("[store] unknow storage type %v", typ))
	}
}

func NewStorageByType(lg *zap.Logger, typ StorageType, dir string) (s Storage) {
	if err := typ.Validate(); err != nil {
		lg.Fatal("[store] new storage",
			zap.String("storage-type", typ.String()),
			zap.String("storage-directory", dir),
			zap.Error(err),
		)
	}
	switch typ {
	case StorageTypeWAL:
		s = NewWALStorage(lg, genLogDirPath(dir), genSnapDirPath(dir))
	case StorageTypeLevelDB:
		s = NewStorageLeveldb(lg, genLogDirPath(dir), genSnapDirPath(dir))
	}
	return
}

func genLogDirPath(dir string) string {
	return path.Join(dir, logDirPrefix)
}

func genSnapDirPath(dir string) string {
	return path.Join(dir, snapDirPrefix)
}
