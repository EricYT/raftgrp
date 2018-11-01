package store

import (
	"fmt"

	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

type Storage interface {
	// for raft instance
	raft.Storage

	// memory storage interfaces
	//ApplySnapshot overwrites the contents of this Storage object with
	//those of then given snapshot
	ApplySnapshot(snap raftpb.Snapshot) error
	Append(entries []raftpb.Entry) error
	CreateSnapshot(snapi uint64, cs *raftpb.ConfState, data []byte) (snap raftpb.Snapshot, err error)
	Compact(compacti uint64) error

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

const (
	StorageTypeWAL StorageType = iota
	StorageTypeLevelDB
)

func HaveStorage(typ StorageType, dir string) bool {
	switch typ {
	case StorageTypeWAL:
		return haveWALBackend(dir)
	case StorageTypeLevelDB:
		return haveLevelDbBackend(dir)
	default:
		panic(fmt.Sprintf("[store] unknow storage type %v", typ))
	}
}
