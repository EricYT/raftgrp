package store

import (
	"io"

	"github.com/coreos/etcd/etcdserver/api/snap"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/wal/walpb"

	"go.uber.org/zap"
)

type Storage interface {
	// for raft instance
	raft.Storage

	// memory storage interfaces
	//ApplySnapshot overwrites the contents of this Storage object with
	//those of then given snapshot
	ApplySnapshot(snap raftpb.Snapshot) error
	// Append the new entries to storage.
	Append(entries []raftpb.Entry) error

	// Save function saves ents and state to the underlying stable storage.
	// Save MUST block until st and ents are on stable storage.
	Save(st raftpb.HardState, ents []raftpb.Entry) error
	// SaveSnap function saves snapshot to the underlying stable storage.
	SaveSnap(snap raftpb.Snapshot) error
	// Close closes the Storage and performs finalization.
	Close() error
}

type storage struct {
	*raft.MemoryStorage

	*wal.WAL
	*snap.Snapshotter
}

func newStorage(m *raft.MemoryStorage, w *wal.WAL, s *snap.Snapshotter) *storage {
	return &storage{m, w, s}
}

// SaveSnap saves the snapshot to disk and release the locked
// wal files since they will not be used.
func (st *storage) SaveSnap(snap raftpb.Snapshot) error {
	walsnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	err := st.WAL.SaveSnapshot(walsnap)
	if err != nil {
		return err
	}
	err = st.Snapshotter.SaveSnap(snap)
	if err != nil {
		return err
	}
	return st.WAL.ReleaseLockTo(snap.Metadata.Index)
}

func readWAL(lg *zap.Logger, waldir string, snap walpb.Snapshot) (w *wal.WAL, st raftpb.HardState, ents []raftpb.Entry) {
	var err error

	repaired := false
	for {
		if w, err = wal.Open(lg, waldir, snap); err != nil {
			lg.Fatal("failed to open WAL", zap.Error(err))
		}
		if _, st, ents, err = w.ReadAll(); err != nil {
			w.Close()
			// we can only repair ErrUnexpectedEOF and we never repair twice.
			if repaired || err != io.ErrUnexpectedEOF {
				lg.Fatal("failed to read WAL, cannot be repaired", zap.Error(err))
			}
			if !wal.Repair(lg, waldir) {
				lg.Fatal("failed to repair WAL", zap.Error(err))
			} else {
				lg.Info("repaired WAL", zap.Error(err))
				repaired = true
			}
			continue
		}
		break
	}
	return w, st, ents
}

func ensureWAL(lg *zap.Logger, waldir string) bool {
	if !wal.Exist(waldir) {
		w, err := wal.Create(lg, waldir, nil)
		if err != nil {
			lg.Panic("[storage] create wal directory error ",
				zap.String("wal-directory", waldir),
				zap.Error(err),
			)
		}
		w.Close()
		return true
	}
	return false
}

// storage initialize functions
func NewStorage(lg *zap.Logger, waldir string, snapshot *raftpb.Snapshot, ss *snap.Snapshotter) (*storage, bool) {
	isNew := ensureWAL(lg, waldir)
	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	w, st, ents := readWAL(lg, waldir, walsnap)
	ms := raft.NewMemoryStorage()
	if snapshot != nil {
		ms.ApplySnapshot(*snapshot)
	}
	ms.SetHardState(st)
	ms.Append(ents)

	return newStorage(ms, w, ss), isNew
}
