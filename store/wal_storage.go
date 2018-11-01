package store

import (
	"io"

	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"go.etcd.io/etcd/wal/walpb"

	"go.uber.org/zap"
)

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
	if err := st.WAL.ReleaseLockTo(snap.Metadata.Index); err != nil {
		return err
	}
	return nil
}

func (st *storage) CreateSnapshot(snapi uint64, cs *raftpb.ConfState, data []byte) (snap raftpb.Snapshot, err error) {
	snap, err = st.MemoryStorage.CreateSnapshot(snapi, cs, data)
	if err != nil {
		// the snapshot was done asynchronously with the progress of raft.
		// raft might have already got a newer snapshot.
		if err == raft.ErrSnapOutOfDate {
			return
		}
	}
	return
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

// storage initialize functions
func NewStorage(lg *zap.Logger, waldir string, ss *snap.Snapshotter) *storage {
	w, err := wal.Create(lg, waldir, nil)
	if err != nil {
		lg.Fatal("[storage] create wal directory error",
			zap.String("wal-directory", waldir),
			zap.Error(err),
		)
	}
	ms := raft.NewMemoryStorage()

	return newStorage(ms, w, ss)
}

func RestartStorage(lg *zap.Logger, waldir string, snapshot *raftpb.Snapshot, ss *snap.Snapshotter) *storage {
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

	return newStorage(ms, w, ss)
}

func haveWALBackend(dir string) bool {
	return wal.Exist(dir)
}
