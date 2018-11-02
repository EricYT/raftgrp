package store

import (
	"io"

	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"go.etcd.io/etcd/wal/walpb"

	"go.uber.org/zap"
)

type walStorage struct {
	*raft.MemoryStorage
	*wal.WAL
	*snap.Snapshotter
}

func newWALStorage(m *raft.MemoryStorage, w *wal.WAL, s *snap.Snapshotter) *walStorage {
	return &walStorage{m, w, s}
}

// SaveSnap saves the snapshot to disk and release the locked
// wal files since they will not be used.
func (st *walStorage) SaveSnap(snap raftpb.Snapshot) error {
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

func (st *walStorage) CreateSnapshot(snapi uint64, cs *raftpb.ConfState, data []byte) (snap raftpb.Snapshot, err error) {
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

// NOTICE: WAL just replay the ents into memory storage
func (st *walStorage) Append(snap raftpb.Snapshot, ents []raftpb.Entry) (err error) {
	return st.MemoryStorage.Append(ents)
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

func ensureWAL(lg *zap.Logger, waldir string) {
	if !wal.Exist(waldir) {
		// FIXME: Maybe we can put something into the wal metadata.
		w, err := wal.Create(lg, waldir, nil)
		if err != nil {
			lg.Fatal("[walStorage] create wal directory error",
				zap.String("wal-directory", waldir),
				zap.Error(err),
			)
		}
		w.Close()
	}
}

// storage initialize functions
func NewWALStorage(lg *zap.Logger, waldir, snapdir string) *walStorage {
	ensureWAL(lg, waldir)

	// initialize snapshot dir
	if err := fileutil.TouchDirAll(snapdir); err != nil {
		lg.Fatal("[store] touch snapshot dir failed.",
			zap.String("snap-dir", snapdir),
			zap.Error(err),
		)
	}

	ss := snap.New(lg, snapdir)
	snapshot, err := ss.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		lg.Fatal("[store] load snapshot failed.",
			zap.String("snap-dir", snapdir),
			zap.Error(err),
		)
	}

	// memory storage
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

	return newWALStorage(ms, w, ss)
}

func haveWALBackend(dir string) bool {
	return wal.Exist(dir)
}
