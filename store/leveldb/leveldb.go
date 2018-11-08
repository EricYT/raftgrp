package leveldb

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
)

// using leveldb to store raft logs

// not the raft log entry
var ErrNotEntry error = errors.New("[storeage beackend] not raft log entry")

// parse index error
var ErrParseIndex error = errors.New("[storage backend] parse index failed")

// Special keys
var (
	snapshotDirPrefix = "snap"
	leveldbDirPrefix  = "log"

	hardStatePrefix  = []byte("HardStatePrefix")
	entryPrefix      = []byte("IndexPrefix")
	entryCountPrefix = []byte("CountIndexPrefix")
	dumpEntryPrefix  = []byte("DumpEntryPrefix")
)

/* A copy of raft Storage interface
type Storage interface {
	// InitialState returns the saved HardState and ConfState information.
	InitialState() (raftpb.HardState, raftpb.ConfState, error)
	// Entries returns a slice of log entries in the range [lo,hi).
	// MaxSize limits the total size of the log entries returned, but
	// Entries returns at least one entry if any.
	Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error)
	// Term returns the term of entry i, which must be in the range
	// [FirstIndex()-1, LastIndex()]. The term of the entry before
	// FirstIndex is retained for matching purposes even though the
	// rest of that entry may not be available.
	Term(i uint64) (uint64, error)
	// LastIndex returns the index of the last entry in the log.
	LastIndex() (uint64, error)
	// FirstIndex returns the index of the first log entry that is
	// possibly available via Entries (older entries have been incorporated
	// into the latest Snapshot; if storage only contains the dummy entry the
	// first log entry is not available).
	FirstIndex() (uint64, error)
	// Snapshot returns the most recent snapshot.
	// If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
	// so raft state machine could know that Storage needs some time to prepare
	// snapshot and call Snapshot later.
	Snapshot() (raftpb.Snapshot, error)
}
*/

var _ raft.Storage = (*LeveldbBackend)(nil)

// FIXME: What should we do for the recently entries have written,
// Use a application LRU cache to store it, or handle it by the
// backend leveldb block cache.

type LeveldbBackend struct {
	lg      *zap.Logger
	logdir  string
	snapdir string

	mu sync.Mutex

	ss *snap.Snapshotter // seperate the store of snapshot and logs, it's a problem chook and egg.
	db *leveldb.DB

	snap  raftpb.Snapshot
	state raftpb.HardState

	index uint64 // index of the dummy entry
	term  uint64 // term of the dummy entry
	count uint64 // the count current entries already written including the dummy entry
}

func NewLevelDBBackend(lg *zap.Logger, logdir, snapdir string) (*LeveldbBackend, error) {
	// reloading snapshot firstly
	if err := fileutil.TouchDirAll(snapdir); err != nil {
		return nil, errors.Wrapf(err, "[NewLevelDBBackend] touch snapshot directory %s failed.", snapdir)
	}
	ss := snap.New(lg, snapdir)
	snapshot, err := ss.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		return nil, errors.Wrapf(err, "[NewLevelDBBackend] load snapshot %s failed.", snapdir)
	}

	// using snapshot to detect db directory
	lg.Info("[NewLevelDBBackend]",
		zap.String("log-dir", logdir),
		zap.String("snap-dir", snapdir),
	)
	db, err := leveldb.OpenFile(logdir, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "[NewLevelDBBackend] open db file %s failed.", logdir)
	}

	lb := &LeveldbBackend{
		lg:      lg,
		logdir:  logdir,
		snapdir: snapdir,
		ss:      ss,
		db:      db,
	}

	if snapshot != nil {
		lb.snap = *snapshot
	}

	if err := lb.initializeState(); err != nil {
		db.Close()
		return nil, errors.Wrap(err, "[NewLevelDBBackend] initialize state failed")
	}

	return lb, nil
}

func (lb *LeveldbBackend) initializeState() error {
	// hard state
	hs, err := lb.db.Get(hardStatePrefix, nil)
	if err != nil {
		if err != leveldb.ErrNotFound {
			return err
		}
	}
	if err == nil {
		s := &lb.state
		if err := s.Unmarshal(hs); err != nil {
			return err
		}
	}

	// dump entry
	if !raft.IsEmptySnap(lb.snap) {
		lb.index = lb.snap.Metadata.Index
		lb.term = lb.snap.Metadata.Term
	}
	dumpe, err := lb.db.Get(dumpEntryPrefix, nil)
	if err != nil {
		if err != leveldb.ErrNotFound {
			return err
		}
	}
	if err == nil {
		e := &raftpb.Entry{}
		if err := e.Unmarshal(dumpe); err != nil {
			return err
		}
		lb.index = e.Index
		lb.term = e.Term
	}

	// count
	count, err := lb.db.Get(entryCountPrefix, nil)
	if err != nil {
		if err != leveldb.ErrNotFound {
			return err
		}
	}
	if err == nil {
		lb.count, err = strconv.ParseUint(string(count), 10, 64)
		if err != nil {
			return err
		}
	} else {
		// including the dummp entry
		lb.count = 1
	}

	return nil
}

// query methods for raft instance
func (lb *LeveldbBackend) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return lb.state, lb.snap.Metadata.ConfState, nil
}

func (lb *LeveldbBackend) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	lb.lg.Debug("[LeveldbBackend] fetch entries",
		zap.Uint64("dummy-index", lb.index),
		zap.Uint64("dummy-term", lb.index),
		zap.Uint64("count", lb.count),
		zap.Uint64("low", lo),
		zap.Uint64("high", hi),
		zap.Uint64("max-size", maxSize),
	)

	offset := lb.index
	if lo <= offset {
		return nil, raft.ErrCompacted
	}
	if hi > lb.lastIndex()+1 {
		lb.lg.Fatal("[LeveldbBackend] hi is out of bound.",
			zap.String("leveldb-dir", lb.logdir),
			zap.Uint64("index", lb.index),
			zap.Uint64("term", lb.term),
			zap.Uint64("count", lb.count),
			zap.Uint64("low", lo),
			zap.Uint64("high", hi),
		)
	}

	// only contains the dummy entries.
	if lb.count == 1 {
		return nil, raft.ErrUnavailable
	}

	ents, err := lb.getEntries(lo, hi)
	if err != nil {
		return nil, err
	}

	lb.lg.Debug("[LeveldbBackend] fetch from db",
		zap.Uint64("dummy-index", lb.index),
		zap.Uint64("dummy-term", lb.index),
		zap.Uint64("count", lb.count),
		zap.Uint64("low", lo),
		zap.Uint64("high", hi),
		zap.Uint64("max-size", maxSize),
		zap.Any("entries", ents),
	)

	return limitSize(ents, maxSize), nil
}

func limitSize(ents []raftpb.Entry, maxsize uint64) []raftpb.Entry {
	if len(ents) == 0 {
		return ents
	}
	size := ents[0].Size()
	var limit int
	for limit = 1; limit < len(ents); limit++ {
		size += ents[limit].Size()
		if uint64(size) > maxsize {
			break
		}
	}
	return ents[:limit]
}

func (lb *LeveldbBackend) Term(i uint64) (uint64, error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return lb.getTerm(i)
}

func (lb *LeveldbBackend) getTerm(i uint64) (uint64, error) {
	offset := lb.index
	if i < offset {
		return 0, raft.ErrCompacted
	}
	if (i - offset) >= lb.count {
		return 0, raft.ErrUnavailable
	}

	if i == lb.index {
		return lb.term, nil
	}

	e, err := lb.getEntry(i)
	if err != nil {
		return 0, err
	}

	return e.Term, nil
}

func (lb *LeveldbBackend) getEntry(i uint64) (*raftpb.Entry, error) {
	ikey := lb.encodeEntryKey(i)
	val, err := lb.db.Get(ikey, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "[LeveldbBackend] gen entry by index %d key %s error", i, ikey)
	}

	var e raftpb.Entry
	if err := e.Unmarshal(val); err != nil {
		return nil, errors.Wrapf(err, "[LeveldbBackend] unmarhsal entry by index %d key %s error", i, ikey)
	}

	return &e, nil
}

func (lb *LeveldbBackend) encodeEntryKey(i uint64) []byte {
	// NOTICE: padding zero to make iter keys by it's index.
	return []byte(fmt.Sprintf("%s#%020d", entryPrefix, i))
}

func (lb *LeveldbBackend) decodeEntryKey(key []byte) (uint64, error) {
	if !bytes.HasPrefix(key, entryPrefix) {
		return 0, ErrNotEntry
	}
	vals := bytes.Split(key, []byte("#"))
	index, err := strconv.ParseUint(string(vals[1]), 10, 64)
	if err != nil {
		return 0, ErrParseIndex
	}
	return index, nil
}

// returns a slice of logs in the range [lo, hi)
func (lb *LeveldbBackend) getEntries(lo, hi uint64) ([]raftpb.Entry, error) {
	iter := lb.db.NewIterator(nil, nil)
	defer iter.Release()

	lb.lg.Debug("[LeveldbBackend] get entries",
		zap.Uint64("index-low", lo),
		zap.Uint64("index-high", hi),
	)

	ents := make([]raftpb.Entry, 0, hi-lo)
	for ok := iter.Seek(lb.encodeEntryKey(lo)); ok; ok = iter.Next() {

		key := iter.Key()
		i, err := lb.decodeEntryKey(key)
		if err != nil {
			if err == ErrNotEntry {
				continue
			}
			return nil, err
		}
		if i >= hi {
			break
		}

		lb.lg.Debug("[LeveldbBackend] iter entries",
			zap.String("key", string(iter.Key())),
			zap.String("val", string(iter.Value())),
		)

		var e raftpb.Entry
		if err := e.Unmarshal(iter.Value()); err != nil {
			return nil, err
		}
		ents = append(ents, e)
	}

	err := iter.Error()
	return ents, err
}

func (lb *LeveldbBackend) LastIndex() (uint64, error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return lb.lastIndex(), nil
}

func (lb *LeveldbBackend) lastIndex() uint64 {
	return lb.index + lb.count - 1
}

func (lb *LeveldbBackend) FirstIndex() (uint64, error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return lb.firstIndex(), nil
}

func (lb *LeveldbBackend) firstIndex() uint64 {
	return lb.index + 1
}

func (lb *LeveldbBackend) Snapshot() (raftpb.Snapshot, error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return lb.snap, nil
}

// backend persist methods

// AppendEntries try to replay entries again when there is a remote snapshot
// have to operate.
func (lb *LeveldbBackend) AppendEntries(ents []raftpb.Entry) error {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return lb.saveEntries(ents)
}

// Original method Compact discards all log entries prior to compactIndex.
// FIXME: just pushing the index forward
func (lb *LeveldbBackend) Compact(compactIndex uint64) error {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	offset := lb.index
	if compactIndex <= offset {
		return raft.ErrCompacted
	}

	if compactIndex > lb.lastIndex() {
		lb.lg.Fatal("[LeveldbBackend] compact out of index bound",
			zap.Uint64("compact-index", compactIndex),
			zap.Uint64("last-index", lb.lastIndex()),
		)
	}

	term, err := lb.getTerm(compactIndex)
	if err != nil {
		return err
	}

	newcount := lb.count - (compactIndex - lb.index)

	// modify persist layer
	dummye := &raftpb.Entry{
		Term:  term,
		Index: compactIndex,
	}
	data, err := dummye.Marshal()
	if err != nil {
		return err
	}

	batch := new(leveldb.Batch)
	batch.Put(dumpEntryPrefix, data)
	batch.Put(entryCountPrefix, []byte(fmt.Sprintf("%d", newcount)))
	if err := lb.db.Write(batch, nil); err != nil {
		return err
	}

	lb.index = compactIndex
	lb.term = term
	lb.count = newcount

	return nil
}

// Write entries, hard state in order.
func (lb *LeveldbBackend) Save(st raftpb.HardState, ents []raftpb.Entry) error {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if err := lb.saveEntries(ents); err != nil {
		lb.lg.Error("[LeveldbBackend] save entries failed.",
			zap.Error(err),
		)
		return err
	}

	if err := lb.saveHardState(&st); err != nil {
		lb.lg.Error("[LeveldbBackend] save hard state failed.",
			zap.Error(err),
		)
		return err
	}

	return nil
}

func (lb *LeveldbBackend) saveEntries(ents []raftpb.Entry) error {
	if len(ents) == 0 {
		return nil
	}

	first := lb.firstIndex()
	last := ents[0].Index + uint64(len(ents)) - 1

	// short cut if there is no new entry.
	if last < first {
		return nil
	}
	// truncate compacted entries
	if first > ents[0].Index {
		ents = ents[first-ents[0].Index:]
	}

	// persist entries batch
	batch := new(leveldb.Batch)
	for i := range ents {
		ikey := lb.encodeEntryKey(ents[i].Index)
		val, err := ents[i].Marshal()
		if err != nil {
			return errors.Wrapf(err, "[LeveldbBackend] marshal entry %d failed", ents[i].Index)
		}
		lb.lg.Debug("[LeveldbBackend] save entry",
			zap.Uint64("index", ents[i].Index),
			zap.Uint64("term", ents[i].Term),
			zap.String("index", string(ikey)),
		)
		batch.Put(ikey, val)
	}
	offset := ents[0].Index - lb.index
	count := fmt.Sprintf("%d", offset+uint64(len(ents)))
	batch.Put(entryCountPrefix, []byte(count))

	if err := lb.db.Write(batch, nil); err != nil {
		return errors.Wrap(err, "[LeveldbBackend] batch write entries failed")
	}

	lb.count = offset + uint64(len(ents))

	return nil
}

func (lb *LeveldbBackend) saveHardState(st *raftpb.HardState) error {
	if raft.IsEmptyHardState(*st) {
		return nil
	}

	data, err := st.Marshal()
	if err != nil {
		lb.lg.Error("[LeveldbBackend] marshal hard state failed.",
			zap.Any("hard-state", st),
			zap.Error(err),
		)
		return err
	}
	if err := lb.db.Put(hardStatePrefix, data, nil); err != nil {
		lb.lg.Error("[LeveldbBackend] save hard state failed.",
			zap.Any("hard-state", st),
			zap.Error(err),
		)
		return err
	}

	lb.state = *st

	return nil
}

// create a snapshot, but we didn't modify the LeveldbBackend state right now
func (lb *LeveldbBackend) CreateSnapshot(i uint64, cs *raftpb.ConfState, data []byte) (raftpb.Snapshot, error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if i <= lb.snap.Metadata.Index {
		return raftpb.Snapshot{}, raft.ErrSnapOutOfDate
	}

	if i > lb.lastIndex() {
		lb.lg.Fatal("[LeveldbBackend] snapshot is out of bound",
			zap.Uint64("snapshot-index", i),
			zap.Uint64("last-index", lb.lastIndex()),
		)
	}

	term, err := lb.getTerm(i)
	if err != nil {
		return raftpb.Snapshot{}, err
	}

	lb.snap.Data = data
	lb.snap.Metadata.Term = term
	lb.snap.Metadata.Index = i

	if cs != nil {
		lb.snap.Metadata.ConfState = *cs
	}

	return lb.snap, nil
}

// just save a snapshot time-of-point
func (lb *LeveldbBackend) SaveSnap(snap raftpb.Snapshot) error {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	// save snapshot
	if err := lb.ss.SaveSnap(snap); err != nil {
		return err
	}

	return nil
}

// install remote snapshot and discard all entries before.
func (lb *LeveldbBackend) ApplySnapshot(snap raftpb.Snapshot) error {
	// FIXME: We must to discard all entries before the snapshot.
	// The easy way it to generate a new leveldb instance by the specific snap.Metadata.Term
	// and snap.Metadata.Index, but I think its too heavy to do that.
	// I seperate the key by this right now, but it's difficult to clean up all the entries
	// before.

	lb.mu.Lock()
	defer lb.mu.Unlock()

	if lb.snap.Metadata.Index >= snap.Metadata.Index {
		return raft.ErrSnapOutOfDate
	}

	// close current leveldb
	if err := lb.db.Close(); err != nil {
		lb.lg.Fatal("[LeveldbBackend] save snapshot close leveldb failed.",
			zap.Error(err),
		)
	}

	// backup the old logs
	backupDbDir := fmt.Sprintf("%s-%d-%d-backup", lb.logdir, lb.snap.Metadata.Term, lb.snap.Metadata.Index)
	if err := os.Rename(lb.logdir, backupDbDir); err != nil {
		lb.lg.Fatal("[LeveldbBackend] backup leveldb failed.",
			zap.Uint64("term", lb.snap.Metadata.Term),
			zap.Uint64("index", lb.snap.Metadata.Index),
			zap.String("log-dir", lb.logdir),
			zap.String("backup-db-dir", backupDbDir),
		)
	}

	// create a new backend
	db, err := leveldb.OpenFile(lb.logdir, nil)
	if err != nil {
		return err
	}

	lb.lg.Info("[LeveldbBackend] save snapshot",
		zap.String("log-dir", lb.logdir),
		zap.Uint64("term", snap.Metadata.Term),
		zap.Uint64("index", snap.Metadata.Index),
	)

	lb.snap = snap

	lb.db = db
	lb.index = snap.Metadata.Index
	lb.term = snap.Metadata.Term
	lb.count = 1

	return nil
}

func (lb *LeveldbBackend) Close() error {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return lb.db.Close()
}

func Exist(dir string) bool {
	f, err := os.Open(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
		panic(fmt.Sprintf("have leveldb open dir %s failed. %s", dir, err))
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		panic(fmt.Sprintf("state leveldb dir %s failed. %s", dir, err))
	}
	if !stat.IsDir() {
		return false
	}

	ents, err := f.Readdir(0)
	if err != nil {
		panic(fmt.Sprintf("read leveldb dir %s failed. %s", dir, err))
	}
	for _, e := range ents {
		n := e.Name()
		log.Printf("[leveldbExist] dir: %s name: %s isdir: %v", dir, n, e.IsDir())
		if !e.IsDir() && strings.HasPrefix(n, "MANIFEST-") {
			return true
		}
	}

	return false
}
