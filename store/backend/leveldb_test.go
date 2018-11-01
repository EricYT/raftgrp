package backend

import (
	"log"
	"math"
	"os"
	"path"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft"
	pb "go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
)

func TestEmptyLevelDBStart(t *testing.T) {
	tmpdir := path.Join(os.TempDir(), "leveldb")
	defer os.RemoveAll(tmpdir)

	backend, err := NewLevelDBBackend(zap.NewExample(), tmpdir)
	assert.Nil(t, err)
	defer backend.Close()

	first, err := backend.FirstIndex()
	assert.Nil(t, err)
	assert.Equal(t, uint64(1), first)

	last, err := backend.LastIndex()
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), last)
}

func TestSave(t *testing.T) {
	tmpdir := path.Join(os.TempDir(), "leveldb")
	defer os.RemoveAll(tmpdir)

	// FIXME: use etcd MemoryStorage to check the validate results.
	backend, err := NewLevelDBBackend(zap.NewExample(), tmpdir)
	assert.Nil(t, err)
	defer backend.Close()

}

func TestStorageTerm(t *testing.T) {
	tmpdir := path.Join(os.TempDir(), "leveldb")
	defer os.RemoveAll(tmpdir)

	// FIXME: use etcd MemoryStorage to check the validate results.
	backend, err := NewLevelDBBackend(zap.NewExample(), tmpdir)
	assert.Nil(t, err)
	defer backend.Close()

	ents := []pb.Entry{{Index: 2, Term: 2}, {Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	tests := []struct {
		i uint64

		werr   error
		wterm  uint64
		wpanic bool
	}{
		{2, nil, 2, false},
		{3, nil, 3, false},
		{4, nil, 4, false},
		{5, nil, 5, false},
		{6, raft.ErrUnavailable, 0, false},
	}

	err = backend.Save(pb.HardState{}, ents)
	assert.Nil(t, err)

	for i, tt := range tests {
		term, err := backend.Term(tt.i)
		if err != tt.werr {
			t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
		}
		if term != tt.wterm {
			t.Errorf("#%d: term = %d, want %d", i, term, tt.wterm)
		}
	}
}

func TestStorageEntries(t *testing.T) {
	tmpdir := path.Join(os.TempDir(), "leveldb")
	defer os.RemoveAll(tmpdir)

	// FIXME: use etcd MemoryStorage to check the validate results.
	backend, err := NewLevelDBBackend(zap.NewExample(), tmpdir)
	assert.Nil(t, err)
	defer backend.Close()

	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 6}}

	err = backend.Save(pb.HardState{}, ents)
	assert.Nil(t, err)

	// set dummp entry for test
	backend.index = 3
	backend.term = 3
	backend.count = 4

	tests := []struct {
		lo, hi, maxsize uint64

		werr     error
		wentries []pb.Entry
	}{
		{2, 6, math.MaxUint64, raft.ErrCompacted, nil},
		{3, 4, math.MaxUint64, raft.ErrCompacted, nil},
		{4, 5, math.MaxUint64, nil, []pb.Entry{{Index: 4, Term: 4}}},
		{4, 6, math.MaxUint64, nil, []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		{4, 7, math.MaxUint64, nil, []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 6}}},
		// even if maxsize is zero, the first entry should be returned
		{4, 7, 0, nil, []pb.Entry{{Index: 4, Term: 4}}},
		// limit to 2
		{4, 7, uint64(ents[1].Size() + ents[2].Size()), nil, []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		// limit to 2
		{4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size()/2), nil, []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		{4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size() - 1), nil, []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		// all
		{4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size()), nil, []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 6}}},
	}

	for i, tt := range tests {
		entries, err := backend.Entries(tt.lo, tt.hi, tt.maxsize)
		log.Printf("[test] #%d: entries: %v, err: %v", i, entries, err)
		if err != tt.werr {
			t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
		}
		if !reflect.DeepEqual(entries, tt.wentries) {
			t.Errorf("#%d: entries = %v, want %v", i, entries, tt.wentries)
		}
	}
}

func TestStorageLastIndex(t *testing.T) {
	tmpdir := path.Join(os.TempDir(), "leveldb")
	defer os.RemoveAll(tmpdir)

	// FIXME: use etcd MemoryStorage to check the validate results.
	backend, err := NewLevelDBBackend(zap.NewExample(), tmpdir)
	assert.Nil(t, err)
	defer backend.Close()

	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}

	err = backend.Save(pb.HardState{}, ents)
	assert.Nil(t, err)

	// for test
	backend.index = 3
	backend.term = 3
	backend.count = 3

	last, err := backend.LastIndex()
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if last != 5 {
		t.Errorf("term = %d, want %d", last, 5)
	}

	backend.Save(pb.HardState{}, []pb.Entry{{Index: 6, Term: 5}})
	last, err = backend.LastIndex()
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if last != 6 {
		t.Errorf("last = %d, want %d", last, 5)
	}
}

func TestStorageFirstIndex(t *testing.T) {
	tmpdir := path.Join(os.TempDir(), "leveldb")
	defer os.RemoveAll(tmpdir)

	// FIXME: use etcd MemoryStorage to check the validate results.
	backend, err := NewLevelDBBackend(zap.NewExample(), tmpdir)
	assert.Nil(t, err)
	defer backend.Close()

	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}

	backend.Save(pb.HardState{}, ents)

	// for test
	backend.index = 3
	backend.term = 3
	backend.count = 3

	first, err := backend.FirstIndex()
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if first != 4 {
		t.Errorf("first = %d, want %d", first, 4)
	}

	backend.Compact(4)
	first, err = backend.FirstIndex()
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if first != 5 {
		t.Errorf("first = %d, want %d", first, 5)
	}
}
