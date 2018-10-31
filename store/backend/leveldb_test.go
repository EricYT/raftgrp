package backend_test

import (
	"os"
	"path"
	"testing"

	"github.com/EricYT/raftgrp/store/backend"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestEmptyLevelDBStart(t *testing.T) {
	tmpdir := path.Join(os.TempDir(), "leveldb")
	defer os.RemoveAll(tmpdir)

	backend, err := backend.NewLevelDBBackend(zap.NewExample(), tmpdir)
	assert.Nil(t, err)
	defer backend.Close()

	first, err := backend.FirstIndex()
	assert.Nil(t, err)
	assert.Equal(t, uint64(1), first)

	last, err := backend.LastIndex()
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), last)
}
