package main

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"log"
	"sync"
	"sync/atomic"

	"github.com/EricYT/go-examples/utils/wait"
	"github.com/EricYT/raftgrp"
	"github.com/pkg/errors"
)

var (
	ErrBlobStoreNotFound error = errors.New("blob: not found")
	ErrKvStoreNotFound   error = errors.New("kv: not found")
)

// This implementation is not same as etcd raft example,
// we think the data is large enough to consume
// big effect to store more than once.

type BlobId uint64

type kvstore struct {
	group     raftgrp.RaftGrouper
	blobStore *blobStore

	mu      sync.Mutex
	rid     uint64
	w       wait.Wait
	kvStore map[string]BlobId
}

func newKvStore(grp raftgrp.RaftGrouper) *kvstore {
	kv := &kvstore{
		group:     grp,
		blobStore: newBlobStore(),

		w:       wait.NewWait(),
		kvStore: make(map[string]BlobId),
	}

	return kv
}

func (kv *kvstore) Start() (err error) {
	//FIXME: reloading all data from snapshotter

	return nil
}

func (kv *kvstore) Lookup(key string) (val []byte, err error) {
	kv.mu.Lock()
	bid, ok := kv.kvStore[key]
	kv.mu.Unlock()
	if !ok {
		return nil, ErrKvStoreNotFound
	}
	if val, err = kv.blobStore.Get(bid); err == nil {
		return val, nil
	}
	panic("kvstore and blob store missing match")
}

type kvMeta struct {
	ReqId   uint64 `json:"req_id"`
	Key     string `json:"key"`
	BId     BlobId `json:"b_id"`
	Payload []byte `json:"payload"`
}

func (m *kvMeta) Marshal() []byte {
	val, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return val
}

func (m *kvMeta) Unmarshal(payload []byte) {
	err := json.Unmarshal(payload, m)
	if err != nil {
		panic(err)
	}
}

func (kv *kvstore) Put(key string, val []byte) (err error) {
	log.Printf("[kvstore] put key(%s)", key)
	// unloading value
	bid, err := kv.blobStore.Put(val)
	if err != nil {
		return err
	}
	log.Println("[kvstore] put blob id ", bid)

	reqid := atomic.AddUint64(&kv.rid, 1)
	meta := &kvMeta{ReqId: reqid, Key: key, BId: bid}
	payload := meta.Marshal()

	done := kv.w.Register(reqid)

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	if err := kv.group.Propose(ctx, payload); err != nil {
		kv.w.Trigger(reqid, nil)
		return err
	}

	select {
	case res := <-done:
		if res == nil {
			return nil
		}
		err = res.(error)
		return err
	case <-ctx.Done():
		kv.w.Trigger(reqid, nil)
		return ctx.Err()
	}
}

func (kv *kvstore) Apply(payload []byte) (err error) {
	meta := &kvMeta{}
	meta.Unmarshal(payload)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.kvStore[meta.Key] = meta.BId

	// trigger done
	if kv.w.IsRegister(meta.ReqId) {
		kv.w.Trigger(meta.ReqId, nil)
	}

	return nil
}

// redering payload send to others
func (kv *kvstore) RenderMessage(payload []byte) (p []byte, err error) {
	meta := &kvMeta{}
	meta.Unmarshal(payload)
	val, err := kv.blobStore.Get(meta.BId)
	if err != nil {
		return nil, errors.Wrap(err, "kvstore: render message get blob error")
	}
	log.Printf("[kvstore] render message payload: (%#v) val from blob: (%x)", meta, md5.Sum(val))
	meta.ReqId = 0
	meta.BId = 0
	meta.Payload = val

	return meta.Marshal(), nil
}

// unloading payload when receive other message
func (kv *kvstore) ProcessMessage(payload []byte) (p []byte, err error) {
	meta := &kvMeta{}
	meta.Unmarshal(payload)
	log.Printf("[kvstore] process message payload: (%x)", md5.Sum(meta.Payload))

	bid, err := kv.blobStore.Put(meta.Payload)
	if err != nil {
		return nil, errors.Wrap(err, "kvstore: process message put blob error")
	}

	meta.BId = bid
	meta.Payload = nil

	return meta.Marshal(), nil
}

// loading and unloading blob data
func (kv *kvstore) PutBlob(blob []byte) (bid BlobId, err error) {
	return kv.blobStore.Put(blob)
}

func (kv *kvstore) GetBlob(bid BlobId) (blob []byte, err error) {
	return kv.blobStore.Get(bid)
}

func (kv *kvstore) AddNode(nodeid uint64) error {
	panic("not implement")
}

func (kv *kvstore) RemoveNode(nodeid uint64) error {
	panic("not implement")
}

// a memory implementation of blob store engine
type blobStore struct {
	mu        sync.Mutex
	id        BlobId
	blobStore map[BlobId][]byte
}

func newBlobStore() *blobStore {
	return &blobStore{
		id:        1,
		blobStore: make(map[BlobId][]byte),
	}
}

func (bs *blobStore) Start() (err error) {
	// FIXME: reloading all blob data

	return nil
}

func (bs *blobStore) Get(id BlobId) (blob []byte, err error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	if blob, ok := bs.blobStore[id]; ok {
		return blob, nil
	}
	return nil, ErrBlobStoreNotFound
}

func (bs *blobStore) Put(blob []byte) (id BlobId, err error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	id = bs.id
	bs.id++
	bs.blobStore[id] = blob
	return id, nil
}
