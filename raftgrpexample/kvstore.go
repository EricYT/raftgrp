package main

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"log"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/EricYT/go-examples/utils/wait"
	"github.com/EricYT/raftgrp"
	"github.com/EricYT/raftgrp/transport"
	"github.com/davecgh/go-spew/spew"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/pkg/types"
)

var (
	ErrBlobStoreNotFound error = errors.New("blob: not found")
	ErrKvStoreNotFound   error = errors.New("kv: not found")
)

const (
	metaDirectory string = "meta"
	blobDirectory string = "blob"
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
		log.Printf("[kvMeta] unmarshal error: %s payload: %s", string(payload))
		panic(err)
	}
	log.Printf("[kvMeta] unmarshal payload: key (%s) bid (%v) payload (%x)", m.Key, m.BId, md5.Sum(m.Payload))
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

func (kv *kvstore) OnApply(payload []byte) (err error) {
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

func (kv *kvstore) OnLeaderStart() {
	log.Printf("[kvstore] I'm the king of jungle!")
}

func (kv *kvstore) OnLeaderStop() {
	log.Printf("[kvstore] Gone with the wind.")
}

func (kv *kvstore) OnLeaderChange() {
	log.Printf("[kvstore] A new king is born.")
}

func (kv *kvstore) OnError(err error) {
	log.Fatalf("[kvstore] Go die. %v", err)
}

// snapshot
func (kv *kvstore) OnSnapshotSave() (sr transport.SnapshotReader, err error) {
	log.Printf("[kvstore] on snapshot save\n")

	snap := &transport.SnapshotFileBase{
		ID:   "kvstore-test",
		Meta: make(map[string]string),
	}
	snap.Dir = "./volume1"
	snap.Meta["date"] = time.Now().Format(time.RFC1123)

	// test codes
	dh, err := os.Open(snap.Dir)
	if err != nil {
		return nil, err
	}
	defer dh.Close()

	fs, err := dh.Readdir(0)
	if err != nil {
		return nil, err
	}

	for _, file := range fs {
		if !file.IsDir() {
			continue
		}
		log.Printf("[kvstore] directory name: %s\n", file.Name())
		switch path.Base(file.Name()) {
		case metaDirectory:
			snapdir := &transport.Directory{
				Dir: path.Join("/", metaDirectory),
			}
			log.Printf("[kvstore] meta directory operation")
			metah, err := os.Open(path.Join(snap.Dir, file.Name()))
			if err != nil {
				return nil, err
			}
			metafs, err := metah.Readdir(0)
			if err != nil {
				metah.Close()
				return nil, err
			}
			metah.Close()
			for _, metaf := range metafs {
				if metaf.IsDir() {
					continue
				}
				log.Printf("[kvstore] meta directory file: %s\n", metaf.Name())
				f := &transport.File{
					Filename: path.Join(snapdir.Dir, metaf.Name()),
					Size:     metaf.Size(),
					Meta:     make(map[string]string),
				}
				f.Meta["name"] = metaf.Name()
				snapdir.Files = append(snapdir.Files, f)
			}
			snap.Directories = append(snap.Directories, snapdir)

		case blobDirectory:
			log.Printf("[kvstore] blob directory operation")
			snapdir := &transport.Directory{
				Dir: path.Join("/", blobDirectory),
			}
			blobh, err := os.Open(path.Join(snap.Dir, file.Name()))
			if err != nil {
				return nil, err
			}
			blobs, err := blobh.Readdir(0)
			if err != nil {
				blobh.Close()
				return nil, err
			}
			blobh.Close()
			for _, blob := range blobs {
				if blob.IsDir() {
					continue
				}
				log.Printf("[kvstore] blob directory file: %s\n", blob.Name())
				f := &transport.File{
					Filename: path.Join(snapdir.Dir, blob.Name()),
					Size:     blob.Size(),
					Meta:     make(map[string]string),
				}
				f.Meta["name"] = blob.Name()
				snapdir.Files = append(snapdir.Files, f)
			}
			snap.Directories = append(snap.Directories, snapdir)

		default:
		}
	}

	spew.Dump(snap)

	// snap.Directories = []*transport.Directory{
	// 	&transport.Directory{
	// 		Dir: "/blob",
	// 		Files: []*transport.File{
	// 			&transport.File{
	// 				Filename:  "/blob/foo.txt",
	// 				ParentDir: snap.Dir,
	// 			},
	// 			&transport.File{
	// 				Filename:  "/blob/foo-1.txt",
	// 				ParentDir: snap.Dir,
	// 			},
	// 		},
	// 	},
	// 	&transport.Directory{
	// 		Dir: "/meta",
	// 		Files: []*transport.File{
	// 			&transport.File{
	// 				Filename:  "/meta/bar.txt",
	// 				ParentDir: snap.Dir,
	// 			},
	// 			&transport.File{
	// 				Filename:  "/meta/bar-1.txt",
	// 				ParentDir: snap.Dir,
	// 			},
	// 		},
	// 	},
	// }

	return transport.NewSnapshotFileReader(snap), nil
}

func (kv *kvstore) OnSnapshotLoad(sw transport.SnapshotWriter) (err error) {
	log.Printf("[kvstore] on snapshot load\n")
	err = sw.Commit()
	if err != nil {
		log.Printf("[kvstore] on snapshot load commit failed. %v\n", err)
		return err
	}
	return nil
}

// FIXME: for leader view
func (kv *kvstore) UnmarshalSnapshotParter(p []byte) (sp transport.SnapshotParter, err error) {
	parter := transport.NewFile()
	err = parter.Unmarshal(p)
	if err != nil {
		log.Printf("[kvstore] unmarshal parter(%s) failed. %v\n", string(p), err)
		return nil, err
	}

	parter.SetParentDir("./volume1")

	log.Printf("[kvstore] unmarshal snapshot file parter. parentDir: %s\n", parter.ParentDir)

	return parter, err
}

// FIXME: for someone try to sync snapshot
func (kv *kvstore) UnmarshalSnapshotWriter(p []byte) (r transport.SnapshotWriter, err error) {
	filebase := transport.NewSnapshotFileBase()
	err = filebase.Unmarshal(p)
	if err != nil {
		log.Printf("[kvstore] unmarshal snapshot file base(%s) failed. %v\n", string(p), err)
		return nil, err
	}

	filewriter := transport.NewSnapshotFileWriter(filebase)

	filewriter.SetTmpDir("./volume2/tmp")
	filewriter.SetTargetDir("./volume2")

	if err := filewriter.EnsureTmpDirectories(); err != nil {
		log.Printf("[kvstore] ensure tmp directories failed. %v\n", err)
		return nil, err
	}

	log.Printf("[kvstore] unmarshal snapshot file writer. dir: %s tmp: %s\n", filewriter.Dir, filewriter.TmpDir)

	return filewriter, nil
}

// redering payload send to others
// FIXME: If we can't cache the meta marshalling data, there are many same
// copies for different peers we have to send messages.
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

func (kv *kvstore) AddPeer(id uint64, addr string) error {
	m := &raftgrp.Member{
		ID: types.ID(id),
		RaftAttributes: raftgrp.RaftAttributes{
			Addr: addr,
		},
	}

	if err := kv.group.AddMember(context.TODO(), m); err != nil {
		return err
	}

	return nil
}

func (kv *kvstore) RemovePeer(id uint64) error {
	if err := kv.group.RemoveMember(context.TODO(), id); err != nil {
		return err
	}
	return nil
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
