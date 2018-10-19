package raftgrp

import (
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"go.uber.org/zap"
)

type URLMap map[name]url.URL

type RaftGroupTopology struct {
	lg *zap.Logger

	localID types.ID
	gid     types.ID
	token   string // FIXME:

	// backend

	sync.Mutex
	members map[types.ID]*Member
	// removed contains the ids of removed members in the group.
	// removed id cannot be reused.
	removed map[types.ID]bool
}

func NewRaftGroupTopology(lg *zap.Logger, token string, urlmap URLMap) (*RaftGroupTopology, error) {
	t := NewTopology(lg, token)
	for name, url := range urlmap {
		m := NewMember(name, url, token, nil)
		if _, ok := t.members[m.ID]; ok {
			return nil, errors.Errorf("member exists with identical ID %v", m)
		}
		if uint64(m.ID) == raft.None {
			return nil, errors.Errorf("cannot use %x as member id", raft.None)
		}
		t.members[m.ID] = m
	}
	t.genID()
	return t, nil
}

func NewTopology(lg *zap.Logger, token string) *RaftGroupTopology {
	return &RaftGroupTopology{
		lg:      lg,
		token:   token,
		members: make(map[types.ID]*Member),
		removed: make(map[types.ID]bool),
	}
}

func (t *RaftGroupTopology) SetID(localID, gid types.ID) {
	t.localID = localID
	t.gid = gid
}

func (t *RaftGroupTopology) ID() types.ID { return t.gid }

func (t *RaftGroupTopology) MemberIDs() []types.ID {
	t.Lock()
	defer t.Unlock()
	var ids []types.ID
	for _, m := range t.members {
		ids = append(ids, m.ID)
	}
	sort.Sort(types.IDSlice(ids))
	return ids
}

func (t *RaftGroupTopology) genID() {
	mIDs := t.MemberIDs()
	b := make([]byte, 8*len(mIDs))
	for i, id := range mIDs {
		binary.BigEndian.PutUint64(b[8*i:], uint64(id))
	}
	hash := sha1.Sum(b)
	t.gid = types.ID(binary.BigEndian.Uint64(hash[:8]))
}

// member
type RaftAttributes struct {
	PeerURL url.URL
}

type Attributes struct {
	Name string
}

type Member struct {
	ID types.ID
	RaftAttributes
	Attributes
}

func NewMember(name string, peerURL url.URL, clusterName string, now *time.Time) *Member {
	m := &Member{
		RaftAttributes: RaftAttributes{PeerURL, peerURL},
		Attributes:     Attributes{Name: name},
	}

	var b []byte
	b = append(b, []byte(peerURL)...)
	b = append(b, []byte(clusterName)...)
	if now != nil {
		b = append(b, []byte(fmt.Sprintf("%d", now.Unix())...))
	}

	hash := sha1.Sum(b)
	m.ID = types.ID(binary.BigEndian.Uint64(hash[:8]))
	return m
}

func (m *Member) Clone() *Member {
	if m == nil {
		return nil
	}

	mm := &Member{
		ID: m.ID,
		Attributes: Attributes{
			Name: m.Name,
		},
		RaftAttributes: RaftAttributes{
			PeerURL: m.PeerURL,
		},
	}
	return mm
}

func (m *Member) IsStarted() bool {
	return len(m.Name) != 0
}
