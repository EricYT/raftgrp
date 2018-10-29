package raftgrp

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"

	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
)

type RaftGroupTopology struct {
	lg *zap.Logger

	groupID uint64
	peerID  types.ID

	// TODO: backend

	sync.Mutex
	members map[types.ID]*Member
	// removed contains the ids of removed members in the group.
	// removed id cannot be reused.
	removed map[types.ID]bool
}

func NewTopologyWithPeers(lg *zap.Logger, peers []*Peer) (*RaftGroupTopology, error) {
	t := NewTopology(lg)
	for i := range peers {
		p := peers[i]
		m := NewMember(p.ID, p.Addr)
		if _, ok := t.members[m.ID]; ok {
			return nil, fmt.Errorf("member exists with identical ID %v", m)
		}
		if uint64(m.ID) == raft.None {
			return nil, fmt.Errorf("cannot use %x as member id", raft.None)
		}
		t.members[m.ID] = m
	}
	return t, nil
}

func NewTopology(lg *zap.Logger) *RaftGroupTopology {
	return &RaftGroupTopology{
		lg:      lg,
		members: make(map[types.ID]*Member),
		removed: make(map[types.ID]bool),
	}
}

func (t *RaftGroupTopology) SetID(peerID types.ID) {
	t.peerID = peerID
}

func (t *RaftGroupTopology) SetGroupID(gid uint64) {
	t.groupID = gid
}

func (t *RaftGroupTopology) PeerID() types.ID { return t.peerID }

func (t *RaftGroupTopology) GroupID() uint64 { return t.groupID }

func (t *RaftGroupTopology) Members() []*Member {
	t.Lock()
	defer t.Unlock()
	var ms MemberByID
	for _, m := range t.members {
		ms = append(ms, m)
	}
	sort.Sort(ms)
	return ms
}

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

func (t *RaftGroupTopology) IsIDRemoved(id types.ID) bool {
	t.Lock()
	defer t.Unlock()
	_, ok := t.removed[id]
	return ok
}

func (t *RaftGroupTopology) MemberByID(id types.ID) *Member {
	t.Lock()
	defer t.Unlock()
	if mem, ok := t.members[id]; ok {
		return mem.Clone()
	}
	return nil
}

func (t *RaftGroupTopology) AddMember(m *Member) {
	t.Lock()
	defer t.Unlock()
	t.members[m.ID] = m
	t.lg.Info("added member",
		zap.Uint64("group-id", t.groupID),
		zap.String("local-member-id", t.peerID.String()),
		zap.String("added-peer-id", m.ID.String()),
		zap.String("added-peer-addr", m.Addr),
	)
}

func (t *RaftGroupTopology) RemoveMember(id types.ID) {
	t.Lock()
	defer t.Unlock()
	m, ok := t.members[id]
	delete(t.members, id)
	t.removed[id] = true

	if ok {
		t.lg.Info("removed member",
			zap.Uint64("group-id", t.GroupID()),
			zap.String("local-member-id", t.peerID.String()),
			zap.String("removed-peer-id", id.String()),
			zap.String("removed-peer-addr", m.Addr),
		)
	} else {
		t.lg.Info("skiped removing already removed member",
			zap.Uint64("group-id", t.GroupID()),
			zap.String("local-member-id", t.peerID.String()),
			zap.String("removed-peer-id", id.String()),
		)
	}
}

var (
	ErrIDRemoved  error = errors.New("topology: id removed")
	ErrIDExist    error = errors.New("topology: id exist")
	ErrIDNotFound error = errors.New("topology: id not found")
)

func (t *RaftGroupTopology) ValidateConfigurationChange(cc raftpb.ConfChange) error {
	id := types.ID(cc.NodeID)
	if t.IsIDRemoved(id) {
		return ErrIDRemoved
	}
	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		if t.MemberByID(id) != nil {
			return ErrIDExist
		}
	case raftpb.ConfChangeRemoveNode:
		if t.MemberByID(id) == nil {
			return ErrIDNotFound
		}
	case raftpb.ConfChangeUpdateNode:
		panic("not implement")
	default:
		panic("unknow type of configuration change")
	}
	return nil
}

func (t *RaftGroupTopology) Marshal() []byte {
	t.Lock()
	defer t.Unlock()

	p := &RaftGroupTopologyPersistStructure{
		GroupID: t.groupID,
		PeerID:  t.peerID,
		Members: t.members,
	}
	for r, _ := range t.removed {
		p.Removed = append(p.Removed, r)
	}

	data, _ := json.Marshal(p)
	return data
}

func (t *RaftGroupTopology) Recovery(data []byte) error {
	var p RaftGroupTopologyPersistStructure
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}

	t.Lock()
	t.groupID = p.GroupID
	t.peerID = p.PeerID
	t.members = p.Members

	for i := range p.Removed {
		t.removed[p.Removed[i]] = true
	}

	t.Unlock()

	return nil
}

// persist structure
type RaftGroupTopologyPersistStructure struct {
	GroupID uint64               `json:"group_id"`
	PeerID  types.ID             `json:"peer_id"`
	Members map[types.ID]*Member `json:"members"`
	Removed []types.ID           `json:"removed"`
}

// member
type RaftAttributes struct {
	Addr string `json:"addr"`
}

type Member struct {
	ID             types.ID `json:"id"`
	RaftAttributes `json:"raft_attributes"`
}

func NewMember(id uint64, addr string) *Member {
	m := &Member{
		ID:             types.ID(id),
		RaftAttributes: RaftAttributes{Addr: addr},
	}
	return m
}

func (m *Member) Clone() *Member {
	if m == nil {
		return nil
	}

	mm := &Member{
		ID: m.ID,
		RaftAttributes: RaftAttributes{
			Addr: m.Addr,
		},
	}
	return mm
}

func (m *Member) IsStarted() bool {
	panic("not implement")
	return false
}

// sort by member id
type MemberByID []*Member

func (ms MemberByID) Len() int           { return len(ms) }
func (ms MemberByID) Less(i, j int) bool { return ms[i].ID < ms[j].ID }
func (ms MemberByID) Swap(i, j int)      { ms[i], ms[j] = ms[j], ms[i] }
