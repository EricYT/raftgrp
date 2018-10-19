package raftgrp

import (
	"fmt"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

// config
type GroupConfig struct {
	*zap.Logger

	ID    uint64
	Peers []*Peer

	DataDir string

	TickMs        uint
	ElectionTicks int
	PreVote       bool
}

func (g GroupConfig) MemberDir() string {
	return filepath.Join(g.DataDir, fmt.Sprintf("member-%d", g.ID))
}

func (g GroupConfig) WALDir() string {
	return filepath.Join(g.MemberDir(), "wal")
}

func (g GroupConfig) SnapDir() string {
	return filepath.Join(g.MemberDir(), "snap")
}

func (g GroupConfig) peerDialTimeout() time.Duration {
	return time.Second * time.Duration(g.ElectionTicks*int(g.TickMs)) * time.Millisecond
}

type Peer struct {
	ID   uint64
	Addr string
}

// peers => "1=http://127.0.0.1:9527"
func ParsePeers(ps []string) []*Peer {
	peers := make([]*Peer, len(ps))
	for i := range ps {
		peer := &Peer{}
		p := strings.Split(ps[i], "=")
		log.Println(p)
		peer.ID, _ = strconv.ParseUint(p[0], 10, 64)
		peer.Addr = p[1]
		peers[i] = peer
	}
	return peers
}
