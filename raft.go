package raftgrp

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/EricYT/raftgrp/store"

	"go.etcd.io/etcd/pkg/contention"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"

	"go.uber.org/zap"
)

const (
	maxSizePerMsg   = 1 * 1024 * 1024
	maxInflightMsgs = 4096 / 8
)

type raftNode struct {
	lg *zap.Logger

	tickMu *sync.Mutex
	raftNodeConfig

	// util
	ticker *time.Ticker

	// contention detectors for raft heartbeat messge
	td *contention.TimeoutDetector

	stopped chan struct{}
	done    chan struct{}
}

type raftNodeConfig struct {
	lg *zap.Logger

	isIDRemoved func(id uint64) bool
	raft.Node
	storage   store.Storage
	heartbeat time.Duration
	transport Transporter
}

func newRaftNode(cfg raftNodeConfig) *raftNode {
	r := &raftNode{
		lg:             cfg.lg,
		tickMu:         new(sync.Mutex),
		raftNodeConfig: cfg,
		// set up contention detectors for raft heartbeat message.
		// expect to send a heartbeat within 2 heartbeat intervals.
		td:      contention.NewTimeoutDetector(2 * cfg.heartbeat),
		stopped: make(chan struct{}),
		done:    make(chan struct{}),
	}
	if r.heartbeat == 0 {
		r.ticker = &time.Ticker{}
	} else {
		r.ticker = time.NewTicker(r.heartbeat)
	}
	return r
}

// raft.Node does not have locks in Raft package
func (r *raftNode) tick() {
	r.tickMu.Lock()
	r.Tick()
	r.tickMu.Unlock()
}

// main loop
func (r *raftNode) start(rh *raftReadyHandler) {
	go func() {
		defer r.onStop()

		for {
			select {
			case <-r.ticker.C:
				r.tick()
			case rd := <-r.Ready(): // core logic
				if rd.SoftState != nil {
					rh.updateSoftState(rd.SoftState)
					r.td.Reset()
				}

				if !raft.IsEmptyHardState(rd.HardState) || len(rd.Entries) != 0 {
					r.lg.Debug("[raftNode] hard state",
						zap.Uint64("term", rd.HardState.Term),
						zap.Uint64("vote", rd.HardState.Vote),
						zap.Uint64("commit", rd.HardState.Commit),
						zap.Int("entries-count", len(rd.Entries)),
					)
				}

				// persist hardstate and entries
				if err := r.storage.Save(rd.HardState, rd.Entries); err != nil {
					r.lg.Fatal("failed to save Raft hard state and entries", zap.Error(err))
				}

				// install snapshot
				if !raft.IsEmptySnap(rd.Snapshot) {
					r.lg.Info("[raftNode] install snapshot.",
						zap.Any("snap-metadata", rd.Snapshot.Metadata),
					)
					if err := r.storage.SaveSnap(rd.Snapshot); err != nil {
						r.lg.Fatal("failed to save Raft snapshot", zap.Error(err))
					}
					if err := r.storage.ApplySnapshot(rd.Snapshot); err != nil {
						r.lg.Fatal("failed to apply snapshot", zap.Error(err))
					}
				}
				// FIXME: try to replay entries again after saving snapshot
				// This interface behavior depends on what backend store
				// we used.
				if err := r.storage.Append(rd.Snapshot, rd.Entries); err != nil {
					r.lg.Fatal("failed to apply entries", zap.Error(err))
				}

				// send messages to others
				// FIXME: this method can be operated parallel.
				r.transport.Send(r.processMessages(rd.Messages))

				// apply all
				rh.applyAll(rd.Snapshot, rd.CommittedEntries)

				r.Advance()

			case <-r.stopped:
				return
			}
		}
	}()
}

func (r *raftNode) processMessages(ms []raftpb.Message) []raftpb.Message {
	for i := len(ms) - 1; i >= 0; i-- {
		if r.isIDRemoved(ms[i].To) {
			ms[i].To = 0
		}

		if ms[i].Type == raftpb.MsgHeartbeat {
			ok, exceed := r.td.Observe(ms[i].To)
			if !ok {
				// TODO: limit request rate.
				r.lg.Warn(
					"leader failed to send out heartbeat on time; took too long, leader is overloaded likely from slow disk",
					zap.Duration("heartbeat-interval", r.heartbeat),
					zap.Duration("expected-duration", 2*r.heartbeat),
					zap.Duration("exceeded-duration", exceed),
				)
			}
		}
	}

	return ms
}

func (r *raftNode) stop() {
	r.stopped <- struct{}{}
	<-r.done
}

func (r *raftNode) onStop() {
	r.Stop()
	r.ticker.Stop()
	if err := r.storage.Close(); err != nil {
		r.lg.Panic("failed to close Raft storage", zap.Error(err))
	}
	close(r.done)
}

// advanceTicks advances ticks of Raft node.
// This can be used for fast-forwarding election
// ticks in multi data-center deployments, thus
// speeding up election process.
func (r *raftNode) advanceTicks(ticks int) {
	for i := 0; i < ticks; i++ {
		r.tick()
	}
}

func startNode(cfg GroupConfig, id uint64, members []*Member, s store.Storage) (n raft.Node) {
	// raft instance configuration
	peers := make([]raft.Peer, len(members))
	for i := range members {
		mem := members[i]
		ctx, _ := json.Marshal(&mem)
		peers[i] = raft.Peer{ID: uint64(mem.ID), Context: ctx}
	}
	c := &raft.Config{
		ID:                        id,
		ElectionTick:              cfg.ElectionTicks,
		HeartbeatTick:             1,
		Storage:                   s,
		MaxSizePerMsg:             maxSizePerMsg,
		MaxInflightMsgs:           maxInflightMsgs,
		PreVote:                   cfg.PreVote,
		DisableProposalForwarding: true,
	}
	n = raft.StartNode(c, peers)
	return n
}

func restartNode(cfg GroupConfig, id uint64, s store.Storage) (n raft.Node) {
	c := &raft.Config{
		ID:                        id,
		ElectionTick:              cfg.ElectionTicks,
		HeartbeatTick:             1,
		Storage:                   s,
		MaxSizePerMsg:             maxSizePerMsg,
		MaxInflightMsgs:           maxInflightMsgs,
		PreVote:                   cfg.PreVote,
		DisableProposalForwarding: true,
	}
	n = raft.RestartNode(c)
	return n
}
