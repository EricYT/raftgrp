package raftgrp

import trans "github.com/EricYT/raftgrp/transport"

// UserStateMachine is a bundle of methods the user
// have to implement.
type UserStateMachine interface {
	// RenderMessage rewrites messages before send them
	// to peers
	RenderMessage(payload []byte) (p []byte, err error)
	// ProcessMessage recovers messages before process
	// by raft instance
	ProcessMessage(payload []byte) (p []byte, err error)

	// OnApply applies commited message to user
	OnApply(payload []byte) (err error)

	// OnLeaderStart triggers when the node become leader
	// right now, just be called once.
	OnLeaderStart()

	// OnLeaderStop triggers when the node was leader
	// but lost the role now.
	OnLeaderStop()

	// OnLeaderChange triggers when the leader changed.
	OnLeaderChange()

	// OnError was called, the raft group can't run any more.
	OnError(err error)

	// OnSnapshotSave takes a snapshot point-in-time, use
	// writer to save all files in the state machine.
	OnSnapshotSave() (reader trans.SnapshotReader, err error)

	// OnSnapshotLoad intent to install a new snapshot to local.
	OnSnapshotLoad(writer trans.SnapshotWriter) error

	// UnmarshalSnapshotWriter parses snapshot from bytes to SnapshotWriter for follower
	UnmarshalSnapshotWriter(d []byte) (writer trans.SnapshotWriter, err error)

	// UnmarshalSnapshotParter parse snapshot parter form bytes for leader reading snapshots
	UnmarshalSnapshotParter(d []byte) (parter trans.SnapshotParter, err error)
}
