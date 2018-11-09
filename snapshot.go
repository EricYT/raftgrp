package raftgrp

import (
	"encoding/json"
)

type SnapshotMetadata struct {
	StateMachineMeta []byte `json:"state_machine_meta"`
	TopologyMeta     []byte `json:"topology_meta"`
}

func (s *SnapshotMetadata) Marshal() (p []byte, err error) {
	// FIXME: Protobuf
	p, err = json.Marshal(s)
	return
}
