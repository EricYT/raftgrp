package backend

import "errors"

// not the raft log entry
var ErrNotEntry error = errors.New("[storeage beackend] not raft log entry")

// parse index error
var ErrParseIndex error = errors.New("[storage backend] parse index failed")
