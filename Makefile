

all:
	echo "Nothing to do"

generate:
	#protoc -I /Users/yutao/Codes/go/src/github.com/coreos/etcd -I /Users/yutao/Codes/go/src/github.com/gogo/protobuf/ -I proto proto/raftgrp.proto --go_out=plugins=grpc:proto
	#protoc -I /Users/yutao/Codes/go/src -I proto proto/raftgrp.proto --go_out=plugins=grpc:proto
	protoc -I proto proto/raftgrp.proto --go_out=plugins=grpc:proto
