package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/EricYT/raftgrp"
	"go.uber.org/zap"
)

func main() {
	fmt.Println("raft group example go...")
	id := flag.Int("id", 1, "node id")
	gid := flag.Int("gid", 1, "group id")
	cluster := flag.String("cluster", "1=127.0.0.1:9527,2=127.0.0.1:9528", "comma separated cluster peers")
	grpc := flag.String("grpc-addr", "127.0.0.1:9527", "grpc server address")
	port := flag.Int("port", 9021, "admin port")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()

	logger := zap.NewExample()
	log.Printf("raft ready to start cluster: %s id: %d gid: %d admin-port: %d grpc-address: %s join: %t", *cluster, *id, *gid, *port, *grpc, *join)

	// raft group manager initialize
	mgr := raftgrp.NewRaftGroupManager(logger, *grpc)
	if err := mgr.Start(); err != nil {
		log.Fatalf("[main] start raft group manager error: %s", err)
	}

	grp, err := mgr.NewRaftGroup(logger, uint64(*gid), uint64(*id), strings.Split(*cluster, ","), "log")
	if err != nil {
		log.Fatalf("[main] new raft group error: %s", err)
	}
	grp.Start()
	defer grp.Stop()
	log.Println("raft group start ok")
	kv := newKvStore(grp)
	//grp.SetFSM(kv)

	// serve http
	serveHttpKVAPI(kv, *port, nil, nil)
}
