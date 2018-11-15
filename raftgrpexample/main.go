package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/EricYT/raftgrp"
	"go.uber.org/zap"

	// pprof
	"net/http"
	_ "net/http/pprof"
)

func main() {
	fmt.Println("raft group example go...")
	pprofPort := flag.Int("pprof-port", 6060, "pprof port")

	id := flag.Int("id", 1, "node id")
	gid := flag.Int("gid", 1, "group id")
	cluster := flag.String("cluster", "1=127.0.0.1:9527,2=127.0.0.1:9528", "comma separated cluster peers")
	grpc := flag.String("grpc-addr", "127.0.0.1:9527", "grpc server address")
	port := flag.Int("port", 9021, "admin port")
	newCluster := flag.Bool("new-cluster", false, "join an existing cluster")
	flag.Parse()

	// pprof
	go func() {
		log.Fatalln(http.ListenAndServe(fmt.Sprintf("localhost:%d", *pprofPort), nil))
	}()

	logger := zap.NewExample()
	log.Printf("raft ready to start cluster: %s id: %d gid: %d admin-port: %d grpc-address: %s join: %t", *cluster, *id, *gid, *port, *grpc, *newCluster)

	// raft group manager initialize
	mgr := raftgrp.NewRaftGroupManager(logger, fmt.Sprintf("log-%d", *id), *grpc)
	if err := mgr.Start(); err != nil {
		log.Fatalf("[main] start raft group manager error: %s", err)
	}

	grp, err := mgr.NewRaftGroup(logger, uint64(*gid), uint64(*id), strings.Split(*cluster, ","), *newCluster)
	if err != nil {
		log.Fatalf("[main] new raft group error: %s", err)
	}
	kv := newKvStore(grp)
	// set usm for group
	grp.SetUserStateMachine(kv)

	grp.Start()
	defer grp.Stop()
	log.Println("raft group start ok")

	// serve http
	serveHttpKVAPI(kv, *port)

	<-(chan struct{})(nil)
}
