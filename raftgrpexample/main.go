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
	cluster := flag.String("cluster", "1=http://127.0.0.1:9527,2=http://127.0.0.1:9528", "comma separated cluster peers")
	id := flag.Int("id", 1, "node id")
	port := flag.Int("port", 9021, "admin port")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()

	logger := zap.NewExample()
	log.Printf("raft ready to start cluster: %s id: %d port: %d join: %t", *cluster, *id, *port, *join)
	grp, err := raftgrp.NewRaftGroup(raftgrp.GroupConfig{
		Logger:        logger,
		ID:            uint64(*id),
		Peers:         raftgrp.ParsePeers(strings.Split(*cluster, ",")),
		DataDir:       "log",
		TickMs:        1000,
		ElectionTicks: 10,
		PreVote:       false,
	})
	if err != nil {
		panic(err)
	}
	grp.Start()
	defer grp.Stop()
	log.Println("raft group start ok")
	kv := newKvStore(grp)
	//grp.SetFSM(kv)

	// serve http
	serveHttpKVAPI(kv, *port, nil, nil)
}
