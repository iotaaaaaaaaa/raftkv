// raftkv runs one node of a replicated key-value store.
//
// Example, three nodes on one machine:
//
//	CLUSTER="n1=127.0.0.1:7001/127.0.0.1:8001,n2=127.0.0.1:7002/127.0.0.1:8002,n3=127.0.0.1:7003/127.0.0.1:8003"
//	raftkv -id n1 -cluster "$CLUSTER" -data data/n1
//	raftkv -id n2 -cluster "$CLUSTER" -data data/n2
//	raftkv -id n3 -cluster "$CLUSTER" -data data/n3
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/iotaaaaaaaaa/raftkv/internal/kv"
	"github.com/iotaaaaaaaaa/raftkv/internal/raft"
)

func main() {
	var (
		id      = flag.String("id", "", "this node's ID (must appear in -cluster)")
		cluster = flag.String("cluster", "", "comma-separated id=raftAddr/httpAddr for every node")
		dataDir = flag.String("data", "data", "directory for this node's Raft state")
	)
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(log, *id, *cluster, *dataDir); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func parseCluster(spec string) (raftAddrs, httpAddrs map[string]string, err error) {
	raftAddrs, httpAddrs = map[string]string{}, map[string]string{}
	for _, part := range strings.Split(spec, ",") {
		id, addrs, ok := strings.Cut(strings.TrimSpace(part), "=")
		raftAddr, httpAddr, ok2 := strings.Cut(addrs, "/")
		if !ok || !ok2 || id == "" {
			return nil, nil, fmt.Errorf("bad -cluster entry %q (want id=raftAddr/httpAddr)", part)
		}
		raftAddrs[id], httpAddrs[id] = raftAddr, httpAddr
	}
	return raftAddrs, httpAddrs, nil
}

func run(log *slog.Logger, id, clusterSpec, dataDir string) error {
	raftAddrs, httpAddrs, err := parseCluster(clusterSpec)
	if err != nil {
		return err
	}
	if _, ok := raftAddrs[id]; !ok {
		return fmt.Errorf("-id %q is not listed in -cluster", id)
	}
	var peers []string
	for p := range raftAddrs {
		if p != id {
			peers = append(peers, p)
		}
	}

	storage, err := raft.OpenFileStorage(dataDir)
	if err != nil {
		return err
	}
	defer storage.Close()

	raftLn, err := net.Listen("tcp", raftAddrs[id])
	if err != nil {
		return err
	}
	defer raftLn.Close()

	transport := raft.NewTCPTransport(raftAddrs)
	defer transport.Close()
	rf, err := raft.New(raft.Config{
		ID: id, Peers: peers,
		ElectionTimeoutMin: 300 * time.Millisecond,
		ElectionTimeoutMax: 600 * time.Millisecond,
		HeartbeatInterval:  75 * time.Millisecond,
		Logger:             log,
	}, transport, storage)
	if err != nil {
		return err
	}
	raft.ServeTCP(raftLn, rf)

	store := kv.New(rf)
	srv := &http.Server{Addr: httpAddrs[id], Handler: kv.Handler(store, rf, httpAddrs, 3*time.Second)}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	log.Info("node started", "id", id, "raft", raftAddrs[id], "http", httpAddrs[id], "peers", peers)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errCh:
		rf.Stop()
		return err
	case s := <-sig:
		log.Info("shutting down", "signal", s.String())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
	rf.Stop()
	return nil
}
