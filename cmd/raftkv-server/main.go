// Command raftkv-server runs one replica of the replicated key-value store.
//
//	raftkv-server -id 0 -peers node0:7000,node1:7000,node2:7000 -data /data
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Navneesh004/raftkv/node"
	"github.com/Navneesh004/raftkv/raft"
)

func main() {
	id := flag.Int("id", 0, "this replica's index in -peers")
	peers := flag.String("peers", "", "comma-separated host:port of every replica, in id order")
	data := flag.String("data", "./data", "directory for persistent state")
	listen := flag.String("listen", "", "listen address (default: the port of this replica's entry in -peers)")
	httpAddr := flag.String("http", "", "optional address for the JSON /status endpoint, e.g. :8080")
	maxRaft := flag.Int("maxraftstate", 64<<10, "snapshot when the persisted Raft state reaches this many bytes (0 = never); every persist rewrites up to this much, so keep it small")
	eMin := flag.Duration("election-min", 300*time.Millisecond, "minimum election timeout")
	eMax := flag.Duration("election-max", 600*time.Millisecond, "maximum election timeout")
	hb := flag.Duration("heartbeat", 50*time.Millisecond, "leader heartbeat interval")
	batch := flag.Int("max-batch", 0, "max entries per AppendEntries (0 = unlimited)")
	noSync := flag.Bool("nosync", false, "skip fsync (unsafe; for benchmarking only)")
	flag.Parse()

	addrs := strings.Split(*peers, ",")
	if *peers == "" || *id < 0 || *id >= len(addrs) {
		fmt.Fprintln(os.Stderr, "need -peers and a valid -id")
		flag.Usage()
		os.Exit(2)
	}
	listenAddr := *listen
	if listenAddr == "" {
		listenAddr = addrs[*id][strings.LastIndex(addrs[*id], ":"):] // ":7000": all interfaces
	}

	logger := log.New(os.Stdout, fmt.Sprintf("[node%d] ", *id), log.Ltime|log.Lmicroseconds)
	cfg := node.Config{
		ID:           *id,
		Peers:        addrs,
		DataDir:      *data,
		ListenAddr:   listenAddr,
		MaxRaftState: *maxRaft,
		NoSync:       *noSync,
		Raft: raft.Config{
			ElectionTimeoutMin: *eMin,
			ElectionTimeoutMax: *eMax,
			HeartbeatInterval:  *hb,
			MaxEntriesPerRPC:   *batch,
			Logger:             logger,
		},
	}
	n, err := node.Start(cfg)
	if err != nil {
		logger.Fatal(err)
	}
	st := n.Status()
	logger.Printf("listening on %s; recovered term %d, log [%d..%d]", listenAddr, st.Term, st.FirstIndex, st.LastIndex)

	if *httpAddr != "" {
		http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(n.Status())
		})
		go func() { logger.Println(http.ListenAndServe(*httpAddr, nil)) }()
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	logger.Println("shutting down")
	n.Stop()
}
