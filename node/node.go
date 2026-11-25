// Package node assembles one deployable replica: a KV server on top of a
// Raft peer, persisted to disk, talking to its peers over TCP.
package node

import (
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/Navneesh004/raftkv/kv"
	"github.com/Navneesh004/raftkv/raft"
	"github.com/Navneesh004/raftkv/transport"
)

// Config describes one replica.
type Config struct {
	ID           int
	Peers        []string // address of every replica, indexed by id
	DataDir      string
	MaxRaftState int // snapshot threshold in bytes; <= 0 disables snapshots
	Raft         raft.Config
	RPCTimeout   time.Duration // peer-to-peer call timeout
	NoSync       bool          // skip fsync (benchmarks only)
	// Where to listen. If Listener is set it is used as is; otherwise
	// ListenAddr, defaulting to Peers[ID].
	Listener   net.Listener
	ListenAddr string
}

// Node is a running replica.
type Node struct {
	cfg     Config
	kv      *kv.Server
	srv     *transport.Server
	clients []*transport.Client
}

// Start brings up a replica, recovering any state found in DataDir.
func Start(cfg Config) (*Node, error) {
	if cfg.ID < 0 || cfg.ID >= len(cfg.Peers) {
		return nil, fmt.Errorf("id %d out of range for %d peers", cfg.ID, len(cfg.Peers))
	}
	if cfg.DataDir == "" {
		return nil, errors.New("data dir required")
	}
	if cfg.RPCTimeout == 0 {
		cfg.RPCTimeout = 500 * time.Millisecond
	}
	if cfg.Raft.ElectionTimeoutMin == 0 {
		logger := cfg.Raft.Logger
		cfg.Raft = raft.DefaultConfig()
		cfg.Raft.Logger = logger
	}

	persister, err := raft.NewFilePersister(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	persister.SetNoSync(cfg.NoSync)

	ln := cfg.Listener
	if ln == nil {
		addr := cfg.ListenAddr
		if addr == "" {
			addr = cfg.Peers[cfg.ID]
		}
		if ln, err = net.Listen("tcp", addr); err != nil {
			return nil, err
		}
	}

	n := &Node{cfg: cfg, srv: transport.NewServer()}
	peers := make([]raft.Peer, len(cfg.Peers))
	for i, addr := range cfg.Peers {
		c := transport.NewClient(addr, cfg.RPCTimeout)
		n.clients = append(n.clients, c)
		peers[i] = c
	}
	n.kv = kv.StartServer(peers, cfg.ID, persister, cfg.MaxRaftState, cfg.Raft)
	if err := n.srv.Register("Raft", n.kv.Raft().RPC()); err != nil {
		return nil, err
	}
	if err := n.srv.Register("KV", n.kv.RPC()); err != nil {
		return nil, err
	}
	go n.srv.Serve(ln)
	return n, nil
}

// Stop shuts the replica down abruptly: connections are dropped and nothing
// is flushed beyond what Raft already persisted — the same as a crash.
func (n *Node) Stop() {
	n.srv.Close()
	n.kv.Kill()
	for _, c := range n.clients {
		c.Close()
	}
}

// Status returns the replica's Raft state.
func (n *Node) Status() raft.Status { return n.kv.Raft().Status() }

// Endpoints returns client connections to every replica, for kv.MakeClerk.
// timeout should exceed the server's command timeout (800ms).
func Endpoints(addrs []string, timeout time.Duration) []kv.Endpoint {
	eps := make([]kv.Endpoint, len(addrs))
	for i, a := range addrs {
		eps[i] = transport.NewClient(a, timeout)
	}
	return eps
}
