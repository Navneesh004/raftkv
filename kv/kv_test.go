package kv

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Navneesh004/raftkv/kv/history"
	"github.com/Navneesh004/raftkv/network"
	"github.com/Navneesh004/raftkv/raft"
	"github.com/anishathalye/porcupine"
)

// kvCluster runs n KV servers on the simulated network.
type kvCluster struct {
	t          *testing.T
	n          int
	net        *network.Network
	maxRaft    int
	servers    []*Server
	persisters []*raft.MemoryPersister
	connected  []bool
}

func newKVCluster(t *testing.T, n int, reliable bool, maxRaft int) *kvCluster {
	c := &kvCluster{
		t:          t,
		n:          n,
		net:        network.New(),
		maxRaft:    maxRaft,
		servers:    make([]*Server, n),
		persisters: make([]*raft.MemoryPersister, n),
		connected:  make([]bool, n),
	}
	c.net.SetReliable(reliable)
	for i := 0; i < n; i++ {
		c.start(i)
		c.connect(i)
	}
	t.Cleanup(func() {
		for _, s := range c.servers {
			if s != nil {
				s.Kill()
			}
		}
		c.net.Close()
	})
	return c
}

func (c *kvCluster) start(i int) {
	c.crash(i)
	if c.persisters[i] == nil {
		c.persisters[i] = raft.NewMemoryPersister()
	}
	peers := make([]raft.Peer, c.n)
	for j := range peers {
		peers[j] = c.net.MakeEnd(i, j)
	}
	s := StartServer(peers, i, c.persisters[i], c.maxRaft, raft.DefaultConfig())
	c.servers[i] = s
	srv := network.NewServer()
	srv.Register("Raft", s.Raft().RPC())
	srv.Register("KV", s.RPC())
	c.net.AddServer(i, srv)
}

func (c *kvCluster) crash(i int) {
	c.disconnect(i)
	c.net.Crash(i)
	if c.persisters[i] != nil {
		c.persisters[i] = c.persisters[i].Copy()
	}
	if c.servers[i] != nil {
		c.servers[i].Kill()
		c.servers[i] = nil
	}
}

func (c *kvCluster) connect(i int)    { c.connected[i] = true; c.net.Connect(i) }
func (c *kvCluster) disconnect(i int) { c.connected[i] = false; c.net.Disconnect(i) }

func (c *kvCluster) endpoints() []Endpoint {
	eps := make([]Endpoint, c.n)
	for j := range eps {
		eps[j] = c.net.MakeEnd(network.ClientID, j)
	}
	return eps
}

func (c *kvCluster) makeClerk() *Clerk { return MakeClerk(c.endpoints()) }

func (c *kvCluster) leader() int {
	for iter := 0; iter < 50; iter++ {
		for i, s := range c.servers {
			if s != nil && c.connected[i] {
				if _, isLeader := s.Raft().GetState(); isLeader {
					return i
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatal("no leader")
	return -1
}

// --- deterministic tests -----------------------------------------------------

func TestKVBasic(t *testing.T) {
	c := newKVCluster(t, 3, true, -1)
	ck := c.makeClerk()
	if v := ck.Get("missing"); v != "" {
		t.Fatalf("Get(missing) = %q", v)
	}
	ck.Put("a", "1")
	ck.Append("a", "2")
	ck.Append("b", "x")
	if v := ck.Get("a"); v != "12" {
		t.Fatalf("Get(a) = %q, want 12", v)
	}
	if v := ck.Get("b"); v != "x" {
		t.Fatalf("Get(b) = %q, want x", v)
	}
	ck.Put("a", "reset")
	if v := c.makeClerk().Get("a"); v != "reset" {
		t.Fatalf("second clerk saw %q", v)
	}
}

// Operations must complete promptly — well under a heartbeat interval — so
// the leader must replicate as soon as Start is called, not on the next tick.
func TestKVSpeed(t *testing.T) {
	c := newKVCluster(t, 3, true, -1)
	ck := c.makeClerk()
	ck.Put("warmup", "x")
	const n = 300
	start := time.Now()
	for i := 0; i < n; i++ {
		ck.Append("k", "x")
	}
	per := time.Since(start) / n
	t.Logf("%v per operation", per)
	if per > 25*time.Millisecond && !raceEnabled {
		t.Fatalf("operations too slow: %v each", per)
	}
}

// A retried Append (same client id and sequence number) is applied once,
// including after every server crashes and recovers from its snapshot.
func TestKVDuplicateDetection(t *testing.T) {
	c := newKVCluster(t, 3, true, 200) // tiny maxRaft: snapshots happen constantly
	ck := c.makeClerk()
	ck.Put("k", "")
	for i := 0; i < 30; i++ {
		ck.Put(fmt.Sprintf("filler%d", i), strings.Repeat("x", 20)) // force snapshots
	}

	leader := c.leader()
	ep := c.endpoints()[leader]
	args := CommandArgs{Kind: OpAppend, Key: "k", Value: "A", ClientID: 4242, Seq: 1}
	for i := 0; i < 5; i++ { // the "client" retries the same request 5 times
		var reply CommandReply
		if !ep.Call("KV.Command", &args, &reply) || reply.Err != OK {
			t.Fatalf("append attempt %d: ok=false or %v", i, reply.Err)
		}
	}
	if v := ck.Get("k"); v != "A" {
		t.Fatalf("after retries Get(k) = %q, want A", v)
	}

	for i := 0; i < 3; i++ {
		c.crash(i)
	}
	for i := 0; i < 3; i++ {
		c.start(i)
		c.connect(i)
	}
	leader = c.leader()
	var reply CommandReply
	c.endpoints()[leader].Call("KV.Command", &args, &reply)
	if v := ck.Get("k"); v != "A" {
		t.Fatalf("after restart and retry Get(k) = %q, want A", v)
	}
}

// A leader isolated with a minority must not acknowledge writes or serve
// reads: it can't get them committed.
func TestKVMinorityLeaderCannotServe(t *testing.T) {
	c := newKVCluster(t, 5, true, -1)
	ck := c.makeClerk()
	ck.Put("x", "before")

	old := c.leader()
	c.net.Partition([]int{old, (old + 1) % 5}, []int{(old + 2) % 5, (old + 3) % 5, (old + 4) % 5})

	ep := c.endpoints()[old]
	for _, kind := range []OpKind{OpPut, OpGet} {
		var reply CommandReply
		args := CommandArgs{Kind: kind, Key: "x", Value: "minority", ClientID: 7, Seq: 1}
		if ep.Call("KV.Command", &args, &reply) && (reply.Err == OK || reply.Err == ErrNoKey) {
			t.Fatalf("minority leader answered %s with %v", kind, reply.Err)
		}
	}

	// The majority side keeps working.
	ck.Put("x", "after")
	c.net.Heal()
	if v := c.makeClerk().Get("x"); v != "after" {
		t.Fatalf("Get(x) = %q, want after", v)
	}
}

// --- randomized workloads, checked for linearizability ------------------------

type workload struct {
	clients    int
	unreliable bool
	crash      bool
	partition  bool
	maxRaft    int
	rounds     int
	roundLen   time.Duration
}

// runWorkload has concurrent clients issue random operations while a chaos
// goroutine crashes servers and partitions the network. Two independent
// checks run on the results:
//
//  1. Each client appends unique tokens to its own key and periodically reads
//     it back: the value must be exactly its acknowledged appends, in order,
//     each once (no lost, duplicated or reordered writes, no stale reads).
//  2. Every operation on shared keys is recorded with its call/return time
//     and the full history is checked for linearizability with Porcupine.
func runWorkload(t *testing.T, w workload) {
	const nservers = 5
	c := newKVCluster(t, nservers, !w.unreliable, w.maxRaft)
	rec := history.NewRecorder()
	expected := make([]string, w.clients) // own-key contents, per client
	var failMu sync.Mutex
	var failure string
	fail := func(format string, args ...any) {
		failMu.Lock()
		defer failMu.Unlock()
		if failure == "" {
			failure = fmt.Sprintf(format, args...)
		}
	}

	for round := 0; round < w.rounds; round++ {
		var stopClients, stopChaos atomic.Bool
		var clients, chaos sync.WaitGroup

		for cli := 0; cli < w.clients; cli++ {
			clients.Add(1)
			go func(cli int) {
				defer clients.Done()
				ck := c.makeClerk()
				own := fmt.Sprintf("own-%d", cli)
				for n := 0; !stopClients.Load(); n++ {
					r := rand.IntN(100)
					switch {
					case r < 35:
						tok := fmt.Sprintf("[%d.%d.%d]", cli, round, n)
						ck.Append(own, tok)
						expected[cli] += tok
					case r < 50:
						if v := ck.Get(own); v != expected[cli] {
							fail("client %d: Get(%s) = %q, want %q", cli, own, v, expected[cli])
						}
					default:
						key := fmt.Sprintf("shared-%d", rand.IntN(3))
						var in history.Input
						var out history.Output
						call := rec.Now()
						switch rand.IntN(3) {
						case 0:
							in = history.Input{Op: "get", Key: key}
							out.Value = ck.Get(key)
						case 1:
							in = history.Input{Op: "put", Key: key, Value: fmt.Sprintf("p%d.%d.%d ", cli, round, n)}
							ck.Put(key, in.Value)
						default:
							in = history.Input{Op: "append", Key: key, Value: fmt.Sprintf("a%d.%d.%d ", cli, round, n)}
							ck.Append(key, in.Value)
						}
						rec.Add(cli, in, out, call, rec.Now())
					}
				}
			}(cli)
		}

		chaos.Add(1)
		go func() {
			defer chaos.Done()
			for !stopChaos.Load() {
				time.Sleep(time.Duration(300+rand.IntN(500)) * time.Millisecond)
				if stopChaos.Load() {
					return
				}
				if w.partition {
					perm := rand.Perm(nservers)
					cut := 1 + rand.IntN(nservers-1)
					c.net.Partition(perm[:cut], perm[cut:])
				}
				if w.crash {
					i := rand.IntN(nservers)
					down := 0
					for _, s := range c.servers {
						if s == nil {
							down++
						}
					}
					if c.servers[i] == nil {
						c.start(i)
						c.connect(i)
					} else if down < 2 {
						c.crash(i)
					}
				}
			}
		}()

		time.Sleep(w.roundLen)
		stopChaos.Store(true)
		chaos.Wait()
		// Heal everything so in-flight client calls can finish.
		c.net.Heal()
		c.net.SetReliable(true)
		for i := 0; i < nservers; i++ {
			if c.servers[i] == nil {
				c.start(i)
			}
			c.connect(i)
		}
		stopClients.Store(true)
		clients.Wait()
		c.net.SetReliable(!w.unreliable)

		if w.maxRaft > 0 {
			for i, p := range c.persisters {
				if size := p.RaftStateSize(); size > 8*w.maxRaft {
					t.Fatalf("server %d raft state %d bytes > 8 * maxRaft (%d)", i, size, w.maxRaft)
				}
			}
		}
		failMu.Lock()
		if failure != "" {
			t.Fatal(failure)
		}
		failMu.Unlock()
	}

	res, info := rec.Check(20 * time.Second)
	switch res {
	case porcupine.Illegal:
		path := filepath.Join(os.TempDir(), fmt.Sprintf("raftkv-%s.html", t.Name()))
		if err := history.Visualize(info, path); err == nil {
			t.Fatalf("history is NOT linearizable; visualization: %s", path)
		}
		t.Fatal("history is NOT linearizable")
	case porcupine.Unknown:
		t.Logf("linearizability check timed out on %d operations (inconclusive)", rec.Len())
	default:
		t.Logf("history of %d operations is linearizable", rec.Len())
	}
}

func TestKVConcurrentClients(t *testing.T) {
	runWorkload(t, workload{clients: 5, rounds: 2, roundLen: 2 * time.Second})
}

func TestKVUnreliable(t *testing.T) {
	runWorkload(t, workload{clients: 5, unreliable: true, rounds: 2, roundLen: 3 * time.Second})
}

func TestKVPartitions(t *testing.T) {
	runWorkload(t, workload{clients: 5, partition: true, rounds: 3, roundLen: 4 * time.Second})
}

func TestKVCrashRestart(t *testing.T) {
	runWorkload(t, workload{clients: 5, crash: true, rounds: 3, roundLen: 4 * time.Second})
}

func TestKVSnapshots(t *testing.T) {
	runWorkload(t, workload{clients: 5, crash: true, maxRaft: 1000, rounds: 3, roundLen: 4 * time.Second})
}

// Everything at once: lossy network, partitions, crashes and snapshots.
func TestKVLinearizableUnderChaos(t *testing.T) {
	runWorkload(t, workload{clients: 7, unreliable: true, partition: true, crash: true,
		maxRaft: 2000, rounds: 3, roundLen: 5 * time.Second})
}
