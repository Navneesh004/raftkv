package node

import (
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/Navneesh004/raftkv/kv"
)

// End-to-end over real TCP and real files: a 3-replica cluster keeps
// serving after its leader crashes, the crashed replica recovers from disk
// and catches up, and data survives a restart of the whole cluster.
func TestClusterOverTCP(t *testing.T) {
	const n = 3
	dir := t.TempDir()
	addrs := make([]string, n)
	lns := make([]net.Listener, n)
	for i := range addrs {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns[i] = ln
		addrs[i] = ln.Addr().String()
	}
	cfg := func(i int) Config {
		return Config{ID: i, Peers: addrs, DataDir: filepath.Join(dir, fmt.Sprint(i)), MaxRaftState: 4096}
	}
	nodes := make([]*Node, n)
	for i := range nodes {
		c := cfg(i)
		c.Listener = lns[i]
		var err error
		if nodes[i], err = Start(c); err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		for _, nd := range nodes {
			if nd != nil {
				nd.Stop()
			}
		}
	}()

	ck := kv.MakeClerk(Endpoints(addrs, time.Second))
	for i := 0; i < 50; i++ {
		ck.Append("log", fmt.Sprintf("%d,", i))
	}

	leader := findLeader(t, nodes)
	nodes[leader].Stop()
	nodes[leader] = nil
	t0 := time.Now()
	ck.Put("after-crash", "yes")
	t.Logf("first write after leader crash took %v", time.Since(t0))
	for i := 50; i < 100; i++ {
		ck.Append("log", fmt.Sprintf("%d,", i))
	}

	// Restart the crashed replica from its data directory.
	var err error
	if nodes[leader], err = Start(cfg(leader)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		caughtUp := true
		target := nodes[(leader+1)%n].Status().CommitIndex
		if nodes[leader].Status().LastApplied < target {
			caughtUp = false
		}
		if caughtUp {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("restarted replica never caught up")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Full-cluster restart: everything comes back from disk.
	for i, nd := range nodes {
		nd.Stop()
		nodes[i] = nil
	}
	for i := range nodes {
		if nodes[i], err = Start(cfg(i)); err != nil {
			t.Fatal(err)
		}
	}
	ck = kv.MakeClerk(Endpoints(addrs, time.Second))
	want := ""
	for i := 0; i < 100; i++ {
		want += fmt.Sprintf("%d,", i)
	}
	if got := ck.Get("log"); got != want {
		t.Fatalf("after restart Get(log) = %q\nwant %q", got, want)
	}
	if got := ck.Get("after-crash"); got != "yes" {
		t.Fatalf("after restart Get(after-crash) = %q", got)
	}
}

func findLeader(t *testing.T, nodes []*Node) int {
	for iter := 0; iter < 100; iter++ {
		for i, nd := range nodes {
			if nd != nil && nd.Status().Role == "leader" {
				return i
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no leader")
	return -1
}
