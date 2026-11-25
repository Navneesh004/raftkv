package raft

import (
	"bytes"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Every peer crashes and restarts, several times; committed entries survive.
func TestPersistCrashAll(t *testing.T) {
	c := newCluster(t, 3, true, false)
	c.one(11, 3, true)

	for i := 0; i < 3; i++ {
		c.start(i)
	}
	for i := 0; i < 3; i++ {
		c.connect(i)
	}
	c.one(12, 3, true)

	leader1 := c.checkOneLeader()
	c.start(leader1)
	c.connect(leader1)
	c.one(13, 3, true)

	leader2 := c.checkOneLeader()
	c.disconnect(leader2)
	c.one(14, 2, true)
	c.start(leader2)
	c.connect(leader2)

	c.wait(4, 3, -1) // wait for leader2 to rejoin and catch up

	i3 := (c.checkOneLeader() + 1) % 3
	c.disconnect(i3)
	c.one(15, 2, true)
	c.start(i3)
	c.connect(i3)
	c.one(16, 3, true)
}

// Repeatedly crash and restart different peers, including the leader.
func TestPersistRepeatedCrashes(t *testing.T) {
	c := newCluster(t, 5, true, false)
	index := 1
	for iters := 0; iters < 5; iters++ {
		c.one(10+index, 5, true)
		index++

		leader1 := c.checkOneLeader()
		c.disconnect((leader1 + 1) % 5)
		c.disconnect((leader1 + 2) % 5)
		c.one(10+index, 3, true)
		index++

		c.crash((leader1 + 0) % 5)
		c.crash((leader1 + 3) % 5)
		c.crash((leader1 + 4) % 5)

		c.start((leader1 + 1) % 5)
		c.start((leader1 + 2) % 5)
		c.connect((leader1 + 1) % 5)
		c.connect((leader1 + 2) % 5)
		time.Sleep(time.Second)

		c.start((leader1 + 3) % 5)
		c.connect((leader1 + 3) % 5)
		c.one(10+index, 3, true)
		index++

		c.start((leader1 + 4) % 5)
		c.start((leader1 + 0) % 5)
		c.connect((leader1 + 4) % 5)
		c.connect((leader1 + 0) % 5)
	}
	c.one(1000, 5, true)
}

// The "Figure 8" scenario: leaders crash right after appending entries, so
// entries from old terms are on some peers but not on a majority. A leader
// that counted replicas of old-term entries would commit something that a
// later leader overwrites; the harness's checker would catch that.
func TestPersistFigure8(t *testing.T) {
	c := newCluster(t, 5, true, false)
	c.one(rand.Int(), 1, true)

	nup := 5
	for iters := 0; iters < 600; iters++ {
		leader := -1
		for i := 0; i < 5; i++ {
			if c.rafts[i] != nil {
				if _, _, ok := c.rafts[i].Start(rand.Int()); ok {
					leader = i
				}
			}
		}
		if rand.IntN(1000) < 100 {
			time.Sleep(time.Duration(rand.IntN(500)) * time.Millisecond)
		} else {
			time.Sleep(time.Duration(rand.IntN(13)) * time.Millisecond)
		}
		if leader != -1 {
			c.crash(leader)
			nup--
		}
		if nup < 3 {
			s := rand.IntN(5)
			if c.rafts[s] == nil {
				c.start(s)
				c.connect(s)
				nup++
			}
		}
	}
	for i := 0; i < 5; i++ {
		if c.rafts[i] == nil {
			c.start(i)
			c.connect(i)
		}
	}
	c.one(rand.Int(), 5, true)
}

// Agreement under a lossy, reordering network with many concurrent clients.
func TestPersistUnreliableAgree(t *testing.T) {
	c := newCluster(t, 5, false, false)
	var wg sync.WaitGroup
	for iters := 1; iters < 50; iters++ {
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func(iters, j int) {
				defer wg.Done()
				c.one(100*iters+j, 1, true)
			}(iters, j)
		}
		c.one(iters, 1, true)
	}
	c.net.SetReliable(true)
	wg.Wait()
	c.one(100, 5, true)
}

// Figure 8 again, but with a lossy network and partitions instead of crashes.
func TestPersistFigure8Unreliable(t *testing.T) {
	c := newCluster(t, 5, false, false)
	c.one(rand.IntN(10000), 1, true)

	nup := 5
	for iters := 0; iters < 600; iters++ {
		if iters == 200 {
			c.net.SetLongDelays(true)
		}
		leader := -1
		for i := 0; i < 5; i++ {
			if _, _, ok := c.rafts[i].Start(rand.IntN(10000)); ok && c.connected[i] {
				leader = i
			}
		}
		if rand.IntN(1000) < 100 {
			time.Sleep(time.Duration(rand.IntN(500)) * time.Millisecond)
		} else {
			time.Sleep(time.Duration(rand.IntN(13)) * time.Millisecond)
		}
		if leader != -1 && rand.IntN(1000) < 500 {
			c.disconnect(leader)
			nup--
		}
		if nup < 3 {
			s := rand.IntN(5)
			if !c.connected[s] {
				c.connect(s)
				nup++
			}
		}
	}
	c.net.SetLongDelays(false)
	for i := 0; i < 5; i++ {
		c.connect(i)
	}
	c.one(rand.IntN(10000), 5, true)
}

// Churn: clients submit commands while peers crash, restart, disconnect and
// reconnect at random. Every command a client saw committed must be in the
// final log.
func churn(t *testing.T, reliable bool) {
	c := newCluster(t, 5, reliable, false)
	var stop atomic.Bool

	client := func(me int, out chan []int) {
		var values []int
		defer func() { out <- values }()
		for !stop.Load() {
			x := rand.Int()
			index, ok := -1, false
			for _, rf := range c.liveRafts() {
				if rf == nil {
					continue
				}
				if idx, _, isLeader := rf.Start(x); isLeader {
					index, ok = idx, true
				}
			}
			if ok {
				for _, to := range []int{10, 20, 50, 100, 200} {
					if nd, cmd := c.nCommitted(index); nd > 0 {
						if cmd == x {
							values = append(values, x)
						}
						break
					}
					time.Sleep(time.Duration(to) * time.Millisecond)
				}
			} else {
				time.Sleep(time.Duration(79+me*17) * time.Millisecond)
			}
		}
	}

	const ncli = 3
	outs := make([]chan []int, ncli)
	for i := range outs {
		outs[i] = make(chan []int, 1)
		go client(i, outs[i])
	}
	for iters := 0; iters < 20; iters++ {
		if rand.IntN(1000) < 200 {
			c.disconnect(rand.IntN(5))
		}
		if rand.IntN(1000) < 500 {
			i := rand.IntN(5)
			if c.rafts[i] == nil {
				c.start(i)
			}
			c.connect(i)
		}
		if rand.IntN(1000) < 200 {
			i := rand.IntN(5)
			if c.rafts[i] != nil {
				c.crash(i)
			}
		}
		// Keep crashes rare enough that a majority is usually up.
		time.Sleep(c.cfg.ElectionTimeoutMax*7/10 + time.Duration(rand.IntN(50))*time.Millisecond)
	}
	time.Sleep(time.Second)
	c.net.SetReliable(true)
	for i := 0; i < 5; i++ {
		if c.rafts[i] == nil {
			c.start(i)
		}
		c.connect(i)
	}
	stop.Store(true)

	var values []int
	for _, ch := range outs {
		values = append(values, <-ch...)
	}
	time.Sleep(time.Second)
	last := c.one(rand.Int(), 5, true)

	really := map[int]bool{}
	for index := 1; index <= last; index++ {
		if v := c.wait(index, 5, -1); v != -1 {
			really[v] = true
		}
	}
	for _, v := range values {
		if !really[v] {
			t.Fatalf("committed value %d is missing from the final log", v)
		}
	}
}

func TestPersistChurnReliable(t *testing.T)   { churn(t, true) }
func TestPersistChurnUnreliable(t *testing.T) { churn(t, false) }

// --- FilePersister -----------------------------------------------------------

func TestFilePersisterRoundTrip(t *testing.T) {
	dir := t.TempDir()
	p, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.ReadRaftState()) != 0 {
		t.Fatal("fresh persister is not empty")
	}
	p.Save([]byte("state-1"), []byte("snap-1"))
	p.Save([]byte("state-2"), nil)

	q, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(q.ReadRaftState(), []byte("state-2")) || q.ReadSnapshot() != nil {
		t.Fatalf("reloaded %q / %q", q.ReadRaftState(), q.ReadSnapshot())
	}
}

// A crash halfway through a save leaves a half-written temp file. The real
// file must be untouched, and loading must ignore the temp file.
func TestFilePersisterTornWrite(t *testing.T) {
	dir := t.TempDir()
	p, _ := NewFilePersister(dir)
	p.Save([]byte("good"), []byte("snap"))

	full := encodeStateFile([]byte("newer state that never finished"), nil)
	if err := os.WriteFile(filepath.Join(dir, "raft.state.tmp"), full[:len(full)/2], 0o644); err != nil {
		t.Fatal(err)
	}
	q, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	if string(q.ReadRaftState()) != "good" || string(q.ReadSnapshot()) != "snap" {
		t.Fatalf("got %q / %q", q.ReadRaftState(), q.ReadSnapshot())
	}
}

// Flipped bits in the state file are detected rather than silently loaded.
func TestFilePersisterDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	p, _ := NewFilePersister(dir)
	p.Save([]byte("some raft state"), []byte("snapshot"))
	path := filepath.Join(dir, "raft.state")
	data, _ := os.ReadFile(path)
	data[len(data)-3] ^= 0xFF
	os.WriteFile(path, data, 0o644)
	if _, err := NewFilePersister(dir); err == nil {
		t.Fatal("corruption was not detected")
	}
}

// A Raft peer restarted on a FilePersister recovers its term, vote and log.
func TestFilePersisterRaftRestart(t *testing.T) {
	dir := t.TempDir()
	p, _ := NewFilePersister(dir)
	applyCh := make(chan ApplyMsg, 10)
	rf := Make([]Peer{nil}, 0, p, applyCh, DefaultConfig())
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, isLeader := rf.GetState(); isLeader {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("single node never became leader")
		}
		time.Sleep(20 * time.Millisecond)
	}
	rf.Start(7)
	rf.Start(8)
	<-applyCh
	<-applyCh
	term, _ := rf.GetState()
	rf.Kill()

	p2, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	applyCh2 := make(chan ApplyMsg, 10)
	rf2 := Make([]Peer{nil}, 0, p2, applyCh2, DefaultConfig())
	defer rf2.Kill()
	st := rf2.Status()
	if st.LastIndex != 2 || st.Term < term {
		t.Fatalf("after restart: last index %d term %d, want 2 and >= %d", st.LastIndex, st.Term, term)
	}
}
