package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/Navneesh004/raftkv/network"
)

// cluster is the test harness: n Raft peers on a simulated network, plus a
// checker that watches every apply channel and fails the test the moment two
// peers apply different commands at the same index.
type cluster struct {
	t          *testing.T
	n          int
	net        *network.Network
	cfg        Config
	snapshots  bool // peers snapshot every snapEvery entries
	rafts      []*Raft
	persisters []*MemoryPersister
	stops      []chan struct{}
	connected  []bool

	mu          sync.Mutex
	logs        []map[int]int // per peer: index -> command applied
	lastApplied []int
	applyErr    string
}

const snapEvery = 10

func newCluster(t *testing.T, n int, reliable, snapshots bool) *cluster {
	t.Helper()
	c := &cluster{
		t:           t,
		n:           n,
		net:         network.New(),
		cfg:         DefaultConfig(),
		snapshots:   snapshots,
		rafts:       make([]*Raft, n),
		persisters:  make([]*MemoryPersister, n),
		stops:       make([]chan struct{}, n),
		connected:   make([]bool, n),
		logs:        make([]map[int]int, n),
		lastApplied: make([]int, n),
	}
	c.net.SetReliable(reliable)
	for i := 0; i < n; i++ {
		c.logs[i] = map[int]int{}
		c.start(i)
	}
	for i := 0; i < n; i++ {
		c.connect(i)
	}
	t.Cleanup(c.cleanup)
	return c
}

func (c *cluster) cleanup() {
	for i := range c.rafts {
		if c.rafts[i] != nil {
			c.rafts[i].Kill()
		}
		if c.stops[i] != nil {
			close(c.stops[i])
			c.stops[i] = nil
		}
	}
	c.net.Close()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.applyErr != "" {
		c.t.Fatal(c.applyErr)
	}
}

// start (re)starts peer i from whatever its persister holds.
func (c *cluster) start(i int) {
	c.crash(i)
	if c.persisters[i] == nil {
		c.persisters[i] = NewMemoryPersister()
	}
	ends := make([]Peer, c.n)
	for j := 0; j < c.n; j++ {
		ends[j] = c.net.MakeEnd(i, j)
	}

	// A restarted peer replays its log from the snapshot point.
	c.mu.Lock()
	c.lastApplied[i] = 0
	if snap := c.persisters[i].ReadSnapshot(); len(snap) > 0 {
		if err := c.loadSnapshot(i, snap); err != "" {
			c.applyErr = err
		}
	}
	c.mu.Unlock()

	applyCh := make(chan ApplyMsg)
	rf := Make(ends, i, c.persisters[i], applyCh, c.cfg)
	c.mu.Lock()
	c.rafts[i] = rf
	c.mu.Unlock()
	stop := make(chan struct{})
	c.stops[i] = stop
	go c.applier(i, rf, applyCh, stop)

	srv := network.NewServer()
	srv.Register("Raft", rf.RPC())
	c.net.AddServer(i, srv)
}

// crash kills peer i. Its persisted state survives; anything it writes after
// this point is lost, as on a real crash.
func (c *cluster) crash(i int) {
	c.disconnect(i)
	c.net.Crash(i)
	if c.persisters[i] != nil {
		c.persisters[i] = c.persisters[i].Copy()
	}
	if c.rafts[i] != nil {
		c.rafts[i].Kill()
		c.mu.Lock()
		c.rafts[i] = nil
		c.mu.Unlock()
	}
	if c.stops[i] != nil {
		close(c.stops[i])
		c.stops[i] = nil
	}
}

// liveRafts returns a copy of the current peers, for use by goroutines other
// than the test goroutine (which is the only one that swaps peers).
func (c *cluster) liveRafts() []*Raft {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*Raft(nil), c.rafts...)
}

func (c *cluster) connect(i int) {
	c.connected[i] = true
	c.net.Connect(i)
}

func (c *cluster) disconnect(i int) {
	c.connected[i] = false
	c.net.Disconnect(i)
}

func (c *cluster) applier(i int, rf *Raft, ch chan ApplyMsg, stop chan struct{}) {
	for {
		var m ApplyMsg
		select {
		case <-stop:
			return
		case m = <-ch:
		}
		c.mu.Lock()
		var err string
		switch {
		case m.CommandValid:
			err = c.checkApplyLocked(i, m)
		case m.SnapshotValid:
			err = c.loadSnapshot(i, m.Snapshot)
		}
		if err != "" && c.applyErr == "" {
			c.applyErr = err
		}
		var snap []byte
		if c.snapshots && m.CommandValid && m.CommandIndex%snapEvery == 0 {
			snap = c.makeSnapshotLocked(i, m.CommandIndex)
		}
		c.mu.Unlock()
		if snap != nil {
			rf.Snapshot(m.CommandIndex, snap)
		}
	}
}

func (c *cluster) checkApplyLocked(i int, m ApplyMsg) string {
	cmd, ok := m.Command.(int)
	if !ok {
		return fmt.Sprintf("peer %d applied non-int command %v", i, m.Command)
	}
	if m.CommandIndex != c.lastApplied[i]+1 {
		return fmt.Sprintf("peer %d applied index %d out of order (last %d)", i, m.CommandIndex, c.lastApplied[i])
	}
	for j := range c.logs {
		if old, ok := c.logs[j][m.CommandIndex]; ok && old != cmd {
			return fmt.Sprintf("SAFETY: index %d: peer %d applied %d but peer %d applied %d",
				m.CommandIndex, i, cmd, j, old)
		}
	}
	c.logs[i][m.CommandIndex] = cmd
	c.lastApplied[i] = m.CommandIndex
	return ""
}

// The test "state machine" is the list of applied commands.
func (c *cluster) makeSnapshotLocked(i, index int) []byte {
	cmds := make([]int, index+1)
	for k := 1; k <= index; k++ {
		cmds[k] = c.logs[i][k]
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(cmds); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func (c *cluster) loadSnapshot(i int, data []byte) string {
	var cmds []int
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&cmds); err != nil {
		return fmt.Sprintf("peer %d: bad snapshot: %v", i, err)
	}
	index := len(cmds) - 1
	if index < c.lastApplied[i] {
		return fmt.Sprintf("peer %d: snapshot %d older than applied %d", i, index, c.lastApplied[i])
	}
	for k := 1; k <= index; k++ {
		for j := range c.logs {
			if old, ok := c.logs[j][k]; ok && old != cmds[k] {
				return fmt.Sprintf("SAFETY: snapshot index %d has %d but peer %d applied %d", k, cmds[k], j, old)
			}
		}
		c.logs[i][k] = cmds[k]
	}
	c.lastApplied[i] = index
	return ""
}

func (c *cluster) checkErr() {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.applyErr != "" {
		c.t.Fatal(c.applyErr)
	}
}

// checkOneLeader waits for exactly one leader among connected peers and
// fails if two peers ever claim leadership of the same term.
func (c *cluster) checkOneLeader() int {
	c.t.Helper()
	for iter := 0; iter < 10; iter++ {
		time.Sleep(time.Duration(450+rand.IntN(100)) * time.Millisecond)
		leaders := map[int][]int{}
		for i := 0; i < c.n; i++ {
			if c.connected[i] && c.rafts[i] != nil {
				if term, isLeader := c.rafts[i].GetState(); isLeader {
					leaders[term] = append(leaders[term], i)
				}
			}
		}
		lastTerm := -1
		for term, ids := range leaders {
			if len(ids) > 1 {
				c.t.Fatalf("term %d has %d leaders: %v", term, len(ids), ids)
			}
			if term > lastTerm {
				lastTerm = term
			}
		}
		if lastTerm != -1 {
			return leaders[lastTerm][0]
		}
	}
	c.t.Fatal("expected one leader, got none")
	return -1
}

// checkTerms fails unless every connected peer is in the same term.
func (c *cluster) checkTerms() int {
	c.t.Helper()
	term := -1
	for i := 0; i < c.n; i++ {
		if c.connected[i] && c.rafts[i] != nil {
			t, _ := c.rafts[i].GetState()
			if term == -1 {
				term = t
			} else if t != term {
				c.t.Fatalf("peers disagree on term: %d vs %d", term, t)
			}
		}
	}
	return term
}

// checkNoLeader fails if any connected peer thinks it is leader.
func (c *cluster) checkNoLeader() {
	c.t.Helper()
	for i := 0; i < c.n; i++ {
		if c.connected[i] && c.rafts[i] != nil {
			if _, isLeader := c.rafts[i].GetState(); isLeader {
				c.t.Fatalf("peer %d is leader, expected none", i)
			}
		}
	}
}

// nCommitted reports how many peers have applied index, and the command.
func (c *cluster) nCommitted(index int) (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.applyErr != "" {
		c.t.Fatal(c.applyErr)
	}
	count, cmd := 0, -1
	for i := range c.logs {
		if v, ok := c.logs[i][index]; ok {
			if count > 0 && v != cmd {
				c.t.Fatalf("index %d: committed values differ: %d vs %d", index, cmd, v)
			}
			count++
			cmd = v
		}
	}
	return count, cmd
}

// one submits cmd to whichever peer is leader and waits until at least
// `expected` peers have applied it. With retry, it re-submits if a leader
// change loses the command. Returns the index it committed at.
func (c *cluster) one(cmd, expected int, retry bool) int {
	c.t.Helper()
	t0 := time.Now()
	starts := 0
	for time.Since(t0) < 10*time.Second {
		index := -1
		for k := 0; k < c.n; k++ {
			starts = (starts + 1) % c.n
			if c.connected[starts] && c.rafts[starts] != nil {
				if idx, _, ok := c.rafts[starts].Start(cmd); ok {
					index = idx
					break
				}
			}
		}
		if index != -1 {
			t1 := time.Now()
			for time.Since(t1) < 2*time.Second {
				nd, got := c.nCommitted(index)
				if nd > 0 && nd >= expected && got == cmd {
					return index
				}
				time.Sleep(20 * time.Millisecond)
			}
			if !retry {
				c.t.Fatalf("one(%d) failed to reach agreement", cmd)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	c.t.Fatalf("one(%d) failed to reach agreement in 10s", cmd)
	return -1
}

// wait blocks until at least n peers applied index, or the term moves past
// startTerm (in which case the entry may never commit).
func (c *cluster) wait(index, n, startTerm int) int {
	c.t.Helper()
	to := 10 * time.Millisecond
	for iter := 0; iter < 30; iter++ {
		if nd, _ := c.nCommitted(index); nd >= n {
			break
		}
		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
		if startTerm > -1 {
			for _, rf := range c.rafts {
				if rf != nil {
					if t, _ := rf.GetState(); t > startTerm {
						return -1
					}
				}
			}
		}
	}
	nd, cmd := c.nCommitted(index)
	if nd < n {
		c.t.Fatalf("only %d peers applied index %d, wanted %d", nd, index, n)
	}
	return cmd
}
