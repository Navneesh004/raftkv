package raft

import (
	"math/rand/v2"
	"sync"
	"testing"
	"time"
)

// All peers agree on a series of commands under normal operation.
func TestAgreeBasic(t *testing.T) {
	c := newCluster(t, 3, true, false)
	for index := 1; index <= 5; index++ {
		if nd, _ := c.nCommitted(index); nd > 0 {
			t.Fatal("entry committed before Start")
		}
		if got := c.one(index*100, 3, false); got != index {
			t.Fatalf("got index %d, expected %d", got, index)
		}
	}
}

// Agreement continues with one follower down, and the follower catches up
// when it comes back.
func TestAgreeFollowerFailure(t *testing.T) {
	c := newCluster(t, 3, true, false)
	c.one(101, 3, false)

	leader := c.checkOneLeader()
	c.disconnect((leader + 1) % 3)

	c.one(102, 2, false)
	time.Sleep(time.Second)
	c.one(103, 2, false)

	c.connect((leader + 1) % 3)
	c.one(104, 3, true)
	time.Sleep(time.Second)
	c.one(105, 3, true)
}

// Nothing commits without a majority; once the majority is back, the
// cluster recovers and continues.
func TestAgreeNoMajority(t *testing.T) {
	c := newCluster(t, 5, true, false)
	c.one(10, 5, false)

	leader := c.checkOneLeader()
	c.disconnect((leader + 1) % 5)
	c.disconnect((leader + 2) % 5)
	c.disconnect((leader + 3) % 5)

	index, _, ok := c.rafts[leader].Start(20)
	if !ok {
		t.Fatal("leader rejected Start")
	}
	if index != 2 {
		t.Fatalf("expected index 2, got %d", index)
	}
	time.Sleep(2 * time.Second)
	if n, _ := c.nCommitted(index); n > 0 {
		t.Fatalf("%d peers committed without a majority", n)
	}

	c.connect((leader + 1) % 5)
	c.connect((leader + 2) % 5)
	c.connect((leader + 3) % 5)
	// The disconnected majority may have elected its own leader meanwhile.
	leader2 := c.checkOneLeader()
	if _, _, ok := c.rafts[leader2].Start(30); !ok {
		t.Fatal("new leader rejected Start")
	}
	c.one(1000, 5, true)
}

// Many concurrent Starts all end up in the log, each at a distinct index.
func TestAgreeConcurrentStarts(t *testing.T) {
	c := newCluster(t, 3, true, false)
	var success bool
	for try := 0; try < 5 && !success; try++ {
		if try > 0 {
			time.Sleep(3 * time.Second)
		}
		leader := c.checkOneLeader()
		_, term, ok := c.rafts[leader].Start(1)
		if !ok {
			continue
		}
		var wg sync.WaitGroup
		var mu sync.Mutex
		indexes := []int{}
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				idx, term1, ok := c.rafts[leader].Start(100 + i)
				if ok && term1 == term {
					mu.Lock()
					indexes = append(indexes, idx)
					mu.Unlock()
				}
			}(i)
		}
		wg.Wait()
		if t2, _ := c.rafts[leader].GetState(); t2 != term {
			continue // leadership changed; try again
		}
		cmds := map[int]bool{}
		failed := false
		for _, idx := range indexes {
			cmd := c.wait(idx, 3, term)
			if cmd == -1 {
				failed = true
				break
			}
			cmds[cmd] = true
		}
		if failed {
			continue
		}
		for i := 0; i < 5; i++ {
			if !cmds[100+i] {
				t.Fatalf("command %d missing", 100+i)
			}
		}
		success = true
	}
	if !success {
		t.Fatal("term changed too often")
	}
}

// A leader cut off in a minority can't commit, and its uncommitted entries
// are overwritten when the network heals.
func TestAgreePartitionedLeaderRejoins(t *testing.T) {
	c := newCluster(t, 3, true, false)
	c.one(101, 3, true)

	leader1 := c.checkOneLeader()
	c.disconnect(leader1)
	// The isolated leader accepts commands it can never commit.
	c.rafts[leader1].Start(102)
	c.rafts[leader1].Start(103)
	c.rafts[leader1].Start(104)

	c.one(103, 2, true) // the majority moves on

	leader2 := c.checkOneLeader()
	c.disconnect(leader2)
	c.connect(leader1) // old leader is back, with its stale entries

	c.one(104, 2, true)

	c.connect(leader2)
	c.one(105, 3, true)
}

// Followers far behind with long divergent logs catch up quickly (this
// exercises the fast-backup optimisation).
func TestAgreeBackup(t *testing.T) {
	c := newCluster(t, 5, true, false)
	c.one(rand.Int(), 5, true)

	// Put the leader and one follower in a minority partition.
	leader1 := c.checkOneLeader()
	c.disconnect((leader1 + 2) % 5)
	c.disconnect((leader1 + 3) % 5)
	c.disconnect((leader1 + 4) % 5)

	// Lots of commands that won't commit.
	for i := 0; i < 50; i++ {
		c.rafts[leader1].Start(rand.Int())
	}
	time.Sleep(500 * time.Millisecond)

	c.disconnect((leader1 + 0) % 5)
	c.disconnect((leader1 + 1) % 5)

	// Allow the other partition to recover and commit lots.
	c.connect((leader1 + 2) % 5)
	c.connect((leader1 + 3) % 5)
	c.connect((leader1 + 4) % 5)
	for i := 0; i < 50; i++ {
		c.one(rand.Int(), 3, true)
	}

	// Now another partitioned leader and one follower.
	leader2 := c.checkOneLeader()
	other := (leader1 + 2) % 5
	if leader2 == other {
		other = (leader2 + 1) % 5
	}
	c.disconnect(other)

	// Lots more commands that won't commit.
	for i := 0; i < 50; i++ {
		c.rafts[leader2].Start(rand.Int())
	}
	time.Sleep(500 * time.Millisecond)

	// Bring the original leader back to life.
	for i := 0; i < 5; i++ {
		c.disconnect(i)
	}
	c.connect((leader1 + 0) % 5)
	c.connect((leader1 + 1) % 5)
	c.connect(other)
	for i := 0; i < 50; i++ {
		c.one(rand.Int(), 3, true)
	}

	// Everyone back.
	for i := 0; i < 5; i++ {
		c.connect(i)
	}
	c.one(rand.Int(), 5, true)
}

// An idle cluster and a busy one should not use an unreasonable number of
// RPCs (catches heartbeat storms and replication loops that never settle).
func TestAgreeRPCCount(t *testing.T) {
	c := newCluster(t, 3, true, false)
	leader := c.checkOneLeader()

	before := c.net.TotalRPCs()
	if before > 60 {
		t.Fatalf("too many RPCs (%d) for one election", before)
	}

	var term int
	var ok bool
	for try := 0; try < 5; try++ {
		start := c.net.TotalRPCs()
		var index int
		index, term, ok = c.rafts[leader].Start(1)
		if !ok {
			leader = c.checkOneLeader()
			continue
		}
		iters := 10
		for i := 1; i < iters+2; i++ {
			idx, term1, ok1 := c.rafts[leader].Start(100 + i)
			if !ok1 || term1 != term {
				ok = false
				break
			}
			if idx != index+i {
				t.Fatal("Start used the wrong index")
			}
		}
		if !ok {
			continue
		}
		for i := 1; i < iters+1; i++ {
			if c.wait(index+i, 3, term) == -1 {
				ok = false
				break
			}
		}
		if !ok {
			continue
		}
		total := c.net.TotalRPCs() - start
		if total > (iters+1+3)*3 {
			t.Fatalf("too many RPCs (%d) for %d entries", total, iters)
		}
		break
	}
	if !ok {
		t.Fatal("term changed too often")
	}

	time.Sleep(time.Second)
	idle := c.net.TotalRPCs()
	time.Sleep(time.Second)
	if perSec := c.net.TotalRPCs() - idle; perSec > 3*2*25 {
		t.Fatalf("too many RPCs (%d) per idle second", perSec)
	}
}
