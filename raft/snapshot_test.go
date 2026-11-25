package raft

import (
	"math/rand/v2"
	"testing"
)

// snapCommon drives a cluster that snapshots every few entries while a
// follower is disconnected or crashed, so it must catch up through
// InstallSnapshot rather than AppendEntries.
func snapCommon(t *testing.T, disconnect, reliable, crash bool) {
	const iters = 20
	c := newCluster(t, 3, reliable, true)
	c.one(rand.Int(), 3, true)
	leader1 := c.checkOneLeader()

	for i := 0; i < iters; i++ {
		victim := (leader1 + 1) % 3
		sender := leader1
		if i%3 == 1 {
			sender = (leader1 + 1) % 3
			victim = leader1
		}
		if disconnect {
			c.disconnect(victim)
			c.one(rand.Int(), 2, true)
		}
		if crash {
			c.crash(victim)
			c.one(rand.Int(), 2, true)
		}
		// Send enough entries to force at least one snapshot on the others.
		nn := snapEvery/2 + rand.IntN(snapEvery)
		for j := 0; j < nn; j++ {
			if rf := c.rafts[sender]; rf != nil {
				rf.Start(rand.Int())
			}
		}
		// Let applier threads catch up with the Start()s.
		if !disconnect && !crash {
			c.one(rand.Int(), 3, true)
		} else {
			c.one(rand.Int(), 2, true)
		}
		if size := c.persisters[sender].RaftStateSize(); size >= 8*1024 {
			t.Fatalf("raft state is %d bytes; snapshots are not trimming the log", size)
		}
		if disconnect {
			// The victim may need InstallSnapshot to catch up.
			c.connect(victim)
			c.one(rand.Int(), 3, true)
			leader1 = c.checkOneLeader()
		}
		if crash {
			c.start(victim)
			c.connect(victim)
			c.one(rand.Int(), 3, true)
			leader1 = c.checkOneLeader()
		}
	}
	c.checkErr()
}

func TestSnapshotBasic(t *testing.T)                  { snapCommon(t, false, true, false) }
func TestSnapshotInstall(t *testing.T)                { snapCommon(t, true, true, false) }
func TestSnapshotInstallUnreliable(t *testing.T)      { snapCommon(t, true, false, false) }
func TestSnapshotInstallCrash(t *testing.T)           { snapCommon(t, false, true, true) }
func TestSnapshotInstallUnreliableCrash(t *testing.T) { snapCommon(t, false, false, true) }

// A peer restarted from a snapshot (with nothing after it in its log) must
// come back with the right lastApplied and keep agreeing with the others.
func TestSnapshotInit(t *testing.T) {
	c := newCluster(t, 3, true, true)
	c.one(rand.Int(), 3, true)
	for i := 0; i < snapEvery+1; i++ {
		c.one(rand.Int(), 3, true)
	}
	for i := 0; i < 3; i++ {
		c.crash(i)
	}
	for i := 0; i < 3; i++ {
		c.start(i)
		c.connect(i)
	}
	c.one(rand.Int(), 3, true)
	for i := 0; i < 3; i++ {
		c.crash(i)
	}
	for i := 0; i < 3; i++ {
		c.start(i)
		c.connect(i)
	}
	c.one(rand.Int(), 3, true)
}
