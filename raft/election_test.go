package raft

import (
	"math/rand/v2"
	"testing"
	"time"
)

// One leader is elected at startup, and it stays leader while nothing fails.
func TestElectionInitial(t *testing.T) {
	c := newCluster(t, 3, true, false)
	c.checkOneLeader()

	time.Sleep(50 * time.Millisecond)
	term1 := c.checkTerms()
	if term1 < 1 {
		t.Fatalf("term is %d, expected >= 1", term1)
	}
	// With no failures there should be no new elections.
	time.Sleep(2 * time.Second)
	if term2 := c.checkTerms(); term1 != term2 {
		t.Logf("warning: term changed from %d to %d with no failures", term1, term2)
	}
	c.checkOneLeader()
}

// A new leader takes over when the old one is disconnected, the old leader
// rejoining doesn't disturb it, and with no majority there is no leader.
func TestElectionReElection(t *testing.T) {
	c := newCluster(t, 3, true, false)
	leader1 := c.checkOneLeader()

	c.disconnect(leader1)
	c.checkOneLeader()

	// The old leader rejoins; it must not end up as a second leader.
	c.connect(leader1)
	leader2 := c.checkOneLeader()

	// Without a majority, nobody can be elected.
	c.disconnect(leader2)
	c.disconnect((leader2 + 1) % 3)
	time.Sleep(2 * time.Second)
	c.checkNoLeader()

	// Restoring a majority elects a leader again.
	c.connect((leader2 + 1) % 3)
	c.checkOneLeader()

	c.connect(leader2)
	c.checkOneLeader()
}

// Repeated random disconnects of up to a minority of a 7-node cluster
// always leave exactly one leader per term.
func TestElectionMany(t *testing.T) {
	c := newCluster(t, 7, true, false)
	c.checkOneLeader()
	for iter := 0; iter < 10; iter++ {
		i1, i2, i3 := rand.IntN(7), rand.IntN(7), rand.IntN(7)
		c.disconnect(i1)
		c.disconnect(i2)
		c.disconnect(i3)
		// At most 3 of 7 are down, so a majority remains.
		c.checkOneLeader()
		c.connect(i1)
		c.connect(i2)
		c.connect(i3)
	}
	c.checkOneLeader()
}

// Leaders are sampled continuously while the network is lossy and partitions
// keep changing; no term may ever have two leaders.
func TestElectionNoTwoLeadersSameTerm(t *testing.T) {
	c := newCluster(t, 5, false, false)
	leaderOf := map[int]int{} // term -> leader
	deadline := time.Now().Add(6 * time.Second)
	nextShuffle := time.Now()
	for time.Now().Before(deadline) {
		if time.Now().After(nextShuffle) {
			perm := rand.Perm(5)
			cut := 1 + rand.IntN(3)
			c.net.Partition(perm[:cut], perm[cut:])
			nextShuffle = time.Now().Add(time.Duration(300+rand.IntN(700)) * time.Millisecond)
		}
		for i, rf := range c.rafts {
			if term, isLeader := rf.GetState(); isLeader {
				if other, ok := leaderOf[term]; ok && other != i {
					t.Fatalf("term %d has two leaders: %d and %d", term, other, i)
				}
				leaderOf[term] = i
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.net.Heal()
	c.checkOneLeader()
	if len(leaderOf) < 2 {
		t.Logf("only saw %d terms with a leader", len(leaderOf))
	}
}

// A minority partition never elects a leader, however long it waits, while
// the majority side does.
func TestElectionMinorityCannotElect(t *testing.T) {
	c := newCluster(t, 5, true, false)
	leader := c.checkOneLeader()
	oldTerm, _ := c.rafts[leader].GetState()

	minority := []int{leader, (leader + 1) % 5}
	majority := []int{(leader + 2) % 5, (leader + 3) % 5, (leader + 4) % 5}
	c.net.Partition(minority, majority)

	// The majority elects a new leader; the old leader may still believe it
	// leads its old term, but nobody in the minority may win a newer term.
	deadline := time.Now().Add(3 * time.Second)
	sawMajorityLeader := false
	for time.Now().Before(deadline) {
		for _, i := range minority {
			term, isLeader := c.rafts[i].GetState()
			if isLeader && term > oldTerm {
				t.Fatalf("minority peer %d became leader in term %d", i, term)
			}
		}
		for _, i := range majority {
			if _, isLeader := c.rafts[i].GetState(); isLeader {
				sawMajorityLeader = true
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !sawMajorityLeader {
		t.Fatal("majority side never elected a leader")
	}
	c.net.Heal()
	c.checkOneLeader()
}
