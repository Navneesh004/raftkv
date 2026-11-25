// Package raft implements the Raft consensus algorithm as described in
// "In Search of an Understandable Consensus Algorithm (Extended Version)"
// by Diego Ongaro and John Ousterhout.
//
// A service (e.g. the key-value store in package kv) creates one Raft peer per
// server, hands commands to the leader with Start, and receives every
// committed command, in log order, on the apply channel.
//
// The code is split by concern:
//
//	raft.go         state, public API, apply loop
//	election.go     election timer, RequestVote (§5.2, §5.4.1)
//	replication.go  AppendEntries, commit rules, fast backup (§5.3, §5.4.2)
//	snapshot.go     log compaction and InstallSnapshot (§7)
//	persist.go      crash-safe persistence of term, vote and log (§5.1)
package raft

import (
	"log"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// Role is the state a peer is in.
type Role int

// The three Raft roles.
const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	default:
		return "leader"
	}
}

// Entry is one slot of the replicated log.
type Entry struct {
	Index   int
	Term    int
	Command any
}

// ApplyMsg is sent on the apply channel for every committed command, or when
// the service must load a snapshot received from the leader.
type ApplyMsg struct {
	CommandValid bool
	Command      any
	CommandIndex int
	CommandTerm  int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex int
	SnapshotTerm  int
}

// Peer is how a Raft peer talks to another one. Call returns false if the
// RPC could not be completed (lost request, lost reply, timeout, crash).
// Both the simulated test network and the TCP transport implement it.
type Peer interface {
	Call(method string, args any, reply any) bool
}

// Config holds the timing and tuning knobs.
type Config struct {
	// Election timeout is drawn uniformly from [Min, Max) every time the
	// timer is reset. Randomisation is what keeps split votes rare (§5.2).
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	// HeartbeatInterval is how often an idle leader sends empty AppendEntries.
	HeartbeatInterval time.Duration
	// MaxEntriesPerRPC caps how many entries one AppendEntries may carry.
	// 0 means unlimited (batching on); 1 disables batching.
	MaxEntriesPerRPC int
	// DisableFastBackup makes the leader step nextIndex back one entry per
	// rejection instead of one term per rejection. Only for experiments.
	DisableFastBackup bool
	// DisableGroupCommit makes the leader fsync each new entry inside
	// Start, holding the lock. Only for experiments.
	DisableGroupCommit bool
	// Logger, if set, receives one line per role change.
	Logger *log.Logger
}

// DefaultConfig returns settings suitable for both tests and a LAN cluster.
func DefaultConfig() Config {
	return Config{
		ElectionTimeoutMin: 300 * time.Millisecond,
		ElectionTimeoutMax: 600 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
	}
}

// Raft is a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []Peer
	me        int
	persister Persister
	cfg       Config
	dead      atomic.Bool
	stopCh    chan struct{}

	// Persistent state on all servers (saved before answering any RPC).
	currentTerm int
	votedFor    int     // -1 = nobody in currentTerm
	log         []Entry // log[0] is a sentinel holding the snapshot's last index/term

	// Volatile state on all servers.
	role             Role
	leaderID         int
	commitIndex      int
	lastApplied      int
	electionDeadline time.Time

	// Volatile state on leaders, reinitialised after every election.
	nextIndex  []int
	matchIndex []int

	// Snapshot of the state machine covering log[0].Index and earlier.
	snapshot []byte

	applyCh         chan ApplyMsg
	applyCond       *sync.Cond
	pendingSnapshot *ApplyMsg       // snapshot from the leader waiting to be applied
	replicateCh     []chan struct{} // per-peer "you have new entries" signal

	// Persistence bookkeeping (see persist.go).
	durableIndex int           // highest log index known to be on disk (guarded by mu)
	stateVersion uint64        // version of the most recently encoded state (guarded by mu)
	persistMu    sync.Mutex    // serialises writes; never held while acquiring mu
	savedVersion uint64        // version of the state on disk (guarded by persistMu)
	syncCh       chan struct{} // wakes the group-commit syncer
}

// Make creates a Raft peer. peers[me] is never called. persister holds any
// state saved before a crash; applyCh receives committed commands.
func Make(peers []Peer, me int, persister Persister, applyCh chan ApplyMsg, cfg Config) *Raft {
	if cfg.ElectionTimeoutMin == 0 {
		cfg = DefaultConfig()
	}
	rf := &Raft{
		peers:       peers,
		me:          me,
		persister:   persister,
		cfg:         cfg,
		stopCh:      make(chan struct{}),
		votedFor:    -1,
		log:         []Entry{{Index: 0, Term: 0}},
		leaderID:    -1,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		applyCh:     applyCh,
		replicateCh: make([]chan struct{}, len(peers)),
		syncCh:      make(chan struct{}, 1),
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	for i := range rf.replicateCh {
		rf.replicateCh[i] = make(chan struct{}, 1)
	}
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()
	// Everything up to the snapshot is committed and already reflected in the
	// service's state (the service loads the snapshot itself on startup).
	rf.commitIndex = rf.firstIndex()
	rf.lastApplied = rf.firstIndex()
	rf.durableIndex = rf.lastIndex() // everything we loaded came from disk
	rf.resetElectionTimerLocked()

	go rf.ticker()
	go rf.applier()
	go rf.syncer()
	return rf
}

// RPC returns the value to register with an RPC server under the name "Raft".
func (rf *Raft) RPC() *RPCService { return &RPCService{rf: rf} }

// GetState returns the current term and whether this peer believes it is
// the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// Start asks the leader to append command to the replicated log. It returns
// immediately, without waiting for the entry to commit. There is no
// guarantee the entry will ever commit: the leader may fail or lose an
// election. The returned index is where the entry will appear if it commits.
func (rf *Raft) Start(command any) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader || rf.killed() {
		return -1, rf.currentTerm, false
	}
	index = rf.lastIndex() + 1
	rf.log = append(rf.log, Entry{Index: index, Term: rf.currentTerm, Command: command})
	rf.nextIndex[rf.me] = index + 1
	if rf.cfg.DisableGroupCommit {
		rf.persist() // one fsync per entry, under the lock
	} else {
		// Group commit: the syncer fsyncs everything appended since its last
		// write in one go, outside the lock. The leader counts itself toward
		// a majority only once the entry is durable (see markDurableLocked).
		select {
		case rf.syncCh <- struct{}{}:
		default:
		}
	}
	for i := range rf.peers {
		if i != rf.me {
			rf.signalReplicator(i)
		}
	}
	return index, rf.currentTerm, true
}

// Kill stops the peer. Its goroutines exit soon after; it stops sending on
// applyCh and answers no more RPCs usefully.
func (rf *Raft) Kill() {
	if rf.dead.Swap(true) {
		return
	}
	close(rf.stopCh)
	rf.mu.Lock()
	rf.applyCond.Broadcast()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool { return rf.dead.Load() }

// Status is a snapshot of a peer's internals, used by monitoring and tests.
type Status struct {
	ID          int    `json:"id"`
	Term        int    `json:"term"`
	Role        string `json:"role"`
	LeaderID    int    `json:"leader"`
	CommitIndex int    `json:"commitIndex"`
	LastApplied int    `json:"lastApplied"`
	FirstIndex  int    `json:"firstIndex"`
	LastIndex   int    `json:"lastIndex"`
}

// Status reports the peer's current state.
func (rf *Raft) Status() Status {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return Status{
		ID:          rf.me,
		Term:        rf.currentTerm,
		Role:        rf.role.String(),
		LeaderID:    rf.leaderID,
		CommitIndex: rf.commitIndex,
		LastApplied: rf.lastApplied,
		FirstIndex:  rf.firstIndex(),
		LastIndex:   rf.lastIndex(),
	}
}

// --- log helpers (the log is offset by the snapshot) -----------------------

func (rf *Raft) firstIndex() int         { return rf.log[0].Index }
func (rf *Raft) lastIndex() int          { return rf.log[len(rf.log)-1].Index }
func (rf *Raft) lastTerm() int           { return rf.log[len(rf.log)-1].Term }
func (rf *Raft) termAt(index int) int    { return rf.log[index-rf.firstIndex()].Term }
func (rf *Raft) entryAt(index int) Entry { return rf.log[index-rf.firstIndex()] }

// --- role transitions -------------------------------------------------------

// becomeFollowerLocked steps down. If term is newer, it adopts it and forgets
// its vote. Returns true if persistent state changed (caller must persist).
func (rf *Raft) becomeFollowerLocked(term int) bool {
	changed := false
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		changed = true
	}
	if rf.role != Follower {
		rf.logf("S%d -> follower (term %d)", rf.me, rf.currentTerm)
	}
	rf.role = Follower
	return changed
}

func (rf *Raft) resetElectionTimerLocked() {
	span := int64(rf.cfg.ElectionTimeoutMax - rf.cfg.ElectionTimeoutMin)
	d := rf.cfg.ElectionTimeoutMin
	if span > 0 {
		d += time.Duration(rand.Int64N(span))
	}
	rf.electionDeadline = time.Now().Add(d)
}

func (rf *Raft) logf(format string, args ...any) {
	if rf.cfg.Logger != nil {
		rf.cfg.Logger.Printf(format, args...)
	}
}

// --- apply loop --------------------------------------------------------------

// applier delivers committed entries (and leader snapshots) to the service in
// order. It is the only goroutine that sends on applyCh, which is what keeps
// the order correct, and it never holds the lock while sending.
func (rf *Raft) applier() {
	for {
		rf.mu.Lock()
		for !rf.killed() && rf.pendingSnapshot == nil && rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		if snap := rf.pendingSnapshot; snap != nil {
			rf.pendingSnapshot = nil
			if snap.SnapshotIndex > rf.lastApplied {
				rf.lastApplied = snap.SnapshotIndex
			}
			rf.mu.Unlock()
			if !rf.deliver(*snap) {
				return
			}
			continue
		}
		start, end := rf.lastApplied+1, rf.commitIndex
		if start <= rf.firstIndex() {
			panic("raft: applier is behind the snapshot without a pending snapshot")
		}
		msgs := make([]ApplyMsg, 0, end-start+1)
		for i := start; i <= end; i++ {
			e := rf.entryAt(i)
			msgs = append(msgs, ApplyMsg{CommandValid: true, Command: e.Command, CommandIndex: i, CommandTerm: e.Term})
		}
		rf.lastApplied = end
		rf.mu.Unlock()
		for _, m := range msgs {
			if !rf.deliver(m) {
				return
			}
		}
	}
}

func (rf *Raft) deliver(m ApplyMsg) bool {
	select {
	case rf.applyCh <- m:
		return true
	case <-rf.stopCh:
		return false
	}
}
