package kv

import (
	"bytes"
	"encoding/gob"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Navneesh004/raftkv/raft"
)

// How long a server waits for its log entry to be applied before telling the
// client to try again (the leader may have lost leadership silently).
const commandTimeout = 800 * time.Millisecond

// Server is one replica of the key-value store.
type Server struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	persister    raft.Persister
	maxRaftState int // snapshot when Raft's persisted state reaches this size; <= 0 disables
	dead         atomic.Bool
	stopCh       chan struct{}

	// Replicated state machine, rebuilt from the snapshot plus the log.
	data        map[string]string
	lastSeq     map[int64]int64 // per client: highest Seq applied (duplicate detection)
	lastApplied int

	waiters map[int]chan result // log index -> RPC handler waiting for it
}

type result struct {
	op    Op
	value string
	err   Err
}

// StartServer creates a replica. peers are the Raft peers (peers[me] unused).
func StartServer(peers []raft.Peer, me int, persister raft.Persister, maxRaftState int, cfg raft.Config) *Server {
	kv := &Server{
		me:           me,
		applyCh:      make(chan raft.ApplyMsg, 128),
		persister:    persister,
		maxRaftState: maxRaftState,
		stopCh:       make(chan struct{}),
		data:         map[string]string{},
		lastSeq:      map[int64]int64{},
		waiters:      map[int]chan result{},
	}
	kv.restore(persister.ReadSnapshot())
	kv.rf = raft.Make(peers, me, persister, kv.applyCh, cfg)
	go kv.applier()
	return kv
}

// Raft returns the underlying Raft peer.
func (kv *Server) Raft() *raft.Raft { return kv.rf }

// Kill stops the server and its Raft peer.
func (kv *Server) Kill() {
	if kv.dead.Swap(true) {
		return
	}
	kv.rf.Kill()
	close(kv.stopCh)
}

// RPC returns the value to register with an RPC server under the name "KV".
func (kv *Server) RPC() *RPCService { return &RPCService{kv: kv} }

// RPCService exposes the client-facing RPCs in net/rpc shape.
type RPCService struct{ kv *Server }

// Command executes a Get, Put or Append.
func (s *RPCService) Command(args *CommandArgs, reply *CommandReply) error {
	s.kv.command(args, reply)
	return nil
}

// Status reports this server's Raft state.
func (s *RPCService) Status(args *StatusArgs, reply *StatusReply) error {
	reply.Raft = s.kv.rf.Status()
	s.kv.mu.Lock()
	reply.Keys = len(s.kv.data)
	s.kv.mu.Unlock()
	return nil
}

func (kv *Server) command(args *CommandArgs, reply *CommandReply) {
	reply.LeaderID = -1
	if kv.dead.Load() {
		reply.Err = ErrWrongLeader
		return
	}
	// A retried write that was already applied can be answered right away.
	// Gets always go through the log: answering from local state could
	// return stale data if this server was deposed without noticing.
	if args.Kind != OpGet {
		kv.mu.Lock()
		dup := kv.lastSeq[args.ClientID] >= args.Seq
		kv.mu.Unlock()
		if dup {
			reply.Err = OK
			return
		}
	}

	op := Op{Kind: args.Kind, Key: args.Key, Value: args.Value, ClientID: args.ClientID, Seq: args.Seq}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.LeaderID = kv.rf.Status().LeaderID
		return
	}

	ch := make(chan result, 1)
	kv.mu.Lock()
	kv.waiters[index] = ch
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		if kv.waiters[index] == ch {
			delete(kv.waiters, index)
		}
		kv.mu.Unlock()
	}()

	timer := time.NewTimer(commandTimeout)
	defer timer.Stop()
	select {
	case res := <-ch:
		// A different op at our index means we lost leadership and another
		// leader's entry took the slot. The client must retry.
		if res.op.ClientID != op.ClientID || res.op.Seq != op.Seq {
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err, reply.Value = res.err, res.value
	case <-timer.C:
		reply.Err = ErrTimeout
	case <-kv.stopCh:
		reply.Err = ErrWrongLeader
	}
}

// applier executes committed operations in log order, so every replica goes
// through exactly the same sequence of states.
func (kv *Server) applier() {
	for {
		var m raft.ApplyMsg
		select {
		case <-kv.stopCh:
			return
		case m = <-kv.applyCh:
		}

		if m.SnapshotValid {
			kv.mu.Lock()
			if m.SnapshotIndex > kv.lastApplied {
				kv.restore(m.Snapshot)
				kv.lastApplied = m.SnapshotIndex
			}
			kv.mu.Unlock()
			continue
		}
		if !m.CommandValid {
			continue
		}

		kv.mu.Lock()
		if m.CommandIndex <= kv.lastApplied {
			kv.mu.Unlock()
			continue
		}
		kv.lastApplied = m.CommandIndex
		op := m.Command.(Op)
		res := result{op: op, err: OK}
		switch op.Kind {
		case OpGet:
			v, ok := kv.data[op.Key]
			if !ok {
				res.err = ErrNoKey
			}
			res.value = v
		case OpPut, OpAppend:
			// Duplicate detection: apply each (client, seq) at most once.
			if kv.lastSeq[op.ClientID] < op.Seq {
				if op.Kind == OpPut {
					kv.data[op.Key] = op.Value
				} else {
					kv.data[op.Key] += op.Value
				}
				kv.lastSeq[op.ClientID] = op.Seq
			}
		}
		if ch, ok := kv.waiters[m.CommandIndex]; ok {
			ch <- res // buffered; each channel gets at most one result
			delete(kv.waiters, m.CommandIndex)
		}
		var snap []byte
		if kv.maxRaftState > 0 && kv.persister.RaftStateSize() >= kv.maxRaftState {
			snap = kv.encodeSnapshot()
		}
		kv.mu.Unlock()

		if snap != nil {
			kv.rf.Snapshot(m.CommandIndex, snap)
		}
	}
}

// The snapshot holds the data and the duplicate table: without the latter, a
// restarted replica would re-apply a retried Append.
func (kv *Server) encodeSnapshot() []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(kv.data); err != nil {
		panic(err)
	}
	if err := enc.Encode(kv.lastSeq); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func (kv *Server) restore(snap []byte) {
	if len(snap) == 0 {
		return
	}
	var data map[string]string
	var lastSeq map[int64]int64
	dec := gob.NewDecoder(bytes.NewReader(snap))
	if dec.Decode(&data) != nil || dec.Decode(&lastSeq) != nil {
		panic("kv: corrupt snapshot")
	}
	if data == nil {
		data = map[string]string{}
	}
	if lastSeq == nil {
		lastSeq = map[int64]int64{}
	}
	kv.data, kv.lastSeq = data, lastSeq
}
