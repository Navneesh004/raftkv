package raft

// RPC message types. Field names follow Figure 2 of the Raft paper.

// RequestVoteArgs is sent by candidates to gather votes (§5.2).
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateID  int // candidate requesting the vote
	LastLogIndex int // index of candidate's last log entry (§5.4)
	LastLogTerm  int // term of candidate's last log entry (§5.4)
}

// RequestVoteReply is the answer to RequestVote.
type RequestVoteReply struct {
	Term        int  // currentTerm, for the candidate to update itself
	VoteGranted bool // true means the candidate received the vote
}

// AppendEntriesArgs is sent by the leader to replicate entries; with no
// entries it is a heartbeat (§5.3).
type AppendEntriesArgs struct {
	Term         int     // leader's term
	LeaderID     int     // so followers can redirect clients
	PrevLogIndex int     // index of the entry immediately preceding the new ones
	PrevLogTerm  int     // term of the PrevLogIndex entry
	Entries      []Entry // entries to store (empty for heartbeat)
	LeaderCommit int     // leader's commitIndex
}

// AppendEntriesReply is the answer to AppendEntries.
//
// On rejection the follower also returns where the conflict starts so the
// leader can skip back a whole term at once ("fast backup", §5.3 last
// paragraphs):
//   - ConflictTerm == -1: the follower's log is too short; ConflictIndex is
//     its last index + 1.
//   - otherwise ConflictTerm is the term of the follower's entry at
//     PrevLogIndex and ConflictIndex is the first index it holds for that term.
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

// InstallSnapshotArgs carries the leader's snapshot to a follower that is so
// far behind that the entries it needs were already compacted away (§7).
type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// InstallSnapshotReply is the answer to InstallSnapshot.
type InstallSnapshotReply struct {
	Term int
}

// RPCService is the value registered with an RPC server under the name
// "Raft". It exposes only the three Raft RPCs in the net/rpc method shape,
// so the same type works over the simulated network and over TCP.
type RPCService struct{ rf *Raft }

// RequestVote handles the RequestVote RPC.
func (s *RPCService) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	s.rf.handleRequestVote(args, reply)
	return nil
}

// AppendEntries handles the AppendEntries RPC.
func (s *RPCService) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	s.rf.handleAppendEntries(args, reply)
	return nil
}

// InstallSnapshot handles the InstallSnapshot RPC.
func (s *RPCService) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	s.rf.handleInstallSnapshot(args, reply)
	return nil
}
