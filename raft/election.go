package raft

import "time"

// ticker starts an election whenever the election timer expires without the
// peer hearing from a current leader or granting a vote.
func (rf *Raft) ticker() {
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-rf.stopCh:
			return
		case <-t.C:
		}
		rf.mu.Lock()
		if rf.role != Leader && time.Now().After(rf.electionDeadline) {
			rf.startElectionLocked()
		}
		rf.mu.Unlock()
	}
}

// startElectionLocked turns this peer into a candidate for a new term and
// asks every other peer for its vote in parallel.
func (rf *Raft) startElectionLocked() {
	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.leaderID = -1
	rf.persist()
	rf.resetElectionTimerLocked()
	rf.logf("S%d -> candidate (term %d)", rf.me, rf.currentTerm)

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.lastIndex(),
		LastLogTerm:  rf.lastTerm(),
	}
	votes := 1
	if votes > len(rf.peers)/2 {
		rf.becomeLeaderLocked()
		return
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			// Never hold the lock across an RPC: two peers waiting on each
			// other's lock would deadlock.
			var reply RequestVoteReply
			if !rf.peers[peer].Call("Raft.RequestVote", args, &reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				if rf.becomeFollowerLocked(reply.Term) {
					rf.persist()
				}
				return
			}
			// Ignore stale replies from an election we are no longer running.
			if rf.currentTerm != args.Term || rf.role != Candidate {
				return
			}
			if reply.VoteGranted {
				votes++
				if votes > len(rf.peers)/2 {
					rf.becomeLeaderLocked()
				}
			}
		}(i)
	}
}

func (rf *Raft) becomeLeaderLocked() {
	rf.role = Leader
	rf.leaderID = rf.me
	last := rf.lastIndex()
	for i := range rf.peers {
		rf.nextIndex[i] = last + 1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = rf.durableIndex // count ourselves only for what is on disk
	rf.logf("S%d -> LEADER (term %d, last index %d)", rf.me, rf.currentTerm, last)
	term := rf.currentTerm
	for i := range rf.peers {
		if i != rf.me {
			go rf.replicator(i, term)
		}
	}
}

// handleRequestVote implements the receiver side of RequestVote (§5.2, §5.4.1).
func (rf *Raft) handleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	dirty := false
	defer func() {
		if dirty {
			rf.persist() // before the reply leaves this function
		}
	}()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		dirty = rf.becomeFollowerLocked(args.Term)
	}
	reply.Term = rf.currentTerm

	// Election restriction: only vote for a candidate whose log is at least
	// as up to date as ours, so a new leader always holds every committed entry.
	upToDate := args.LastLogTerm > rf.lastTerm() ||
		(args.LastLogTerm == rf.lastTerm() && args.LastLogIndex >= rf.lastIndex())
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && upToDate {
		if rf.votedFor != args.CandidateID {
			rf.votedFor = args.CandidateID
			dirty = true
		}
		reply.VoteGranted = true
		// Granting a vote is one of only three reasons to reset the timer.
		rf.resetElectionTimerLocked()
	}
}
