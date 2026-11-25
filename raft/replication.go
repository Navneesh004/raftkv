package raft

import "time"

// replicator is the leader's per-follower loop for one term. It sends one
// AppendEntries (or InstallSnapshot) at a time: immediately when Start adds
// entries, and every HeartbeatInterval otherwise. While an RPC is in flight,
// new entries pile up and go out together in the next RPC, which is what
// batching means here.
func (rf *Raft) replicator(peer, term int) {
	for {
		stillLeader, more := rf.replicateOnce(peer, term)
		if !stillLeader {
			return
		}
		if more {
			continue // follower is behind or rejected us: retry right away
		}
		select {
		case <-rf.stopCh:
			return
		case <-rf.replicateCh[peer]:
		case <-time.After(rf.cfg.HeartbeatInterval):
		}
	}
}

func (rf *Raft) signalReplicator(peer int) {
	select {
	case rf.replicateCh[peer] <- struct{}{}:
	default:
	}
}

// replicateOnce sends one RPC to peer. stillLeader is false once this peer is
// no longer leader of term; more is true if there is more to send right now.
func (rf *Raft) replicateOnce(peer, term int) (stillLeader, more bool) {
	rf.mu.Lock()
	if rf.role != Leader || rf.currentTerm != term || rf.killed() {
		rf.mu.Unlock()
		return false, false
	}
	if rf.nextIndex[peer] <= rf.firstIndex() {
		// The entries the follower needs were compacted into the snapshot.
		rf.mu.Unlock()
		return rf.sendSnapshot(peer, term)
	}

	next := rf.nextIndex[peer]
	prev := next - 1
	n := rf.lastIndex() - prev
	if max := rf.cfg.MaxEntriesPerRPC; max > 0 && n > max {
		n = max
	}
	entries := make([]Entry, n)
	copy(entries, rf.log[next-rf.firstIndex():])
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     rf.me,
		PrevLogIndex: prev,
		PrevLogTerm:  rf.termAt(prev),
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	var reply AppendEntriesReply
	if !rf.peers[peer].Call("Raft.AppendEntries", args, &reply) {
		return true, false // lost; the next heartbeat retries
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		if rf.becomeFollowerLocked(reply.Term) {
			rf.persist()
		}
		return false, false
	}
	// The world may have moved on while the RPC was in flight.
	if rf.role != Leader || rf.currentTerm != term {
		return false, false
	}

	if reply.Success {
		match := prev + len(entries)
		if match > rf.matchIndex[peer] {
			rf.matchIndex[peer] = match
		}
		if match+1 > rf.nextIndex[peer] {
			rf.nextIndex[peer] = match + 1
		}
		rf.advanceCommitLocked()
		return true, rf.nextIndex[peer] <= rf.lastIndex()
	}

	// Rejected: move nextIndex back and retry immediately.
	var newNext int
	switch {
	case rf.cfg.DisableFastBackup:
		newNext = prev
	case reply.ConflictTerm == -1:
		newNext = reply.ConflictIndex
	default:
		newNext = reply.ConflictIndex
		// If we also have entries from ConflictTerm, resume right after our
		// last one: everything up to there matches.
		for i := rf.lastIndex(); i >= rf.firstIndex(); i-- {
			if t := rf.termAt(i); t == reply.ConflictTerm {
				newNext = i + 1
				break
			} else if t < reply.ConflictTerm {
				break
			}
		}
	}
	if newNext <= rf.matchIndex[peer] {
		newNext = rf.matchIndex[peer] + 1 // a reordered reply must not undo progress
	}
	if newNext < 1 {
		newNext = 1
	}
	if newNext > rf.lastIndex()+1 {
		newNext = rf.lastIndex() + 1
	}
	rf.nextIndex[peer] = newNext
	return true, true
}

// advanceCommitLocked moves commitIndex to the highest index stored on a
// majority — but only by counting replicas of an entry from the current term.
// Entries from older terms commit indirectly when a later current-term entry
// commits. Counting old-term entries is unsafe (Figure 8 of the paper).
func (rf *Raft) advanceCommitLocked() {
	for n := rf.lastIndex(); n > rf.commitIndex; n-- {
		if rf.termAt(n) != rf.currentTerm {
			return // all earlier entries are from older terms too
		}
		count := 0
		for i := range rf.peers {
			if rf.matchIndex[i] >= n {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.applyCond.Broadcast()
			return
		}
	}
}

// handleAppendEntries implements the receiver side of AppendEntries (§5.3).
func (rf *Raft) handleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	dirty := false
	defer func() {
		if dirty {
			rf.persist()
		}
	}()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return // stale leader; our term in the reply makes it step down
	}
	// A current leader exists: candidates and stale leaders step down.
	dirty = rf.becomeFollowerLocked(args.Term)
	reply.Term = rf.currentTerm
	rf.leaderID = args.LeaderID
	rf.resetElectionTimerLocked()

	first := rf.firstIndex()
	if args.PrevLogIndex < first {
		// The leader is behind our snapshot point; ask it to start after it.
		reply.ConflictTerm = -1
		reply.ConflictIndex = first + 1
		return
	}
	if args.PrevLogIndex > rf.lastIndex() {
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastIndex() + 1
		return
	}
	if t := rf.termAt(args.PrevLogIndex); t != args.PrevLogTerm {
		reply.ConflictTerm = t
		i := args.PrevLogIndex
		for i > first+1 && rf.termAt(i-1) == t {
			i--
		}
		reply.ConflictIndex = i
		return
	}

	// The logs match up to PrevLogIndex. Append new entries, truncating our
	// log only at the first real conflict: a delayed, shorter AppendEntries
	// must never chop off entries a newer one already added.
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx <= rf.lastIndex() && rf.termAt(idx) == e.Term {
			continue
		}
		if idx <= rf.commitIndex {
			panic("raft: leader tried to overwrite a committed entry")
		}
		rf.log = append(rf.log[:idx-first], args.Entries[i:]...)
		dirty = true
		break
	}

	if args.LeaderCommit > rf.commitIndex {
		newCommit := args.LeaderCommit
		if lastNew := args.PrevLogIndex + len(args.Entries); lastNew < newCommit {
			newCommit = lastNew
		}
		if newCommit > rf.commitIndex {
			rf.commitIndex = newCommit
			rf.applyCond.Broadcast()
		}
	}
	reply.Success = true
}
