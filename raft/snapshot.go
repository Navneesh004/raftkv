package raft

// Snapshot is called by the service once it has saved its state up to and
// including index. Raft then discards that prefix of the log, which keeps
// memory and the persisted state bounded (§7).
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.firstIndex() || index > rf.lastApplied {
		return // already compacted, or not yet applied
	}
	rf.compactLocked(index, rf.termAt(index))
	rf.snapshot = snapshot
	rf.persist()
}

// compactLocked drops every entry up to and including index, keeping a new
// sentinel that remembers (index, term) for consistency checks.
func (rf *Raft) compactLocked(index, term int) {
	var rest []Entry
	if index < rf.lastIndex() && index >= rf.firstIndex() && rf.termAt(index) == term {
		rest = rf.log[index-rf.firstIndex()+1:]
	}
	// Copy into a fresh slice so the old backing array can be freed.
	newLog := make([]Entry, 0, 1+len(rest))
	newLog = append(newLog, Entry{Index: index, Term: term})
	newLog = append(newLog, rest...)
	rf.log = newLog
}

func (rf *Raft) sendSnapshot(peer, term int) (stillLeader, more bool) {
	rf.mu.Lock()
	if rf.role != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return false, false
	}
	args := &InstallSnapshotArgs{
		Term:              term,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.firstIndex(),
		LastIncludedTerm:  rf.log[0].Term,
		Data:              rf.snapshot,
	}
	rf.mu.Unlock()

	var reply InstallSnapshotReply
	if !rf.peers[peer].Call("Raft.InstallSnapshot", args, &reply) {
		return true, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		if rf.becomeFollowerLocked(reply.Term) {
			rf.persist()
		}
		return false, false
	}
	if rf.role != Leader || rf.currentTerm != term {
		return false, false
	}
	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
	}
	if args.LastIncludedIndex+1 > rf.nextIndex[peer] {
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
	}
	return true, rf.nextIndex[peer] <= rf.lastIndex()
}

// handleInstallSnapshot implements the receiver side of InstallSnapshot.
func (rf *Raft) handleInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	dirty := rf.becomeFollowerLocked(args.Term)
	reply.Term = rf.currentTerm
	rf.leaderID = args.LeaderID
	rf.resetElectionTimerLocked()

	if args.LastIncludedIndex <= rf.commitIndex {
		// We already have (and will apply) everything in this snapshot.
		if dirty {
			rf.persist()
		}
		return
	}

	rf.compactLocked(args.LastIncludedIndex, args.LastIncludedTerm)
	rf.snapshot = args.Data
	rf.commitIndex = args.LastIncludedIndex
	rf.persist()

	rf.pendingSnapshot = &ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	rf.applyCond.Broadcast()
}
