package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Persister stores Raft's persistent state and the service's snapshot.
// Save must be atomic: after a crash, a reader sees either the old pair or
// the new pair, never a mix and never a torn write.
type Persister interface {
	Save(raftState, snapshot []byte)
	ReadRaftState() []byte
	ReadSnapshot() []byte
	RaftStateSize() int
}

// persist synchronously writes currentTerm, votedFor and the log. It is
// called with the lock held, before replying to any RPC that changed them: a
// peer that forgets its vote after a crash could vote twice in one term, and
// one that forgets entries could un-commit them.
func (rf *Raft) persist() {
	rf.stateVersion++
	rf.save(rf.stateVersion, rf.encodeState(), rf.snapshot)
	// Synchronous and under the lock, so the whole current log is durable.
	rf.durableIndex = rf.lastIndex()
	rf.markDurableLocked(rf.durableIndex)
}

// save writes one encoded state, unless a newer version is already on disk.
// The version check matters because the syncer encodes under the lock but
// writes outside it: without it, a slow syncer write could overwrite a newer
// term or vote saved in the meantime.
func (rf *Raft) save(version uint64, state, snapshot []byte) {
	rf.persistMu.Lock()
	defer rf.persistMu.Unlock()
	if version <= rf.savedVersion {
		return // a newer state, which includes everything in this one, is on disk
	}
	rf.persister.Save(state, snapshot)
	rf.savedVersion = version
}

// markDurableLocked records that the log up to index is on disk. A leader
// only counts itself toward a majority for durable entries: if it crashed
// right after acknowledging a commit it hadn't saved, it could come back
// without an entry the cluster believes is committed.
func (rf *Raft) markDurableLocked(index int) {
	if index > rf.durableIndex {
		rf.durableIndex = index
	}
	if rf.role == Leader && rf.durableIndex > rf.matchIndex[rf.me] {
		rf.matchIndex[rf.me] = rf.durableIndex
		rf.advanceCommitLocked()
	}
}

// syncer is the leader's group-commit loop. Each round encodes the state
// under the lock, fsyncs it without the lock (so Start and RPC handlers keep
// running), then marks everything it covered as durable. Entries appended
// during one fsync all go out in the next, so one fsync is shared by many
// client requests.
func (rf *Raft) syncer() {
	for {
		select {
		case <-rf.stopCh:
			return
		case <-rf.syncCh:
		}
		rf.mu.Lock()
		if rf.durableIndex >= rf.lastIndex() {
			rf.mu.Unlock()
			continue
		}
		rf.stateVersion++
		version, state, snap := rf.stateVersion, rf.encodeState(), rf.snapshot
		target, term := rf.lastIndex(), rf.currentTerm
		rf.mu.Unlock()

		rf.save(version, state, snap)

		rf.mu.Lock()
		// A leader never truncates its own log within a term, so if the term
		// is unchanged everything up to target is still ours and on disk.
		if rf.currentTerm == term && target <= rf.lastIndex() {
			rf.markDurableLocked(target)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) encodeState() []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(rf.currentTerm); err != nil {
		panic(err)
	}
	if err := enc.Encode(rf.votedFor); err != nil {
		panic(err)
	}
	if err := enc.Encode(rf.log); err != nil {
		panic(fmt.Sprintf("raft: cannot encode log (did you gob.Register your command type?): %v", err))
	}
	return buf.Bytes()
}

func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 {
		return
	}
	dec := gob.NewDecoder(bytes.NewReader(data))
	var term, votedFor int
	var log []Entry
	if dec.Decode(&term) != nil || dec.Decode(&votedFor) != nil || dec.Decode(&log) != nil {
		panic("raft: corrupt persisted state")
	}
	rf.currentTerm, rf.votedFor, rf.log = term, votedFor, log
}

// MemoryPersister keeps state in memory. Tests use it to simulate crashes:
// Copy() gives the restarted peer what was durably saved, while the dead
// peer's late writes go to the old, discarded object.
type MemoryPersister struct {
	mu        sync.Mutex
	raftState []byte
	snapshot  []byte
}

// NewMemoryPersister returns an empty in-memory persister.
func NewMemoryPersister() *MemoryPersister { return &MemoryPersister{} }

// Copy returns an independent copy.
func (p *MemoryPersister) Copy() *MemoryPersister {
	p.mu.Lock()
	defer p.mu.Unlock()
	return &MemoryPersister{raftState: clone(p.raftState), snapshot: clone(p.snapshot)}
}

// Save implements Persister.
func (p *MemoryPersister) Save(raftState, snapshot []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.raftState = clone(raftState)
	p.snapshot = clone(snapshot)
}

// ReadRaftState implements Persister.
func (p *MemoryPersister) ReadRaftState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return clone(p.raftState)
}

// ReadSnapshot implements Persister.
func (p *MemoryPersister) ReadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return clone(p.snapshot)
}

// RaftStateSize implements Persister.
func (p *MemoryPersister) RaftStateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.raftState)
}

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}

// FilePersister stores state in a single file, replaced atomically:
//
//  1. write the new contents to <file>.tmp
//  2. fsync it, so the bytes are on disk
//  3. rename it over <file> (atomic on POSIX filesystems)
//  4. fsync the directory, so the rename itself survives a power cut
//
// A crash at any point leaves either the complete old file or the complete
// new one. A CRC over the contents detects any other corruption on load.
//
// File layout: "RKV1" | crc32(rest) | len(state) | state | len(snap) | snap
type FilePersister struct {
	mu        sync.Mutex
	path      string
	noSync    bool
	raftState []byte
	snapshot  []byte
}

const fileMagic = "RKV1"

// NewFilePersister opens (or creates) the state file in dir.
func NewFilePersister(dir string) (*FilePersister, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	p := &FilePersister{path: filepath.Join(dir, "raft.state")}
	data, err := os.ReadFile(p.path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return p, nil
	case err != nil:
		return nil, err
	}
	p.raftState, p.snapshot, err = decodeStateFile(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", p.path, err)
	}
	return p, nil
}

// SetNoSync skips fsync. Only for measuring how much fsync costs; a node
// running this way can lose acknowledged writes on power loss.
func (p *FilePersister) SetNoSync(v bool) { p.noSync = v }

// Save implements Persister. It panics if the disk write fails: continuing
// without durable state would break Raft's safety guarantees.
func (p *FilePersister) Save(raftState, snapshot []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.writeFile(encodeStateFile(raftState, snapshot)); err != nil {
		panic(fmt.Sprintf("raft: persist failed: %v", err))
	}
	p.raftState = clone(raftState)
	p.snapshot = clone(snapshot)
}

func (p *FilePersister) writeFile(data []byte) error {
	tmp := p.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if !p.noSync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, p.path); err != nil {
		return err
	}
	if !p.noSync {
		if d, err := os.Open(filepath.Dir(p.path)); err == nil {
			d.Sync() // not supported on every OS (e.g. Windows); best effort
			d.Close()
		}
	}
	return nil
}

// ReadRaftState implements Persister.
func (p *FilePersister) ReadRaftState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return clone(p.raftState)
}

// ReadSnapshot implements Persister.
func (p *FilePersister) ReadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return clone(p.snapshot)
}

// RaftStateSize implements Persister.
func (p *FilePersister) RaftStateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.raftState)
}

func encodeStateFile(state, snap []byte) []byte {
	body := make([]byte, 0, 16+len(state)+len(snap))
	body = binary.BigEndian.AppendUint64(body, uint64(len(state)))
	body = append(body, state...)
	body = binary.BigEndian.AppendUint64(body, uint64(len(snap)))
	body = append(body, snap...)
	out := make([]byte, 0, 8+len(body))
	out = append(out, fileMagic...)
	out = binary.BigEndian.AppendUint32(out, crc32.ChecksumIEEE(body))
	return append(out, body...)
}

var errCorrupt = errors.New("corrupt state file")

func decodeStateFile(data []byte) (state, snap []byte, err error) {
	if len(data) < 8 || string(data[:4]) != fileMagic {
		return nil, nil, errCorrupt
	}
	body := data[8:]
	if crc32.ChecksumIEEE(body) != binary.BigEndian.Uint32(data[4:8]) {
		return nil, nil, fmt.Errorf("%w: checksum mismatch", errCorrupt)
	}
	r := bytes.NewReader(body)
	read := func() ([]byte, error) {
		var n uint64
		if err := binary.Read(r, binary.BigEndian, &n); err != nil {
			return nil, errCorrupt
		}
		if n > uint64(r.Len()) {
			return nil, errCorrupt
		}
		b := make([]byte, n)
		if _, err := io.ReadFull(r, b); err != nil {
			return nil, errCorrupt
		}
		if n == 0 {
			return nil, nil
		}
		return b, nil
	}
	if state, err = read(); err != nil {
		return nil, nil, err
	}
	if snap, err = read(); err != nil {
		return nil, nil, err
	}
	return state, snap, nil
}
