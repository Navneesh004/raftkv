package kv

import (
	"crypto/rand"
	"encoding/binary"
	"time"
)

// Endpoint is a connection to one server (simulated or TCP).
type Endpoint interface {
	Call(method string, args any, reply any) bool
}

// Clerk is a client of the key-value service. It retries each request until
// it succeeds, so every call eventually returns once a majority of servers is
// up and connected. A Clerk issues one request at a time; use one Clerk per
// goroutine.
type Clerk struct {
	servers  []Endpoint
	clientID int64
	seq      int64
	leader   int // the server that answered last time
}

// MakeClerk returns a client that talks to the given servers.
func MakeClerk(servers []Endpoint) *Clerk {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return &Clerk{
		servers:  servers,
		clientID: int64(binary.LittleEndian.Uint64(b[:]) >> 1),
	}
}

// Get returns the value for key, or "" if the key does not exist.
func (ck *Clerk) Get(key string) string { return ck.do(OpGet, key, "") }

// Put sets key to value.
func (ck *Clerk) Put(key, value string) { ck.do(OpPut, key, value) }

// Append appends value to key's value (missing keys count as "").
func (ck *Clerk) Append(key, value string) { ck.do(OpAppend, key, value) }

func (ck *Clerk) do(kind OpKind, key, value string) string {
	ck.seq++
	args := CommandArgs{Kind: kind, Key: key, Value: value, ClientID: ck.clientID, Seq: ck.seq}
	tries := 0
	// Servers that failed this request (unreachable or timed out). Their
	// followers may still name them as leader — e.g. a leader cut off in a
	// minority — so hints pointing at them are ignored, otherwise the clerk
	// could bounce between a stale leader and its follower forever.
	failed := make([]bool, len(ck.servers))
	for {
		var reply CommandReply
		ok := ck.servers[ck.leader].Call("KV.Command", &args, &reply)
		if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
			return reply.Value
		}
		if !ok || reply.Err == ErrTimeout {
			failed[ck.leader] = true
		}
		// Follow the leader hint if it points somewhere new, else round-robin.
		if ok && reply.Err == ErrWrongLeader && reply.LeaderID >= 0 &&
			reply.LeaderID < len(ck.servers) && reply.LeaderID != ck.leader && !failed[reply.LeaderID] {
			ck.leader = reply.LeaderID
		} else {
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}
		tries++
		if tries%len(ck.servers) == 0 {
			time.Sleep(10 * time.Millisecond) // no leader right now; back off a little
		}
	}
}
