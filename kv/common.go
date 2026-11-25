// Package kv is a linearizable, fault-tolerant key-value store built on the
// raft package. Clients send Get, Put and Append to the leader, which
// replicates each operation through the Raft log before executing it.
package kv

import (
	"encoding/gob"

	"github.com/Navneesh004/raftkv/raft"
)

// Err is the outcome of a request.
type Err string

// Possible outcomes.
const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrTimeout     Err = "ErrTimeout"
)

// OpKind is the type of operation.
type OpKind string

// Supported operations.
const (
	OpGet    OpKind = "Get"
	OpPut    OpKind = "Put"
	OpAppend OpKind = "Append"
)

// CommandArgs is the single client RPC. (ClientID, Seq) identifies a request
// uniquely, which is how retried writes are recognised and applied once.
type CommandArgs struct {
	Kind     OpKind
	Key      string
	Value    string
	ClientID int64
	Seq      int64
}

// CommandReply is the answer to CommandArgs.
type CommandReply struct {
	Err      Err
	Value    string
	LeaderID int // hint: who this server thinks the leader is (-1 = unknown)
}

// Op is what goes into the Raft log.
type Op struct {
	Kind     OpKind
	Key      string
	Value    string
	ClientID int64
	Seq      int64
}

// StatusArgs asks a server for its state.
type StatusArgs struct{}

// StatusReply describes a server's Raft state and store size.
type StatusReply struct {
	Raft raft.Status
	Keys int
}

func init() {
	// Log entries carry commands as interface values; gob must know the type.
	gob.Register(Op{})
}
