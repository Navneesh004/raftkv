// Package network is an in-process, fault-injecting RPC network used by the
// tests. Every node of a cluster runs inside the same test binary, and every
// message goes through this package, which can drop it, delay it, or refuse
// to deliver it because of a partition or a crashed node.
//
// Arguments and replies are gob-encoded on the way in and out, exactly like a
// real wire, so a handler never shares memory with its caller. This catches
// unexported fields, aliasing bugs and anything else that would only break
// once the code runs over TCP.
package network

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand/v2"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ClientID is the "from" id used by external clients (e.g. KV clerks). A
// client is never placed in a partition group; it can reach any connected
// server, which is how a real client behaves when it can see every node.
const ClientID = -1

// Network connects a set of numbered nodes.
type Network struct {
	mu         sync.Mutex
	reliable   bool
	longDelays bool
	closed     bool
	servers    map[int]*Server
	connected  map[int]bool
	group      map[int]int // partition group of each node
	gen        map[int]int // bumped on Crash; invalidates the old instance's ends

	totalRPCs  atomic.Int64
	totalBytes atomic.Int64
	perServer  map[int]int
}

// New returns a reliable network with no partitions.
func New() *Network {
	return &Network{
		reliable:  true,
		servers:   map[int]*Server{},
		connected: map[int]bool{},
		group:     map[int]int{},
		gen:       map[int]int{},
		perServer: map[int]int{},
	}
}

// Close makes every subsequent call fail.
func (n *Network) Close() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.closed = true
}

// SetReliable toggles packet loss and random delay. When unreliable, each
// request and each reply is dropped with probability 10% and every request is
// delayed by up to 27ms, which also reorders messages.
func (n *Network) SetReliable(v bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.reliable = v
}

// SetLongDelays makes calls to unreachable nodes hang for up to 2 seconds
// before failing, like a TCP connection to a host that silently vanished.
func (n *Network) SetLongDelays(v bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.longDelays = v
}

// AddServer installs (or replaces) the RPC server for node id.
func (n *Network) AddServer(id int, s *Server) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.servers[id] = s
}

// Crash removes node id's server and invalidates every end it created, so
// in-flight requests and replies of the old instance are lost.
func (n *Network) Crash(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.servers, id)
	n.gen[id]++
}

// Connect lets node id send and receive messages.
func (n *Network) Connect(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.connected[id] = true
}

// Disconnect cuts node id off from everybody.
func (n *Network) Disconnect(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.connected[id] = false
}

// Partition splits the nodes into groups. Nodes can only talk to nodes in
// their own group. Nodes not mentioned end up in group 0.
func (n *Network) Partition(groups ...[]int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for id := range n.group {
		n.group[id] = 0
	}
	for g, ids := range groups {
		for _, id := range ids {
			n.group[id] = g + 1
		}
	}
}

// Heal removes every partition (connect/disconnect state is unchanged).
func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for id := range n.group {
		n.group[id] = 0
	}
}

// TotalRPCs is the number of requests delivered to a handler.
func (n *Network) TotalRPCs() int { return int(n.totalRPCs.Load()) }

// TotalBytes is the number of argument bytes delivered.
func (n *Network) TotalBytes() int64 { return n.totalBytes.Load() }

// RPCCount is the number of requests delivered to node id.
func (n *Network) RPCCount(id int) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.perServer[id]
}

func (n *Network) reachableLocked(from, to int) bool {
	if n.closed || !n.connected[to] {
		return false
	}
	if from == ClientID {
		return true
	}
	return n.connected[from] && n.group[from] == n.group[to]
}

// End is one node's handle for calling another node.
type End struct {
	net      *Network
	from, to int
	gen      int
}

// MakeEnd returns a handle that node `from` uses to call node `to`. Use
// ClientID as `from` for external clients.
func (n *Network) MakeEnd(from, to int) *End {
	n.mu.Lock()
	defer n.mu.Unlock()
	return &End{net: n, from: from, to: to, gen: n.gen[from]}
}

// Call sends an RPC and waits for the reply. It returns false if the request
// or the reply was lost; in that case reply is left untouched. Like a real
// network, a false return does not tell the caller whether the handler ran.
func (e *End) Call(method string, args any, reply any) bool {
	n := e.net
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(args); err != nil {
		panic(fmt.Sprintf("network: cannot encode args of %s: %v", method, err))
	}

	n.mu.Lock()
	stale := e.from != ClientID && n.gen[e.from] != e.gen
	srv := n.servers[e.to]
	ok := !stale && srv != nil && n.reachableLocked(e.from, e.to)
	reliable, long := n.reliable, n.longDelays
	n.mu.Unlock()

	if !ok {
		// Unreachable: fail, but only after a while, like a timeout would.
		max := 100
		if long {
			max = 2000
		}
		time.Sleep(time.Duration(rand.IntN(max)) * time.Millisecond)
		return false
	}
	if !reliable {
		time.Sleep(time.Duration(rand.IntN(27)) * time.Millisecond)
		if rand.IntN(10) == 0 {
			return false // request lost
		}
	}

	n.totalRPCs.Add(1)
	n.totalBytes.Add(int64(buf.Len()))
	n.mu.Lock()
	n.perServer[e.to]++
	n.mu.Unlock()

	out := srv.dispatch(method, buf.Bytes())

	// If the target crashed, was replaced or got cut off while the handler
	// ran, the reply never makes it back.
	n.mu.Lock()
	ok = n.servers[e.to] == srv && n.reachableLocked(e.from, e.to) &&
		(e.from == ClientID || n.gen[e.from] == e.gen)
	n.mu.Unlock()
	if !ok {
		return false
	}
	if !reliable && rand.IntN(10) == 0 {
		return false // reply lost
	}
	if err := gob.NewDecoder(bytes.NewReader(out)).Decode(reply); err != nil {
		panic(fmt.Sprintf("network: cannot decode reply of %s: %v", method, err))
	}
	return true
}

// Server dispatches incoming calls to registered services. A service is any
// value whose exported methods look like `func (*T) Name(args *A, reply *R) error`
// (the same shape net/rpc uses, so the same service works over TCP).
type Server struct {
	mu       sync.Mutex
	services map[string]*service
}

type service struct {
	rcvr    reflect.Value
	methods map[string]reflect.Method
}

// NewServer returns an empty server.
func NewServer() *Server { return &Server{services: map[string]*service{}} }

// Register exposes rcvr's RPC-shaped methods as "name.Method".
func (s *Server) Register(name string, rcvr any) {
	svc := &service{rcvr: reflect.ValueOf(rcvr), methods: map[string]reflect.Method{}}
	t := reflect.TypeOf(rcvr)
	errType := reflect.TypeOf((*error)(nil)).Elem()
	for i := 0; i < t.NumMethod(); i++ {
		m := t.Method(i)
		mt := m.Type
		if mt.NumIn() == 3 && mt.In(1).Kind() == reflect.Pointer &&
			mt.In(2).Kind() == reflect.Pointer && mt.NumOut() == 1 && mt.Out(0) == errType {
			svc.methods[m.Name] = m
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.services[name] = svc
}

func (s *Server) dispatch(method string, argBytes []byte) []byte {
	svcName, mName, found := strings.Cut(method, ".")
	s.mu.Lock()
	svc := s.services[svcName]
	s.mu.Unlock()
	if !found || svc == nil {
		panic("network: unknown service in " + method)
	}
	m, ok := svc.methods[mName]
	if !ok {
		panic("network: unknown method " + method)
	}
	args := reflect.New(m.Type.In(1).Elem())
	if err := gob.NewDecoder(bytes.NewReader(argBytes)).Decode(args.Interface()); err != nil {
		panic(fmt.Sprintf("network: cannot decode args of %s: %v", method, err))
	}
	reply := reflect.New(m.Type.In(2).Elem())
	m.Func.Call([]reflect.Value{svc.rcvr, args, reply})
	var out bytes.Buffer
	if err := gob.NewEncoder(&out).Encode(reply.Interface()); err != nil {
		panic(fmt.Sprintf("network: cannot encode reply of %s: %v", method, err))
	}
	return out.Bytes()
}
