// Package transport carries Raft and KV RPCs over real TCP connections using
// the standard library's net/rpc (gob encoding). It is what the deployed
// cluster uses; tests mostly use the simulated network package instead.
package transport

import (
	"errors"
	"net"
	"net/rpc"
	"reflect"
	"sync"
	"time"
)

// Server accepts RPC connections for a set of registered services.
type Server struct {
	rpcs   *rpc.Server
	mu     sync.Mutex
	ln     net.Listener
	conns  map[net.Conn]struct{}
	closed bool
}

// NewServer returns a server with no services.
func NewServer() *Server {
	return &Server{rpcs: rpc.NewServer(), conns: map[net.Conn]struct{}{}}
}

// Register exposes rcvr's RPC-shaped methods as "name.Method".
func (s *Server) Register(name string, rcvr any) error {
	return s.rpcs.RegisterName(name, rcvr)
}

// Serve accepts connections on ln until Close is called.
func (s *Server) Serve(ln net.Listener) {
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			conn.Close()
			return
		}
		s.conns[conn] = struct{}{}
		s.mu.Unlock()
		go func() {
			s.rpcs.ServeConn(conn)
			s.mu.Lock()
			delete(s.conns, conn)
			s.mu.Unlock()
		}()
	}
}

// Close stops accepting connections and drops every open one, which is what
// a crashed process looks like to its peers.
func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	if s.ln != nil {
		s.ln.Close()
	}
	for c := range s.conns {
		c.Close()
	}
}

// Client is a lazily (re)connecting RPC client for one remote server. It is
// safe for concurrent use. It implements raft.Peer and kv.Endpoint.
type Client struct {
	addr    string
	timeout time.Duration

	mu         sync.Mutex
	c          *rpc.Client
	lastDialAt time.Time
}

// NewClient returns a client for addr. Each call fails after timeout.
func NewClient(addr string, timeout time.Duration) *Client {
	return &Client{addr: addr, timeout: timeout}
}

const redialInterval = 50 * time.Millisecond

func (c *Client) get() *rpc.Client {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.c != nil {
		return c.c
	}
	// Don't hammer a dead host: at most one dial attempt per interval.
	if time.Since(c.lastDialAt) < redialInterval {
		return nil
	}
	c.lastDialAt = time.Now()
	conn, err := net.DialTimeout("tcp", c.addr, c.timeout)
	if err != nil {
		return nil
	}
	c.c = rpc.NewClient(conn)
	return c.c
}

func (c *Client) drop(cl *rpc.Client) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.c == cl {
		c.c.Close()
		c.c = nil
	}
}

// Call performs an RPC. It returns false on any failure: connection refused,
// broken connection, or no reply within the timeout.
func (c *Client) Call(method string, args any, reply any) bool {
	cl := c.get()
	if cl == nil {
		return false
	}
	// Decode into a fresh value and copy it over only on success, so a reply
	// that arrives after we gave up can never race with the caller.
	fresh := reflect.New(reflect.TypeOf(reply).Elem())
	call := cl.Go(method, args, fresh.Interface(), make(chan *rpc.Call, 1))
	timer := time.NewTimer(c.timeout)
	defer timer.Stop()
	select {
	case <-call.Done:
		if call.Error != nil {
			var se rpc.ServerError
			if !errors.As(call.Error, &se) {
				c.drop(cl) // connection-level failure: reconnect next time
			}
			return false
		}
		reflect.ValueOf(reply).Elem().Set(fresh.Elem())
		return true
	case <-timer.C:
		return false
	}
}

// Close closes the underlying connection.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.c != nil {
		c.c.Close()
		c.c = nil
	}
}
