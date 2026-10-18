// Package realenv runs nodes on the real world: the wall clock, UDP on the loopback interface,
// files under one directory and a randomly seeded generator. It shows that a service written
// against env runs unchanged outside the simulator. Each node has one goroutine that runs all of
// its callbacks in order, so services still need no locks.
package realenv

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	mrand "math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/EquinoxWN/deterministic-sim-testing/env"
)

const mailboxSize = 4096

// ErrClosed is returned once the cluster has been closed.
var ErrClosed = errors.New("realenv: cluster closed")

// Cluster is a set of nodes on 127.0.0.1, each with its own UDP socket and directory.
type Cluster struct {
	root    string
	mu      sync.Mutex
	nodes   map[env.NodeID]*node
	ports   map[int]env.NodeID
	started bool
	closed  bool
	wg      sync.WaitGroup
}

type node struct {
	c    *Cluster
	id   env.NodeID
	n    env.Node
	conn *net.UDPConn
	addr *net.UDPAddr
	box  chan func()
	done chan struct{}
	rng  *mrand.Rand
	dir  string
}

// New returns an empty cluster that keeps node files under root.
func New(root string) (*Cluster, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return nil, err
	}
	return &Cluster{root: abs, nodes: map[env.NodeID]*node{}, ports: map[int]env.NodeID{}}, nil
}

// Add binds a UDP socket on loopback for a node; it must be called before Start.
func (c *Cluster) Add(id env.NodeID, n env.Node) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || c.closed {
		return errors.New("realenv: Add after Start")
	}
	if _, dup := c.nodes[id]; dup {
		return fmt.Errorf("realenv: node %d added twice", id)
	}
	dir := filepath.Join(c.root, fmt.Sprintf("n%d", id))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	var seed [32]byte
	if _, err := rand.Read(seed[:]); err != nil {
		return err
	}
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return err
	}
	addr := conn.LocalAddr().(*net.UDPAddr)
	c.nodes[id] = &node{
		c: c, id: id, n: n, conn: conn, addr: addr, dir: dir,
		box: make(chan func(), mailboxSize), done: make(chan struct{}),
		rng: mrand.New(mrand.NewChaCha8(seed)),
	}
	c.ports[addr.Port] = id
	return nil
}

// Start launches every node's loop and reader, then calls each node's Start.
func (c *Cluster) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || c.closed {
		return errors.New("realenv: Start called twice or after Close")
	}
	c.started = true
	ids := c.sortedIDs()
	for _, id := range ids {
		nd := c.nodes[id]
		c.wg.Add(2)
		go nd.loop()
		go nd.read()
	}
	for _, id := range ids {
		nd := c.nodes[id]
		nd.post(func() { nd.n.Start(nd) })
	}
	return nil
}

// Do runs f on the node's goroutine and waits, so tests can read node state without races.
func (c *Cluster) Do(id env.NodeID, f func()) error {
	nd, ok := c.nodes[id]
	if !ok {
		return env.ErrUnknownNode
	}
	finished := make(chan struct{})
	if !nd.post(func() { f(); close(finished) }) {
		return ErrClosed
	}
	select {
	case <-finished:
		return nil
	case <-nd.done:
		return ErrClosed
	}
}

// Dir returns the directory that holds a node's files.
func (c *Cluster) Dir(id env.NodeID) string { return filepath.Join(c.root, fmt.Sprintf("n%d", id)) }

// Close stops every node and waits for its goroutines to exit.
func (c *Cluster) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	var errs []error
	for _, id := range c.sortedIDs() {
		nd := c.nodes[id]
		close(nd.done)
		errs = append(errs, nd.conn.Close())
	}
	c.mu.Unlock()
	c.wg.Wait()
	return errors.Join(errs...)
}

// sortedIDs lists node IDs in order.
func (c *Cluster) sortedIDs() []env.NodeID {
	ids := make([]env.NodeID, 0, len(c.nodes))
	for id := range c.nodes {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// loop runs the node's callbacks one at a time.
func (nd *node) loop() {
	defer nd.c.wg.Done()
	for {
		select {
		case f := <-nd.box:
			f()
		case <-nd.done:
			return
		}
	}
}

// read turns datagrams from cluster members into Receive callbacks.
func (nd *node) read() {
	defer nd.c.wg.Done()
	buf := make([]byte, 4+env.MaxMessage+1)
	for {
		n, from, err := nd.conn.ReadFromUDP(buf)
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil || n < 4 || n > 4+env.MaxMessage || !from.IP.IsLoopback() {
			continue // Windows reports ICMP port-unreachable as a read error; skip it
		}
		src, ok := nd.c.ports[from.Port]
		if !ok || env.NodeID(binary.BigEndian.Uint32(buf)) != src {
			continue
		}
		payload := slices.Clone(buf[4:n])
		select {
		case nd.box <- func() { nd.n.Receive(src, payload) }:
		default: // a full mailbox drops the message, as a congested network would
		}
	}
}

// post queues f on the node; it reports false once the cluster is closed.
func (nd *node) post(f func()) bool {
	select {
	case nd.box <- f:
		return true
	case <-nd.done:
		return false
	}
}

// ID returns the node's identity.
func (nd *node) ID() env.NodeID { return nd.id }

// Now returns the wall-clock time.
func (nd *node) Now() time.Time { return time.Now() }

type rtimer struct {
	tm      *time.Timer
	stopped bool
	fired   bool
}

// Stop cancels the timer; call it only from the node's callbacks.
func (t *rtimer) Stop() bool {
	if t.stopped || t.fired {
		return false
	}
	t.stopped = true
	t.tm.Stop()
	return true
}

// AfterFunc runs f on the node's goroutine after d.
func (nd *node) AfterFunc(d time.Duration, f func()) env.Timer {
	t := &rtimer{}
	t.tm = time.AfterFunc(d, func() {
		nd.post(func() {
			if !t.stopped {
				t.fired = true
				f()
			}
		})
	})
	return t
}

// Send writes one datagram: the sender's ID, then the payload.
func (nd *node) Send(to env.NodeID, payload []byte) error {
	if len(payload) > env.MaxMessage {
		return env.ErrMessageTooLarge
	}
	dst, ok := nd.c.nodes[to]
	if !ok {
		return env.ErrUnknownNode
	}
	frame := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(frame, uint32(nd.id))
	copy(frame[4:], payload)
	_, err := nd.conn.WriteToUDP(frame, dst.addr)
	return err
}

// path checks a file name and joins it to the node's directory.
func (nd *node) path(name string) (string, error) {
	if !env.ValidName(name) {
		return "", env.ErrBadName
	}
	return filepath.Join(nd.dir, name), nil
}

// Append adds data to a file in the node's directory.
func (nd *node) Append(name string, data []byte) error {
	p, err := nd.path(name)
	if err != nil {
		return err
	}
	if st, err := os.Stat(p); err == nil && st.Size()+int64(len(data)) > env.MaxFileSize {
		return env.ErrFileTooLarge
	}
	f, err := os.OpenFile(p, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, werr := f.Write(data)
	return errors.Join(werr, f.Close())
}

// Sync flushes the file to stable storage.
func (nd *node) Sync(name string) error {
	p, err := nd.path(name)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(p, os.O_WRONLY, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return env.ErrNotFound
	}
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

// ReadFile reads a file from the node's directory.
func (nd *node) ReadFile(name string) ([]byte, error) {
	p, err := nd.path(name)
	if err != nil {
		return nil, err
	}
	st, err := os.Stat(p)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, env.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if st.Size() > env.MaxFileSize {
		return nil, env.ErrFileTooLarge
	}
	return os.ReadFile(p)
}

// Uint64 draws from the node's own generator.
func (nd *node) Uint64() uint64 { return nd.rng.Uint64() }

// IntN draws from the node's own generator.
func (nd *node) IntN(n int) int { return nd.rng.IntN(n) }

// Float64 draws from the node's own generator.
func (nd *node) Float64() float64 { return nd.rng.Float64() }

var _ env.Env = (*node)(nil)
