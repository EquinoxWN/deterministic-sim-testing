// Package sim runs nodes in a deterministic, single-threaded simulation. One seeded random
// generator decides every scheduling choice (message latency, drops, duplicates, the order of
// simultaneous events) and every value a node draws from env.Rand, so the same seed and the same
// code always produce the same run, recorded as a trace hash.
package sim

import (
	"container/heap"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/EquinoxWN/deterministic-sim-testing/env"
)

// Epoch is the virtual time at which every simulation starts.
var Epoch = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

// Defaults for the run limits.
const (
	DefaultMaxEvents = 5_000_000
	DefaultMaxTime   = 24 * time.Hour
)

// NetConfig describes the simulated network.
type NetConfig struct {
	MinLatency time.Duration // shortest one-way delay
	MaxLatency time.Duration // longest one-way delay
	DropRate   float64       // probability a message is lost
	DupRate    float64       // probability a message is delivered twice
}

// Config describes one simulation run.
type Config struct {
	Seed      uint64
	Net       NetConfig
	MaxEvents int           // stop after this many events (default DefaultMaxEvents)
	MaxTime   time.Duration // stop at this virtual time (default DefaultMaxTime)
	Trace     io.Writer     // optional: every event as one line
}

// StopReason says why a run ended.
type StopReason string

// Reasons a run ends.
const (
	Quiescent        StopReason = "quiescent"
	MaxEventsReached StopReason = "max-events"
	MaxTimeReached   StopReason = "max-time"
	Halted           StopReason = "halted"
	Panicked         StopReason = "panic"
)

// Result summarises a finished run.
type Result struct {
	Seed        uint64
	Stop        StopReason
	Detail      string // halt reason or panic value
	Events      int    // starts, deliveries and timers that ran
	Sent        int
	Delivered   int
	Dropped     int
	Duplicated  int
	TimersFired int
	VirtualTime time.Duration
	TraceHash   string // SHA-256 over every event line
}

type eventKind uint8

const (
	evStart eventKind = iota
	evDeliver
	evTimer
)

type event struct {
	at      time.Duration
	tie     uint64
	seq     uint64
	kind    eventKind
	node    env.NodeID
	from    env.NodeID
	payload []byte
	timer   *timer
}

type queue []*event

// Len is the number of pending events.
func (q queue) Len() int { return len(q) }

// Less orders by time, then the random tie-breaker, then insertion.
func (q queue) Less(i, j int) bool {
	a, b := q[i], q[j]
	if a.at != b.at {
		return a.at < b.at
	}
	if a.tie != b.tie {
		return a.tie < b.tie
	}
	return a.seq < b.seq
}

// Swap exchanges two events.
func (q queue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }

// Push adds an event.
func (q *queue) Push(x any) { *q = append(*q, x.(*event)) }

// Pop removes the last event.
func (q *queue) Pop() any {
	old := *q
	e := old[len(old)-1]
	old[len(old)-1] = nil
	*q = old[:len(old)-1]
	return e
}

type timer struct {
	id      uint64
	f       func()
	stopped bool
	fired   bool
}

// Stop cancels the timer if it has not fired.
func (t *timer) Stop() bool {
	if t.stopped || t.fired {
		return false
	}
	t.stopped = true
	return true
}

type nodeState struct {
	node env.Node
	env  *nodeEnv
}

// Simulator owns the virtual clock, the event queue and the random generator.
type Simulator struct {
	cfg       Config
	rng       *rand.Rand
	now       time.Duration
	seq       uint64
	nextTimer uint64
	q         queue
	nodes     map[env.NodeID]*nodeState
	order     []env.NodeID
	hash      hash.Hash
	line      []byte
	res       Result
	halted    bool
	ran       bool
}

// New checks the configuration and returns an empty simulator.
func New(cfg Config) (*Simulator, error) {
	n := cfg.Net
	switch {
	case n.MinLatency < 0 || n.MaxLatency < n.MinLatency:
		return nil, errors.New("sim: need 0 <= MinLatency <= MaxLatency")
	case n.DropRate < 0 || n.DropRate > 1 || n.DupRate < 0 || n.DupRate > 1:
		return nil, errors.New("sim: DropRate and DupRate must be in [0, 1]")
	case cfg.MaxEvents < 0 || cfg.MaxTime < 0:
		return nil, errors.New("sim: limits must not be negative")
	}
	if cfg.MaxEvents == 0 {
		cfg.MaxEvents = DefaultMaxEvents
	}
	if cfg.MaxTime == 0 {
		cfg.MaxTime = DefaultMaxTime
	}
	return &Simulator{
		cfg:   cfg,
		rng:   rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15)),
		nodes: map[env.NodeID]*nodeState{},
		hash:  sha256.New(),
		res:   Result{Seed: cfg.Seed},
	}, nil
}

// AddNode registers a node; it must be called before Run.
func (s *Simulator) AddNode(id env.NodeID, n env.Node) error {
	if s.ran {
		return errors.New("sim: AddNode after Run")
	}
	if _, dup := s.nodes[id]; dup {
		return fmt.Errorf("sim: node %d added twice", id)
	}
	s.nodes[id] = &nodeState{node: n, env: &nodeEnv{s: s, id: id, Disk: NewDisk()}}
	s.order = append(s.order, id)
	slices.Sort(s.order)
	return nil
}

// Disk returns a node's simulated disk for inspection.
func (s *Simulator) Disk(id env.NodeID) *Disk {
	if n, ok := s.nodes[id]; ok {
		return n.env.Disk
	}
	return nil
}

// Now returns the current virtual time.
func (s *Simulator) Now() time.Time { return Epoch.Add(s.now) }

// Halt stops the run after the current event, for example when an invariant fails.
func (s *Simulator) Halt(reason string) {
	if !s.halted {
		s.halted = true
		s.res.Detail = reason
	}
}

// Run executes events until the queue is empty, a limit is reached, Halt is called or a node panics.
func (s *Simulator) Run() (res Result) {
	if s.ran {
		panic("sim: Run called twice")
	}
	s.ran = true
	for _, id := range s.order {
		s.push(&event{kind: evStart, node: id})
	}
	defer func() {
		if r := recover(); r != nil {
			s.res.Stop, s.res.Detail = Panicked, fmt.Sprint(r)
			s.traceLine("panic %v", r)
			res = s.finish()
		}
	}()
	for {
		if s.halted {
			s.res.Stop = Halted
			break
		}
		if len(s.q) == 0 {
			s.res.Stop = Quiescent
			break
		}
		if s.res.Events >= s.cfg.MaxEvents {
			s.res.Stop = MaxEventsReached
			break
		}
		ev := heap.Pop(&s.q).(*event)
		if ev.at > s.cfg.MaxTime {
			s.res.Stop = MaxTimeReached
			break
		}
		s.now = ev.at
		s.dispatch(ev)
	}
	return s.finish()
}

// finish fills in the totals.
func (s *Simulator) finish() Result {
	s.res.VirtualTime = s.now
	s.res.TraceHash = hex.EncodeToString(s.hash.Sum(nil))
	return s.res
}

// dispatch runs one event's callback.
func (s *Simulator) dispatch(ev *event) {
	n := s.nodes[ev.node]
	switch ev.kind {
	case evStart:
		s.res.Events++
		s.traceLine("%d start n%d", s.now, ev.node)
		n.node.Start(n.env)
	case evDeliver:
		s.res.Events++
		s.res.Delivered++
		h := fnv.New64a()
		_, _ = h.Write(ev.payload)
		s.traceLine("%d recv n%d<-n%d len=%d sum=%016x", s.now, ev.node, ev.from, len(ev.payload), h.Sum64())
		n.node.Receive(ev.from, ev.payload)
	case evTimer:
		if ev.timer.stopped {
			return
		}
		ev.timer.fired = true
		s.res.Events++
		s.res.TimersFired++
		s.traceLine("%d timer n%d t%d", s.now, ev.node, ev.timer.id)
		ev.timer.f()
	}
}

// push schedules an event with a random tie-breaker.
func (s *Simulator) push(ev *event) {
	s.seq++
	ev.seq = s.seq
	ev.tie = s.rng.Uint64()
	heap.Push(&s.q, ev)
}

// traceLine adds one line to the trace hash and the optional trace writer.
func (s *Simulator) traceLine(format string, args ...any) {
	s.line = fmt.Appendf(s.line[:0], format, args...)
	s.line = append(s.line, '\n')
	_, _ = s.hash.Write(s.line)
	if s.cfg.Trace != nil {
		_, _ = s.cfg.Trace.Write(s.line)
	}
}

// latency draws a one-way delay.
func (s *Simulator) latency() time.Duration {
	lo, hi := s.cfg.Net.MinLatency, s.cfg.Net.MaxLatency
	if hi == lo {
		return lo
	}
	return lo + time.Duration(s.rng.Int64N(int64(hi-lo)+1))
}

type nodeEnv struct {
	s  *Simulator
	id env.NodeID
	*Disk
}

// ID returns the node's identity.
func (e *nodeEnv) ID() env.NodeID { return e.id }

// Now returns the virtual time.
func (e *nodeEnv) Now() time.Time { return e.s.Now() }

// AfterFunc schedules f on this node after d of virtual time.
func (e *nodeEnv) AfterFunc(d time.Duration, f func()) env.Timer {
	if d < 0 {
		d = 0
	}
	e.s.nextTimer++
	t := &timer{id: e.s.nextTimer, f: f}
	e.s.push(&event{at: e.s.now + d, kind: evTimer, node: e.id, timer: t})
	return t
}

// Send applies the network model: drop, duplicate, then a random latency per copy.
func (e *nodeEnv) Send(to env.NodeID, payload []byte) error {
	s := e.s
	if len(payload) > env.MaxMessage {
		return env.ErrMessageTooLarge
	}
	if _, ok := s.nodes[to]; !ok {
		return env.ErrUnknownNode
	}
	s.res.Sent++
	if s.rng.Float64() < s.cfg.Net.DropRate {
		s.res.Dropped++
		s.traceLine("%d drop n%d->n%d len=%d", s.now, e.id, to, len(payload))
		return nil
	}
	copies := 1
	if s.rng.Float64() < s.cfg.Net.DupRate {
		copies = 2
		s.res.Duplicated++
	}
	for range copies {
		s.push(&event{at: s.now + s.latency(), kind: evDeliver, node: to, from: e.id, payload: slices.Clone(payload)})
	}
	return nil
}

// Uint64 draws from the simulation's generator.
func (e *nodeEnv) Uint64() uint64 { return e.s.rng.Uint64() }

// IntN draws from the simulation's generator.
func (e *nodeEnv) IntN(n int) int { return e.s.rng.IntN(n) }

// Float64 draws from the simulation's generator.
func (e *nodeEnv) Float64() float64 { return e.s.rng.Float64() }

var _ env.Env = (*nodeEnv)(nil)
