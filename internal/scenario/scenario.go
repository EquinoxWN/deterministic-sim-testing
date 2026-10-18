// Package scenario wires the example key-value store into the simulator or the real runtime,
// runs a workload and checks its invariants.
package scenario

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"time"

	"github.com/EquinoxWN/deterministic-sim-testing/env"
	"github.com/EquinoxWN/deterministic-sim-testing/example/kv"
	"github.com/EquinoxWN/deterministic-sim-testing/realenv"
	"github.com/EquinoxWN/deterministic-sim-testing/sim"
)

// Options sizes the cluster, the workload and the network.
type Options struct {
	Backups       int
	Clients       int
	Ops           int // per client
	Keys          int // per client
	Net           sim.NetConfig
	ClientTimeout time.Duration
	Retransmit    time.Duration
	Bug           bool // backups use kv's ArrivalOrderBug
	Trace         io.Writer
}

// Default is the workload used by the tests, the CLI and the recorded results.
func Default() Options {
	return Options{
		Backups: 2, Clients: 3, Ops: 40, Keys: 4,
		Net:           sim.NetConfig{MinLatency: time.Millisecond, MaxLatency: 20 * time.Millisecond, DropRate: 0.05, DupRate: 0.02},
		ClientTimeout: 100 * time.Millisecond,
		Retransmit:    50 * time.Millisecond,
	}
}

// validate bounds the options so a run cannot grow without limit.
func (o Options) validate() error {
	switch {
	case o.Backups < 0 || o.Backups > 8:
		return errors.New("scenario: backups must be 0..8")
	case o.Clients < 1 || o.Clients > 64:
		return errors.New("scenario: clients must be 1..64")
	case o.Ops < 1 || o.Ops > 10_000:
		return errors.New("scenario: ops must be 1..10000")
	case o.Keys < 1 || o.Keys > 1000:
		return errors.New("scenario: keys must be 1..1000")
	case o.ClientTimeout <= 0 || o.Retransmit <= 0:
		return errors.New("scenario: timeouts must be positive")
	}
	return nil
}

type cluster struct {
	backups  []env.NodeID
	clients  []env.NodeID
	replicas map[env.NodeID]*kv.Replica
	cls      map[env.NodeID]*kv.Client
}

// build creates the nodes: primary 1, backups 2.., clients 101..
func build(o Options) *cluster {
	c := &cluster{replicas: map[env.NodeID]*kv.Replica{}, cls: map[env.NodeID]*kv.Client{}}
	for i := range o.Backups {
		c.backups = append(c.backups, env.NodeID(2+i))
	}
	rc := kv.ReplicaConfig{Backups: c.backups, RetransmitEvery: o.Retransmit, ArrivalOrderBug: o.Bug}
	for _, id := range append([]env.NodeID{kv.PrimaryID}, c.backups...) {
		c.replicas[id] = kv.NewReplica(rc)
	}
	for i := range o.Clients {
		id := env.NodeID(101 + i)
		c.clients = append(c.clients, id)
		c.cls[id] = kv.NewClient(kv.ClientConfig{Ops: o.Ops, Keys: o.Keys, Timeout: o.ClientTimeout})
	}
	return c
}

// nodes lists every node in ID order.
func (c *cluster) nodes() map[env.NodeID]env.Node {
	all := map[env.NodeID]env.Node{}
	for id, r := range c.replicas {
		all[id] = r
	}
	for id, cl := range c.cls {
		all[id] = cl
	}
	return all
}

// Outcome is a finished simulated run and its invariant failures.
type Outcome struct {
	sim.Result
	Failures []string
	Retries  int
}

// OK reports whether the run ended quietly with every invariant holding.
func (o Outcome) OK() bool { return o.Stop == sim.Quiescent && len(o.Failures) == 0 }

// Run simulates one seed and checks the invariants.
func Run(seed uint64, o Options) (Outcome, error) {
	if err := o.validate(); err != nil {
		return Outcome{}, err
	}
	s, err := sim.New(sim.Config{Seed: seed, Net: o.Net, Trace: o.Trace})
	if err != nil {
		return Outcome{}, err
	}
	c := build(o)
	nodes := c.nodes()
	for _, id := range slices.Sorted(maps.Keys(nodes)) {
		if err := s.AddNode(id, nodes[id]); err != nil {
			return Outcome{}, err
		}
	}
	res := s.Run()
	out := Outcome{Result: res}
	if res.Stop != sim.Quiescent {
		out.Failures = append(out.Failures, fmt.Sprintf("run stopped: %s %s", res.Stop, res.Detail))
	}
	out.Failures = append(out.Failures, c.check(o, func(id env.NodeID) ([]byte, error) { return s.Disk(id).ReadFile("wal") })...)
	for _, cl := range c.cls {
		out.Retries += cl.Retries()
	}
	return out, nil
}

// check verifies completion, read correctness, replica agreement and log replay.
func (c *cluster) check(o Options, wal func(env.NodeID) ([]byte, error)) []string {
	var fails []string
	for _, id := range c.clients {
		cl := c.cls[id]
		if cl.Completed() != o.Ops {
			fails = append(fails, fmt.Sprintf("client %d completed %d of %d operations", id, cl.Completed(), o.Ops))
		}
		fails = append(fails, cl.Violations()...)
	}
	primary := c.replicas[kv.PrimaryID]
	logLen, committed, _ := primary.Progress()
	if committed != logLen {
		fails = append(fails, fmt.Sprintf("primary committed %d of %d writes", committed, logLen))
	}
	want := primary.State()
	for _, id := range append([]env.NodeID{kv.PrimaryID}, c.backups...) {
		r := c.replicas[id]
		if id != kv.PrimaryID {
			if d := diff(want, r.State()); d != "" {
				fails = append(fails, fmt.Sprintf("backup %d diverged from the primary: %s", id, d))
			}
		}
		data, err := wal(id)
		if err != nil && !(errors.Is(err, env.ErrNotFound) && logLen == 0) {
			fails = append(fails, fmt.Sprintf("node %d: read wal: %v", id, err))
			continue
		}
		replayed, err := kv.ReplayWAL(data)
		if err != nil {
			fails = append(fails, fmt.Sprintf("node %d: %v", id, err))
		} else if d := diff(r.State(), replayed); d != "" {
			fails = append(fails, fmt.Sprintf("node %d: wal replay differs from memory: %s", id, d))
		}
	}
	return fails
}

// diff describes the first key on which two states differ, or returns "".
func diff(want, got map[string]string) string {
	keys := slices.Sorted(maps.Keys(want))
	for _, k := range slices.Sorted(maps.Keys(got)) {
		if _, ok := want[k]; !ok {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	n, first := 0, ""
	for _, k := range keys {
		w, wok := want[k]
		g, gok := got[k]
		if w != g || wok != gok {
			if n == 0 && !gok {
				first = fmt.Sprintf("%s missing, want %q", k, w)
			} else if n == 0 {
				first = fmt.Sprintf("%s = %q, want %q", k, g, w)
			}
			n++
		}
	}
	if n == 0 {
		return ""
	}
	return fmt.Sprintf("%d of %d keys differ, first %s", n, len(keys), first)
}

// SweepResult summarises many seeds.
type SweepResult struct {
	Runs         int
	Failed       []uint64
	FirstFailure string
	Kinds        map[string]int // failing seeds per kind of first failure
	Events       int
	VirtualTime  time.Duration
	Wall         time.Duration
}

// Sweep runs seeds from, from+1, ... and collects the failing ones.
func Sweep(from uint64, count int, o Options) (SweepResult, error) {
	start := time.Now()
	r := SweepResult{Kinds: map[string]int{}}
	for i := range count {
		seed := from + uint64(i)
		out, err := Run(seed, o)
		if err != nil {
			return r, err
		}
		r.Runs++
		r.Events += out.Events
		r.VirtualTime += out.VirtualTime
		if !out.OK() {
			if len(r.Failed) == 0 {
				r.FirstFailure = out.Failures[0]
			}
			r.Kinds[Kind(out.Failures[0])]++
			r.Failed = append(r.Failed, seed)
		}
	}
	r.Wall = time.Since(start)
	return r, nil
}

var (
	quoted = regexp.MustCompile(`"[^"]*"`)
	number = regexp.MustCompile(`[0-9]+`)
)

// Kind reduces a failure message to its shape by masking values and numbers.
func Kind(failure string) string {
	return number.ReplaceAllString(quoted.ReplaceAllString(failure, "V"), "N")
}

// Replay runs a seed several times and returns each run's trace hash.
func Replay(seed uint64, o Options, runs int) ([]Outcome, error) {
	var outs []Outcome
	for range runs {
		out, err := Run(seed, o)
		if err != nil {
			return nil, err
		}
		outs = append(outs, out)
	}
	return outs, nil
}

// RealOutcome is a run on the real runtime.
type RealOutcome struct {
	Failures []string
	Ops      int
	Retries  int
	Wall     time.Duration
}

// RunReal runs the same nodes over UDP loopback, the wall clock and files under root.
func RunReal(root string, o Options, timeout time.Duration) (RealOutcome, error) {
	if err := o.validate(); err != nil {
		return RealOutcome{}, err
	}
	rc, err := realenv.New(root)
	if err != nil {
		return RealOutcome{}, err
	}
	c := build(o)
	nodes := c.nodes()
	for _, id := range slices.Sorted(maps.Keys(nodes)) {
		if err := rc.Add(id, nodes[id]); err != nil {
			return RealOutcome{}, errors.Join(err, rc.Close())
		}
	}
	start := time.Now()
	if err := rc.Start(); err != nil {
		return RealOutcome{}, errors.Join(err, rc.Close())
	}
	settled := waitFor(timeout, func() bool { return c.settled(rc, o) })
	wall := time.Since(start)
	if err := rc.Close(); err != nil {
		return RealOutcome{}, err
	}
	out := RealOutcome{Wall: wall, Ops: o.Ops * o.Clients}
	if !settled {
		out.Failures = append(out.Failures, fmt.Sprintf("not settled after %s", timeout))
	}
	out.Failures = append(out.Failures, c.check(o, func(id env.NodeID) ([]byte, error) {
		b, err := os.ReadFile(filepath.Join(rc.Dir(id), "wal"))
		if errors.Is(err, os.ErrNotExist) {
			return nil, env.ErrNotFound
		}
		return b, err
	})...)
	for _, cl := range c.cls {
		out.Retries += cl.Retries()
	}
	return out, nil
}

// settled reports, from each node's own goroutine, whether all work is done and replicated.
func (c *cluster) settled(rc *realenv.Cluster, o Options) bool {
	ok := true
	for _, id := range c.clients {
		cl := c.cls[id]
		_ = rc.Do(id, func() { ok = ok && cl.Completed() == o.Ops })
	}
	var logLen, committed uint64
	_ = rc.Do(kv.PrimaryID, func() { logLen, committed, _ = c.replicas[kv.PrimaryID].Progress() })
	ok = ok && committed == logLen
	for _, id := range c.backups {
		r := c.replicas[id]
		_ = rc.Do(id, func() { _, _, applied := r.Progress(); ok = ok && applied == logLen })
	}
	return ok
}

// waitFor polls cond every 10 ms until it holds or the timeout passes.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}
