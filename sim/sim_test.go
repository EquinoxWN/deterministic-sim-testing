package sim

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/EquinoxWN/deterministic-sim-testing/env"
)

// pinger bounces a payload back and forth and logs what it sees.
type pinger struct {
	peer  env.NodeID
	serve bool
	left  int
	e     env.Env
	log   []string
}

func (p *pinger) Start(e env.Env) {
	p.e = e
	if p.serve {
		_ = e.Send(p.peer, []byte("ping"))
		e.AfterFunc(time.Duration(e.IntN(50))*time.Millisecond, func() { _ = e.Send(p.peer, []byte("late")) })
	}
}

func (p *pinger) Receive(from env.NodeID, b []byte) {
	p.log = append(p.log, fmt.Sprintf("%s<-%d@%s", b, from, p.e.Now().Sub(Epoch)))
	if p.left > 0 {
		p.left--
		_ = p.e.Send(from, b)
	}
}

// pingRun runs two pingers on a lossy network.
func pingRun(t *testing.T, seed uint64) (Result, []string) {
	t.Helper()
	s, err := New(Config{Seed: seed, Net: NetConfig{MinLatency: time.Millisecond, MaxLatency: 30 * time.Millisecond, DropRate: 0.1, DupRate: 0.1}})
	if err != nil {
		t.Fatal(err)
	}
	a, b := &pinger{peer: 2, serve: true, left: 30}, &pinger{peer: 1, left: 30}
	if err := s.AddNode(2, b); err != nil {
		t.Fatal(err)
	}
	if err := s.AddNode(1, a); err != nil {
		t.Fatal(err)
	}
	res := s.Run()
	return res, append(a.log, b.log...)
}

func TestSameSeedSameRun(t *testing.T) {
	r1, l1 := pingRun(t, 42)
	r2, l2 := pingRun(t, 42)
	if !reflect.DeepEqual(r1, r2) || !reflect.DeepEqual(l1, l2) {
		t.Fatalf("seed 42 diverged:\n%+v\n%+v", r1, r2)
	}
	if r1.Stop != Quiescent || r1.Delivered == 0 || len(l1) != r1.Delivered {
		t.Fatalf("unexpected run %+v with %d log lines", r1, len(l1))
	}
}

func TestDifferentSeedsDifferentRuns(t *testing.T) {
	seen := map[string]uint64{}
	for seed := uint64(1); seed <= 20; seed++ {
		r, _ := pingRun(t, seed)
		if prev, dup := seen[r.TraceHash]; dup {
			t.Fatalf("seeds %d and %d produced the same trace", prev, seed)
		}
		seen[r.TraceHash] = seed
	}
}

// recorder logs timer firings with their virtual time.
type recorder struct {
	e     env.Env
	order []string
	stop  func(e env.Env, r *recorder)
}

func (r *recorder) Start(e env.Env) {
	r.e = e
	for _, d := range []time.Duration{30, 10, 20, 0} {
		e.AfterFunc(d*time.Millisecond, func() { r.order = append(r.order, e.Now().Sub(Epoch).String()) })
	}
	if r.stop != nil {
		r.stop(e, r)
	}
}

func (r *recorder) Receive(env.NodeID, []byte) {}

func TestTimersFireInVirtualTimeOrder(t *testing.T) {
	s, _ := New(Config{Seed: 7})
	r := &recorder{}
	_ = s.AddNode(1, r)
	res := s.Run()
	want := []string{"0s", "10ms", "20ms", "30ms"}
	if !reflect.DeepEqual(r.order, want) || res.TimersFired != 4 || res.VirtualTime != 30*time.Millisecond {
		t.Fatalf("order %v, result %+v", r.order, res)
	}
}

func TestTimerStop(t *testing.T) {
	s, _ := New(Config{Seed: 7})
	var first, second, afterFire bool
	r := &recorder{stop: func(e env.Env, r *recorder) {
		tm := e.AfterFunc(5*time.Millisecond, func() { r.order = append(r.order, "cancelled") })
		first, second = tm.Stop(), tm.Stop()
		var fired env.Timer
		fired = e.AfterFunc(time.Millisecond, func() { afterFire = !fired.Stop() })
	}}
	_ = s.AddNode(1, r)
	s.Run()
	if !first || second || !afterFire || strings.Contains(strings.Join(r.order, ","), "cancelled") {
		t.Fatalf("Stop: first %v second %v after fire %v, order %v", first, second, afterFire, r.order)
	}
}

// blaster sends n messages at start and records send errors.
type blaster struct {
	n    int
	errs []error
}

func (b *blaster) Start(e env.Env) {
	for range b.n {
		_ = e.Send(2, []byte("x"))
	}
	b.errs = append(b.errs, e.Send(2, make([]byte, env.MaxMessage+1)), e.Send(99, nil))
}

func (b *blaster) Receive(env.NodeID, []byte) {}

// sink records arrival times.
type sink struct {
	e    env.Env
	arr  []time.Duration
	from map[env.NodeID]int
}

func (k *sink) Start(e env.Env) { k.e, k.from = e, map[env.NodeID]int{} }

func (k *sink) Receive(from env.NodeID, _ []byte) {
	k.arr = append(k.arr, k.e.Now().Sub(Epoch))
	k.from[from]++
}

func TestNetworkModel(t *testing.T) {
	const n = 20000
	s, _ := New(Config{Seed: 3, Net: NetConfig{MinLatency: 2 * time.Millisecond, MaxLatency: 9 * time.Millisecond, DropRate: 0.1, DupRate: 0.05}})
	b, k := &blaster{n: n}, &sink{}
	_ = s.AddNode(1, b)
	_ = s.AddNode(2, k)
	res := s.Run()
	if res.Sent != n || res.Delivered != n-res.Dropped+res.Duplicated || len(k.arr) != res.Delivered {
		t.Fatalf("counts do not add up: %+v, arrivals %d", res, len(k.arr))
	}
	if d := float64(res.Dropped) / n; d < 0.09 || d > 0.11 {
		t.Fatalf("drop rate %.3f, want about 0.10", d)
	}
	if d := float64(res.Duplicated) / float64(n-res.Dropped); d < 0.04 || d > 0.06 {
		t.Fatalf("dup rate %.3f, want about 0.05", d)
	}
	for _, a := range k.arr {
		if a < 2*time.Millisecond || a > 9*time.Millisecond {
			t.Fatalf("latency %s outside [2ms, 9ms]", a)
		}
	}
	if !errors.Is(b.errs[0], env.ErrMessageTooLarge) || !errors.Is(b.errs[1], env.ErrUnknownNode) {
		t.Fatalf("send errors %v", b.errs)
	}
}

// ticker re-arms a one-second timer forever.
type ticker struct{ e env.Env }

func (k *ticker) Start(e env.Env) { k.e = e; k.tick() }
func (k *ticker) tick()           { k.e.AfterFunc(time.Second, k.tick) }

func (k *ticker) Receive(env.NodeID, []byte) {}

func TestLimitsStopEndlessRuns(t *testing.T) {
	s, _ := New(Config{Seed: 1, MaxTime: 10 * time.Second})
	_ = s.AddNode(1, &ticker{})
	res := s.Run()
	if res.Stop != MaxTimeReached || res.TimersFired != 10 || res.VirtualTime != 10*time.Second {
		t.Fatalf("max time: %+v", res)
	}
	s, _ = New(Config{Seed: 1, MaxEvents: 5})
	_ = s.AddNode(1, &ticker{})
	if res := s.Run(); res.Stop != MaxEventsReached || res.Events != 5 {
		t.Fatalf("max events: %+v", res)
	}
}

// fn runs a callback at start.
type fn func(e env.Env)

func (f fn) Start(e env.Env)            { f(e) }
func (f fn) Receive(env.NodeID, []byte) {}

func TestHaltAndPanicEndTheRun(t *testing.T) {
	s, _ := New(Config{Seed: 1})
	_ = s.AddNode(1, fn(func(e env.Env) { e.AfterFunc(time.Second, func() { s.Halt("invariant broken") }) }))
	_ = s.AddNode(2, &ticker{})
	if res := s.Run(); res.Stop != Halted || res.Detail != "invariant broken" || res.VirtualTime != time.Second {
		t.Fatalf("halt: %+v", res)
	}
	s, _ = New(Config{Seed: 1})
	_ = s.AddNode(1, fn(func(env.Env) { panic("node bug") }))
	res := s.Run()
	if res.Stop != Panicked || res.Detail != "node bug" || res.TraceHash == "" {
		t.Fatalf("panic: %+v", res)
	}
}

func TestConfigAndSetupErrors(t *testing.T) {
	bad := []Config{
		{Net: NetConfig{MinLatency: -1}},
		{Net: NetConfig{MinLatency: 2, MaxLatency: 1}},
		{Net: NetConfig{DropRate: 1.5}},
		{Net: NetConfig{DupRate: -0.1}},
		{MaxEvents: -1},
		{MaxTime: -1},
	}
	for i, c := range bad {
		if _, err := New(c); err == nil {
			t.Errorf("config %d accepted", i)
		}
	}
	s, _ := New(Config{})
	if err := s.AddNode(1, &ticker{}); err != nil {
		t.Fatal(err)
	}
	if err := s.AddNode(1, &ticker{}); err == nil {
		t.Error("duplicate node accepted")
	}
	if s.Disk(1) == nil || s.Disk(9) != nil {
		t.Error("Disk lookup wrong")
	}
	s.Halt("stop at once")
	s.Run()
	if err := s.AddNode(2, &ticker{}); err == nil {
		t.Error("AddNode after Run accepted")
	}
	defer func() {
		if recover() == nil {
			t.Error("second Run did not panic")
		}
	}()
	s.Run()
}

// mapSender sends in map iteration order: the classic hidden source of nondeterminism.
type mapSender struct{}

func (mapSender) Start(e env.Env) {
	m := map[int]bool{}
	for i := range 16 {
		m[i] = true
	}
	for k := range m {
		_ = e.Send(2, []byte{byte(k)})
	}
}

func (mapSender) Receive(env.NodeID, []byte) {}

func TestTraceHashExposesMapOrderNondeterminism(t *testing.T) {
	hashes := map[string]bool{}
	for range 10 {
		s, _ := New(Config{Seed: 5, Net: NetConfig{MaxLatency: time.Millisecond}})
		_ = s.AddNode(1, mapSender{})
		_ = s.AddNode(2, &sink{})
		hashes[s.Run().TraceHash] = true
	}
	if len(hashes) < 2 {
		t.Fatal("ten runs of a map-order-dependent node gave one trace; the hash should expose it")
	}
}
