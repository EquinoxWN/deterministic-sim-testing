package kv

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/EquinoxWN/deterministic-sim-testing/env"
	"github.com/EquinoxWN/deterministic-sim-testing/sim"
)

type sent struct {
	to env.NodeID
	m  message
}

type fakeTimer struct {
	f       func()
	stopped bool
}

func (t *fakeTimer) Stop() bool { was := !t.stopped; t.stopped = true; return was }

// fakeEnv captures sends and timers so one node can be driven by hand.
type fakeEnv struct {
	id     env.NodeID
	out    []sent
	timers []*fakeTimer
	ints   []int
	*sim.Disk
}

func newFake(id env.NodeID, ints ...int) *fakeEnv {
	return &fakeEnv{id: id, ints: ints, Disk: sim.NewDisk()}
}

func (f *fakeEnv) ID() env.NodeID   { return f.id }
func (f *fakeEnv) Now() time.Time   { return sim.Epoch }
func (f *fakeEnv) Uint64() uint64   { return 0 }
func (f *fakeEnv) Float64() float64 { return 0 }

func (f *fakeEnv) IntN(int) int {
	if len(f.ints) == 0 {
		return 0
	}
	v := f.ints[0]
	f.ints = f.ints[1:]
	return v
}

func (f *fakeEnv) AfterFunc(_ time.Duration, fn func()) env.Timer {
	t := &fakeTimer{f: fn}
	f.timers = append(f.timers, t)
	return t
}

func (f *fakeEnv) Send(to env.NodeID, p []byte) error {
	m, ok := decode(p)
	if !ok {
		panic("node sent an undecodable message: " + string(p))
	}
	f.out = append(f.out, sent{to, m})
	return nil
}

// take returns and clears the captured sends.
func (f *fakeEnv) take() []sent { s := f.out; f.out = nil; return s }

// fire runs the newest timer that is still pending.
func (f *fakeEnv) fire(t *testing.T) {
	t.Helper()
	for i := len(f.timers) - 1; i >= 0; i-- {
		if tm := f.timers[i]; !tm.stopped {
			tm.stopped = true
			tm.f()
			return
		}
	}
	t.Fatal("no pending timer")
}

func repl(seq uint64, k, v string) []byte {
	return encode(message{Type: tRepl, Seq: seq, Key: k, Value: v})
}

func acks(s []sent) []uint64 {
	var out []uint64
	for _, x := range s {
		if x.m.Type == tAck && x.to == PrimaryID {
			out = append(out, x.m.Seq)
		}
	}
	return out
}

// walState replays a node's log.
func walState(t *testing.T, f *fakeEnv) map[string]string {
	t.Helper()
	b, err := f.ReadFile(walName)
	if err != nil {
		t.Fatal(err)
	}
	st, err := ReplayWAL(b)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestBackupAppliesInSequenceOrder(t *testing.T) {
	f := newFake(2)
	r := NewReplica(ReplicaConfig{Backups: []env.NodeID{2, 3}})
	r.Start(f)
	r.Receive(PrimaryID, repl(2, "a", "second"))
	if got := acks(f.take()); !reflect.DeepEqual(got, []uint64{0}) || len(r.State()) != 0 {
		t.Fatalf("out-of-order write applied early: acks %v state %v", got, r.State())
	}
	r.Receive(PrimaryID, repl(1, "a", "first"))
	r.Receive(PrimaryID, repl(1, "a", "first")) // duplicate
	if got := acks(f.take()); !reflect.DeepEqual(got, []uint64{2, 2}) {
		t.Fatalf("acks %v", got)
	}
	want := map[string]string{"a": "second"}
	if !reflect.DeepEqual(r.State(), want) || !reflect.DeepEqual(walState(t, f), want) {
		t.Fatalf("state %v wal %v", r.State(), walState(t, f))
	}
	if b, _ := f.ReadFile(walName); f.Synced(walName) != len(b) || strings.Count(string(b), "\n") != 2 {
		t.Fatalf("wal not synced or duplicated: %q", b)
	}
}

func TestArrivalOrderBugKeepsStaleValue(t *testing.T) {
	f := newFake(2)
	r := NewReplica(ReplicaConfig{Backups: []env.NodeID{2}, ArrivalOrderBug: true})
	r.Start(f)
	r.Receive(PrimaryID, repl(2, "a", "second"))
	r.Receive(PrimaryID, repl(1, "a", "first"))
	if got := acks(f.take()); !reflect.DeepEqual(got, []uint64{2, 2}) {
		t.Fatalf("buggy backup should ack the highest seen: %v", got)
	}
	if r.State()["a"] != "first" {
		t.Fatalf("buggy backup should end on the stale value, got %v", r.State())
	}
}

func TestBackupIgnoresNonPrimary(t *testing.T) {
	f := newFake(2)
	r := NewReplica(ReplicaConfig{Backups: []env.NodeID{2, 3}})
	r.Start(f)
	r.Receive(3, repl(1, "a", "x"))
	r.Receive(101, repl(1, "a", "x"))
	r.Receive(PrimaryID, encode(message{Type: tPut, Req: 1, Key: "a"}))
	if len(f.take()) != 0 || len(r.State()) != 0 {
		t.Fatal("backup accepted a message it should ignore")
	}
}

// primaryWithPut starts a primary with two backups and one write from client 101.
func primaryWithPut(t *testing.T) (*Replica, *fakeEnv) {
	t.Helper()
	f := newFake(PrimaryID)
	r := NewReplica(ReplicaConfig{Backups: []env.NodeID{2, 3}, RetransmitEvery: time.Millisecond})
	r.Start(f)
	r.Receive(101, encode(message{Type: tPut, Req: 1, Key: "a", Value: "1"}))
	return r, f
}

func TestPrimaryCommitsOnlyAfterEveryBackup(t *testing.T) {
	r, f := primaryWithPut(t)
	out := f.take()
	if len(out) != 2 || out[0].to != 2 || out[1].to != 3 || out[0].m.Type != tRepl {
		t.Fatalf("expected replication to 2 and 3, got %+v", out)
	}
	r.Receive(2, encode(message{Type: tAck, Seq: 1}))
	if len(f.take()) != 0 || len(r.State()) != 0 {
		t.Fatal("primary answered before every backup acknowledged")
	}
	r.Receive(3, encode(message{Type: tAck, Seq: 1}))
	out = f.take()
	if len(out) != 1 || out[0].to != 101 || out[0].m.Type != tReply || out[0].m.Req != 1 || r.State()["a"] != "1" {
		t.Fatalf("expected reply to client after both acks, got %+v state %v", out, r.State())
	}
	if !f.timers[0].stopped {
		t.Fatal("resend timer still armed with nothing to resend")
	}
	if l, c, _ := r.Progress(); l != 1 || c != 1 {
		t.Fatalf("progress %d/%d", c, l)
	}
}

func TestPrimaryDeduplicatesRetries(t *testing.T) {
	r, f := primaryWithPut(t)
	r.Receive(101, encode(message{Type: tPut, Req: 1, Key: "a", Value: "1"}))
	if l, _, _ := r.Progress(); l != 1 {
		t.Fatalf("retry was ordered twice: log %d", l)
	}
	r.Receive(2, encode(message{Type: tAck, Seq: 1}))
	r.Receive(3, encode(message{Type: tAck, Seq: 1}))
	f.take()
	r.Receive(101, encode(message{Type: tPut, Req: 1, Key: "a", Value: "1"}))
	r.Receive(101, encode(message{Type: tPut, Req: 0, Key: "a", Value: "old"}))
	out := f.take()
	if len(out) != 1 || out[0].m.Req != 1 {
		t.Fatalf("retry after commit should get the cached reply only, got %+v", out)
	}
	r.Receive(101, encode(message{Type: tGet, Req: 2, Key: "a"}))
	r.Receive(101, encode(message{Type: tGet, Req: 2, Key: "a"}))
	out = f.take()
	if len(out) != 2 || out[0].m != out[1].m || !out[0].m.Found || out[0].m.Value != "1" {
		t.Fatalf("get and its retry: %+v", out)
	}
}

func TestPrimaryResendsUnacknowledgedWrites(t *testing.T) {
	r, f := primaryWithPut(t)
	f.take()
	f.fire(t)
	if out := f.take(); len(out) != 2 {
		t.Fatalf("first resend should go to both backups: %+v", out)
	}
	r.Receive(2, encode(message{Type: tAck, Seq: 1}))
	f.fire(t)
	out := f.take()
	if len(out) != 1 || out[0].to != 3 || out[0].m.Seq != 1 {
		t.Fatalf("second resend should go to backup 3 only: %+v", out)
	}
}

func TestPrimaryIgnoresStrangersAndBadInput(t *testing.T) {
	r, f := primaryWithPut(t)
	f.take()
	r.Receive(101, encode(message{Type: tAck, Seq: 1}))         // a client cannot acknowledge
	r.Receive(2, encode(message{Type: tPut, Req: 9, Key: "b"})) // a backup is not a client
	r.Receive(2, encode(message{Type: tAck, Seq: 99}))          // beyond the log
	r.Receive(101, []byte(`{"t":"put","r":5,"x":1}`))
	if len(f.take()) != 0 {
		t.Fatal("primary reacted to a message it should ignore")
	}
	if l, c, _ := r.Progress(); l != 1 || c != 0 {
		t.Fatalf("progress changed: %d/%d", c, l)
	}
}

func TestDecodeRejectsMalformed(t *testing.T) {
	bad := [][]byte{
		[]byte(`not json`),
		[]byte(`{"t":"put","unknown":1}`),
		[]byte(`{"t":"put","k":"` + strings.Repeat("k", maxKey+1) + `"}`),
		[]byte(`{"t":"put","v":"` + strings.Repeat("v", maxValue+1) + `"}`),
		make([]byte, env.MaxMessage+1),
	}
	for i, b := range bad {
		if _, ok := decode(b); ok {
			t.Errorf("input %d accepted", i)
		}
	}
	m := message{Type: tReply, Req: 3, Key: "k", Value: "v", Found: true}
	if got, ok := decode(encode(m)); !ok || got != m {
		t.Fatalf("round trip: %+v", got)
	}
}

func TestReplayWALAndHashState(t *testing.T) {
	st, err := ReplayWAL([]byte("{\"s\":1,\"k\":\"a\",\"v\":\"1\"}\n{\"s\":2,\"k\":\"a\",\"v\":\"2\"}\n"))
	if err != nil || !reflect.DeepEqual(st, map[string]string{"a": "2"}) {
		t.Fatalf("replay %v %v", st, err)
	}
	if _, err := ReplayWAL([]byte("{\"s\":1,\"k\":\"a\",\"v\":\"1\"}\n{broken\n")); err == nil || !strings.Contains(err.Error(), "line 2") {
		t.Fatalf("corrupt log: %v", err)
	}
	a := map[string]string{"x": "1", "y": "2"}
	b := map[string]string{"y": "2", "x": "1"}
	c := map[string]string{"x": "1", "y": "3"}
	if HashState(a) != HashState(b) || HashState(a) == HashState(c) {
		t.Fatal("HashState must depend on contents only")
	}
}

func TestClientFlagsStaleReadAndRetries(t *testing.T) {
	// IntN draws: key 0, put, jitter; retry jitter; key 0, get, jitter.
	f := newFake(101, 0, 0, 0, 0, 0, 1, 0)
	c := NewClient(ClientConfig{Ops: 2, Keys: 1, Timeout: 10 * time.Millisecond})
	c.Start(f)
	put := f.take()[0].m
	if put.Type != tPut || put.Key != "c101-k0" || put.Value != "v101-1" {
		t.Fatalf("first op %+v", put)
	}
	f.fire(t)
	if retry := f.take(); len(retry) != 1 || retry[0].m != put || c.Retries() != 1 {
		t.Fatalf("retry %+v", retry)
	}
	c.Receive(PrimaryID, encode(message{Type: tReply, Req: 7}))
	c.Receive(2, encode(message{Type: tReply, Req: 1}))
	if c.Completed() != 0 {
		t.Fatal("client accepted a stale or foreign reply")
	}
	c.Receive(PrimaryID, encode(message{Type: tReply, Req: 1}))
	get := f.take()[0].m
	if get.Type != tGet || get.Req != 2 {
		t.Fatalf("second op %+v", get)
	}
	c.Receive(PrimaryID, encode(message{Type: tReply, Req: 2, Key: get.Key, Value: "v101-0", Found: true}))
	if c.Completed() != 2 || len(c.Violations()) != 1 || !strings.Contains(c.Violations()[0], `last acknowledged write "v101-1"`) {
		t.Fatalf("completed %d violations %v", c.Completed(), c.Violations())
	}
	if len(f.take()) != 0 {
		t.Fatal("client kept sending after its last operation")
	}
}
