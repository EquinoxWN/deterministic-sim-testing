package realenv

import (
	"encoding/binary"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/EquinoxWN/deterministic-sim-testing/env"
)

// node under test: logs events to a channel owned by the test.
type probe struct {
	e      env.Env
	events chan string
	start  func(p *probe)
	recv   func(p *probe, from env.NodeID, b []byte)
}

func (p *probe) Start(e env.Env) {
	p.e = e
	if p.start != nil {
		p.start(p)
	}
}

func (p *probe) Receive(from env.NodeID, b []byte) {
	if p.recv != nil {
		p.recv(p, from, b)
	}
}

// expect waits for the next event.
func expect(t *testing.T, ch chan string, want string) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("event %q, want %q", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %q", want)
	}
}

// newCluster builds a cluster in a test directory and closes it at the end.
func newCluster(t *testing.T, nodes map[env.NodeID]env.Node) *Cluster {
	t.Helper()
	c, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	for id := env.NodeID(1); id <= 9; id++ {
		if n, ok := nodes[id]; ok {
			if err := c.Add(id, n); err != nil {
				t.Fatal(err)
			}
		}
	}
	return c
}

func TestMessagesTimersAndFilesOnTheRealRuntime(t *testing.T) {
	events := make(chan string, 16)
	a := &probe{events: events,
		start: func(p *probe) {
			if err := p.e.Append("wal", []byte("hello")); err != nil {
				events <- err.Error()
			}
			_ = p.e.Sync("wal")
			_ = p.e.Send(2, []byte("ping"))
		},
		recv: func(p *probe, from env.NodeID, b []byte) {
			data, err := p.e.ReadFile("wal")
			if err == nil && from == 2 && string(b) == "pong" {
				events <- "a got pong, file " + string(data)
			}
		}}
	b := &probe{events: events,
		recv: func(p *probe, from env.NodeID, msg []byte) {
			before := p.e.Now()
			p.e.AfterFunc(20*time.Millisecond, func() {
				if p.e.Now().Sub(before) >= 20*time.Millisecond && p.e.IntN(10) < 10 {
					_ = p.e.Send(from, []byte("pong"))
				}
			})
		}}
	c := newCluster(t, map[env.NodeID]env.Node{1: a, 2: b})
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	expect(t, events, "a got pong, file hello")
	if got, err := os.ReadFile(filepath.Join(c.Dir(1), "wal")); err != nil || string(got) != "hello" {
		t.Fatalf("file on disk %q %v", got, err)
	}
}

func TestStoppedTimerNeverRuns(t *testing.T) {
	events := make(chan string, 4)
	n := &probe{start: func(p *probe) {
		tm := p.e.AfterFunc(10*time.Millisecond, func() { events <- "cancelled timer ran" })
		first, second := tm.Stop(), tm.Stop()
		if !first || second {
			events <- "bad Stop results"
		}
		p.e.AfterFunc(40*time.Millisecond, func() { events <- "second" })
	}}
	c := newCluster(t, map[env.NodeID]env.Node{1: n})
	_ = c.Start()
	expect(t, events, "second")
}

func TestInputValidation(t *testing.T) {
	var errs []error
	done := make(chan struct{})
	n := &probe{start: func(p *probe) {
		errs = append(errs,
			p.e.Send(1, make([]byte, env.MaxMessage+1)),
			p.e.Send(42, nil),
			p.e.Append("../escape", nil),
			p.e.Sync("missing"),
		)
		_, err := p.e.ReadFile("missing")
		errs = append(errs, err)
		close(done)
	}}
	c := newCluster(t, map[env.NodeID]env.Node{1: n})
	if err := c.Add(1, n); err == nil {
		t.Fatal("duplicate Add accepted")
	}
	_ = c.Start()
	<-done
	want := []error{env.ErrMessageTooLarge, env.ErrUnknownNode, env.ErrBadName, env.ErrNotFound, env.ErrNotFound}
	for i := range want {
		if !errors.Is(errs[i], want[i]) {
			t.Errorf("check %d: %v, want %v", i, errs[i], want[i])
		}
	}
	if err := c.Add(2, n); err == nil || c.Start() == nil {
		t.Fatal("Add or Start after Start accepted")
	}
	if err := c.Do(9, func() {}); !errors.Is(err, env.ErrUnknownNode) {
		t.Fatalf("Do on unknown node: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal("second Close failed")
	}
	if err := c.Do(1, func() {}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Do after Close: %v", err)
	}
}

func TestDatagramsFromOutsideTheClusterAreDropped(t *testing.T) {
	events := make(chan string, 8)
	recv := func(p *probe, from env.NodeID, b []byte) { events <- string(b) }
	n2 := &probe{recv: recv}
	n1 := &probe{}
	c := newCluster(t, map[env.NodeID]env.Node{1: n1, 2: n2})
	_ = c.Start()
	stranger, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer stranger.Close()
	frame := binary.BigEndian.AppendUint32(nil, 1) // claims to be node 1
	frame = append(frame, "forged"...)
	if _, err := stranger.WriteToUDP(frame, c.nodes[2].addr); err != nil {
		t.Fatal(err)
	}
	// A real member whose header names someone else is dropped too.
	spoof := binary.BigEndian.AppendUint32(nil, 7)
	spoof = append(spoof, "spoofed"...)
	_, _ = c.nodes[1].conn.WriteToUDP(spoof, c.nodes[2].addr)
	_ = c.Do(1, func() { _ = n1.e.Send(2, []byte("genuine")) })
	expect(t, events, "genuine")
	select {
	case got := <-events:
		t.Fatalf("unexpected delivery %q", got)
	case <-time.After(100 * time.Millisecond):
	}
}
