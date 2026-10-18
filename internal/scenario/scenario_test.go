package scenario

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/EquinoxWN/deterministic-sim-testing/sim"
)

// goldenSeed1 is the trace hash of seed 1 with the default workload. It pins determinism across
// runs, operating systems and Go releases: CI recomputes it on Linux.
const goldenSeed1 = "12f98fd4f16c55549bc08e267ca90b6983019aff0a9528692c2d388dc4234473"

func TestCorrectStoreHoldsOnEverySeed(t *testing.T) {
	r, err := Sweep(1, 300, Default())
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Failed) != 0 {
		t.Fatalf("%d of %d seeds failed, first %v: %s", len(r.Failed), r.Runs, r.Failed[0], r.FirstFailure)
	}
}

func TestSeedSweepFindsTheBugAndItReplaysExactly(t *testing.T) {
	o := Default()
	o.Bug = true
	r, err := Sweep(1, 100, o)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Failed) == 0 {
		t.Fatal("100 seeds did not find the arrival-order bug")
	}
	if !strings.Contains(r.FirstFailure, "diverged from the primary") {
		t.Fatalf("unexpected first failure: %s", r.FirstFailure)
	}
	total := 0
	for _, n := range r.Kinds {
		total += n
	}
	if total != len(r.Failed) || Kind(`backup 2: k1 = "v1-3", want "v1-9"`) != "backup N: kN = V, want V" {
		t.Fatalf("kinds %v do not cover %d failures", r.Kinds, len(r.Failed))
	}
	outs, err := Replay(r.Failed[0], o, 3)
	if err != nil {
		t.Fatal(err)
	}
	for _, out := range outs[1:] {
		if !reflect.DeepEqual(out, outs[0]) {
			t.Fatalf("seed %d replayed differently:\n%+v\n%+v", r.Failed[0], outs[0], out)
		}
	}
	if outs[0].OK() || outs[0].Failures[0] != r.FirstFailure {
		t.Fatalf("replay did not reproduce the failure: %v", outs[0].Failures)
	}
}

func TestGoldenTrace(t *testing.T) {
	var buf bytes.Buffer
	o := Default()
	o.Trace = &buf
	out, err := Run(1, o)
	if err != nil {
		t.Fatal(err)
	}
	if out.TraceHash != goldenSeed1 {
		t.Fatalf("seed 1 trace hash %s, want %s: determinism changed", out.TraceHash, goldenSeed1)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if !strings.HasPrefix(lines[0], "0 start n") || len(lines) != out.Events+out.Dropped {
		t.Fatalf("trace has %d lines for %d events and %d drops; first %q", len(lines), out.Events, out.Dropped, lines[0])
	}
}

func TestPerfectNetworkNeedsNoRetries(t *testing.T) {
	o := Default()
	o.Net = sim.NetConfig{MinLatency: time.Millisecond, MaxLatency: 20 * time.Millisecond}
	out, err := Run(9, o)
	if err != nil || !out.OK() || out.Retries != 0 || out.Dropped != 0 {
		t.Fatalf("out %+v err %v", out, err)
	}
}

func TestLossyNetworkForcesRetriesButStaysCorrect(t *testing.T) {
	o := Default()
	o.Net.DropRate, o.Net.DupRate = 0.3, 0.2
	out, err := Run(4, o)
	if err != nil || !out.OK() || out.Retries == 0 || out.Duplicated == 0 {
		t.Fatalf("out %+v err %v", out, err)
	}
}

func TestOptionsAreBounded(t *testing.T) {
	for _, mod := range []func(*Options){
		func(o *Options) { o.Backups = 9 },
		func(o *Options) { o.Clients = 0 },
		func(o *Options) { o.Ops = 10_001 },
		func(o *Options) { o.Keys = 0 },
		func(o *Options) { o.ClientTimeout = 0 },
		func(o *Options) { o.Net.DropRate = 2 },
	} {
		o := Default()
		mod(&o)
		if _, err := Run(1, o); err == nil {
			t.Errorf("options %+v accepted", o)
		}
	}
}

func TestSameNodesOnTheRealRuntime(t *testing.T) {
	out, err := RunReal(t.TempDir(), Default(), 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Failures) != 0 {
		t.Fatalf("real runtime: %v", out.Failures)
	}
}
