// Command dst runs the example key-value store in the deterministic simulator, sweeps seeds,
// replays a seed, runs the same nodes on the real runtime, and lints service packages.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/EquinoxWN/deterministic-sim-testing/dstlint"
	"github.com/EquinoxWN/deterministic-sim-testing/internal/scenario"
)

const usage = `usage: dst <command> [flags]

commands:
  run     simulate one seed and check the invariants
  sweep   simulate a range of seeds and list the failing ones
  replay  simulate one seed several times and compare the trace hashes
  real    run the same nodes over UDP loopback, the wall clock and files
  lint    check that service packages use only env for time, network, disk and randomness

run "dst <command> -h" for the flags of a command`

// flagOutput receives flag usage and parse errors.
var flagOutput io.Writer = os.Stderr

// main dispatches to a subcommand and maps errors to exit codes.
func main() {
	code, err := run(os.Args[1:], os.Stdout)
	if err != nil {
		fmt.Fprintln(os.Stderr, "dst:", err)
	}
	os.Exit(code)
}

// run executes one subcommand; exit 1 means invariants or lint failed, 2 means bad usage or an error.
func run(args []string, out io.Writer) (int, error) {
	if len(args) == 0 {
		return 2, errors.New(usage)
	}
	cmd, rest := args[0], args[1:]
	switch cmd {
	case "run":
		return cmdRun(rest, out)
	case "sweep":
		return cmdSweep(rest, out)
	case "replay":
		return cmdReplay(rest, out)
	case "real":
		return cmdReal(rest, out)
	case "lint":
		return cmdLint(rest, out)
	}
	return 2, fmt.Errorf("unknown command %q\n%s", cmd, usage)
}

// workloadFlags registers the shared workload flags.
func workloadFlags(fs *flag.FlagSet) *scenario.Options {
	o := scenario.Default()
	fs.IntVar(&o.Backups, "backups", o.Backups, "backup replicas (0..8)")
	fs.IntVar(&o.Clients, "clients", o.Clients, "clients (1..64)")
	fs.IntVar(&o.Ops, "ops", o.Ops, "operations per client (1..10000)")
	fs.IntVar(&o.Keys, "keys", o.Keys, "keys per client (1..1000)")
	fs.Float64Var(&o.Net.DropRate, "drop", o.Net.DropRate, "message drop probability")
	fs.Float64Var(&o.Net.DupRate, "dup", o.Net.DupRate, "message duplication probability")
	fs.DurationVar(&o.Net.MinLatency, "min-latency", o.Net.MinLatency, "shortest one-way delay")
	fs.DurationVar(&o.Net.MaxLatency, "max-latency", o.Net.MaxLatency, "longest one-way delay")
	fs.BoolVar(&o.Bug, "bug", false, "backups apply writes in arrival order (deliberate bug)")
	return &o
}

// parse parses flags and rejects stray arguments.
func parse(fs *flag.FlagSet, args []string) error {
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 && fs.Name() != "lint" {
		return fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	return nil
}

// cmdRun simulates one seed, optionally writing the event trace.
func cmdRun(args []string, out io.Writer) (int, error) {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(flagOutput)
	o := workloadFlags(fs)
	seed := fs.Uint64("seed", 1, "seed")
	trace := fs.String("trace", "", "write every event to this file")
	if err := parse(fs, args); err != nil {
		return 2, err
	}
	var tw *bufio.Writer
	if *trace != "" {
		f, err := os.Create(filepath.Clean(*trace))
		if err != nil {
			return 2, err
		}
		defer f.Close()
		tw = bufio.NewWriter(f)
		defer tw.Flush()
		o.Trace = tw
	}
	res, err := scenario.Run(*seed, *o)
	if err != nil {
		return 2, err
	}
	fmt.Fprintf(out, "seed=%d stop=%s events=%d sent=%d dropped=%d duplicated=%d retries=%d virtual=%s\n",
		res.Seed, res.Stop, res.Events, res.Sent, res.Dropped, res.Duplicated, res.Retries, res.VirtualTime)
	fmt.Fprintf(out, "trace=%s\n", res.TraceHash)
	return verdict(out, res.Failures)
}

// cmdSweep simulates a range of seeds.
func cmdSweep(args []string, out io.Writer) (int, error) {
	fs := flag.NewFlagSet("sweep", flag.ContinueOnError)
	fs.SetOutput(flagOutput)
	o := workloadFlags(fs)
	from := fs.Uint64("from", 1, "first seed")
	count := fs.Int("count", 1000, "number of seeds (1..100000)")
	if err := parse(fs, args); err != nil {
		return 2, err
	}
	if *count < 1 || *count > 100_000 {
		return 2, errors.New("count must be 1..100000")
	}
	r, err := scenario.Sweep(*from, *count, *o)
	if err != nil {
		return 2, err
	}
	ratio := float64(r.VirtualTime) / float64(max(r.Wall, time.Nanosecond))
	fmt.Fprintf(out, "seeds=%d..%d runs=%d failed=%d events=%d virtual=%s wall=%s virtual/wall=%.0fx events/s=%.0f\n",
		*from, *from+uint64(*count)-1, r.Runs, len(r.Failed), r.Events, r.VirtualTime.Round(time.Millisecond),
		r.Wall.Round(time.Millisecond), ratio, float64(r.Events)/r.Wall.Seconds())
	if len(r.Failed) == 0 {
		fmt.Fprintln(out, "RESULT PASS: every seed ended quiescent with all invariants holding")
		return 0, nil
	}
	shown := r.Failed[:min(len(r.Failed), 10)]
	fmt.Fprintf(out, "failing seeds (first %d): %v\nfirst failure: %s\n", len(shown), shown, r.FirstFailure)
	for _, k := range slices.Sorted(maps.Keys(r.Kinds)) {
		fmt.Fprintf(out, "kind: %4d x %s\n", r.Kinds[k], k)
	}
	fmt.Fprintf(out, "RESULT FAIL: %d of %d seeds broke an invariant\n", len(r.Failed), r.Runs)
	return 1, nil
}

// cmdReplay runs one seed several times and compares the traces.
func cmdReplay(args []string, out io.Writer) (int, error) {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	fs.SetOutput(flagOutput)
	o := workloadFlags(fs)
	seed := fs.Uint64("seed", 1, "seed")
	runs := fs.Int("runs", 3, "runs (2..100)")
	if err := parse(fs, args); err != nil {
		return 2, err
	}
	if *runs < 2 || *runs > 100 {
		return 2, errors.New("runs must be 2..100")
	}
	outs, err := scenario.Replay(*seed, *o, *runs)
	if err != nil {
		return 2, err
	}
	same := true
	for i, r := range outs {
		first := "ok"
		if len(r.Failures) > 0 {
			first = r.Failures[0]
		}
		fmt.Fprintf(out, "run %d: events=%d trace=%s invariants: %s\n", i+1, r.Events, r.TraceHash, first)
		same = same && r.TraceHash == outs[0].TraceHash && strings.Join(r.Failures, "\n") == strings.Join(outs[0].Failures, "\n")
	}
	if !same {
		fmt.Fprintln(out, "RESULT FAIL: the same seed produced different runs")
		return 1, nil
	}
	fmt.Fprintf(out, "RESULT PASS: %d runs of seed %d are identical\n", *runs, *seed)
	return 0, nil
}

// cmdReal runs the workload on the real runtime.
func cmdReal(args []string, out io.Writer) (int, error) {
	fs := flag.NewFlagSet("real", flag.ContinueOnError)
	fs.SetOutput(flagOutput)
	o := workloadFlags(fs)
	dir := fs.String("dir", ".tmp/real", "directory for node files (emptied first)")
	timeout := fs.Duration("timeout", 30*time.Second, "give up after this long")
	if err := parse(fs, args); err != nil {
		return 2, err
	}
	if o.Net != scenario.Default().Net {
		return 2, errors.New("network flags apply to the simulator only")
	}
	root := filepath.Clean(*dir)
	if err := os.RemoveAll(root); err != nil {
		return 2, err
	}
	r, err := scenario.RunReal(root, *o, *timeout)
	if err != nil {
		return 2, err
	}
	fmt.Fprintf(out, "real runtime: %d operations over UDP loopback in %s, %d client retries\n",
		r.Ops, r.Wall.Round(time.Millisecond), r.Retries)
	return verdict(out, r.Failures)
}

// cmdLint checks service package directories.
func cmdLint(args []string, out io.Writer) (int, error) {
	fs := flag.NewFlagSet("lint", flag.ContinueOnError)
	fs.SetOutput(flagOutput)
	if err := parse(fs, args); err != nil {
		return 2, err
	}
	if fs.NArg() == 0 {
		return 2, errors.New("lint needs at least one package directory")
	}
	found := 0
	for _, dir := range fs.Args() {
		fsd, err := dstlint.Check(dir)
		if err != nil {
			return 2, err
		}
		for _, f := range fsd {
			fmt.Fprintln(out, f)
		}
		found += len(fsd)
	}
	if found > 0 {
		fmt.Fprintf(out, "dstlint: %d finding(s)\n", found)
		return 1, nil
	}
	fmt.Fprintf(out, "dstlint: %s clean\n", strings.Join(fs.Args(), ", "))
	return 0, nil
}

// verdict prints the invariant failures and the exit code that goes with them.
func verdict(out io.Writer, fails []string) (int, error) {
	if len(fails) == 0 {
		fmt.Fprintln(out, "RESULT PASS: all invariants hold")
		return 0, nil
	}
	for _, f := range fails {
		fmt.Fprintln(out, "violation:", f)
	}
	fmt.Fprintf(out, "RESULT FAIL: %d invariant violation(s)\n", len(fails))
	return 1, nil
}
