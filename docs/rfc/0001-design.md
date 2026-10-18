# RFC 0001: deterministic-sim-testing design

- **Status:** Accepted for M1
- **Author:** EquinoxWN
- **Created:** 2026-10-17

## Problem

Distributed systems fail on rare interleavings: a message delayed past another, a retry that
arrives after the original, a timer that fires between two writes. These bugs show up once in a
long CI run or in production and then cannot be reproduced, because the run depended on the
operating system's scheduler, the wall clock, the network and a random generator, none of which
can be replayed.

Deterministic simulation testing (the approach used by FoundationDB and TigerBeetle) removes that
dependence: the service is written against interfaces for time, network, disk and randomness; a
simulator implements them in one thread, and one seed decides every choice. Any run, including a
failing one, can then be replayed exactly from its seed.

This project builds that framework in Go and proves it on an example primary-backup key-value
store, including a deliberate replication bug that only some message orders expose.

**Users:** engineers who build or test replicated services (consensus, replication, queues,
transactions) and want failures they can replay, and people learning how simulation testing works.

## Define: critical-to-quality measures (CTQs)

| CTQ | Target | Measured by |
|---|---|---|
| Same seed, same run | identical trace hash (SHA-256 over every event) and identical invariant results on every run of a seed | `TestSameSeedSameRun`, `dst replay`, `TestGoldenTrace` |
| Determinism across machines | the seed-1 trace hash of the default workload is the same on Windows and Linux | golden hash test locally and in CI |
| Different seeds explore different runs | 20 seeds give 20 different traces | `TestDifferentSeedsDifferentRuns` |
| Services use only the interfaces | the example service imports no `time.Now`, `math/rand`, `os`, `net`, `sync` or goroutines | `dst lint example/kv` in `make lint` |
| Hidden nondeterminism is exposed | a node that sends in map iteration order gives more than one trace over 10 runs | `TestTraceHashExposesMapOrderNondeterminism` |
| Interfaces really swap | the unchanged example service passes the same invariants on UDP loopback, the wall clock and real files | `TestSameNodesOnTheRealRuntime`, `dst real` |
| Correct store holds | 1,000 seeds with 5% message loss and 2% duplication: 0 invariant failures | `dst sweep -count 1000` |
| Bugs are found and replayable | the seeded arrival-order bug fails on at least one seed in 100, and its failing seed replays to the same trace and failure | `TestSeedSweepFindsTheBugAndItReplaysExactly`, `dst sweep -bug`, `dst replay` |
| Fast enough for sweeps | 1,000 seeds of the default workload in under 10 s on a laptop | `dst sweep` wall time |

## Goals (M1)

- Services depend only on interfaces for time, network, disk and randomness, which the framework
  swaps for simulated versions.
- One seeded random generator drives all scheduling and faults, so the same seed always produces
  the same run.

## Non-goals

- Crashes, restarts, partitions, torn writes and clock skew (M2). The M1 network already loses,
  duplicates and delays messages, because those are scheduling choices of the network model.
- Saving failing seeds automatically and shrinking a failing run to a minimal case (M3).
- Testing multi-raft-kv and percolator-txn (M3).
- Running unmodified programs: services must be written against `env` (or adapted to it).
- Running as a hosted production service.

## Design

```
            +-------------------- env (interfaces) --------------------+
            |  Clock: Now, AfterFunc   Network: Send   Disk: Append,    |
            |  Sync, ReadFile          Rand: Uint64, IntN, Float64      |
            +---------------+--------------------------+---------------+
                            |                          |
          sim (one goroutine, one seed)        realenv (one goroutine per node)
          virtual clock, event heap,           wall clock, UDP on 127.0.0.1,
          network model, in-memory disk,       files under one directory,
          PCG generator, trace hash            ChaCha8 seeded from crypto/rand
                            \                          /
                             example/kv: primary-backup store + clients
                             internal/scenario: workload, invariants, sweeps
                             cmd/dst: run, sweep, replay, real, lint
```

- **env**: the only way a node touches the world. A node implements `Start(env.Env)` and
  `Receive(from, payload)`; timers call back into it. Every runtime runs a node's callbacks one at a
  time, so services need no locks. File names are restricted to `[a-z0-9._-]`, messages to 64 KiB.
- **sim**: a discrete-event simulator. Events (node start, message delivery, timer) sit in a heap
  ordered by virtual time, then a random tie-breaker drawn from the seeded generator, then
  insertion order. Running an event moves the virtual clock to its time, so idle time costs
  nothing. `Send` draws, in order, whether the message is dropped, whether it is duplicated and a
  latency for each copy. Every value a node draws from `env.Rand` comes from the same PCG generator
  (`math/rand/v2`, a fixed algorithm). Each event is written as one line into a SHA-256 trace hash
  (and optionally a trace file). Runs end when the queue is empty, at an event or virtual-time
  limit, on `Halt`, or when a node panics (the panic is reported with the seed).
- **realenv**: the same interfaces on the real world, to prove the swap. Each node has a UDP socket
  on 127.0.0.1, a directory, a ChaCha8 generator seeded from `crypto/rand`, and one goroutine that
  runs its callbacks; datagrams from outside the cluster or with a forged sender are dropped.
- **dstlint**: an AST check that a service package imports nothing nondeterministic (`math/rand`,
  `crypto/rand`, `os`, `net`, `sync`, `runtime`, ...), calls no wall-clock function and starts no
  goroutines. It cannot see map iteration order; the trace hash catches that at run time.
- **example/kv**: node 1 is the primary; it orders writes, logs them to its write-ahead log,
  replicates them to every backup and answers the client after all backups acknowledge. Backups
  apply in sequence order, buffering gaps, and acknowledge the highest contiguous sequence. The
  primary resends unacknowledged writes; clients retry with jittered timeouts; the primary
  deduplicates retries per client. `ArrivalOrderBug` makes backups apply writes in arrival order
  and acknowledge the highest sequence seen.
- **scenario**: three clients each run 40 random reads and writes on their own four keys over a
  primary and two backups. Invariants: every operation completes; every read returns the client's
  last acknowledged write; the primary committed its whole log; every backup's state equals the
  primary's; every node's write-ahead log replays to its in-memory state.

## Alternatives considered

| Option | Why not (yet) |
|---|---|
| Control Go's real scheduler (run goroutines under a deterministic scheduler, as some research tools patch the runtime) | Needs runtime patches or `GOMAXPROCS=1` plus hooks, and still leaks nondeterminism through `select`, map order and timers. A single-threaded event loop over explicit interfaces is simple and complete (ADR 0002). |
| Record and replay a real run (log every nondeterministic input) | Reproduces a failure only after it happened once and was recorded, and cannot explore new orders. A seed both explores and replays. |
| Jepsen-style testing on real nodes with fault injection | Finds real bugs on real systems, but runs take minutes, failures are not replayable, and it needs several machines or containers. Useful later against the real runtime. |
| Separate generators per node or per subsystem | Keeps one node's draws from shifting another's, but the backlog asks for one generator, and a single stream makes the trace the full record of every choice (ADR 0003). |
| Model checking (TLA+, exhaustive state search) | Proves properties of a model, not of the Go code; the state space of this store with retries is large. Complementary, not a replacement. |

## Measurement plan

- `go test ./...` for the unit and property tests; the golden trace hash runs on Windows locally
  and on Linux in CI.
- `make bench` (`scripts/e2e.sh`) on the author's laptop: `dst sweep -count 1000` on the correct
  store and on the seeded bug, `dst replay` of the first failing seed, and `dst real`. Record seeds
  failing, failure kinds, events, virtual time and wall time.
- Results, root causes of defects and the controls that hold them go into `docs/results/m1.md`
  (Measure, Analyze, Improve, Control). CI reruns the sweeps on every push.

## Milestones

- **M1:** Services depend only on interfaces for time, network, disk and randomness, which the framework swaps for simulated versions. One seeded random generator drives all scheduling and faults, so the same seed always produces the same run.
- **M2:** The virtual clock jumps ahead, simulating hours of cluster time in seconds. Crashes, partitions, torn writes and clock skew are injected according to the seed.
- **M3:** When an invariant fails, the seed is saved; the run replays exactly and is shrunk to a minimal failing case. It tests multi-raft-kv and percolator-txn, and the README lists the real bugs it found.

## Risks and open questions

- Determinism holds only while services keep to `env`. The lint catches imports, wall-clock calls
  and goroutines; map iteration order is caught only when it changes the trace, so the golden hash
  and replay checks stay in CI.
- A change to the order of random draws in the simulator changes every trace; old seeds then
  replay a different run. Seeds are tied to a code version (the golden test makes such changes
  explicit).
- The simulated network and disk are models: they find bugs in the protocol, not in the kernel,
  the network stack or the file system.
- The example store has a fixed primary; failover is out of scope until crashes exist (M2).
