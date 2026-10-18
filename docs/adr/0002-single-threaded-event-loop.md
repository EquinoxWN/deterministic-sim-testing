# ADR 0002: Simulate with a single-threaded event loop over explicit interfaces

- **Status:** Accepted

## Context

A run is reproducible only if every source of nondeterminism is under the seed's control. In Go,
those sources are the goroutine scheduler, `select` (which picks a ready case at random), map
iteration order, the wall clock, timers, the network, the file system and random generators.
There are two ways to control them: intercept them inside an ordinary concurrent program (patch or
wrap the runtime, force `GOMAXPROCS=1`, hook timers), or make services reach them only through
interfaces that a single-threaded simulator implements.

## Decision

Services are event handlers (`Start`, `Receive`, timer callbacks) and touch the world only through
`env.Env`. The simulator runs every callback of every node on one goroutine, in the order of a
heap keyed by virtual time and a seeded tie-breaker. `dstlint` rejects service code that imports
`os`, `net`, `sync`, `runtime` or random packages, calls wall-clock functions, or starts
goroutines. A real runtime (`realenv`) implements the same interfaces with one goroutine per
node, so the service code is identical in both.

## Consequences

- Determinism does not depend on Go runtime internals or on `GOMAXPROCS`; the simulator
  (`sim`, about 460 lines) uses only the Go standard library.
- Idle time is free: the clock jumps to the next event, so the default workload covers 39 minutes
  of virtual time in about 2 seconds of wall time over 1,000 seeds.
- Services must be written in an event-driven style. Existing code that blocks on channels or
  calls `time.Sleep` has to be adapted before it can be simulated; M3 has to adapt multi-raft-kv
  and percolator-txn this way.
- One CPU core per simulation. Seed sweeps can run in parallel processes later; M1 keeps them
  sequential so test machines are not overloaded.
- Map iteration order inside a handler is still nondeterministic and invisible to the lint; the
  trace hash and replay checks catch it when it changes behaviour.
