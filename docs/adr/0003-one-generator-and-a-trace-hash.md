# ADR 0003: One seeded generator for every choice, and a trace hash as the proof

- **Status:** Accepted

## Context

Several components draw random numbers: the network (drop, duplicate, latency), the scheduler
(order of simultaneous events) and the services (client operations, retry jitter). They could each
own a generator derived from the seed, which keeps one component's draws from shifting another's
when code changes. Or they could share one generator. Separately, "the same seed produces the same
run" needs a check that is cheap, exact and comparable across machines.

## Decision

One PCG generator from `math/rand/v2` (a fixed, documented algorithm, seeded with the run's seed)
supplies every draw: the tie-breaker of every scheduled event, the network's drop, duplicate and
latency draws in that order, and every `env.Rand` call by any node. Every executed event (start,
delivery with payload length and FNV-64a checksum, timer, drop) is written as one text line into a
SHA-256 hash. `dst replay` compares hashes across runs, and a golden test pins the hash of seed 1
so CI on Linux checks the value recorded on Windows.

## Consequences

- The trace hash is a complete record of the run's choices: two runs with the same hash made the
  same decisions in the same order. A divergence shows up as a different hash, without diffing logs.
- Any change to the order of draws (a new `Send` in a handler, an extra event) changes all traces.
  Old seeds stay meaningful only for the code version they were found on. The golden test makes
  such a change explicit in the commit that causes it.
- Adding a fault type in M2 (crashes, partitions) adds draws and shifts every existing trace; the
  golden hash will be updated then, deliberately.
- Per-component generators would make traces more stable across code changes; that matters once
  saved seeds must survive refactors (M3) and can be revisited then.
