#!/usr/bin/env bash
# End-to-end proof: the correct store survives every seed, the seeded bug is found, its failing
# seed replays to the identical trace, and the same nodes pass on the real runtime.
# Prints one GitHub ::notice per result (::error on failure). Usage: scripts/e2e.sh <dst binary> [seeds]
set -euo pipefail

DST=${1:?usage: scripts/e2e.sh <dst binary> [seeds]}
SEEDS=${2:-1000}
OUT=.tmp/e2e
mkdir -p "$OUT"

fail() { echo "::error title=dst e2e::$1"; exit 1; }

# 1. Correct store: every seed must end quiescent with all invariants holding.
"$DST" sweep -count "$SEEDS" | tee "$OUT/sweep.log" || fail "correct store failed a seed (see $OUT/sweep.log)"
echo "::notice title=dst sweep::correct store: $(head -1 "$OUT/sweep.log")"

# 2. Deliberate bug: the sweep must exit 1 and name failing seeds.
set +e
"$DST" sweep -count "$SEEDS" -bug | tee "$OUT/sweep-bug.log"
code=${PIPESTATUS[0]}
set -e
[ "$code" -eq 1 ] || fail "the arrival-order bug was not found in $SEEDS seeds (exit $code)"
seed=$(sed -n 's/^failing seeds (first [0-9]*): \[\([0-9]*\).*/\1/p' "$OUT/sweep-bug.log")
[ -n "$seed" ] || fail "no failing seed printed"
echo "::notice title=dst bug found::$(head -1 "$OUT/sweep-bug.log"); first failing seed $seed"

# 3. Replay: the failing seed must produce the identical trace and failure three times.
"$DST" replay -bug -seed "$seed" -runs 3 | tee "$OUT/replay.log" || fail "seed $seed did not replay identically"
echo "::notice title=dst replay::$(tail -1 "$OUT/replay.log")"

# 4. Same nodes on UDP loopback, the wall clock and real files.
"$DST" real -dir "$OUT/real" | tee "$OUT/real.log" || fail "real runtime broke an invariant"
echo "::notice title=dst real runtime::$(head -1 "$OUT/real.log")"

echo "E2E PASS"
