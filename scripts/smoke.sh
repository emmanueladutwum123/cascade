#!/usr/bin/env bash
#
# End-to-end smoke test: start a venue, start the plant, connect a subscriber, and
# assert the plant actually delivered market data and recovered every gap.
#
# Uses a unicast UDP address rather than a multicast group. Multicast is the production
# transport and what the plant is designed around, but CI containers frequently have no
# multicast route on loopback, and a smoke test that fails for reasons unrelated to the
# code under test is worse than no smoke test. The receive path is identical either way.

set -euo pipefail

BUILD="${1:-build}"
FEED_ADDR="${FEED_ADDR:-127.0.0.1}"
FEED_PORT="${FEED_PORT:-31337}"
RECOVERY_PORT="${RECOVERY_PORT:-31338}"
PLANT_PORT="${PLANT_PORT:-31400}"
INSTRUMENTS="${INSTRUMENTS:-50}"
RATE="${RATE:-40000}"
LOSS="${LOSS:-0.003}"
RUNTIME="${RUNTIME:-12}"

WORK="$(mktemp -d)"
FEEDSIM_PID=""
PLANT_PID=""

cleanup() {
  [ -n "$FEEDSIM_PID" ] && kill "$FEEDSIM_PID" 2>/dev/null || true
  [ -n "$PLANT_PID" ] && kill "$PLANT_PID" 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT

fail() {
  echo "SMOKE FAILED: $*" >&2
  echo "--- feedsim ---" >&2; tail -20 "$WORK/feedsim.log" >&2 || true
  echo "--- plant ---" >&2;  tail -30 "$WORK/plant.log" >&2 || true
  echo "--- subscriber ---" >&2; cat "$WORK/sub.log" >&2 || true
  exit 1
}

echo "== starting venue ($INSTRUMENTS instruments, ${RATE}/s, ${LOSS} packet loss)"
"$BUILD/apps/cascade-feedsim" \
  --group "$FEED_ADDR" --port "$FEED_PORT" --recovery-port "$RECOVERY_PORT" \
  --interface 127.0.0.1 --instruments "$INSTRUMENTS" --rate "$RATE" \
  --loss "$LOSS" --seconds "$RUNTIME" > "$WORK/feedsim.log" 2>&1 &
FEEDSIM_PID=$!
sleep 1

echo "== starting plant"
"$BUILD/apps/cascaded" \
  --channel "$FEED_ADDR:$FEED_PORT:127.0.0.1:$RECOVERY_PORT" \
  --interface 127.0.0.1 --port "$PLANT_PORT" \
  --instruments "$INSTRUMENTS" --fanout 2 --report 3 > "$WORK/plant.log" 2>&1 &
PLANT_PID=$!
sleep 2

kill -0 "$FEEDSIM_PID" 2>/dev/null || fail "venue exited early"
kill -0 "$PLANT_PID" 2>/dev/null || fail "plant exited early"

echo "== connecting subscriber"
"$BUILD/apps/cascade-sub" \
  --host 127.0.0.1 --port "$PLANT_PORT" \
  --first "$INSTRUMENTS" --seconds 6 > "$WORK/sub.log" 2>&1 || fail "subscriber exited non-zero"

cat "$WORK/sub.log"

# --- assertions ------------------------------------------------------------

grep -q "logged in" "$WORK/sub.log" || fail "subscriber never logged in"

GRANTED=$(sed -n 's/.*subscriptions   : \([0-9]*\) granted.*/\1/p' "$WORK/sub.log")
[ "${GRANTED:-0}" -eq "$INSTRUMENTS" ] || fail "expected $INSTRUMENTS subscriptions, got ${GRANTED:-0}"

UPDATES=$(sed -n 's/.*book updates    : \([0-9]*\) .*/\1/p' "$WORK/sub.log")
[ "${UPDATES:-0}" -gt 100 ] || fail "expected book updates to flow, got ${UPDATES:-0}"

TRADES=$(sed -n 's/.*trades          : \([0-9]*\)$/\1/p' "$WORK/sub.log")
[ "${TRADES:-0}" -gt 0 ] || fail "expected trade prints, got ${TRADES:-0}"

# The venue is dropping packets on purpose. Recovery has to have run, and it has to
# have worked: a plant that quietly loses market data is the failure this whole
# component exists to prevent.
GAPS=$(grep -o 'gaps=[0-9]*' "$WORK/plant.log" | tail -1 | cut -d= -f2)
[ "${GAPS:-0}" -gt 0 ] || fail "injected packet loss produced no gaps -- recovery path untested"

LOST=$(grep -o 'lost [0-9]*' "$WORK/plant.log" | tail -1 | cut -d' ' -f2)
[ "${LOST:-1}" -eq 0 ] || fail "$LOST messages were lost unrecoverably"

grep -q "marking every book on this channel stale" "$WORK/plant.log" \
  && fail "books were declared stale: recovery did not keep up"

echo
echo "SMOKE PASSED"
echo "  subscriptions : $GRANTED"
echo "  book updates  : $UPDATES"
echo "  trades        : $TRADES"
echo "  feed gaps     : $GAPS (all recovered, 0 messages lost)"
