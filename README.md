# cascade

[![ci](https://github.com/emmanueladutwum123/cascade/actions/workflows/ci.yml/badge.svg)](https://github.com/emmanueladutwum123/cascade/actions/workflows/ci.yml)
[![license](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![C++20](https://img.shields.io/badge/C%2B%2B-20-00599C.svg)](https://en.cppreference.com/w/cpp/20)

A low-latency market data ticker plant in C++20, with zero third-party dependencies.

It consumes an unreliable UDP multicast exchange feed, recovers lost packets, builds
order books from order-by-order events, and fans conflated state out to thousands of TCP
subscribers — without a single lock on the market-data path.

```
exchange ──multicast──▶ feed handler ──▶ book shard ──▶ fan-out ──TCP──▶ subscribers
                        gap recovery     lock-free      conflation
                                         order books    entitlements
                                                        eviction
```

---

## Why this is harder than routing messages

Four properties of exchange market data make it a genuinely difficult systems problem:

- **The feed is unreliable.** Venues use UDP multicast so one copy serves the whole
  market. There are no delivery guarantees, no ordering, and no way to ask the exchange
  to slow down. Every receiver handles loss, reordering and duplication on its own.
- **The book is a fold over the entire message stream.** The feed publishes order
  lifecycle events; the book is a projection you maintain. One lost message corrupts it
  permanently and *silently*.
- **Rates are high and tolerances are small.** Millions of messages per second at the
  open, over 90% of them cancels. A few milliseconds behind means trading on prices that
  no longer exist.
- **Some consumers are slow, and must not slow anyone else down.** One copy of the data
  has to serve a pricing engine, a risk system and a screen on a congested VPN.

Full reasoning for every design decision, including the ones that were rejected, is in
[`docs/architecture.md`](docs/architecture.md).

---

## Measured

Apple M2, 8 cores, Release (`-O3 -mcpu=native`). Full output:
[`docs/benchmark-output.txt`](docs/benchmark-output.txt).

### Primitives

| | |
|---|---|
| Message decode (big-endian wire → normalised event) | **1.28 ns** |
| Seqlock publish (368-byte book image) | **4.79 ns** |
| Seqlock read (uncontended) | **5.18 ns** |
| SPSC ring push + pop round trip | **6.80 ns** |
| Order map lookup, 500k live orders | **7.13 ns** |
| Order map erase + insert at steady occupancy | **34.36 ns** |
| Price ladder add + remove near the touch (200 levels deep) | **23.05 ns** |

### Pipeline

| Stage | Throughput |
|---|---|
| Decode | **115 M msg/s** |
| Decode + book build | **4.5 M msg/s** |
| Decode + book build + fan-out to 50 subscribers | **3.3 M msg/s** |

### Wire-to-wire latency

From a feed message being decoded off the wire to the resulting book state being framed
for a subscriber, at **250k msg/s** offered load, 500 instruments, 50 subscribers ×
100 subscriptions:

| p50 | p90 | p99 | p99.9 | max |
|---|---|---|---|---|
| **1.54 µs** | **2.04 µs** | **11.4 µs** | 1.66 ms | 7.81 ms |

### Where the work goes

Over 2M feed events:

```
book changes visible at published depth      410,219
book changes below depth                   1,589,781   79.5% stopped at the shard
updates delivered across 50 subscribers    3,108,873
states conflated away                        752,449
```

**Four fifths of all book changes never leave the shard.** Most order activity happens
below the published depth and is invisible to every subscriber; filtering it there saves
a seqlock write, a fan-out wake-up and a socket write per subscriber, each time.

### How these were measured

A benchmark that confidently measures the wrong thing is worse than none, so:

- **Latency is measured at a stated load, not at saturation.** Pushing a system as hard
  as it will go and timing the result measures queue depth, not latency.
- **Each message is timed from when it was *due*, not when it was sent.** Otherwise a
  producer that falls behind silently discards exactly the delayed samples — the
  coordinated-omission trap that makes a struggling system look healthy.
- **Measured at the exit**, by parsing the frames the plant actually wrote and
  differencing the ingest timestamp they carry. No instrumentation in the hot path.
- **Clock resolution is printed alongside the numbers.** `CLOCK_MONOTONIC` on macOS is
  quantised to 1 µs — coarser than the entire budget being measured — so timestamps come
  from the hardware counter instead (41.7 ns floor here).

The p99.9 and max are noisy run to run: this is an 8-core laptop with no core isolation,
no IRQ affinity and a desktop running. p50 and p90 are stable. Treat the tail as an upper
bound on a hostile machine, not as what the design achieves.

---

## Build and run

Needs CMake ≥ 3.20 and a C++20 compiler. Nothing else.

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Run the whole system — a venue publishing with deliberate packet loss, the plant
recovering from it, and a subscriber:

```sh
./scripts/smoke.sh build
```

Or drive it by hand, in three terminals:

```sh
# 1. the exchange: 100 instruments at 80k msg/s, dropping 0.2% of datagrams
./build/apps/cascade-feedsim --instruments 100 --rate 80000 --loss 0.002

# 2. the plant
./build/apps/cascaded --instruments 100 --fanout 2

# 3. a subscriber, printing top of book
./build/apps/cascade-sub --first 5 --print
```

```
[sub] logged in: session=1 venues=0x00000001 max_subs=100
  AAAA      11.0106 x1000    |    10.8820 x600      v59565
  AAAB       9.0071 x300     |     8.8920 x1200     v37419
  AAAC      12.2796 x800     |    12.1570 x1800     v20914
  TRADE AAAA      11.2546 x200
```

Things worth trying:

```sh
# Permissioning: authenticated, but not licensed for this venue.
./build/apps/cascade-sub --token cascade-unentitled --first 3
#   AAAA: NOT ENTITLED   (0 granted, 3 refused, no data leaked)

# Slow consumer: stop reading the socket for 8s and watch the plant cope, then give up.
./build/apps/cascade-sub --first 100 --stall 8 --seconds 25
#   [sub] EVICTED: trade queue overrun (stalled 1539.7 ms)
#   [sub]   states conflated: 6168 (largest single gap 32)
```

That last one is the design in miniature: **6,168 book states were collapsed** because a
quote is a state and only the latest one matters — and then the client was disconnected
for overrunning the *trade* queue, because a trade is an event and collapsing two prints
would fabricate a tape that never happened.

Benchmarks:

```sh
./build/apps/cascade-bench                  # everything
./build/apps/cascade-bench --micro-only     # primitives
./build/apps/cascade-bench --rate 500000 --fanout 2
```

---

## Design, briefly

### A shard is a feed channel

An order-by-order feed identifies orders by id: a Delete carries an order id and nothing
else. To apply it you must be the thread already holding that order — so the Add and the
Delete have to land on the same shard, and no hash of an order id can guarantee that.

Venues solve this by partitioning multicast groups by symbol range. cascade mirrors that:
**one feed handler and one book shard per channel**, an order's whole lifecycle on one
thread. Which means the order map, the ladders and the level vectors need no locks, no
atomics and no defensive copying — the hottest code in the system is ordinary
single-threaded C++.

### Conflation falls out of the data structure

Shards tell fan-out threads which instruments moved through a **hierarchical atomic
bitmap**, not a queue. A queue must be sized for the worst burst and has no good answer
when it overflows — dropping loses an update, blocking puts the market-data path behind
the slowest consumer.

A bitmap has neither problem, because setting a bit twice is setting it once. A thousand
updates to one instrument while the fan-out is busy collapse to a single set bit, which is
exactly right: a subscriber wants the instrument's *current* state, not a replay of
history. Overflow isn't handled — it's impossible.

### Quotes conflate; trades do not

A quote is a **state**: two updates to the same book collapse into the later one with
nothing lost. So a backed-up subscriber gets a *flag* saying the instrument is owed, and
the next pass re-reads current state. Memory is bounded by subscription count, not by how
far behind the client is.

A trade is an **event**: two prints are two facts. Collapsing them would invent a tape
that never happened. So trades are queued, bounded, and a client that overruns is
disconnected — because a subscriber holding a silently incomplete tape computes wrong
volumes and never knows.

Every update carries `conflated_count`, derived from the version gap so it is exact. A
conflated feed that hides its own conflation makes a quiet market indistinguishable from
a saturated link.

### Losing data is reported, never hidden

When a gap can't be recovered, there is no way to build a correct book across a hole in
an order-by-order stream. The plant discards the affected books, flags them stale, and
says so immediately. **A plant that keeps publishing a book it knows is wrong is worse
than one that goes dark**, because downstream systems will trade on it.

### Eviction is timed, not sized

A momentarily full socket is normal — a scheduler blip, the open. Evicting on depth would
disconnect healthy clients every morning. A socket that *stays* full means the client is
structurally too slow, and waiting will not fix it. The notice names the reason, because
"too slow" and "idle" demand different fixes and a bare TCP reset leaves an operator
guessing.

---

## Two bugs the live system found

Both had passed every unit test and were only visible running the real thing.

**Gaps were tracked against the newest packet seen, not the earliest one held.** Packets
keep arriving during a gap, so the hole appeared to grow to span all of them — retransmit
requests asked for hundreds of messages already sitting in the reorder window. In a live
run: 42,000 retransmits served for ~1,500 genuinely missing messages.

**The reorder window was sized for network reordering rather than recovery latency.**
While a gap is open, *every* arriving packet is ahead of us and must be held. At 10k
packets/sec a 250 ms recovery deadline means 2,500 packets arrive before the outcome is
known; a 64-packet window overflowed 6 ms in, and each packet dropped from it became a
second hole that recovery was never asked to fill. One lost datagram cascaded into 20,000
lost messages.

Same workload, after:

| | before | after |
|---|---|---|
| gaps detected | 274 | 226 |
| gaps recovered | 272 | **226** |
| messages lost | **39,904** | **0** |
| duplicate packets | 5,750 | 14 |

Both are now pinned by tests (`a_retransmit_asks_only_for_the_hole`,
`a_full_reorder_window_abandons_rather_than_cascading`).

---

## Testing

**117 cases**, no third-party framework — zero dependencies keeps the build reproducible
anywhere a C++20 compiler exists.

The ones that matter:

- `differential_against_unordered_map` — 300k randomised ops checked against
  `std::unordered_map` step by step
- `deep_book_stays_sorted_under_random_traffic` — 20k ladder ops against `std::map`
- `concurrent_producer_consumer_preserves_every_message` — 2M messages through the SPSC
  ring, exactly once and in order
- `readers_never_observe_a_torn_quote` — 4 readers against 400k writes, checksum-verified
- `concurrent_marks_are_never_lost` — 3 producers racing a drain on the dirty bitmap
- `churn_does_not_degrade_probe_chains` — 200k insert/erase at constant occupancy, the
  property backward-shift deletion exists to hold
- `snapshot_and_stream_join_has_no_gap_and_no_duplicate` — the join contract
- `a_full_socket_conflates_rather_than_queueing` — conflation against a socket whose
  capacity the test controls exactly

CI runs {Linux, macOS} × {gcc, clang} × {Debug, Release} with warnings as errors, then
ASan, UBSan and TSan, then the end-to-end smoke test.

The seqlock's read is a benign data race by the letter of the memory model, so under
ThreadSanitizer it compiles to a mutex-backed equivalent — suppressing the report would
also hide real races in the same file.

---

## Layout

```
include/cascade/
  core/     lock-free primitives: SPSC ring, seqlock, dirty bitmap,
            open-addressing hash map, HdrHistogram-style latency histogram
  proto/    wire formats: MoldUDP64 + ITCH-shaped feed, subscriber protocol
  feed/     decoder, gap detection and recovery state machine
  book/     price ladders, order books, sharded book builder
  dist/     subscriber, conflation, entitlements, fan-out, publication log
  net/      sockets, multicast, TCP, framing, output buffering
  sim/      order flow generator with the statistical shape of a real venue
apps/       cascaded, cascade-feedsim, cascade-sub, cascade-bench
docs/       architecture.md, benchmark-output.txt
```

## Scope

Deliberately absent: a snapshot/refresh feed for rebuilding after unrecoverable loss
(real venues provide one, e.g. NASDAQ GLIMPSE); a historical tick archive; cross-venue
consolidation or NBBO; IPv6; source-specific multicast; authentication beyond a bearer
token. See [`docs/architecture.md`](docs/architecture.md) §9.

## License

Apache 2.0.
