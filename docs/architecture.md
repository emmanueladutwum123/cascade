# Architecture

This document explains what cascade does, why each tier is shaped the way it is, and
which alternatives were rejected and on what grounds.

---

## 1. The problem

A market data plant sits between an exchange and everyone who needs to know what the
market is doing. That sounds like a message router. It isn't, because four properties of
the input make it something harder.

**The feed is unreliable and unordered.** Venues distribute market data over UDP
multicast, because it lets the exchange send one copy for the whole market and have the
network replicate it — the venue's cost is independent of how many firms are listening,
and everyone gets the same bytes at the same instant, which is a fairness property as
much as an efficiency one. What it gives up is delivery guarantees, ordering, and flow
control. Every receiver independently handles loss, reordering, duplication, and the
fact that it cannot ask the exchange to slow down.

**The feed describes orders; consumers want prices.** An order-by-order feed publishes
individual order lifecycle events — added, cancelled, executed, replaced. The order
*book* is a projection the consumer maintains. That projection is a running fold over
the entire message stream, which means a single lost message corrupts it permanently
and silently. There is no way to detect after the fact that a book is wrong.

**The message rates are large and the tolerances are small.** A liquid venue produces
millions of messages per second at the open, over 90% of them cancels. A consumer that
falls a few milliseconds behind is trading on prices that no longer exist.

**Consumers are heterogeneous and some of them are slow.** A pricing engine wants every
event on three instruments. A risk system wants the current state of fifty thousand. A
trader's screen wants whatever fits down a congested VPN. The plant has to serve all of
them from one copy of the data, and — critically — the slowest of them must not be able
to slow down anyone else.

---

## 2. Shape of the system

```
   exchange (UDP multicast, one group per channel)
        │
        │  MoldUDP64 framing, ITCH-shaped messages, big-endian
        ▼
┌───────────────────┐        ┌──────────────────────────┐
│   feed handler    │◄──TCP──┤  venue retransmit server  │
│  gap detection,   │        └──────────────────────────┘
│  reorder window,  │
│  recovery         │
└─────────┬─────────┘
          │  decoded events, in order, one 64-byte struct each
          ▼
┌───────────────────┐
│    book shard     │   single-threaded, owns its state outright
│  order map +      │   ── no locks anywhere on this path ──
│  price ladders    │
└─────────┬─────────┘
          │
          ├──── seqlock cell per instrument ──────┐  published book state
          ├──── dirty bitmap per fan-out thread ──┤  "these moved"
          └──── SPSC trade ring per fan-out ──────┤  un-conflatable prints
                                                  ▼
                                    ┌──────────────────────────┐
                                    │      fan-out thread      │
                                    │  conflation, entitlement,│
                                    │  framing, eviction       │
                                    └────────────┬─────────────┘
                                                 │  TCP, host-endian frames
                        ┌────────────────────────┼────────────────────────┐
                        ▼                        ▼                        ▼
                   subscriber               subscriber               subscriber
```

Threads:

| Thread          | Count            | Owns                                            |
|-----------------|------------------|-------------------------------------------------|
| Feed channel    | one per channel  | a feed handler and a book shard, exclusively     |
| Fan-out         | configurable     | a disjoint set of subscriber connections         |
| Acceptor        | one              | the listening socket, nothing else               |

---

## 3. Partitioning: why a shard is a feed channel

The obvious way to shard a book builder is to hash instruments across threads. That is
what cascade does — but the partition is *not* free to be chosen, and the reason is in
the wire format.

An order-by-order feed identifies orders by id. A Delete message carries an order id and
nothing else — no symbol, no price, no side, because the venue assumes you already have
the order. To apply it you must be the thread holding that order. So the Add and the
Delete have to land on the same shard.

There is no hash of an order id that can guarantee that, because the order id is minted
by the venue and carries no instrument information. Routing after arrival is therefore
impossible in general.

Venues solve this by partitioning their multicast groups by symbol range, so every
message for an instrument — and every message for its orders — arrives on one channel.
cascade mirrors that partition: **one feed handler and one book shard per channel**, and
an order's whole lifecycle stays on the thread that owns it.

The payoff is large. Because exactly one thread ever mutates a given book, the order map,
the price ladders and the level vectors need no locks, no atomics, and no defensive
copying. The hottest code in the system is ordinary single-threaded C++.

---

## 4. Feed handling: three situations that look identical

`FeedHandler` consumes a sequenced, unreliable stream and emits an ordered event stream.
Its whole job is distinguishing three cases that all present as "the sequence number is
not what I expected", and which demand completely different responses.

### Reordering

The network delivered packet 5 before packet 4. Normal on any path with equal-cost
multipath, and it resolves in microseconds.

Treating this as loss is actively harmful: it fires a retransmit request for a packet
already in flight, and under load *every receiver doing that at once* is how a venue's
recovery server gets buried — precisely during the incident where it is needed. So the
handler holds the early packet in a reorder window and waits out a grace period first.

### Recoverable loss

Packet 4 really is gone. After the grace period, the handler asks the venue's retransmit
server for it over TCP — TCP because recovery is rare, per-receiver, and must arrive,
which is exactly the workload TCP is good at and multicast is not.

Two details here are load-bearing, and both were found by running the system rather than
by reasoning about it:

**The hole is `[expected, earliest held)`, not `[expected, newest seen)`.** Packets keep
arriving while a gap is open. If the gap is tracked against the newest arrival it appears
to grow to span all of them, so the retransmit request asks for hundreds of messages that
are already sitting in the reorder window. In a live run this produced 42,000 retransmits
served for about 1,500 genuinely missing messages.

**The reorder window is sized by recovery latency, not by network reordering.** Genuine
reordering resolves within a handful of packets, so a small window looks sufficient. It
isn't: while a gap is open, *every* subsequent packet is ahead of us and must be held. At
10,000 packets/second a 250 ms recovery deadline means 2,500 packets arrive before the
outcome is known. A 64-packet window overflows 6 ms in, and every packet dropped from it
becomes a second hole that recovery was never asked to fill. One lost datagram cascaded
into 20,000 lost messages.

When the window does overflow, the handler abandons the gap immediately rather than
dropping held packets. Waiting longer cannot help — there is nowhere to put what arrives
next — and abandoning bounds the loss to what was actually missing.

### Unrecoverable loss

The retransmit never came, or came back rejected because the range aged out of the
venue's buffer.

There is no way to build a correct book across a hole in an order-by-order stream. The
only honest move is to discard the affected books, flag them stale, and tell subscribers
immediately. **A plant that keeps publishing a book it knows is wrong is worse than one
that goes dark**, because downstream systems will trade on it.

### Heartbeats

On a quiet instrument, a loss at the end of a burst is invisible — the next message might
be hours away. Venue heartbeats carry the next sequence number, which is the only way to
notice.

---

## 5. Book building

Two data structures do the work, and both were chosen against a more obvious alternative.

### The order map

Every live order, keyed by exchange order id. Touched on every Delete, Execute, Cancel
and Replace — the large majority of an order-by-order feed. On a liquid venue that is
millions of entries and tens of millions of lookups a second, making it the single
hottest structure in the plant.

`std::unordered_map` is the wrong tool: it is node-based, so every lookup is a pointer
chase into a separately allocated node — a near-guaranteed cache miss that prefetching
cannot hide — and every insert allocates and every erase frees, on the hot path.

`FlatHashMap` is open-addressing with linear probing, so keys and values live in one flat
array and the common case touches a single cache line. Collisions probe forward linearly,
which the hardware prefetcher handles for free.

Deletion uses **backward shifting** rather than tombstones. Tombstones are simpler but
degrade steadily under this workload's actual access pattern — insert/erase churn at
roughly constant occupancy — until probe chains are mostly dead entries and a rehash is
forced. Backward shifting keeps every chain as short as a freshly built table. The
`churn_does_not_degrade_probe_chains` test exists to hold that property.

### The price ladder

A flat sorted vector, not a `std::map`. A map allocates a red-black node per level, so
walking the top ten levels — which happens on every publish — is ten dependent cache
misses through scattered memory.

The non-obvious part is the ordering: levels are stored **worst-first, best-last**.
Market data is overwhelmingly concentrated at the top of book, where new best prices
appear and are consumed constantly. With the best price at `back()`, those become
`push_back`/`pop_back`, which move nothing. Storing best-first would memmove the entire
ladder on exactly the most frequent operation.

### The change filter

The highest-leverage optimisation in the system, and it is four lines.

Most order events happen deep in the book and change nothing within the published depth —
an order added forty levels down, or cancelled, is invisible to every subscriber. Before
publishing, the shard renders the book and compares it against the last published image.
If nothing a subscriber could see has changed, the event stops there.

**Measured: ~80% of book changes never leave the shard.** Each one suppressed is a seqlock
write, a fan-out wake-up, a conflation update and a socket write per subscriber that
never happens.

---

## 6. Crossing threads without locks

Three mechanisms, each chosen for a different reason.

### Published state: a seqlock

A shard publishes each instrument's book through a versioned cell (`SeqlockCell`). The
version is even when stable and odd mid-write; a reader accepts a value only if it
sampled the same even version before and after copying.

The alternatives are both worse here. A mutex makes readers contend with each other and
lets a descheduled reader block the writer — unacceptable when the writer is the
market-data hot path. A `shared_ptr` swap allocates on every update and puts an atomic
refcount on a line every reader touches.

The seqlock lets the writer proceed unconditionally and never block; readers that race
simply retry. The cost is that a reader may copy a torn value before discovering it must
retry, which is why the payload must be trivially copyable and the torn bytes must never
be acted on.

That racy read is a genuine data race by the letter of the memory model, though a benign
and standard one. Under ThreadSanitizer the cell compiles to a mutex-backed equivalent
instead — suppressing the report would also hide real races in the same file.

### Change notification: a hierarchical dirty bitmap

This is the decision that makes conflation free.

A queue of `(symbol, version)` notifications has to be sized for the worst burst, and
when it overflows there is no good answer: dropping loses an update forever, blocking
puts the market-data path behind the slowest consumer.

A bitmap has neither problem, because **setting a bit twice is the same as setting it
once**. A thousand updates to one instrument while the fan-out is busy collapse into a
single set bit — which is exactly the desired semantics, since a subscriber wants the
instrument's *current* state, not a replay of states that are already history. Overflow
is not handled; it is impossible.

The catch is that draining a bitmap means scanning it. A summary level — one bit per 64
instruments — means the drain skips 4,096 quiet instruments per word it rejects.

### Trades: SPSC rings

Trades take the opposite path, and the asymmetry is the heart of the design.

**A quote is a state.** Two updates to the same book collapse into the later one with no
information lost.

**A trade is an event.** Two prints are two distinct facts; collapsing them would
fabricate a tape that never happened. So trades are queued in a bounded ring per fan-out
thread, and a consumer that overruns it is disconnected rather than quietly handed a
false tape. A subscriber holding a silently incomplete tape will compute wrong volumes
and wrong VWAPs and never know.

### Interest masks

Each instrument carries a bitmask of which fan-out threads have a subscriber for it. An
instrument nobody is watching costs nothing beyond building its book: no dirty marks, no
wake-ups, no ring writes. On a venue with 10,000 listings and a client watching 50, that
is the difference between the fan-out tier being idle and being saturated.

---

## 7. The fan-out tier

### Partition by subscriber, not by instrument

Mirroring the shard layout would be the natural-looking choice and is wrong here: a
client subscribing to 5,000 symbols would then be written to by every fan-out thread at
once, and its socket and output buffer would need locking.

Giving each subscriber exactly one owning thread means a connection's entire state —
buffer, subscriptions, conflation flags, eviction timers — is single-threaded and
lock-free. The only cross-thread contact is the inbound dirty set and trade ring.

Each dirty instrument's image is read through the seqlock **once per pass** and the copy
fanned out to every listener, so the expensive part does not multiply by the fan-out
factor.

### Conflation

When a book moves and the subscriber's socket has no room, the plant does not queue the
update. It sets a flag saying the instrument is owed, and on a later pass re-reads the
instrument's *current* image and sends that.

Consequences:

- Memory is bounded by the number of subscriptions, not by how far behind the client is.
- The client receives the freshest possible data rather than a backlog of history.
- A thousand updates during a stall cost one delivery.

Every update carries `conflated_count`: exactly how many book states the subscriber never
saw. It is derived from the version gap rather than counted, which makes it exact by
construction. Reporting it is deliberate — **a conflated feed that hides its own
conflation makes a quiet market indistinguishable from a saturated link**, and that
distinction matters enormously to anything trading on the data.

### Slow-consumer eviction

Eviction is **time-based, not depth-based**. A momentarily full socket is completely
normal — a scheduler blip, the opening burst — and evicting on depth alone would
disconnect healthy clients every morning. What is not normal is a socket that *stays*
full, because that means the client is structurally too slow and no amount of waiting
will fix it.

The eviction notice names the reason. "You were too slow to consume the trade stream"
and "you were idle" demand completely different fixes on the client side, and a bare TCP
reset leaves an operator guessing.

In a live test, a subscriber that stopped reading for 8 seconds had 6,168 book states
conflated away and was then evicted for **trade queue overrun** — the un-conflatable
stream overran before the stall deadline elapsed, which is exactly what the split
delivery model predicts.

### Entitlements

Redistributing a venue's data to a client that has not licensed it is a contractual
violation with real financial consequences. Entitlements are granted per venue — the unit
an exchange actually licenses — and resolution is a single 32-bit mask AND, cheap enough
to run per message.

That matters because checking only at subscribe time would leave a revoked client
streaming until it reconnected, which can be hours. An unknown symbol or unknown token is
a refusal, never a default-allow: failing open would hand out data for any symbol a
client cared to guess.

---

## 8. The subscriber protocol

Deliberately *not* a copy of the exchange's, in three ways:

**Little-endian and naturally aligned.** The upstream feed is big-endian because venues
standardised on it decades ago; both ends of this link are ours, so a received frame is
used in place with zero byte-swapping.

**Book images, not order events.** Subscribers want prices, not order lifecycle. Shipping
a top-N image means a lagging subscriber is brought current with one message rather than a
replay — which is what makes conflation possible at all.

**Quotes and trades are separate streams with different delivery guarantees**, for the
reasons in §6.

Frames are length-prefixed, so a client can skip an unrecognised type instead of
desynchronising — which is what lets the protocol gain a message type without a flag day.

### The snapshot/stream join

A subscriber arriving mid-stream needs a consistent image *plus* every update after it,
with no gap and no duplicate. Both naive orderings fail: snapshot-then-subscribe loses
anything in between, subscribe-then-snapshot delivers updates the snapshot already
contains.

cascade's join:

1. Read the current image through the seqlock; call its version `V`.
2. Acknowledge the subscription with `V`, and send that image.
3. Deliver every subsequent state whose version exceeds `V`.

Step 3 is what makes it airtight. Versions are monotonic per instrument and increment
only on a visible change, so any state at or below `V` is already reflected in the
snapshot and filters out, and any state above it is not. The subscriber provably sees
each version exactly once, and can verify it from `start_version` on the wire.

For subscribers that need *every* state rather than the current one, `PublicationLog`
holds a bounded window of published images and the same filter applies against log
positions. It is bounded, so a reader can fall out of the back — that is detected and
resynchronised rather than papered over. Maintaining it costs a full image copy per
publish, so it is only enabled when a subscriber actually needs it.

---

## 9. What is not here

Being explicit about the boundaries.

- **No snapshot/refresh feed.** After an unrecoverable gap, real venues offer a separate
  channel (NASDAQ's GLIMPSE, for instance) to rebuild a book from scratch. cascade flags
  the books stale and rebuilds from subsequent adds, which leaves it missing any order
  that was resting before the reset. The stale flag is honest about that.
- **No historical tick store.** The publication log is a bounded live window, not an
  archive.
- **No cross-venue consolidation.** One plant consumes one venue's channels; there is no
  NBBO or composite book.
- **IPv4 only**, and no source-specific multicast.
- **No authentication beyond a bearer token.** The entitlement model is real; the
  credential handling is not production-grade.

---

## 10. Testing

117 cases, no third-party framework — the project has zero dependencies, which keeps the
build reproducible anywhere a C++20 compiler exists.

The tests worth knowing about:

| Test | What it holds |
|------|---------------|
| `differential_against_unordered_map` | 300k randomised ops against `std::unordered_map`, step by step |
| `deep_book_stays_sorted_under_random_traffic` | 20k ladder ops differentially against `std::map` |
| `concurrent_producer_consumer_preserves_every_message` | 2M messages through the SPSC ring, exactly once, in order |
| `readers_never_observe_a_torn_quote` | 4 readers vs 400k writes, checksum-verified |
| `concurrent_marks_are_never_lost` | 3 producers racing a drain on the dirty bitmap |
| `churn_does_not_degrade_probe_chains` | 200k insert/erase at constant occupancy |
| `a_retransmit_asks_only_for_the_hole` | pins the gap-sizing bug found in production |
| `a_full_reorder_window_abandons_rather_than_cascading` | pins the cascade-loss bug |
| `snapshot_and_stream_join_has_no_gap_and_no_duplicate` | the join contract |
| `a_full_socket_conflates_rather_than_queueing` | conflation against a socket the test controls exactly |

CI runs every combination of {Linux, macOS} × {gcc, clang} × {Debug, Release} with
warnings as errors, then ASan, UBSan and TSan, then the end-to-end smoke test — a venue
publishing with injected packet loss, the plant recovering every gap, a subscriber
receiving real books.

---

## 11. Measurement

See `README.md` for numbers. Four things about how they were taken, because a benchmark
that measures the wrong thing confidently is worse than none:

- **Latency is paced, not saturated.** Measuring latency while pushing a system as hard
  as it will go measures queue depth. The load is stated.
- **Timed from when each message was due**, not when it was sent, so producer backlog
  appears in the distribution instead of silently removing the slowest samples — the
  coordinated-omission trap.
- **Measured at the exit**, by parsing the frames the plant actually wrote and
  differencing the ingest stamp they carry. No instrumentation inside the hot path.
- **Clock resolution is reported alongside.** `clock_gettime(CLOCK_MONOTONIC)` on macOS
  is quantised to 1 µs, coarser than the entire budget being measured; timestamps are
  derived from the hardware counter instead (~41.7 ns on Apple Silicon), and that floor
  is printed so a reader can tell measurement from noise.
