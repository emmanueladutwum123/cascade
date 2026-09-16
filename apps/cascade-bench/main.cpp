// SPDX-License-Identifier: Apache-2.0
//
// Benchmark harness for the plant.
//
// Every number this prints is measured through the real code path, not a stand-in:
// messages are encoded into genuine MoldUDP64 packets, parsed by the real decoder,
// applied by the real shard, routed by the real fan-out and framed by the real
// subscriber encoder. The only substitution is the socket, because measuring the
// kernel's TCP stack would tell you about the kernel rather than about this program.
//
// Latency is reported as a distribution, never as a mean. For a ticker plant the mean
// is close to meaningless -- what decides whether a subscriber sees a stale book is
// p99.9, and an average hides it completely.

#include <atomic>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "cascade/book/book_shard.hpp"
#include "cascade/core/clock.hpp"
#include "cascade/core/flat_hash_map.hpp"
#include "cascade/core/histogram.hpp"
#include "cascade/core/seqlock.hpp"
#include "cascade/core/spsc_ring.hpp"
#include "cascade/dist/fanout.hpp"
#include "cascade/feed/feed_handler.hpp"
#include "cascade/sim/market.hpp"

using namespace cascade;

namespace {

// ---------------------------------------------------------------------------
// Plumbing
// ---------------------------------------------------------------------------

/// Absorbs bytes without a syscall. A real socket would make this benchmark a
/// measurement of the kernel's TCP stack rather than of the plant.
class NullSink : public net::ByteSink {
 public:
  long write_some(const net::OutputBuffer::Span* spans, int span_count) override {
    long total = 0;
    for (int i = 0; i < span_count; ++i) total += static_cast<long>(spans[i].length);
    bytes += static_cast<std::uint64_t>(total);
    return total;
  }
  std::uint64_t bytes{0};
};

struct Options {
  std::uint32_t instruments{500};
  std::uint32_t shards{2};
  std::uint32_t subscribers{50};
  std::uint32_t symbols_per_subscriber{100};
  std::uint64_t messages{5'000'000};
  /// Offered load for the latency run, in messages per second.
  std::uint64_t rate{300'000};
  std::uint32_t fanout_threads{1};
  bool micro{true};
  bool pipeline{true};
};

void print_header(const char* title) {
  std::printf("\n\033[1m%s\033[0m\n", title);
  for (int i = 0; i < 78; ++i) std::putchar('-');
  std::putchar('\n');
}

void report_rate(const char* label, std::uint64_t count, double seconds) {
  const double per_second = seconds > 0 ? static_cast<double>(count) / seconds : 0.0;
  std::printf("  %-34s %12.2f M/s   (%llu in %.3fs)\n", label, per_second / 1e6,
              static_cast<unsigned long long>(count), seconds);
}

/// Stop the optimiser from deleting work whose result is never used.
///
/// Without this, a benchmark of a self-contained operation on a local object measures
/// nothing at all: the compiler proves the object is never observed and removes the
/// whole loop, and the harness cheerfully reports 0.00 ns. The empty asm block with a
/// memory clobber makes the value observable to something the compiler cannot see
/// through, at zero instruction cost.
template <typename T>
CASCADE_ALWAYS_INLINE void do_not_optimize(const T& value) {
  asm volatile("" : : "r,m"(value) : "memory");
}

CASCADE_ALWAYS_INLINE void clobber_memory() { asm volatile("" : : : "memory"); }

/// Time a tight loop with the cycle counter.
///
/// `clock_gettime` costs ~20-25ns even through the vDSO, which is more than most of
/// the operations below take. Timing them with it would measure the clock. The cycle
/// counter is a single instruction, and amortising over many iterations puts its own
/// overhead well below the noise floor.
template <typename Fn>
double time_per_op_ns(std::uint64_t iterations, Fn&& fn) {
  const std::uint64_t start = cpu_ticks();
  fn(iterations);
  const std::uint64_t elapsed = cpu_ticks() - start;
  return ticks_to_nanos(elapsed) / static_cast<double>(iterations);
}

// ---------------------------------------------------------------------------
// Microbenchmarks: the primitives everything else is built on
// ---------------------------------------------------------------------------

void bench_primitives() {
  print_header("Primitives (single-threaded, amortised over the loop)");

  {
    SpscRing<std::uint64_t, 1024> ring;
    const double ns = time_per_op_ns(20'000'000, [&](std::uint64_t n) {
      std::uint64_t sink = 0;
      for (std::uint64_t i = 0; i < n; ++i) {
        ring.try_push(i);
        ring.try_pop(sink);
        do_not_optimize(sink);
      }
    });
    std::printf("  %-34s %9.2f ns  push + pop round trip\n", "SpscRing", ns);
  }

  {
    SeqlockCell<book::BookImage> cell;
    book::BookImage image{};
    image.bid_levels = 10;
    image.ask_levels = 10;
    const double store_ns = time_per_op_ns(5'000'000, [&](std::uint64_t n) {
      for (std::uint64_t i = 0; i < n; ++i) {
        image.version = i;
        cell.store(image);
        clobber_memory();
      }
    });
    const double load_ns = time_per_op_ns(5'000'000, [&](std::uint64_t n) {
      book::BookImage out;
      for (std::uint64_t i = 0; i < n; ++i) {
        do_not_optimize(cell.try_load(out));
        do_not_optimize(out);
      }
    });
    std::printf("  %-34s %9.2f ns  publish a %zu-byte book image\n", "Seqlock store",
                store_ns, sizeof(book::BookImage));
    std::printf("  %-34s %9.2f ns  uncontended read\n", "Seqlock load", load_ns);
  }

  {
    // Sized and driven the way the order map actually is: a churn of insert and erase
    // at roughly constant occupancy, which is the pattern that destroys a tombstone map.
    FlatHashMap<std::uint64_t, std::uint64_t, Mix64Hash> map(0, 1u << 20);
    constexpr std::uint64_t kLive = 500'000;
    for (std::uint64_t i = 1; i <= kLive; ++i) map.insert_or_assign(i, i);

    const double find_ns = time_per_op_ns(10'000'000, [&](std::uint64_t n) {
      for (std::uint64_t i = 0; i < n; ++i) {
        do_not_optimize(map.find((i % kLive) + 1));
      }
    });
    const double churn_ns = time_per_op_ns(2'000'000, [&](std::uint64_t n) {
      for (std::uint64_t i = 0; i < n; ++i) {
        const std::uint64_t key = (i % kLive) + 1;
        map.erase(key);
        map.insert_or_assign(key, i);
        // Keeping the map observably live is enough to stop the calls being elided.
        // A full memory clobber here would also force every cached load to be
        // re-read, which measures the barrier rather than the map.
        do_not_optimize(map.size());
      }
    });
    std::printf("  %-34s %9.2f ns  %llu live entries\n", "FlatHashMap find", find_ns,
                static_cast<unsigned long long>(kLive));
    std::printf("  %-34s %9.2f ns  erase + insert at steady occupancy\n",
                "FlatHashMap churn", churn_ns);
    std::printf("  %-34s %9zu     longest probe chain after churn\n", "  (health)",
                map.max_probe_distance());
  }

  {
    // A ladder of realistic depth. The add path is dominated by the top-of-book fast
    // path, which is what real flow hits.
    book::BidLadder ladder;
    for (int i = 0; i < 200; ++i) {
      ladder.add(price_from_double(100.0 - i * 0.01), 100);
    }
    const double ns = time_per_op_ns(5'000'000, [&](std::uint64_t n) {
      for (std::uint64_t i = 0; i < n; ++i) {
        const Price price = price_from_double(100.0 - static_cast<double>(i % 10) * 0.01);
        ladder.add(price, 100);
        ladder.remove(price, 100, 1);
        do_not_optimize(ladder.best_price());
      }
    });
    std::printf("  %-34s %9.2f ns  add + remove near the touch (200 levels deep)\n",
                "Order book ladder", ns);
  }

  {
    unsigned char scratch[proto::kMaxMessageSize];
    const std::size_t bytes = feed::encode::add_order(
        scratch, 1'000, 42, Symbol::from_text("AAPL"), Side::kBuy, 100,
        price_from_double(190.0));
    feed::FeedEvent event;
    const double ns = time_per_op_ns(20'000'000, [&](std::uint64_t n) {
      for (std::uint64_t i = 0; i < n; ++i) {
        do_not_optimize(feed::decode_message(scratch, bytes, i, i, event));
        do_not_optimize(event);
      }
    });
    std::printf("  %-34s %9.2f ns  big-endian wire -> normalised event\n",
                "Message decode", ns);
  }
}

// ---------------------------------------------------------------------------
// The pipeline, end to end
// ---------------------------------------------------------------------------

/// Measures the plant's real output latency.
///
/// Every book update a subscriber is sent carries the `ingest_ns` of the feed message
/// that produced it. Parsing the frames the plant actually wrote and differencing that
/// stamp against the current clock gives wire-to-wire latency measured at the exit,
/// through the real encoder, with no instrumentation inside the hot path at all.
///
/// This is also why latency is NOT measured per stage with the cycle counter on this
/// hardware: Apple Silicon's user-readable counter ticks at 24MHz, so its resolution is
/// ~41.7ns -- an order of magnitude coarser than a decode. Timing individual messages
/// with it would produce a histogram of quantisation noise. Stage costs are therefore
/// reported as amortised throughput, where the clock's granularity is irrelevant, and
/// the distribution below is taken where latencies are microseconds and the clock can
/// honestly resolve them.
class MeasuringSink : public net::ByteSink {
 public:
  explicit MeasuringSink(Histogram* histogram) : histogram_(histogram) {}

  long write_some(const net::OutputBuffer::Span* spans, int span_count) override {
    long total = 0;
    for (int i = 0; i < span_count; ++i) {
      buffer_.insert(buffer_.end(), spans[i].data, spans[i].data + spans[i].length);
      total += static_cast<long>(spans[i].length);
    }
    bytes += static_cast<std::uint64_t>(total);
    drain_frames();
    return total;
  }

  std::uint64_t bytes{0};
  std::uint64_t updates{0};

 private:
  void drain_frames() {
    const std::uint64_t now = hires_nanos();
    std::size_t offset = read_offset_;
    while (offset + sizeof(proto::FrameHeader) <= buffer_.size()) {
      proto::FrameHeader header;
      std::memcpy(&header, buffer_.data() + offset, sizeof(header));
      const std::size_t total = sizeof(header) + header.payload_bytes;
      if (offset + total > buffer_.size()) break;  // frame still arriving
      if (header.type == static_cast<std::uint8_t>(proto::ClientMsgType::kBookUpdate)) {
        proto::BookUpdateMsg body;
        std::memcpy(&body, buffer_.data() + offset + sizeof(header), sizeof(body));
        if (body.ingest_ns != 0 && now > body.ingest_ns) {
          histogram_->record(now - body.ingest_ns);
          ++updates;
        }
      }
      offset += total;
    }
    // Advance a read cursor rather than erasing from the front: erase() is an O(n)
    // memmove on every socket write, and charging that to the plant would inflate the
    // very latency this class exists to measure. Compact only when the consumed
    // prefix has grown large.
    read_offset_ = offset;  // `offset` already started at read_offset_
    if (read_offset_ > (1u << 16)) {
      buffer_.erase(buffer_.begin(), buffer_.begin() + static_cast<long>(read_offset_));
      read_offset_ = 0;
    }
  }

  Histogram* histogram_;
  std::vector<unsigned char> buffer_;
  std::size_t read_offset_{0};
};

/// Route an event to the shard that owns it.
///
/// Order-referencing messages carry no symbol, so they cannot be routed after the fact;
/// on a real venue they arrive on the same multicast channel as the Add that created
/// the order. This mirrors that by remembering which channel each order opened on.
class ChannelRouter {
 public:
  explicit ChannelRouter(std::uint32_t shards)
      : shards_(shards), order_channel_(0, 1u << 20) {}

  /// Returns the owning shard, or `kUnrouted` if the order predates our session.
  static constexpr std::uint32_t kUnrouted = UINT32_MAX;
  std::uint32_t route(const feed::FeedEvent& event) {
    if (event.symbol != 0) {
      const std::uint32_t shard = book::BookShard::shard_for(event.packed_symbol(), shards_);
      if (event.msg_type() == proto::MsgType::kAddOrder) {
        order_channel_.insert_or_assign(event.order_id, shard);
      }
      return shard;
    }
    const std::uint32_t* owner = order_channel_.find(event.order_id);
    if (!owner) return kUnrouted;
    if (event.msg_type() == proto::MsgType::kReplace) {
      order_channel_.insert_or_assign(event.aux_id, *owner);
    }
    return *owner;
  }

 private:
  std::uint32_t shards_;
  FlatHashMap<std::uint64_t, std::uint32_t, Mix64Hash> order_channel_;
};

struct Pipeline {
  dist::EntitlementTable entitlements;
  dist::InstrumentRegistry registry;
  std::vector<std::unique_ptr<book::BookShard>> shards;
  std::vector<book::BookShard*> shard_pointers;
  std::vector<std::unique_ptr<dist::FanoutThread>> fanouts;
  std::vector<std::unique_ptr<net::ByteSink>> sinks;
  std::vector<std::uint32_t> slots;
  std::vector<std::uint32_t> slot_owner;  ///< Which fan-out thread holds each slot.

  /// Subscribers whose frames are parsed to measure latency.
  ///
  /// Deliberately a handful rather than all of them: decoding every frame the plant
  /// writes costs more than the plant spends producing them, and doing it inside the
  /// fan-out thread would put the measurement squarely in the path it is measuring.
  /// Latency is a property of the pipeline, not of how many clients are watching, so a
  /// sample reports it faithfully at a fraction of the cost.
  static constexpr std::uint32_t kInstrumentedSubscribers = 2;

  Pipeline(const Options& options, const std::vector<Symbol>& symbols,
           Histogram* latency = nullptr) {
    entitlements.register_venue(0, "SIMX");

    for (std::uint32_t id = 0; id < options.shards; ++id) {
      book::BookShard::Config config;
      config.shard_id = id;
      config.max_symbols = options.instruments + 16;
      config.initial_orders = 1u << 20;
      shards.push_back(std::make_unique<book::BookShard>(config));
      shard_pointers.push_back(shards.back().get());
    }

    for (Symbol symbol : symbols) {
      const std::uint32_t shard_id =
          book::BookShard::shard_for(symbol, options.shards);
      const std::uint32_t index = shards[shard_id]->register_symbol(symbol);
      registry.add(symbol, dist::InstrumentLocation{shard_id, index});
      entitlements.assign_symbol(symbol, 0);
    }

    for (std::uint32_t id = 0; id < options.fanout_threads; ++id) {
      dist::FanoutThread::Config config;
      config.fanout_id = id;
      config.max_subscribers = options.subscribers + 8;
      config.dirty_capacity = options.instruments + 16;
      fanouts.push_back(std::make_unique<dist::FanoutThread>(config, shard_pointers,
                                                             &registry, &entitlements));
    }

    for (std::uint32_t s = 0; s < options.subscribers; ++s) {
      const std::uint32_t owner = s % options.fanout_threads;
      if (latency && s < kInstrumentedSubscribers) {
        sinks.push_back(std::make_unique<MeasuringSink>(latency));
      } else {
        sinks.push_back(std::make_unique<NullSink>());
      }
      dist::Subscriber::Config subscriber_config;
      subscriber_config.output_capacity = 1u << 20;
      auto subscriber = std::make_unique<dist::Subscriber>(s + 1, subscriber_config,
                                                           sinks.back().get());
      dist::ClientEntitlement entitlement;
      entitlement.client_id = "bench";
      entitlement.venue_mask = dist::EntitlementTable::mask_of({0});
      entitlement.max_subscriptions = options.symbols_per_subscriber + 1;
      subscriber->set_client("bench", entitlement);
      const std::uint32_t slot = fanouts[owner]->admit(std::move(subscriber));
      slots.push_back(slot);
      slot_owner.push_back(owner);

      // Overlapping subscription sets, so instruments have varying fan-out factors
      // rather than every client watching the same thing.
      for (std::uint32_t k = 0; k < options.symbols_per_subscriber; ++k) {
        const std::size_t pick = (s * 7 + k * 13) % symbols.size();
        fanouts[owner]->subscribe(slot, symbols[pick], proto::kFlagConflated);
      }
    }
  }
};

/// Per-stage cost, as amortised throughput over a large batch.
void bench_stage_throughput(const Options& options) {
  print_header("Stage throughput (amortised; the 24MHz counter cannot time single ops)");

  sim::MarketSimulator::Config sim_config;
  sim_config.instrument_count = options.instruments;

  // Pre-generate the message stream so generation is not charged to any stage.
  const std::uint64_t batch = options.messages < 2'000'000 ? options.messages : 2'000'000;
  std::vector<unsigned char> stream;
  std::vector<std::uint32_t> lengths;
  {
    sim::MarketSimulator simulator(sim_config);
    unsigned char scratch[proto::kMaxMessageSize];
    stream.reserve(batch * proto::kMaxMessageSize);
    lengths.reserve(batch);
    for (std::uint64_t i = 0; i < batch; ++i) {
      const std::size_t bytes = simulator.next_message(scratch, wall_nanos());
      stream.insert(stream.end(), scratch, scratch + bytes);
      lengths.push_back(static_cast<std::uint32_t>(bytes));
    }
  }

  // --- decode alone ---
  {
    feed::FeedEvent event;
    const std::uint64_t start = mono_nanos();
    std::size_t offset = 0;
    for (std::uint64_t i = 0; i < batch; ++i) {
      do_not_optimize(feed::decode_message(stream.data() + offset, lengths[i], i, 0, event));
      offset += lengths[i];
    }
    report_rate("decode", batch, static_cast<double>(mono_nanos() - start) / 1e9);
  }

  // --- decode + book build ---
  double decode_and_apply_seconds = 0;
  {
    sim::MarketSimulator probe(sim_config);
    Pipeline pipeline(options, probe.symbols());
    ChannelRouter router(options.shards);
    feed::FeedEvent event;

    const std::uint64_t start = mono_nanos();
    std::size_t offset = 0;
    std::uint64_t applied = 0;
    for (std::uint64_t i = 0; i < batch; ++i) {
      if (feed::decode_message(stream.data() + offset, lengths[i], i, 0, event) ==
          feed::DecodeResult::kOk) {
        const std::uint32_t shard = router.route(event);
        if (shard != ChannelRouter::kUnrouted) {
          pipeline.shards[shard]->apply(event);
          ++applied;
        }
      }
      offset += lengths[i];
    }
    decode_and_apply_seconds = static_cast<double>(mono_nanos() - start) / 1e9;
    report_rate("decode + book build", applied, decode_and_apply_seconds);
  }

  // --- the whole thing, fan-out included ---
  {
    sim::MarketSimulator probe(sim_config);
    Pipeline pipeline(options, probe.symbols());
    ChannelRouter router(options.shards);
    feed::FeedEvent event;

    const std::uint64_t start = mono_nanos();
    std::size_t offset = 0;
    std::uint64_t applied = 0;
    for (std::uint64_t i = 0; i < batch; ++i) {
      if (feed::decode_message(stream.data() + offset, lengths[i], i, 0, event) ==
          feed::DecodeResult::kOk) {
        const std::uint32_t shard = router.route(event);
        if (shard != ChannelRouter::kUnrouted) {
          pipeline.shards[shard]->apply(event);
          ++applied;
        }
      }
      offset += lengths[i];
      // A fan-out pass every 64 messages, roughly what a real thread manages between
      // socket wake-ups at this rate.
      if ((i & 0x3F) == 0) {
        for (auto& fanout : pipeline.fanouts) fanout->poll(mono_nanos());
      }
    }
    const double seconds = static_cast<double>(mono_nanos() - start) / 1e9;
    report_rate("decode + book + fan-out", applied, seconds);
    std::printf("  %-34s %12.2f ns/msg\n", "  (fan-out share)",
                (seconds - decode_and_apply_seconds) * 1e9 / static_cast<double>(applied));

    // What the plant's filters actually saved.
    std::uint64_t published = 0, suppressed = 0, trades = 0;
    for (const auto& shard : pipeline.shards) {
      published += shard->stats().books_published;
      suppressed += shard->stats().publishes_suppressed;
      trades += shard->stats().trades;
    }
    std::uint64_t sent = 0, conflated = 0, bytes_out = 0;
    for (std::size_t i = 0; i < pipeline.slots.size(); ++i) {
      dist::Subscriber* subscriber =
          pipeline.fanouts[pipeline.slot_owner[i]]->subscriber_at(pipeline.slots[i]);
      if (!subscriber) continue;
      sent += subscriber->stats().book_updates_sent;
      conflated += subscriber->stats().book_updates_conflated;
      bytes_out += subscriber->stats().bytes_sent;
    }
    const double suppressed_pct =
        published + suppressed ? 100.0 * static_cast<double>(suppressed) /
                                     static_cast<double>(published + suppressed)
                               : 0.0;

    print_header("Where the work goes");
    std::printf("  %-34s %14llu\n", "feed events applied",
                static_cast<unsigned long long>(applied));
    std::printf("  %-34s %14llu\n", "book changes visible at depth",
                static_cast<unsigned long long>(published));
    std::printf("  %-34s %14llu   %.1f%% stopped at the shard\n",
                "book changes below depth",
                static_cast<unsigned long long>(suppressed), suppressed_pct);
    std::printf("  %-34s %14llu\n", "trades printed",
                static_cast<unsigned long long>(trades));
    std::printf("  %-34s %14llu   across %u subscribers\n", "updates delivered",
                static_cast<unsigned long long>(sent), options.subscribers);
    std::printf("  %-34s %14llu\n", "states conflated away",
                static_cast<unsigned long long>(conflated));
    std::printf("  %-34s %14.1f MB\n", "bytes written to subscribers",
                static_cast<double>(bytes_out) / (1024.0 * 1024.0));
  }
}

/// Wire-to-wire latency with the tiers running concurrently, at a stated offered load.
///
/// Two things here matter more than the numbers themselves.
///
/// **The load is paced.** Measuring latency while pushing a system as hard as it will
/// go measures queue depth, not latency: every result is "the backlog was this deep",
/// and it says nothing about what a subscriber experiences at a realistic rate. Real
/// venues have a message rate; the plant is measured against one.
///
/// **Latency is timed from when each message was *due*, not from when it was sent.**
/// If the producer falls behind schedule, timing from the actual send silently discards
/// exactly the samples that were delayed -- the coordinated-omission trap, which makes
/// a saturated system look healthy because it stops asking questions while it is
/// struggling. Stamping the scheduled time instead means producer backlog shows up in
/// the distribution, where it belongs.
void bench_wire_to_wire(const Options& options) {
  char title[160];
  std::snprintf(title, sizeof(title),
                "Wire-to-wire latency at %.0fk msg/s offered load (feed -> book -> "
                "fan-out -> frame)",
                static_cast<double>(options.rate) / 1e3);
  print_header(title);

  sim::MarketSimulator::Config sim_config;
  sim_config.instrument_count = options.instruments;
  sim::MarketSimulator simulator(sim_config);
  const std::vector<Symbol> symbols = simulator.symbols();

  Histogram latency(10'000'000'000ull, 3);
  Pipeline pipeline(options, symbols, &latency);

  using EventRing = SpscRing<feed::FeedEvent, 1u << 16>;
  std::vector<std::unique_ptr<EventRing>> rings;
  for (std::uint32_t i = 0; i < options.shards; ++i) {
    rings.push_back(std::make_unique<EventRing>());
  }

  std::atomic<bool> feed_done{false};
  std::atomic<bool> shards_done{false};
  std::vector<std::thread> shard_threads;
  std::vector<std::uint64_t> shard_applied(options.shards, 0);

  // One thread per shard, as each would be driven by its own multicast channel.
  for (std::uint32_t id = 0; id < options.shards; ++id) {
    shard_threads.emplace_back([&, id] {
      feed::FeedEvent event;
      std::uint64_t applied = 0;
      while (true) {
        if (rings[id]->try_pop(event)) {
          pipeline.shards[id]->apply(event);
          ++applied;
        } else if (feed_done.load(std::memory_order_acquire)) {
          break;
        } else {
          cpu_relax();
        }
      }
      shard_applied[id] = applied;
    });
  }

  std::vector<std::thread> fanout_threads;
  for (std::uint32_t id = 0; id < options.fanout_threads; ++id) {
    fanout_threads.emplace_back([&, id] {
      while (!shards_done.load(std::memory_order_acquire)) {
        // Spinning flat out when idle steals cores from the shard threads, which on a
        // machine with fewer cores than threads makes latency worse, not better.
        if (!pipeline.fanouts[id]->has_work()) {
          for (int spin = 0; spin < 64; ++spin) cpu_relax();
          continue;
        }
        pipeline.fanouts[id]->poll(mono_nanos());
      }
      pipeline.fanouts[id]->poll(mono_nanos());  // final drain
    });
  }

  unsigned char scratch[proto::kMaxMessageSize];
  feed::FeedEvent event;
  ChannelRouter router(options.shards);

  const double interval_ns = 1e9 / static_cast<double>(options.rate);
  const std::uint64_t wall_start = mono_nanos();
  const std::uint64_t start = hires_nanos();
  std::uint64_t behind_schedule = 0;

  for (std::uint64_t i = 0; i < options.messages; ++i) {
    const std::uint64_t due =
        start + static_cast<std::uint64_t>(static_cast<double>(i) * interval_ns);
    const std::uint64_t now = hires_nanos();
    if (now < due) {
      while (hires_nanos() < due) cpu_relax();
    } else if (now - due > 1'000'000) {
      ++behind_schedule;
    }

    const std::size_t bytes = simulator.next_message(scratch, wall_nanos());
    // Stamp the message with when it was *due*, not with now. Anything the producer
    // could not keep up with then shows as latency rather than vanishing.
    if (feed::decode_message(scratch, bytes, i, due, event) != feed::DecodeResult::kOk) {
      continue;
    }
    const std::uint32_t shard = router.route(event);
    if (shard == ChannelRouter::kUnrouted) continue;
    while (!rings[shard]->try_push(event)) cpu_relax();
  }

  feed_done.store(true, std::memory_order_release);
  for (auto& thread : shard_threads) thread.join();
  shards_done.store(true, std::memory_order_release);
  for (auto& thread : fanout_threads) thread.join();
  const double seconds = static_cast<double>(mono_nanos() - wall_start) / 1e9;

  std::uint64_t applied = 0;
  for (std::uint64_t count : shard_applied) applied += count;

  report_rate("delivered at offered load", applied, seconds);
  if (behind_schedule) {
    std::printf("  %-34s %12llu messages (offered load exceeds capacity;\n"
                "  %-34s %12s  the delay is included in the distribution)\n",
                "producer fell behind on", static_cast<unsigned long long>(behind_schedule),
                "", "");
  }
  std::putchar('\n');
  std::printf("%s\n", latency.summary("  wire-to-wire").c_str());
  std::printf("\n  Distribution (feed message decoded -> subscriber frame written):\n%s",
              latency.distribution().c_str());
}

// ---------------------------------------------------------------------------

void usage() {
  std::printf(
      "cascade-bench -- benchmark the ticker plant\n\n"
      "  --instruments N     instruments to simulate (default 500)\n"
      "  --shards N          book-building shards / feed channels (default 2)\n"
      "  --subscribers N     connected subscribers (default 50)\n"
      "  --symbols N         subscriptions per subscriber (default 100)\n"
      "  --messages N        feed messages to run (default 5000000)\n"
      "  --rate N            offered load for the latency run, msg/s (default 300000)\n"
      "  --fanout N          fan-out threads (default 1)\n"
      "  --micro-only        primitives only\n"
      "  --pipeline-only     skip the primitive microbenchmarks\n"
      "  --help\n");
}

bool parse(int argc, char** argv, Options& options) {
  for (int i = 1; i < argc; ++i) {
    const std::string argument = argv[i];
    auto next = [&](std::uint64_t& out) {
      if (i + 1 >= argc) return false;
      out = std::strtoull(argv[++i], nullptr, 10);
      return true;
    };
    std::uint64_t value = 0;
    if (argument == "--help") { usage(); return false; }
    else if (argument == "--instruments" && next(value)) options.instruments = static_cast<std::uint32_t>(value);
    else if (argument == "--shards" && next(value)) options.shards = static_cast<std::uint32_t>(value);
    else if (argument == "--subscribers" && next(value)) options.subscribers = static_cast<std::uint32_t>(value);
    else if (argument == "--symbols" && next(value)) options.symbols_per_subscriber = static_cast<std::uint32_t>(value);
    else if (argument == "--messages" && next(value)) options.messages = value;
    else if (argument == "--rate" && next(value)) options.rate = value ? value : 1;
    else if (argument == "--fanout" && next(value)) options.fanout_threads = static_cast<std::uint32_t>(value ? value : 1);
    else if (argument == "--micro-only") options.pipeline = false;
    else if (argument == "--pipeline-only") options.micro = false;
    else { std::printf("unknown argument: %s\n\n", argument.c_str()); usage(); return false; }
  }
  if (options.shards == 0) options.shards = 1;
  if (options.fanout_threads == 0) options.fanout_threads = 1;
  if (options.fanout_threads > book::kMaxFanoutThreads) options.fanout_threads = book::kMaxFanoutThreads;
  if (options.symbols_per_subscriber > options.instruments) {
    options.symbols_per_subscriber = options.instruments;
  }
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  Options options;
  if (!parse(argc, argv, options)) return 0;

  std::printf("cascade-bench\n");
  std::printf("  hardware threads   : %u\n", std::thread::hardware_concurrency());
  std::printf("  tick frequency     : %.3f MHz (latency floor %.1f ns)\n",
              tick_frequency_hz() / 1e6, hires_resolution_nanos());
  std::printf("  cache line assumed : %zu bytes\n", kCacheLine);
  std::printf("  instruments        : %u across %u shards\n", options.instruments,
              options.shards);
  std::printf("  subscribers        : %u x %u subscriptions over %u fan-out thread(s)\n",
              options.subscribers, options.symbols_per_subscriber,
              options.fanout_threads);
  std::printf("  messages           : %llu\n", static_cast<unsigned long long>(options.messages));
  std::printf("  latency run load   : %llu msg/s\n",
              static_cast<unsigned long long>(options.rate));

  if (options.micro) bench_primitives();
  if (options.pipeline) {
    bench_stage_throughput(options);
    bench_wire_to_wire(options);
  }
  std::putchar('\n');
  return 0;
}
