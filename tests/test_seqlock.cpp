// SPDX-License-Identifier: Apache-2.0
#include "cascade/core/seqlock.hpp"

#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>

#include "test_harness.hpp"

using cascade::SeqlockCell;

namespace {
// Stands in for a top-of-book quote: several fields that must be mutually consistent.
struct Quote {
  std::uint64_t sequence{0};
  std::int64_t bid_px{0};
  std::int64_t ask_px{0};
  std::uint32_t bid_qty{0};
  std::uint32_t ask_qty{0};
  std::uint64_t checksum{0};

  static std::uint64_t compute_checksum(std::uint64_t seq, std::int64_t bid,
                                        std::int64_t ask, std::uint32_t bq,
                                        std::uint32_t aq) {
    return seq * 0x9E3779B97F4A7C15ull ^ static_cast<std::uint64_t>(bid) * 31u ^
           static_cast<std::uint64_t>(ask) * 131u ^ bq * 7u ^ aq * 17u;
  }
  bool consistent() const {
    return checksum == compute_checksum(sequence, bid_px, ask_px, bid_qty, ask_qty);
  }
};
}  // namespace

TEST(store_then_load_roundtrips) {
  SeqlockCell<Quote> cell;
  Quote q{42, 10050, 10060, 300, 500, 0};
  q.checksum = Quote::compute_checksum(q.sequence, q.bid_px, q.ask_px, q.bid_qty, q.ask_qty);
  cell.store(q);

  const Quote got = cell.load();
  CHECK_EQ(got.sequence, std::uint64_t{42});
  CHECK_EQ(got.bid_px, std::int64_t{10050});
  CHECK_EQ(got.ask_qty, std::uint32_t{500});
  CHECK(got.consistent());
}

TEST(default_constructed_cell_is_zeroed) {
  SeqlockCell<Quote> cell;
  const Quote got = cell.load();
  CHECK_EQ(got.sequence, std::uint64_t{0});
  CHECK_EQ(got.bid_px, std::int64_t{0});
}

TEST(version_advances_by_two_per_publish) {
  SeqlockCell<Quote> cell;
  CHECK_EQ(cell.version(), std::uint64_t{0});
  Quote q{};
  cell.store(q);
  CHECK_EQ(cell.version(), std::uint64_t{2});
  cell.store(q);
  CHECK_EQ(cell.version(), std::uint64_t{4});
  // Even version at rest is the invariant readers rely on to know no write is in flight.
  CHECK_EQ(cell.version() % 2, std::uint64_t{0});
}

// The point of the whole structure: many readers hammering a cell while a writer
// updates it must never observe a half-written quote. The checksum makes a torn read
// detectable; without the version protocol this test fails quickly.
TEST(readers_never_observe_a_torn_quote) {
  SeqlockCell<Quote> cell;
  std::atomic<bool> stop{false};
  std::atomic<std::uint64_t> torn{0};
  std::atomic<std::uint64_t> reads{0};

  Quote initial{1, 100, 101, 1, 1, 0};
  initial.checksum = Quote::compute_checksum(1, 100, 101, 1, 1);
  cell.store(initial);

  std::vector<std::thread> readers;
  for (int r = 0; r < 4; ++r) {
    readers.emplace_back([&] {
      std::uint64_t local_reads = 0, local_torn = 0;
      while (!stop.load(std::memory_order_relaxed)) {
        Quote q;
        if (cell.try_load(q)) {
          ++local_reads;
          if (!q.consistent()) ++local_torn;
        }
      }
      reads.fetch_add(local_reads, std::memory_order_relaxed);
      torn.fetch_add(local_torn, std::memory_order_relaxed);
    });
  }

  for (std::uint64_t i = 1; i <= 400'000; ++i) {
    Quote q;
    q.sequence = i;
    q.bid_px = static_cast<std::int64_t>(10000 + (i % 97));
    q.ask_px = q.bid_px + 10;
    q.bid_qty = static_cast<std::uint32_t>(i % 1000);
    q.ask_qty = static_cast<std::uint32_t>(i % 777);
    q.checksum = Quote::compute_checksum(q.sequence, q.bid_px, q.ask_px, q.bid_qty, q.ask_qty);
    cell.store(q);
  }
  stop.store(true);
  for (auto& t : readers) t.join();

  CHECK_EQ(torn.load(), std::uint64_t{0});
  CHECK_GE(reads.load(), std::uint64_t{1});  // readers actually ran
}

// A reader must always make progress eventually, and the value it gets must be one the
// writer actually published (monotonically non-decreasing sequence).
TEST(reader_sees_monotonically_advancing_sequences) {
  SeqlockCell<Quote> cell;
  std::atomic<bool> stop{false};
  std::atomic<bool> ok{true};

  std::thread reader([&] {
    std::uint64_t last = 0;
    while (!stop.load(std::memory_order_relaxed)) {
      const Quote q = cell.load();
      if (q.sequence < last) { ok.store(false); return; }
      last = q.sequence;
    }
  });

  for (std::uint64_t i = 1; i <= 200'000; ++i) {
    Quote q{};
    q.sequence = i;
    q.checksum = Quote::compute_checksum(i, 0, 0, 0, 0);
    cell.store(q);
  }
  stop.store(true);
  reader.join();
  CHECK(ok.load());
}
