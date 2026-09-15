// SPDX-License-Identifier: Apache-2.0
#include "cascade/core/spsc_ring.hpp"

#include <atomic>
#include <thread>
#include <vector>

#include "test_harness.hpp"

using cascade::SpscRing;

TEST(push_pop_fifo_order) {
  SpscRing<int, 8> ring;
  for (int i = 0; i < 8; ++i) CHECK(ring.try_push(i));
  CHECK(!ring.try_push(99));  // full: capacity is exact, no wasted slot

  for (int i = 0; i < 8; ++i) {
    int out = -1;
    CHECK(ring.try_pop(out));
    CHECK_EQ(out, i);
  }
  int drained = -1;
  CHECK(!ring.try_pop(drained));
}

TEST(wraps_around_many_times) {
  SpscRing<std::uint64_t, 4> ring;
  for (std::uint64_t round = 0; round < 1000; ++round) {
    CHECK(ring.try_push(round));
    CHECK(ring.try_push(round + 1));
    std::uint64_t out = 0;
    CHECK(ring.try_pop(out));
    CHECK_EQ(out, round);
    CHECK(ring.try_pop(out));
    CHECK_EQ(out, round + 1);
  }
  CHECK_EQ(ring.size_approx(), std::size_t{0});
  CHECK_EQ(ring.produced(), std::uint64_t{2000});
}

TEST(size_and_counters_track) {
  SpscRing<int, 16> ring;
  CHECK(ring.empty_approx());
  for (int i = 0; i < 5; ++i) CHECK(ring.try_push(i));
  CHECK_EQ(ring.size_approx(), std::size_t{5});
  int out = 0;
  CHECK(ring.try_pop(out));
  CHECK_EQ(ring.size_approx(), std::size_t{4});
  CHECK_EQ(ring.produced(), std::uint64_t{5});
  CHECK_EQ(ring.consumed(), std::uint64_t{1});
}

TEST(moves_rather_than_copies) {
  // The hot path pushes message structs; make sure move semantics are honoured so a
  // future payload with a heap member is not silently deep-copied per message.
  struct Tracked {
    int value{0};
    int* moves{nullptr};
    Tracked() = default;
    Tracked(int v, int* m) : value(v), moves(m) {}
    Tracked(Tracked&& other) noexcept : value(other.value), moves(other.moves) {
      if (moves) ++*moves;
    }
    Tracked& operator=(Tracked&& other) noexcept {
      value = other.value;
      moves = other.moves;
      if (moves) ++*moves;
      return *this;
    }
    Tracked(const Tracked&) = delete;
    Tracked& operator=(const Tracked&) = delete;
  };

  int moves = 0;
  SpscRing<Tracked, 4> ring;
  CHECK(ring.try_push(Tracked{7, &moves}));
  Tracked out;
  CHECK(ring.try_pop(out));
  CHECK_EQ(out.value, 7);
  CHECK_GE(moves, 1);
}

// The real correctness question for a lock-free queue is not whether it works
// single-threaded but whether the memory ordering holds under contention. This runs a
// producer and consumer flat out and verifies that every value arrives exactly once,
// in order, with no duplicates and no drops.
TEST(concurrent_producer_consumer_preserves_every_message) {
  constexpr std::uint64_t kMessages = 2'000'000;
  SpscRing<std::uint64_t, 1024> ring;
  std::atomic<bool> consumer_ok{true};

  std::thread consumer([&] {
    std::uint64_t expected = 0;
    while (expected < kMessages) {
      std::uint64_t value = 0;
      if (ring.try_pop(value)) {
        if (value != expected) { consumer_ok.store(false); return; }
        ++expected;
      } else {
        cascade::cpu_relax();
      }
    }
  });

  for (std::uint64_t i = 0; i < kMessages; ++i) {
    while (!ring.try_push(i)) cascade::cpu_relax();
  }
  consumer.join();

  CHECK(consumer_ok.load());
  CHECK_EQ(ring.produced(), kMessages);
  CHECK_EQ(ring.consumed(), kMessages);
}

// A payload wider than a machine word: catches an implementation that publishes the
// tail before the whole slot is written, which a single-word payload can hide.
TEST(concurrent_wide_payload_is_never_torn) {
  struct Wide {
    std::uint64_t a{0}, b{0}, c{0}, d{0};
  };
  constexpr std::uint64_t kMessages = 500'000;
  SpscRing<Wide, 256> ring;
  std::atomic<bool> ok{true};

  std::thread consumer([&] {
    for (std::uint64_t i = 0; i < kMessages; ++i) {
      Wide w;
      while (!ring.try_pop(w)) cascade::cpu_relax();
      // Every field is derived from the same counter, so any torn publish shows up
      // as fields that disagree with each other.
      if (w.a != i || w.b != ~i || w.c != (i << 1) || w.d != (i ^ 0xA5A5A5A5A5A5A5A5ull)) {
        ok.store(false);
        return;
      }
    }
  });

  for (std::uint64_t i = 0; i < kMessages; ++i) {
    Wide w{i, ~i, i << 1, i ^ 0xA5A5A5A5A5A5A5A5ull};
    while (!ring.try_push(w)) cascade::cpu_relax();
  }
  consumer.join();
  CHECK(ok.load());
}
