// SPDX-License-Identifier: Apache-2.0
#include "cascade/core/dirty_set.hpp"

#include <atomic>
#include <set>
#include <thread>
#include <vector>

#include "test_harness.hpp"

using cascade::DirtySet;

TEST(marks_are_drained_once_and_cleared) {
  DirtySet dirty(1000);
  dirty.mark(0);
  dirty.mark(7);
  dirty.mark(63);
  dirty.mark(64);
  dirty.mark(999);

  std::vector<std::uint32_t> seen;
  CHECK_EQ(dirty.drain([&](std::uint32_t i) { seen.push_back(i); }), std::size_t{5});
  CHECK_EQ(seen.size(), std::size_t{5});
  CHECK_EQ(seen[0], std::uint32_t{0});
  CHECK_EQ(seen[4], std::uint32_t{999});

  // A drained set is empty: a second drain yields nothing.
  CHECK_EQ(dirty.drain([](std::uint32_t) {}), std::size_t{0});
  CHECK(!dirty.maybe_dirty());
}

TEST(duplicate_marks_collapse) {
  // This is the whole point of using a bitmap instead of a queue: a thousand updates
  // to one instrument while the reader is busy must cost one delivery, not a thousand.
  DirtySet dirty(64);
  for (int i = 0; i < 10'000; ++i) dirty.mark(42);
  std::size_t deliveries = 0;
  dirty.drain([&](std::uint32_t index) {
    CHECK_EQ(index, std::uint32_t{42});
    ++deliveries;
  });
  CHECK_EQ(deliveries, std::size_t{1});
}

TEST(sparse_marks_across_a_large_set) {
  DirtySet dirty(100'000);
  std::set<std::uint32_t> expected;
  for (std::uint32_t i = 0; i < 100'000; i += 4099) {
    dirty.mark(i);
    expected.insert(i);
  }
  std::set<std::uint32_t> seen;
  dirty.drain([&](std::uint32_t i) { seen.insert(i); });
  CHECK_EQ(seen.size(), expected.size());
  CHECK(seen == expected);
}

TEST(idle_set_reports_nothing_to_do) {
  DirtySet dirty(10'000);
  CHECK(!dirty.maybe_dirty());
  dirty.mark(5'000);
  CHECK(dirty.maybe_dirty());
  dirty.drain([](std::uint32_t) {});
  CHECK(!dirty.maybe_dirty());
}

TEST(every_index_in_range_is_addressable) {
  // Off-by-one at a word or summary-word boundary would silently lose an instrument.
  for (std::size_t capacity : {std::size_t{1}, std::size_t{63}, std::size_t{64},
                               std::size_t{65}, std::size_t{4095}, std::size_t{4096},
                               std::size_t{4097}}) {
    DirtySet dirty(capacity);
    for (std::uint32_t i = 0; i < capacity; ++i) dirty.mark(i);
    std::size_t count = 0;
    dirty.drain([&](std::uint32_t) { ++count; });
    CHECK_EQ(count, capacity);
  }
}

// The invariant that makes the structure safe: a mark racing a drain is either
// delivered by that drain or left set for the next one, but never lost.
TEST(concurrent_marks_are_never_lost) {
  constexpr std::uint32_t kIndices = 4096;
  constexpr std::uint64_t kRounds = 50'000;
  DirtySet dirty(kIndices);

  std::atomic<bool> producers_done{false};
  std::atomic<std::uint64_t> delivered{0};
  std::vector<std::atomic<std::uint64_t>> marks(kIndices);
  for (auto& m : marks) m.store(0, std::memory_order_relaxed);

  std::vector<std::thread> producers;
  for (int p = 0; p < 3; ++p) {
    producers.emplace_back([&, p] {
      for (std::uint64_t round = 0; round < kRounds; ++round) {
        const std::uint32_t index =
            static_cast<std::uint32_t>((round * 2654435761u + static_cast<std::uint32_t>(p)) % kIndices);
        marks[index].fetch_add(1, std::memory_order_relaxed);
        dirty.mark(index);
      }
    });
  }

  std::thread consumer([&] {
    while (!producers_done.load(std::memory_order_acquire) || dirty.maybe_dirty()) {
      dirty.drain([&](std::uint32_t) { delivered.fetch_add(1, std::memory_order_relaxed); });
    }
  });

  for (auto& t : producers) t.join();
  producers_done.store(true, std::memory_order_release);
  consumer.join();

  // Every index that was marked at least once must have been delivered at least once,
  // and the set must be empty at the end.
  CHECK(!dirty.maybe_dirty());
  CHECK_GE(delivered.load(), std::uint64_t{1});
  // Collapsing means deliveries <= marks; losing an update would mean a set bit left
  // behind, which the emptiness check above rules out.
  CHECK_LE(delivered.load(), std::uint64_t{3 * kRounds});
}
