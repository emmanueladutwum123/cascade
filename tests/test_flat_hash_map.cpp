// SPDX-License-Identifier: Apache-2.0
#include "cascade/core/flat_hash_map.hpp"

#include <cstdint>
#include <random>
#include <unordered_map>
#include <vector>

#include "test_harness.hpp"

using cascade::FlatHashMap;

namespace {
// The book builder's keys are exchange order ids, which are dense and sequential.
// Identity-hashing them would be pathological for a power-of-two table, so the real
// map is always used with a mixing hash; these tests use one too.
struct MixHash {
  std::size_t operator()(std::uint64_t x) const noexcept {
    x ^= x >> 33;
    x *= 0xFF51AFD7ED558CCDull;
    x ^= x >> 33;
    return static_cast<std::size_t>(x);
  }
};

// A deliberately terrible hash: every key lands in the same slot. Nothing about
// correctness may depend on the hash being good, only performance.
struct AlwaysCollide {
  std::size_t operator()(std::uint64_t) const noexcept { return 0; }
};

using Map = FlatHashMap<std::uint64_t, std::uint64_t, MixHash>;
}  // namespace

TEST(insert_find_erase_basics) {
  Map map(0, 16);
  CHECK(map.empty());
  CHECK(map.find(42) == nullptr);

  CHECK(map.insert_or_assign(42, std::uint64_t{100}));
  CHECK_EQ(map.size(), std::size_t{1});
  CHECK(map.find(42) != nullptr);
  CHECK_EQ(*map.find(42), std::uint64_t{100});

  // Re-inserting an existing key overwrites and does not grow the map.
  CHECK(!map.insert_or_assign(42, std::uint64_t{200}));
  CHECK_EQ(map.size(), std::size_t{1});
  CHECK_EQ(*map.find(42), std::uint64_t{200});

  CHECK(map.erase(42));
  CHECK_EQ(map.size(), std::size_t{0});
  CHECK(map.find(42) == nullptr);
  CHECK(!map.erase(42));  // erasing twice is not an error
}

TEST(grows_and_preserves_every_entry) {
  Map map(0, 8);
  for (std::uint64_t i = 1; i <= 10'000; ++i) CHECK(map.insert_or_assign(i, i * 3));
  CHECK_EQ(map.size(), std::size_t{10'000});
  CHECK_GE(map.capacity(), std::size_t{10'000});
  for (std::uint64_t i = 1; i <= 10'000; ++i) {
    const std::uint64_t* found = map.find(i);
    CHECK(found != nullptr);
    if (found) CHECK_EQ(*found, i * 3);
  }
  CHECK(map.load_factor() <= 0.75);
}

// The reason backward-shift deletion exists. A tombstone map under this pattern
// degrades until probe chains are mostly dead slots; this asserts the invariant that
// makes backward shifting worth the complexity.
TEST(churn_does_not_degrade_probe_chains) {
  Map map(0, 1024);
  for (std::uint64_t i = 1; i <= 600; ++i) map.insert_or_assign(i, i);

  const std::size_t capacity_before = map.capacity();
  // Steady-state churn: constant occupancy, 200k insert/erase pairs.
  for (std::uint64_t round = 0; round < 200'000; ++round) {
    const std::uint64_t victim = (round % 600) + 1;
    CHECK(map.erase(victim));
    CHECK(map.insert_or_assign(victim, round));
  }
  CHECK_EQ(map.size(), std::size_t{600});
  CHECK_EQ(map.capacity(), capacity_before);  // no tombstone-forced rehash

  // With ~59% load and a mixing hash, chains stay short. A tombstone implementation
  // would show an unbounded maximum here.
  CHECK_LE(map.max_probe_distance(), std::size_t{64});
  for (std::uint64_t i = 1; i <= 600; ++i) CHECK(map.find(i) != nullptr);
}

TEST(erase_preserves_chains_under_total_collision) {
  // Every key probes from slot 0, so the whole table is one probe chain. Erasing from
  // the middle of it is the case backward shifting has to get exactly right.
  FlatHashMap<std::uint64_t, std::uint64_t, AlwaysCollide> map(0, 64);
  for (std::uint64_t i = 1; i <= 40; ++i) map.insert_or_assign(i, i * 7);

  for (std::uint64_t i = 1; i <= 40; i += 2) CHECK(map.erase(i));

  for (std::uint64_t i = 1; i <= 40; ++i) {
    const std::uint64_t* found = map.find(i);
    if (i % 2 == 1) {
      CHECK(found == nullptr);
    } else {
      CHECK(found != nullptr);
      if (found) CHECK_EQ(*found, i * 7);
    }
  }
  CHECK_EQ(map.size(), std::size_t{20});
}

TEST(erase_handles_wraparound_chains) {
  // Force a chain that wraps the end of the table back to slot 0.
  struct NearEnd {
    std::size_t operator()(std::uint64_t) const noexcept { return 15; }
  };
  FlatHashMap<std::uint64_t, std::uint64_t, NearEnd> map(0, 16);
  for (std::uint64_t i = 1; i <= 10; ++i) map.insert_or_assign(i, i);
  CHECK(map.erase(3));
  CHECK(map.erase(7));
  for (std::uint64_t i = 1; i <= 10; ++i) {
    const bool expected = (i != 3 && i != 7);
    CHECK_EQ(map.find(i) != nullptr, expected);
  }
}

// The real proof: run a long randomised workload against std::unordered_map and assert
// the two agree at every step. Any ordering, probing or deletion bug shows up here.
TEST(differential_against_unordered_map) {
  Map map(0, 64);
  std::unordered_map<std::uint64_t, std::uint64_t> reference;
  std::mt19937_64 rng(0xC0FFEE);
  std::vector<std::uint64_t> live;

  for (int step = 0; step < 300'000; ++step) {
    const int action = static_cast<int>(rng() % 100);

    if (action < 50 || live.empty()) {
      const std::uint64_t key = (rng() % 50'000) + 1;  // 0 is the empty sentinel
      const std::uint64_t value = rng();
      const bool inserted = map.insert_or_assign(key, value);
      const bool ref_inserted = reference.insert_or_assign(key, value).second;
      CHECK_EQ(inserted, ref_inserted);
      if (inserted) live.push_back(key);
    } else if (action < 80) {
      const std::uint64_t key = live[rng() % live.size()];
      const bool erased = map.erase(key);
      const bool ref_erased = reference.erase(key) > 0;
      CHECK_EQ(erased, ref_erased);
      if (erased) {
        for (std::size_t i = 0; i < live.size(); ++i) {
          if (live[i] == key) { live[i] = live.back(); live.pop_back(); break; }
        }
      }
    } else {
      const std::uint64_t key = (rng() % 50'000) + 1;
      const std::uint64_t* found = map.find(key);
      const auto ref_it = reference.find(key);
      if (ref_it == reference.end()) {
        CHECK(found == nullptr);
      } else {
        CHECK(found != nullptr);
        if (found) CHECK_EQ(*found, ref_it->second);
      }
    }

    if (step % 25'000 == 0) CHECK_EQ(map.size(), reference.size());
  }

  CHECK_EQ(map.size(), reference.size());
  for (const auto& [key, value] : reference) {
    const std::uint64_t* found = map.find(key);
    CHECK(found != nullptr);
    if (found) CHECK_EQ(*found, value);
  }
}

TEST(for_each_visits_every_live_entry_exactly_once) {
  Map map(0, 16);
  for (std::uint64_t i = 1; i <= 500; ++i) map.insert_or_assign(i, i);
  for (std::uint64_t i = 1; i <= 500; i += 3) map.erase(i);

  std::unordered_map<std::uint64_t, int> visits;
  std::uint64_t sum = 0;
  map.for_each([&](std::uint64_t k, std::uint64_t v) {
    ++visits[k];
    sum += v;
    CHECK_EQ(k, v);
  });
  CHECK_EQ(visits.size(), map.size());
  for (const auto& [key, count] : visits) {
    (void)key;
    CHECK_EQ(count, 1);
  }
  CHECK(sum > 0);
}

TEST(clear_empties_without_losing_capacity) {
  Map map(0, 16);
  for (std::uint64_t i = 1; i <= 1'000; ++i) map.insert_or_assign(i, i);
  const std::size_t capacity = map.capacity();
  map.clear();
  CHECK(map.empty());
  CHECK_EQ(map.capacity(), capacity);
  CHECK(map.find(500) == nullptr);
  CHECK(map.insert_or_assign(500, std::uint64_t{1}));
  CHECK_EQ(map.size(), std::size_t{1});
}
