// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <utility>
#include <vector>

#include "cascade/core/platform.hpp"

namespace cascade {

/// Open-addressing hash map with linear probing and backward-shift deletion.
///
/// The book builder keeps every live order in a map keyed by exchange order id and
/// touches it on *every* Delete, Execute, Cancel and Replace — which is the majority of
/// an order-by-order feed. On a liquid venue that is millions of entries and tens of
/// millions of lookups a second, so the map is the single hottest data structure in
/// the plant. `std::unordered_map` is the wrong tool for it:
///
///   * It is node-based, so every lookup is a pointer chase into a separately allocated
///     node — a near-guaranteed cache miss that prefetching cannot hide.
///   * Every insert allocates and every erase frees, on the hot path.
///   * Bucket lists mean the hash is computed, a bucket is read, *then* a chain is
///     walked, with each hop an independent miss.
///
/// Linear probing puts keys and values in one flat array, so the common case is a
/// single cache line touched, and the hardware prefetcher handles collisions for free
/// because probing walks forward linearly.
///
/// Deletion uses backward shifting rather than tombstones. Tombstones are simpler but
/// degrade steadily under the book builder's actual access pattern — a churn of
/// insert/erase at roughly constant occupancy — until probe chains are mostly dead
/// entries and a rehash is forced. Backward shifting keeps every probe chain exactly
/// as short as a freshly built table.
///
/// An `empty_key` sentinel marks free slots inline, so a probe is one memory access
/// rather than one into an occupancy bitmap plus one into the slot array.
template <typename K, typename V, typename Hash = std::hash<K>>
class FlatHashMap {
 public:
  using value_type = std::pair<K, V>;

  /// `empty_key` must be a key the caller will never insert. For exchange order ids 0
  /// is the natural choice: venues number orders from 1.
  explicit FlatHashMap(K empty_key, std::size_t initial_capacity = 1024,
                       Hash hash = Hash())
      : hash_(hash), empty_key_(empty_key) {
    std::size_t capacity = 8;
    while (capacity < initial_capacity) capacity <<= 1;
    reset_to_capacity(capacity);
  }

  std::size_t size() const noexcept { return size_; }
  std::size_t capacity() const noexcept { return slots_.size(); }
  bool empty() const noexcept { return size_ == 0; }
  double load_factor() const noexcept {
    return static_cast<double>(size_) / static_cast<double>(slots_.size());
  }

  /// Pointer to the value, or nullptr. The pointer is invalidated by any insert that
  /// grows the table or by any erase, which is why the hot path looks up and uses the
  /// result immediately rather than holding it.
  CASCADE_ALWAYS_INLINE V* find(const K& key) noexcept {
    std::size_t index = slot_for(key);
    while (slots_[index].first != empty_key_) {
      if (slots_[index].first == key) return &slots_[index].second;
      index = (index + 1) & mask_;
    }
    return nullptr;
  }

  CASCADE_ALWAYS_INLINE const V* find(const K& key) const noexcept {
    return const_cast<FlatHashMap*>(this)->find(key);
  }

  bool contains(const K& key) const noexcept { return find(key) != nullptr; }

  /// Insert or overwrite. Returns true if a new entry was created.
  template <typename U>
  bool insert_or_assign(const K& key, U&& value) {
    if (CASCADE_UNLIKELY(size_ + 1 > grow_threshold_)) grow();
    std::size_t index = slot_for(key);
    while (slots_[index].first != empty_key_) {
      if (slots_[index].first == key) {
        slots_[index].second = std::forward<U>(value);
        return false;
      }
      index = (index + 1) & mask_;
    }
    slots_[index].first = key;
    slots_[index].second = std::forward<U>(value);
    ++size_;
    return true;
  }

  /// Returns true if an entry was removed.
  bool erase(const K& key) noexcept {
    std::size_t index = slot_for(key);
    while (slots_[index].first != empty_key_) {
      if (slots_[index].first == key) {
        erase_at(index);
        return true;
      }
      index = (index + 1) & mask_;
    }
    return false;
  }

  void clear() noexcept {
    for (auto& slot : slots_) slot.first = empty_key_;
    size_ = 0;
  }

  /// Visit every live entry. Order is unspecified and changes across rehashes, so
  /// callers must not depend on it.
  template <typename Fn>
  void for_each(Fn&& fn) const {
    for (const auto& slot : slots_) {
      if (slot.first != empty_key_) fn(slot.first, slot.second);
    }
  }

  /// Longest probe distance in the table. Exposed because it is the honest health
  /// metric for an open-addressing map: a creeping maximum means the table is
  /// degrading long before the load factor looks alarming.
  std::size_t max_probe_distance() const noexcept {
    std::size_t worst = 0;
    for (std::size_t i = 0; i < slots_.size(); ++i) {
      if (slots_[i].first == empty_key_) continue;
      const std::size_t ideal = slot_for(slots_[i].first);
      const std::size_t distance = (i - ideal) & mask_;
      if (distance > worst) worst = distance;
    }
    return worst;
  }

 private:
  CASCADE_ALWAYS_INLINE std::size_t slot_for(const K& key) const noexcept {
    return static_cast<std::size_t>(hash_(key)) & mask_;
  }

  void reset_to_capacity(std::size_t capacity) {
    slots_.assign(capacity, value_type{empty_key_, V{}});
    mask_ = capacity - 1;
    // 0.75 is the point where linear probing's expected probe count starts climbing
    // steeply; below it the table stays effectively O(1) with excellent locality.
    grow_threshold_ = (capacity * 3) / 4;
    size_ = 0;
  }

  void grow() {
    std::vector<value_type> old_slots;
    old_slots.swap(slots_);
    reset_to_capacity((old_slots.size() ? old_slots.size() : 8) * 2);
    for (auto& slot : old_slots) {
      if (slot.first == empty_key_) continue;
      std::size_t index = slot_for(slot.first);
      while (slots_[index].first != empty_key_) index = (index + 1) & mask_;
      slots_[index].first = slot.first;
      slots_[index].second = std::move(slot.second);
      ++size_;
    }
  }

  /// Backward-shift deletion.
  ///
  /// Clearing a slot outright would break any probe chain that runs through it, so
  /// instead we walk forward from the hole and pull back the first element that is
  /// allowed to move: one whose ideal slot does not lie strictly inside the span we
  /// would be moving it across. Repeating until the next slot is empty leaves the
  /// table indistinguishable from one the element was never inserted into.
  void erase_at(std::size_t hole) noexcept {
    std::size_t probe = hole;
    while (true) {
      probe = (probe + 1) & mask_;
      if (slots_[probe].first == empty_key_) break;

      const std::size_t ideal = slot_for(slots_[probe].first);
      // Can `probe` legally move back into `hole`? Only if `hole` is still at or after
      // its ideal slot along the probe path. Expressed as cyclic distances, which
      // handles the table wrapping without a special case.
      const std::size_t hole_distance = (hole - ideal) & mask_;
      const std::size_t probe_distance = (probe - ideal) & mask_;
      if (hole_distance >= probe_distance) continue;  // moving it would orphan it

      slots_[hole].first = slots_[probe].first;
      slots_[hole].second = std::move(slots_[probe].second);
      hole = probe;
    }
    slots_[hole].first = empty_key_;
    slots_[hole].second = V{};
    --size_;
  }

  Hash hash_;
  K empty_key_;
  std::size_t mask_{0};
  std::size_t size_{0};
  std::size_t grow_threshold_{0};
  std::vector<value_type> slots_;
};

}  // namespace cascade
