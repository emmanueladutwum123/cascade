// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "cascade/core/platform.hpp"

namespace cascade {

/// A concurrent "which of these changed?" set: a hierarchical atomic bitmap.
///
/// This is how a book shard tells a fan-out thread that an instrument moved, and
/// choosing a bitmap over a queue is the single decision that makes conflation free.
///
/// A queue of (symbol, version) notifications has to be sized for the worst burst, and
/// when it overflows there is no good answer — dropping a notification loses an update
/// forever, blocking puts the market-data hot path behind the slowest consumer. A
/// bitmap has neither problem, because **setting a bit twice is the same as setting it
/// once**. A thousand updates to one instrument while the fan-out is busy collapse into
/// a single set bit, which is exactly the semantics we want: the subscriber should get
/// the instrument's *current* state, not a replay of states that are already history.
/// Overflow is not handled, it is impossible.
///
/// The catch is that draining a bitmap means scanning it, and at 50,000 instruments a
/// naive scan reads 782 words to find perhaps three set bits. A summary level fixes
/// that: one bit per 64 instruments, so the drain skips 4,096 quiet instruments per
/// word it rejects, and a sparse update pattern costs a handful of loads.
class DirtySet {
 public:
  explicit DirtySet(std::size_t capacity) : capacity_(capacity) {
    const std::size_t words = (capacity + 63) / 64;
    const std::size_t summary_words = (words + 63) / 64;
    // std::atomic is not movable, so the arrays are allocated once and never resized;
    // capacity is a startup decision, which it should be anyway.
    words_ = std::make_unique<std::atomic<std::uint64_t>[]>(words ? words : 1);
    summary_ = std::make_unique<std::atomic<std::uint64_t>[]>(summary_words ? summary_words : 1);
    word_count_ = words;
    summary_word_count_ = summary_words;
    for (std::size_t i = 0; i < word_count_; ++i) words_[i].store(0, std::memory_order_relaxed);
    for (std::size_t i = 0; i < summary_word_count_; ++i)
      summary_[i].store(0, std::memory_order_relaxed);
  }

  std::size_t capacity() const noexcept { return capacity_; }

  /// Mark one index dirty. Safe to call from any number of threads.
  CASCADE_ALWAYS_INLINE void mark(std::uint32_t index) noexcept {
    const std::size_t word = index >> 6;
    const std::uint64_t bit = 1ull << (index & 63);

    // A plain load before the read-modify-write. An atomic fetch_or takes exclusive
    // ownership of the cache line every time; a load does not. Because market data is
    // heavily concentrated in a small set of active instruments, the bit is usually
    // already set and this turns the common case from a cross-core RMW into a
    // shared-state read that several shards can do concurrently.
    if ((words_[word].load(std::memory_order_relaxed) & bit) == 0) {
      const std::uint64_t previous = words_[word].fetch_or(bit, std::memory_order_release);
      if (previous == 0) {
        // This word just went from quiet to dirty, so the summary needs updating too.
        // Only the first bit in a word pays for this.
        const std::size_t summary_word = word >> 6;
        summary_[summary_word].fetch_or(1ull << (word & 63), std::memory_order_release);
      }
    }
  }

  /// Atomically take and clear every dirty index, invoking `fn(index)` for each.
  /// Returns how many were drained.
  ///
  /// Claiming a whole word with one exchange (rather than clearing bit by bit) means a
  /// shard marking during the drain either lands in the word before we take it — and
  /// is delivered now — or after, and is delivered on the next pass. There is no
  /// window in which a mark is lost.
  template <typename Fn>
  std::size_t drain(Fn&& fn) {
    std::size_t drained = 0;
    for (std::size_t s = 0; s < summary_word_count_; ++s) {
      if (summary_[s].load(std::memory_order_acquire) == 0) continue;
      std::uint64_t summary_bits = summary_[s].exchange(0, std::memory_order_acq_rel);

      while (summary_bits) {
        const std::size_t word_offset = static_cast<std::size_t>(__builtin_ctzll(summary_bits));
        summary_bits &= summary_bits - 1;  // clear the lowest set bit
        const std::size_t word = (s << 6) + word_offset;
        if (word >= word_count_) continue;

        std::uint64_t bits = words_[word].exchange(0, std::memory_order_acq_rel);
        while (bits) {
          const std::uint32_t bit_offset = static_cast<std::uint32_t>(__builtin_ctzll(bits));
          bits &= bits - 1;
          const std::uint32_t index = static_cast<std::uint32_t>(word << 6) + bit_offset;
          if (index < capacity_) {
            fn(index);
            ++drained;
          }
        }
      }
    }
    return drained;
  }

  /// Cheap "is there anything to do?" check, so an idle fan-out thread can park
  /// without scanning.
  bool maybe_dirty() const noexcept {
    for (std::size_t s = 0; s < summary_word_count_; ++s) {
      if (summary_[s].load(std::memory_order_acquire) != 0) return true;
    }
    return false;
  }

  /// Exact count of set bits. Diagnostics only: it is O(capacity/64) and racy.
  std::size_t count_approx() const noexcept {
    std::size_t total = 0;
    for (std::size_t w = 0; w < word_count_; ++w) {
      total += static_cast<std::size_t>(
          __builtin_popcountll(words_[w].load(std::memory_order_relaxed)));
    }
    return total;
  }

 private:
  std::size_t capacity_;
  std::size_t word_count_{0};
  std::size_t summary_word_count_{0};
  std::unique_ptr<std::atomic<std::uint64_t>[]> words_;
  std::unique_ptr<std::atomic<std::uint64_t>[]> summary_;
};

}  // namespace cascade
