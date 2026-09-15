// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <utility>

#include "cascade/core/platform.hpp"

namespace cascade {

/// Wait-free single-producer / single-consumer ring buffer.
///
/// This is the only queue on the hot path between the feed handler and the book
/// builders, so it is built to the usual three rules:
///
///  1. **No false sharing.** The producer's cursor and the consumer's cursor live on
///     separate cache lines. Without this, every push invalidates the consumer's line
///     and vice versa, which costs more than the queue operation itself.
///  2. **Cached counters.** A naive implementation loads the *other* side's atomic on
///     every operation, which is a guaranteed cross-core read. Instead each side keeps a
///     private, possibly-stale copy and only refreshes it when the queue looks
///     full (producer) or empty (consumer). In steady state neither side touches the
///     other's cache line at all.
///  3. **Free-running counters.** `head_`/`tail_` are monotonic 64-bit sequence numbers
///     masked on use, so "full" is `tail - head == Capacity` and there is no ambiguous
///     empty/full state costing a slot. At 100M msg/s a 64-bit counter takes ~5,800
///     years to wrap, so the unsigned-difference arithmetic never needs a wrap guard.
///
/// `Capacity` must be a power of two.
template <typename T, std::size_t Capacity>
class SpscRing {
  static_assert(Capacity >= 2, "capacity must be at least 2");
  static_assert((Capacity & (Capacity - 1)) == 0, "capacity must be a power of two");
  static_assert(std::is_nothrow_destructible<T>::value, "T must be nothrow-destructible");

  static constexpr std::uint64_t kMask = Capacity - 1;

 public:
  using value_type = T;

  SpscRing() = default;
  SpscRing(const SpscRing&) = delete;
  SpscRing& operator=(const SpscRing&) = delete;

  static constexpr std::size_t capacity() noexcept { return Capacity; }

  /// Producer side. Returns false if the ring is full; never blocks, never allocates.
  template <typename U>
  CASCADE_ALWAYS_INLINE bool try_push(U&& value) noexcept {
    const std::uint64_t tail = tail_.load(std::memory_order_relaxed);
    if (CASCADE_UNLIKELY(tail - head_cache_ >= Capacity)) {
      // Our cached view says full. Pay for one cross-core read to find out for real.
      head_cache_ = head_.load(std::memory_order_acquire);
      if (tail - head_cache_ >= Capacity) return false;
    }
    slots_[tail & kMask] = std::forward<U>(value);
    // Release: the slot write must be visible before the consumer can observe the
    // bumped tail and read that slot.
    tail_.store(tail + 1, std::memory_order_release);
    return true;
  }

  /// Consumer side. Returns false if the ring is empty.
  CASCADE_ALWAYS_INLINE bool try_pop(T& out) noexcept {
    const std::uint64_t head = head_.load(std::memory_order_relaxed);
    if (CASCADE_UNLIKELY(head == tail_cache_)) {
      tail_cache_ = tail_.load(std::memory_order_acquire);
      if (head == tail_cache_) return false;
    }
    out = std::move(slots_[head & kMask]);
    head_.store(head + 1, std::memory_order_release);
    return true;
  }

  /// Number of readable elements. Approximate when called from a third thread; exact
  /// when called from either the producer or the consumer.
  std::size_t size_approx() const noexcept {
    const std::uint64_t tail = tail_.load(std::memory_order_acquire);
    const std::uint64_t head = head_.load(std::memory_order_acquire);
    return static_cast<std::size_t>(tail - head);
  }

  bool empty_approx() const noexcept { return size_approx() == 0; }

  /// Total elements ever pushed / popped. Used by the metrics tier to compute
  /// throughput and queue depth without perturbing the hot path.
  std::uint64_t produced() const noexcept { return tail_.load(std::memory_order_relaxed); }
  std::uint64_t consumed() const noexcept { return head_.load(std::memory_order_relaxed); }

 private:
  // --- consumer-owned line -------------------------------------------------
  alignas(kCacheLine) std::atomic<std::uint64_t> head_{0};
  std::uint64_t tail_cache_{0};

  // --- producer-owned line -------------------------------------------------
  alignas(kCacheLine) std::atomic<std::uint64_t> tail_{0};
  std::uint64_t head_cache_{0};

  // --- payload -------------------------------------------------------------
  alignas(kCacheLine) T slots_[Capacity]{};
};

}  // namespace cascade
