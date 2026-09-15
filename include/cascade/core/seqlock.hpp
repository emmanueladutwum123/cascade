// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>
#include <type_traits>

#include "cascade/core/platform.hpp"

#if defined(CASCADE_THREAD_SANITIZER)
#  include <mutex>
#endif

namespace cascade {

/// Single-writer / many-reader versioned cell (a "seqlock").
///
/// This is how a book shard publishes top-of-book to the fan-out threads. The
/// alternative designs are both worse for this workload:
///
///   * A mutex makes readers contend with each other and lets a descheduled reader
///     block the writer — unacceptable when the writer is the market-data hot path.
///   * A shared_ptr swap allocates on every update and puts an atomic refcount on a
///     line that every reader touches.
///
/// The seqlock lets the writer proceed unconditionally and never block: readers detect
/// that they raced and simply retry. The cost is that a reader may read a torn value
/// before discovering it must retry, so `T` must be trivially copyable and the torn
/// bytes must never be acted on — which is exactly what the version check guarantees.
///
/// Protocol: `seq_` is even when the value is stable and odd while a write is in
/// flight. A reader accepts the value only if it sampled the same even version before
/// and after the copy.
template <typename T>
class SeqlockCell {
  static_assert(std::is_trivially_copyable<T>::value,
                "seqlock payload must be trivially copyable: readers may copy a torn value");

 public:
  SeqlockCell() noexcept { std::memset(&value_, 0, sizeof(value_)); }

  SeqlockCell(const SeqlockCell&) = delete;
  SeqlockCell& operator=(const SeqlockCell&) = delete;

#if !defined(CASCADE_THREAD_SANITIZER)

  /// Publish a new value. Wait-free; callable only from the single writer thread.
  CASCADE_ALWAYS_INLINE void store(const T& value) noexcept {
    const std::uint64_t seq = seq_.load(std::memory_order_relaxed);
    // Go odd: readers that sample this version will reject it.
    seq_.store(seq + 1, std::memory_order_relaxed);
    // Ensure the odd version is visible before any payload byte changes.
    std::atomic_thread_fence(std::memory_order_release);

    std::memcpy(&value_, &value, sizeof(T));

    // Ensure every payload byte is visible before the even version is.
    std::atomic_thread_fence(std::memory_order_release);
    seq_.store(seq + 2, std::memory_order_relaxed);
  }

  /// Attempt one read. Returns false if a write was in flight or raced us, in which
  /// case `out` holds indeterminate bytes and must be discarded.
  CASCADE_ALWAYS_INLINE bool try_load(T& out) const noexcept {
    const std::uint64_t before = seq_.load(std::memory_order_acquire);
    if (before & 1u) return false;  // write in flight

    std::memcpy(&out, &value_, sizeof(T));

    // The payload read must not be reordered after the second version sample,
    // otherwise we could validate against a version we had not yet observed.
    std::atomic_thread_fence(std::memory_order_acquire);
    return seq_.load(std::memory_order_relaxed) == before;
  }

#else  // CASCADE_THREAD_SANITIZER

  // The racy memcpy above is a genuine data race by the letter of the memory model
  // (benign, and the standard technique), but silencing TSan on it would also hide
  // real races in this file. Under TSan we use a mutex-backed cell with identical
  // semantics so the rest of the system is still checked properly.
  void store(const T& value) noexcept {
    std::lock_guard<std::mutex> guard(mutex_);
    std::memcpy(&value_, &value, sizeof(T));
    seq_.store(seq_.load(std::memory_order_relaxed) + 2, std::memory_order_relaxed);
  }

  bool try_load(T& out) const noexcept {
    std::lock_guard<std::mutex> guard(mutex_);
    std::memcpy(&out, &value_, sizeof(T));
    return true;
  }

#endif

  /// Spin until a consistent value is read. Bounded in practice: a reader can only be
  /// forced to retry by a concurrent write, and writes are short and non-blocking.
  CASCADE_ALWAYS_INLINE T load() const noexcept {
    T out;
    while (!try_load(out)) cpu_relax();
    return out;
  }

  /// Even version counter; increments by 2 per publish. Exposed for metrics and for
  /// the snapshot/delta join, which needs to know *which* version it captured.
  std::uint64_t version() const noexcept { return seq_.load(std::memory_order_acquire); }

 private:
  alignas(kCacheLine) mutable std::atomic<std::uint64_t> seq_{0};
#if defined(CASCADE_THREAD_SANITIZER)
  mutable std::mutex mutex_;
#endif
  alignas(kCacheLine) T value_;
};

}  // namespace cascade
