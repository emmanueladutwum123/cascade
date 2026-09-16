// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>

#include "cascade/book/order_book.hpp"
#include "cascade/core/platform.hpp"

namespace cascade::dist {

/// A bounded, single-writer/many-reader log of every book image a shard published.
///
/// This exists to serve the one thing a conflated feed cannot: a subscriber that needs
/// *every* state an instrument passed through, not just its current one. Someone
/// recording the tape, or reconstructing a decision an algorithm made at a particular
/// microsecond, cannot be handed a collapsed view.
///
/// It also solves the classic joining problem. A subscriber arriving mid-stream needs
/// a consistent image *plus* every update after it, with no gap and no duplicate. The
/// naive approaches both fail: taking a snapshot and then subscribing loses anything
/// that happened in between, and subscribing then snapshotting delivers updates the
/// snapshot already contains. The join here is:
///
///   1. Read the current image through the seqlock; call its version V.
///   2. Start reading the log from its current write position.
///   3. Deliver every entry for that instrument whose version is greater than V.
///
/// Step 3 is what makes it airtight. An entry appended *after* we sampled the cursor
/// but carrying a version at or below V is one the snapshot already reflects, and it is
/// filtered. An entry with a version above V is one the snapshot does not reflect, and
/// it is delivered. Versions are monotonic per instrument, so the subscriber provably
/// sees each one exactly once.
///
/// The log is bounded, so a reader can fall out of the back of it. That is detected
/// rather than papered over: the reader is resynchronised from a fresh snapshot and
/// told that it happened.
class PublicationLog {
 public:
  explicit PublicationLog(std::size_t capacity) {
    std::size_t rounded = 1;
    while (rounded < capacity) rounded <<= 1;
    capacity_ = rounded;
    mask_ = rounded - 1;
    slots_ = std::make_unique<book::BookImage[]>(rounded);
  }

  std::size_t capacity() const noexcept { return capacity_; }

  /// Maintaining the log costs a full image copy per publish, which is pure waste when
  /// every subscriber is conflated — the common case. The shard checks this first, so
  /// a plant with no incremental subscribers pays nothing at all for the feature.
  bool enabled() const noexcept { return enabled_.load(std::memory_order_relaxed); }
  void set_enabled(bool enabled) noexcept {
    enabled_.store(enabled, std::memory_order_relaxed);
  }

  /// Append one published image. Called only from the shard that owns this log.
  CASCADE_ALWAYS_INLINE void append(const book::BookImage& image) noexcept {
    const std::uint64_t position = write_position_.load(std::memory_order_relaxed);
    slots_[position & mask_] = image;
    // Release: the slot contents must be visible before a reader can see the cursor
    // that makes the slot readable.
    write_position_.store(position + 1, std::memory_order_release);
  }

  std::uint64_t write_position() const noexcept {
    return write_position_.load(std::memory_order_acquire);
  }

  /// Oldest position still readable. Anything below this has been overwritten.
  std::uint64_t oldest_position() const noexcept {
    const std::uint64_t write = write_position();
    return write > capacity_ ? write - capacity_ : 0;
  }

  /// Read the entry at `position`. Returns false if it has been (or was being)
  /// overwritten, which means the reader has fallen behind and must resynchronise.
  ///
  /// The check runs *after* the copy as well as before it: the writer may lap the
  /// reader mid-copy, and the resulting image would be a mix of two entries. Verifying
  /// that the slot was still in the window when the copy finished is what makes a torn
  /// read detectable instead of silently corrupt.
  CASCADE_ALWAYS_INLINE bool read(std::uint64_t position,
                                  book::BookImage& out) const noexcept {
    const std::uint64_t write_before = write_position_.load(std::memory_order_acquire);
    if (position >= write_before) return false;               // nothing published yet
    if (write_before - position > capacity_) return false;    // already overwritten

    out = slots_[position & mask_];

    std::atomic_thread_fence(std::memory_order_acquire);
    const std::uint64_t write_after = write_position_.load(std::memory_order_relaxed);
    return (write_after - position) <= capacity_;
  }

 private:
  std::size_t capacity_{0};
  std::uint64_t mask_{0};
  alignas(kCacheLine) std::atomic<std::uint64_t> write_position_{0};
  alignas(kCacheLine) std::atomic<bool> enabled_{false};
  std::unique_ptr<book::BookImage[]> slots_;
};

}  // namespace cascade::dist
