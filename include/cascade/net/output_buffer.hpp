// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

#include "cascade/core/platform.hpp"

namespace cascade::net {

/// A fixed-capacity byte ring for a subscriber's pending socket writes.
///
/// Two properties matter more than speed here.
///
/// **Writes are all-or-nothing.** A frame either fits entirely or is refused. Writing
/// half a frame and hoping for room later would desynchronise the subscriber's parser
/// permanently, and the refusal is not a failure — it is the backpressure signal that
/// triggers conflation. The caller responds by collapsing the update rather than
/// queuing it, which is the entire point.
///
/// **Capacity is hard.** An unbounded output queue turns one slow subscriber into an
/// out-of-memory kill for every subscriber on the process. Bounding it converts that
/// into a single client being disconnected, which is recoverable.
class OutputBuffer {
 public:
  explicit OutputBuffer(std::size_t capacity) {
    std::size_t rounded = 1;
    while (rounded < capacity) rounded <<= 1;
    data_.resize(rounded);
    mask_ = rounded - 1;
  }

  std::size_t capacity() const noexcept { return data_.size(); }
  std::size_t pending() const noexcept { return static_cast<std::size_t>(tail_ - head_); }
  std::size_t available() const noexcept { return capacity() - pending(); }
  bool empty() const noexcept { return head_ == tail_; }

  /// Append bytes, or nothing at all if they do not fit.
  CASCADE_ALWAYS_INLINE bool write(const void* bytes, std::size_t length) noexcept {
    if (length > available()) return false;
    const auto* source = static_cast<const unsigned char*>(bytes);
    const std::size_t offset = static_cast<std::size_t>(tail_) & mask_;
    const std::size_t first = capacity() - offset;
    if (length <= first) {
      std::memcpy(data_.data() + offset, source, length);
    } else {
      // Wraps the end of the ring: two copies rather than a rotation.
      std::memcpy(data_.data() + offset, source, first);
      std::memcpy(data_.data(), source + first, length - first);
    }
    tail_ += length;
    return true;
  }

  /// Contiguous readable regions, at most two because the ring wraps at most once.
  /// Handing both to `writev` drains a wrapped buffer in a single syscall instead of
  /// two, which matters when the fan-out is flushing thousands of sockets per pass.
  struct Span {
    const unsigned char* data{nullptr};
    std::size_t length{0};
  };
  int readable(Span spans[2]) const noexcept {
    const std::size_t count = pending();
    if (count == 0) return 0;
    const std::size_t offset = static_cast<std::size_t>(head_) & mask_;
    const std::size_t first = capacity() - offset;
    if (count <= first) {
      spans[0] = Span{data_.data() + offset, count};
      return 1;
    }
    spans[0] = Span{data_.data() + offset, first};
    spans[1] = Span{data_.data(), count - first};
    return 2;
  }

  /// Release bytes the socket accepted. A short write is normal, so this takes a count
  /// rather than assuming the whole buffer went.
  void consume(std::size_t bytes) noexcept {
    head_ += bytes > pending() ? pending() : bytes;
  }

  void clear() noexcept { head_ = tail_ = 0; }

 private:
  std::vector<unsigned char> data_;
  std::size_t mask_{0};
  // Free-running counters, masked on use: "full" and "empty" stay distinguishable
  // without sacrificing a byte of capacity.
  std::uint64_t head_{0};
  std::uint64_t tail_{0};
};

/// Where a subscriber's bytes actually go. An interface because it is called once per
/// flush, not once per message, so the indirection is free — and because it lets the
/// entire conflation and eviction state machine be tested against a fake socket whose
/// capacity the test controls exactly.
class ByteSink {
 public:
  virtual ~ByteSink() = default;
  /// Write up to `length` bytes. Returns the number accepted (possibly 0 if the
  /// socket would block), or -1 on a fatal error.
  virtual long write_some(const OutputBuffer::Span* spans, int span_count) = 0;
};

}  // namespace cascade::net
