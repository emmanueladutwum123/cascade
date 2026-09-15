// SPDX-License-Identifier: Apache-2.0
// Shared packet-building helpers for the feed handler and integration tests.
#pragma once

#include <cstring>
#include <vector>

#include "cascade/feed/decoder.hpp"
#include "cascade/proto/feed.hpp"

namespace cascade::test {

/// Builds MoldUDP64 datagrams the way the exchange simulator does, so the tests drive
/// the handler with real bytes rather than with hand-constructed structs. A parser bug
/// that only shows up against the wire format is exactly the bug worth catching.
class PacketBuilder {
 public:
  explicit PacketBuilder(const char* session = "CASCADE001") {
    std::memset(session_, ' ', sizeof(session_));
    const std::size_t length = std::strlen(session);
    std::memcpy(session_, session, length < 10 ? length : 10);
  }

  void begin(std::uint64_t first_sequence) {
    buffer_.assign(sizeof(proto::PacketHeader), 0);
    first_sequence_ = first_sequence;
    count_ = 0;
  }

  /// Append one already-encoded message, with its length prefix.
  void append(const unsigned char* message, std::size_t bytes) {
    proto::MessageLengthPrefix prefix{};
    prefix.length.set(static_cast<std::uint16_t>(bytes));
    const auto* prefix_bytes = reinterpret_cast<const unsigned char*>(&prefix);
    buffer_.insert(buffer_.end(), prefix_bytes, prefix_bytes + sizeof(prefix));
    buffer_.insert(buffer_.end(), message, message + bytes);
    ++count_;
  }

  template <typename EncodeFn>
  void append_with(EncodeFn&& encode) {
    unsigned char scratch[proto::kMaxMessageSize];
    const std::size_t bytes = encode(scratch);
    append(scratch, bytes);
  }

  /// Finish the packet and return it. `override_count` forges the message count, which
  /// the malformed-input tests need.
  const std::vector<unsigned char>& finish(int override_count = -1) {
    proto::PacketHeader header{};
    std::memcpy(header.session, session_, sizeof(session_));
    header.sequence.set(first_sequence_);
    header.message_count.set(override_count >= 0
                                 ? static_cast<std::uint16_t>(override_count)
                                 : count_);
    std::memcpy(buffer_.data(), &header, sizeof(header));
    return buffer_;
  }

  std::vector<unsigned char> heartbeat(std::uint64_t next_sequence) const {
    std::vector<unsigned char> packet(sizeof(proto::PacketHeader), 0);
    proto::PacketHeader header{};
    std::memcpy(header.session, session_, sizeof(session_));
    header.sequence.set(next_sequence);
    header.message_count.set(proto::kHeartbeatMessageCount);
    std::memcpy(packet.data(), &header, sizeof(header));
    return packet;
  }

  std::uint16_t count() const { return count_; }
  const char* session() const { return session_; }

 private:
  char session_[10]{};
  std::vector<unsigned char> buffer_;
  std::uint64_t first_sequence_{0};
  std::uint16_t count_{0};
};

/// One packet carrying `n` Add Order messages for a single instrument.
inline std::vector<unsigned char> add_order_packet(PacketBuilder& builder,
                                                   std::uint64_t first_sequence,
                                                   std::uint64_t first_order_id, int n,
                                                   const char* symbol = "TEST") {
  builder.begin(first_sequence);
  for (int i = 0; i < n; ++i) {
    const std::uint64_t order_id = first_order_id + static_cast<std::uint64_t>(i);
    builder.append_with([&](unsigned char* out) {
      return feed::encode::add_order(out, 1'000 + order_id, order_id,
                                     Symbol::from_text(symbol), Side::kBuy,
                                     100, price_from_double(100.0));
    });
  }
  return builder.finish();
}

}  // namespace cascade::test
