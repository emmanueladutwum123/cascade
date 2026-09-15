// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstring>

#include "cascade/feed/event.hpp"
#include "cascade/proto/feed.hpp"

namespace cascade::feed {

/// Outcome of decoding one message out of a packet.
enum class DecodeResult { kOk, kTruncated, kUnknownType };

/// Decode one wire message into a normalised `FeedEvent`.
///
/// Every field is read with `memcpy` out of the byte buffer rather than by casting a
/// pointer to a packed struct. That is not timidity: the buffer is a `char` array
/// filled by `recv`, and reinterpreting it as a struct is a strict-aliasing violation
/// that compilers are increasingly willing to exploit. `memcpy` into a local is
/// defined behaviour and, at these sizes, compiles to exactly the same loads.
///
/// Bounds are checked before every read. This is the one place in the system that
/// parses bytes from the network, so a length field that cannot be trusted has to be
/// treated as hostile even when the sender is a friendly exchange: a truncated
/// datagram from a flaky NIC looks identical to a malicious one.
inline DecodeResult decode_message(const unsigned char* data, std::size_t bytes,
                                   std::uint64_t sequence, std::uint64_t ingest_ns,
                                   FeedEvent& out) noexcept {
  if (bytes < sizeof(proto::MsgHeader)) return DecodeResult::kTruncated;

  proto::MsgHeader header;
  std::memcpy(&header, data, sizeof(header));

  out = FeedEvent{};
  out.sequence = sequence;
  out.ingest_ns = ingest_ns;
  out.exchange_ns = header.timestamp_ns.value();
  out.type = header.type;

  switch (static_cast<proto::MsgType>(header.type)) {
    case proto::MsgType::kAddOrder: {
      if (bytes < sizeof(proto::AddOrderMsg)) return DecodeResult::kTruncated;
      proto::AddOrderMsg message;
      std::memcpy(&message, data, sizeof(message));
      out.order_id = message.order_id.value();
      out.symbol = Symbol::from_wire(message.symbol).raw();
      out.side = message.side;
      out.quantity = message.quantity.value();
      out.price = message.price.value();
      return DecodeResult::kOk;
    }
    case proto::MsgType::kReplace: {
      if (bytes < sizeof(proto::ReplaceMsg)) return DecodeResult::kTruncated;
      proto::ReplaceMsg message;
      std::memcpy(&message, data, sizeof(message));
      out.order_id = message.old_order_id.value();
      out.aux_id = message.new_order_id.value();
      out.quantity = message.quantity.value();
      out.price = message.price.value();
      return DecodeResult::kOk;
    }
    case proto::MsgType::kCancel: {
      if (bytes < sizeof(proto::CancelMsg)) return DecodeResult::kTruncated;
      proto::CancelMsg message;
      std::memcpy(&message, data, sizeof(message));
      out.order_id = message.order_id.value();
      out.quantity = message.cancelled_quantity.value();
      return DecodeResult::kOk;
    }
    case proto::MsgType::kDelete: {
      if (bytes < sizeof(proto::DeleteMsg)) return DecodeResult::kTruncated;
      proto::DeleteMsg message;
      std::memcpy(&message, data, sizeof(message));
      out.order_id = message.order_id.value();
      return DecodeResult::kOk;
    }
    case proto::MsgType::kExecute: {
      if (bytes < sizeof(proto::ExecuteMsg)) return DecodeResult::kTruncated;
      proto::ExecuteMsg message;
      std::memcpy(&message, data, sizeof(message));
      out.order_id = message.order_id.value();
      out.quantity = message.executed_quantity.value();
      out.aux_id = message.match_id.value();
      return DecodeResult::kOk;
    }
    case proto::MsgType::kTrade: {
      if (bytes < sizeof(proto::TradeMsg)) return DecodeResult::kTruncated;
      proto::TradeMsg message;
      std::memcpy(&message, data, sizeof(message));
      out.symbol = Symbol::from_wire(message.symbol).raw();
      out.side = message.side;
      out.quantity = message.quantity.value();
      out.price = message.price.value();
      out.aux_id = message.match_id.value();
      return DecodeResult::kOk;
    }
    case proto::MsgType::kSystemEvent: {
      if (bytes < sizeof(proto::SystemEventMsg)) return DecodeResult::kTruncated;
      proto::SystemEventMsg message;
      std::memcpy(&message, data, sizeof(message));
      out.event_code = message.event_code;
      return DecodeResult::kOk;
    }
  }
  // An unrecognised type is not fatal. Messages are length-prefixed precisely so a
  // consumer can skip a type added after it was built, which is what lets a venue
  // introduce one without a coordinated flag day.
  return DecodeResult::kUnknownType;
}

/// Encode helpers, used by the exchange simulator and by the tests that drive the
/// decoder against real bytes rather than against hand-built structs.
namespace encode {

inline std::size_t add_order(unsigned char* out, std::uint64_t timestamp_ns,
                             std::uint64_t order_id, Symbol symbol, Side side,
                             std::uint32_t quantity, Price price) noexcept {
  proto::AddOrderMsg message{};
  message.header.type = static_cast<std::uint8_t>(proto::MsgType::kAddOrder);
  message.header.timestamp_ns.set(timestamp_ns);
  message.order_id.set(order_id);
  symbol.to_wire(message.symbol);
  message.side = static_cast<std::uint8_t>(side);
  message.quantity.set(quantity);
  message.price.set(price);
  std::memcpy(out, &message, sizeof(message));
  return sizeof(message);
}

inline std::size_t delete_order(unsigned char* out, std::uint64_t timestamp_ns,
                                std::uint64_t order_id) noexcept {
  proto::DeleteMsg message{};
  message.header.type = static_cast<std::uint8_t>(proto::MsgType::kDelete);
  message.header.timestamp_ns.set(timestamp_ns);
  message.order_id.set(order_id);
  std::memcpy(out, &message, sizeof(message));
  return sizeof(message);
}

inline std::size_t cancel_order(unsigned char* out, std::uint64_t timestamp_ns,
                                std::uint64_t order_id, std::uint32_t quantity) noexcept {
  proto::CancelMsg message{};
  message.header.type = static_cast<std::uint8_t>(proto::MsgType::kCancel);
  message.header.timestamp_ns.set(timestamp_ns);
  message.order_id.set(order_id);
  message.cancelled_quantity.set(quantity);
  std::memcpy(out, &message, sizeof(message));
  return sizeof(message);
}

inline std::size_t execute_order(unsigned char* out, std::uint64_t timestamp_ns,
                                 std::uint64_t order_id, std::uint32_t quantity,
                                 std::uint64_t match_id) noexcept {
  proto::ExecuteMsg message{};
  message.header.type = static_cast<std::uint8_t>(proto::MsgType::kExecute);
  message.header.timestamp_ns.set(timestamp_ns);
  message.order_id.set(order_id);
  message.executed_quantity.set(quantity);
  message.match_id.set(match_id);
  std::memcpy(out, &message, sizeof(message));
  return sizeof(message);
}

inline std::size_t replace_order(unsigned char* out, std::uint64_t timestamp_ns,
                                 std::uint64_t old_order_id, std::uint64_t new_order_id,
                                 std::uint32_t quantity, Price price) noexcept {
  proto::ReplaceMsg message{};
  message.header.type = static_cast<std::uint8_t>(proto::MsgType::kReplace);
  message.header.timestamp_ns.set(timestamp_ns);
  message.old_order_id.set(old_order_id);
  message.new_order_id.set(new_order_id);
  message.quantity.set(quantity);
  message.price.set(price);
  std::memcpy(out, &message, sizeof(message));
  return sizeof(message);
}

inline std::size_t trade(unsigned char* out, std::uint64_t timestamp_ns, Symbol symbol,
                         Side side, std::uint32_t quantity, Price price,
                         std::uint64_t match_id) noexcept {
  proto::TradeMsg message{};
  message.header.type = static_cast<std::uint8_t>(proto::MsgType::kTrade);
  message.header.timestamp_ns.set(timestamp_ns);
  symbol.to_wire(message.symbol);
  message.side = static_cast<std::uint8_t>(side);
  message.quantity.set(quantity);
  message.price.set(price);
  message.match_id.set(match_id);
  std::memcpy(out, &message, sizeof(message));
  return sizeof(message);
}

inline std::size_t system_event(unsigned char* out, std::uint64_t timestamp_ns,
                                char event_code) noexcept {
  proto::SystemEventMsg message{};
  message.header.type = static_cast<std::uint8_t>(proto::MsgType::kSystemEvent);
  message.header.timestamp_ns.set(timestamp_ns);
  message.event_code = static_cast<std::uint8_t>(event_code);
  std::memcpy(out, &message, sizeof(message));
  return sizeof(message);
}

}  // namespace encode
}  // namespace cascade::feed
