// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <type_traits>

#include "cascade/proto/byteorder.hpp"
#include "cascade/proto/symbol.hpp"

namespace cascade::proto {

// ---------------------------------------------------------------------------
// Upstream feed: "CMD" (Cascade Market Data)
//
// An order-by-order feed in the shape of NASDAQ ITCH 5.0, carried in MoldUDP64
// framing over UDP multicast. The shape is copied from the real thing deliberately,
// because the two properties that make an exchange feed hard to consume are both
// consequences of that design and are faithfully reproduced here:
//
//   1. It is unreliable and unordered. UDP multicast gives one-to-many fan-out at
//      line rate with no per-receiver cost to the exchange, and in exchange gives up
//      delivery guarantees entirely. Every packet carries a sequence number and the
//      receiver is responsible for noticing gaps and recovering them out of band.
//
//   2. It is order-by-order, not level-by-level. The feed describes individual order
//      lifecycle events; the *book* is a projection the consumer has to maintain. A
//      single dropped Delete permanently corrupts a price level, which is why gap
//      recovery is not optional.
//
// Every field is big-endian, as on real venues.
// ---------------------------------------------------------------------------

/// MoldUDP64 packet header. One UDP datagram carries a header plus N length-prefixed
/// messages, so a busy instrument's events are amortised over one syscall rather than
/// one datagram each.
struct CASCADE_PACKED PacketHeader {
  char session[10];       ///< Session identifier; changes on exchange restart.
  be64 sequence;          ///< Sequence number of the FIRST message in this packet.
  be16 message_count;     ///< 0 = heartbeat (carries the sequence but no payload).
};
static_assert(sizeof(PacketHeader) == 20, "MoldUDP64 header is 20 bytes on the wire");

inline constexpr std::uint16_t kHeartbeatMessageCount = 0;
inline constexpr std::uint16_t kEndOfSessionMessageCount = 0xFFFF;

/// Message type discriminators, matching ITCH's mnemonic letters.
enum class MsgType : std::uint8_t {
  kSystemEvent = 'S',   ///< Session lifecycle (open/close).
  kAddOrder    = 'A',   ///< A new displayable order joins the book.
  kReplace     = 'U',   ///< Cancel-replace: old id dies, new id born, price/qty change.
  kCancel      = 'X',   ///< Partial cancel: reduce the resting quantity.
  kDelete      = 'D',   ///< Full cancel: remove the order entirely.
  kExecute     = 'E',   ///< Resting order traded against; reduce quantity.
  kTrade       = 'P',   ///< Non-displayable execution; affects tape, not the book.
};

/// Header common to every message, immediately after its 2-byte length prefix.
struct CASCADE_PACKED MsgHeader {
  std::uint8_t type;    ///< A `MsgType`. Single byte, so no endianness concern.
  be64 timestamp_ns;    ///< Exchange send time, nanos since the UNIX epoch.
};
static_assert(sizeof(MsgHeader) == 9, "unexpected message header layout");

struct CASCADE_PACKED SystemEventMsg {
  MsgHeader header;
  std::uint8_t event_code;  ///< 'O' start of messages, 'Q' market open,
                            ///< 'M' market close, 'C' end of messages.
};

struct CASCADE_PACKED AddOrderMsg {
  MsgHeader header;
  be64 order_id;
  char symbol[8];           ///< Space-padded ASCII, packs straight into a `Symbol`.
  std::uint8_t side;        ///< A `Side`: 'B' or 'S'.
  be32 quantity;
  bei64 price;              ///< Fixed-point micro-units.
};

struct CASCADE_PACKED ReplaceMsg {
  MsgHeader header;
  be64 old_order_id;
  be64 new_order_id;        ///< Replace mints a new id: queue priority is forfeited.
  be32 quantity;
  bei64 price;
};

struct CASCADE_PACKED CancelMsg {
  MsgHeader header;
  be64 order_id;
  be32 cancelled_quantity;  ///< Amount removed, not the amount remaining.
};

struct CASCADE_PACKED DeleteMsg {
  MsgHeader header;
  be64 order_id;
};

struct CASCADE_PACKED ExecuteMsg {
  MsgHeader header;
  be64 order_id;
  be32 executed_quantity;
  be64 match_id;
};

struct CASCADE_PACKED TradeMsg {
  MsgHeader header;
  char symbol[8];
  std::uint8_t side;
  be32 quantity;
  bei64 price;
  be64 match_id;
};

/// Largest message the feed can produce. The feed handler's receive buffer and the
/// retransmit server's framing both size themselves off this, so a new message type
/// bigger than this is a compile-time decision rather than a runtime buffer overrun.
inline constexpr std::size_t kMaxMessageSize = sizeof(TradeMsg);
static_assert(kMaxMessageSize >= sizeof(AddOrderMsg) && kMaxMessageSize >= sizeof(ReplaceMsg),
              "kMaxMessageSize must dominate every message type");

/// A UDP datagram must fit a path MTU without fragmenting: a fragmented multicast
/// datagram is lost entirely if any fragment is lost, which converts a 1-in-10,000
/// packet loss into a far higher message loss rate. 1400 leaves room for IP/UDP
/// headers and any tunnelling in the path.
inline constexpr std::size_t kMaxDatagramSize = 1400;

/// Messages are length-prefixed inside a packet so a consumer can skip a type it does
/// not understand instead of desynchronising — this is what lets the exchange add a
/// message type without a flag day.
struct CASCADE_PACKED MessageLengthPrefix {
  be16 length;
};

// ---------------------------------------------------------------------------
// Retransmit (gap recovery) protocol
//
// Carried over TCP, not multicast: recovery is a rare, per-receiver, must-arrive
// conversation, which is exactly the workload TCP is good at and multicast is not.
// ---------------------------------------------------------------------------

enum class RecoveryType : std::uint8_t {
  kRequest  = 'R',  ///< "Send me [first, first+count)."
  kResponse = 'S',  ///< A replayed run of messages.
  kReject   = 'J',  ///< Range unavailable (aged out of the retransmit buffer).
};

struct CASCADE_PACKED RecoveryRequest {
  std::uint8_t type;      ///< RecoveryType::kRequest
  char session[10];
  be64 first_sequence;
  be16 count;             ///< Bounded so one request cannot monopolise the server.
};

struct CASCADE_PACKED RecoveryResponseHeader {
  std::uint8_t type;      ///< kResponse or kReject
  char session[10];
  be64 first_sequence;
  be16 count;             ///< 0 on reject.
  be32 payload_bytes;     ///< Total bytes of length-prefixed messages that follow.
};

/// A single request may not ask for more than this many messages. Recovery runs on the
/// same thread that serves other receivers, so an unbounded range would let one badly
/// behaved consumer stall recovery for everybody.
inline constexpr std::uint16_t kMaxRecoveryBatch = 1024;

}  // namespace cascade::proto
