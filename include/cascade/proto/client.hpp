// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>

#include "cascade/proto/byteorder.hpp"
#include "cascade/proto/symbol.hpp"

namespace cascade::proto {

// ---------------------------------------------------------------------------
// Downstream subscriber protocol
//
// Unlike the upstream feed, this protocol is ours to define, and it is deliberately
// NOT a copy of the exchange's. Three differences, each load-bearing:
//
//   * **Little-endian, natively aligned.** The upstream feed is big-endian because
//     exchanges standardised on it decades ago; we have no such obligation, and both
//     ends of this link are ours. Matching host layout means a received frame is used
//     in place with zero byte-swapping and zero parsing.
//
//   * **Book images, not order events.** Subscribers want prices, not order lifecycle.
//     Shipping a top-N image instead of the events that produced it means a slow
//     subscriber can be brought current with one message rather than a replay, which
//     is what makes conflation possible at all.
//
//   * **Quotes and trades are separate streams with different delivery guarantees.**
//     A quote is a *state*: two updates to the same book collapse into the later one
//     with no information lost, so quotes are value-conflated. A trade is an *event*:
//     two trades are two distinct facts and collapsing them would fabricate history,
//     so trades are queued and a subscriber that cannot keep up is disconnected rather
//     than quietly given a false tape.
// ---------------------------------------------------------------------------

static_assert(!kHostIsBigEndian,
              "the subscriber protocol is little-endian on the wire; a big-endian "
              "build would need explicit conversion in the codec");

inline constexpr std::uint16_t kClientProtocolVersion = 1;

/// Book depth carried in an update. Ten levels covers the depth any pricing or
/// smart-order-routing decision actually reads; beyond that the marginal level is
/// almost never consulted but costs 16 bytes on every single update.
inline constexpr std::uint8_t kMaxDepth = 10;

enum class ClientMsgType : std::uint8_t {
  // client -> server
  kLogin        = 0x01,
  kSubscribe    = 0x10,
  kUnsubscribe  = 0x12,
  kClientHeartbeat = 0x30,

  // server -> client
  kLoginAck     = 0x02,
  kSubscribeAck = 0x11,
  kBookUpdate   = 0x20,
  kTradeTick    = 0x21,
  kSymbolStatus = 0x22,
  kHeartbeat    = 0x31,
  kEvicted      = 0x32,
  kReject       = 0x33,
};

/// Every frame is length-prefixed so the reader can consume a whole message or none of
/// it, and can skip an unrecognised type instead of desynchronising the stream.
struct CASCADE_PACKED FrameHeader {
  std::uint16_t payload_bytes;  ///< Bytes following this header.
  std::uint8_t type;            ///< A `ClientMsgType`.
  std::uint8_t flags;
};
static_assert(sizeof(FrameHeader) == 4, "unexpected frame header layout");

inline constexpr std::uint16_t kMaxFramePayload = 8192;

// --- session ---------------------------------------------------------------

struct CASCADE_PACKED LoginMsg {
  std::uint16_t protocol_version;
  char client_id[16];
  char token[32];   ///< Opaque credential; resolves to an entitlement set server-side.
};

enum class LoginStatus : std::uint8_t {
  kOk                 = 0,
  kUnknownClient      = 1,
  kBadToken           = 2,
  kVersionMismatch    = 3,
  kCapacityExceeded   = 4,
};

struct CASCADE_PACKED LoginAckMsg {
  std::uint8_t status;              ///< A `LoginStatus`.
  std::uint8_t pad[3];
  std::uint16_t server_version;
  std::uint16_t pad2;
  std::uint64_t session_id;
  std::uint32_t max_subscriptions;  ///< Server-enforced cap for this client.
  std::uint32_t entitled_venues;    ///< Bitmask, so the client can fail fast locally.
};

// --- subscription ----------------------------------------------------------

/// Delivery mode is per-subscription, not per-connection: one client may want a
/// conflated view of 5,000 symbols for a display and every event on the three it
/// actually trades.
enum SubscribeFlags : std::uint8_t {
  kFlagConflated   = 0x01,  ///< Collapse superseded book states (the default).
  kFlagIncremental = 0x02,  ///< Deliver every book state; evict if the client falls behind.
  kFlagWithTrades  = 0x04,  ///< Also deliver the (never-conflated) trade stream.
  kFlagSnapshotOnly= 0x08,  ///< One image, then implicitly unsubscribe.
};

struct CASCADE_PACKED SubscribeMsg {
  std::uint8_t flags;
  std::uint8_t pad[1];
  std::uint16_t symbol_count;
  // Followed by `symbol_count` packed 8-byte symbols.
};

enum class SubscribeStatus : std::uint8_t {
  kOk              = 0,
  kNotEntitled     = 1,  ///< Authenticated, but not licensed for this instrument's venue.
  kUnknownSymbol   = 2,
  kLimitExceeded   = 3,
  kAlreadySubscribed = 4,
};

struct CASCADE_PACKED SubscribeAckMsg {
  std::uint64_t symbol;
  std::uint8_t status;         ///< A `SubscribeStatus`.
  std::uint8_t pad[3];
  /// The book version the subscription starts from. In incremental mode the client is
  /// guaranteed every version strictly greater than this, with no gap and no repeat —
  /// this field is what makes the snapshot/stream join verifiable by the client.
  std::uint64_t start_version;
};

// --- market data -----------------------------------------------------------

struct CASCADE_PACKED PriceLevel {
  std::int64_t price;         ///< Fixed-point micro-units; `kNoPrice` if the level is empty.
  std::uint32_t quantity;
  std::uint32_t order_count;  ///< Orders resting at this level; a liquidity-quality signal.
};
static_assert(sizeof(PriceLevel) == 16, "price level should stay at 16 bytes");

enum BookFlags : std::uint8_t {
  kBookStale      = 0x01,  ///< Feed gap in progress: this image may be behind reality.
  kBookRecovering = 0x02,  ///< Gap recovery under way for this instrument's channel.
  kBookCrossed    = 0x04,  ///< bid >= ask. Real, and worth flagging rather than hiding.
  kBookHalted     = 0x08,
};

struct CASCADE_PACKED BookUpdateMsg {
  std::uint64_t symbol;
  std::uint64_t version;        ///< Per-symbol monotonic book version.
  std::uint64_t exchange_ns;    ///< Exchange timestamp of the event behind this state.
  std::uint64_t ingest_ns;      ///< When the feed handler first saw that event.

  /// How many book states were superseded and never sent because this subscriber was
  /// behind. Reporting it is a deliberate choice: a conflated feed that hides its own
  /// conflation makes it impossible for a consumer to tell a quiet market from a
  /// saturated link, and that distinction matters enormously to a trading system.
  std::uint32_t conflated_count;

  std::uint8_t bid_levels;
  std::uint8_t ask_levels;
  std::uint8_t flags;           ///< A `BookFlags` mask.
  std::uint8_t pad;
  // Followed by `bid_levels` PriceLevels (descending), then `ask_levels` (ascending).
};

struct CASCADE_PACKED TradeTickMsg {
  std::uint64_t symbol;
  std::uint64_t match_id;
  std::uint64_t exchange_ns;
  std::uint64_t ingest_ns;
  std::int64_t price;
  std::uint32_t quantity;
  std::uint8_t aggressor_side;  ///< A `Side`.
  std::uint8_t pad[3];
};

enum class SymbolState : std::uint8_t {
  kOk         = 0,
  kStale      = 1,   ///< Gap detected upstream; the book is not known to be current.
  kRecovered  = 2,   ///< Recovery completed; the book is authoritative again.
  kHalted     = 3,
  kClosed     = 4,
};

struct CASCADE_PACKED SymbolStatusMsg {
  std::uint64_t symbol;
  std::uint8_t state;  ///< A `SymbolState`.
  std::uint8_t pad[7];
  std::uint64_t version;
};

// --- session control -------------------------------------------------------

struct CASCADE_PACKED HeartbeatMsg {
  std::uint64_t server_ns;
  std::uint64_t messages_sent;
  std::uint64_t messages_conflated;  ///< Cumulative, so a client can see its own lag.
};

/// Why a subscriber was disconnected. Naming the reason matters: "you were too slow to
/// consume the trade stream" and "you were idle" demand completely different fixes on
/// the client side, and a generic disconnect leaves the operator guessing.
enum class EvictReason : std::uint8_t {
  kSlowConsumer      = 1,  ///< Send buffer stayed full past the configured deadline.
  kTradeQueueOverrun = 2,  ///< Un-conflatable backlog exceeded its bound.
  kHeartbeatTimeout  = 3,
  kProtocolViolation = 4,
  kServerShutdown    = 5,
  kEntitlementRevoked= 6,
};

struct CASCADE_PACKED EvictedMsg {
  std::uint8_t reason;  ///< An `EvictReason`.
  std::uint8_t pad[7];
  std::uint64_t backlog_bytes;    ///< What the server was holding when it gave up.
  std::uint64_t stalled_nanos;    ///< How long the client had been unable to keep up.
};

struct CASCADE_PACKED RejectMsg {
  std::uint16_t code;
  std::uint16_t text_bytes;
  std::uint8_t pad[4];
  // Followed by `text_bytes` of UTF-8.
};

}  // namespace cascade::proto
