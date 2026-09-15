// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <type_traits>

#include "cascade/core/platform.hpp"
#include "cascade/proto/feed.hpp"
#include "cascade/proto/symbol.hpp"

namespace cascade::feed {

/// A decoded, normalised market-data event: the internal currency of the plant.
///
/// The feed handler decodes the venue's big-endian wire format into this once, and
/// every tier downstream works on it without re-parsing. It is deliberately one flat
/// POD rather than a variant or a class hierarchy: it crosses an SPSC ring by value,
/// so it must be trivially copyable, and a virtual dispatch per message at tens of
/// millions of messages a second is not affordable.
///
/// The fields are packed to **exactly one 64-byte cache line**. That is worth the
/// small ugliness of `aux_id` doing double duty — a Replace's new order id and an
/// Execute's match id never coexist — because it means moving an event through the
/// ring touches one line instead of two, halving the memory traffic on the hottest
/// path in the system.
struct FeedEvent {
  std::uint64_t sequence{0};      ///< Feed sequence number this event arrived under.
  std::uint64_t exchange_ns{0};   ///< Venue's own timestamp.
  std::uint64_t ingest_ns{0};     ///< When we took it off the wire; the latency origin.
  std::uint64_t order_id{0};
  std::uint64_t symbol{0};        ///< Packed `Symbol`; 0 when the message references
                                  ///< an order by id and the symbol must be looked up.
  std::uint64_t aux_id{0};        ///< Replace: the new order id. Execute/Trade: match id.
  Price price{0};
  std::uint32_t quantity{0};
  std::uint8_t type{0};           ///< A `proto::MsgType`.
  std::uint8_t side{0};           ///< A `Side`.
  std::uint8_t flags{0};
  std::uint8_t event_code{0};     ///< System-event code, for `kSystemEvent`.

  proto::MsgType msg_type() const noexcept {
    return static_cast<proto::MsgType>(type);
  }
  Side order_side() const noexcept { return static_cast<Side>(side); }
  Symbol packed_symbol() const noexcept { return Symbol(symbol); }
};

static_assert(sizeof(FeedEvent) == 64,
              "FeedEvent must stay exactly one cache line: it is copied per message "
              "through the hot-path ring");
static_assert(std::is_trivially_copyable<FeedEvent>::value,
              "FeedEvent crosses a lock-free ring by value");

/// Flags set by the feed handler to tell downstream tiers how much to trust an event.
enum FeedEventFlags : std::uint8_t {
  kEventRecovered = 0x01,  ///< Arrived via retransmit rather than the live multicast.
  kEventGapBefore = 0x02,  ///< A gap preceded this event and was not recovered; any
                           ///< book built from here on is not known to be correct.
  kEventReordered = 0x04,  ///< Arrived out of order and was held in the reorder window.
};

/// An execution, as published to subscribers. Kept separate from `FeedEvent` because
/// trades are the one thing in the system that may never be conflated: two trades are
/// two distinct facts, and collapsing them would fabricate a tape that never happened.
struct TradeEvent {
  std::uint64_t symbol{0};
  std::uint64_t match_id{0};
  std::uint64_t exchange_ns{0};
  std::uint64_t ingest_ns{0};
  Price price{0};
  std::uint32_t quantity{0};
  std::uint8_t aggressor_side{0};
  std::uint8_t flags{0};
  std::uint8_t pad_[2]{};
};
static_assert(std::is_trivially_copyable<TradeEvent>::value, "must cross a ring by value");

}  // namespace cascade::feed
