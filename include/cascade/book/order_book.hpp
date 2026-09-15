// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <vector>

#include "cascade/core/platform.hpp"
#include "cascade/proto/client.hpp"
#include "cascade/proto/symbol.hpp"

namespace cascade::book {

/// One aggregated price level.
struct BookLevel {
  Price price{0};
  std::uint64_t quantity{0};   ///< 64-bit: aggregate size at a level can exceed 2^32.
  std::uint32_t order_count{0};
};

/// One side of a book, as a price ladder.
///
/// Structure choice is the whole story here. A `std::map<Price, Level>` is the obvious
/// answer and the wrong one: every level is a separately allocated red-black node, so
/// walking the top ten levels — which happens on every single publish — is ten
/// dependent cache misses through scattered memory.
///
/// A flat sorted vector puts the whole ladder in contiguous memory, so the top of book
/// is one or two cache lines and the binary search prefetches well. The cost is that
/// inserting a level in the middle memmoves the tail, but that memmove is a streaming
/// copy of 16-byte PODs and beats a tree's pointer chasing until the book is far deeper
/// than any real instrument.
///
/// The ordering is the non-obvious part: levels are stored **worst-first, best-last**.
/// Market data is overwhelmingly concentrated at the top of book — new best prices
/// appear and are consumed constantly — and with the best price at `back()` those
/// become `push_back`/`pop_back`, which move nothing at all. Storing best-first would
/// memmove the entire ladder on exactly the most frequent operation.
template <bool IsBid>
class Ladder {
 public:
  /// Is `a` a more aggressive price than `b`? Higher is better for bids, lower for asks.
  static constexpr bool better(Price a, Price b) noexcept {
    return IsBid ? (a > b) : (a < b);
  }

  void reserve(std::size_t levels) { levels_.reserve(levels); }

  bool empty() const noexcept { return levels_.empty(); }
  std::size_t depth() const noexcept { return levels_.size(); }

  /// Top of book, or nullptr if this side is empty.
  CASCADE_ALWAYS_INLINE const BookLevel* best() const noexcept {
    return levels_.empty() ? nullptr : &levels_.back();
  }

  Price best_price() const noexcept {
    return levels_.empty() ? kNoPrice : levels_.back().price;
  }

  /// Add resting quantity at a price, creating the level if it is new.
  void add(Price price, std::uint64_t quantity, std::uint32_t orders = 1) {
    // Fast path: a new order at or improving the current best. This is the modal case
    // on a live feed, and it costs one comparison and an amortised O(1) push_back.
    if (!levels_.empty()) {
      BookLevel& top = levels_.back();
      if (top.price == price) {
        top.quantity += quantity;
        top.order_count += orders;
        return;
      }
      if (better(price, top.price)) {
        levels_.push_back(BookLevel{price, quantity, orders});
        return;
      }
    } else {
      levels_.push_back(BookLevel{price, quantity, orders});
      return;
    }

    const auto it = lower_bound_for(price);
    if (it != levels_.end() && it->price == price) {
      it->quantity += quantity;
      it->order_count += orders;
    } else {
      levels_.insert(it, BookLevel{price, quantity, orders});
    }
  }

  /// Remove resting quantity. Returns false if the level is unknown or would go
  /// negative, which on a live feed means we have lost a message and the book is
  /// corrupt — the caller escalates that to a gap/resync rather than clamping silently.
  bool remove(Price price, std::uint64_t quantity, std::uint32_t orders = 1) {
    const auto it = lower_bound_for(price);
    if (it == levels_.end() || it->price != price) return false;
    if (it->quantity < quantity || it->order_count < orders) return false;

    it->quantity -= quantity;
    it->order_count -= orders;

    // A level is empty exactly when no orders rest at it. "No orders but non-zero
    // size", or the reverse, cannot happen on a well-formed feed; if it does we have
    // silently lost or double-applied a message. Drop the level so we never quote a
    // phantom price, and report failure so the caller can resync rather than trade
    // off a book we no longer trust.
    const bool no_orders = (it->order_count == 0);
    const bool no_quantity = (it->quantity == 0);
    if (no_orders != no_quantity) {
      erase_at(it);
      return false;
    }
    if (no_orders) erase_at(it);
    return true;
  }

  const BookLevel* find(Price price) const {
    const auto it = const_cast<Ladder*>(this)->lower_bound_for(price);
    if (it == levels_.end() || it->price != price) return nullptr;
    return &*it;
  }

  /// Copy the best `max_levels` into a wire-format array, best first.
  std::uint8_t copy_top(proto::PriceLevel* out, std::uint8_t max_levels) const noexcept {
    const std::size_t available = levels_.size();
    const std::size_t take = available < max_levels ? available : max_levels;
    for (std::size_t i = 0; i < take; ++i) {
      const BookLevel& level = levels_[available - 1 - i];  // walk best -> worse
      out[i].price = level.price;
      out[i].quantity = level.quantity > UINT32_MAX
                            ? UINT32_MAX
                            : static_cast<std::uint32_t>(level.quantity);
      out[i].order_count = level.order_count;
    }
    return static_cast<std::uint8_t>(take);
  }

  void clear() noexcept { levels_.clear(); }

  /// Total resting quantity across the whole side. Diagnostics only — it is O(depth)
  /// and must not be called from the hot path.
  std::uint64_t total_quantity() const noexcept {
    std::uint64_t total = 0;
    for (const BookLevel& level : levels_) total += level.quantity;
    return total;
  }

  /// Levels ordered worst-first. Exposed for tests and for the archive writer.
  const std::vector<BookLevel>& raw_levels() const noexcept { return levels_; }

 private:
  void erase_at(std::vector<BookLevel>::iterator it) noexcept {
    // Top of book is the modal case (an order at the best price being fully consumed),
    // and popping the back moves nothing.
    if (it + 1 == levels_.end()) levels_.pop_back();
    else levels_.erase(it);
  }

  std::vector<BookLevel>::iterator lower_bound_for(Price price) noexcept {
    // The ladder is sorted increasing in "betterness", so the levels worse than
    // `price` form a prefix and this is a textbook partition point.
    return std::lower_bound(levels_.begin(), levels_.end(), price,
                            [](const BookLevel& level, Price target) {
                              return better(target, level.price);
                            });
  }

  std::vector<BookLevel> levels_;
};

using BidLadder = Ladder<true>;
using AskLadder = Ladder<false>;

/// A trivially-copyable book image: what a shard publishes and what the fan-out ships.
///
/// It is a flat POD with fixed-size level arrays rather than anything heap-backed,
/// because it has to cross a seqlock (which memcpys it) and then a socket (which
/// writes it) without either step allocating or chasing a pointer.
struct BookImage {
  std::uint64_t symbol{0};
  std::uint64_t version{0};      ///< Monotonic; increments only on a *visible* change.
  std::uint64_t exchange_ns{0};
  std::uint64_t ingest_ns{0};
  std::uint64_t update_count{0}; ///< Total events applied, visible or not.
  std::uint8_t bid_levels{0};
  std::uint8_t ask_levels{0};
  std::uint8_t flags{0};
  std::uint8_t pad_[5]{};
  proto::PriceLevel bids[proto::kMaxDepth]{};
  proto::PriceLevel asks[proto::kMaxDepth]{};

  Price best_bid() const noexcept { return bid_levels ? bids[0].price : kNoPrice; }
  Price best_ask() const noexcept { return ask_levels ? asks[0].price : kNoPrice; }
  bool crossed() const noexcept {
    return bid_levels && ask_levels && bids[0].price >= asks[0].price;
  }
  /// Midpoint, or `kNoPrice` if either side is empty. The most-requested derived value
  /// in market data, so it lives on the image rather than being recomputed downstream.
  Price mid() const noexcept {
    if (!bid_levels || !ask_levels) return kNoPrice;
    return (bids[0].price + asks[0].price) / 2;
  }
};
static_assert(std::is_trivially_copyable<BookImage>::value,
              "BookImage crosses a seqlock and must be memcpy-able");

/// A single instrument's book, driven by an order-by-order feed.
///
/// The book itself holds only aggregated levels. Individual order state lives in the
/// shard's order map, because a Delete or Execute message carries nothing but an order
/// id — the price and size it is removing have to be recovered from that map.
class OrderBook {
 public:
  explicit OrderBook(Symbol symbol = Symbol()) : symbol_(symbol) {
    bids_.reserve(64);
    asks_.reserve(64);
  }

  Symbol symbol() const noexcept { return symbol_; }
  void set_symbol(Symbol symbol) noexcept { symbol_ = symbol; }

  const BidLadder& bids() const noexcept { return bids_; }
  const AskLadder& asks() const noexcept { return asks_; }

  void add(Side side, Price price, std::uint64_t quantity) {
    if (side == Side::kBuy) bids_.add(price, quantity);
    else asks_.add(price, quantity);
    ++update_count_;
  }

  /// Reduce resting size without removing the order (a partial cancel or a partial fill).
  bool reduce(Side side, Price price, std::uint64_t quantity) {
    const bool ok = side == Side::kBuy ? bids_.remove(price, quantity, 0)
                                       : asks_.remove(price, quantity, 0);
    ++update_count_;
    return ok;
  }

  /// Remove an order entirely: its remaining size and its slot in the order count.
  bool remove(Side side, Price price, std::uint64_t quantity) {
    const bool ok = side == Side::kBuy ? bids_.remove(price, quantity, 1)
                                       : asks_.remove(price, quantity, 1);
    ++update_count_;
    return ok;
  }

  bool empty() const noexcept { return bids_.empty() && asks_.empty(); }

  void clear() noexcept {
    bids_.clear();
    asks_.clear();
    ++update_count_;
  }

  std::uint64_t update_count() const noexcept { return update_count_; }

  void set_flags(std::uint8_t flags) noexcept { flags_ = flags; }
  std::uint8_t flags() const noexcept { return flags_; }

  /// Render the current top of book into `image`.
  void snapshot(BookImage& image, std::uint64_t exchange_ns,
                std::uint64_t ingest_ns) const noexcept {
    image.symbol = symbol_.raw();
    image.exchange_ns = exchange_ns;
    image.ingest_ns = ingest_ns;
    image.update_count = update_count_;
    image.bid_levels = bids_.copy_top(image.bids, proto::kMaxDepth);
    image.ask_levels = asks_.copy_top(image.asks, proto::kMaxDepth);
    image.flags = flags_;
    if (image.crossed()) image.flags |= proto::kBookCrossed;
    // Zero the unused tail so that two images with identical visible state compare
    // equal byte-for-byte — which is what the change filter below depends on.
    for (std::uint8_t i = image.bid_levels; i < proto::kMaxDepth; ++i)
      image.bids[i] = proto::PriceLevel{kNoPrice, 0, 0};
    for (std::uint8_t i = image.ask_levels; i < proto::kMaxDepth; ++i)
      image.asks[i] = proto::PriceLevel{kNoPrice, 0, 0};
  }

 private:
  Symbol symbol_;
  BidLadder bids_;
  AskLadder asks_;
  std::uint64_t update_count_{0};
  std::uint8_t flags_{0};
};

/// Do two images differ in anything a subscriber can see?
///
/// This is the single highest-leverage filter in the plant. On a real venue the large
/// majority of order events happen deep in the book and change nothing within the top
/// ten levels — an order added 40 levels down, or cancelled, is invisible to every
/// subscriber. Testing for that before publishing means those events cost one memcmp
/// and then stop dead, instead of propagating a seqlock write, a fan-out wake-up, a
/// conflation-slot update and a socket write per subscriber.
CASCADE_ALWAYS_INLINE bool visible_change(const BookImage& a, const BookImage& b) noexcept {
  if (a.bid_levels != b.bid_levels || a.ask_levels != b.ask_levels) return true;
  if (a.flags != b.flags) return true;
  const std::size_t bid_bytes = a.bid_levels * sizeof(proto::PriceLevel);
  const std::size_t ask_bytes = a.ask_levels * sizeof(proto::PriceLevel);
  return std::memcmp(a.bids, b.bids, bid_bytes) != 0 ||
         std::memcmp(a.asks, b.asks, ask_bytes) != 0;
}

}  // namespace cascade::book
