// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <vector>

#include "cascade/book/order_book.hpp"
#include "cascade/core/dirty_set.hpp"
#include "cascade/core/flat_hash_map.hpp"
#include "cascade/core/seqlock.hpp"
#include "cascade/core/spsc_ring.hpp"
#include "cascade/dist/publication_log.hpp"
#include "cascade/feed/event.hpp"

namespace cascade::book {

/// Capacity of a shard's per-fan-out trade ring.
///
/// Trades cannot be conflated, so unlike book updates they genuinely can back up. The
/// ring is sized for a burst — an opening auction print, a halt release — and when it
/// does overflow the answer is to evict the subscriber, not to drop a print. A feed
/// that quietly loses trades is worse than one that disconnects you.
inline constexpr std::size_t kTradeRingCapacity = 8192;
using TradeRing = SpscRing<feed::TradeEvent, kTradeRingCapacity>;

/// The maximum number of fan-out threads a shard can feed, fixed by the width of the
/// per-instrument interest mask. 32 fan-out threads is far past the point where the
/// NIC, not the CPU, is the constraint.
inline constexpr std::uint32_t kMaxFanoutThreads = 32;

/// One shard of the book-building tier.
///
/// The central design decision is that **a shard is single-threaded and owns its state
/// outright.** Instruments are partitioned across shards by a hash of the symbol, and
/// exactly one thread ever mutates a given book. That means the order map, the ladders
/// and the level vectors need no locks, no atomics and no defensive copying: the
/// hottest code in the system is ordinary single-threaded C++.
///
/// Concurrency is pushed entirely to the boundaries:
///   * Readers get published state through a per-instrument seqlock, so a fan-out
///     thread reading a book can never block the shard writing it.
///   * Change notification goes through a dirty bitmap, where duplicate marks collapse,
///     so a slow reader can never apply backpressure to the market-data path.
///   * Trades go through per-reader SPSC rings, because they must not be collapsed.
///
/// Nothing on the apply path allocates. All memory is reserved at construction, so a
/// burst cannot trigger a malloc and a page fault in the middle of the open.
class BookShard {
 public:
  struct Config {
    std::uint32_t shard_id{0};
    std::size_t max_symbols{4096};
    std::size_t initial_orders{1u << 16};
  };

  struct Stats {
    std::uint64_t events_applied{0};
    std::uint64_t books_published{0};
    std::uint64_t publishes_suppressed{0};  ///< Changed below the visible depth.
    std::uint64_t trades{0};
    std::uint64_t trades_dropped{0};        ///< Fan-out trade ring was full.
    std::uint64_t unknown_order{0};         ///< Referenced an order we never saw.
    std::uint64_t unknown_symbol{0};        ///< Not in the security master.
    std::uint64_t book_corrupt{0};          ///< Ladder rejected a removal.
    std::uint64_t symbols{0};
  };

  explicit BookShard(Config config)
      : config_(config),
        symbol_to_index_(0, config.max_symbols * 2),
        orders_(0, config.initial_orders) {
    if (config.max_symbols == 0) throw std::invalid_argument("max_symbols must be positive");
    books_.reserve(config.max_symbols);
    last_published_.reserve(config.max_symbols);
    // Seqlock cells and interest masks hold atomics, which are neither copyable nor
    // movable, so they are allocated once up front rather than grown on demand. A
    // shard's instrument capacity is a startup decision in any case.
    cells_ = std::make_unique<SeqlockCell<BookImage>[]>(config.max_symbols);
    quote_interest_ = std::make_unique<std::atomic<std::uint32_t>[]>(config.max_symbols);
    trade_interest_ = std::make_unique<std::atomic<std::uint32_t>[]>(config.max_symbols);
    for (std::size_t i = 0; i < config.max_symbols; ++i) {
      quote_interest_[i].store(0, std::memory_order_relaxed);
      trade_interest_[i].store(0, std::memory_order_relaxed);
    }
  }

  std::uint32_t shard_id() const noexcept { return config_.shard_id; }
  const Stats& stats() const noexcept { return stats_; }
  std::size_t symbol_count() const noexcept { return books_.size(); }

  /// Which shard owns an instrument. Hashing rather than range-partitioning means a
  /// single hot instrument cannot pull a whole contiguous block of the alphabet onto
  /// one core, which is exactly what happens when you partition by first letter.
  ///
  /// **A shard is a feed channel, not an arbitrary slice of instruments.** This is the
  /// constraint the whole partitioning scheme has to respect, and it comes from the
  /// wire format: an order-by-order feed identifies orders by id alone. A Delete
  /// carries an order id and nothing else — no symbol — so the only way to know which
  /// book it touches is to already hold that order, which means the Add and the Delete
  /// must land on the same shard.
  ///
  /// There is no hash of an order id that can guarantee that, so routing by instrument
  /// after the fact is impossible. Venues solve it by partitioning their multicast
  /// groups by symbol range, so every message for an instrument — and every message for
  /// its orders — arrives on one channel. The plant mirrors that: one feed handler and
  /// one shard per channel, and an order's whole lifecycle stays on the thread that
  /// owns it. `shard_for` is therefore used to *assign* instruments to channels at
  /// startup, never to route a message that has already arrived.
  static std::uint32_t shard_for(Symbol symbol, std::uint32_t shard_count) noexcept {
    return static_cast<std::uint32_t>(symbol.hash() % shard_count);
  }

  // --- instrument registry -------------------------------------------------

  /// Register an instrument. **Startup only**, before any feed traffic is applied.
  ///
  /// Registration is deliberately not something the feed path does on first sight of a
  /// symbol. Doing it lazily would mean the shard thread mutating (and rehashing) the
  /// symbol map while fan-out threads are reading it to resolve subscriptions -- a data
  /// race on the single structure both tiers need. Venues publish a security master
  /// before the open precisely so consumers can build this table up front, so the plant
  /// does the same: the map is written once and is read-only for the whole session.
  ///
  /// An instrument that appears on the feed without being registered is counted and its
  /// events dropped, rather than being admitted and quietly racing the fan-out.
  std::uint32_t register_symbol(Symbol symbol) {
    if (const std::uint32_t* existing = symbol_to_index_.find(symbol.raw())) return *existing;
    if (books_.size() >= config_.max_symbols) {
      throw std::runtime_error("shard instrument capacity exceeded");
    }
    const std::uint32_t index = static_cast<std::uint32_t>(books_.size());
    books_.emplace_back(symbol);
    last_published_.emplace_back();
    last_published_.back().symbol = symbol.raw();
    symbol_to_index_.insert_or_assign(symbol.raw(), index);
    stats_.symbols = books_.size();
    return index;
  }

  /// Index for an instrument, or `kNoIndex` if it is not registered. Const, so the
  /// fan-out can resolve a subscription without mutating shard state.
  static constexpr std::uint32_t kNoIndex = UINT32_MAX;
  std::uint32_t lookup(Symbol symbol) const {
    const std::uint32_t* found = symbol_to_index_.find(symbol.raw());
    return found ? *found : kNoIndex;
  }

  /// Published state for an instrument. The fan-out reads through this and is never
  /// blocked by the shard writing it.
  const SeqlockCell<BookImage>& published(std::uint32_t index) const noexcept {
    return cells_[index];
  }

  const OrderBook& book(std::uint32_t index) const noexcept { return books_[index]; }

  // --- fan-out wiring ------------------------------------------------------

  /// Attach the publication log that serves un-conflated subscribers.
  void set_publication_log(dist::PublicationLog* log) noexcept { log_ = log; }
  dist::PublicationLog* publication_log() const noexcept { return log_; }

  /// Register a fan-out thread's dirty set and trade ring with this shard.
  void attach_fanout(std::uint32_t fanout_id, DirtySet* dirty, TradeRing* trades) {
    if (fanout_id >= kMaxFanoutThreads) throw std::invalid_argument("fanout_id too large");
    if (fanouts_.size() <= fanout_id) fanouts_.resize(fanout_id + 1);
    fanouts_[fanout_id] = Fanout{dirty, trades};
  }

  /// Declare that a fan-out thread does or does not have a subscriber for an
  /// instrument. Interest is tracked per instrument so that an instrument nobody is
  /// watching costs nothing beyond building its book — no dirty marks, no wake-ups,
  /// no trade-ring writes. On a venue with 10,000 listings and a client watching 50,
  /// this is the difference between the fan-out tier being idle and being saturated.
  void set_quote_interest(std::uint32_t index, std::uint32_t fanout_id, bool interested) {
    update_mask(quote_interest_[index], fanout_id, interested);
  }
  void set_trade_interest(std::uint32_t index, std::uint32_t fanout_id, bool interested) {
    update_mask(trade_interest_[index], fanout_id, interested);
  }

  // --- the hot path --------------------------------------------------------

  /// Apply one decoded feed event. Called only from the shard's own thread.
  void apply(const feed::FeedEvent& event) {
    ++stats_.events_applied;
    switch (event.msg_type()) {
      case proto::MsgType::kAddOrder:    apply_add(event); break;
      case proto::MsgType::kDelete:      apply_delete(event); break;
      case proto::MsgType::kCancel:      apply_reduce(event, false); break;
      case proto::MsgType::kExecute:     apply_reduce(event, true); break;
      case proto::MsgType::kReplace:     apply_replace(event); break;
      case proto::MsgType::kTrade:       apply_trade(event); break;
      case proto::MsgType::kSystemEvent: apply_system_event(event); break;
    }
  }

  /// Discard an instrument's book and mark it stale.
  ///
  /// Called when a feed gap could not be recovered. The book is built from a stream of
  /// deltas, so a single lost message leaves it permanently and undetectably wrong —
  /// there is no way to "catch up" except to throw it away and rebuild from the next
  /// clean image. Publishing the empty, flagged book is deliberate: subscribers must
  /// be told their prices are no longer trustworthy, immediately, rather than being
  /// left holding a stale book that still looks live.
  void reset_symbol(std::uint32_t index, std::uint64_t now_ns) {
    books_[index].clear();
    books_[index].set_flags(proto::kBookStale);
    publish(index, now_ns, now_ns, /*force=*/true);
  }

  void clear_stale(std::uint32_t index, std::uint64_t now_ns) {
    books_[index].set_flags(0);
    publish(index, now_ns, now_ns, /*force=*/true);
  }

  /// Mark every instrument in this shard stale. Used when the whole channel gaps.
  void mark_all_stale(std::uint64_t now_ns) {
    for (std::uint32_t i = 0; i < books_.size(); ++i) reset_symbol(i, now_ns);
  }

 private:
  struct Fanout {
    DirtySet* dirty{nullptr};
    TradeRing* trades{nullptr};
  };

  /// What we remember about a live order.
  ///
  /// Delete, Cancel and Execute messages carry nothing but an order id — the venue
  /// assumes the consumer knows the rest. Caching `book_index` alongside the price and
  /// size means those messages, the majority of the feed, resolve with a single hash
  /// lookup instead of one for the order and a second for its instrument.
  struct OrderEntry {
    std::uint32_t book_index{0};
    std::uint32_t quantity{0};
    Price price{0};
    std::uint8_t side{0};
    std::uint8_t pad_[7]{};
  };

  static void update_mask(std::atomic<std::uint32_t>& mask, std::uint32_t fanout_id,
                          bool interested) {
    const std::uint32_t bit = 1u << fanout_id;
    if (interested) mask.fetch_or(bit, std::memory_order_release);
    else mask.fetch_and(~bit, std::memory_order_release);
  }

  void apply_add(const feed::FeedEvent& event) {
    const std::uint32_t index = lookup(event.packed_symbol());
    if (index == kNoIndex) { ++stats_.unknown_symbol; return; }
    books_[index].add(event.order_side(), event.price, event.quantity);
    OrderEntry entry;
    entry.book_index = index;
    entry.quantity = event.quantity;
    entry.price = event.price;
    entry.side = event.side;
    orders_.insert_or_assign(event.order_id, entry);
    publish(index, event.exchange_ns, event.ingest_ns);
  }

  void apply_delete(const feed::FeedEvent& event) {
    const OrderEntry* entry = orders_.find(event.order_id);
    if (!entry) { ++stats_.unknown_order; return; }
    const OrderEntry copy = *entry;
    if (!books_[copy.book_index].remove(static_cast<Side>(copy.side), copy.price,
                                        copy.quantity)) {
      ++stats_.book_corrupt;
    }
    orders_.erase(event.order_id);
    publish(copy.book_index, event.exchange_ns, event.ingest_ns);
  }

  /// A partial cancel or a partial fill. Both reduce a resting order; a fill also
  /// prints to the tape.
  void apply_reduce(const feed::FeedEvent& event, bool is_execution) {
    OrderEntry* entry = orders_.find(event.order_id);
    if (!entry) { ++stats_.unknown_order; return; }

    const std::uint32_t taken =
        event.quantity < entry->quantity ? event.quantity : entry->quantity;
    const std::uint32_t book_index = entry->book_index;
    const Price price = entry->price;
    const std::uint8_t side = entry->side;
    const bool fully_consumed = (taken >= entry->quantity);

    if (is_execution) emit_trade(book_index, event, price, taken, side);

    // A reduction that takes the whole order is a removal, not a resize: the order is
    // no longer resting and its slot in the level's order count has to go with it.
    // Treating it as a plain size reduction would leave a phantom order behind and
    // slowly inflate every level's order count.
    if (fully_consumed) {
      if (!books_[book_index].remove(static_cast<Side>(side), price, taken)) {
        ++stats_.book_corrupt;
      }
      orders_.erase(event.order_id);
    } else {
      entry->quantity -= taken;
      if (!books_[book_index].reduce(static_cast<Side>(side), price, taken)) {
        ++stats_.book_corrupt;
      }
    }
    publish(book_index, event.exchange_ns, event.ingest_ns);
  }

  /// Cancel-replace. The venue mints a new order id because a replaced order loses its
  /// place in the queue, so this is genuinely a delete followed by an add, not an
  /// in-place edit — and modelling it as one would silently preserve queue priority
  /// that the order no longer has.
  void apply_replace(const feed::FeedEvent& event) {
    const OrderEntry* existing = orders_.find(event.order_id);
    if (!existing) { ++stats_.unknown_order; return; }
    const OrderEntry old_entry = *existing;

    if (!books_[old_entry.book_index].remove(static_cast<Side>(old_entry.side),
                                             old_entry.price, old_entry.quantity)) {
      ++stats_.book_corrupt;
    }
    orders_.erase(event.order_id);

    books_[old_entry.book_index].add(static_cast<Side>(old_entry.side), event.price,
                                     event.quantity);
    OrderEntry replacement;
    replacement.book_index = old_entry.book_index;
    replacement.quantity = event.quantity;
    replacement.price = event.price;
    replacement.side = old_entry.side;
    orders_.insert_or_assign(event.aux_id, replacement);

    publish(old_entry.book_index, event.exchange_ns, event.ingest_ns);
  }

  /// A non-displayable execution: it prints to the tape but never rested in the book,
  /// so there is nothing to remove.
  void apply_trade(const feed::FeedEvent& event) {
    const std::uint32_t index = lookup(event.packed_symbol());
    if (index == kNoIndex) { ++stats_.unknown_symbol; return; }
    emit_trade(index, event, event.price, event.quantity, event.side);
  }

  void apply_system_event(const feed::FeedEvent& event) {
    // 'M' (market close) and 'C' (end of messages) both mean no further quotes are
    // authoritative, so books are flagged rather than left looking live.
    if (event.event_code == 'M' || event.event_code == 'C') {
      for (std::uint32_t i = 0; i < books_.size(); ++i) {
        books_[i].set_flags(proto::kBookHalted);
        publish(i, event.exchange_ns, event.ingest_ns, /*force=*/true);
      }
    }
  }

  void emit_trade(std::uint32_t index, const feed::FeedEvent& event, Price price,
                  std::uint32_t quantity, std::uint8_t side) {
    ++stats_.trades;
    std::uint32_t mask = trade_interest_[index].load(std::memory_order_acquire);
    if (!mask) return;  // nobody is watching the tape for this instrument

    feed::TradeEvent trade;
    trade.symbol = books_[index].symbol().raw();
    trade.book_index = index;
    trade.match_id = event.aux_id;
    trade.exchange_ns = event.exchange_ns;
    trade.ingest_ns = event.ingest_ns;
    trade.price = price;
    trade.quantity = quantity;
    trade.aggressor_side = side;
    trade.flags = event.flags;

    while (mask) {
      const std::uint32_t fanout_id = static_cast<std::uint32_t>(__builtin_ctz(mask));
      mask &= mask - 1;
      if (fanout_id >= fanouts_.size()) continue;
      TradeRing* ring = fanouts_[fanout_id].trades;
      // A full ring means that fan-out thread is not keeping up with the tape. We do
      // not block the market-data path for it; the fan-out notices the drop and evicts
      // the subscriber, which is the only honest outcome for un-conflatable data.
      if (ring && !ring->try_push(trade)) ++stats_.trades_dropped;
    }
  }

  /// Render the book and publish it if — and only if — a subscriber could tell.
  void publish(std::uint32_t index, std::uint64_t exchange_ns, std::uint64_t ingest_ns,
               bool force = false) {
    BookImage& previous = last_published_[index];
    books_[index].snapshot(staging_, exchange_ns, ingest_ns);

    if (!force && !visible_change(previous, staging_)) {
      ++stats_.publishes_suppressed;
      return;
    }

    staging_.version = previous.version + 1;
    previous = staging_;
    cells_[index].store(staging_);
    // Only subscribers asking for every state need the log, and maintaining it costs a
    // full image copy per publish. Checking first means a plant whose subscribers are
    // all conflated -- the common case -- pays nothing for the feature.
    if (log_ && log_->enabled()) log_->append(staging_);
    ++stats_.books_published;

    std::uint32_t mask = quote_interest_[index].load(std::memory_order_acquire);
    while (mask) {
      const std::uint32_t fanout_id = static_cast<std::uint32_t>(__builtin_ctz(mask));
      mask &= mask - 1;
      if (fanout_id < fanouts_.size() && fanouts_[fanout_id].dirty) {
        fanouts_[fanout_id].dirty->mark(index);
      }
    }
  }

  Config config_;
  FlatHashMap<std::uint64_t, std::uint32_t, SymbolRawHash> symbol_to_index_;
  FlatHashMap<std::uint64_t, OrderEntry, OrderIdHash> orders_;
  std::vector<OrderBook> books_;
  std::vector<BookImage> last_published_;
  std::unique_ptr<SeqlockCell<BookImage>[]> cells_;
  std::unique_ptr<std::atomic<std::uint32_t>[]> quote_interest_;
  std::unique_ptr<std::atomic<std::uint32_t>[]> trade_interest_;
  std::vector<Fanout> fanouts_;
  dist::PublicationLog* log_{nullptr};
  BookImage staging_;  ///< Reused across publishes so the hot path never allocates.
  Stats stats_;
};

}  // namespace cascade::book
