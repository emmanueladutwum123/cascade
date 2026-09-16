// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "cascade/book/order_book.hpp"
#include "cascade/core/flat_hash_map.hpp"
#include "cascade/dist/entitlements.hpp"
#include "cascade/feed/event.hpp"
#include "cascade/net/output_buffer.hpp"
#include "cascade/proto/client.hpp"

namespace cascade::dist {

/// Per-instrument state for one subscriber.
struct Subscription {
  Symbol symbol;
  std::uint32_t shard_id{0};
  std::uint32_t book_index{0};
  std::uint8_t flags{proto::kFlagConflated};
  VenueId venue{kUnknownVenue};

  /// Highest version this subscriber has actually been sent. Together with the
  /// version being sent now, this *is* the conflation count -- versions are monotonic
  /// per instrument and increment only on a visible change, so the gap between them is
  /// exactly the number of book states the client never saw. Deriving it beats keeping
  /// a running tally, which double-counts every retry of the same superseded state.
  std::uint64_t last_sent_version{0};
  /// Waiting to be sent: the book moved but the socket had no room.
  bool pending{false};
  bool active{true};
};

/// One connected subscriber: its subscriptions, its output buffer, and the policy that
/// decides what it is sent and when it is disconnected.
///
/// The central idea is that **a subscriber that cannot keep up must degrade, not
/// propagate**. Nothing a slow client does may reach back into the book-building path.
/// There are exactly two ways to honour that, and which one applies depends on what
/// kind of data is backed up:
///
///   * **Quotes are states.** If three updates to one instrument pile up behind a full
///     socket, sending all three is pointless — only the last describes the market. So
///     the pending update is a *flag*, not a queue entry, and when the socket drains we
///     re-read the instrument's current image and send that. Memory is bounded by the
///     number of subscriptions, not by how far behind the client is, and the client
///     gets the freshest possible data rather than a backlog of history.
///
///   * **Trades are events.** Two prints are two facts; collapsing them would invent a
///     tape that never happened. They are queued, the queue is bounded, and a client
///     that overruns it is disconnected. Disconnecting is the honest outcome: a
///     subscriber holding a silently incomplete tape will compute wrong volumes and
///     wrong VWAPs and never know it.
///
/// Slow-consumer eviction is time-based, not depth-based. A momentarily full socket is
/// completely normal — a scheduler blip, an opening burst — and evicting on depth alone
/// would disconnect healthy clients every morning. What is not normal is a socket that
/// stays full, because that means the client is structurally too slow and no amount of
/// waiting will fix it.
class Subscriber {
 public:
  struct Config {
    std::size_t output_capacity{1u << 20};   ///< 1MB of pending socket bytes.
    std::size_t trade_queue_capacity{4096};
    /// How long the output buffer may stay full before the client is disconnected.
    std::uint64_t stall_deadline_ns{2'000'000'000};  // 2s
    std::uint32_t max_subscriptions{8192};
  };

  struct Stats {
    std::uint64_t book_updates_sent{0};
    /// Book states the client provably never saw, summed exactly from version gaps.
    std::uint64_t book_updates_conflated{0};
    /// Times an update could not be written because the socket was backed up. Distinct
    /// from the above: this counts attempts, that counts information actually skipped.
    std::uint64_t offers_deferred{0};
    std::uint64_t trades_sent{0};
    std::uint64_t trades_queued{0};
    std::uint64_t bytes_sent{0};
    std::uint64_t flushes{0};
    std::uint64_t would_block{0};
    std::uint64_t resyncs{0};  ///< Fell out of the publication log and was re-imaged.
  };

  Subscriber(std::uint64_t session_id, Config config, net::ByteSink* sink)
      : session_id_(session_id),
        config_(config),
        output_(config.output_capacity),
        sink_(sink),
        subscription_index_(0, 1024) {
    trade_queue_.reserve(config.trade_queue_capacity);
  }

  std::uint64_t session_id() const noexcept { return session_id_; }
  const Stats& stats() const noexcept { return stats_; }
  const std::vector<Subscription>& subscriptions() const noexcept { return subscriptions_; }
  std::size_t pending_bytes() const noexcept { return output_.pending(); }
  std::size_t queued_trades() const noexcept { return trade_queue_.size(); }
  bool evicted() const noexcept { return evicted_; }
  proto::EvictReason evict_reason() const noexcept { return evict_reason_; }

  const std::string& client_id() const noexcept { return client_id_; }
  void set_client(std::string client_id, ClientEntitlement entitlement) {
    client_id_ = std::move(client_id);
    entitlement_ = std::move(entitlement);
  }
  const ClientEntitlement& entitlement() const noexcept { return entitlement_; }

  // --- subscriptions -------------------------------------------------------

  /// Record a subscription. The caller has already resolved entitlement and located
  /// the instrument; this returns the subscriber-local index.
  std::uint32_t add_subscription(Symbol symbol, std::uint32_t shard_id,
                                 std::uint32_t book_index, std::uint8_t flags,
                                 VenueId venue) {
    if (const std::uint32_t* existing = subscription_index_.find(symbol.raw())) {
      subscriptions_[*existing].active = true;
      return *existing;
    }
    const std::uint32_t index = static_cast<std::uint32_t>(subscriptions_.size());
    Subscription subscription;
    subscription.symbol = symbol;
    subscription.shard_id = shard_id;
    subscription.book_index = book_index;
    subscription.flags = flags;
    subscription.venue = venue;
    subscriptions_.push_back(subscription);
    subscription_index_.insert_or_assign(symbol.raw(), index);
    return index;
  }

  Subscription* find_subscription(Symbol symbol) {
    const std::uint32_t* index = subscription_index_.find(symbol.raw());
    return index ? &subscriptions_[*index] : nullptr;
  }

  const Subscription* subscription_at(std::uint32_t index) const {
    return index < subscriptions_.size() ? &subscriptions_[index] : nullptr;
  }

  void remove_subscription(Symbol symbol) {
    const std::uint32_t* index = subscription_index_.find(symbol.raw());
    if (!index) return;
    subscriptions_[*index].active = false;
    subscriptions_[*index].pending = false;
    subscription_index_.erase(symbol.raw());
  }

  std::size_t active_subscription_count() const noexcept {
    std::size_t count = 0;
    for (const Subscription& subscription : subscriptions_) {
      if (subscription.active) ++count;
    }
    return count;
  }

  // --- delivery ------------------------------------------------------------

  /// Offer a book image. Sends it if the socket has room; otherwise records that this
  /// instrument is owed and moves on. Never blocks, never queues a stale image.
  void offer_book(std::uint32_t subscription_index, const book::BookImage& image) {
    if (evicted_ || subscription_index >= subscriptions_.size()) return;
    Subscription& subscription = subscriptions_[subscription_index];
    if (!subscription.active) return;
    if (image.version <= subscription.last_sent_version) return;  // nothing new

    // Exactly how many states this client is about to skip over. Zero on the first
    // update, where there is no previous version to measure a gap against.
    const std::uint64_t gap =
        subscription.last_sent_version == 0
            ? 0
            : image.version - subscription.last_sent_version - 1;
    const std::uint32_t skipped =
        gap > UINT32_MAX ? UINT32_MAX : static_cast<std::uint32_t>(gap);

    if (!encode_book_update(image, skipped)) {
      // No room. Flag the instrument and let the next drain re-read its *current*
      // image: sending this one once space appears would ship a book already history.
      if (!subscription.pending) {
        subscription.pending = true;
        pending_queue_.push_back(subscription_index);
      }
      ++stats_.offers_deferred;
      return;
    }

    subscription.last_sent_version = image.version;
    subscription.pending = false;
    stats_.book_updates_conflated += skipped;
    ++stats_.book_updates_sent;
  }

  /// Offer a trade print. Trades may not be collapsed, so this queues, and a full
  /// queue is an eviction rather than a silent drop.
  void offer_trade(const feed::TradeEvent& trade) {
    if (evicted_) return;
    // Anything already queued must go first, or the tape would arrive out of order.
    if (trade_queue_.empty() && encode_trade(trade)) {
      ++stats_.trades_sent;
      return;
    }
    if (trade_queue_.size() >= config_.trade_queue_capacity) {
      evict(proto::EvictReason::kTradeQueueOverrun);
      return;
    }
    trade_queue_.push_back(trade);
    ++stats_.trades_queued;
  }

  /// Instruments whose current state this subscriber still owes. The fan-out re-reads
  /// each one's live image and offers it again.
  const std::vector<std::uint32_t>& pending_subscriptions() const noexcept {
    return pending_queue_;
  }

  /// Hand over the pending list and reset the per-instrument flags together.
  ///
  /// These two pieces of state must move as one. `offer_book` only appends to the
  /// queue when the flag is not already set -- that is what stops one instrument
  /// occupying the queue a thousand times -- so draining the queue without clearing
  /// the flags leaves every instrument marked-but-unqueued. A retry that still cannot
  /// fit would then fail to re-queue itself, and the subscriber would stop receiving
  /// that instrument for the rest of the session while looking perfectly healthy.
  void take_pending(std::vector<std::uint32_t>& out) {
    out = pending_queue_;
    pending_queue_.clear();
    for (std::uint32_t index : out) {
      if (index < subscriptions_.size()) subscriptions_[index].pending = false;
    }
  }

  void clear_pending_queue() noexcept {
    for (std::uint32_t index : pending_queue_) {
      if (index < subscriptions_.size()) subscriptions_[index].pending = false;
    }
    pending_queue_.clear();
  }

  // --- socket --------------------------------------------------------------

  /// Push whatever is buffered at the socket. Returns false if the connection is done.
  bool flush(std::uint64_t now_ns) {
    if (evicted_) return false;
    ++stats_.flushes;

    // Trades queued behind a full buffer get first claim on new space: unlike a book
    // image, a print cannot be regenerated from current state.
    while (!trade_queue_.empty()) {
      if (!encode_trade(trade_queue_.front())) break;
      trade_queue_.erase(trade_queue_.begin());
      ++stats_.trades_sent;
    }

    net::OutputBuffer::Span spans[2];
    const int span_count = output_.readable(spans);
    if (span_count == 0) {
      stalled_since_ns_ = 0;
      return true;
    }

    const long written = sink_->write_some(spans, span_count);
    if (written < 0) return false;
    if (written == 0) {
      ++stats_.would_block;
      // Start the clock on the first refusal, not on every one: what matters is how
      // long the client has been unable to keep up, not how often we noticed.
      if (stalled_since_ns_ == 0) stalled_since_ns_ = now_ns;
      if (now_ns - stalled_since_ns_ >= config_.stall_deadline_ns) {
        evict(proto::EvictReason::kSlowConsumer);
        return false;
      }
      return true;
    }

    output_.consume(static_cast<std::size_t>(written));
    stats_.bytes_sent += static_cast<std::uint64_t>(written);
    stalled_since_ns_ = 0;
    return true;
  }

  std::uint64_t stalled_nanos(std::uint64_t now_ns) const noexcept {
    return stalled_since_ns_ == 0 ? 0 : now_ns - stalled_since_ns_;
  }

  void evict(proto::EvictReason reason) {
    if (evicted_) return;
    evicted_ = true;
    evict_reason_ = reason;
  }

  // --- frame encoders ------------------------------------------------------

  bool encode_login_ack(proto::LoginStatus status, std::uint32_t max_subscriptions,
                        std::uint32_t entitled_venues) {
    proto::LoginAckMsg body{};
    body.status = static_cast<std::uint8_t>(status);
    body.server_version = proto::kClientProtocolVersion;
    body.session_id = session_id_;
    body.max_subscriptions = max_subscriptions;
    body.entitled_venues = entitled_venues;
    return emit(proto::ClientMsgType::kLoginAck, &body, sizeof(body));
  }

  bool encode_subscribe_ack(Symbol symbol, proto::SubscribeStatus status,
                            std::uint64_t start_version) {
    proto::SubscribeAckMsg body{};
    body.symbol = symbol.raw();
    body.status = static_cast<std::uint8_t>(status);
    body.start_version = start_version;
    return emit(proto::ClientMsgType::kSubscribeAck, &body, sizeof(body));
  }

  bool encode_symbol_status(Symbol symbol, proto::SymbolState state,
                            std::uint64_t version) {
    proto::SymbolStatusMsg body{};
    body.symbol = symbol.raw();
    body.state = static_cast<std::uint8_t>(state);
    body.version = version;
    return emit(proto::ClientMsgType::kSymbolStatus, &body, sizeof(body));
  }

  bool encode_heartbeat(std::uint64_t now_ns) {
    proto::HeartbeatMsg body{};
    body.server_ns = now_ns;
    body.messages_sent = stats_.book_updates_sent + stats_.trades_sent;
    body.messages_conflated = stats_.book_updates_conflated;
    return emit(proto::ClientMsgType::kHeartbeat, &body, sizeof(body));
  }

  /// Tell the client why it is being disconnected, and try hard to get the message
  /// out: a bare TCP reset leaves an operator guessing between "too slow", "idle" and
  /// "server restarted", which demand completely different fixes.
  /// `backlog_bytes` is passed in rather than read from the buffer, because the caller
  /// has usually just discarded that buffer to make room for this very message.
  bool encode_evicted(proto::EvictReason reason, std::uint64_t stalled_ns,
                      std::uint64_t backlog_bytes) {
    proto::EvictedMsg body{};
    body.reason = static_cast<std::uint8_t>(reason);
    body.backlog_bytes = backlog_bytes;
    body.stalled_nanos = stalled_ns;
    return emit(proto::ClientMsgType::kEvicted, &body, sizeof(body));
  }

  void note_resync() { ++stats_.resyncs; }

  /// Best-effort write that ignores the evicted flag.
  ///
  /// The ordinary `flush` refuses once a subscriber is evicted, which is right for
  /// market data but wrong for the eviction notice itself — the one message the client
  /// most needs. This gets that notice out on a socket that may well refuse it, and
  /// does not care if it fails.
  void flush_final() {
    net::OutputBuffer::Span spans[2];
    const int span_count = output_.readable(spans);
    if (span_count == 0) return;
    const long written = sink_->write_some(spans, span_count);
    if (written > 0) {
      output_.consume(static_cast<std::size_t>(written));
      stats_.bytes_sent += static_cast<std::uint64_t>(written);
    }
  }

  /// Discard everything buffered. Used only when an eviction notice has to be written
  /// into a buffer the client has already proven it cannot drain.
  void discard_buffered() {
    output_.clear();
    trade_queue_.clear();
  }

 private:
  bool emit(proto::ClientMsgType type, const void* body, std::size_t body_bytes) {
    proto::FrameHeader header{};
    header.payload_bytes = static_cast<std::uint16_t>(body_bytes);
    header.type = static_cast<std::uint8_t>(type);
    // All-or-nothing: check space for the whole frame before writing any of it, or a
    // refusal mid-frame would desynchronise the client's parser permanently.
    if (output_.available() < sizeof(header) + body_bytes) return false;
    output_.write(&header, sizeof(header));
    output_.write(body, body_bytes);
    return true;
  }

  bool encode_book_update(const book::BookImage& image, std::uint32_t conflated_count) {
    proto::BookUpdateMsg body{};
    body.symbol = image.symbol;
    body.version = image.version;
    body.exchange_ns = image.exchange_ns;
    body.ingest_ns = image.ingest_ns;
    body.conflated_count = conflated_count;
    body.bid_levels = image.bid_levels;
    body.ask_levels = image.ask_levels;
    body.flags = image.flags;

    const std::size_t level_bytes =
        static_cast<std::size_t>(image.bid_levels + image.ask_levels) *
        sizeof(proto::PriceLevel);
    proto::FrameHeader header{};
    header.payload_bytes = static_cast<std::uint16_t>(sizeof(body) + level_bytes);
    header.type = static_cast<std::uint8_t>(proto::ClientMsgType::kBookUpdate);

    if (output_.available() < sizeof(header) + sizeof(body) + level_bytes) return false;
    output_.write(&header, sizeof(header));
    output_.write(&body, sizeof(body));
    // Only populated levels go on the wire. A thinly quoted instrument costs a
    // fraction of a full-depth one, which adds up across a fan-out tier.
    if (image.bid_levels) {
      output_.write(image.bids, image.bid_levels * sizeof(proto::PriceLevel));
    }
    if (image.ask_levels) {
      output_.write(image.asks, image.ask_levels * sizeof(proto::PriceLevel));
    }
    return true;
  }

  bool encode_trade(const feed::TradeEvent& trade) {
    proto::TradeTickMsg body{};
    body.symbol = trade.symbol;
    body.match_id = trade.match_id;
    body.exchange_ns = trade.exchange_ns;
    body.ingest_ns = trade.ingest_ns;
    body.price = trade.price;
    body.quantity = trade.quantity;
    body.aggressor_side = trade.aggressor_side;
    return emit(proto::ClientMsgType::kTradeTick, &body, sizeof(body));
  }

  std::uint64_t session_id_;
  Config config_;
  net::OutputBuffer output_;
  net::ByteSink* sink_;

  std::string client_id_;
  ClientEntitlement entitlement_;

  std::vector<Subscription> subscriptions_;
  FlatHashMap<std::uint64_t, std::uint32_t, SymbolRawHash> subscription_index_;
  std::vector<std::uint32_t> pending_queue_;
  std::vector<feed::TradeEvent> trade_queue_;

  std::uint64_t stalled_since_ns_{0};
  bool evicted_{false};
  proto::EvictReason evict_reason_{proto::EvictReason::kServerShutdown};
  Stats stats_;
};

}  // namespace cascade::dist
