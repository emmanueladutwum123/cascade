// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "cascade/book/book_shard.hpp"
#include "cascade/core/dirty_set.hpp"
#include "cascade/dist/entitlements.hpp"
#include "cascade/dist/instrument_registry.hpp"
#include "cascade/dist/subscriber.hpp"

namespace cascade::dist {

/// One fan-out thread: it owns a set of subscribers and does all their work.
///
/// The tier scales by partitioning *subscribers* across threads, not instruments.
/// Partitioning by instrument would be the obvious mirror of the shard layout, but it
/// is wrong here: a single client subscribing to 5,000 symbols would then be written to
/// by every fan-out thread at once, and its socket and output buffer would need locking.
/// Giving each subscriber exactly one owning thread means a connection's entire state —
/// buffer, subscriptions, conflation flags, eviction timers — is single-threaded and
/// lock-free, and the only cross-thread contact is the inbound dirty set and trade ring,
/// which the shards write and this thread drains.
///
/// A pass over the tier is:
///   1. Flush, to make room in output buffers.
///   2. Drain each shard's dirty set; for each instrument that moved, read its image
///      once through the seqlock and offer it to every subscriber watching it.
///   3. Drain each shard's trade ring and route the prints.
///   4. Retry instruments that were owed from last pass, re-reading current state.
///   5. Flush again, and evict anyone who has run out of time.
///
/// Reading each dirty instrument's image *once* and fanning the copy out to N listeners
/// is the point of step 2: the seqlock read is the expensive part, and doing it per
/// subscriber would multiply it by the fan-out factor for no benefit.
class FanoutThread {
 public:
  struct Config {
    std::uint32_t fanout_id{0};
    std::size_t max_subscribers{1024};
    std::size_t dirty_capacity{4096};  ///< Must cover each shard's instrument count.
    Subscriber::Config subscriber{};
  };

  struct Stats {
    std::uint64_t passes{0};
    std::uint64_t instruments_routed{0};
    std::uint64_t trades_routed{0};
    std::uint64_t subscribers_admitted{0};
    std::uint64_t subscribers_evicted{0};
    std::uint64_t subscriptions_granted{0};
    std::uint64_t subscriptions_refused{0};
    std::uint64_t pending_retries{0};
  };

  FanoutThread(Config config, std::vector<book::BookShard*> shards,
               const InstrumentRegistry* registry, const EntitlementTable* entitlements)
      : config_(config),
        shards_(std::move(shards)),
        registry_(registry),
        entitlements_(entitlements) {
    links_.resize(shards_.size());
    for (std::size_t shard_id = 0; shard_id < shards_.size(); ++shard_id) {
      ShardLink& link = links_[shard_id];
      link.dirty = std::make_unique<DirtySet>(config_.dirty_capacity);
      link.trades = std::make_unique<book::TradeRing>();
      link.listeners.resize(config_.dirty_capacity);
      shards_[shard_id]->attach_fanout(config_.fanout_id, link.dirty.get(),
                                       link.trades.get());
    }
    subscribers_.resize(config_.max_subscribers);
  }

  std::uint32_t fanout_id() const noexcept { return config_.fanout_id; }
  const Stats& stats() const noexcept { return stats_; }
  std::size_t subscriber_count() const noexcept { return live_subscribers_; }

  // --- connection lifecycle ------------------------------------------------

  /// Take ownership of a subscriber. Returns its slot, or `kNoSlot` when full.
  static constexpr std::uint32_t kNoSlot = UINT32_MAX;
  std::uint32_t admit(std::unique_ptr<Subscriber> subscriber) {
    for (std::uint32_t slot = 0; slot < subscribers_.size(); ++slot) {
      if (subscribers_[slot]) continue;
      subscribers_[slot] = std::move(subscriber);
      ++live_subscribers_;
      ++stats_.subscribers_admitted;
      return slot;
    }
    return kNoSlot;
  }

  Subscriber* subscriber_at(std::uint32_t slot) {
    return slot < subscribers_.size() ? subscribers_[slot].get() : nullptr;
  }

  /// Drop a subscriber and every trace of it.
  ///
  /// Withdrawing interest matters as much as removing the listener: an instrument that
  /// nobody is left watching must stop marking this thread dirty, or a departed client
  /// keeps costing the shard work for the rest of the session.
  void release(std::uint32_t slot) {
    Subscriber* subscriber = subscriber_at(slot);
    if (!subscriber) return;
    for (const Subscription& subscription : subscriber->subscriptions()) {
      if (!subscription.active) continue;
      detach_listener(subscription.shard_id, subscription.book_index, slot);
    }
    subscribers_[slot].reset();
    --live_subscribers_;
  }

  // --- subscription --------------------------------------------------------

  /// Resolve, authorise and activate one subscription, acknowledging either way.
  proto::SubscribeStatus subscribe(std::uint32_t slot, Symbol symbol,
                                   std::uint8_t flags) {
    Subscriber* subscriber = subscriber_at(slot);
    if (!subscriber) return proto::SubscribeStatus::kUnknownSymbol;

    const InstrumentLocation* location = registry_ ? registry_->find(symbol) : nullptr;
    if (!location) return refuse(*subscriber, symbol, proto::SubscribeStatus::kUnknownSymbol);

    // Entitlement is checked against the venue that lists the instrument. An unknown
    // venue is a refusal, never a default-allow.
    const VenueId venue =
        entitlements_ ? entitlements_->venue_for(symbol) : static_cast<VenueId>(0);
    if (entitlements_ &&
        !EntitlementTable::permits(subscriber->entitlement().venue_mask, venue)) {
      return refuse(*subscriber, symbol, proto::SubscribeStatus::kNotEntitled);
    }

    const std::uint32_t limit = subscriber->entitlement().max_subscriptions;
    if (limit && subscriber->active_subscription_count() >= limit) {
      return refuse(*subscriber, symbol, proto::SubscribeStatus::kLimitExceeded);
    }

    // Un-conflated delivery costs the plant a publication log, so it is gated
    // separately from the right to see the instrument at all.
    std::uint8_t effective_flags = flags;
    if ((effective_flags & proto::kFlagIncremental) &&
        !subscriber->entitlement().allow_incremental) {
      effective_flags = static_cast<std::uint8_t>(
          (effective_flags & ~proto::kFlagIncremental) | proto::kFlagConflated);
    }
    if (!subscriber->entitlement().allow_trades) {
      effective_flags = static_cast<std::uint8_t>(effective_flags & ~proto::kFlagWithTrades);
    }

    const std::uint32_t subscription_index = subscriber->add_subscription(
        symbol, location->shard_id, location->book_index, effective_flags, venue);

    attach_listener(location->shard_id, location->book_index, slot, subscription_index);
    book::BookShard* shard = shards_[location->shard_id];
    shard->set_quote_interest(location->book_index, config_.fanout_id, true);
    if (effective_flags & proto::kFlagWithTrades) {
      shard->set_trade_interest(location->book_index, config_.fanout_id, true);
    }

    // The join: capture the current image, acknowledge with the version it carries,
    // then deliver that same image. Everything above this version reaches the client
    // through the ordinary update path, so there is no gap and no duplicate.
    const book::BookImage image = shard->published(location->book_index).load();
    subscriber->encode_subscribe_ack(symbol, proto::SubscribeStatus::kOk, image.version);
    if (image.version > 0) subscriber->offer_book(subscription_index, image);
    ++stats_.subscriptions_granted;
    return proto::SubscribeStatus::kOk;
  }

  void unsubscribe(std::uint32_t slot, Symbol symbol) {
    Subscriber* subscriber = subscriber_at(slot);
    if (!subscriber) return;
    const Subscription* subscription = subscriber->find_subscription(symbol);
    if (!subscription) return;
    const std::uint32_t shard_id = subscription->shard_id;
    const std::uint32_t book_index = subscription->book_index;
    subscriber->remove_subscription(symbol);
    detach_listener(shard_id, book_index, slot);
  }

  // --- the loop body -------------------------------------------------------

  /// One pass over everything this thread owns.
  void poll(std::uint64_t now_ns) {
    ++stats_.passes;

    flush_all(now_ns);
    route_book_updates();
    route_trades();
    retry_pending();
    flush_all(now_ns);
    reap_evicted(now_ns);
  }

  /// Is there anything waiting? Lets an idle thread park instead of spinning a core.
  bool has_work() const noexcept {
    for (const ShardLink& link : links_) {
      if (link.dirty && link.dirty->maybe_dirty()) return true;
      if (link.trades && !link.trades->empty_approx()) return true;
    }
    for (const auto& subscriber : subscribers_) {
      if (subscriber && subscriber->pending_bytes() > 0) return true;
    }
    return false;
  }

  void broadcast_heartbeat(std::uint64_t now_ns) {
    for (auto& subscriber : subscribers_) {
      if (subscriber) subscriber->encode_heartbeat(now_ns);
    }
  }

 private:
  struct Listener {
    std::uint32_t slot{0};
    std::uint32_t subscription_index{0};
  };

  struct ShardLink {
    std::unique_ptr<DirtySet> dirty;
    std::unique_ptr<book::TradeRing> trades;
    std::vector<std::vector<Listener>> listeners;  ///< Indexed by book index.
  };

  proto::SubscribeStatus refuse(Subscriber& subscriber, Symbol symbol,
                                proto::SubscribeStatus status) {
    subscriber.encode_subscribe_ack(symbol, status, 0);
    ++stats_.subscriptions_refused;
    return status;
  }

  void attach_listener(std::uint32_t shard_id, std::uint32_t book_index,
                       std::uint32_t slot, std::uint32_t subscription_index) {
    auto& listeners = links_[shard_id].listeners[book_index];
    for (Listener& listener : listeners) {
      if (listener.slot == slot) {
        listener.subscription_index = subscription_index;
        return;
      }
    }
    listeners.push_back(Listener{slot, subscription_index});
  }

  void detach_listener(std::uint32_t shard_id, std::uint32_t book_index,
                       std::uint32_t slot) {
    if (shard_id >= links_.size()) return;
    auto& listeners = links_[shard_id].listeners[book_index];
    for (std::size_t i = 0; i < listeners.size(); ++i) {
      if (listeners[i].slot != slot) continue;
      listeners[i] = listeners.back();
      listeners.pop_back();
      break;
    }
    if (listeners.empty()) {
      // Nobody on this thread is watching any more, so stop being told about it.
      shards_[shard_id]->set_quote_interest(book_index, config_.fanout_id, false);
      shards_[shard_id]->set_trade_interest(book_index, config_.fanout_id, false);
    }
  }

  void route_book_updates() {
    for (std::size_t shard_id = 0; shard_id < links_.size(); ++shard_id) {
      ShardLink& link = links_[shard_id];
      if (!link.dirty) continue;
      book::BookShard* shard = shards_[shard_id];
      link.dirty->drain([&](std::uint32_t book_index) {
        const auto& listeners = link.listeners[book_index];
        if (listeners.empty()) return;
        // One seqlock read, shared across every subscriber watching the instrument.
        const book::BookImage image = shard->published(book_index).load();
        for (const Listener& listener : listeners) {
          Subscriber* subscriber = subscribers_[listener.slot].get();
          if (subscriber) subscriber->offer_book(listener.subscription_index, image);
        }
        ++stats_.instruments_routed;
      });
    }
  }

  void route_trades() {
    feed::TradeEvent trade;
    for (ShardLink& link : links_) {
      if (!link.trades) continue;
      while (link.trades->try_pop(trade)) {
        if (trade.book_index >= link.listeners.size()) continue;
        for (const Listener& listener : link.listeners[trade.book_index]) {
          Subscriber* subscriber = subscribers_[listener.slot].get();
          if (!subscriber) continue;
          const Subscription* subscription =
              subscriber->subscription_at(listener.subscription_index);
          if (!subscription || !(subscription->flags & proto::kFlagWithTrades)) continue;
          subscriber->offer_trade(trade);
        }
        ++stats_.trades_routed;
      }
    }
  }

  /// Re-offer instruments a subscriber was owed, using whatever the book says *now*.
  /// Deliberately not the image that was current when the socket filled up: that one
  /// is already history, and shipping it would spend the client's scarce bandwidth on
  /// a stale price.
  void retry_pending() {
    for (auto& holder : subscribers_) {
      Subscriber* subscriber = holder.get();
      if (!subscriber || subscriber->evicted()) continue;
      if (subscriber->pending_subscriptions().empty()) continue;

      subscriber->take_pending(pending_scratch_);
      for (std::uint32_t subscription_index : pending_scratch_) {
        const Subscription* subscription =
            subscriber->subscription_at(subscription_index);
        if (!subscription || !subscription->active) continue;
        const book::BookImage image =
            shards_[subscription->shard_id]->published(subscription->book_index).load();
        subscriber->offer_book(subscription_index, image);
        ++stats_.pending_retries;
      }
    }
  }

  void flush_all(std::uint64_t now_ns) {
    for (std::uint32_t slot = 0; slot < subscribers_.size(); ++slot) {
      Subscriber* subscriber = subscribers_[slot].get();
      if (!subscriber) continue;
      if (!subscriber->flush(now_ns) && !subscriber->evicted()) {
        // The socket itself failed: nothing to tell the client, just let go.
        release(slot);
      }
    }
  }

  void reap_evicted(std::uint64_t now_ns) {
    for (std::uint32_t slot = 0; slot < subscribers_.size(); ++slot) {
      Subscriber* subscriber = subscribers_[slot].get();
      if (!subscriber || !subscriber->evicted()) continue;

      // Say why before hanging up. The buffer is discarded first because the client
      // has already proven it cannot drain it, and the notice is worth more than the
      // market data stuck behind it.
      const std::uint64_t stalled = subscriber->stalled_nanos(now_ns);
      // Capture the backlog before discarding it: the notice exists to tell an operator
      // how far behind the client had fallen, and measuring after the discard would
      // report zero every time.
      const std::uint64_t backlog = subscriber->pending_bytes();
      subscriber->discard_buffered();
      subscriber->encode_evicted(subscriber->evict_reason(), stalled, backlog);
      subscriber->flush_final();
      ++stats_.subscribers_evicted;
      release(slot);
    }
  }

  Config config_;
  std::vector<book::BookShard*> shards_;
  const InstrumentRegistry* registry_{nullptr};
  const EntitlementTable* entitlements_{nullptr};
  std::vector<ShardLink> links_;
  std::vector<std::unique_ptr<Subscriber>> subscribers_;
  std::vector<std::uint32_t> pending_scratch_;
  std::size_t live_subscribers_{0};
  Stats stats_;
};

}  // namespace cascade::dist
