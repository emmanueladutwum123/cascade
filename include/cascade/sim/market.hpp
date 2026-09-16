// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <random>
#include <string>
#include <vector>

#include "cascade/feed/decoder.hpp"
#include "cascade/proto/feed.hpp"
#include "cascade/proto/symbol.hpp"

namespace cascade::sim {

/// Generates order flow with the statistical shape of a real equity venue.
///
/// Benchmarking a ticker plant against uniform random traffic would flatter it badly,
/// because the properties that make real market data hard are exactly the ones a naive
/// generator lacks:
///
///   * **Cancels dominate.** On a modern equity venue well over 90% of order events are
///     cancels and replaces, not trades. A generator that mostly adds orders would let
///     the order map grow monotonically and never exercise the deletion path that the
///     whole open-addressing design exists to keep fast.
///
///   * **Activity is concentrated near the touch.** Orders cluster at the best few
///     price levels. That is what makes the visible-change filter earn its keep — and a
///     generator spreading orders uniformly across 200 levels would make the filter look
///     far more effective than it is, since almost nothing would reach the top ten.
///
///   * **Instrument activity is heavily skewed.** A handful of names carry most of the
///     message volume. Uniform instrument selection would spread load evenly across
///     shards and hide the imbalance a real plant has to survive.
///
///   * **Prices move.** A static mid means the top of book never changes after warm-up,
///     so nothing is ever published. The mid follows a random walk so books genuinely
///     churn.
class MarketSimulator {
 public:
  struct Config {
    std::uint32_t instrument_count{100};
    std::uint32_t orders_per_instrument{40};  ///< Resting depth to build at warm-up.
    /// Probability that an event adds a new order rather than removing one. Below 0.5
    /// the book drains; at 0.5 resting depth is stable, which is what a real venue
    /// looks like over any short window.
    double add_probability{0.50};
    /// Of the removals, how many are executions (the rest are cancels). Real venues run
    /// far below 10%; this is the ratio that decides how much tape is generated.
    double execution_fraction{0.04};
    double replace_fraction{0.15};   ///< Of adds, how many are cancel-replaces.
    /// Standard deviation of the per-event mid move, in ticks.
    double volatility_ticks{0.35};
    double tick_size{0.01};
    /// How tightly orders cluster around the touch, in ticks.
    double depth_concentration{3.0};
    /// Resting orders per instrument the book gravitates back towards.
    ///
    /// Without this the simulated book grows without bound, because cancels are biased
    /// towards recent orders and old ones are never cleaned up. A real venue's resting
    /// depth is roughly stationary -- orders left far behind a moving mid get pulled --
    /// and the difference is not cosmetic: an ever-deepening ladder turns every insert
    /// into a memmove of thousands of levels and would make the book builder look an
    /// order of magnitude slower than it is on real data.
    std::uint32_t target_resting_orders{160};
    /// Fraction of cancels drawn uniformly from the whole side rather than from the
    /// recent tail. Purely recency-biased cancels never retire stale orders.
    double deep_cancel_fraction{0.35};
    std::uint64_t seed{0x5EED};
  };

  explicit MarketSimulator(Config config) : config_(config), rng_(config.seed) {
    instruments_.reserve(config_.instrument_count);
    for (std::uint32_t i = 0; i < config_.instrument_count; ++i) {
      Instrument instrument;
      instrument.symbol = Symbol::from_text(symbol_text(i));
      // A spread of starting prices, so fixed-point scaling is exercised across
      // magnitudes rather than only around one price.
      instrument.mid_ticks = 1'000.0 + static_cast<double>(i % 400) * 25.0;
      instruments_.push_back(instrument);
      // Zipf-like weighting: instrument 0 carries roughly as much flow as the whole
      // tail, which is what a real venue's volume distribution looks like.
      weights_.push_back(1.0 / (1.0 + static_cast<double>(i)));
      weight_total_ += weights_.back();
    }
  }

  const std::vector<Symbol> symbols() const {
    std::vector<Symbol> out;
    out.reserve(instruments_.size());
    for (const Instrument& instrument : instruments_) out.push_back(instrument.symbol);
    return out;
  }

  std::uint64_t messages_generated() const noexcept { return messages_; }
  std::uint64_t live_orders() const noexcept {
    std::uint64_t total = 0;
    for (const Instrument& instrument : instruments_) {
      total += instrument.bids.size() + instrument.asks.size();
    }
    return total;
  }

  /// Emit one message into `out` (which must hold `proto::kMaxMessageSize` bytes).
  /// Returns the encoded length.
  std::size_t next_message(unsigned char* out, std::uint64_t timestamp_ns) {
    ++messages_;
    Instrument& instrument = instruments_[pick_instrument()];

    // The mid random-walks, so the top of book genuinely moves.
    instrument.mid_ticks += normal_(rng_) * config_.volatility_ticks;
    if (instrument.mid_ticks < 10.0) instrument.mid_ticks = 10.0;

    const std::size_t resting = instrument.bids.size() + instrument.asks.size();
    const bool have_orders = resting > 0;
    // Mean-revert the resting depth: below target, favour adds; above it, favour
    // removals. This is what keeps the book stationary the way a real one is.
    double add_probability = config_.add_probability;
    if (resting > config_.target_resting_orders) add_probability = 0.15;
    else if (resting < config_.target_resting_orders / 2) add_probability = 0.85;

    if (!have_orders || uniform_(rng_) < add_probability) {
      return emit_add(instrument, out, timestamp_ns);
    }
    const double roll = uniform_(rng_);
    if (roll < config_.execution_fraction) return emit_execution(instrument, out, timestamp_ns);
    if (roll < config_.execution_fraction + config_.replace_fraction) {
      return emit_replace(instrument, out, timestamp_ns);
    }
    return emit_cancel(instrument, out, timestamp_ns);
  }

  /// Build the resting depth an instrument would already have at the open, so
  /// measurements are taken against a warm book rather than an empty one.
  template <typename EmitFn>
  void warm_up(EmitFn&& emit, std::uint64_t timestamp_ns) {
    unsigned char scratch[proto::kMaxMessageSize];
    for (Instrument& instrument : instruments_) {
      for (std::uint32_t i = 0; i < config_.orders_per_instrument; ++i) {
        const std::size_t bytes = emit_add(instrument, scratch, timestamp_ns);
        emit(scratch, bytes);
      }
    }
  }

 private:
  struct Order {
    std::uint64_t id{0};
    Price price{0};
    std::uint32_t quantity{0};
  };

  struct Instrument {
    Symbol symbol;
    double mid_ticks{1'000.0};
    std::vector<Order> bids;
    std::vector<Order> asks;
  };

  static std::string symbol_text(std::uint32_t index) {
    // Deterministic, readable, and distinct across the whole range: base-26 over four
    // letters gives 456,976 names, far more than any venue lists.
    std::string text;
    std::uint32_t value = index;
    for (int i = 0; i < 4; ++i) {
      text.insert(text.begin(), static_cast<char>('A' + (value % 26)));
      value /= 26;
    }
    return text;
  }

  std::uint32_t pick_instrument() {
    // Weighted selection so a few names carry most of the flow.
    double target = uniform_(rng_) * weight_total_;
    for (std::uint32_t i = 0; i < weights_.size(); ++i) {
      target -= weights_[i];
      if (target <= 0.0) return i;
    }
    return static_cast<std::uint32_t>(weights_.size() - 1);
  }

  Price price_for(const Instrument& instrument, Side side) {
    // Exponential offset from the touch: most orders sit within a tick or two, with a
    // thin tail reaching deeper, which is the real shape of a limit order book.
    const double depth = exponential_(rng_) * config_.depth_concentration;
    const double ticks = side == Side::kBuy ? instrument.mid_ticks - 0.5 - depth
                                            : instrument.mid_ticks + 0.5 + depth;
    return price_from_double(ticks * config_.tick_size);
  }

  std::size_t emit_add(Instrument& instrument, unsigned char* out,
                       std::uint64_t timestamp_ns) {
    const Side side = uniform_(rng_) < 0.5 ? Side::kBuy : Side::kSell;
    Order order;
    order.id = ++next_order_id_;
    order.price = price_for(instrument, side);
    order.quantity = 100 * (1 + static_cast<std::uint32_t>(rng_() % 20));
    (side == Side::kBuy ? instrument.bids : instrument.asks).push_back(order);
    return feed::encode::add_order(out, timestamp_ns, order.id, instrument.symbol, side,
                                   order.quantity, order.price);
  }

  /// Pick a resting order, favouring recent ones. Real cancels are heavily biased
  /// toward orders placed moments ago; uniform selection would make the book's
  /// composition drift towards stale orders that no venue would still show.
  Order* pick_order(Instrument& instrument, Side& side_out, std::size_t& index_out) {
    std::vector<Order>* book = nullptr;
    if (instrument.bids.empty()) { book = &instrument.asks; side_out = Side::kSell; }
    else if (instrument.asks.empty()) { book = &instrument.bids; side_out = Side::kBuy; }
    else if (uniform_(rng_) < 0.5) { book = &instrument.bids; side_out = Side::kBuy; }
    else { book = &instrument.asks; side_out = Side::kSell; }
    if (book->empty()) return nullptr;

    if (uniform_(rng_) < config_.deep_cancel_fraction) {
      // The tail that retires stale orders sitting far behind the mid.
      index_out = rng_() % book->size();
    } else {
      const std::size_t span = book->size() < 32 ? book->size() : 32;
      index_out = book->size() - 1 - (rng_() % span);
    }
    return &(*book)[index_out];
  }

  void erase_order(Instrument& instrument, Side side, std::size_t index) {
    std::vector<Order>& book = side == Side::kBuy ? instrument.bids : instrument.asks;
    book[index] = book.back();
    book.pop_back();
  }

  std::size_t emit_cancel(Instrument& instrument, unsigned char* out,
                          std::uint64_t timestamp_ns) {
    Side side{};
    std::size_t index = 0;
    Order* order = pick_order(instrument, side, index);
    if (!order) return emit_add(instrument, out, timestamp_ns);

    // Most cancels remove the order outright; a minority shave size off it.
    if (order->quantity > 200 && uniform_(rng_) < 0.25) {
      const std::uint32_t taken = order->quantity / 2;
      order->quantity -= taken;
      return feed::encode::cancel_order(out, timestamp_ns, order->id, taken);
    }
    const std::uint64_t id = order->id;
    erase_order(instrument, side, index);
    return feed::encode::delete_order(out, timestamp_ns, id);
  }

  std::size_t emit_execution(Instrument& instrument, unsigned char* out,
                             std::uint64_t timestamp_ns) {
    Side side{};
    std::size_t index = 0;
    Order* order = pick_order(instrument, side, index);
    if (!order) return emit_add(instrument, out, timestamp_ns);

    const std::uint32_t taken =
        order->quantity > 100 ? 100 * (1 + static_cast<std::uint32_t>(rng_() % 2))
                              : order->quantity;
    const std::uint64_t id = order->id;
    const std::uint64_t match = ++next_match_id_;
    if (taken >= order->quantity) erase_order(instrument, side, index);
    else order->quantity -= taken;
    return feed::encode::execute_order(out, timestamp_ns, id, taken, match);
  }

  std::size_t emit_replace(Instrument& instrument, unsigned char* out,
                           std::uint64_t timestamp_ns) {
    Side side{};
    std::size_t index = 0;
    Order* order = pick_order(instrument, side, index);
    if (!order) return emit_add(instrument, out, timestamp_ns);

    const std::uint64_t old_id = order->id;
    const std::uint64_t new_id = ++next_order_id_;
    const Price price = price_for(instrument, side);
    const std::uint32_t quantity = 100 * (1 + static_cast<std::uint32_t>(rng_() % 20));
    order->id = new_id;
    order->price = price;
    order->quantity = quantity;
    return feed::encode::replace_order(out, timestamp_ns, old_id, new_id, quantity, price);
  }

  Config config_;
  std::mt19937_64 rng_;
  std::uniform_real_distribution<double> uniform_{0.0, 1.0};
  std::normal_distribution<double> normal_{0.0, 1.0};
  std::exponential_distribution<double> exponential_{1.0};

  std::vector<Instrument> instruments_;
  std::vector<double> weights_;
  double weight_total_{0.0};
  std::uint64_t next_order_id_{0};
  std::uint64_t next_match_id_{0};
  std::uint64_t messages_{0};
};

}  // namespace cascade::sim
