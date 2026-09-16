// SPDX-License-Identifier: Apache-2.0
#include "cascade/book/book_shard.hpp"

#include <set>
#include <vector>

#include "test_harness.hpp"

using cascade::DirtySet;
using cascade::Price;
using cascade::Side;
using cascade::Symbol;
using cascade::book::BookImage;
using cascade::book::BookShard;
using cascade::book::TradeRing;
using cascade::feed::FeedEvent;
using cascade::feed::TradeEvent;
using cascade::proto::MsgType;

namespace {

constexpr Price px(double v) { return cascade::price_from_double(v); }

FeedEvent add_order(std::uint64_t id, const char* symbol, Side side, double price,
                    std::uint32_t quantity, std::uint64_t seq = 1) {
  FeedEvent e;
  e.type = static_cast<std::uint8_t>(MsgType::kAddOrder);
  e.sequence = seq;
  e.order_id = id;
  e.symbol = Symbol::from_text(symbol).raw();
  e.side = static_cast<std::uint8_t>(side);
  e.price = px(price);
  e.quantity = quantity;
  e.exchange_ns = 1'000 + seq;
  e.ingest_ns = 2'000 + seq;
  return e;
}

FeedEvent by_id(MsgType type, std::uint64_t id, std::uint32_t quantity = 0,
                std::uint64_t seq = 1) {
  FeedEvent e;
  e.type = static_cast<std::uint8_t>(type);
  e.sequence = seq;
  e.order_id = id;
  e.quantity = quantity;
  e.exchange_ns = 1'000 + seq;
  e.ingest_ns = 2'000 + seq;
  return e;
}

BookImage read(const BookShard& shard, std::uint32_t index) {
  return shard.published(index).load();
}

/// Instruments come from a security master before the session opens, never lazily from
/// the feed, so every test registers up front the way the real control plane does.
void register_symbols(BookShard& shard, std::initializer_list<const char*> symbols) {
  for (const char* text : symbols) shard.register_symbol(Symbol::from_text(text));
}

}  // namespace

TEST(add_builds_a_book_and_publishes_it) {
  BookShard shard(BookShard::Config{});
  register_symbols(shard, {"AAPL"});
  shard.apply(add_order(1, "AAPL", Side::kBuy, 190.00, 100, 1));
  shard.apply(add_order(2, "AAPL", Side::kSell, 190.05, 200, 2));

  const std::uint32_t index = shard.lookup(Symbol::from_text("AAPL"));
  CHECK(index != BookShard::kNoIndex);

  const BookImage image = read(shard, index);
  CHECK_EQ(image.best_bid(), px(190.00));
  CHECK_EQ(image.best_ask(), px(190.05));
  CHECK_EQ(image.bids[0].quantity, std::uint32_t{100});
  CHECK_EQ(image.asks[0].quantity, std::uint32_t{200});
  CHECK_EQ(image.version, std::uint64_t{2});  // one version per visible change
  CHECK_EQ(shard.stats().events_applied, std::uint64_t{2});
}

TEST(delete_removes_the_order_it_names) {
  BookShard shard(BookShard::Config{});
  register_symbols(shard, {"IBM"});
  shard.apply(add_order(1, "IBM", Side::kBuy, 140.00, 100, 1));
  shard.apply(add_order(2, "IBM", Side::kBuy, 139.95, 500, 2));
  const std::uint32_t index = shard.lookup(Symbol::from_text("IBM"));

  shard.apply(by_id(MsgType::kDelete, 1, 0, 3));
  const BookImage image = read(shard, index);
  CHECK_EQ(image.bid_levels, std::uint8_t{1});
  CHECK_EQ(image.best_bid(), px(139.95));  // best rolls down
  CHECK_EQ(shard.stats().book_corrupt, std::uint64_t{0});
}

TEST(partial_cancel_reduces_without_removing) {
  BookShard shard(BookShard::Config{});
  register_symbols(shard, {"IBM"});
  shard.apply(add_order(1, "IBM", Side::kBuy, 140.00, 100, 1));
  const std::uint32_t index = shard.lookup(Symbol::from_text("IBM"));

  shard.apply(by_id(MsgType::kCancel, 1, 40, 2));
  BookImage image = read(shard, index);
  CHECK_EQ(image.bids[0].quantity, std::uint32_t{60});
  CHECK_EQ(image.bids[0].order_count, std::uint32_t{1});  // still resting

  // Cancelling the remainder must retire the order, not leave a zero-size ghost.
  shard.apply(by_id(MsgType::kCancel, 1, 60, 3));
  image = read(shard, index);
  CHECK_EQ(image.bid_levels, std::uint8_t{0});
  CHECK_EQ(shard.stats().book_corrupt, std::uint64_t{0});
}

TEST(execute_reduces_the_book_and_prints_a_trade) {
  BookShard shard(BookShard::Config{});
  DirtySet dirty(1024);
  TradeRing trades;
  shard.attach_fanout(0, &dirty, &trades);
  register_symbols(shard, {"MSFT"});

  shard.apply(add_order(1, "MSFT", Side::kSell, 420.00, 300, 1));
  const std::uint32_t index = shard.lookup(Symbol::from_text("MSFT"));
  shard.set_trade_interest(index, 0, true);

  FeedEvent fill = by_id(MsgType::kExecute, 1, 120, 2);
  fill.aux_id = 0xFEED;
  shard.apply(fill);

  const BookImage image = read(shard, index);
  CHECK_EQ(image.asks[0].quantity, std::uint32_t{180});

  TradeEvent trade;
  CHECK(trades.try_pop(trade));
  CHECK_EQ(trade.symbol, Symbol::from_text("MSFT").raw());
  CHECK_EQ(trade.quantity, std::uint32_t{120});
  CHECK_EQ(trade.price, px(420.00));
  CHECK_EQ(trade.match_id, std::uint64_t{0xFEED});
  CHECK_EQ(shard.stats().trades, std::uint64_t{1});
}

TEST(trades_are_not_emitted_when_nobody_is_watching) {
  // An instrument with no tape subscriber must not cost a ring write.
  BookShard shard(BookShard::Config{});
  DirtySet dirty(1024);
  TradeRing trades;
  shard.attach_fanout(0, &dirty, &trades);
  register_symbols(shard, {"MSFT"});

  shard.apply(add_order(1, "MSFT", Side::kSell, 420.00, 300, 1));
  shard.apply(by_id(MsgType::kExecute, 1, 120, 2));

  TradeEvent trade;
  CHECK(!trades.try_pop(trade));
  CHECK_EQ(shard.stats().trades, std::uint64_t{1});  // counted, just not delivered
}

TEST(replace_forfeits_queue_position) {
  BookShard shard(BookShard::Config{});
  register_symbols(shard, {"GOOG"});
  shard.apply(add_order(1, "GOOG", Side::kBuy, 150.00, 100, 1));
  const std::uint32_t index = shard.lookup(Symbol::from_text("GOOG"));

  FeedEvent replace = by_id(MsgType::kReplace, 1, 250, 2);
  replace.aux_id = 99;          // the new order id
  replace.price = px(150.10);
  shard.apply(replace);

  const BookImage image = read(shard, index);
  CHECK_EQ(image.bid_levels, std::uint8_t{1});
  CHECK_EQ(image.best_bid(), px(150.10));
  CHECK_EQ(image.bids[0].quantity, std::uint32_t{250});

  // The old id is gone; the new one is live and can itself be deleted.
  shard.apply(by_id(MsgType::kDelete, 1, 0, 3));
  CHECK_EQ(shard.stats().unknown_order, std::uint64_t{1});
  shard.apply(by_id(MsgType::kDelete, 99, 0, 4));
  CHECK_EQ(read(shard, index).bid_levels, std::uint8_t{0});
}

TEST(messages_for_unknown_orders_are_counted_not_crashed) {
  // Joining mid-session means seeing deletes for orders that were added before we
  // started listening. That is expected, must not corrupt the book, and must be visible
  // in the stats so an operator can tell a warm-up from a real problem.
  BookShard shard(BookShard::Config{});
  shard.apply(by_id(MsgType::kDelete, 12345, 0, 1));
  shard.apply(by_id(MsgType::kExecute, 12345, 10, 2));
  shard.apply(by_id(MsgType::kCancel, 12345, 10, 3));
  shard.apply(by_id(MsgType::kReplace, 12345, 10, 4));
  CHECK_EQ(shard.stats().unknown_order, std::uint64_t{4});
  CHECK_EQ(shard.stats().book_corrupt, std::uint64_t{0});
}

// The change filter, end to end: deep-book churn must not reach the fan-out at all.
TEST(activity_below_the_visible_depth_never_reaches_the_fanout) {
  BookShard shard(BookShard::Config{});
  DirtySet dirty(1024);
  TradeRing trades;
  shard.attach_fanout(0, &dirty, &trades);
  register_symbols(shard, {"SPY"});

  std::uint64_t seq = 1;
  for (int i = 0; i < 15; ++i) {
    shard.apply(add_order(100 + static_cast<std::uint64_t>(i), "SPY", Side::kBuy,
                          400.00 - i * 0.01, 100, seq++));
  }
  const std::uint32_t index = shard.lookup(Symbol::from_text("SPY"));
  shard.set_quote_interest(index, 0, true);
  dirty.drain([](std::uint32_t) {});

  const std::uint64_t published_before = shard.stats().books_published;
  const std::uint64_t suppressed_before = shard.stats().publishes_suppressed;
  const std::uint64_t version_before = read(shard, index).version;

  // 5,000 orders at prices far below the tenth level. Not one is visible.
  for (int i = 0; i < 5'000; ++i) {
    shard.apply(add_order(10'000 + static_cast<std::uint64_t>(i), "SPY", Side::kBuy,
                          390.00 - (i % 50) * 0.01, 100, seq++));
  }

  CHECK_EQ(shard.stats().books_published, published_before);
  CHECK_EQ(shard.stats().publishes_suppressed - suppressed_before, std::uint64_t{5'000});
  CHECK_EQ(read(shard, index).version, version_before);
  CHECK_EQ(dirty.drain([](std::uint32_t) {}), std::size_t{0});

  // ...but an order that does improve the best price gets through immediately.
  shard.apply(add_order(99'999, "SPY", Side::kBuy, 400.50, 100, seq++));
  CHECK_EQ(shard.stats().books_published, published_before + 1);
  CHECK_EQ(dirty.drain([](std::uint32_t) {}), std::size_t{1});
}

TEST(only_interested_fanouts_are_notified) {
  BookShard shard(BookShard::Config{});
  DirtySet dirty_a(1024), dirty_b(1024);
  TradeRing trades_a, trades_b;
  shard.attach_fanout(0, &dirty_a, &trades_a);
  shard.attach_fanout(1, &dirty_b, &trades_b);
  register_symbols(shard, {"NVDA"});

  shard.apply(add_order(1, "NVDA", Side::kBuy, 900.00, 100, 1));
  const std::uint32_t index = shard.lookup(Symbol::from_text("NVDA"));
  shard.set_quote_interest(index, 1, true);  // only fan-out 1 cares

  dirty_a.drain([](std::uint32_t) {});
  dirty_b.drain([](std::uint32_t) {});
  shard.apply(add_order(2, "NVDA", Side::kBuy, 900.50, 100, 2));

  CHECK_EQ(dirty_a.drain([](std::uint32_t) {}), std::size_t{0});
  CHECK_EQ(dirty_b.drain([](std::uint32_t) {}), std::size_t{1});

  // Interest is revocable, and revoking it stops the notifications.
  shard.set_quote_interest(index, 1, false);
  shard.apply(add_order(3, "NVDA", Side::kBuy, 901.00, 100, 3));
  CHECK_EQ(dirty_b.drain([](std::uint32_t) {}), std::size_t{0});
}

TEST(reset_clears_the_book_and_flags_it_stale) {
  // An unrecoverable gap means the book is built from an incomplete delta stream and
  // is permanently wrong. Subscribers must be told at once, not left holding it.
  BookShard shard(BookShard::Config{});
  register_symbols(shard, {"AMD"});
  shard.apply(add_order(1, "AMD", Side::kBuy, 170.00, 100, 1));
  shard.apply(add_order(2, "AMD", Side::kSell, 170.10, 100, 2));
  const std::uint32_t index = shard.lookup(Symbol::from_text("AMD"));

  const std::uint64_t version_before = read(shard, index).version;
  shard.reset_symbol(index, 9'999);

  const BookImage image = read(shard, index);
  CHECK_EQ(image.bid_levels, std::uint8_t{0});
  CHECK_EQ(image.ask_levels, std::uint8_t{0});
  CHECK((image.flags & cascade::proto::kBookStale) != 0);
  CHECK_GE(image.version, version_before + 1);  // the reset is itself a publish

  shard.clear_stale(index, 10'000);
  CHECK_EQ(read(shard, index).flags & cascade::proto::kBookStale, 0);
}

TEST(shard_routing_spreads_symbols_across_shards) {
  // Hash routing, not alphabetical: a range partition would drop every symbol starting
  // with a hot letter onto one core.
  const char* symbols[] = {"AAPL", "AMZN", "AMD",  "ABNB", "ADBE", "MSFT",
                           "META", "NVDA", "GOOG", "TSLA", "NFLX", "INTC"};
  std::vector<int> per_shard(4, 0);
  for (const char* text : symbols) {
    per_shard[BookShard::shard_for(Symbol::from_text(text), 4)]++;
  }
  int used = 0;
  for (int count : per_shard) if (count > 0) ++used;
  CHECK_GE(used, 3);  // 12 symbols over 4 shards should touch at least 3
}

TEST(market_close_flags_every_book_in_the_shard) {
  BookShard shard(BookShard::Config{});
  register_symbols(shard, {"AAPL", "MSFT"});
  shard.apply(add_order(1, "AAPL", Side::kBuy, 190.00, 100, 1));
  shard.apply(add_order(2, "MSFT", Side::kBuy, 420.00, 100, 2));

  FeedEvent close;
  close.type = static_cast<std::uint8_t>(MsgType::kSystemEvent);
  close.event_code = 'M';
  shard.apply(close);

  for (const char* text : {"AAPL", "MSFT"}) {
    const std::uint32_t index = shard.lookup(Symbol::from_text(text));
    CHECK((read(shard, index).flags & cascade::proto::kBookHalted) != 0);
  }
}

TEST(symbol_capacity_is_enforced_rather_than_overrunning) {
  BookShard::Config config;
  config.max_symbols = 2;
  BookShard shard(config);
  shard.register_symbol(Symbol::from_text("AAA"));
  shard.register_symbol(Symbol::from_text("BBB"));
  bool threw = false;
  try {
    shard.register_symbol(Symbol::from_text("CCC"));
  } catch (const std::runtime_error&) {
    threw = true;
  }
  CHECK(threw);
  CHECK_EQ(shard.symbol_count(), std::size_t{2});
}

TEST(events_for_unregistered_instruments_are_dropped_and_counted) {
  // Admitting an unknown symbol would mean the shard thread rehashing the symbol map
  // while fan-out threads read it to resolve subscriptions. Counting and dropping keeps
  // that map read-only for the whole session, which is what makes it safe to share.
  BookShard shard(BookShard::Config{});
  register_symbols(shard, {"AAPL"});

  shard.apply(add_order(1, "NOPE", Side::kBuy, 10.0, 100, 1));
  CHECK_EQ(shard.stats().unknown_symbol, std::uint64_t{1});
  CHECK_EQ(shard.symbol_count(), std::size_t{1});

  // A registered instrument on the same shard is unaffected.
  shard.apply(add_order(2, "AAPL", Side::kBuy, 190.0, 100, 2));
  const std::uint32_t index = shard.lookup(Symbol::from_text("AAPL"));
  CHECK_EQ(read(shard, index).bid_levels, std::uint8_t{1});
}

TEST(the_publication_log_records_every_published_version) {
  BookShard shard(BookShard::Config{});
  register_symbols(shard, {"AAPL"});
  cascade::dist::PublicationLog log(64);
  log.set_enabled(true);
  shard.set_publication_log(&log);

  shard.apply(add_order(1, "AAPL", Side::kBuy, 190.00, 100, 1));
  shard.apply(add_order(2, "AAPL", Side::kBuy, 190.05, 100, 2));
  shard.apply(add_order(3, "AAPL", Side::kSell, 190.10, 100, 3));

  CHECK_EQ(log.write_position(), std::uint64_t{3});
  BookImage image;
  for (std::uint64_t position = 0; position < 3; ++position) {
    CHECK(log.read(position, image));
    CHECK_EQ(image.version, position + 1);
  }
}

TEST(a_disabled_publication_log_costs_nothing) {
  // Every subscriber conflated is the common case, and the log is a full image copy
  // per publish; it must not be paid for when nobody reads it.
  BookShard shard(BookShard::Config{});
  register_symbols(shard, {"AAPL"});
  cascade::dist::PublicationLog log(64);
  shard.set_publication_log(&log);  // left disabled

  for (std::uint64_t i = 1; i <= 10; ++i) {
    shard.apply(add_order(i, "AAPL", Side::kBuy, 190.0 + static_cast<double>(i), 100, i));
  }
  CHECK_EQ(log.write_position(), std::uint64_t{0});
  CHECK_EQ(shard.stats().books_published, std::uint64_t{10});
}
