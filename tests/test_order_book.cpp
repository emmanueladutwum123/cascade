// SPDX-License-Identifier: Apache-2.0
#include "cascade/book/order_book.hpp"

#include <map>
#include <random>

#include "test_harness.hpp"

using cascade::Price;
using cascade::Side;
using cascade::Symbol;
using cascade::book::AskLadder;
using cascade::book::BidLadder;
using cascade::book::BookImage;
using cascade::book::OrderBook;
using cascade::book::visible_change;

namespace {
constexpr Price px(double v) { return cascade::price_from_double(v); }
}  // namespace

TEST(bid_ladder_orders_best_first) {
  BidLadder bids;
  bids.add(px(100.00), 500);
  bids.add(px(100.50), 300);   // new best
  bids.add(px(99.75), 900);    // deep
  bids.add(px(100.25), 100);   // between

  CHECK_EQ(bids.depth(), std::size_t{4});
  CHECK_EQ(bids.best_price(), px(100.50));

  cascade::proto::PriceLevel top[10];
  const std::uint8_t n = bids.copy_top(top, 10);
  CHECK_EQ(n, std::uint8_t{4});
  // For bids, "best" means highest, and levels must descend from there.
  CHECK_EQ(top[0].price, px(100.50));
  CHECK_EQ(top[1].price, px(100.25));
  CHECK_EQ(top[2].price, px(100.00));
  CHECK_EQ(top[3].price, px(99.75));
  CHECK_EQ(top[0].quantity, std::uint32_t{300});
}

TEST(ask_ladder_orders_best_first) {
  AskLadder asks;
  asks.add(px(101.00), 500);
  asks.add(px(100.50), 300);   // new best (lower is better for asks)
  asks.add(px(102.00), 900);
  asks.add(px(100.75), 100);

  CHECK_EQ(asks.best_price(), px(100.50));
  cascade::proto::PriceLevel top[10];
  const std::uint8_t n = asks.copy_top(top, 10);
  CHECK_EQ(n, std::uint8_t{4});
  CHECK_EQ(top[0].price, px(100.50));
  CHECK_EQ(top[1].price, px(100.75));
  CHECK_EQ(top[2].price, px(101.00));
  CHECK_EQ(top[3].price, px(102.00));
}

TEST(orders_at_one_price_aggregate) {
  BidLadder bids;
  bids.add(px(50.00), 100);
  bids.add(px(50.00), 250);
  bids.add(px(50.00), 50);
  CHECK_EQ(bids.depth(), std::size_t{1});
  const auto* level = bids.best();
  CHECK(level != nullptr);
  if (level) {
    CHECK_EQ(level->quantity, std::uint64_t{400});
    CHECK_EQ(level->order_count, std::uint32_t{3});  // three distinct orders resting
  }
}

TEST(level_disappears_when_its_last_order_leaves) {
  BidLadder bids;
  bids.add(px(50.00), 100);
  bids.add(px(49.00), 200);
  CHECK(bids.remove(px(50.00), 100, 1));
  CHECK_EQ(bids.depth(), std::size_t{1});
  CHECK_EQ(bids.best_price(), px(49.00));  // best rolls down to the next level
}

TEST(partial_reduction_keeps_the_level) {
  BidLadder bids;
  bids.add(px(50.00), 100);
  bids.add(px(50.00), 100);
  CHECK(bids.remove(px(50.00), 40, 0));  // partial cancel: size down, order still resting
  const auto* level = bids.best();
  CHECK(level != nullptr);
  if (level) {
    CHECK_EQ(level->quantity, std::uint64_t{160});
    CHECK_EQ(level->order_count, std::uint32_t{2});
  }
}

TEST(removing_more_than_rests_is_rejected_not_clamped) {
  // Losing a message must surface as an error the caller can act on, never as a
  // silently clamped book that looks healthy and quotes the wrong size.
  BidLadder bids;
  bids.add(px(50.00), 100);
  CHECK(!bids.remove(px(50.00), 500, 1));
  CHECK(!bids.remove(px(77.00), 10, 1));  // unknown level
  CHECK_EQ(bids.depth(), std::size_t{1});  // book untouched by the rejected removal
}

TEST(inconsistent_level_state_is_reported_and_dropped) {
  // Zero size with orders still resting means we double-applied or lost a message.
  BidLadder bids;
  bids.add(px(50.00), 100);
  bids.add(px(50.00), 100);
  CHECK(!bids.remove(px(50.00), 200, 0));  // all size gone, two orders "still" there
  CHECK_EQ(bids.depth(), std::size_t{0});  // phantom level dropped rather than quoted
}

TEST(deep_book_stays_sorted_under_random_traffic) {
  // Differential check against an ordered map: after thousands of random adds and
  // removes at random depths, the flat ladder must agree exactly with a std::map.
  BidLadder bids;
  std::map<Price, std::pair<std::uint64_t, std::uint32_t>, std::greater<Price>> reference;
  std::mt19937_64 rng(7);

  for (int step = 0; step < 20'000; ++step) {
    const Price price = px(90.0 + static_cast<double>(rng() % 2000) / 100.0);
    if (rng() % 2 == 0 || reference.empty()) {
      const std::uint64_t quantity = (rng() % 500) + 1;
      bids.add(price, quantity);
      auto& entry = reference[price];
      entry.first += quantity;
      entry.second += 1;
    } else {
      auto it = reference.begin();
      std::advance(it, static_cast<long>(rng() % reference.size()));
      const std::uint64_t take = (rng() % it->second.first) + 1;
      const bool full = (take == it->second.first);
      const std::uint32_t orders = full ? it->second.second : 0;
      CHECK(bids.remove(it->first, take, orders));
      it->second.first -= take;
      it->second.second -= orders;
      if (it->second.second == 0) reference.erase(it);
    }
  }

  CHECK_EQ(bids.depth(), reference.size());
  const auto& levels = bids.raw_levels();
  // Stored worst-first, so reading backwards must match the descending reference.
  std::size_t i = levels.size();
  for (const auto& [price, agg] : reference) {
    CHECK(i > 0);
    if (i == 0) break;
    --i;
    CHECK_EQ(levels[i].price, price);
    CHECK_EQ(levels[i].quantity, agg.first);
    CHECK_EQ(levels[i].order_count, agg.second);
  }
}

TEST(book_snapshot_reports_both_sides_and_mid) {
  OrderBook book(Symbol::from_text("AAPL"));
  book.add(Side::kBuy, px(190.00), 100);
  book.add(Side::kBuy, px(189.95), 200);
  book.add(Side::kSell, px(190.05), 150);
  book.add(Side::kSell, px(190.10), 300);

  BookImage image;
  book.snapshot(image, 1'000, 2'000);

  CHECK_EQ(image.symbol, Symbol::from_text("AAPL").raw());
  CHECK_EQ(image.bid_levels, std::uint8_t{2});
  CHECK_EQ(image.ask_levels, std::uint8_t{2});
  CHECK_EQ(image.best_bid(), px(190.00));
  CHECK_EQ(image.best_ask(), px(190.05));
  CHECK_EQ(image.mid(), px(190.025));
  CHECK(!image.crossed());
  CHECK_EQ(image.exchange_ns, std::uint64_t{1'000});
  CHECK_EQ(image.ingest_ns, std::uint64_t{2'000});
}

TEST(empty_sides_report_no_price_rather_than_zero) {
  // A zero price is a real price. An empty side must be distinguishable from a book
  // where someone is genuinely bidding zero, or downstream pricing will be nonsense.
  OrderBook book(Symbol::from_text("TSLA"));
  BookImage image;
  book.snapshot(image, 0, 0);
  CHECK_EQ(image.bid_levels, std::uint8_t{0});
  CHECK_EQ(image.best_bid(), cascade::kNoPrice);
  CHECK_EQ(image.mid(), cascade::kNoPrice);

  book.add(Side::kBuy, px(100.0), 10);
  book.snapshot(image, 0, 0);
  CHECK_EQ(image.mid(), cascade::kNoPrice);  // still no ask: no midpoint exists
}

TEST(crossed_book_is_flagged_not_hidden) {
  OrderBook book(Symbol::from_text("SPY"));
  book.add(Side::kBuy, px(400.10), 100);
  book.add(Side::kSell, px(400.00), 100);  // bid above ask
  BookImage image;
  book.snapshot(image, 0, 0);
  CHECK(image.crossed());
  CHECK((image.flags & cascade::proto::kBookCrossed) != 0);
}

TEST(snapshot_truncates_to_the_published_depth) {
  OrderBook book(Symbol::from_text("QQQ"));
  for (int i = 0; i < 40; ++i) {
    book.add(Side::kBuy, px(100.0 - i * 0.01), 10);
    book.add(Side::kSell, px(101.0 + i * 0.01), 10);
  }
  BookImage image;
  book.snapshot(image, 0, 0);
  CHECK_EQ(image.bid_levels, cascade::proto::kMaxDepth);
  CHECK_EQ(image.ask_levels, cascade::proto::kMaxDepth);
  CHECK_EQ(image.bids[0].price, px(100.0));   // still the true best
  CHECK_EQ(image.asks[0].price, px(101.0));
}

// The change filter is the plant's biggest single saving, so its two failure modes
// both matter: missing a visible change (subscribers see a stale book) and reporting
// one that is not visible (the saving evaporates).
TEST(change_filter_ignores_activity_below_the_published_depth) {
  OrderBook book(Symbol::from_text("IBM"));
  for (int i = 0; i < 30; ++i) book.add(Side::kBuy, px(100.0 - i * 0.01), 10);

  BookImage before;
  book.snapshot(before, 0, 0);

  book.add(Side::kBuy, px(99.50), 5'000);  // 50 ticks down: far below the top ten
  BookImage after;
  book.snapshot(after, 0, 0);
  CHECK(!visible_change(before, after));
}

TEST(change_filter_catches_every_visible_kind_of_change) {
  OrderBook book(Symbol::from_text("IBM"));
  book.add(Side::kBuy, px(100.00), 100);
  book.add(Side::kSell, px(100.05), 100);
  BookImage base;
  book.snapshot(base, 0, 0);

  {  // size change at an existing level
    OrderBook b2(Symbol::from_text("IBM"));
    b2.add(Side::kBuy, px(100.00), 150);
    b2.add(Side::kSell, px(100.05), 100);
    BookImage img;
    b2.snapshot(img, 0, 0);
    CHECK(visible_change(base, img));
  }
  {  // a new best price
    OrderBook b2(Symbol::from_text("IBM"));
    b2.add(Side::kBuy, px(100.01), 100);
    b2.add(Side::kSell, px(100.05), 100);
    BookImage img;
    b2.snapshot(img, 0, 0);
    CHECK(visible_change(base, img));
  }
  {  // a level count change
    OrderBook b2(Symbol::from_text("IBM"));
    b2.add(Side::kBuy, px(100.00), 100);
    b2.add(Side::kBuy, px(99.99), 100);
    b2.add(Side::kSell, px(100.05), 100);
    BookImage img;
    b2.snapshot(img, 0, 0);
    CHECK(visible_change(base, img));
  }
  {  // identical book: no change
    OrderBook b2(Symbol::from_text("IBM"));
    b2.add(Side::kBuy, px(100.00), 100);
    b2.add(Side::kSell, px(100.05), 100);
    BookImage img;
    b2.snapshot(img, 99'999, 88'888);  // timestamps differ, visible state does not
    CHECK(!visible_change(base, img));
  }
}
