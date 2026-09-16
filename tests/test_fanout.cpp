// SPDX-License-Identifier: Apache-2.0
#include "cascade/dist/fanout.hpp"

#include <memory>
#include <unordered_map>
#include <vector>

#include "test_harness.hpp"

using cascade::Side;
using cascade::Symbol;
using cascade::book::BookShard;
using cascade::dist::ClientEntitlement;
using cascade::dist::EntitlementTable;
using cascade::dist::FanoutThread;
using cascade::dist::InstrumentLocation;
using cascade::dist::InstrumentRegistry;
using cascade::dist::Subscriber;
using cascade::feed::FeedEvent;
using cascade::proto::MsgType;
using cascade::proto::SubscribeStatus;

namespace {

class FakeSocket : public cascade::net::ByteSink {
 public:
  long write_some(const cascade::net::OutputBuffer::Span* spans,
                  int span_count) override {
    std::size_t accepted = 0;
    for (int i = 0; i < span_count; ++i) {
      const std::size_t room = window_ > accepted ? window_ - accepted : 0;
      const std::size_t take = spans[i].length < room ? spans[i].length : room;
      received.insert(received.end(), spans[i].data, spans[i].data + take);
      accepted += take;
      if (take < spans[i].length) break;
    }
    return static_cast<long>(accepted);
  }
  void set_window(std::size_t bytes) { window_ = bytes; }
  std::vector<unsigned char> received;

 private:
  std::size_t window_{1u << 30};
};

std::size_t count_frames(const std::vector<unsigned char>& bytes,
                         cascade::proto::ClientMsgType type) {
  std::size_t offset = 0, found = 0;
  while (offset + sizeof(cascade::proto::FrameHeader) <= bytes.size()) {
    cascade::proto::FrameHeader header{};
    std::memcpy(&header, bytes.data() + offset, sizeof(header));
    const std::size_t total = sizeof(header) + header.payload_bytes;
    if (offset + total > bytes.size()) break;
    if (header.type == static_cast<std::uint8_t>(type)) ++found;
    offset += total;
  }
  return found;
}

/// The last book update in a byte stream, as a client would parse it. Levels follow
/// the fixed header on the wire rather than being struct members, so they are decoded
/// into `levels` the same way a real client decoder has to.
bool last_book_update(const std::vector<unsigned char>& bytes,
                      cascade::proto::BookUpdateMsg& out,
                      std::vector<cascade::proto::PriceLevel>* levels = nullptr) {
  std::size_t offset = 0;
  bool found = false;
  while (offset + sizeof(cascade::proto::FrameHeader) <= bytes.size()) {
    cascade::proto::FrameHeader header{};
    std::memcpy(&header, bytes.data() + offset, sizeof(header));
    const std::size_t total = sizeof(header) + header.payload_bytes;
    if (offset + total > bytes.size()) break;
    if (header.type ==
        static_cast<std::uint8_t>(cascade::proto::ClientMsgType::kBookUpdate)) {
      std::memcpy(&out, bytes.data() + offset + sizeof(header), sizeof(out));
      if (levels) {
        levels->clear();
        const std::size_t count =
            static_cast<std::size_t>(out.bid_levels) + out.ask_levels;
        const unsigned char* base =
            bytes.data() + offset + sizeof(header) + sizeof(out);
        for (std::size_t i = 0; i < count; ++i) {
          cascade::proto::PriceLevel level{};
          std::memcpy(&level, base + i * sizeof(level), sizeof(level));
          levels->push_back(level);
        }
      }
      found = true;
    }
    offset += total;
  }
  return found;
}

FeedEvent add_order(std::uint64_t id, const char* symbol, Side side, double price,
                    std::uint32_t quantity, std::uint64_t seq) {
  FeedEvent event;
  event.type = static_cast<std::uint8_t>(MsgType::kAddOrder);
  event.sequence = seq;
  event.order_id = id;
  event.symbol = Symbol::from_text(symbol).raw();
  event.side = static_cast<std::uint8_t>(side);
  event.price = cascade::price_from_double(price);
  event.quantity = quantity;
  event.exchange_ns = 1'000 + seq;
  event.ingest_ns = 2'000 + seq;
  return event;
}

/// A miniature but complete plant: a security master, shards, one fan-out thread and
/// an entitlement table, wired exactly as the daemon wires them.
struct MiniPlant {
  static constexpr std::uint32_t kShardCount = 2;

  EntitlementTable entitlements;
  InstrumentRegistry registry;
  std::vector<std::unique_ptr<BookShard>> shards;
  std::vector<BookShard*> shard_pointers;
  std::unique_ptr<FanoutThread> fanout;

  explicit MiniPlant(std::initializer_list<const char*> symbols) {
    entitlements.register_venue(0, "TESTX");
    entitlements.register_venue(1, "OTHERX");

    for (std::uint32_t id = 0; id < kShardCount; ++id) {
      BookShard::Config config;
      config.shard_id = id;
      config.max_symbols = 256;
      shards.push_back(std::make_unique<BookShard>(config));
      shard_pointers.push_back(shards.back().get());
    }

    // The security master: every instrument registered before any traffic, so the
    // symbol map is read-only for the whole session.
    for (const char* text : symbols) {
      const Symbol symbol = Symbol::from_text(text);
      const std::uint32_t shard_id = BookShard::shard_for(symbol, kShardCount);
      const std::uint32_t book_index = shards[shard_id]->register_symbol(symbol);
      registry.add(symbol, InstrumentLocation{shard_id, book_index});
      entitlements.assign_symbol(symbol, 0);
    }

    FanoutThread::Config config;
    config.fanout_id = 0;
    config.dirty_capacity = 256;
    fanout = std::make_unique<FanoutThread>(config, shard_pointers, &registry,
                                            &entitlements);
  }

  /// Deliver an event the way a channel would.
  ///
  /// Order-referencing messages (Delete, Execute, Cancel, Replace) carry no symbol, so
  /// they cannot be routed after the fact. On a real venue they arrive on the same
  /// multicast channel as the Add that created the order, which is what keeps an
  /// order's whole lifecycle on one shard. The test models that by remembering which
  /// shard each order was opened on.
  void apply(const FeedEvent& event) {
    std::uint32_t shard_id = 0;
    if (event.symbol != 0) {
      shard_id = BookShard::shard_for(event.packed_symbol(), kShardCount);
      if (event.msg_type() == MsgType::kAddOrder) order_channel[event.order_id] = shard_id;
    } else {
      const auto it = order_channel.find(event.order_id);
      if (it == order_channel.end()) return;
      shard_id = it->second;
      if (event.msg_type() == MsgType::kReplace) order_channel[event.aux_id] = shard_id;
    }
    shards[shard_id]->apply(event);
  }

  std::unordered_map<std::uint64_t, std::uint32_t> order_channel;

  std::uint32_t connect(FakeSocket& socket, std::uint32_t venue_mask,
                        Subscriber::Config config = Subscriber::Config{}) {
    auto subscriber = std::make_unique<Subscriber>(1, config, &socket);
    ClientEntitlement entitlement;
    entitlement.client_id = "test-client";
    entitlement.venue_mask = venue_mask;
    entitlement.max_subscriptions = 100;
    entitlement.allow_trades = true;
    subscriber->set_client("test-client", entitlement);
    return fanout->admit(std::move(subscriber));
  }
};

}  // namespace

TEST(a_subscriber_receives_updates_end_to_end) {
  MiniPlant plant({"AAPL", "MSFT"});
  FakeSocket socket;
  const std::uint32_t slot = plant.connect(socket, EntitlementTable::mask_of({0}));
  CHECK(slot != FanoutThread::kNoSlot);

  CHECK_EQ(static_cast<int>(plant.fanout->subscribe(slot, Symbol::from_text("AAPL"),
                                                    cascade::proto::kFlagConflated)),
           static_cast<int>(SubscribeStatus::kOk));

  plant.apply(add_order(1, "AAPL", Side::kBuy, 190.00, 100, 1));
  plant.apply(add_order(2, "AAPL", Side::kSell, 190.05, 200, 2));
  plant.fanout->poll(1'000);

  CHECK_EQ(count_frames(socket.received, cascade::proto::ClientMsgType::kSubscribeAck),
           std::size_t{1});
  CHECK_GE(count_frames(socket.received, cascade::proto::ClientMsgType::kBookUpdate),
           std::size_t{1});

  cascade::proto::BookUpdateMsg update{};
  CHECK(last_book_update(socket.received, update));
  CHECK_EQ(update.symbol, Symbol::from_text("AAPL").raw());
  CHECK_EQ(update.bid_levels, std::uint8_t{1});
  CHECK_EQ(update.ask_levels, std::uint8_t{1});
}

TEST(a_subscriber_hears_nothing_about_instruments_it_did_not_ask_for) {
  MiniPlant plant({"AAPL", "MSFT"});
  FakeSocket socket;
  const std::uint32_t slot = plant.connect(socket, EntitlementTable::mask_of({0}));
  plant.fanout->subscribe(slot, Symbol::from_text("AAPL"), cascade::proto::kFlagConflated);

  plant.apply(add_order(1, "MSFT", Side::kBuy, 420.00, 100, 1));
  plant.fanout->poll(1'000);

  cascade::proto::BookUpdateMsg update{};
  if (last_book_update(socket.received, update)) {
    CHECK_EQ(update.symbol, Symbol::from_text("AAPL").raw());
  }
}

TEST(entitlements_are_enforced_at_subscribe) {
  MiniPlant plant({"AAPL"});
  FakeSocket socket;
  // Entitled to venue 1 only; AAPL lists on venue 0.
  const std::uint32_t slot = plant.connect(socket, EntitlementTable::mask_of({1}));

  CHECK_EQ(static_cast<int>(plant.fanout->subscribe(slot, Symbol::from_text("AAPL"),
                                                    cascade::proto::kFlagConflated)),
           static_cast<int>(SubscribeStatus::kNotEntitled));

  plant.apply(add_order(1, "AAPL", Side::kBuy, 190.00, 100, 1));
  plant.fanout->poll(1'000);

  // Refused, and no market data leaked despite the instrument being active.
  CHECK_EQ(count_frames(socket.received, cascade::proto::ClientMsgType::kBookUpdate),
           std::size_t{0});
  CHECK_EQ(plant.fanout->stats().subscriptions_refused, std::uint64_t{1});
}

TEST(an_unknown_instrument_is_refused) {
  MiniPlant plant({"AAPL"});
  FakeSocket socket;
  const std::uint32_t slot = plant.connect(socket, EntitlementTable::mask_of({0, 1}));
  CHECK_EQ(static_cast<int>(plant.fanout->subscribe(slot, Symbol::from_text("NOPE"),
                                                    cascade::proto::kFlagConflated)),
           static_cast<int>(SubscribeStatus::kUnknownSymbol));
}

// The join: a client arriving after the market has moved must get the current book
// immediately, not wait for the next tick.
TEST(a_late_subscriber_is_given_the_current_book_at_once) {
  MiniPlant plant({"AAPL"});
  plant.apply(add_order(1, "AAPL", Side::kBuy, 190.00, 100, 1));
  plant.apply(add_order(2, "AAPL", Side::kSell, 190.05, 200, 2));

  FakeSocket socket;
  const std::uint32_t slot = plant.connect(socket, EntitlementTable::mask_of({0}));
  plant.fanout->subscribe(slot, Symbol::from_text("AAPL"), cascade::proto::kFlagConflated);
  plant.fanout->poll(1'000);

  cascade::proto::BookUpdateMsg update{};
  CHECK(last_book_update(socket.received, update));
  CHECK_EQ(update.bid_levels, std::uint8_t{1});
  CHECK_EQ(update.ask_levels, std::uint8_t{1});
  CHECK_GE(update.version, std::uint64_t{2});
}

TEST(two_subscribers_share_one_seqlock_read_per_instrument) {
  // Reading the image once and fanning the copy out is the point of the routing step;
  // both clients must nonetheless see the same data.
  MiniPlant plant({"AAPL"});
  FakeSocket socket_a, socket_b;
  const std::uint32_t slot_a = plant.connect(socket_a, EntitlementTable::mask_of({0}));
  const std::uint32_t slot_b = plant.connect(socket_b, EntitlementTable::mask_of({0}));
  plant.fanout->subscribe(slot_a, Symbol::from_text("AAPL"), cascade::proto::kFlagConflated);
  plant.fanout->subscribe(slot_b, Symbol::from_text("AAPL"), cascade::proto::kFlagConflated);

  plant.apply(add_order(1, "AAPL", Side::kBuy, 190.00, 100, 1));
  plant.fanout->poll(1'000);

  cascade::proto::BookUpdateMsg update_a, update_b;
  CHECK(last_book_update(socket_a.received, update_a));
  CHECK(last_book_update(socket_b.received, update_b));
  CHECK_EQ(update_a.version, update_b.version);
  CHECK_EQ(update_a.symbol, update_b.symbol);
  // One routing event for the instrument, not one per subscriber.
  CHECK_EQ(plant.fanout->stats().instruments_routed, std::uint64_t{1});
}

TEST(trades_reach_only_subscribers_that_asked_for_the_tape) {
  MiniPlant plant({"AAPL"});
  FakeSocket quotes_only, with_tape;
  const std::uint32_t slot_a = plant.connect(quotes_only, EntitlementTable::mask_of({0}));
  const std::uint32_t slot_b = plant.connect(with_tape, EntitlementTable::mask_of({0}));
  plant.fanout->subscribe(slot_a, Symbol::from_text("AAPL"), cascade::proto::kFlagConflated);
  plant.fanout->subscribe(slot_b, Symbol::from_text("AAPL"),
                          cascade::proto::kFlagConflated | cascade::proto::kFlagWithTrades);

  plant.apply(add_order(1, "AAPL", Side::kSell, 190.05, 500, 1));
  FeedEvent fill;
  fill.type = static_cast<std::uint8_t>(MsgType::kExecute);
  fill.order_id = 1;
  fill.quantity = 200;
  fill.aux_id = 0xABC;
  fill.exchange_ns = 3'000;
  fill.ingest_ns = 4'000;
  plant.apply(fill);
  plant.fanout->poll(1'000);

  CHECK_EQ(count_frames(quotes_only.received, cascade::proto::ClientMsgType::kTradeTick),
           std::size_t{0});
  CHECK_EQ(count_frames(with_tape.received, cascade::proto::ClientMsgType::kTradeTick),
           std::size_t{1});
}

// End-to-end conflation: a client that cannot keep up must be caught up with current
// state, not with a replay, and must be told how much it missed.
TEST(a_backed_up_subscriber_is_caught_up_with_current_state) {
  MiniPlant plant({"AAPL"});
  FakeSocket socket;
  socket.set_window(0);  // the client stops reading
  Subscriber::Config config;
  config.output_capacity = 512;
  config.stall_deadline_ns = 60'000'000'000ull;  // far away: not testing eviction here
  const std::uint32_t slot = plant.connect(socket, EntitlementTable::mask_of({0}), config);
  plant.fanout->subscribe(slot, Symbol::from_text("AAPL"), cascade::proto::kFlagConflated);

  for (std::uint64_t i = 1; i <= 500; ++i) {
    plant.apply(add_order(i, "AAPL", Side::kBuy, 190.00 + static_cast<double>(i) * 0.01,
                          100, i));
    plant.fanout->poll(1'000);
  }

  Subscriber* subscriber = plant.fanout->subscriber_at(slot);
  CHECK(subscriber != nullptr);
  if (!subscriber) return;
  // Before the client drains, all we know is that updates could not be written.
  // How much information it actually skipped is only knowable once one gets through.
  CHECK_GE(subscriber->stats().offers_deferred, std::uint64_t{1});

  // The client starts reading again.
  socket.set_window(1u << 20);
  plant.fanout->poll(2'000);
  plant.fanout->poll(3'000);

  cascade::proto::BookUpdateMsg update{};
  std::vector<cascade::proto::PriceLevel> levels;
  CHECK(last_book_update(socket.received, update, &levels));
  // It is handed the *latest* best bid, not the next one in a queue.
  CHECK_GE(update.bid_levels, std::uint8_t{1});
  CHECK(!levels.empty());
  if (!levels.empty()) {
    CHECK_EQ(levels[0].price, cascade::price_from_double(190.00 + 500 * 0.01));
  }
  CHECK_GE(update.conflated_count, std::uint32_t{1});
}

TEST(a_persistently_slow_subscriber_is_evicted_with_a_reason) {
  MiniPlant plant({"AAPL"});
  FakeSocket socket;
  socket.set_window(0);
  Subscriber::Config config;
  config.output_capacity = 512;
  config.stall_deadline_ns = 1'000'000;  // 1ms
  const std::uint32_t slot = plant.connect(socket, EntitlementTable::mask_of({0}), config);
  plant.fanout->subscribe(slot, Symbol::from_text("AAPL"), cascade::proto::kFlagConflated);

  for (std::uint64_t i = 1; i <= 100; ++i) {
    plant.apply(add_order(i, "AAPL", Side::kBuy, 190.00 + static_cast<double>(i) * 0.01,
                          100, i));
  }
  plant.fanout->poll(1'000);
  CHECK_EQ(plant.fanout->subscriber_count(), std::size_t{1});

  plant.fanout->poll(1'000 + 5'000'000);  // past the deadline
  CHECK_EQ(plant.fanout->subscriber_count(), std::size_t{0});
  CHECK_EQ(plant.fanout->stats().subscribers_evicted, std::uint64_t{1});

  // The plant discards the client's backlog and attempts the eviction notice, but a
  // socket that is refusing every byte cannot receive that either -- so the notice is
  // best-effort by nature, and the test asserts the eviction itself rather than
  // pretending a wedged socket would deliver it. That the notice is encoded correctly
  // is covered where it can be observed, in the subscriber unit tests.
  Subscriber* gone = plant.fanout->subscriber_at(slot);
  CHECK(gone == nullptr);
}

// A departed client must stop costing the shard work for the rest of the session.
TEST(releasing_a_subscriber_withdraws_shard_interest) {
  MiniPlant plant({"AAPL"});
  FakeSocket socket;
  const std::uint32_t slot = plant.connect(socket, EntitlementTable::mask_of({0}));
  plant.fanout->subscribe(slot, Symbol::from_text("AAPL"), cascade::proto::kFlagConflated);

  plant.apply(add_order(1, "AAPL", Side::kBuy, 190.00, 100, 1));
  plant.fanout->poll(1'000);
  const std::uint64_t routed_before = plant.fanout->stats().instruments_routed;

  plant.fanout->release(slot);
  CHECK_EQ(plant.fanout->subscriber_count(), std::size_t{0});

  for (std::uint64_t i = 2; i <= 50; ++i) {
    plant.apply(add_order(i, "AAPL", Side::kBuy, 190.00 + static_cast<double>(i) * 0.01,
                          100, i));
  }
  plant.fanout->poll(2'000);
  CHECK_EQ(plant.fanout->stats().instruments_routed, routed_before);
}

TEST(unsubscribing_stops_delivery_but_keeps_the_connection) {
  MiniPlant plant({"AAPL", "MSFT"});
  FakeSocket socket;
  const std::uint32_t slot = plant.connect(socket, EntitlementTable::mask_of({0}));
  plant.fanout->subscribe(slot, Symbol::from_text("AAPL"), cascade::proto::kFlagConflated);
  plant.fanout->subscribe(slot, Symbol::from_text("MSFT"), cascade::proto::kFlagConflated);

  plant.fanout->unsubscribe(slot, Symbol::from_text("AAPL"));
  socket.received.clear();

  plant.apply(add_order(1, "AAPL", Side::kBuy, 190.00, 100, 1));
  plant.apply(add_order(2, "MSFT", Side::kBuy, 420.00, 100, 2));
  plant.fanout->poll(1'000);

  cascade::proto::BookUpdateMsg update{};
  CHECK(last_book_update(socket.received, update));
  CHECK_EQ(update.symbol, Symbol::from_text("MSFT").raw());
  CHECK_EQ(plant.fanout->subscriber_count(), std::size_t{1});
}

TEST(a_subscription_limit_is_enforced) {
  MiniPlant plant({"AAPL", "MSFT", "IBM"});
  FakeSocket socket;
  auto subscriber = std::make_unique<Subscriber>(1, Subscriber::Config{}, &socket);
  ClientEntitlement entitlement;
  entitlement.venue_mask = EntitlementTable::mask_of({0});
  entitlement.max_subscriptions = 2;
  subscriber->set_client("capped", entitlement);
  const std::uint32_t slot = plant.fanout->admit(std::move(subscriber));

  CHECK_EQ(static_cast<int>(plant.fanout->subscribe(slot, Symbol::from_text("AAPL"), 1)),
           static_cast<int>(SubscribeStatus::kOk));
  CHECK_EQ(static_cast<int>(plant.fanout->subscribe(slot, Symbol::from_text("MSFT"), 1)),
           static_cast<int>(SubscribeStatus::kOk));
  CHECK_EQ(static_cast<int>(plant.fanout->subscribe(slot, Symbol::from_text("IBM"), 1)),
           static_cast<int>(SubscribeStatus::kLimitExceeded));
}

TEST(incremental_delivery_is_downgraded_when_not_entitled) {
  // Un-conflated delivery costs the plant a publication log, so it is a separate grant
  // from the right to see the instrument. Downgrading beats refusing: the client still
  // gets its data, just conflated.
  MiniPlant plant({"AAPL"});
  FakeSocket socket;
  auto subscriber = std::make_unique<Subscriber>(1, Subscriber::Config{}, &socket);
  ClientEntitlement entitlement;
  entitlement.venue_mask = EntitlementTable::mask_of({0});
  entitlement.max_subscriptions = 10;
  entitlement.allow_incremental = false;
  subscriber->set_client("basic", entitlement);
  const std::uint32_t slot = plant.fanout->admit(std::move(subscriber));

  CHECK_EQ(static_cast<int>(plant.fanout->subscribe(slot, Symbol::from_text("AAPL"),
                                                    cascade::proto::kFlagIncremental)),
           static_cast<int>(SubscribeStatus::kOk));

  Subscriber* admitted = plant.fanout->subscriber_at(slot);
  CHECK(admitted != nullptr);
  if (!admitted) return;
  const auto* subscription = admitted->find_subscription(Symbol::from_text("AAPL"));
  CHECK(subscription != nullptr);
  if (!subscription) return;
  CHECK_EQ(subscription->flags & cascade::proto::kFlagIncremental, 0);
  CHECK((subscription->flags & cascade::proto::kFlagConflated) != 0);
}

TEST(an_idle_tier_reports_no_work) {
  MiniPlant plant({"AAPL"});
  FakeSocket socket;
  const std::uint32_t slot = plant.connect(socket, EntitlementTable::mask_of({0}));
  plant.fanout->subscribe(slot, Symbol::from_text("AAPL"), cascade::proto::kFlagConflated);
  plant.fanout->poll(1'000);
  CHECK(!plant.fanout->has_work());

  plant.apply(add_order(1, "AAPL", Side::kBuy, 190.00, 100, 1));
  CHECK(plant.fanout->has_work());
}
