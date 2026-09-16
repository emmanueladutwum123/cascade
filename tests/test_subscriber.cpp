// SPDX-License-Identifier: Apache-2.0
#include "cascade/dist/subscriber.hpp"

#include <vector>

#include "cascade/dist/publication_log.hpp"
#include "test_harness.hpp"

using cascade::Symbol;
using cascade::book::BookImage;
using cascade::dist::ClientEntitlement;
using cascade::dist::EntitlementTable;
using cascade::dist::PublicationLog;
using cascade::dist::Subscriber;
using cascade::dist::Subscription;
using cascade::feed::TradeEvent;
using cascade::net::OutputBuffer;

namespace {

/// A socket whose capacity the test controls exactly. This is what makes the
/// conflation and eviction state machine testable at all: a real socket's buffer
/// behaviour is unrepeatable, so backpressure would be untestable against one.
class FakeSocket : public cascade::net::ByteSink {
 public:
  long write_some(const OutputBuffer::Span* spans, int span_count) override {
    if (fail_) return -1;
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

  /// Bytes the socket will accept on each flush. Zero simulates EAGAIN.
  void set_window(std::size_t bytes) { window_ = bytes; }
  void fail_next() { fail_ = true; }

  std::vector<unsigned char> received;

 private:
  std::size_t window_{1u << 30};
  bool fail_{false};
};

BookImage make_image(const char* symbol, std::uint64_t version, std::int64_t bid,
                     std::int64_t ask) {
  BookImage image;
  image.symbol = Symbol::from_text(symbol).raw();
  image.version = version;
  image.bid_levels = 1;
  image.ask_levels = 1;
  image.bids[0] = cascade::proto::PriceLevel{bid, 100, 1};
  image.asks[0] = cascade::proto::PriceLevel{ask, 100, 1};
  return image;
}

/// Count frames of a given type in a raw byte stream, exactly as a client would parse.
std::size_t count_frames(const std::vector<unsigned char>& bytes,
                         cascade::proto::ClientMsgType type) {
  std::size_t offset = 0, found = 0;
  while (offset + sizeof(cascade::proto::FrameHeader) <= bytes.size()) {
    cascade::proto::FrameHeader header;
    std::memcpy(&header, bytes.data() + offset, sizeof(header));
    const std::size_t total = sizeof(header) + header.payload_bytes;
    if (offset + total > bytes.size()) break;
    if (header.type == static_cast<std::uint8_t>(type)) ++found;
    offset += total;
  }
  return found;
}

Subscriber::Config small_config() {
  Subscriber::Config config;
  config.output_capacity = 512;
  config.trade_queue_capacity = 4;
  config.stall_deadline_ns = 1'000'000;  // 1ms
  return config;
}

}  // namespace

TEST(book_updates_are_framed_and_written) {
  FakeSocket socket;
  Subscriber subscriber(1, Subscriber::Config{}, &socket);
  const std::uint32_t sub = subscriber.add_subscription(Symbol::from_text("AAPL"), 0, 0,
                                                        cascade::proto::kFlagConflated, 0);

  subscriber.offer_book(sub, make_image("AAPL", 1, 1000, 1010));
  CHECK(subscriber.flush(0));

  CHECK_EQ(subscriber.stats().book_updates_sent, std::uint64_t{1});
  CHECK_EQ(count_frames(socket.received, cascade::proto::ClientMsgType::kBookUpdate),
           std::size_t{1});

  // The frame must round-trip: this is the contract a client parser depends on.
  cascade::proto::FrameHeader header;
  std::memcpy(&header, socket.received.data(), sizeof(header));
  cascade::proto::BookUpdateMsg body;
  std::memcpy(&body, socket.received.data() + sizeof(header), sizeof(body));
  CHECK_EQ(body.symbol, Symbol::from_text("AAPL").raw());
  CHECK_EQ(body.version, std::uint64_t{1});
  CHECK_EQ(body.bid_levels, std::uint8_t{1});
  CHECK_EQ(body.conflated_count, std::uint32_t{0});
}

TEST(stale_versions_are_not_resent) {
  FakeSocket socket;
  Subscriber subscriber(1, Subscriber::Config{}, &socket);
  const std::uint32_t sub = subscriber.add_subscription(Symbol::from_text("AAPL"), 0, 0,
                                                        cascade::proto::kFlagConflated, 0);
  subscriber.offer_book(sub, make_image("AAPL", 5, 1000, 1010));
  subscriber.offer_book(sub, make_image("AAPL", 5, 1000, 1010));  // same version
  subscriber.offer_book(sub, make_image("AAPL", 3, 900, 910));    // older
  CHECK_EQ(subscriber.stats().book_updates_sent, std::uint64_t{1});
}

// The heart of the design. A backed-up socket must not queue superseded book states;
// it must remember that the instrument is owed and send whatever is current later.
TEST(a_full_socket_conflates_rather_than_queueing) {
  FakeSocket socket;
  socket.set_window(0);  // EAGAIN: nothing drains
  Subscriber subscriber(1, small_config(), &socket);
  const std::uint32_t sub = subscriber.add_subscription(Symbol::from_text("AAPL"), 0, 0,
                                                        cascade::proto::kFlagConflated, 0);

  // Fill the 512-byte buffer, then keep offering.
  for (std::uint64_t version = 1; version <= 200; ++version) {
    subscriber.offer_book(sub, make_image("AAPL", version, 1000 + static_cast<std::int64_t>(version), 2000));
    subscriber.flush(0);
  }

  // Attempts that could not be written. Distinct from book_updates_conflated, which
  // counts information actually skipped and is only knowable when an update does go out.
  CHECK_GE(subscriber.stats().offers_deferred, std::uint64_t{100});
  // Only as many as the 512-byte buffer could hold; everything after that conflated.
  CHECK_GE(subscriber.stats().book_updates_sent, std::uint64_t{1});
  CHECK_LE(subscriber.stats().book_updates_sent, std::uint64_t{10});
  // Memory is bounded by subscriptions, not by how far behind the client is.
  CHECK_LE(subscriber.pending_bytes(), std::size_t{512});
  // Exactly one instrument is owed, however many updates were collapsed.
  CHECK_EQ(subscriber.pending_subscriptions().size(), std::size_t{1});
  CHECK_EQ(subscriber.pending_subscriptions()[0], sub);
}

TEST(a_drained_socket_sends_current_state_and_reports_what_was_collapsed) {
  FakeSocket socket;
  socket.set_window(0);
  Subscriber subscriber(1, small_config(), &socket);
  const std::uint32_t sub = subscriber.add_subscription(Symbol::from_text("AAPL"), 0, 0,
                                                        cascade::proto::kFlagConflated, 0);

  for (std::uint64_t version = 1; version <= 100; ++version) {
    subscriber.offer_book(sub, make_image("AAPL", version, 1000, 2000));
    subscriber.flush(0);
  }
  CHECK_GE(subscriber.stats().offers_deferred, std::uint64_t{1});

  // The client catches up. It must be given the *latest* book, not the backlog.
  socket.set_window(1u << 20);
  subscriber.flush(0);
  socket.received.clear();
  subscriber.clear_pending_queue();
  subscriber.offer_book(sub, make_image("AAPL", 500, 1234, 5678));
  subscriber.flush(0);

  cascade::proto::FrameHeader header;
  std::memcpy(&header, socket.received.data(), sizeof(header));
  cascade::proto::BookUpdateMsg body;
  std::memcpy(&body, socket.received.data() + sizeof(header), sizeof(body));
  CHECK_EQ(body.version, std::uint64_t{500});
  // Levels are appended after the fixed header rather than being struct members, so a
  // thinly quoted instrument costs only the levels it actually has.
  CHECK_EQ(body.bid_levels, std::uint8_t{1});
  cascade::proto::PriceLevel level;
  std::memcpy(&level, socket.received.data() + sizeof(header) + sizeof(body),
              sizeof(level));
  CHECK_EQ(level.price, std::int64_t{1234});
  // The count of collapsed updates is reported rather than hidden: a conflated feed
  // that hides its own conflation makes a quiet market indistinguishable from a
  // saturated link, and that distinction matters to anything trading on it.
  CHECK_GE(body.conflated_count, std::uint32_t{1});
}

TEST(trades_are_queued_not_collapsed) {
  FakeSocket socket;
  socket.set_window(0);
  Subscriber subscriber(1, small_config(), &socket);

  for (std::uint64_t i = 0; i < 3; ++i) {
    TradeEvent trade;
    trade.symbol = Symbol::from_text("AAPL").raw();
    trade.match_id = i;
    trade.price = 1000;
    trade.quantity = 10;
    subscriber.offer_trade(trade);
  }
  CHECK(!subscriber.evicted());

  // Every print must survive to the wire once the client drains: two trades are two
  // facts and collapsing them would fabricate a tape.
  socket.set_window(1u << 20);
  for (int i = 0; i < 10; ++i) subscriber.flush(0);
  CHECK_EQ(count_frames(socket.received, cascade::proto::ClientMsgType::kTradeTick),
           std::size_t{3});
}

TEST(trade_queue_overrun_evicts_rather_than_dropping_a_print) {
  FakeSocket socket;
  socket.set_window(0);
  Subscriber subscriber(1, small_config(), &socket);  // trade queue holds 4

  for (std::uint64_t i = 0; i < 100; ++i) {
    TradeEvent trade;
    trade.symbol = Symbol::from_text("AAPL").raw();
    trade.match_id = i;
    subscriber.offer_trade(trade);
  }
  CHECK(subscriber.evicted());
  CHECK_EQ(static_cast<int>(subscriber.evict_reason()),
           static_cast<int>(cascade::proto::EvictReason::kTradeQueueOverrun));
}

// A momentarily full socket is normal and must not disconnect anyone; a persistently
// full one means the client is structurally too slow and waiting will not help.
TEST(slow_consumer_eviction_is_time_based_not_depth_based) {
  FakeSocket socket;
  socket.set_window(0);
  Subscriber subscriber(1, small_config(), &socket);  // 1ms stall deadline
  const std::uint32_t sub = subscriber.add_subscription(Symbol::from_text("AAPL"), 0, 0,
                                                        cascade::proto::kFlagConflated, 0);

  for (std::uint64_t version = 1; version <= 50; ++version) {
    subscriber.offer_book(sub, make_image("AAPL", version, 1000, 2000));
  }

  // Buffer is full, but only briefly. Not an eviction.
  CHECK(subscriber.flush(1'000));
  CHECK(!subscriber.evicted());
  CHECK(subscriber.flush(1'000 + 500'000));
  CHECK(!subscriber.evicted());
  CHECK_GE(subscriber.stalled_nanos(1'000 + 500'000), std::uint64_t{1});

  // Still full past the deadline. Now it goes.
  CHECK(!subscriber.flush(1'000 + 2'000'000));
  CHECK(subscriber.evicted());
  CHECK_EQ(static_cast<int>(subscriber.evict_reason()),
           static_cast<int>(cascade::proto::EvictReason::kSlowConsumer));
}

TEST(a_recovering_client_resets_the_stall_clock) {
  FakeSocket socket;
  socket.set_window(0);
  Subscriber subscriber(1, small_config(), &socket);
  const std::uint32_t sub = subscriber.add_subscription(Symbol::from_text("AAPL"), 0, 0,
                                                        cascade::proto::kFlagConflated, 0);
  for (std::uint64_t version = 1; version <= 50; ++version) {
    subscriber.offer_book(sub, make_image("AAPL", version, 1000, 2000));
  }

  subscriber.flush(1'000);
  CHECK_GE(subscriber.stalled_nanos(1'000), std::uint64_t{0});

  socket.set_window(1u << 20);   // the client catches up
  subscriber.flush(1'000 + 900'000);
  CHECK_EQ(subscriber.stalled_nanos(1'000 + 900'000), std::uint64_t{0});

  // Well past the original deadline, but the clock restarted, so no eviction.
  socket.set_window(0);
  for (std::uint64_t version = 51; version <= 100; ++version) {
    subscriber.offer_book(sub, make_image("AAPL", version, 1000, 2000));
  }
  CHECK(subscriber.flush(1'000 + 950'000));
  CHECK(!subscriber.evicted());
}

TEST(a_partial_socket_write_is_resumed_not_repeated) {
  FakeSocket socket;
  socket.set_window(20);  // accepts a trickle per flush
  Subscriber subscriber(1, Subscriber::Config{}, &socket);
  const std::uint32_t sub = subscriber.add_subscription(Symbol::from_text("AAPL"), 0, 0,
                                                        cascade::proto::kFlagConflated, 0);
  subscriber.offer_book(sub, make_image("AAPL", 1, 1000, 1010));
  const std::size_t total = subscriber.pending_bytes();

  for (int i = 0; i < 100 && subscriber.pending_bytes() > 0; ++i) subscriber.flush(0);

  CHECK_EQ(subscriber.pending_bytes(), std::size_t{0});
  CHECK_EQ(socket.received.size(), total);  // every byte exactly once, in order
  CHECK_EQ(count_frames(socket.received, cascade::proto::ClientMsgType::kBookUpdate),
           std::size_t{1});
}

TEST(a_frame_is_never_half_written) {
  // A frame that did not fit must not be partially emitted: the client's parser would
  // never resynchronise.
  FakeSocket socket;
  socket.set_window(0);
  Subscriber::Config config = small_config();
  config.output_capacity = 128;
  Subscriber subscriber(1, config, &socket);
  const std::uint32_t sub = subscriber.add_subscription(Symbol::from_text("AAPL"), 0, 0,
                                                        cascade::proto::kFlagConflated, 0);

  for (std::uint64_t version = 1; version <= 50; ++version) {
    subscriber.offer_book(sub, make_image("AAPL", version, 1000, 2000));
  }
  socket.set_window(1u << 20);
  subscriber.flush(0);

  // Every byte that made it out must parse as whole frames, with nothing left over.
  std::size_t offset = 0;
  while (offset + sizeof(cascade::proto::FrameHeader) <= socket.received.size()) {
    cascade::proto::FrameHeader header;
    std::memcpy(&header, socket.received.data() + offset, sizeof(header));
    offset += sizeof(header) + header.payload_bytes;
  }
  CHECK_EQ(offset, socket.received.size());
}

TEST(a_dead_socket_ends_the_connection) {
  FakeSocket socket;
  Subscriber subscriber(1, Subscriber::Config{}, &socket);
  const std::uint32_t sub = subscriber.add_subscription(Symbol::from_text("AAPL"), 0, 0,
                                                        cascade::proto::kFlagConflated, 0);
  subscriber.offer_book(sub, make_image("AAPL", 1, 1000, 1010));
  socket.fail_next();
  CHECK(!subscriber.flush(0));
}

TEST(unsubscribing_stops_delivery) {
  FakeSocket socket;
  Subscriber subscriber(1, Subscriber::Config{}, &socket);
  const std::uint32_t sub = subscriber.add_subscription(Symbol::from_text("AAPL"), 0, 0,
                                                        cascade::proto::kFlagConflated, 0);
  subscriber.offer_book(sub, make_image("AAPL", 1, 1000, 1010));
  CHECK_EQ(subscriber.active_subscription_count(), std::size_t{1});

  subscriber.remove_subscription(Symbol::from_text("AAPL"));
  CHECK_EQ(subscriber.active_subscription_count(), std::size_t{0});
  subscriber.offer_book(sub, make_image("AAPL", 2, 1000, 1010));
  CHECK_EQ(subscriber.stats().book_updates_sent, std::uint64_t{1});
}

TEST(eviction_notice_names_the_reason) {
  FakeSocket socket;
  Subscriber subscriber(1, Subscriber::Config{}, &socket);
  subscriber.encode_evicted(cascade::proto::EvictReason::kSlowConsumer, 12'345);
  subscriber.flush(0);

  CHECK_EQ(count_frames(socket.received, cascade::proto::ClientMsgType::kEvicted),
           std::size_t{1});
  cascade::proto::EvictedMsg body;
  std::memcpy(&body, socket.received.data() + sizeof(cascade::proto::FrameHeader),
              sizeof(body));
  CHECK_EQ(body.reason, static_cast<std::uint8_t>(cascade::proto::EvictReason::kSlowConsumer));
  CHECK_EQ(body.stalled_nanos, std::uint64_t{12'345});
}

// --- publication log ------------------------------------------------------

TEST(publication_log_replays_in_order) {
  PublicationLog log(8);
  for (std::uint64_t version = 1; version <= 5; ++version) {
    log.append(make_image("AAPL", version, 1000, 2000));
  }
  CHECK_EQ(log.write_position(), std::uint64_t{5});

  BookImage image;
  for (std::uint64_t position = 0; position < 5; ++position) {
    CHECK(log.read(position, image));
    CHECK_EQ(image.version, position + 1);
  }
  CHECK(!log.read(5, image));  // nothing published there yet
}

TEST(publication_log_detects_a_reader_that_fell_behind) {
  // The log is bounded, so a reader can be lapped. That must be detected and
  // resynchronised, never silently returned as a mixture of two entries.
  PublicationLog log(8);
  for (std::uint64_t version = 1; version <= 20; ++version) {
    log.append(make_image("AAPL", version, 1000, 2000));
  }
  BookImage image;
  CHECK(!log.read(0, image));   // long overwritten
  CHECK(!log.read(11, image));  // just outside the window
  CHECK(log.read(12, image));   // oldest still readable
  CHECK_EQ(image.version, std::uint64_t{13});
  CHECK_EQ(log.oldest_position(), std::uint64_t{12});
}

// The join contract: snapshot at version V, then every log entry above V, exactly once.
TEST(snapshot_and_stream_join_has_no_gap_and_no_duplicate) {
  PublicationLog log(64);
  for (std::uint64_t version = 1; version <= 10; ++version) {
    log.append(make_image("AAPL", version, 1000, 2000));
  }

  // A subscriber arrives: it takes the current image, then starts reading forward.
  const BookImage snapshot = make_image("AAPL", 10, 1000, 2000);
  const std::uint64_t cursor = log.write_position();

  // More publishes land, including one that raced the snapshot.
  for (std::uint64_t version = 11; version <= 15; ++version) {
    log.append(make_image("AAPL", version, 1000, 2000));
  }

  std::vector<std::uint64_t> delivered;
  BookImage image;
  for (std::uint64_t position = cursor; log.read(position, image); ++position) {
    if (image.version <= snapshot.version) continue;  // the filter that makes it exact
    delivered.push_back(image.version);
  }

  CHECK_EQ(delivered.size(), std::size_t{5});
  for (std::size_t i = 0; i < delivered.size(); ++i) {
    CHECK_EQ(delivered[i], static_cast<std::uint64_t>(11 + i));
  }
}

// --- entitlements ---------------------------------------------------------

TEST(entitlements_gate_by_venue) {
  EntitlementTable table;
  table.register_venue(0, "NASDAQ");
  table.register_venue(1, "NYSE");
  table.assign_symbol(Symbol::from_text("AAPL"), 0);
  table.assign_symbol(Symbol::from_text("BRK.A"), 1);

  ClientEntitlement granted;
  granted.client_id = "desk-1";
  granted.venue_mask = EntitlementTable::mask_of({0});
  granted.max_subscriptions = 100;
  table.grant("token-abc", granted);

  ClientEntitlement resolved;
  CHECK(table.resolve("token-abc", resolved));
  CHECK_EQ(resolved.client_id, std::string("desk-1"));

  CHECK(EntitlementTable::permits(resolved.venue_mask,
                                  table.venue_for(Symbol::from_text("AAPL"))));
  CHECK(!EntitlementTable::permits(resolved.venue_mask,
                                   table.venue_for(Symbol::from_text("BRK.A"))));
}

TEST(an_unknown_symbol_is_refused_rather_than_defaulted) {
  // Failing open here would hand out data for any symbol a client cared to guess.
  EntitlementTable table;
  table.register_venue(0, "NASDAQ");
  const std::uint32_t everything = 0xFFFFFFFFu;
  CHECK_EQ(static_cast<int>(table.venue_for(Symbol::from_text("NOPE"))),
           static_cast<int>(cascade::dist::kUnknownVenue));
  CHECK(!EntitlementTable::permits(everything, table.venue_for(Symbol::from_text("NOPE"))));
}

TEST(an_unknown_token_does_not_resolve) {
  EntitlementTable table;
  ClientEntitlement resolved;
  CHECK(!table.resolve("no-such-token", resolved));
}

TEST(revocation_bumps_the_version_so_connections_notice) {
  // Checking only at subscribe time would leave a revoked client streaming until it
  // reconnected, which can be hours.
  EntitlementTable table;
  ClientEntitlement granted;
  granted.venue_mask = EntitlementTable::mask_of({0, 1});
  table.grant("token", granted);

  const std::uint64_t before = table.version();
  ClientEntitlement resolved;
  CHECK(table.resolve("token", resolved));

  table.revoke("token");
  CHECK_GE(table.version(), before + 1);
  CHECK(!table.resolve("token", resolved));
  CHECK_EQ(table.client_count(), std::size_t{0});
}
