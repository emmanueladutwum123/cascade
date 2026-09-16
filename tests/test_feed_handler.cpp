// SPDX-License-Identifier: Apache-2.0
#include "cascade/feed/feed_handler.hpp"

#include <vector>

#include "test_feed_handler.hpp"
#include "test_harness.hpp"

using cascade::feed::FeedEvent;
using cascade::feed::FeedHandler;
using cascade::feed::RecoveryRequester;
using cascade::test::add_order_packet;
using cascade::test::PacketBuilder;

namespace {

/// Collects delivered events so a test can assert on exact order-id sequence.
struct Collector {
  std::vector<std::uint64_t> order_ids;
  std::vector<std::uint8_t> flags;
  void operator()(const FeedEvent& event) {
    order_ids.push_back(event.order_id);
    flags.push_back(event.flags);
  }
};

struct RecordingRequester : RecoveryRequester {
  struct Ask { std::uint64_t first; std::uint16_t count; };
  std::vector<Ask> asks;
  void request_retransmit(std::uint64_t first, std::uint16_t count) override {
    asks.push_back({first, count});
  }
};

FeedHandler::Config fast_config() {
  FeedHandler::Config config;
  config.reorder_window = 8;
  config.gap_grace_ns = 1'000'000;         // 1ms
  config.recovery_deadline_ns = 10'000'000;  // 10ms
  return config;
}

}  // namespace

TEST(in_order_packets_deliver_every_message) {
  FeedHandler handler(fast_config());
  PacketBuilder builder;
  Collector collected;

  for (int packet = 0; packet < 5; ++packet) {
    const std::uint64_t sequence = 1 + static_cast<std::uint64_t>(packet) * 3;
    const auto bytes = add_order_packet(builder, sequence, sequence, 3);
    handler.on_datagram(bytes.data(), bytes.size(), 1'000, collected);
  }

  CHECK_EQ(collected.order_ids.size(), std::size_t{15});
  for (std::size_t i = 0; i < collected.order_ids.size(); ++i) {
    CHECK_EQ(collected.order_ids[i], static_cast<std::uint64_t>(i + 1));
  }
  CHECK_EQ(handler.stats().gaps_detected, std::uint64_t{0});
  CHECK_EQ(handler.expected_sequence(), std::uint64_t{16});
}

// Reordering is not loss. A packet arriving early must be held and delivered in its
// proper place, without a retransmit request — otherwise every multipath hiccup on the
// network turns into load on the recovery server.
TEST(reordered_packets_are_held_and_delivered_in_order) {
  FeedHandler handler(fast_config());
  RecordingRequester requester;
  handler.set_recovery_requester(&requester);
  PacketBuilder builder;
  Collector collected;

  const auto packet1 = add_order_packet(builder, 1, 1, 3);
  const auto packet2 = add_order_packet(builder, 4, 4, 3);
  const auto packet3 = add_order_packet(builder, 7, 7, 3);

  handler.on_datagram(packet1.data(), packet1.size(), 1'000, collected);
  handler.on_datagram(packet3.data(), packet3.size(), 1'000, collected);  // early
  CHECK_EQ(collected.order_ids.size(), std::size_t{3});  // 7-9 held, not delivered

  handler.on_datagram(packet2.data(), packet2.size(), 1'000, collected);  // the filler

  CHECK_EQ(collected.order_ids.size(), std::size_t{9});
  for (std::size_t i = 0; i < 9; ++i) {
    CHECK_EQ(collected.order_ids[i], static_cast<std::uint64_t>(i + 1));
  }
  CHECK_EQ(handler.stats().packets_reordered, std::uint64_t{1});
  CHECK(requester.asks.empty());  // resolved before the grace period: no retransmit
  CHECK(!handler.in_gap());
}

TEST(deeply_reordered_run_is_released_in_one_cascade) {
  FeedHandler handler(fast_config());
  PacketBuilder builder;
  Collector collected;
  // Without this the handler would adopt the first packet it sees as the session
  // start -- correct behaviour when joining a live feed, but it would mean the
  // out-of-order packet defined the baseline and there would be no gap to test.
  handler.start_session(builder.session(), 1);

  std::vector<std::vector<unsigned char>> packets;
  for (int i = 0; i < 6; ++i) {
    const std::uint64_t sequence = 1 + static_cast<std::uint64_t>(i) * 2;
    packets.push_back(add_order_packet(builder, sequence, sequence, 2));
  }

  // Deliver 2..5 first, then the packet that unblocks all of them at once.
  for (int i = 5; i >= 1; --i) {
    handler.on_datagram(packets[static_cast<std::size_t>(i)].data(),
                        packets[static_cast<std::size_t>(i)].size(), 1'000, collected);
  }
  CHECK_EQ(collected.order_ids.size(), std::size_t{0});

  handler.on_datagram(packets[0].data(), packets[0].size(), 1'000, collected);
  CHECK_EQ(collected.order_ids.size(), std::size_t{12});
  for (std::size_t i = 0; i < 12; ++i) {
    CHECK_EQ(collected.order_ids[i], static_cast<std::uint64_t>(i + 1));
  }
}

TEST(duplicate_packets_are_dropped_not_replayed) {
  // Replaying an order event would double-apply it and permanently corrupt the book,
  // so a retransmit racing the original must be recognised and discarded.
  FeedHandler handler(fast_config());
  PacketBuilder builder;
  Collector collected;

  const auto packet = add_order_packet(builder, 1, 1, 3);
  handler.on_datagram(packet.data(), packet.size(), 1'000, collected);
  handler.on_datagram(packet.data(), packet.size(), 1'000, collected);
  handler.on_datagram(packet.data(), packet.size(), 1'000, collected);

  CHECK_EQ(collected.order_ids.size(), std::size_t{3});
  CHECK_EQ(handler.stats().packets_duplicate, std::uint64_t{2});
}

TEST(a_gap_triggers_a_retransmit_request_only_after_the_grace_period) {
  FeedHandler handler(fast_config());
  RecordingRequester requester;
  handler.set_recovery_requester(&requester);
  PacketBuilder builder;
  Collector collected;

  const auto packet1 = add_order_packet(builder, 1, 1, 3);
  const auto packet3 = add_order_packet(builder, 7, 7, 3);
  handler.on_datagram(packet1.data(), packet1.size(), 1'000, collected);
  handler.on_datagram(packet3.data(), packet3.size(), 1'000, collected);

  CHECK(handler.in_gap());
  CHECK_EQ(handler.stats().gaps_detected, std::uint64_t{1});

  // Inside the grace period: still assumed to be reordering.
  handler.poll(1'000 + 500'000, collected);
  CHECK(requester.asks.empty());

  // Past it: now we ask.
  handler.poll(1'000 + 2'000'000, collected);
  CHECK_EQ(requester.asks.size(), std::size_t{1});
  CHECK_EQ(requester.asks[0].first, std::uint64_t{4});
  CHECK_EQ(requester.asks[0].count, std::uint16_t{3});

  // And only once, while the same gap stays open.
  handler.poll(1'000 + 3'000'000, collected);
  CHECK_EQ(requester.asks.size(), std::size_t{1});
}

TEST(a_recovered_range_closes_the_gap_and_releases_held_packets) {
  FeedHandler handler(fast_config());
  PacketBuilder builder;
  Collector collected;

  const auto packet1 = add_order_packet(builder, 1, 1, 3);
  const auto packet3 = add_order_packet(builder, 7, 7, 3);
  handler.on_datagram(packet1.data(), packet1.size(), 1'000, collected);
  handler.on_datagram(packet3.data(), packet3.size(), 1'000, collected);

  // The recovery server returns the missing run, payload only.
  const auto recovery = add_order_packet(builder, 4, 4, 3);
  const unsigned char* payload = recovery.data() + sizeof(cascade::proto::PacketHeader);
  const std::size_t payload_bytes = recovery.size() - sizeof(cascade::proto::PacketHeader);
  handler.on_recovered(payload, payload_bytes, 4, 3, 2'000, collected);

  CHECK_EQ(collected.order_ids.size(), std::size_t{9});
  for (std::size_t i = 0; i < 9; ++i) {
    CHECK_EQ(collected.order_ids[i], static_cast<std::uint64_t>(i + 1));
  }
  CHECK(!handler.in_gap());
  CHECK_EQ(handler.stats().gaps_recovered, std::uint64_t{1});
  CHECK_EQ(handler.stats().messages_recovered, std::uint64_t{3});
  // Recovered messages are tagged, so a consumer can tell them from live ones.
  CHECK((collected.flags[3] & cascade::feed::kEventRecovered) != 0);
  CHECK_EQ(collected.flags[0] & cascade::feed::kEventRecovered, 0);
}

// A retransmit may legitimately return more than was asked for. Trimming has to happen
// message by message: dropping the whole packet would re-open the gap, and replaying
// the overlap would corrupt the book.
TEST(an_overlapping_retransmit_is_trimmed_not_replayed) {
  FeedHandler handler(fast_config());
  PacketBuilder builder;
  Collector collected;

  const auto packet1 = add_order_packet(builder, 1, 1, 3);
  handler.on_datagram(packet1.data(), packet1.size(), 1'000, collected);
  CHECK_EQ(handler.expected_sequence(), std::uint64_t{4});

  // Server replays [2,7) though we only needed [4,7).
  const auto recovery = add_order_packet(builder, 2, 2, 5);
  const unsigned char* payload = recovery.data() + sizeof(cascade::proto::PacketHeader);
  const std::size_t payload_bytes = recovery.size() - sizeof(cascade::proto::PacketHeader);
  handler.on_recovered(payload, payload_bytes, 2, 5, 2'000, collected);

  CHECK_EQ(collected.order_ids.size(), std::size_t{6});
  for (std::size_t i = 0; i < 6; ++i) {
    CHECK_EQ(collected.order_ids[i], static_cast<std::uint64_t>(i + 1));
  }
  CHECK_EQ(handler.expected_sequence(), std::uint64_t{7});
}

TEST(an_unrecoverable_gap_is_abandoned_and_reported) {
  FeedHandler handler(fast_config());
  PacketBuilder builder;
  Collector collected;

  std::uint64_t lost_from = 0, lost_count = 0;
  handler.set_loss_handler([&](std::uint64_t first, std::uint64_t count, std::uint64_t) {
    lost_from = first;
    lost_count = count;
  });

  const auto packet1 = add_order_packet(builder, 1, 1, 3);
  const auto packet3 = add_order_packet(builder, 7, 7, 3);
  handler.on_datagram(packet1.data(), packet1.size(), 1'000, collected);
  handler.on_datagram(packet3.data(), packet3.size(), 1'000, collected);

  handler.poll(1'000 + 50'000'000, collected);  // past the recovery deadline

  CHECK_EQ(lost_from, std::uint64_t{4});
  CHECK_EQ(lost_count, std::uint64_t{3});
  CHECK_EQ(handler.stats().messages_lost, std::uint64_t{3});
  CHECK(!handler.in_gap());

  // The held packet is released, and the first message after the hole is flagged so
  // the book builder knows its state is not derived from a complete stream.
  CHECK_EQ(collected.order_ids.size(), std::size_t{6});
  CHECK_EQ(collected.order_ids[3], std::uint64_t{7});
  CHECK((collected.flags[3] & cascade::feed::kEventGapBefore) != 0);
  CHECK_EQ(collected.flags[4] & cascade::feed::kEventGapBefore, 0);  // only the first
}

TEST(a_rejected_recovery_abandons_immediately) {
  // If the server says the range has aged out, waiting for the full deadline only adds
  // latency to news the subscriber needs now.
  FeedHandler handler(fast_config());
  PacketBuilder builder;
  Collector collected;
  bool reported = false;
  handler.set_loss_handler([&](std::uint64_t, std::uint64_t, std::uint64_t) {
    reported = true;
  });

  const auto packet1 = add_order_packet(builder, 1, 1, 3);
  const auto packet3 = add_order_packet(builder, 7, 7, 3);
  handler.on_datagram(packet1.data(), packet1.size(), 1'000, collected);
  handler.on_datagram(packet3.data(), packet3.size(), 1'000, collected);

  handler.on_recovery_rejected(2'000, collected);
  CHECK(reported);
  CHECK(!handler.in_gap());
  CHECK_EQ(handler.stats().messages_lost, std::uint64_t{3});
}

// On a quiet instrument a loss at the end of a burst is otherwise invisible until the
// next trade, which could be hours away. Heartbeats are what make it detectable.
TEST(a_heartbeat_reveals_a_gap_on_a_quiet_feed) {
  FeedHandler handler(fast_config());
  RecordingRequester requester;
  handler.set_recovery_requester(&requester);
  PacketBuilder builder;
  Collector collected;

  const auto packet1 = add_order_packet(builder, 1, 1, 3);
  handler.on_datagram(packet1.data(), packet1.size(), 1'000, collected);
  CHECK(!handler.in_gap());

  const auto beat = builder.heartbeat(10);  // venue says it has sent through 9
  handler.on_datagram(beat.data(), beat.size(), 2'000, collected);

  CHECK(handler.in_gap());
  CHECK_EQ(handler.stats().heartbeats, std::uint64_t{1});
  handler.poll(2'000 + 2'000'000, collected);
  CHECK_EQ(requester.asks.size(), std::size_t{1});
  CHECK_EQ(requester.asks[0].first, std::uint64_t{4});
  CHECK_EQ(requester.asks[0].count, std::uint16_t{6});
}

TEST(a_session_change_resets_sequencing) {
  // A venue restart renumbers from scratch; treating the new low sequence as a
  // duplicate would silently discard the entire new session.
  FeedHandler handler(fast_config());
  Collector collected;

  PacketBuilder morning("SESSION-AM");
  const auto first = add_order_packet(morning, 1000, 1, 3);
  handler.on_datagram(first.data(), first.size(), 1'000, collected);
  CHECK_EQ(collected.order_ids.size(), std::size_t{3});

  PacketBuilder afternoon("SESSION-PM");
  const auto second = add_order_packet(afternoon, 1, 100, 3);
  handler.on_datagram(second.data(), second.size(), 2'000, collected);

  CHECK_EQ(collected.order_ids.size(), std::size_t{6});
  CHECK_EQ(collected.order_ids[3], std::uint64_t{100});
  CHECK_EQ(handler.expected_sequence(), std::uint64_t{4});
}

TEST(the_reorder_window_has_a_bounded_cost) {
  // More out-of-order packets than the window can hold must not grow memory without
  // limit; the furthest-ahead packet is the one to drop.
  FeedHandler config_handler(fast_config());
  PacketBuilder builder;
  Collector collected;

  const auto packet1 = add_order_packet(builder, 1, 1, 2);
  config_handler.on_datagram(packet1.data(), packet1.size(), 1'000, collected);

  for (int i = 0; i < 40; ++i) {
    const std::uint64_t sequence = 100 + static_cast<std::uint64_t>(i) * 2;
    const auto packet = add_order_packet(builder, sequence, sequence, 2);
    config_handler.on_datagram(packet.data(), packet.size(), 1'000, collected);
  }

  CHECK_GE(config_handler.stats().packets_dropped_window, std::uint64_t{1});
  CHECK_EQ(collected.order_ids.size(), std::size_t{2});  // still stuck behind the gap
}

TEST(malformed_packets_are_rejected_without_reading_past_the_buffer) {
  // The one place in the system that parses bytes off the network. A length field that
  // cannot be trusted has to be treated as hostile, because a truncated datagram from
  // a flaky NIC is indistinguishable from a crafted one.
  //
  // Each case gets its own handler: a malformed packet still advances the sequence, so
  // sharing one would see later cases correctly rejected as duplicates before they
  // were ever parsed, and the test would prove nothing.
  PacketBuilder builder;
  Collector collected;
  const auto valid = add_order_packet(builder, 1, 1, 2);

  {  // shorter than a packet header
    FeedHandler handler(fast_config());
    const unsigned char runt[8] = {0};
    handler.on_datagram(runt, sizeof(runt), 1'000, collected);
    CHECK_EQ(handler.stats().malformed_packets, std::uint64_t{1});
    CHECK_EQ(handler.stats().messages_delivered, std::uint64_t{0});
  }

  {  // header claims more messages than the payload contains
    FeedHandler handler(fast_config());
    std::vector<unsigned char> forged(valid);
    cascade::proto::PacketHeader header{};
    std::memcpy(&header, forged.data(), sizeof(header));
    header.message_count.set(50);
    std::memcpy(forged.data(), &header, sizeof(header));
    handler.on_datagram(forged.data(), forged.size(), 1'000, collected);
    CHECK_EQ(handler.stats().malformed_packets, std::uint64_t{1});
    // The two well-formed messages that were present are still delivered; parsing
    // stops at the point the packet stops making sense rather than discarding it all.
    CHECK_EQ(handler.stats().messages_delivered, std::uint64_t{2});
  }

  {  // payload cut off mid-message
    FeedHandler handler(fast_config());
    std::vector<unsigned char> chopped(valid.begin(), valid.begin() + 25);
    handler.on_datagram(chopped.data(), chopped.size(), 1'000, collected);
    CHECK_EQ(handler.stats().malformed_packets, std::uint64_t{1});
    CHECK_EQ(handler.stats().messages_delivered, std::uint64_t{0});
  }

  {  // a message whose declared length is zero would otherwise loop forever
    FeedHandler handler(fast_config());
    std::vector<unsigned char> zero_length(sizeof(cascade::proto::PacketHeader) + 2, 0);
    cascade::proto::PacketHeader header{};
    std::memcpy(&header, valid.data(), sizeof(header));
    header.message_count.set(1);
    std::memcpy(zero_length.data(), &header, sizeof(header));
    handler.on_datagram(zero_length.data(), zero_length.size(), 1'000, collected);
    CHECK_EQ(handler.stats().malformed_packets, std::uint64_t{1});
  }
}

TEST(unknown_message_types_are_skipped_without_desyncing) {
  // Length prefixes exist so a venue can add a message type without a flag day.
  FeedHandler handler(fast_config());
  PacketBuilder builder;
  Collector collected;

  builder.begin(1);
  builder.append_with([](unsigned char* out) {
    return cascade::feed::encode::add_order(out, 1'000, 1, cascade::Symbol::from_text("A"),
                                            cascade::Side::kBuy, 10,
                                            cascade::price_from_double(1.0));
  });
  unsigned char future_message[24] = {0};
  future_message[0] = 'Z';  // a type this build has never heard of
  builder.append(future_message, sizeof(future_message));
  builder.append_with([](unsigned char* out) {
    return cascade::feed::encode::add_order(out, 1'001, 2, cascade::Symbol::from_text("A"),
                                            cascade::Side::kBuy, 10,
                                            cascade::price_from_double(1.0));
  });
  const auto& packet = builder.finish();
  handler.on_datagram(packet.data(), packet.size(), 1'000, collected);

  // Both known messages arrive; the unknown one is stepped over cleanly.
  CHECK_EQ(collected.order_ids.size(), std::size_t{2});
  CHECK_EQ(collected.order_ids[0], std::uint64_t{1});
  CHECK_EQ(collected.order_ids[1], std::uint64_t{2});
  CHECK_EQ(handler.expected_sequence(), std::uint64_t{4});
}

// A retransmit request must ask for what is actually missing, not for everything that
// has arrived since. Packets that kept coming during the gap are already held in the
// reorder window; asking for them again floods the venue's recovery server with work
// nobody needs, and is how a recovery server gets buried during the one incident where
// it matters.
TEST(a_retransmit_asks_only_for_the_hole) {
  FeedHandler handler(fast_config());
  RecordingRequester requester;
  handler.set_recovery_requester(&requester);
  PacketBuilder builder;
  Collector collected;
  handler.start_session(builder.session(), 1);

  const auto packet1 = add_order_packet(builder, 1, 1, 3);
  handler.on_datagram(packet1.data(), packet1.size(), 1'000, collected);

  // Sequences 4-6 are lost. Everything from 7 onwards keeps arriving normally.
  for (std::uint64_t sequence = 7; sequence < 7 + 3 * 5; sequence += 3) {
    const auto packet = add_order_packet(builder, sequence, sequence, 3);
    handler.on_datagram(packet.data(), packet.size(), 1'000, collected);
  }

  handler.poll(1'000 + 2'000'000, collected);
  CHECK_EQ(requester.asks.size(), std::size_t{1});
  CHECK_EQ(requester.asks[0].first, std::uint64_t{4});
  // Three missing, not the eighteen that have gone by since.
  CHECK_EQ(requester.asks[0].count, std::uint16_t{3});
}

TEST(abandoning_a_gap_loses_only_the_hole) {
  FeedHandler handler(fast_config());
  PacketBuilder builder;
  Collector collected;
  handler.start_session(builder.session(), 1);

  std::uint64_t lost_count = 0;
  handler.set_loss_handler([&](std::uint64_t, std::uint64_t count, std::uint64_t) {
    lost_count = count;
  });

  const auto packet1 = add_order_packet(builder, 1, 1, 3);
  handler.on_datagram(packet1.data(), packet1.size(), 1'000, collected);
  for (std::uint64_t sequence = 7; sequence < 7 + 3 * 4; sequence += 3) {
    const auto packet = add_order_packet(builder, sequence, sequence, 3);
    handler.on_datagram(packet.data(), packet.size(), 1'000, collected);
  }

  handler.poll(1'000 + 50'000'000, collected);

  // Only 4, 5 and 6 are gone. Everything held behind them is released, not discarded.
  CHECK_EQ(lost_count, std::uint64_t{3});
  CHECK_EQ(handler.stats().messages_lost, std::uint64_t{3});
  CHECK_EQ(collected.order_ids.size(), std::size_t{15});
  CHECK_EQ(collected.order_ids[3], std::uint64_t{7});
  CHECK_EQ(collected.order_ids.back(), std::uint64_t{18});
}

// When the window fills, waiting longer cannot help -- there is nowhere to put what
// arrives next. Abandoning immediately bounds the loss to the original hole; dropping
// held packets to make room would turn one lost datagram into thousands of lost
// messages, each a hole nobody would ever ask to have filled.
TEST(a_full_reorder_window_abandons_rather_than_cascading) {
  FeedHandler::Config config;
  config.reorder_window = 4;
  config.gap_grace_ns = 1'000'000;
  config.recovery_deadline_ns = 60'000'000'000ull;  // never reached in this test
  FeedHandler handler(config);
  PacketBuilder builder;
  Collector collected;
  handler.start_session(builder.session(), 1);

  std::uint64_t lost_count = 0;
  handler.set_loss_handler([&](std::uint64_t, std::uint64_t count, std::uint64_t) {
    lost_count = count;
  });

  const auto packet1 = add_order_packet(builder, 1, 1, 2);
  handler.on_datagram(packet1.data(), packet1.size(), 1'000, collected);

  // Sequence 3-4 lost; six more packets arrive into a four-slot window.
  for (std::uint64_t sequence = 5; sequence < 5 + 2 * 6; sequence += 2) {
    const auto packet = add_order_packet(builder, sequence, sequence, 2);
    handler.on_datagram(packet.data(), packet.size(), 1'000, collected);
  }
  CHECK_GE(handler.stats().packets_dropped_window, std::uint64_t{1});

  handler.poll(1'000 + 2'000'000, collected);

  // Two messages lost -- the actual hole -- rather than everything the window could
  // not hold, and the connection resumes cleanly from what was held.
  CHECK_EQ(lost_count, std::uint64_t{2});
  CHECK(!handler.in_gap());
  CHECK_GE(collected.order_ids.size(), std::size_t{6});
  CHECK_EQ(collected.order_ids[2], std::uint64_t{5});
}
