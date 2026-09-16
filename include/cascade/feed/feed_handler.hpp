// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <cstring>
#include <functional>
#include <utility>
#include <vector>

#include "cascade/core/clock.hpp"
#include "cascade/core/platform.hpp"
#include "cascade/feed/decoder.hpp"
#include "cascade/feed/event.hpp"
#include "cascade/proto/feed.hpp"

namespace cascade::feed {

/// Where a retransmit request goes. Kept as an interface because recovery is rare —
/// a virtual call here costs nothing measurable — and because it lets the gap state
/// machine be tested exhaustively without a socket anywhere near it.
class RecoveryRequester {
 public:
  virtual ~RecoveryRequester() = default;
  /// Ask for `[first_sequence, first_sequence + count)`.
  virtual void request_retransmit(std::uint64_t first_sequence, std::uint16_t count) = 0;
};

/// Consumes a sequenced, unreliable multicast feed and emits an in-order event stream.
///
/// This is where the fundamental bargain of multicast market data gets paid for. The
/// venue gets to send one copy for the whole market, and in return every receiver must
/// independently solve: packets arrive out of order, packets do not arrive at all,
/// packets arrive twice, and the consumer cannot ask the sender to slow down.
///
/// The handler distinguishes three situations that look identical at first glance and
/// demand completely different responses:
///
///   * **Reordering.** The network delivered 5 before 4. This is normal on any path
///     with equal-cost multipath, and it resolves itself in microseconds. Treating it
///     as loss would fire a retransmit request for a packet already in flight — and
///     under load, every receiver doing that at once is how a recovery server gets
///     buried. The answer is a small reorder window: hold the future packet, wait.
///
///   * **Recoverable loss.** 4 really is gone. After a short grace period the handler
///     asks the retransmit server for it over TCP. The grace period matters: request
///     too eagerly and you generate load for packets that were merely late.
///
///   * **Unrecoverable loss.** The retransmit never came, or came back rejected
///     because the range aged out. There is no way to build a correct book across a
///     hole in an order-by-order stream, so the only honest move is to declare the
///     affected books stale, skip forward, and tell subscribers — immediately. A plant
///     that silently continues publishing a book it knows is wrong is worse than one
///     that goes dark, because downstream systems will trade on it.
///
/// The handler is single-threaded and owns its buffers; `poll()` drives its timers.
class FeedHandler {
 public:
  struct Config {
    /// Packets held while a gap is outstanding.
    ///
    /// Sized by *recovery latency*, not by network reordering. Genuine reordering
    /// resolves within a handful of packets, so a small window looks sufficient -- and
    /// is catastrophically wrong. While a gap is open every subsequent packet is ahead
    /// of us and must be held; at 10,000 packets/second a 250ms recovery deadline means
    /// 2,500 packets arrive before we know the outcome. A 64-packet window overflows
    /// 6ms in, and every packet dropped from it becomes a second hole that recovery was
    /// never asked to fill -- turning one lost datagram into tens of thousands of lost
    /// messages. That cascade is exactly what this default exists to prevent.
    std::size_t reorder_window{4096};
    /// How long a gap must persist before we ask for a retransmit, rather than
    /// assuming the packet is merely late.
    std::uint64_t gap_grace_ns{2'000'000};          // 2ms
    /// How long we keep waiting before declaring the range unrecoverable.
    std::uint64_t recovery_deadline_ns{250'000'000};  // 250ms
    /// Cap on a single retransmit ask, so one receiver cannot monopolise the server.
    std::uint16_t max_retransmit_batch{proto::kMaxRecoveryBatch};
  };

  struct Stats {
    std::uint64_t packets_received{0};
    std::uint64_t packets_duplicate{0};
    std::uint64_t packets_reordered{0};   ///< Arrived early, held, delivered in order.
    std::uint64_t packets_dropped_window{0};  ///< Evicted from a full reorder window.
    std::uint64_t messages_delivered{0};
    std::uint64_t messages_recovered{0};  ///< Arrived by retransmit.
    std::uint64_t messages_lost{0};       ///< Given up on; books declared stale.
    std::uint64_t gaps_detected{0};
    std::uint64_t gaps_recovered{0};
    std::uint64_t retransmits_requested{0};
    std::uint64_t heartbeats{0};
    std::uint64_t decode_errors{0};
    std::uint64_t malformed_packets{0};
  };

  /// The default-argument form (`FeedHandler(Config config = Config{})`) is ill-formed
  /// here: a nested class's default member initialisers are not yet usable in a
  /// default argument of the enclosing class. A delegating default constructor gets
  /// the same ergonomics without the language corner.
  FeedHandler() : FeedHandler(Config{}) {}

  explicit FeedHandler(Config config) : config_(config) {
    slots_.resize(config_.reorder_window ? config_.reorder_window : 1);
  }

  void set_recovery_requester(RecoveryRequester* requester) noexcept {
    requester_ = requester;
  }

  const Stats& stats() const noexcept { return stats_; }
  std::uint64_t expected_sequence() const noexcept { return expected_sequence_; }
  bool in_gap() const noexcept { return gap_open_; }

  /// Begin (or restart) a session at a known sequence. A session change means the
  /// venue restarted and its numbering began again, so every book built from the old
  /// session is meaningless and the caller resets them.
  void start_session(const char session[10], std::uint64_t first_sequence) noexcept {
    std::memcpy(session_, session, sizeof(session_));
    expected_sequence_ = first_sequence;
    session_known_ = true;
    close_gap();
    for (auto& slot : slots_) slot.occupied = false;
  }

  /// Feed one received datagram. `sink(const FeedEvent&)` is invoked for every message
  /// that can be delivered in order as a result — which may be none (the packet is
  /// buffered past a gap) or many (the packet filled a gap and released everything
  /// behind it).
  template <typename Sink>
  void on_datagram(const unsigned char* data, std::size_t bytes, std::uint64_t ingest_ns,
                   Sink&& sink) {
    ++stats_.packets_received;
    if (bytes < sizeof(proto::PacketHeader)) {
      ++stats_.malformed_packets;
      return;
    }

    proto::PacketHeader header;
    std::memcpy(&header, data, sizeof(header));
    const std::uint64_t sequence = header.sequence.value();
    const std::uint16_t count = header.message_count.value();

    if (!session_known_) {
      // Joining a live feed mid-session: adopt whatever the first packet says rather
      // than declaring an enormous gap back to sequence 1. The books we build from
      // here are incomplete until the instruments trade, which is exactly why every
      // book starts life flagged stale.
      std::memcpy(session_, header.session, sizeof(session_));
      expected_sequence_ = sequence;
      session_known_ = true;
    } else if (std::memcmp(session_, header.session, sizeof(session_)) != 0) {
      // A different session id on the same group means the venue restarted. Old
      // sequence numbers no longer relate to new ones.
      std::memcpy(session_, header.session, sizeof(session_));
      expected_sequence_ = sequence;
      close_gap();
      for (auto& slot : slots_) slot.occupied = false;
    }

    if (count == proto::kHeartbeatMessageCount) {
      ++stats_.heartbeats;
      // A heartbeat carries the next sequence the venue will send. On a quiet
      // instrument that is the *only* way to learn a packet went missing — without it
      // a loss at the end of a burst is invisible until the next trade, possibly
      // hours later.
      if (sequence > expected_sequence_) note_gap(sequence, ingest_ns);
      return;
    }
    if (count == proto::kEndOfSessionMessageCount) {
      session_known_ = false;
      return;
    }

    const std::size_t payload_bytes = bytes - sizeof(proto::PacketHeader);
    const unsigned char* payload = data + sizeof(proto::PacketHeader);

    if (sequence == expected_sequence_) {
      deliver(payload, payload_bytes, sequence, count, ingest_ns, /*recovered=*/false,
              sink);
      expected_sequence_ = sequence + count;
      drain_reorder_window(ingest_ns, sink);
      if (gap_open_ && expected_sequence_ >= gap_end_) close_gap(/*recovered=*/true);
      return;
    }

    if (sequence < expected_sequence_) {
      // Already have it, or part of it. A retransmit racing the original delivery is
      // the normal cause, and delivering the overlap twice would double-apply order
      // events and corrupt the book.
      const std::uint64_t packet_end = sequence + count;
      if (packet_end <= expected_sequence_) {
        ++stats_.packets_duplicate;
        return;
      }
      const std::uint64_t skip = expected_sequence_ - sequence;
      deliver_from(payload, payload_bytes, sequence, count, skip, ingest_ns,
                   /*recovered=*/true, sink);
      expected_sequence_ = packet_end;
      drain_reorder_window(ingest_ns, sink);
      if (gap_open_ && expected_sequence_ >= gap_end_) close_gap(/*recovered=*/true);
      return;
    }

    // Ahead of us: either reordering or a real hole. Hold it and find out which.
    note_gap(sequence, ingest_ns);
    buffer_packet(payload, payload_bytes, sequence, count, ingest_ns);
  }

  /// Drive the gap timers. Call regularly (the feed handler's own loop does so between
  /// datagrams); recovery decisions are time-based and nothing else advances them.
  template <typename Sink>
  void poll(std::uint64_t now_ns, Sink&& sink) {
    if (!gap_open_) return;

    if (!retransmit_sent_ && now_ns - gap_opened_ns_ >= config_.gap_grace_ns) {
      request_retransmit();
      retransmit_sent_ = true;
    }

    if (window_overflowed_ || now_ns - gap_opened_ns_ >= config_.recovery_deadline_ns) {
      abandon_gap(now_ns, sink);
    }
  }

  /// Feed a retransmitted run of messages, as returned by the recovery server. Shares
  /// the ordinary path so recovered and live messages cannot diverge in handling.
  template <typename Sink>
  void on_recovered(const unsigned char* payload, std::size_t bytes,
                    std::uint64_t first_sequence, std::uint16_t count,
                    std::uint64_t ingest_ns, Sink&& sink) {
    if (first_sequence > expected_sequence_) {
      buffer_packet(payload, bytes, first_sequence, count, ingest_ns);
      return;
    }
    const std::uint64_t packet_end = first_sequence + count;
    if (packet_end <= expected_sequence_) {
      ++stats_.packets_duplicate;
      return;
    }
    const std::uint64_t skip = expected_sequence_ - first_sequence;
    deliver_from(payload, bytes, first_sequence, count, skip, ingest_ns,
                 /*recovered=*/true, sink);
    expected_sequence_ = packet_end;
    drain_reorder_window(ingest_ns, sink);
    if (gap_open_ && expected_sequence_ >= gap_end_) close_gap(/*recovered=*/true);
  }

  /// The recovery server cannot serve the range (it has aged out of its buffer).
  /// There is nothing left to wait for, so stop waiting.
  template <typename Sink>
  void on_recovery_rejected(std::uint64_t now_ns, Sink&& sink) {
    if (gap_open_) abandon_gap(now_ns, sink);
  }

 private:
  struct Slot {
    std::uint64_t sequence{0};
    std::uint16_t count{0};
    std::uint16_t bytes{0};
    bool occupied{false};
    unsigned char data[proto::kMaxDatagramSize]{};
  };

  template <typename Sink>
  void deliver(const unsigned char* payload, std::size_t bytes, std::uint64_t sequence,
               std::uint16_t count, std::uint64_t ingest_ns, bool recovered, Sink& sink) {
    deliver_from(payload, bytes, sequence, count, 0, ingest_ns, recovered, sink);
  }

  /// Walk the length-prefixed messages in a packet, skipping the first `skip` of them.
  ///
  /// `skip` exists because a retransmit can legitimately overlap what we already have:
  /// asking for [100,110) may return a packet starting at 98. The overlap must be
  /// dropped message by message, not packet by packet, or we either replay events
  /// (corrupting the book) or discard the whole packet (re-opening the gap).
  template <typename Sink>
  void deliver_from(const unsigned char* payload, std::size_t bytes,
                    std::uint64_t base_sequence, std::uint16_t count, std::uint64_t skip,
                    std::uint64_t ingest_ns, bool recovered, Sink& sink) {
    std::size_t offset = 0;
    FeedEvent event;
    for (std::uint16_t i = 0; i < count; ++i) {
      if (offset + sizeof(proto::MessageLengthPrefix) > bytes) {
        ++stats_.malformed_packets;
        return;
      }
      proto::MessageLengthPrefix prefix;
      std::memcpy(&prefix, payload + offset, sizeof(prefix));
      const std::size_t length = prefix.length.value();
      offset += sizeof(prefix);
      if (length == 0 || offset + length > bytes) {
        ++stats_.malformed_packets;
        return;
      }

      if (i >= skip) {
        const std::uint64_t sequence = base_sequence + i;
        const DecodeResult result =
            decode_message(payload + offset, length, sequence, ingest_ns, event);
        if (result == DecodeResult::kOk) {
          if (recovered) {
            event.flags |= kEventRecovered;
            ++stats_.messages_recovered;
          }
          if (gap_abandoned_) {
            // First messages after an unrecoverable hole. Marking them lets the book
            // builder know its state is not derived from a complete stream.
            event.flags |= kEventGapBefore;
            gap_abandoned_ = false;
          }
          sink(event);
          ++stats_.messages_delivered;
        } else if (result == DecodeResult::kTruncated) {
          ++stats_.decode_errors;
        }
        // kUnknownType is skipped deliberately; the length prefix keeps us in sync.
      }
      offset += length;
    }
  }

  void buffer_packet(const unsigned char* payload, std::size_t bytes,
                     std::uint64_t sequence, std::uint16_t count,
                     std::uint64_t ingest_ns) {
    (void)ingest_ns;
    if (bytes > proto::kMaxDatagramSize) {
      ++stats_.malformed_packets;
      return;
    }
    Slot* target = nullptr;
    for (Slot& slot : slots_) {
      if (slot.occupied && slot.sequence == sequence) return;  // already held
      if (!slot.occupied && !target) target = &slot;
    }
    if (!target) {
      // The window is full: we are holding `reorder_window` packets behind a hole that
      // recovery has not filled. Dropping held packets to make room is the tempting
      // move and the wrong one -- each one discarded becomes a fresh hole nobody will
      // ever ask for, so one lost datagram silently multiplies into thousands of lost
      // messages. Waiting longer cannot help either, since there is nowhere to put what
      // arrives. Signal that the gap is over so the caller abandons it now, losing only
      // what was actually missing.
      ++stats_.packets_dropped_window;
      window_overflowed_ = true;
      return;
    }
    target->sequence = sequence;
    target->count = count;
    target->bytes = static_cast<std::uint16_t>(bytes);
    target->occupied = true;
    std::memcpy(target->data, payload, bytes);
  }

  /// Release every buffered packet that has become contiguous with our position.
  /// Filling one hole often releases a long run behind it, so this loops.
  template <typename Sink>
  void drain_reorder_window(std::uint64_t ingest_ns, Sink& sink) {
    bool progressed = true;
    while (progressed) {
      progressed = false;
      for (Slot& slot : slots_) {
        if (!slot.occupied) continue;
        if (slot.sequence + slot.count <= expected_sequence_) {
          slot.occupied = false;  // wholly superseded
          ++stats_.packets_duplicate;
          continue;
        }
        if (slot.sequence <= expected_sequence_) {
          const std::uint64_t skip = expected_sequence_ - slot.sequence;
          deliver_from(slot.data, slot.bytes, slot.sequence, slot.count, skip, ingest_ns,
                       /*recovered=*/false, sink);
          expected_sequence_ = slot.sequence + slot.count;
          slot.occupied = false;
          ++stats_.packets_reordered;
          progressed = true;
        }
      }
    }
  }

  /// Record that `sequence` arrived while we were still expecting something earlier.
  ///
  /// `gap_end_` tracks the *earliest* sequence we hold beyond the hole, not the latest
  /// we have seen. The distinction is the difference between a working recovery path
  /// and a broken one. Packets keep arriving while a gap is open, and taking the
  /// newest would grow the hole to span all of them -- so the retransmit request would
  /// ask for hundreds of messages that are sitting in the reorder window already, and
  /// abandoning the gap would discard all of them. The hole is only what is missing:
  /// [expected, first thing we actually have).
  void note_gap(std::uint64_t sequence, std::uint64_t now_ns) noexcept {
    if (!gap_open_) {
      gap_open_ = true;
      gap_start_ = expected_sequence_;
      gap_end_ = sequence;
      gap_opened_ns_ = now_ns;
      retransmit_sent_ = false;
      ++stats_.gaps_detected;
    } else if (sequence < gap_end_ && sequence > expected_sequence_) {
      gap_end_ = sequence;  // something closer to the hole turned up
    }
  }

  void close_gap(bool recovered = false) noexcept {
    if (gap_open_ && recovered) ++stats_.gaps_recovered;
    gap_open_ = false;
    retransmit_sent_ = false;
    window_overflowed_ = false;
  }

  void request_retransmit() {
    if (!requester_) return;
    const std::uint64_t missing = gap_end_ - expected_sequence_;
    if (missing == 0) return;
    const std::uint16_t batch =
        missing > config_.max_retransmit_batch
            ? config_.max_retransmit_batch
            : static_cast<std::uint16_t>(missing);
    requester_->request_retransmit(expected_sequence_, batch);
    ++stats_.retransmits_requested;
  }

  /// Give up on a gap. Skip to the first sequence we actually hold and tell the caller
  /// how much was lost, so it can invalidate the books that depended on it.
  template <typename Sink>
  void abandon_gap(std::uint64_t now_ns, Sink& sink) {
    const std::uint64_t lost = gap_end_ - expected_sequence_;
    stats_.messages_lost += lost;
    expected_sequence_ = gap_end_;
    gap_abandoned_ = true;
    close_gap();
    if (on_loss_) on_loss_(gap_start_, lost, now_ns);
    drain_reorder_window(now_ns, sink);
  }

 public:
  /// Invoked when a gap is abandoned: (first_lost_sequence, message_count, now_ns).
  /// The plant wires this to "mark every book on this channel stale", which is the
  /// only correct response to an unknowable number of missed order events.
  ///
  /// A `std::function` rather than a raw pointer: this fires once per unrecoverable
  /// gap, so the indirection is free, and the handler needs to capture the shard set
  /// it has to invalidate.
  using LossHandler = std::function<void(std::uint64_t, std::uint64_t, std::uint64_t)>;
  void set_loss_handler(LossHandler handler) { on_loss_ = std::move(handler); }

 private:
  Config config_;
  Stats stats_;
  RecoveryRequester* requester_{nullptr};
  LossHandler on_loss_{nullptr};

  char session_[10]{};
  bool session_known_{false};
  std::uint64_t expected_sequence_{0};

  bool gap_open_{false};
  bool gap_abandoned_{false};
  bool window_overflowed_{false};
  bool retransmit_sent_{false};
  std::uint64_t gap_start_{0};
  std::uint64_t gap_end_{0};
  std::uint64_t gap_opened_ns_{0};

  std::vector<Slot> slots_;
};

}  // namespace cascade::feed
