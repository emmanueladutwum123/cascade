// SPDX-License-Identifier: Apache-2.0
//
// An exchange, as far as the plant is concerned.
//
// Publishes a MoldUDP64-framed, ITCH-shaped order-by-order feed over UDP multicast, and
// serves retransmit requests over TCP. It deliberately drops packets: a gap-recovery
// path that is never exercised is a gap-recovery path that does not work, and a feed
// that is perfectly reliable in testing tells you nothing about the case that matters.

#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <random>
#include <string>
#include <vector>

#include "cascade/core/clock.hpp"
#include "cascade/net/tcp.hpp"
#include "cascade/net/udp.hpp"
#include "cascade/proto/feed.hpp"
#include "cascade/sim/market.hpp"

using namespace cascade;

namespace {

std::atomic<bool> g_running{true};
void handle_signal(int) { g_running.store(false); }

struct Options {
  std::string group{"239.10.20.30"};
  std::uint16_t port{31337};
  std::string interface_address{"127.0.0.1"};
  std::uint16_t recovery_port{31338};
  std::uint32_t instruments{200};
  std::uint64_t rate{200'000};
  double loss_rate{0.0005};       ///< Fraction of datagrams deliberately not sent.
  std::uint32_t max_per_packet{8};
  std::uint64_t duration_seconds{0};  ///< 0 = until interrupted.
  std::uint64_t seed{0xFEED};
  bool loopback{true};
};

/// Retains recently sent messages so a receiver that missed one can ask for it.
///
/// Bounded on purpose. An unbounded buffer would let one badly-behaved consumer pin an
/// arbitrary amount of the exchange's memory, and a receiver far enough behind to have
/// aged out is a receiver that should resynchronise from a snapshot rather than replay
/// hours of history. Asking for a range that has aged out gets an explicit reject, so
/// the consumer learns immediately instead of waiting out its recovery deadline.
class RetransmitBuffer {
 public:
  explicit RetransmitBuffer(std::size_t capacity) : capacity_(capacity) {
    entries_.resize(capacity);
  }

  void append(std::uint64_t sequence, const unsigned char* message, std::size_t bytes) {
    Entry& entry = entries_[sequence % capacity_];
    entry.sequence = sequence;
    entry.length = static_cast<std::uint16_t>(bytes);
    std::memcpy(entry.data, message, bytes);
    newest_ = sequence;
  }

  bool get(std::uint64_t sequence, const unsigned char*& message,
           std::uint16_t& bytes) const {
    const Entry& entry = entries_[sequence % capacity_];
    if (entry.sequence != sequence) return false;  // overwritten, or never held
    message = entry.data;
    bytes = entry.length;
    return true;
  }

  std::uint64_t newest() const noexcept { return newest_; }

 private:
  struct Entry {
    std::uint64_t sequence{UINT64_MAX};
    std::uint16_t length{0};
    unsigned char data[proto::kMaxMessageSize]{};
  };
  std::size_t capacity_;
  std::vector<Entry> entries_;
  std::uint64_t newest_{0};
};

/// Serves retransmit requests. One thread, because recovery is rare and must never
/// compete with publishing for the publisher's core.
class RecoveryServer {
 public:
  RecoveryServer(const RetransmitBuffer& buffer, const char session[10])
      : buffer_(buffer) {
    std::memcpy(session_, session, sizeof(session_));
  }

  void open(std::uint16_t port) { listener_.open("0.0.0.0", port); }

  /// Non-blocking: accept what is pending and answer what has arrived.
  void poll() {
    std::string peer;
    net::Socket accepted = listener_.accept(&peer);
    if (accepted.valid()) {
      std::printf("[feedsim] recovery client connected: %s\n", peer.c_str());
      clients_.push_back(Client{std::move(accepted), {}});
    }

    for (std::size_t i = 0; i < clients_.size();) {
      if (!serve(clients_[i])) {
        clients_[i] = std::move(clients_.back());
        clients_.pop_back();
      } else {
        ++i;
      }
    }
  }

  std::uint64_t served() const noexcept { return served_; }
  std::uint64_t rejected() const noexcept { return rejected_; }

 private:
  struct Client {
    net::Socket socket;
    std::vector<unsigned char> inbox;
  };

  bool serve(Client& client) {
    unsigned char scratch[1024];
    const ssize_t bytes = ::read(client.socket.fd(), scratch, sizeof(scratch));
    if (bytes == 0) return false;
    if (bytes < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) return true;
      return false;
    }
    client.inbox.insert(client.inbox.end(), scratch, scratch + bytes);

    while (client.inbox.size() >= sizeof(proto::RecoveryRequest)) {
      proto::RecoveryRequest request{};
      std::memcpy(&request, client.inbox.data(), sizeof(request));
      client.inbox.erase(client.inbox.begin(),
                         client.inbox.begin() + sizeof(request));
      if (!answer(client, request)) return false;
    }
    return true;
  }

  bool answer(Client& client, const proto::RecoveryRequest& request) {
    const std::uint64_t first = request.first_sequence.value();
    std::uint16_t count = request.count.value();
    if (count > proto::kMaxRecoveryBatch) count = proto::kMaxRecoveryBatch;

    std::vector<unsigned char> payload;
    std::uint16_t served = 0;
    for (std::uint16_t i = 0; i < count; ++i) {
      const unsigned char* message = nullptr;
      std::uint16_t length = 0;
      if (!buffer_.get(first + i, message, length)) break;  // aged out
      proto::MessageLengthPrefix prefix{};
      prefix.length.set(length);
      const auto* prefix_bytes = reinterpret_cast<const unsigned char*>(&prefix);
      payload.insert(payload.end(), prefix_bytes, prefix_bytes + sizeof(prefix));
      payload.insert(payload.end(), message, message + length);
      ++served;
    }

    proto::RecoveryResponseHeader header{};
    std::memcpy(header.session, session_, sizeof(session_));
    header.first_sequence.set(first);
    header.count.set(served);
    header.payload_bytes.set(static_cast<std::uint32_t>(payload.size()));
    if (served == 0) {
      // Say so explicitly. Silence would leave the receiver waiting out its whole
      // recovery deadline before concluding the same thing.
      header.type = static_cast<std::uint8_t>(proto::RecoveryType::kReject);
      ++rejected_;
    } else {
      header.type = static_cast<std::uint8_t>(proto::RecoveryType::kResponse);
      served_ += served;
    }

    std::vector<unsigned char> frame(sizeof(header));
    std::memcpy(frame.data(), &header, sizeof(header));
    frame.insert(frame.end(), payload.begin(), payload.end());

    std::size_t sent = 0;
    while (sent < frame.size()) {
      const ssize_t written =
          ::write(client.socket.fd(), frame.data() + sent, frame.size() - sent);
      if (written > 0) { sent += static_cast<std::size_t>(written); continue; }
      if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
        continue;
      }
      return false;
    }
    return true;
  }

  const RetransmitBuffer& buffer_;
  net::TcpListener listener_;
  std::vector<Client> clients_;
  char session_[10]{};
  std::uint64_t served_{0};
  std::uint64_t rejected_{0};
};

void usage() {
  std::printf(
      "cascade-feedsim -- an ITCH-shaped multicast market data feed\n\n"
      "  --group ADDR        multicast group (default 239.10.20.30)\n"
      "  --port N            multicast port (default 31337)\n"
      "  --interface ADDR    outbound interface (default 127.0.0.1)\n"
      "  --recovery-port N   retransmit server port (default 31338)\n"
      "  --instruments N     instruments to publish (default 200)\n"
      "  --rate N            messages per second (default 200000)\n"
      "  --loss RATE         fraction of datagrams to drop (default 0.0005)\n"
      "  --batch N           max messages per datagram (default 8)\n"
      "  --seconds N         run for N seconds (default: until interrupted)\n"
      "  --seed N            simulator seed\n"
      "  --help\n");
}

bool parse(int argc, char** argv, Options& options) {
  for (int i = 1; i < argc; ++i) {
    const std::string argument = argv[i];
    auto next_string = [&](std::string& out) {
      if (i + 1 >= argc) return false;
      out = argv[++i];
      return true;
    };
    auto next_number = [&](std::uint64_t& out) {
      if (i + 1 >= argc) return false;
      out = std::strtoull(argv[++i], nullptr, 10);
      return true;
    };
    std::uint64_t value = 0;
    std::string text;
    if (argument == "--help") { usage(); return false; }
    else if (argument == "--group" && next_string(text)) options.group = text;
    else if (argument == "--interface" && next_string(text)) options.interface_address = text;
    else if (argument == "--port" && next_number(value)) options.port = static_cast<std::uint16_t>(value);
    else if (argument == "--recovery-port" && next_number(value)) options.recovery_port = static_cast<std::uint16_t>(value);
    else if (argument == "--instruments" && next_number(value)) options.instruments = static_cast<std::uint32_t>(value);
    else if (argument == "--rate" && next_number(value)) options.rate = value ? value : 1;
    else if (argument == "--batch" && next_number(value)) options.max_per_packet = static_cast<std::uint32_t>(value ? value : 1);
    else if (argument == "--seconds" && next_number(value)) options.duration_seconds = value;
    else if (argument == "--seed" && next_number(value)) options.seed = value;
    else if (argument == "--loss" && next_string(text)) options.loss_rate = std::strtod(text.c_str(), nullptr);
    else { std::printf("unknown argument: %s\n\n", argument.c_str()); usage(); return false; }
  }
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  Options options;
  if (!parse(argc, argv, options)) return 0;

  std::signal(SIGINT, handle_signal);
  std::signal(SIGTERM, handle_signal);
  std::signal(SIGPIPE, SIG_IGN);  // a departed recovery client must not kill us

  char session[10];
  std::memcpy(session, "CASCADE001", sizeof(session));

  sim::MarketSimulator::Config sim_config;
  sim_config.instrument_count = options.instruments;
  sim_config.seed = options.seed;
  sim::MarketSimulator simulator(sim_config);

  net::UdpSender sender;
  RetransmitBuffer retransmit(1u << 18);
  RecoveryServer recovery(retransmit, session);

  try {
    sender.open(net::Endpoint{options.group, options.port}, options.interface_address,
                /*ttl=*/1, options.loopback);
    recovery.open(options.recovery_port);
  } catch (const net::SocketError& error) {
    std::fprintf(stderr, "[feedsim] startup failed: %s\n", error.what());
    return 1;
  }

  std::printf("[feedsim] publishing %u instruments to %s:%u at %llu msg/s\n",
              options.instruments, options.group.c_str(), options.port,
              static_cast<unsigned long long>(options.rate));
  std::printf("[feedsim] retransmit server on port %u, injecting %.4f%% packet loss\n",
              options.recovery_port, options.loss_rate * 100.0);

  std::mt19937_64 rng(options.seed ^ 0x9E3779B9);
  std::uniform_real_distribution<double> uniform(0.0, 1.0);

  std::vector<unsigned char> packet;
  packet.reserve(proto::kMaxDatagramSize);
  unsigned char scratch[proto::kMaxMessageSize];

  std::uint64_t sequence = 1;
  std::uint64_t packets_sent = 0, packets_dropped = 0, messages_sent = 0;
  const double interval_ns =
      1e9 * static_cast<double>(options.max_per_packet) / static_cast<double>(options.rate);

  const std::uint64_t start = hires_nanos();
  const std::uint64_t wall_start = mono_nanos();
  std::uint64_t last_report = wall_start;
  std::uint64_t last_heartbeat = wall_start;

  while (g_running.load(std::memory_order_relaxed)) {
    if (options.duration_seconds &&
        mono_nanos() - wall_start >= options.duration_seconds * 1'000'000'000ull) {
      break;
    }

    const std::uint64_t due =
        start + static_cast<std::uint64_t>(static_cast<double>(packets_sent) * interval_ns);
    while (hires_nanos() < due) {
      recovery.poll();  // recovery work fills the gaps between publishes
      if (!g_running.load(std::memory_order_relaxed)) break;
    }

    // Build one datagram: a header plus up to `max_per_packet` length-prefixed messages.
    // Batching amortises the syscall; a datagram per message would make the publisher
    // syscall-bound long before the network noticed.
    packet.assign(sizeof(proto::PacketHeader), 0);
    std::uint16_t in_packet = 0;
    for (std::uint32_t i = 0; i < options.max_per_packet; ++i) {
      const std::size_t bytes = simulator.next_message(scratch, wall_nanos());
      if (packet.size() + sizeof(proto::MessageLengthPrefix) + bytes >
          proto::kMaxDatagramSize) {
        break;
      }
      proto::MessageLengthPrefix prefix{};
      prefix.length.set(static_cast<std::uint16_t>(bytes));
      const auto* prefix_bytes = reinterpret_cast<const unsigned char*>(&prefix);
      packet.insert(packet.end(), prefix_bytes, prefix_bytes + sizeof(prefix));
      packet.insert(packet.end(), scratch, scratch + bytes);
      // Held for retransmit whether or not the datagram survives: the whole point is
      // that the receiver can ask for what it did not get.
      retransmit.append(sequence + i, scratch, bytes);
      ++in_packet;
    }
    if (in_packet == 0) continue;

    proto::PacketHeader header{};
    std::memcpy(header.session, session, sizeof(session));
    header.sequence.set(sequence);
    header.message_count.set(in_packet);
    std::memcpy(packet.data(), &header, sizeof(header));

    // Drop on purpose, *after* sequencing. The sequence number still advances, which is
    // exactly what a real loss looks like to a receiver and what makes the hole
    // detectable at all.
    if (uniform(rng) < options.loss_rate) {
      ++packets_dropped;
    } else if (sender.send(packet.data(), packet.size()) < 0) {
      std::fprintf(stderr, "[feedsim] send failed: %s\n", std::strerror(errno));
    }

    sequence += in_packet;
    messages_sent += in_packet;
    ++packets_sent;

    const std::uint64_t now = mono_nanos();
    // A heartbeat on a quiet feed is the only way a receiver can notice that the last
    // packet of a burst went missing.
    if (now - last_heartbeat > 250'000'000ull) {
      proto::PacketHeader beat{};
      std::memcpy(beat.session, session, sizeof(session));
      beat.sequence.set(sequence);
      beat.message_count.set(proto::kHeartbeatMessageCount);
      sender.send(&beat, sizeof(beat));
      last_heartbeat = now;
    }

    if (now - last_report > 2'000'000'000ull) {
      const double seconds = static_cast<double>(now - wall_start) / 1e9;
      std::printf("[feedsim] %.1fs  %llu msgs (%.0fk/s)  %llu packets  %llu dropped  "
                  "retransmits served=%llu rejected=%llu\n",
                  seconds, static_cast<unsigned long long>(messages_sent),
                  static_cast<double>(messages_sent) / seconds / 1e3,
                  static_cast<unsigned long long>(packets_sent),
                  static_cast<unsigned long long>(packets_dropped),
                  static_cast<unsigned long long>(recovery.served()),
                  static_cast<unsigned long long>(recovery.rejected()));
      std::fflush(stdout);
      last_report = now;
    }
    recovery.poll();
  }

  // Tell receivers the session is over rather than just going silent, so they do not
  // spend their recovery deadline chasing a gap that will never be filled.
  proto::PacketHeader closing{};
  std::memcpy(closing.session, session, sizeof(session));
  closing.sequence.set(sequence);
  closing.message_count.set(proto::kEndOfSessionMessageCount);
  sender.send(&closing, sizeof(closing));

  const double seconds = static_cast<double>(mono_nanos() - wall_start) / 1e9;
  std::printf("\n[feedsim] stopped after %.1fs\n", seconds);
  std::printf("[feedsim]   messages sent     : %llu (%.0fk/s)\n",
              static_cast<unsigned long long>(messages_sent),
              static_cast<double>(messages_sent) / seconds / 1e3);
  std::printf("[feedsim]   datagrams sent    : %llu\n",
              static_cast<unsigned long long>(packets_sent - packets_dropped));
  std::printf("[feedsim]   datagrams dropped : %llu (%.4f%%)\n",
              static_cast<unsigned long long>(packets_dropped),
              packets_sent ? 100.0 * static_cast<double>(packets_dropped) /
                                 static_cast<double>(packets_sent)
                           : 0.0);
  std::printf("[feedsim]   retransmits served: %llu (rejected %llu)\n",
              static_cast<unsigned long long>(recovery.served()),
              static_cast<unsigned long long>(recovery.rejected()));
  return 0;
}
