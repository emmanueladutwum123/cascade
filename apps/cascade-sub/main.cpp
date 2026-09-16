// SPDX-License-Identifier: Apache-2.0
//
// cascade-sub -- a subscriber.
//
// Connects to the plant, logs in, subscribes, and either prints the book or reports
// what it received. It also doubles as the plant's slow-consumer test: `--stall N`
// makes it stop reading its socket for N seconds, which is the only honest way to
// verify that conflation and eviction do what they claim.

#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "cascade/core/clock.hpp"
#include "cascade/core/histogram.hpp"
#include "cascade/net/tcp.hpp"
#include "cascade/proto/client.hpp"
#include "cascade/sim/market.hpp"

using namespace cascade;

namespace {

std::atomic<bool> g_running{true};
void handle_signal(int) { g_running.store(false, std::memory_order_release); }

struct Options {
  std::string host{"127.0.0.1"};
  std::uint16_t port{31400};
  std::string token{"cascade-demo-token"};
  std::string client_id{"cascade-sub"};
  std::vector<std::string> symbols;
  std::uint32_t first_n{10};       ///< Subscribe to the first N instruments by default.
  bool with_trades{true};
  bool print_book{false};
  std::uint64_t seconds{10};
  std::uint64_t stall_seconds{0};  ///< Stop reading for this long, to force conflation.
  std::uint64_t stall_after{3};
};

const char* symbol_state_name(std::uint8_t state) {
  switch (static_cast<proto::SymbolState>(state)) {
    case proto::SymbolState::kOk: return "ok";
    case proto::SymbolState::kStale: return "STALE";
    case proto::SymbolState::kRecovered: return "recovered";
    case proto::SymbolState::kHalted: return "halted";
    case proto::SymbolState::kClosed: return "closed";
  }
  return "?";
}

const char* evict_reason_name(std::uint8_t reason) {
  switch (static_cast<proto::EvictReason>(reason)) {
    case proto::EvictReason::kSlowConsumer: return "slow consumer";
    case proto::EvictReason::kTradeQueueOverrun: return "trade queue overrun";
    case proto::EvictReason::kHeartbeatTimeout: return "heartbeat timeout";
    case proto::EvictReason::kProtocolViolation: return "protocol violation";
    case proto::EvictReason::kServerShutdown: return "server shutdown";
    case proto::EvictReason::kEntitlementRevoked: return "entitlement revoked";
  }
  return "unknown";
}

const char* subscribe_status_name(std::uint8_t status) {
  switch (static_cast<proto::SubscribeStatus>(status)) {
    case proto::SubscribeStatus::kOk: return "ok";
    case proto::SubscribeStatus::kNotEntitled: return "NOT ENTITLED";
    case proto::SubscribeStatus::kUnknownSymbol: return "unknown symbol";
    case proto::SubscribeStatus::kLimitExceeded: return "limit exceeded";
    case proto::SubscribeStatus::kAlreadySubscribed: return "already subscribed";
  }
  return "?";
}

bool write_all(int fd, const void* data, std::size_t bytes) {
  const auto* cursor = static_cast<const unsigned char*>(data);
  std::size_t sent = 0;
  while (sent < bytes) {
    const ssize_t written = ::write(fd, cursor + sent, bytes - sent);
    if (written > 0) { sent += static_cast<std::size_t>(written); continue; }
    if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) continue;
    return false;
  }
  return true;
}

void usage() {
  std::printf(
      "cascade-sub -- subscribe to a cascade plant\n\n"
      "  --host ADDR       plant address (default 127.0.0.1)\n"
      "  --port N          plant port (default 31400)\n"
      "  --token T         entitlement token (default cascade-demo-token;\n"
      "                    try cascade-unentitled to see permissioning refuse)\n"
      "  --symbol S        subscribe to S (repeatable)\n"
      "  --first N         subscribe to the first N instruments (default 10)\n"
      "  --no-trades       quotes only\n"
      "  --print           print each book update\n"
      "  --seconds N       run for N seconds (default 10)\n"
      "  --stall N         stop reading the socket for N seconds, to force the\n"
      "                    plant to conflate and then evict (default 0)\n"
      "  --stall-after N   start stalling after N seconds (default 3)\n"
      "  --help\n");
}

bool parse(int argc, char** argv, Options& options) {
  for (int i = 1; i < argc; ++i) {
    const std::string argument = argv[i];
    auto next = [&](std::string& out) {
      if (i + 1 >= argc) return false;
      out = argv[++i];
      return true;
    };
    std::string text;
    if (argument == "--help") { usage(); return false; }
    else if (argument == "--host" && next(text)) options.host = text;
    else if (argument == "--port" && next(text)) options.port = static_cast<std::uint16_t>(std::strtoul(text.c_str(), nullptr, 10));
    else if (argument == "--token" && next(text)) options.token = text;
    else if (argument == "--client" && next(text)) options.client_id = text;
    else if (argument == "--symbol" && next(text)) options.symbols.push_back(text);
    else if (argument == "--first" && next(text)) options.first_n = static_cast<std::uint32_t>(std::strtoul(text.c_str(), nullptr, 10));
    else if (argument == "--seconds" && next(text)) options.seconds = std::strtoull(text.c_str(), nullptr, 10);
    else if (argument == "--stall" && next(text)) options.stall_seconds = std::strtoull(text.c_str(), nullptr, 10);
    else if (argument == "--stall-after" && next(text)) options.stall_after = std::strtoull(text.c_str(), nullptr, 10);
    else if (argument == "--no-trades") options.with_trades = false;
    else if (argument == "--print") options.print_book = true;
    else { std::printf("unknown argument: %s\n\n", argument.c_str()); usage(); return false; }
  }
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  Options options;
  if (!parse(argc, argv, options)) return 0;

  std::signal(SIGINT, handle_signal);
  std::signal(SIGPIPE, SIG_IGN);

  if (options.symbols.empty()) {
    // Default to the first N instruments, named the way the simulator names them.
    sim::MarketSimulator::Config config;
    config.instrument_count = options.first_n;
    sim::MarketSimulator naming(config);
    for (Symbol symbol : naming.symbols()) options.symbols.push_back(symbol.text());
  }

  net::Socket socket;
  try {
    socket = net::tcp_connect(options.host, options.port);
  } catch (const net::SocketError& error) {
    std::fprintf(stderr, "[sub] connect failed: %s\n", error.what());
    return 1;
  }
  std::printf("[sub] connected to %s:%u\n", options.host.c_str(), options.port);

  // --- login ---------------------------------------------------------------
  {
    proto::LoginMsg body{};
    body.protocol_version = proto::kClientProtocolVersion;
    std::strncpy(body.client_id, options.client_id.c_str(), sizeof(body.client_id) - 1);
    std::strncpy(body.token, options.token.c_str(), sizeof(body.token) - 1);
    proto::FrameHeader header{};
    header.payload_bytes = sizeof(body);
    header.type = static_cast<std::uint8_t>(proto::ClientMsgType::kLogin);

    std::vector<unsigned char> frame(sizeof(header) + sizeof(body));
    std::memcpy(frame.data(), &header, sizeof(header));
    std::memcpy(frame.data() + sizeof(header), &body, sizeof(body));
    if (!write_all(socket.fd(), frame.data(), frame.size())) {
      std::fprintf(stderr, "[sub] login write failed\n");
      return 1;
    }
  }

  // --- subscribe -----------------------------------------------------------
  {
    proto::SubscribeMsg body{};
    body.flags = proto::kFlagConflated |
                 (options.with_trades ? proto::kFlagWithTrades : 0);
    body.symbol_count = static_cast<std::uint16_t>(options.symbols.size());

    std::vector<unsigned char> payload(sizeof(body) + options.symbols.size() * 8);
    std::memcpy(payload.data(), &body, sizeof(body));
    for (std::size_t i = 0; i < options.symbols.size(); ++i) {
      Symbol::from_text(options.symbols[i]).to_wire(payload.data() + sizeof(body) + i * 8);
    }

    proto::FrameHeader header{};
    header.payload_bytes = static_cast<std::uint16_t>(payload.size());
    header.type = static_cast<std::uint8_t>(proto::ClientMsgType::kSubscribe);

    std::vector<unsigned char> frame(sizeof(header) + payload.size());
    std::memcpy(frame.data(), &header, sizeof(header));
    std::memcpy(frame.data() + sizeof(header), payload.data(), payload.size());
    if (!write_all(socket.fd(), frame.data(), frame.size())) {
      std::fprintf(stderr, "[sub] subscribe write failed\n");
      return 1;
    }
    std::printf("[sub] subscribed to %zu instrument(s)%s\n", options.symbols.size(),
                options.with_trades ? " with trades" : "");
  }

  net::set_nonblocking(socket.fd(), true);
  net::FrameReader reader(1u << 20);

  // The plant stamps every update with the ingest time of the message that caused it,
  // so a subscriber can measure the plant's latency without the plant's cooperation.
  Histogram staleness(10'000'000'000ull, 3);

  std::uint64_t book_updates = 0, trades = 0, conflated_total = 0, max_conflated = 0;
  std::uint64_t stale_notices = 0, acks_ok = 0, acks_refused = 0;
  bool evicted = false;

  const std::uint64_t start = mono_nanos();
  const std::uint64_t stall_start = start + options.stall_after * 1'000'000'000ull;
  const std::uint64_t stall_end = stall_start + options.stall_seconds * 1'000'000'000ull;
  bool announced_stall = false, announced_resume = false;

  while (g_running.load(std::memory_order_relaxed)) {
    const std::uint64_t now = mono_nanos();
    if (now - start > options.seconds * 1'000'000'000ull) break;

    // Deliberately stop reading. The kernel receive buffer fills, then the plant's
    // send buffer fills, and from there the plant has to decide what to do about a
    // client that cannot keep up -- which is the whole point of the exercise.
    if (options.stall_seconds && now >= stall_start && now < stall_end) {
      if (!announced_stall) {
        std::printf("[sub] --- stalling for %llus (not reading the socket) ---\n",
                    static_cast<unsigned long long>(options.stall_seconds));
        std::fflush(stdout);
        announced_stall = true;
      }
      struct timespec nap { 0, 50'000'000 };
      ::nanosleep(&nap, nullptr);
      continue;
    }
    if (announced_stall && !announced_resume) {
      std::printf("[sub] --- resuming ---\n");
      std::fflush(stdout);
      announced_resume = true;
    }

    const long bytes = reader.fill(socket.fd());
    if (bytes < 0) {
      std::printf("[sub] connection closed by the plant\n");
      break;
    }
    if (bytes == 0) {
      struct timespec nap { 0, 200'000 };
      ::nanosleep(&nap, nullptr);
      continue;
    }

    std::uint8_t type = 0;
    const unsigned char* payload = nullptr;
    std::uint16_t payload_bytes = 0;
    while (reader.next_frame(type, payload, payload_bytes)) {
      switch (static_cast<proto::ClientMsgType>(type)) {
        case proto::ClientMsgType::kLoginAck: {
          proto::LoginAckMsg ack;
          std::memcpy(&ack, payload, sizeof(ack));
          if (ack.status != static_cast<std::uint8_t>(proto::LoginStatus::kOk)) {
            std::printf("[sub] login refused (status %u)\n", ack.status);
            return 1;
          }
          std::printf("[sub] logged in: session=%llu venues=0x%08x max_subs=%u\n",
                      static_cast<unsigned long long>(ack.session_id),
                      ack.entitled_venues, ack.max_subscriptions);
          break;
        }
        case proto::ClientMsgType::kSubscribeAck: {
          proto::SubscribeAckMsg ack;
          std::memcpy(&ack, payload, sizeof(ack));
          if (ack.status == static_cast<std::uint8_t>(proto::SubscribeStatus::kOk)) {
            ++acks_ok;
          } else {
            ++acks_refused;
            std::printf("[sub] %s: %s\n", Symbol(ack.symbol).text().c_str(),
                        subscribe_status_name(ack.status));
          }
          break;
        }
        case proto::ClientMsgType::kBookUpdate: {
          proto::BookUpdateMsg update;
          std::memcpy(&update, payload, sizeof(update));
          ++book_updates;
          conflated_total += update.conflated_count;
          if (update.conflated_count > max_conflated) max_conflated = update.conflated_count;
          if (update.ingest_ns) {
            const std::uint64_t age = hires_nanos();
            if (age > update.ingest_ns) staleness.record(age - update.ingest_ns);
          }
          if (options.print_book && update.bid_levels && update.ask_levels) {
            proto::PriceLevel bid, ask;
            std::memcpy(&bid, payload + sizeof(update), sizeof(bid));
            std::memcpy(&ask, payload + sizeof(update) +
                                  static_cast<std::size_t>(update.bid_levels) * sizeof(bid),
                        sizeof(ask));
            std::printf("  %-6s %10.4f x%-7u | %10.4f x%-7u  v%llu%s%s\n",
                        Symbol(update.symbol).text().c_str(), price_to_double(bid.price),
                        bid.quantity, price_to_double(ask.price), ask.quantity,
                        static_cast<unsigned long long>(update.version),
                        update.conflated_count ? "  (conflated)" : "",
                        (update.flags & proto::kBookStale) ? "  [STALE]" : "");
          }
          break;
        }
        case proto::ClientMsgType::kTradeTick: {
          proto::TradeTickMsg tick;
          std::memcpy(&tick, payload, sizeof(tick));
          ++trades;
          if (options.print_book) {
            std::printf("  TRADE %-6s %10.4f x%u\n", Symbol(tick.symbol).text().c_str(),
                        price_to_double(tick.price), tick.quantity);
          }
          break;
        }
        case proto::ClientMsgType::kSymbolStatus: {
          proto::SymbolStatusMsg status;
          std::memcpy(&status, payload, sizeof(status));
          ++stale_notices;
          std::printf("[sub] %s is now %s\n", Symbol(status.symbol).text().c_str(),
                      symbol_state_name(status.state));
          break;
        }
        case proto::ClientMsgType::kEvicted: {
          proto::EvictedMsg notice;
          std::memcpy(&notice, payload, sizeof(notice));
          std::printf("[sub] EVICTED: %s (backlog %llu bytes, stalled %.1f ms)\n",
                      evict_reason_name(notice.reason),
                      static_cast<unsigned long long>(notice.backlog_bytes),
                      static_cast<double>(notice.stalled_nanos) / 1e6);
          evicted = true;
          break;
        }
        case proto::ClientMsgType::kHeartbeat:
          break;
        default:
          break;
      }
    }
    if (evicted) break;
  }

  const double seconds = static_cast<double>(mono_nanos() - start) / 1e9;
  std::printf("\n[sub] ran for %.1fs\n", seconds);
  std::printf("[sub]   subscriptions   : %llu granted, %llu refused\n",
              static_cast<unsigned long long>(acks_ok),
              static_cast<unsigned long long>(acks_refused));
  std::printf("[sub]   book updates    : %llu (%.0f/s)\n",
              static_cast<unsigned long long>(book_updates),
              static_cast<double>(book_updates) / seconds);
  std::printf("[sub]   trades          : %llu\n", static_cast<unsigned long long>(trades));
  std::printf("[sub]   states conflated: %llu (largest single gap %llu)\n",
              static_cast<unsigned long long>(conflated_total),
              static_cast<unsigned long long>(max_conflated));
  std::printf("[sub]   stale notices   : %llu\n",
              static_cast<unsigned long long>(stale_notices));
  if (staleness.count()) {
    std::printf("%s\n", staleness.summary("[sub]   update age").c_str());
  }
  return evicted ? 2 : 0;
}
