// SPDX-License-Identifier: Apache-2.0
//
// cascaded -- the ticker plant.
//
// Consumes one or more multicast market-data channels, builds order books, and fans
// conflated state out to TCP subscribers.
//
// Thread layout, and the reason for it:
//
//   * One thread per channel, each owning a feed handler and a book shard outright.
//     A channel's messages identify orders by id alone, so an order's whole lifecycle
//     has to stay on the thread holding it; venues partition multicast groups by symbol
//     range for exactly this reason, and the plant mirrors that partition. Nothing in
//     the book-building path takes a lock.
//
//   * N fan-out threads, each owning a disjoint set of subscriber connections. Every
//     connection's socket, output buffer, subscriptions and timers are single-threaded.
//
//   * One acceptor thread, which does nothing but accept and hand off. Accepting on a
//     fan-out thread would put a blocking-ish syscall in the middle of the delivery
//     loop for the sake of an event that happens a few times a day.
//
// The only cross-thread contact between the tiers is the dirty bitmap and the trade
// ring, which shards write and fan-out threads drain.

#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "cascade/book/book_shard.hpp"
#include "cascade/core/clock.hpp"
#include "cascade/dist/fanout.hpp"
#include "cascade/feed/feed_handler.hpp"
#include "cascade/net/tcp.hpp"
#include "cascade/net/udp.hpp"
#include "cascade/proto/client.hpp"
#include "cascade/sim/market.hpp"

using namespace cascade;

namespace {

std::atomic<bool> g_running{true};
void handle_signal(int) { g_running.store(false, std::memory_order_release); }

struct ChannelSpec {
  std::string group{"239.10.20.30"};
  std::uint16_t port{31337};
  std::string recovery_host{"127.0.0.1"};
  std::uint16_t recovery_port{31338};
};

struct Options {
  std::vector<ChannelSpec> channels;
  std::string interface_address{"127.0.0.1"};
  std::string listen_address{"0.0.0.0"};
  std::uint16_t listen_port{31400};
  std::uint32_t instruments{200};
  std::uint32_t fanout_threads{2};
  std::uint64_t report_seconds{5};
};

// ---------------------------------------------------------------------------
// Feed channel
// ---------------------------------------------------------------------------

/// Sends retransmit requests to the venue's recovery server, and feeds the replies back
/// into the handler that asked for them.
class RecoveryClient : public feed::RecoveryRequester {
 public:
  RecoveryClient(std::string host, std::uint16_t port, const char session[10])
      : host_(std::move(host)), port_(port) {
    std::memcpy(session_, session, sizeof(session_));
  }

  void request_retransmit(std::uint64_t first_sequence, std::uint16_t count) override {
    if (!ensure_connected()) return;
    proto::RecoveryRequest request{};
    request.type = static_cast<std::uint8_t>(proto::RecoveryType::kRequest);
    std::memcpy(request.session, session_, sizeof(session_));
    request.first_sequence.set(first_sequence);
    request.count.set(count);

    const auto* bytes = reinterpret_cast<const unsigned char*>(&request);
    std::size_t sent = 0;
    while (sent < sizeof(request)) {
      const ssize_t written = ::write(socket_.fd(), bytes + sent, sizeof(request) - sent);
      if (written > 0) { sent += static_cast<std::size_t>(written); continue; }
      if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
        continue;
      }
      socket_.close();
      return;
    }
    ++requests_;
  }

  /// Drain replies. `deliver(payload, bytes, first_sequence, count)` on a response,
  /// `reject()` when the venue cannot serve the range.
  template <typename DeliverFn, typename RejectFn>
  void poll(DeliverFn&& deliver, RejectFn&& reject) {
    if (!socket_.valid()) return;
    unsigned char scratch[16 * 1024];
    const ssize_t bytes = ::read(socket_.fd(), scratch, sizeof(scratch));
    if (bytes == 0) { socket_.close(); return; }
    if (bytes < 0) {
      if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) socket_.close();
      return;
    }
    inbox_.insert(inbox_.end(), scratch, scratch + bytes);

    while (inbox_.size() >= sizeof(proto::RecoveryResponseHeader)) {
      proto::RecoveryResponseHeader header{};
      std::memcpy(&header, inbox_.data(), sizeof(header));
      const std::size_t payload_bytes = header.payload_bytes.value();
      const std::size_t total = sizeof(header) + payload_bytes;
      if (inbox_.size() < total) break;  // reply still arriving

      if (header.type == static_cast<std::uint8_t>(proto::RecoveryType::kReject)) {
        reject();
        ++rejects_;
      } else {
        deliver(inbox_.data() + sizeof(header), payload_bytes,
                header.first_sequence.value(), header.count.value());
        ++responses_;
      }
      inbox_.erase(inbox_.begin(), inbox_.begin() + static_cast<long>(total));
    }
  }

  std::uint64_t requests() const noexcept { return requests_; }
  std::uint64_t responses() const noexcept { return responses_; }
  std::uint64_t rejects() const noexcept { return rejects_; }

 private:
  bool ensure_connected() {
    if (socket_.valid()) return true;
    // Reconnect lazily. The recovery server being down must never stop the plant from
    // consuming live data -- a gap is bad, going dark is worse.
    const std::uint64_t now = mono_nanos();
    if (now - last_attempt_ns_ < 1'000'000'000ull) return false;
    last_attempt_ns_ = now;
    try {
      socket_ = net::tcp_connect(host_, port_, 500);
      net::set_nonblocking(socket_.fd(), true);
      return true;
    } catch (const net::SocketError&) {
      return false;
    }
  }

  std::string host_;
  std::uint16_t port_;
  char session_[10]{};
  net::Socket socket_;
  std::vector<unsigned char> inbox_;
  std::uint64_t last_attempt_ns_{0};
  std::uint64_t requests_{0}, responses_{0}, rejects_{0};
};

struct Channel {
  ChannelSpec spec;
  std::uint32_t id{0};
  net::UdpReceiver receiver;
  std::unique_ptr<feed::FeedHandler> handler;
  std::unique_ptr<RecoveryClient> recovery;
  book::BookShard* shard{nullptr};
  std::atomic<std::uint64_t> messages{0};
  std::thread thread;
};

void run_channel(Channel& channel) {
  unsigned char datagram[2048];
  auto sink = [&](const feed::FeedEvent& event) {
    channel.shard->apply(event);
    channel.messages.fetch_add(1, std::memory_order_relaxed);
  };

  while (g_running.load(std::memory_order_relaxed)) {
    bool idle = true;
    // Drain the socket in a batch. One datagram per wake-up would make the handler
    // syscall-bound well before the wire was saturated.
    for (int i = 0; i < 64; ++i) {
      const long bytes = channel.receiver.receive(datagram, sizeof(datagram));
      if (bytes <= 0) break;
      channel.handler->on_datagram(datagram, static_cast<std::size_t>(bytes),
                                   hires_nanos(), sink);
      idle = false;
    }

    const std::uint64_t now = hires_nanos();
    channel.handler->poll(now, sink);
    channel.recovery->poll(
        [&](const unsigned char* payload, std::size_t bytes, std::uint64_t first,
            std::uint16_t count) {
          channel.handler->on_recovered(payload, bytes, first, count, hires_nanos(), sink);
        },
        [&] { channel.handler->on_recovery_rejected(hires_nanos(), sink); });

    if (idle) {
      // Park rather than spin: a quiet channel must not hold a core the busy ones need.
      channel.receiver.wait_readable(1);
    }
  }
}

// ---------------------------------------------------------------------------
// Subscriber connections
// ---------------------------------------------------------------------------

struct Connection {
  net::Socket socket;
  std::unique_ptr<net::SocketSink> sink;
  net::FrameReader reader;
  std::string peer;
  std::uint32_t slot{dist::FanoutThread::kNoSlot};
  bool logged_in{false};
};

/// A fan-out thread plus the connections it owns.
struct FanoutWorker {
  std::unique_ptr<dist::FanoutThread> fanout;
  std::vector<std::unique_ptr<Connection>> connections;

  // The acceptor hands new connections over here. A mutex is entirely adequate: it is
  // taken on connect, not per message, and the alternative -- a lock-free queue -- would
  // add complexity to the coldest path in the program.
  std::mutex incoming_mutex;
  std::vector<std::unique_ptr<Connection>> incoming;

  std::thread thread;
  std::atomic<std::uint64_t> logins{0};
  std::atomic<std::uint64_t> disconnects{0};
};

/// Handle one client frame. Returns false to drop the connection.
bool handle_client_frame(FanoutWorker& worker, Connection& connection,
                         const dist::EntitlementTable& entitlements, std::uint8_t type,
                         const unsigned char* payload, std::uint16_t payload_bytes) {
  switch (static_cast<proto::ClientMsgType>(type)) {
    case proto::ClientMsgType::kLogin: {
      if (payload_bytes < sizeof(proto::LoginMsg)) return false;
      proto::LoginMsg login{};
      std::memcpy(&login, payload, sizeof(login));

      dist::Subscriber* subscriber = worker.fanout->subscriber_at(connection.slot);
      if (!subscriber) return false;

      if (login.protocol_version != proto::kClientProtocolVersion) {
        subscriber->encode_login_ack(proto::LoginStatus::kVersionMismatch, 0, 0);
        return false;
      }
      const std::string token(login.token, ::strnlen(login.token, sizeof(login.token)));
      dist::ClientEntitlement entitlement;
      if (!entitlements.resolve(token, entitlement)) {
        // Unknown token is a rejection, not a default-allow: market data is licensed.
        subscriber->encode_login_ack(proto::LoginStatus::kBadToken, 0, 0);
        return false;
      }
      subscriber->set_client(
          std::string(login.client_id, ::strnlen(login.client_id, sizeof(login.client_id))),
          entitlement);
      subscriber->encode_login_ack(proto::LoginStatus::kOk, entitlement.max_subscriptions,
                                   entitlement.venue_mask);
      connection.logged_in = true;
      worker.logins.fetch_add(1, std::memory_order_relaxed);
      return true;
    }

    case proto::ClientMsgType::kSubscribe: {
      if (!connection.logged_in) return false;  // no data before authentication
      if (payload_bytes < sizeof(proto::SubscribeMsg)) return false;
      proto::SubscribeMsg request{};
      std::memcpy(&request, payload, sizeof(request));
      const std::size_t needed =
          sizeof(request) + static_cast<std::size_t>(request.symbol_count) * 8;
      if (payload_bytes < needed) return false;

      for (std::uint16_t i = 0; i < request.symbol_count; ++i) {
        const Symbol symbol =
            Symbol::from_wire(payload + sizeof(request) + static_cast<std::size_t>(i) * 8);
        worker.fanout->subscribe(connection.slot, symbol, request.flags);
      }
      return true;
    }

    case proto::ClientMsgType::kUnsubscribe: {
      if (!connection.logged_in) return false;
      if (payload_bytes < sizeof(proto::SubscribeMsg)) return false;
      proto::SubscribeMsg request{};
      std::memcpy(&request, payload, sizeof(request));
      for (std::uint16_t i = 0; i < request.symbol_count; ++i) {
        const Symbol symbol =
            Symbol::from_wire(payload + sizeof(request) + static_cast<std::size_t>(i) * 8);
        worker.fanout->unsubscribe(connection.slot, symbol);
      }
      return true;
    }

    case proto::ClientMsgType::kClientHeartbeat:
      return true;

    default:
      // An unrecognised type is not fatal; frames are length-prefixed precisely so a
      // newer client can talk to an older server without desynchronising it.
      return true;
  }
}

void run_fanout(FanoutWorker& worker, const dist::EntitlementTable& entitlements,
                const Options& options) {
  while (g_running.load(std::memory_order_relaxed)) {
    {
      std::lock_guard<std::mutex> guard(worker.incoming_mutex);
      for (auto& connection : worker.incoming) {
        worker.connections.push_back(std::move(connection));
      }
      worker.incoming.clear();
    }

    for (std::size_t i = 0; i < worker.connections.size();) {
      Connection& connection = *worker.connections[i];
      bool alive = true;

      const long bytes = connection.reader.fill(connection.socket.fd());
      if (bytes < 0) {
        alive = false;
      } else {
        std::uint8_t type = 0;
        const unsigned char* payload = nullptr;
        std::uint16_t payload_bytes = 0;
        while (alive && connection.reader.next_frame(type, payload, payload_bytes)) {
          alive = handle_client_frame(worker, connection, entitlements, type, payload,
                                      payload_bytes);
        }
      }

      // The fan-out may also have evicted it for being too slow.
      if (alive && worker.fanout->subscriber_at(connection.slot) == nullptr) alive = false;

      if (!alive) {
        std::printf("[cascaded] subscriber disconnected: %s\n", connection.peer.c_str());
        worker.fanout->release(connection.slot);
        worker.disconnects.fetch_add(1, std::memory_order_relaxed);
        worker.connections[i] = std::move(worker.connections.back());
        worker.connections.pop_back();
      } else {
        ++i;
      }
    }

    if (!worker.fanout->has_work() && worker.connections.empty()) {
      struct timespec nap { 0, 500'000 };  // 0.5ms
      ::nanosleep(&nap, nullptr);
      continue;
    }
    worker.fanout->poll(hires_nanos());
  }
  (void)options;
}

// ---------------------------------------------------------------------------

void usage() {
  std::printf(
      "cascaded -- market data ticker plant\n\n"
      "  --channel G:P:RH:RP  feed channel: group, port, recovery host, recovery port\n"
      "                       (repeatable; default 239.10.20.30:31337:127.0.0.1:31338)\n"
      "  --interface ADDR     multicast interface (default 127.0.0.1)\n"
      "  --listen ADDR        subscriber bind address (default 0.0.0.0)\n"
      "  --port N             subscriber port (default 31400)\n"
      "  --instruments N      instruments in the security master (default 200)\n"
      "  --fanout N           fan-out threads (default 2)\n"
      "  --report N           status interval in seconds (default 5)\n"
      "  --help\n");
}

bool parse_channel(const std::string& text, ChannelSpec& spec) {
  std::vector<std::string> parts;
  std::size_t start = 0;
  while (start <= text.size()) {
    const std::size_t colon = text.find(':', start);
    parts.push_back(text.substr(start, colon == std::string::npos ? std::string::npos
                                                                  : colon - start));
    if (colon == std::string::npos) break;
    start = colon + 1;
  }
  if (parts.size() < 2) return false;
  spec.group = parts[0];
  spec.port = static_cast<std::uint16_t>(std::strtoul(parts[1].c_str(), nullptr, 10));
  if (parts.size() > 2) spec.recovery_host = parts[2];
  if (parts.size() > 3) {
    spec.recovery_port = static_cast<std::uint16_t>(std::strtoul(parts[3].c_str(), nullptr, 10));
  }
  return true;
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
    else if (argument == "--channel" && next(text)) {
      ChannelSpec spec;
      if (!parse_channel(text, spec)) { std::printf("bad --channel: %s\n", text.c_str()); return false; }
      options.channels.push_back(spec);
    }
    else if (argument == "--interface" && next(text)) options.interface_address = text;
    else if (argument == "--listen" && next(text)) options.listen_address = text;
    else if (argument == "--port" && next(text)) options.listen_port = static_cast<std::uint16_t>(std::strtoul(text.c_str(), nullptr, 10));
    else if (argument == "--instruments" && next(text)) options.instruments = static_cast<std::uint32_t>(std::strtoul(text.c_str(), nullptr, 10));
    else if (argument == "--fanout" && next(text)) options.fanout_threads = static_cast<std::uint32_t>(std::strtoul(text.c_str(), nullptr, 10));
    else if (argument == "--report" && next(text)) options.report_seconds = std::strtoull(text.c_str(), nullptr, 10);
    else { std::printf("unknown argument: %s\n\n", argument.c_str()); usage(); return false; }
  }
  if (options.channels.empty()) options.channels.push_back(ChannelSpec{});
  if (options.fanout_threads == 0) options.fanout_threads = 1;
  if (options.fanout_threads > book::kMaxFanoutThreads) {
    options.fanout_threads = book::kMaxFanoutThreads;
  }
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  Options options;
  if (!parse(argc, argv, options)) return 0;

  std::signal(SIGINT, handle_signal);
  std::signal(SIGTERM, handle_signal);
  std::signal(SIGPIPE, SIG_IGN);  // a subscriber hanging up must not kill the plant

  const std::uint32_t channel_count = static_cast<std::uint32_t>(options.channels.size());

  // --- security master ------------------------------------------------------
  // Built once, before any traffic, so the symbol maps are read-only for the whole
  // session and the fan-out can resolve subscriptions without locking against the
  // shards. Venues publish exactly this ahead of the open.
  dist::EntitlementTable entitlements;
  entitlements.register_venue(0, "CASCADE-SIM");

  dist::InstrumentRegistry registry;
  std::vector<std::unique_ptr<book::BookShard>> shards;
  std::vector<book::BookShard*> shard_pointers;
  for (std::uint32_t id = 0; id < channel_count; ++id) {
    book::BookShard::Config config;
    config.shard_id = id;
    config.max_symbols = options.instruments + 16;
    config.initial_orders = 1u << 20;
    shards.push_back(std::make_unique<book::BookShard>(config));
    shard_pointers.push_back(shards.back().get());
  }

  {
    sim::MarketSimulator::Config naming;
    naming.instrument_count = options.instruments;
    sim::MarketSimulator naming_source(naming);
    for (Symbol symbol : naming_source.symbols()) {
      const std::uint32_t shard_id = book::BookShard::shard_for(symbol, channel_count);
      const std::uint32_t index = shards[shard_id]->register_symbol(symbol);
      registry.add(symbol, dist::InstrumentLocation{shard_id, index});
      entitlements.assign_symbol(symbol, 0);
    }
  }

  // Two tiers of access, so the entitlement path is exercised rather than decorative.
  {
    dist::ClientEntitlement full;
    full.client_id = "full";
    full.venue_mask = dist::EntitlementTable::mask_of({0});
    full.max_subscriptions = options.instruments;
    full.allow_incremental = true;
    full.allow_trades = true;
    entitlements.grant("cascade-demo-token", full);

    dist::ClientEntitlement limited;
    limited.client_id = "limited";
    limited.venue_mask = 0;  // authenticated, but licensed for nothing
    limited.max_subscriptions = 10;
    entitlements.grant("cascade-unentitled", limited);
  }

  // --- fan-out tier ---------------------------------------------------------
  std::vector<std::unique_ptr<FanoutWorker>> workers;
  for (std::uint32_t id = 0; id < options.fanout_threads; ++id) {
    auto worker = std::make_unique<FanoutWorker>();
    dist::FanoutThread::Config config;
    config.fanout_id = id;
    config.max_subscribers = 512;
    config.dirty_capacity = options.instruments + 16;
    worker->fanout = std::make_unique<dist::FanoutThread>(config, shard_pointers,
                                                          &registry, &entitlements);
    workers.push_back(std::move(worker));
  }

  // --- feed channels --------------------------------------------------------
  char session[10];
  std::memcpy(session, "CASCADE001", sizeof(session));

  std::vector<std::unique_ptr<Channel>> channels;
  for (std::uint32_t id = 0; id < channel_count; ++id) {
    auto channel = std::make_unique<Channel>();
    channel->spec = options.channels[id];
    channel->id = id;
    channel->shard = shards[id].get();
    channel->handler = std::make_unique<feed::FeedHandler>();
    channel->recovery = std::make_unique<RecoveryClient>(channel->spec.recovery_host,
                                                          channel->spec.recovery_port,
                                                          session);
    channel->handler->set_recovery_requester(channel->recovery.get());

    // An unrecoverable gap means every book on this channel was built from an
    // incomplete delta stream and is silently wrong. The only honest response is to
    // throw them away and tell subscribers at once.
    book::BookShard* shard = channel->shard;
    const std::uint32_t channel_id = id;
    channel->handler->set_loss_handler(
        [shard, channel_id](std::uint64_t first, std::uint64_t count, std::uint64_t now) {
          std::fprintf(stderr,
                       "[cascaded] channel %u: %llu messages lost from sequence %llu; "
                       "marking every book on this channel stale\n",
                       channel_id, static_cast<unsigned long long>(count),
                       static_cast<unsigned long long>(first));
          shard->mark_all_stale(now);
        });

    try {
      channel->receiver.open(net::Endpoint{channel->spec.group, channel->spec.port},
                             options.interface_address, /*loopback=*/true);
    } catch (const net::SocketError& error) {
      std::fprintf(stderr, "[cascaded] channel %u failed to open: %s\n", id, error.what());
      return 1;
    }
    channels.push_back(std::move(channel));
  }

  net::TcpListener listener;
  try {
    listener.open(options.listen_address, options.listen_port);
  } catch (const net::SocketError& error) {
    std::fprintf(stderr, "[cascaded] listen failed: %s\n", error.what());
    return 1;
  }

  std::printf("[cascaded] %u instrument(s) across %u channel(s), %u fan-out thread(s)\n",
              options.instruments, channel_count, options.fanout_threads);
  for (const auto& channel : channels) {
    std::printf("[cascaded]   channel %u: %s:%u (recovery %s:%u)\n", channel->id,
                channel->spec.group.c_str(), channel->spec.port,
                channel->spec.recovery_host.c_str(), channel->spec.recovery_port);
  }
  std::printf("[cascaded] subscribers welcome on %s:%u\n", options.listen_address.c_str(),
              options.listen_port);
  std::fflush(stdout);

  for (auto& channel : channels) {
    channel->thread = std::thread([&channel] { run_channel(*channel); });
  }
  for (auto& worker : workers) {
    worker->thread = std::thread(
        [&worker, &entitlements, &options] { run_fanout(*worker, entitlements, options); });
  }

  // --- acceptor (this thread) ----------------------------------------------
  std::uint32_t next_worker = 0;
  std::uint64_t last_report = mono_nanos();
  std::uint64_t accepted_total = 0;

  while (g_running.load(std::memory_order_relaxed)) {
    std::string peer;
    net::Socket accepted = listener.accept(&peer);
    if (accepted.valid()) {
      auto connection = std::make_unique<Connection>();
      connection->peer = peer;
      connection->sink = std::make_unique<net::SocketSink>(accepted.fd());
      connection->socket = std::move(accepted);

      dist::Subscriber::Config config;
      config.output_capacity = 4u << 20;
      config.stall_deadline_ns = 5'000'000'000ull;
      auto subscriber =
          std::make_unique<dist::Subscriber>(++accepted_total, config, connection->sink.get());

      FanoutWorker& worker = *workers[next_worker % options.fanout_threads];
      ++next_worker;
      const std::uint32_t slot = worker.fanout->admit(std::move(subscriber));
      if (slot == dist::FanoutThread::kNoSlot) {
        std::fprintf(stderr, "[cascaded] refusing %s: fan-out full\n", peer.c_str());
      } else {
        connection->slot = slot;
        std::printf("[cascaded] subscriber connected: %s -> fan-out %u slot %u\n",
                    peer.c_str(), worker.fanout->fanout_id(), slot);
        std::fflush(stdout);
        std::lock_guard<std::mutex> guard(worker.incoming_mutex);
        worker.incoming.push_back(std::move(connection));
      }
    } else {
      struct timespec nap { 0, 2'000'000 };  // 2ms; connects are rare
      ::nanosleep(&nap, nullptr);
    }

    const std::uint64_t now = mono_nanos();
    if (options.report_seconds &&
        now - last_report > options.report_seconds * 1'000'000'000ull) {
      last_report = now;
      for (const auto& channel : channels) {
        const feed::FeedHandler::Stats& feed_stats = channel->handler->stats();
        const book::BookShard::Stats& shard_stats = channel->shard->stats();
        const std::uint64_t changes =
            shard_stats.books_published + shard_stats.publishes_suppressed;
        std::printf(
            "[cascaded] ch%u  pkts=%llu msgs=%llu gaps=%llu(rec %llu lost %llu) "
            "dup=%llu reord=%llu | books=%llu suppressed=%.1f%% trades=%llu\n",
            channel->id, static_cast<unsigned long long>(feed_stats.packets_received),
            static_cast<unsigned long long>(feed_stats.messages_delivered),
            static_cast<unsigned long long>(feed_stats.gaps_detected),
            static_cast<unsigned long long>(feed_stats.gaps_recovered),
            static_cast<unsigned long long>(feed_stats.messages_lost),
            static_cast<unsigned long long>(feed_stats.packets_duplicate),
            static_cast<unsigned long long>(feed_stats.packets_reordered),
            static_cast<unsigned long long>(shard_stats.books_published),
            changes ? 100.0 * static_cast<double>(shard_stats.publishes_suppressed) /
                          static_cast<double>(changes)
                    : 0.0,
            static_cast<unsigned long long>(shard_stats.trades));
      }
      for (const auto& worker : workers) {
        const dist::FanoutThread::Stats& stats = worker->fanout->stats();
        std::printf("[cascaded] fan-out %u  subscribers=%zu routed=%llu trades=%llu "
                    "granted=%llu refused=%llu evicted=%llu\n",
                    worker->fanout->fanout_id(), worker->fanout->subscriber_count(),
                    static_cast<unsigned long long>(stats.instruments_routed),
                    static_cast<unsigned long long>(stats.trades_routed),
                    static_cast<unsigned long long>(stats.subscriptions_granted),
                    static_cast<unsigned long long>(stats.subscriptions_refused),
                    static_cast<unsigned long long>(stats.subscribers_evicted));
      }
      std::fflush(stdout);
    }
  }

  std::printf("\n[cascaded] shutting down\n");
  for (auto& channel : channels) if (channel->thread.joinable()) channel->thread.join();
  for (auto& worker : workers) if (worker->thread.joinable()) worker->thread.join();
  return 0;
}
