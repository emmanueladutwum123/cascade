// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <string>

#include "cascade/net/socket.hpp"

namespace cascade::net {

struct Endpoint {
  std::string address;
  std::uint16_t port{0};
};

/// Receives a multicast (or plain unicast) UDP feed.
///
/// Multicast is how every real venue distributes market data: the exchange sends one
/// copy and the network replicates it, so the venue's cost is independent of how many
/// participants are listening, and every participant gets the same bytes at the same
/// time — which is a fairness property, not just an efficiency one. The price is that
/// there are no retransmits, no ordering and no flow control, which is why everything
/// in `FeedHandler` exists.
class UdpReceiver {
 public:
  UdpReceiver() = default;

  /// Bind to `port` and join `group` if it is a multicast address.
  ///
  /// `interface_address` selects which NIC to join on. It is not optional in practice:
  /// a host with several interfaces will otherwise join on whichever one the routing
  /// table prefers, which on a trading host is reliably the wrong one.
  void open(const Endpoint& group, const std::string& interface_address = "",
            bool loopback = false);

  /// Receive one datagram. Returns bytes read, 0 if nothing is ready (non-blocking),
  /// or -1 on error. Never throws: datagram loss is an expected condition here.
  long receive(void* buffer, std::size_t capacity) noexcept;

  /// Block until a datagram arrives or `timeout_ms` elapses. Returns false on timeout.
  bool wait_readable(int timeout_ms) noexcept;

  int fd() const noexcept { return socket_.fd(); }
  bool is_open() const noexcept { return socket_.valid(); }
  void close() noexcept { socket_.close(); }

  /// Datagrams the kernel dropped because our receive buffer was full. This is the
  /// number that distinguishes "the exchange dropped it" from "we were too slow",
  /// and without it a gap is unattributable.
  std::uint64_t kernel_drops() const noexcept;

 private:
  Socket socket_;
  std::string group_;
  std::string interface_;
  bool joined_{false};
};

/// Sends a multicast UDP feed. Used by the exchange simulator and by nothing else in
/// production — the plant is a consumer of market data, not a publisher of it.
class UdpSender {
 public:
  UdpSender() = default;

  void open(const Endpoint& destination, const std::string& interface_address = "",
            int ttl = 1, bool loopback = true);

  /// Returns bytes sent, or -1. A partial send is impossible for UDP: the datagram
  /// goes whole or not at all.
  long send(const void* data, std::size_t bytes) noexcept;

  int fd() const noexcept { return socket_.fd(); }
  bool is_open() const noexcept { return socket_.valid(); }
  void close() noexcept { socket_.close(); }

 private:
  Socket socket_;
  // Stored as raw storage so the header does not drag <netinet/in.h> into every
  // translation unit that merely wants to hold a sender.
  alignas(8) unsigned char destination_[128]{};
  std::uint32_t destination_len_{0};
};

}  // namespace cascade::net
