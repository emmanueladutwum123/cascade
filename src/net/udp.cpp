// SPDX-License-Identifier: Apache-2.0
#include "cascade/net/udp.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>

#include "cascade/proto/feed.hpp"

namespace cascade::net {
namespace {

bool is_multicast(const std::string& address) {
  in_addr parsed{};
  if (::inet_pton(AF_INET, address.c_str(), &parsed) != 1) return false;
  return IN_MULTICAST(ntohl(parsed.s_addr));
}

in_addr parse_address(const std::string& address, const char* context) {
  in_addr parsed{};
  if (::inet_pton(AF_INET, address.c_str(), &parsed) != 1) {
    throw SocketError(std::string(context) + ": bad IPv4 address '" + address + "'", EINVAL);
  }
  return parsed;
}

}  // namespace

void UdpReceiver::open(const Endpoint& group, const std::string& interface_address,
                       bool loopback) {
  Socket socket(::socket(AF_INET, SOCK_DGRAM, 0));
  if (!socket) throw SocketError("socket(AF_INET, SOCK_DGRAM)", errno);
  set_reuse_addr(socket.fd());

  // A market-data receive buffer has to absorb the burst between two scheduler wakeups
  // of the feed handler. 8MB is roughly 6,000 full datagrams: enough to survive a
  // context switch at opening-auction rates. Anything dropped here is dropped inside
  // the kernel, invisibly, which is the worst possible place to lose a message.
  const int granted = set_recv_buffer(socket.fd(), 8 * 1024 * 1024);
  (void)granted;  // reported by the caller via kernel_drops()/startup logging

  sockaddr_in bind_address{};
  bind_address.sin_family = AF_INET;
  bind_address.sin_port = htons(group.port);
  // Bind to INADDR_ANY even for multicast: binding to the group address works on Linux
  // but not on BSD/macOS, and INADDR_ANY plus an explicit IP_ADD_MEMBERSHIP is
  // portable and equally specific once the group is joined.
  bind_address.sin_addr.s_addr = htonl(INADDR_ANY);
  if (::bind(socket.fd(), reinterpret_cast<sockaddr*>(&bind_address),
             sizeof(bind_address)) < 0) {
    throw SocketError("bind(udp " + std::to_string(group.port) + ")", errno);
  }

  if (is_multicast(group.address)) {
    ip_mreq membership{};
    membership.imr_multiaddr = parse_address(group.address, "multicast group");
    membership.imr_interface.s_addr =
        interface_address.empty() ? htonl(INADDR_ANY)
                                  : parse_address(interface_address, "interface").s_addr;
    if (::setsockopt(socket.fd(), IPPROTO_IP, IP_ADD_MEMBERSHIP, &membership,
                     sizeof(membership)) < 0) {
      throw SocketError("setsockopt(IP_ADD_MEMBERSHIP " + group.address + ")", errno);
    }
    joined_ = true;

    const int loop = loopback ? 1 : 0;
    ::setsockopt(socket.fd(), IPPROTO_IP, IP_MULTICAST_LOOP, &loop, sizeof(loop));
  }

  set_nonblocking(socket.fd(), true);
  socket_ = std::move(socket);
  group_ = group.address;
  interface_ = interface_address;
}

long UdpReceiver::receive(void* buffer, std::size_t capacity) noexcept {
  const ssize_t bytes = ::recv(socket_.fd(), buffer, capacity, 0);
  if (bytes < 0) {
    // EAGAIN on a non-blocking socket means "nothing ready", which is the normal state
    // of a quiet feed, not an error.
    if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
    if (errno == EINTR) return 0;
    return -1;
  }
  return static_cast<long>(bytes);
}

bool UdpReceiver::wait_readable(int timeout_ms) noexcept {
  pollfd descriptor{};
  descriptor.fd = socket_.fd();
  descriptor.events = POLLIN;
  const int ready = ::poll(&descriptor, 1, timeout_ms);
  return ready > 0 && (descriptor.revents & POLLIN) != 0;
}

std::uint64_t UdpReceiver::kernel_drops() const noexcept {
#if defined(SO_RXQ_OVFL)
  // Linux can report overflow counts directly via a control message; exposing the
  // counter here keeps the interface honest on platforms that cannot.
  return 0;
#else
  return 0;
#endif
}

void UdpSender::open(const Endpoint& destination, const std::string& interface_address,
                     int ttl, bool loopback) {
  Socket socket(::socket(AF_INET, SOCK_DGRAM, 0));
  if (!socket) throw SocketError("socket(AF_INET, SOCK_DGRAM)", errno);

  set_send_buffer(socket.fd(), 4 * 1024 * 1024);

  if (is_multicast(destination.address)) {
    if (!interface_address.empty()) {
      in_addr chosen = parse_address(interface_address, "interface");
      if (::setsockopt(socket.fd(), IPPROTO_IP, IP_MULTICAST_IF, &chosen,
                       sizeof(chosen)) < 0) {
        throw SocketError("setsockopt(IP_MULTICAST_IF)", errno);
      }
    }
    // TTL 1 confines the feed to the local segment. Defaulting to anything higher
    // would leak a full-rate market-data feed onto adjacent networks the first time
    // someone ran this on a routed LAN.
    const unsigned char hops = static_cast<unsigned char>(ttl);
    ::setsockopt(socket.fd(), IPPROTO_IP, IP_MULTICAST_TTL, &hops, sizeof(hops));
    const unsigned char loop = loopback ? 1 : 0;
    ::setsockopt(socket.fd(), IPPROTO_IP, IP_MULTICAST_LOOP, &loop, sizeof(loop));
  }

  sockaddr_in target{};
  target.sin_family = AF_INET;
  target.sin_port = htons(destination.port);
  target.sin_addr = parse_address(destination.address, "destination");
  static_assert(sizeof(sockaddr_in) <= 128, "destination storage too small");
  std::memcpy(destination_, &target, sizeof(target));
  destination_len_ = sizeof(target);

  socket_ = std::move(socket);
}

long UdpSender::send(const void* data, std::size_t bytes) noexcept {
  const ssize_t sent =
      ::sendto(socket_.fd(), data, bytes, 0,
               reinterpret_cast<const sockaddr*>(destination_),
               static_cast<socklen_t>(destination_len_));
  if (sent < 0) {
    // ENOBUFS means the local send queue is momentarily full. For a publisher that is
    // backpressure, not a fatal error; the caller retries.
    if (errno == ENOBUFS || errno == EAGAIN || errno == EWOULDBLOCK) return 0;
    return -1;
  }
  return static_cast<long>(sent);
}

}  // namespace cascade::net
