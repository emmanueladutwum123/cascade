// SPDX-License-Identifier: Apache-2.0
#include "cascade/net/tcp.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#include "cascade/proto/client.hpp"

namespace cascade::net {
namespace {

in_addr parse_ipv4(const std::string& address, const char* context) {
  in_addr parsed{};
  if (address.empty() || address == "0.0.0.0") {
    parsed.s_addr = htonl(INADDR_ANY);
    return parsed;
  }
  if (::inet_pton(AF_INET, address.c_str(), &parsed) != 1) {
    throw SocketError(std::string(context) + ": bad IPv4 address '" + address + "'", EINVAL);
  }
  return parsed;
}

}  // namespace

void TcpListener::open(const std::string& bind_address, std::uint16_t port, int backlog) {
  Socket socket(::socket(AF_INET, SOCK_STREAM, 0));
  if (!socket) throw SocketError("socket(AF_INET, SOCK_STREAM)", errno);
  set_reuse_addr(socket.fd());

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  address.sin_addr = parse_ipv4(bind_address, "listen address");
  if (::bind(socket.fd(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
    throw SocketError("bind(tcp " + std::to_string(port) + ")", errno);
  }
  if (::listen(socket.fd(), backlog) < 0) throw SocketError("listen", errno);

  set_nonblocking(socket.fd(), true);
  socket_ = std::move(socket);
}

Socket TcpListener::accept(std::string* peer) noexcept {
  sockaddr_in address{};
  socklen_t length = sizeof(address);
  const int fd = ::accept(socket_.fd(), reinterpret_cast<sockaddr*>(&address), &length);
  if (fd < 0) return Socket();

  if (peer) {
    char text[INET_ADDRSTRLEN] = {0};
    ::inet_ntop(AF_INET, &address.sin_addr, text, sizeof(text));
    *peer = std::string(text) + ":" + std::to_string(ntohs(address.sin_port));
  }

  Socket accepted(fd);
  try {
    set_nonblocking(fd, true);
    set_tcp_nodelay(fd);
    // A subscriber's send buffer has to absorb a burst without the fan-out thread
    // blocking; 4MB is roughly a second of a busy client's traffic.
    set_send_buffer(fd, 4 * 1024 * 1024);
  } catch (const SocketError&) {
    return Socket();  // the fd is closed by `accepted`'s destructor
  }
  return accepted;
}

Socket tcp_connect(const std::string& address, std::uint16_t port, int timeout_ms) {
  Socket socket(::socket(AF_INET, SOCK_STREAM, 0));
  if (!socket) throw SocketError("socket(AF_INET, SOCK_STREAM)", errno);

  sockaddr_in target{};
  target.sin_family = AF_INET;
  target.sin_port = htons(port);
  target.sin_addr = parse_ipv4(address, "connect address");

  set_nonblocking(socket.fd(), true);
  if (::connect(socket.fd(), reinterpret_cast<sockaddr*>(&target), sizeof(target)) < 0) {
    if (errno != EINPROGRESS) {
      throw SocketError("connect(" + address + ":" + std::to_string(port) + ")", errno);
    }
    pollfd descriptor{};
    descriptor.fd = socket.fd();
    descriptor.events = POLLOUT;
    if (::poll(&descriptor, 1, timeout_ms) <= 0) {
      throw SocketError("connect timed out to " + address, ETIMEDOUT);
    }
    // poll() reporting writable is not proof the connect succeeded: a refused
    // connection also wakes it. The pending error has to be read explicitly.
    int error = 0;
    socklen_t length = sizeof(error);
    if (::getsockopt(socket.fd(), SOL_SOCKET, SO_ERROR, &error, &length) < 0 || error) {
      throw SocketError("connect(" + address + ")", error ? error : errno);
    }
  }
  set_tcp_nodelay(socket.fd());
  return socket;
}

long SocketSink::write_some(const OutputBuffer::Span* spans, int span_count) {
  iovec vectors[2];
  int count = 0;
  for (int i = 0; i < span_count && i < 2; ++i) {
    if (spans[i].length == 0) continue;
    vectors[count].iov_base = const_cast<unsigned char*>(spans[i].data);
    vectors[count].iov_len = spans[i].length;
    ++count;
  }
  if (count == 0) return 0;

  const ssize_t written = ::writev(fd_, vectors, count);
  if (written < 0) {
    // A full socket buffer is backpressure, not failure: it is the signal that makes
    // the subscriber conflate.
    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) return 0;
    return -1;
  }
  bytes_written_ += static_cast<std::uint64_t>(written);
  return static_cast<long>(written);
}

long FrameReader::fill(int fd) {
  // Compact the consumed prefix before growing, so a long-lived connection does not
  // accumulate a buffer proportional to everything it has ever received.
  if (read_offset_ > 0 && read_offset_ == buffer_.size()) {
    buffer_.clear();
    read_offset_ = 0;
  } else if (read_offset_ > (1u << 15)) {
    buffer_.erase(buffer_.begin(), buffer_.begin() + static_cast<long>(read_offset_));
    read_offset_ = 0;
  }

  unsigned char scratch[16 * 1024];
  const ssize_t bytes = ::read(fd, scratch, sizeof(scratch));
  if (bytes == 0) return -1;  // orderly close
  if (bytes < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) return 0;
    return -1;
  }
  buffer_.insert(buffer_.end(), scratch, scratch + bytes);
  return static_cast<long>(bytes);
}

bool FrameReader::next_frame(std::uint8_t& type, const unsigned char*& payload,
                             std::uint16_t& payload_bytes) {
  if (buffer_.size() - read_offset_ < sizeof(proto::FrameHeader)) return false;
  proto::FrameHeader header;
  std::memcpy(&header, buffer_.data() + read_offset_, sizeof(header));
  const std::size_t total = sizeof(header) + header.payload_bytes;
  if (buffer_.size() - read_offset_ < total) return false;

  type = header.type;
  payload_bytes = header.payload_bytes;
  payload = buffer_.data() + read_offset_ + sizeof(header);
  read_offset_ += total;
  return true;
}

}  // namespace cascade::net
