// SPDX-License-Identifier: Apache-2.0
#include "cascade/net/socket.hpp"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <unistd.h>

namespace cascade::net {

SocketError::SocketError(const std::string& what, int error_number)
    : errno_(error_number) {
  message_ = what;
  message_ += ": ";
  message_ += std::strerror(error_number);
  message_ += " (errno ";
  message_ += std::to_string(error_number);
  message_ += ")";
}

void Socket::close() noexcept {
  if (fd_ >= 0) {
    // close() can report EINTR, and on Linux the descriptor is released anyway, so
    // retrying would risk closing a descriptor another thread has since been handed.
    // Close once, discard the result.
    ::close(fd_);
    fd_ = -1;
  }
}

void set_nonblocking(int fd, bool enable) {
  const int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0) throw SocketError("fcntl(F_GETFL)", errno);
  const int updated = enable ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK);
  if (::fcntl(fd, F_SETFL, updated) < 0) throw SocketError("fcntl(F_SETFL)", errno);
}

void set_reuse_addr(int fd) {
  int on = 1;
  if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) < 0) {
    throw SocketError("setsockopt(SO_REUSEADDR)", errno);
  }
#ifdef SO_REUSEPORT
  // Multiple processes on one host routinely consume the same multicast group — a
  // feed handler and a capture tool, say — and without SO_REUSEPORT the second bind
  // fails outright.
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &on, sizeof(on));
#endif
}

void set_tcp_nodelay(int fd) {
  int on = 1;
  if (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on)) < 0) {
    throw SocketError("setsockopt(TCP_NODELAY)", errno);
  }
}

int set_recv_buffer(int fd, int bytes) {
  ::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &bytes, sizeof(bytes));
  int actual = 0;
  socklen_t length = sizeof(actual);
  if (::getsockopt(fd, SOL_SOCKET, SO_RCVBUF, &actual, &length) < 0) return -1;
  // Linux reports double what was requested (it reserves half for bookkeeping);
  // report the raw kernel value and let the caller compare against what it asked for.
  return actual;
}

int set_send_buffer(int fd, int bytes) {
  ::setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &bytes, sizeof(bytes));
  int actual = 0;
  socklen_t length = sizeof(actual);
  if (::getsockopt(fd, SOL_SOCKET, SO_SNDBUF, &actual, &length) < 0) return -1;
  return actual;
}

int pending_recv_bytes(int fd) {
  int pending = 0;
  if (::ioctl(fd, FIONREAD, &pending) < 0) return -1;
  return pending;
}

}  // namespace cascade::net
