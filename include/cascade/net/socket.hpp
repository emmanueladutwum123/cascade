// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstddef>
#include <cstdint>
#include <exception>
#include <string>

namespace cascade::net {

/// An RAII file descriptor.
///
/// Sockets are the one resource in this system whose leak is silent and fatal: the
/// process keeps running, fd count creeps, and accept() starts failing hours later
/// under load. Ownership is therefore explicit and move-only.
class Socket {
 public:
  Socket() = default;
  explicit Socket(int fd) noexcept : fd_(fd) {}
  ~Socket() { close(); }

  Socket(Socket&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }
  Socket& operator=(Socket&& other) noexcept {
    if (this != &other) {
      close();
      fd_ = other.fd_;
      other.fd_ = -1;
    }
    return *this;
  }
  Socket(const Socket&) = delete;
  Socket& operator=(const Socket&) = delete;

  int fd() const noexcept { return fd_; }
  bool valid() const noexcept { return fd_ >= 0; }
  explicit operator bool() const noexcept { return valid(); }

  int release() noexcept {
    const int fd = fd_;
    fd_ = -1;
    return fd;
  }
  void close() noexcept;

 private:
  int fd_{-1};
};

/// Thrown for socket setup failures. Configuration errors (a bad address, a port in
/// use, a missing multicast route) are startup problems that must be loud; the hot
/// paths below report per-operation failures by return value instead, because a
/// dropped datagram is normal and must not cost an exception.
class SocketError : public std::exception {
 public:
  SocketError(const std::string& what, int error_number);
  const char* what() const noexcept override { return message_.c_str(); }
  int error_number() const noexcept { return errno_; }

 private:
  std::string message_;
  int errno_{0};
};

void set_nonblocking(int fd, bool enable);
void set_reuse_addr(int fd);

/// Disable Nagle's algorithm.
///
/// Nagle holds a small write back until the previous one is acknowledged, to avoid
/// flooding the network with tiny packets. For market data that is exactly wrong: a
/// book update is small *and* urgent, and holding one for an RTT to save a header adds
/// tens of milliseconds of latency to the one thing subscribers pay for.
void set_tcp_nodelay(int fd);

/// Enlarge a socket buffer, returning the size the kernel actually granted.
///
/// The kernel silently clamps to a system maximum, so asking is not getting. A
/// receive buffer too small for a burst drops datagrams inside the kernel, where the
/// application cannot see it happen — the returned value is checked and logged at
/// startup rather than assumed.
int set_recv_buffer(int fd, int bytes);
int set_send_buffer(int fd, int bytes);

/// Bytes sitting unread in the socket's receive queue. This is the direct measure of
/// whether the feed handler is keeping up with the wire.
int pending_recv_bytes(int fd);

}  // namespace cascade::net
