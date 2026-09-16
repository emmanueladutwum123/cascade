// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <string>

#include "cascade/net/output_buffer.hpp"
#include "cascade/net/socket.hpp"

#include <vector>

namespace cascade::net {

/// Accepts subscriber connections.
class TcpListener {
 public:
  TcpListener() = default;

  void open(const std::string& bind_address, std::uint16_t port, int backlog = 128);

  /// Accept one connection. Returns an invalid socket if none is pending.
  /// `peer` receives "address:port" for logging.
  Socket accept(std::string* peer = nullptr) noexcept;

  int fd() const noexcept { return socket_.fd(); }
  bool is_open() const noexcept { return socket_.valid(); }
  void close() noexcept { socket_.close(); }

 private:
  Socket socket_;
};

/// Connects out to a retransmit server or to the plant.
Socket tcp_connect(const std::string& address, std::uint16_t port,
                   int timeout_ms = 5000);

/// A `ByteSink` over a non-blocking TCP socket.
///
/// `writev` rather than two `write` calls: the output ring wraps at most once, and
/// handing both fragments to the kernel in one syscall halves the system-call count on
/// exactly the path that runs per subscriber per fan-out pass.
class SocketSink : public ByteSink {
 public:
  explicit SocketSink(int fd) noexcept : fd_(fd) {}

  long write_some(const OutputBuffer::Span* spans, int span_count) override;

  int fd() const noexcept { return fd_; }
  std::uint64_t bytes_written() const noexcept { return bytes_written_; }

 private:
  int fd_;
  std::uint64_t bytes_written_{0};
};

/// Reads length-prefixed frames off a socket into a reassembly buffer.
///
/// TCP is a byte stream, not a message stream: one `read` may return half a frame or
/// three and a half. Anything that treats a read boundary as a message boundary works
/// perfectly in testing and corrupts under load, so framing is reassembled explicitly.
class FrameReader {
 public:
  explicit FrameReader(std::size_t capacity = 1u << 16) { buffer_.reserve(capacity); }

  /// Pull whatever is available. Returns bytes read, 0 if the socket would block,
  /// or -1 if the peer closed or errored.
  long fill(int fd);

  /// Hand back the next complete frame, or false if one is still arriving.
  /// The pointer is valid until the next `fill` or `next_frame` call.
  bool next_frame(std::uint8_t& type, const unsigned char*& payload,
                  std::uint16_t& payload_bytes);

  std::size_t buffered() const noexcept { return buffer_.size() - read_offset_; }

 private:
  std::vector<unsigned char> buffer_;
  std::size_t read_offset_{0};
};

}  // namespace cascade::net
