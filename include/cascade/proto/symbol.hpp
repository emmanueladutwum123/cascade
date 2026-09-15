// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

#include "cascade/core/platform.hpp"

namespace cascade {

/// An instrument identifier: up to 8 ASCII characters packed into a single 64-bit word.
///
/// Every tier of the plant keys on this — the shard router hashes it, the book map
/// looks it up, the fan-out compares it per subscription — so it has to be cheap.
/// Packing to a `uint64_t` makes comparison one instruction, hashing one multiply, and
/// copying free, with no allocation and no pointer chase anywhere on the hot path.
/// Exchanges already space-pad symbols to a fixed width (ITCH uses 8), so this matches
/// the wire format rather than fighting it.
class Symbol {
 public:
  Symbol() = default;
  explicit constexpr Symbol(std::uint64_t raw) noexcept : raw_(raw) {}

  /// Pack from text. Longer input is truncated to 8 characters; shorter is space-padded,
  /// which is exactly what the exchange does, so "IBM" and "IBM     " are one instrument.
  static Symbol from_text(std::string_view text) noexcept {
    char padded[8];
    std::memset(padded, ' ', sizeof(padded));
    const std::size_t n = text.size() < 8 ? text.size() : 8;
    std::memcpy(padded, text.data(), n);
    std::uint64_t raw;
    std::memcpy(&raw, padded, sizeof(raw));
    return Symbol(raw);
  }

  /// Pack straight from an 8-byte wire field, no bounds check and no branch.
  static CASCADE_ALWAYS_INLINE Symbol from_wire(const void* eight_bytes) noexcept {
    std::uint64_t raw;
    std::memcpy(&raw, eight_bytes, sizeof(raw));
    return Symbol(raw);
  }

  void to_wire(void* eight_bytes) const noexcept {
    std::memcpy(eight_bytes, &raw_, sizeof(raw_));
  }

  /// Human-readable form with the padding stripped.
  std::string text() const {
    char buf[8];
    std::memcpy(buf, &raw_, sizeof(buf));
    std::size_t len = 8;
    while (len > 0 && (buf[len - 1] == ' ' || buf[len - 1] == '\0')) --len;
    return std::string(buf, len);
  }

  constexpr std::uint64_t raw() const noexcept { return raw_; }
  constexpr bool empty() const noexcept { return raw_ == 0; }

  friend constexpr bool operator==(Symbol a, Symbol b) noexcept { return a.raw_ == b.raw_; }
  friend constexpr bool operator!=(Symbol a, Symbol b) noexcept { return a.raw_ != b.raw_; }
  friend constexpr bool operator<(Symbol a, Symbol b) noexcept { return a.raw_ < b.raw_; }

  /// Fibonacci hashing: one multiply and a shift. The high bits of the product mix
  /// every input bit, which matters because packed ASCII symbols share long runs of
  /// identical bytes (the space padding) and would collide badly under a plain mask.
  CASCADE_ALWAYS_INLINE std::uint64_t hash() const noexcept {
    std::uint64_t x = raw_;
    x ^= x >> 33;
    x *= 0xFF51AFD7ED558CCDull;
    x ^= x >> 33;
    x *= 0xC4CEB9FE1A85EC53ull;
    x ^= x >> 33;
    return x;
  }

 private:
  std::uint64_t raw_{0};
};

struct SymbolHash {
  std::size_t operator()(Symbol s) const noexcept {
    return static_cast<std::size_t>(s.hash());
  }
};

/// Prices are fixed-point integers in units of 1e-6 ("micro-units").
///
/// Floating point is disqualified outright: a book has to answer "is this the same
/// price level?" exactly, and 0.1 + 0.2 != 0.3 makes that unanswerable. int64 at 1e-6
/// covers roughly +/- 9.2 billion units of currency with sub-tick resolution for
/// equities, FX and rates alike, and comparisons stay single-instruction.
using Price = std::int64_t;
inline constexpr std::int64_t kPriceScale = 1'000'000;
inline constexpr Price kNoPrice = INT64_MIN;

constexpr Price price_from_double(double v) noexcept {
  return static_cast<Price>(v * static_cast<double>(kPriceScale) + (v >= 0 ? 0.5 : -0.5));
}
constexpr double price_to_double(Price p) noexcept {
  return static_cast<double>(p) / static_cast<double>(kPriceScale);
}

using Quantity = std::uint32_t;
using OrderId = std::uint64_t;

enum class Side : std::uint8_t { kBuy = 'B', kSell = 'S' };

}  // namespace cascade
