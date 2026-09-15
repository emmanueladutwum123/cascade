// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <cstring>
#include <type_traits>

#include "cascade/core/platform.hpp"

namespace cascade {

CASCADE_ALWAYS_INLINE std::uint16_t bswap16(std::uint16_t v) noexcept {
  return __builtin_bswap16(v);
}
CASCADE_ALWAYS_INLINE std::uint32_t bswap32(std::uint32_t v) noexcept {
  return __builtin_bswap32(v);
}
CASCADE_ALWAYS_INLINE std::uint64_t bswap64(std::uint64_t v) noexcept {
  return __builtin_bswap64(v);
}

#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
inline constexpr bool kHostIsBigEndian = true;
#else
inline constexpr bool kHostIsBigEndian = false;
#endif

/// Big-endian (network byte order) integer stored in its wire representation.
///
/// The upstream exchange feed is big-endian because real exchange protocols are:
/// NASDAQ ITCH, OUCH, and the MoldUDP64 framing they ride on all predate the
/// x86 monoculture. Rather than byte-swap a whole struct after receipt, each field
/// carries its own endianness in the type, so a decoder pays for exactly the fields
/// it reads and the compiler folds the swap into a single `rev`/`bswap` instruction.
///
/// Storing raw bytes (not an integer) also keeps the struct trivially copyable and
/// alignment-free, so a message can be read straight out of a UDP datagram with no
/// copy and no unaligned-access UB.
template <typename T>
struct BigEndian {
  static_assert(std::is_integral<T>::value, "BigEndian requires an integral type");
  using value_type = T;

  unsigned char bytes[sizeof(T)];

  CASCADE_ALWAYS_INLINE T value() const noexcept {
    using U = typename std::make_unsigned<T>::type;
    U raw;
    std::memcpy(&raw, bytes, sizeof(T));
    if constexpr (!kHostIsBigEndian) {
      if constexpr (sizeof(T) == 2) raw = bswap16(raw);
      else if constexpr (sizeof(T) == 4) raw = bswap32(raw);
      else if constexpr (sizeof(T) == 8) raw = bswap64(raw);
    }
    return static_cast<T>(raw);
  }

  CASCADE_ALWAYS_INLINE void set(T v) noexcept {
    using U = typename std::make_unsigned<T>::type;
    U raw = static_cast<U>(v);
    if constexpr (!kHostIsBigEndian) {
      if constexpr (sizeof(T) == 2) raw = bswap16(raw);
      else if constexpr (sizeof(T) == 4) raw = bswap32(raw);
      else if constexpr (sizeof(T) == 8) raw = bswap64(raw);
    }
    std::memcpy(bytes, &raw, sizeof(T));
  }

  CASCADE_ALWAYS_INLINE operator T() const noexcept { return value(); }
  CASCADE_ALWAYS_INLINE BigEndian& operator=(T v) noexcept { set(v); return *this; }
};

using be16 = BigEndian<std::uint16_t>;
using be32 = BigEndian<std::uint32_t>;
using be64 = BigEndian<std::uint64_t>;
using bei64 = BigEndian<std::int64_t>;

static_assert(sizeof(be16) == 2 && alignof(be16) == 1, "wire types must be byte-aligned");
static_assert(sizeof(be64) == 8 && alignof(be64) == 1, "wire types must be byte-aligned");
static_assert(std::is_trivially_copyable<be64>::value, "wire types must be memcpy-able");

}  // namespace cascade
