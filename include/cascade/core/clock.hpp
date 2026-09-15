// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <ctime>

#include "cascade/core/platform.hpp"

#if defined(__aarch64__)
#  include <mach/mach_time.h>
#endif

namespace cascade {

/// Nanoseconds since the UNIX epoch. This is the timestamp that goes on the wire and
/// into the tick archive: it is comparable across machines, which matters because a
/// feed handler and a subscriber are not the same process.
CASCADE_ALWAYS_INLINE std::uint64_t wall_nanos() noexcept {
  struct timespec ts;
  ::clock_gettime(CLOCK_REALTIME, &ts);
  return static_cast<std::uint64_t>(ts.tv_sec) * 1'000'000'000ull +
         static_cast<std::uint64_t>(ts.tv_nsec);
}

/// Monotonic nanoseconds. Used for every latency measurement, because CLOCK_REALTIME
/// can step backwards under NTP and would silently produce negative durations.
CASCADE_ALWAYS_INLINE std::uint64_t mono_nanos() noexcept {
  struct timespec ts;
  ::clock_gettime(CLOCK_MONOTONIC, &ts);
  return static_cast<std::uint64_t>(ts.tv_sec) * 1'000'000'000ull +
         static_cast<std::uint64_t>(ts.tv_nsec);
}

/// Raw cycle/tick counter, for measuring intervals far too short for clock_gettime to
/// resolve honestly (a ring push is a handful of nanoseconds; clock_gettime itself
/// costs ~20-25ns even via the vDSO, so timing a push with it measures the clock).
/// The unit is platform-defined; convert with `ticks_to_nanos`.
CASCADE_ALWAYS_INLINE std::uint64_t cpu_ticks() noexcept {
#if defined(__aarch64__)
  std::uint64_t value;
  // CNTVCT_EL0 is the virtual counter: fixed-frequency, unaffected by DVFS, and
  // readable from userspace on Apple Silicon.
  asm volatile("mrs %0, cntvct_el0" : "=r"(value));
  return value;
#elif defined(__x86_64__)
  std::uint32_t lo, hi;
  asm volatile("rdtsc" : "=a"(lo), "=d"(hi));
  return (static_cast<std::uint64_t>(hi) << 32) | lo;
#else
  return mono_nanos();
#endif
}

/// Frequency of `cpu_ticks()` in Hz, measured once on first use.
double tick_frequency_hz() noexcept;

/// Convert a `cpu_ticks()` delta to nanoseconds.
CASCADE_ALWAYS_INLINE double ticks_to_nanos(std::uint64_t ticks) noexcept {
  return static_cast<double>(ticks) * (1e9 / tick_frequency_hz());
}

}  // namespace cascade
