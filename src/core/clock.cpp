// SPDX-License-Identifier: Apache-2.0
#include "cascade/core/clock.hpp"

#include <atomic>

#if defined(__aarch64__)
#  include <sys/sysctl.h>
#  include <sys/types.h>
#endif

namespace cascade {
namespace {

double measure_tick_frequency() noexcept {
#if defined(__aarch64__)
  // On Apple Silicon the virtual counter frequency is exposed directly; reading it
  // beats calibrating against a wall clock, which would inherit that clock's jitter.
  std::uint64_t hz = 0;
  std::size_t size = sizeof(hz);
  if (::sysctlbyname("hw.tbfrequency", &hz, &size, nullptr, 0) == 0 && hz > 0) {
    // hw.tbfrequency reports the timebase in Hz on arm64 Macs (24MHz on M-series).
    return static_cast<double>(hz);
  }
#endif
  // Portable fallback: calibrate the tick counter against CLOCK_MONOTONIC. 20ms is
  // long enough that clock_gettime's own overhead is noise, short enough not to be felt.
  const std::uint64_t t0 = mono_nanos();
  const std::uint64_t c0 = cpu_ticks();
  struct timespec sleep_for { 0, 20'000'000 };
  ::nanosleep(&sleep_for, nullptr);
  const std::uint64_t c1 = cpu_ticks();
  const std::uint64_t t1 = mono_nanos();

  const double elapsed_ns = static_cast<double>(t1 - t0);
  if (elapsed_ns <= 0.0) return 1e9;
  return static_cast<double>(c1 - c0) * 1e9 / elapsed_ns;
}

}  // namespace

double tick_frequency_hz() noexcept {
  // Relaxed double-checked init: the value is idempotent, so a benign duplicate
  // measurement during a startup race is harmless and cheaper than a call_once.
  static std::atomic<double> cached{0.0};
  double value = cached.load(std::memory_order_relaxed);
  if (value == 0.0) {
    value = measure_tick_frequency();
    cached.store(value, std::memory_order_relaxed);
  }
  return value;
}

}  // namespace cascade
