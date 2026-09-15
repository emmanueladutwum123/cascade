// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstddef>
#include <cstdint>
#include <new>

namespace cascade {

// libc++ 17 (Apple clang 15) does not ship std::hardware_destructive_interference_size,
// and on Apple Silicon the value the standard would report is misleading anyway: the
// L1 line is 64B but the adjacent-line prefetcher pulls pairs, so two atomics 64B apart
// still contend. 128B is the padding that actually eliminates false sharing on M-series.
#if defined(__aarch64__) || defined(_M_ARM64)
inline constexpr std::size_t kCacheLine = 128;
#else
inline constexpr std::size_t kCacheLine = 64;
#endif

#if defined(__clang__) || defined(__GNUC__)
#  define CASCADE_ALWAYS_INLINE inline __attribute__((always_inline))
#  define CASCADE_NOINLINE __attribute__((noinline))
#  define CASCADE_LIKELY(x) __builtin_expect(!!(x), 1)
#  define CASCADE_UNLIKELY(x) __builtin_expect(!!(x), 0)
#  define CASCADE_HOT __attribute__((hot))
#  define CASCADE_PACKED __attribute__((packed))
#else
#  define CASCADE_ALWAYS_INLINE inline
#  define CASCADE_NOINLINE
#  define CASCADE_LIKELY(x) (x)
#  define CASCADE_UNLIKELY(x) (x)
#  define CASCADE_HOT
#  define CASCADE_PACKED
#endif

// Detect ThreadSanitizer. The seqlock's racy-read fast path is UB by the letter of the
// standard (it is benign in practice and universally used), so under TSan we compile a
// mutex-backed equivalent instead of teaching the tool to ignore a real race.
#if defined(__has_feature)
#  if __has_feature(thread_sanitizer)
#    define CASCADE_THREAD_SANITIZER 1
#  endif
#endif
#if defined(__SANITIZE_THREAD__)
#  define CASCADE_THREAD_SANITIZER 1
#endif

/// Hint to the CPU that we are in a spin-wait loop, so it can de-prioritise the
/// pipeline / yield SMT resources rather than burning issue slots.
CASCADE_ALWAYS_INLINE void cpu_relax() noexcept {
#if defined(__aarch64__)
  asm volatile("isb" ::: "memory");
#elif defined(__x86_64__) || defined(__i386__)
  asm volatile("pause" ::: "memory");
#else
  // Portable fallback: a compiler barrier, so the loop re-reads its operands.
  asm volatile("" ::: "memory");
#endif
}

}  // namespace cascade
