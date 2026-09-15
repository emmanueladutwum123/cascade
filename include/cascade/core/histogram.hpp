// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace cascade {

/// A log-linear latency histogram in the style of HdrHistogram.
///
/// Why not just keep a running mean and max? Because for a ticker plant the mean is
/// close to meaningless — the number that decides whether a subscriber sees a stale
/// book is p99.9, and a mean hides it completely. And why not keep every sample?
/// Because recording 50M samples/run would itself perturb what we are measuring.
///
/// The structure buckets values by exponent (a "bucket", found with a single count-
/// leading-zeros instruction) and then linearly within that exponent (a "sub-bucket"),
/// which gives constant *relative* error: three significant digits everywhere from
/// 1ns to an hour, in a fixed ~40KB of counters. Recording is branch-light and
/// allocation-free, so it is safe to call on the hot path.
class Histogram {
 public:
  /// `significant_digits` in [1,5] fixes the relative precision of every reported value.
  explicit Histogram(std::uint64_t highest_trackable = 3'600'000'000'000ull,
                     int significant_digits = 3);

  /// Record one sample. Values above `highest_trackable` are clamped into the top
  /// bucket and counted in `overflow_count()` rather than silently dropped, so an
  /// unexpected multi-second stall can never masquerade as a clean run.
  void record(std::uint64_t value) noexcept;

  void reset() noexcept;

  /// Fold another histogram into this one. Per-thread histograms are merged only at
  /// the end of a run, so the hot path never touches a shared cache line.
  void merge(const Histogram& other);

  std::uint64_t count() const noexcept { return total_count_; }
  std::uint64_t overflow_count() const noexcept { return overflow_count_; }
  std::uint64_t min() const noexcept { return total_count_ ? min_ : 0; }
  std::uint64_t max() const noexcept { return max_; }
  double mean() const noexcept;
  double stddev() const noexcept;

  /// Value at the given percentile, e.g. `percentile(99.9)`. Reported as the highest
  /// value equivalent to the bucket, which is the conservative choice: it never
  /// understates latency.
  std::uint64_t percentile(double p) const noexcept;

  /// Multi-line summary with the percentiles that actually matter operationally.
  std::string summary(const char* label, const char* unit = "ns") const;

  /// Percentile distribution suitable for pasting into a plot or a report.
  std::string distribution(const char* unit = "ns") const;

 private:
  std::size_t counts_index_for(std::uint64_t value) const noexcept;
  std::uint64_t value_at_index(std::size_t index) const noexcept;
  std::uint64_t highest_equivalent_value(std::uint64_t value) const noexcept;

  int sub_bucket_half_count_magnitude_{};
  int unit_magnitude_{0};
  std::int32_t sub_bucket_count_{};
  std::int32_t sub_bucket_half_count_{};
  std::uint64_t sub_bucket_mask_{};
  int leading_zero_count_base_{};
  std::int32_t bucket_count_{};

  std::uint64_t highest_trackable_{};
  std::uint64_t total_count_{0};
  std::uint64_t overflow_count_{0};
  std::uint64_t min_{UINT64_MAX};
  std::uint64_t max_{0};
  std::vector<std::uint64_t> counts_;
};

}  // namespace cascade
