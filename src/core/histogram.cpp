// SPDX-License-Identifier: Apache-2.0
#include "cascade/core/histogram.hpp"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <stdexcept>

namespace cascade {
namespace {

int ceil_log2(std::uint64_t value) noexcept {
  int magnitude = 0;
  while ((1ull << magnitude) < value) ++magnitude;
  return magnitude;
}

std::string format_u64(std::uint64_t v) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "%llu", static_cast<unsigned long long>(v));
  return buf;
}

}  // namespace

Histogram::Histogram(std::uint64_t highest_trackable, int significant_digits)
    : highest_trackable_(highest_trackable) {
  if (significant_digits < 1 || significant_digits > 5) {
    throw std::invalid_argument("significant_digits must be in [1,5]");
  }
  if (highest_trackable < 2) {
    throw std::invalid_argument("highest_trackable must be at least 2");
  }

  std::uint64_t largest_single_unit = 2;
  for (int i = 0; i < significant_digits; ++i) largest_single_unit *= 10;

  const int sub_bucket_count_magnitude = ceil_log2(largest_single_unit);
  sub_bucket_half_count_magnitude_ = sub_bucket_count_magnitude - 1;
  sub_bucket_count_ = static_cast<std::int32_t>(1u << sub_bucket_count_magnitude);
  sub_bucket_half_count_ = sub_bucket_count_ / 2;
  sub_bucket_mask_ = static_cast<std::uint64_t>(sub_bucket_count_ - 1) << unit_magnitude_;
  leading_zero_count_base_ = 64 - unit_magnitude_ - sub_bucket_count_magnitude;

  // Add exponent buckets until the top of the range is representable.
  std::int32_t buckets = 1;
  std::uint64_t smallest_untrackable =
      static_cast<std::uint64_t>(sub_bucket_count_) << unit_magnitude_;
  while (smallest_untrackable <= highest_trackable) {
    if (smallest_untrackable > (UINT64_MAX / 2)) { ++buckets; break; }
    smallest_untrackable <<= 1;
    ++buckets;
  }
  bucket_count_ = buckets;

  counts_.assign(static_cast<std::size_t>((bucket_count_ + 1) * sub_bucket_half_count_), 0);
}

std::size_t Histogram::counts_index_for(std::uint64_t value) const noexcept {
  // Exponent selection in one instruction: OR in the sub-bucket mask so that small
  // values still land in bucket 0 rather than producing a negative index.
  const int bucket_index =
      leading_zero_count_base_ - __builtin_clzll(value | sub_bucket_mask_);
  const int sub_bucket_index =
      static_cast<int>(value >> (bucket_index + unit_magnitude_));

  const int bucket_base = (bucket_index + 1) << sub_bucket_half_count_magnitude_;
  const int offset = sub_bucket_index - sub_bucket_half_count_;
  return static_cast<std::size_t>(bucket_base + offset);
}

std::uint64_t Histogram::value_at_index(std::size_t index) const noexcept {
  int bucket_index =
      static_cast<int>(index >> sub_bucket_half_count_magnitude_) - 1;
  int sub_bucket_index =
      static_cast<int>(index & static_cast<std::size_t>(sub_bucket_half_count_ - 1)) +
      sub_bucket_half_count_;
  if (bucket_index < 0) {
    sub_bucket_index -= sub_bucket_half_count_;
    bucket_index = 0;
  }
  return static_cast<std::uint64_t>(sub_bucket_index)
         << (bucket_index + unit_magnitude_);
}

std::uint64_t Histogram::highest_equivalent_value(std::uint64_t value) const noexcept {
  // Every value in a sub-bucket is indistinguishable to the histogram. Reporting the
  // top of that range means a percentile is never optimistic.
  const int bucket_index =
      leading_zero_count_base_ - __builtin_clzll(value | sub_bucket_mask_);
  const std::uint64_t range_size =
      1ull << (unit_magnitude_ + bucket_index);
  const std::uint64_t lowest = value & ~(range_size - 1);
  return lowest + range_size - 1;
}

void Histogram::record(std::uint64_t value) noexcept {
  if (value > highest_trackable_) {
    ++overflow_count_;
    value = highest_trackable_;
  }
  const std::size_t index = counts_index_for(value);
  if (index >= counts_.size()) {  // defensive: cannot happen given the clamp above
    ++overflow_count_;
    return;
  }
  ++counts_[index];
  ++total_count_;
  if (value < min_) min_ = value;
  if (value > max_) max_ = value;
}

void Histogram::reset() noexcept {
  std::fill(counts_.begin(), counts_.end(), 0ull);
  total_count_ = 0;
  overflow_count_ = 0;
  min_ = UINT64_MAX;
  max_ = 0;
}

void Histogram::merge(const Histogram& other) {
  if (other.counts_.size() != counts_.size()) {
    // Different geometry: fall back to re-recording at bucket resolution.
    for (std::size_t i = 0; i < other.counts_.size(); ++i) {
      const std::uint64_t n = other.counts_[i];
      if (!n) continue;
      const std::uint64_t value = other.value_at_index(i);
      for (std::uint64_t k = 0; k < n; ++k) record(value);
    }
    overflow_count_ += other.overflow_count_;
    return;
  }
  for (std::size_t i = 0; i < counts_.size(); ++i) counts_[i] += other.counts_[i];
  total_count_ += other.total_count_;
  overflow_count_ += other.overflow_count_;
  min_ = std::min(min_, other.min_);
  max_ = std::max(max_, other.max_);
}

double Histogram::mean() const noexcept {
  if (total_count_ == 0) return 0.0;
  double weighted = 0.0;
  for (std::size_t i = 0; i < counts_.size(); ++i) {
    if (counts_[i]) weighted += static_cast<double>(counts_[i]) *
                                static_cast<double>(value_at_index(i));
  }
  return weighted / static_cast<double>(total_count_);
}

double Histogram::stddev() const noexcept {
  if (total_count_ < 2) return 0.0;
  const double mu = mean();
  double sum_sq = 0.0;
  for (std::size_t i = 0; i < counts_.size(); ++i) {
    if (!counts_[i]) continue;
    const double delta = static_cast<double>(value_at_index(i)) - mu;
    sum_sq += delta * delta * static_cast<double>(counts_[i]);
  }
  return std::sqrt(sum_sq / static_cast<double>(total_count_));
}

std::uint64_t Histogram::percentile(double p) const noexcept {
  if (total_count_ == 0) return 0;
  if (p < 0.0) p = 0.0;
  if (p > 100.0) p = 100.0;

  // Round up so that e.g. p50 of two samples is the second, not the first.
  std::uint64_t target =
      static_cast<std::uint64_t>((p / 100.0) * static_cast<double>(total_count_) + 0.5);
  if (target == 0) target = 1;
  if (target > total_count_) target = total_count_;

  std::uint64_t seen = 0;
  for (std::size_t i = 0; i < counts_.size(); ++i) {
    seen += counts_[i];
    if (seen >= target) return highest_equivalent_value(value_at_index(i));
  }
  return max_;
}

std::string Histogram::summary(const char* label, const char* unit) const {
  char buf[512];
  std::snprintf(
      buf, sizeof(buf),
      "%-22s n=%-11s min=%-9s p50=%-9s p90=%-9s p99=%-9s p99.9=%-9s p99.99=%-9s max=%s %s",
      label, format_u64(total_count_).c_str(), format_u64(min()).c_str(),
      format_u64(percentile(50)).c_str(), format_u64(percentile(90)).c_str(),
      format_u64(percentile(99)).c_str(), format_u64(percentile(99.9)).c_str(),
      format_u64(percentile(99.99)).c_str(), format_u64(max_).c_str(), unit);
  std::string out(buf);
  if (overflow_count_) {
    out += "  [" + format_u64(overflow_count_) + " samples clamped at range top]";
  }
  return out;
}

std::string Histogram::distribution(const char* unit) const {
  static const double kPercentiles[] = {0,  10, 25, 50,   75,    90,    95,
                                        99, 99.9, 99.99, 99.999, 100};
  std::string out = "  percentile      value(";
  out += unit;
  out += ")\n";
  char buf[128];
  for (double p : kPercentiles) {
    std::snprintf(buf, sizeof(buf), "  %-14.5g  %llu\n", p,
                  static_cast<unsigned long long>(percentile(p)));
    out += buf;
  }
  return out;
}

}  // namespace cascade
