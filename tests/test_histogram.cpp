// SPDX-License-Identifier: Apache-2.0
#include "cascade/core/histogram.hpp"

#include <cmath>
#include <cstdint>
#include <random>

#include "test_harness.hpp"

using cascade::Histogram;

namespace {
// The histogram trades exactness for a bounded footprint, so every assertion here is
// against its advertised relative precision rather than against an exact value.
bool within_relative(std::uint64_t actual, std::uint64_t expected, double tolerance) {
  if (expected == 0) return actual == 0;
  const double delta = std::fabs(static_cast<double>(actual) - static_cast<double>(expected));
  return delta / static_cast<double>(expected) <= tolerance;
}
}  // namespace

TEST(empty_histogram_reports_zeros) {
  Histogram h;
  CHECK_EQ(h.count(), std::uint64_t{0});
  CHECK_EQ(h.min(), std::uint64_t{0});
  CHECK_EQ(h.max(), std::uint64_t{0});
  CHECK_EQ(h.percentile(50), std::uint64_t{0});
  CHECK_EQ(h.mean(), 0.0);
}

TEST(uniform_distribution_percentiles_are_accurate) {
  Histogram h;
  for (std::uint64_t v = 1; v <= 100'000; ++v) h.record(v);

  CHECK_EQ(h.count(), std::uint64_t{100'000});
  CHECK_EQ(h.min(), std::uint64_t{1});
  CHECK_EQ(h.max(), std::uint64_t{100'000});
  CHECK(within_relative(h.percentile(50), 50'000, 0.01));
  CHECK(within_relative(h.percentile(90), 90'000, 0.01));
  CHECK(within_relative(h.percentile(99), 99'000, 0.01));
  CHECK(within_relative(h.percentile(99.9), 99'900, 0.01));
  CHECK(within_relative(static_cast<std::uint64_t>(h.mean()), 50'000, 0.01));
}

TEST(percentiles_are_never_optimistic) {
  // A reported percentile must be >= the true value: operationally it is far worse to
  // understate tail latency than to overstate it by a fraction of a percent.
  Histogram h;
  for (std::uint64_t v = 1; v <= 10'000; ++v) h.record(v);
  CHECK_GE(h.percentile(50), std::uint64_t{5'000});
  CHECK_GE(h.percentile(99), std::uint64_t{9'900});
  CHECK_GE(h.percentile(100), std::uint64_t{10'000});
}

TEST(percentiles_are_monotonic) {
  Histogram h;
  std::mt19937_64 rng(12345);
  std::lognormal_distribution<double> dist(6.0, 1.2);
  for (int i = 0; i < 200'000; ++i) {
    h.record(static_cast<std::uint64_t>(dist(rng)) + 1);
  }
  std::uint64_t previous = 0;
  for (double p : {0.0, 25.0, 50.0, 75.0, 90.0, 99.0, 99.9, 99.99, 100.0}) {
    const std::uint64_t value = h.percentile(p);
    CHECK_GE(value, previous);
    previous = value;
  }
}

TEST(tail_is_resolved_not_averaged_away) {
  // The whole reason for this structure: 1% of samples at 1ms must still be visible at
  // p99 even though 99% of samples sit at 1us. A mean would bury it entirely.
  Histogram h;
  for (int i = 0; i < 99'000; ++i) h.record(1'000);
  for (int i = 0; i < 1'000; ++i) h.record(1'000'000);

  CHECK(within_relative(h.percentile(50), 1'000, 0.01));
  CHECK(within_relative(h.percentile(98), 1'000, 0.01));
  CHECK(within_relative(h.percentile(99.5), 1'000'000, 0.01));
  CHECK_EQ(h.max(), std::uint64_t{1'000'000});
}

TEST(records_span_the_full_dynamic_range) {
  Histogram h;
  const std::uint64_t values[] = {1, 10, 1'000, 1'000'000, 1'000'000'000, 3'599'000'000'000ull};
  for (std::uint64_t v : values) h.record(v);
  CHECK_EQ(h.count(), std::uint64_t{6});
  CHECK_EQ(h.min(), std::uint64_t{1});
  CHECK_GE(h.max(), std::uint64_t{3'599'000'000'000ull});
  CHECK_EQ(h.overflow_count(), std::uint64_t{0});
}

TEST(out_of_range_values_are_counted_not_dropped) {
  Histogram h(1'000'000, 3);
  h.record(500);
  h.record(5'000'000);  // beyond the trackable range
  CHECK_EQ(h.count(), std::uint64_t{2});
  CHECK_EQ(h.overflow_count(), std::uint64_t{1});
  CHECK_GE(h.max(), std::uint64_t{1'000'000});
}

TEST(merge_combines_per_thread_histograms) {
  Histogram a, b;
  for (std::uint64_t v = 1; v <= 50'000; ++v) a.record(v);
  for (std::uint64_t v = 50'001; v <= 100'000; ++v) b.record(v);

  a.merge(b);
  CHECK_EQ(a.count(), std::uint64_t{100'000});
  CHECK_EQ(a.min(), std::uint64_t{1});
  CHECK_EQ(a.max(), std::uint64_t{100'000});
  CHECK(within_relative(a.percentile(50), 50'000, 0.01));
  CHECK(within_relative(a.percentile(99), 99'000, 0.01));
}

TEST(reset_clears_all_state) {
  Histogram h;
  for (std::uint64_t v = 1; v <= 1'000; ++v) h.record(v);
  h.reset();
  CHECK_EQ(h.count(), std::uint64_t{0});
  CHECK_EQ(h.max(), std::uint64_t{0});
  CHECK_EQ(h.percentile(99), std::uint64_t{0});
}

TEST(single_sample_reports_that_sample_everywhere) {
  Histogram h;
  h.record(4'242);
  CHECK_EQ(h.count(), std::uint64_t{1});
  CHECK(within_relative(h.percentile(0), 4'242, 0.01));
  CHECK(within_relative(h.percentile(50), 4'242, 0.01));
  CHECK(within_relative(h.percentile(100), 4'242, 0.01));
}

TEST(summary_and_distribution_render) {
  Histogram h;
  for (std::uint64_t v = 1; v <= 1'000; ++v) h.record(v);
  const std::string summary = h.summary("wire-to-wire");
  CHECK(summary.find("wire-to-wire") != std::string::npos);
  CHECK(summary.find("p99.9") != std::string::npos);
  CHECK(h.distribution().find("percentile") != std::string::npos);
}
