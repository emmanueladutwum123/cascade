// SPDX-License-Identifier: Apache-2.0
#include "test_harness.hpp"

#include <exception>

namespace cascade::test {
namespace {
int g_failures = 0;
}

std::vector<Case>& registry() {
  // Function-local static: the registry must outlive, and be initialised before, the
  // static Registrar objects in every translation unit that registers into it.
  static std::vector<Case> cases;
  return cases;
}

void fail(const char* file, int line, const std::string& message) {
  std::fprintf(stderr, "    FAIL %s:%d: %s\n", file, line, message.c_str());
  ++g_failures;
}

int run_all(const char* suite) {
  std::printf("== %s (%zu cases)\n", suite, registry().size());
  int failed_cases = 0;
  for (const Case& c : registry()) {
    const int before = g_failures;
    try {
      c.fn();
    } catch (const std::exception& e) {
      fail(__FILE__, __LINE__, std::string("threw: ") + e.what());
    } catch (...) {
      fail(__FILE__, __LINE__, "threw a non-std exception");
    }
    const bool ok = (g_failures == before);
    std::printf("  [%s] %s\n", ok ? "PASS" : "FAIL", c.name);
    if (!ok) ++failed_cases;
  }
  if (failed_cases) {
    std::printf("== %s: %d/%zu cases FAILED\n", suite, failed_cases, registry().size());
    return 1;
  }
  std::printf("== %s: all %zu cases passed\n", suite, registry().size());
  return 0;
}

}  // namespace cascade::test

int main() { return cascade::test::run_all("cascade"); }
