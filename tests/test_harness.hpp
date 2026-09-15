// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

namespace cascade::test {

struct Case {
  const char* name;
  void (*fn)();
};

std::vector<Case>& registry();
void fail(const char* file, int line, const std::string& message);
int run_all(const char* suite);

struct Registrar {
  Registrar(const char* name, void (*fn)()) { registry().push_back({name, fn}); }
};

}  // namespace cascade::test

#define TEST(name)                                                              \
  static void name();                                                           \
  static ::cascade::test::Registrar name##_registrar(#name, &name);             \
  static void name()

#define CHECK(cond)                                                             \
  do {                                                                          \
    if (!(cond)) ::cascade::test::fail(__FILE__, __LINE__, "CHECK(" #cond ")"); \
  } while (0)

#define CHECK_EQ(a, b)                                                          \
  do {                                                                          \
    const auto _lhs = (a);                                                      \
    const auto _rhs = (b);                                                      \
    if (!(_lhs == _rhs)) {                                                      \
      ::cascade::test::fail(__FILE__, __LINE__,                                 \
                            std::string("CHECK_EQ(" #a ", " #b ")  lhs=") +     \
                                std::to_string(_lhs) + " rhs=" +                \
                                std::to_string(_rhs));                          \
    }                                                                           \
  } while (0)

#define CHECK_LE(a, b)                                                          \
  do {                                                                          \
    const auto _lhs = (a);                                                      \
    const auto _rhs = (b);                                                      \
    if (!(_lhs <= _rhs)) {                                                      \
      ::cascade::test::fail(__FILE__, __LINE__,                                 \
                            std::string("CHECK_LE(" #a ", " #b ")  lhs=") +     \
                                std::to_string(_lhs) + " rhs=" +                \
                                std::to_string(_rhs));                          \
    }                                                                           \
  } while (0)

#define CHECK_GE(a, b)                                                          \
  do {                                                                          \
    const auto _lhs = (a);                                                      \
    const auto _rhs = (b);                                                      \
    if (!(_lhs >= _rhs)) {                                                      \
      ::cascade::test::fail(__FILE__, __LINE__,                                 \
                            std::string("CHECK_GE(" #a ", " #b ")  lhs=") +     \
                                std::to_string(_lhs) + " rhs=" +                \
                                std::to_string(_rhs));                          \
    }                                                                           \
  } while (0)
