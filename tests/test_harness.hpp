// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdio>
#include <cstdlib>
#include <string>
#include <type_traits>
#include <vector>

namespace cascade::test {

/// Render a value for a failure message.
///
/// `std::to_string` covers only the arithmetic types, and a bare overload set would
/// make `CHECK_EQ` unusable on strings and enums -- which is exactly when a good
/// failure message matters most. These overloads keep the macros type-agnostic.
inline std::string to_text(const std::string& value) { return "\"" + value + "\""; }
inline std::string to_text(const char* value) {
  return value ? "\"" + std::string(value) + "\"" : "(null)";
}
inline std::string to_text(bool value) { return value ? "true" : "false"; }

template <typename T>
inline std::string to_text(const T& value) {
  if constexpr (std::is_enum<T>::value) {
    return std::to_string(static_cast<long long>(value));
  } else if constexpr (std::is_arithmetic<T>::value) {
    // Promote the narrow character types: to_string has no overload for them and
    // would otherwise be ambiguous.
    if constexpr (sizeof(T) == 1) return std::to_string(static_cast<long long>(value));
    else return std::to_string(value);
  } else {
    return "<unprintable>";
  }
}

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
                                ::cascade::test::to_text(_lhs) + " rhs=" +    \
                                ::cascade::test::to_text(_rhs));                          \
    }                                                                           \
  } while (0)

#define CHECK_LE(a, b)                                                          \
  do {                                                                          \
    const auto _lhs = (a);                                                      \
    const auto _rhs = (b);                                                      \
    if (!(_lhs <= _rhs)) {                                                      \
      ::cascade::test::fail(__FILE__, __LINE__,                                 \
                            std::string("CHECK_LE(" #a ", " #b ")  lhs=") +     \
                                ::cascade::test::to_text(_lhs) + " rhs=" +    \
                                ::cascade::test::to_text(_rhs));                          \
    }                                                                           \
  } while (0)

#define CHECK_GE(a, b)                                                          \
  do {                                                                          \
    const auto _lhs = (a);                                                      \
    const auto _rhs = (b);                                                      \
    if (!(_lhs >= _rhs)) {                                                      \
      ::cascade::test::fail(__FILE__, __LINE__,                                 \
                            std::string("CHECK_GE(" #a ", " #b ")  lhs=") +     \
                                ::cascade::test::to_text(_lhs) + " rhs=" +    \
                                ::cascade::test::to_text(_rhs));                          \
    }                                                                           \
  } while (0)
