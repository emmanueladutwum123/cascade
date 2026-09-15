cmake_minimum_required(VERSION 3.20)
project(cascade
        VERSION 0.1.0
        DESCRIPTION "A low-latency market-data ticker plant with a conflating fan-out tier"
        LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "" FORCE)
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------
option(CASCADE_BUILD_TESTS "Build the test suite" ON)
option(CASCADE_BUILD_APPS  "Build the daemons and CLI tools" ON)
option(CASCADE_BUILD_BENCH "Build the benchmark harness" ON)
option(CASCADE_WERROR      "Treat warnings as errors" OFF)
option(CASCADE_NATIVE      "Build for the host microarchitecture (-march=native)" OFF)

set(CASCADE_SANITIZER "" CACHE STRING "Sanitizer to enable: address, thread, undefined, or empty")
set_property(CACHE CASCADE_SANITIZER PROPERTY STRINGS "" address thread undefined)

# ---------------------------------------------------------------------------
# Warning / sanitizer interface targets
# ---------------------------------------------------------------------------
add_library(cascade_flags INTERFACE)
target_compile_options(cascade_flags INTERFACE
  -Wall -Wextra -Wpedantic
  -Wshadow -Wconversion -Wsign-conversion
  -Wnon-virtual-dtor -Wold-style-cast -Wcast-align
  -Wunused -Woverloaded-virtual -Wdouble-promotion
  -Wformat=2 -Wimplicit-fallthrough
)
if(CASCADE_WERROR)
  target_compile_options(cascade_flags INTERFACE -Werror)
endif()
if(CASCADE_NATIVE)
  include(CheckCXXCompilerFlag)
  check_cxx_compiler_flag("-march=native" CASCADE_HAS_MARCH_NATIVE)
  if(CASCADE_HAS_MARCH_NATIVE)
    target_compile_options(cascade_flags INTERFACE -march=native)
  else()
    # Apple clang on arm64 rejects -march=native; -mcpu=native is the equivalent.
    check_cxx_compiler_flag("-mcpu=native" CASCADE_HAS_MCPU_NATIVE)
    if(CASCADE_HAS_MCPU_NATIVE)
      target_compile_options(cascade_flags INTERFACE -mcpu=native)
    endif()
  endif()
endif()

if(CASCADE_SANITIZER)
  message(STATUS "cascade: enabling ${CASCADE_SANITIZER} sanitizer")
  target_compile_options(cascade_flags INTERFACE
    -fsanitize=${CASCADE_SANITIZER} -fno-omit-frame-pointer -g)
  target_link_options(cascade_flags INTERFACE -fsanitize=${CASCADE_SANITIZER})
endif()

# ---------------------------------------------------------------------------
# The library
# ---------------------------------------------------------------------------
add_subdirectory(src)

if(CASCADE_BUILD_APPS)
  add_subdirectory(apps)
endif()

if(CASCADE_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

message(STATUS "cascade ${PROJECT_VERSION}")
message(STATUS "  build type : ${CMAKE_BUILD_TYPE}")
message(STATUS "  compiler   : ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
message(STATUS "  sanitizer  : ${CASCADE_SANITIZER}")
message(STATUS "  tests      : ${CASCADE_BUILD_TESTS}")
