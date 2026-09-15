add_library(cascade
  core/clock.cpp
  core/histogram.cpp
)
add_library(cascade::cascade ALIAS cascade)

target_include_directories(cascade PUBLIC
  $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
)
target_link_libraries(cascade
  PUBLIC  Threads::Threads
  PRIVATE cascade_flags
)
