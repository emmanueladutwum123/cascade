# A deliberately tiny assertion harness rather than a vendored framework: the project
# has zero third-party dependencies, which keeps the build reproducible anywhere a
# C++20 compiler exists and keeps CI honest about what it is actually compiling.
add_library(cascade_test_main STATIC test_main.cpp)
target_link_libraries(cascade_test_main PUBLIC cascade PRIVATE cascade_flags)

function(cascade_add_test name)
  add_executable(${name} ${name}.cpp)
  # cascade_test_main links cascade PUBLIC, so it arrives transitively.
  target_link_libraries(${name} PRIVATE cascade_test_main cascade_flags)
  add_test(NAME ${name} COMMAND ${name})
  set_tests_properties(${name} PROPERTIES TIMEOUT 120)
endfunction()

cascade_add_test(test_spsc_ring)
cascade_add_test(test_seqlock)
cascade_add_test(test_histogram)
