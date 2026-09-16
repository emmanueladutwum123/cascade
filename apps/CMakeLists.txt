function(cascade_add_app name)
  add_executable(${name} ${name}/main.cpp)
  target_link_libraries(${name} PRIVATE cascade cascade_flags)
endfunction()

cascade_add_app(cascade-bench)
cascade_add_app(cascade-feedsim)
cascade_add_app(cascaded)
cascade_add_app(cascade-sub)
