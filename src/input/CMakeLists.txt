file(GLOB_RECURSE SOURCES *.cc)
add_library(viya_input ${SOURCES})

target_link_libraries(viya_input
  glog)

