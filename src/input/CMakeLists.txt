file(GLOB_RECURSE SOURCES *.cc)
add_library(viya_input ${SOURCES})

target_link_libraries(viya_input
  ${Boost_SYSTEM_LIBRARY_RELEASE}
  ${Boost_FILESYSTEM_LIBRARY_RELEASE}
  glog)

