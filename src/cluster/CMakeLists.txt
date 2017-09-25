file(GLOB_RECURSE SOURCES *.cc)
add_library(viya_cluster ${SOURCES})

include_directories(${CPR_INCLUDE_DIRS})

target_link_libraries(viya_cluster
  glog
  ${CPR_LIBRARIES})
