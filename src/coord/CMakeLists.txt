file(GLOB_RECURSE SOURCES *.cc)
add_library(viya_coord ${SOURCES})

include_directories(${CPR_INCLUDE_DIRS})

target_link_libraries(viya_coord
  glog
  ${CPR_LIBRARIES})
