file(GLOB_RECURSE SOURCES *.cc)
add_library(viya_cluster ${SOURCES})

find_package(RdKafka REQUIRED)

include_directories(${CPR_INCLUDE_DIRS}
  ${RDKAFKA_INCLUDE_DIR})

target_link_libraries(viya_cluster
  glog
  ${CPR_LIBRARIES}
  ${RDKAFKA_LIBRARY}
  cppkafka)
