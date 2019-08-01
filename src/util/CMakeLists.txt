file(GLOB_RECURSE SOURCES *.cc)
add_library(viya_util ${SOURCES})

target_link_libraries(viya_util
  coverage_config
  glog)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} DESTINATION include/viyadb FILES_MATCHING PATTERN "*.h")
install(TARGETS viya_util ARCHIVE DESTINATION lib)
