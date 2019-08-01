file(GLOB_RECURSE SOURCES *.cc)
add_library(viya_query ${SOURCES})

target_link_libraries(viya_query
  coverage_config
  viya_codegen)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} DESTINATION include/viyadb FILES_MATCHING PATTERN "*.h")
