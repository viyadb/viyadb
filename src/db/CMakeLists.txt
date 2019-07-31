configure_file("defs.h.in" "defs.h")

file(GLOB_RECURSE SOURCES *.cc)
add_library(viya_db ${SOURCES})

target_link_libraries(viya_db
  coverage_config
  viya_query
  viya_codegen)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} DESTINATION include/viyadb FILES_MATCHING PATTERN "*.h")
