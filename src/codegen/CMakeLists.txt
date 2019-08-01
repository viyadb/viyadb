file(GLOB_RECURSE SOURCES *.cc)
add_library(viya_codegen ${SOURCES})

target_link_libraries(viya_codegen
  coverage_config
  viya_util
  viya_db
  cityhash
  ${Boost_SYSTEM_LIBRARY_RELEASE}
  ${Boost_FILESYSTEM_LIBRARY_RELEASE}
  -ldl
  -lpthread)

