file(GLOB_RECURSE SOURCES *.cc)
add_library(viya_codegen ${SOURCES})

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}/../../third_party/boost-process/include)

target_link_libraries(viya_codegen
  viya_util
  viya_db
  cityhash
  ${Boost_SYSTEM_LIBRARY_RELEASE}
  ${Boost_FILESYSTEM_LIBRARY_RELEASE}
  -ldl
  -lpthread)

