set(BUILD_WITH_INSTALL_RPATH true)

file(GLOB_RECURSE SOURCES *.cc)
add_executable(viyad ${SOURCES})

target_link_libraries(viyad
  coverage_config
  viya_db
  viya_input
  viya_query
  viya_codegen
  viya_cluster
  viya_util
  viya_sql
  glog
  pthread)

install(TARGETS viyad RUNTIME DESTINATION bin)

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath,'$ORIGIN/../lib/'")

