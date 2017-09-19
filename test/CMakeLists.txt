enable_testing()

include_directories(${TEST_SOURCE_DIR}/src
  ${gtest_SOURCE_DIR}/include
  ${gtest_SOURCE_DIR}
  ${Boost_INCLUDE_DIRS}
)

file(GLOB_RECURSE SOURCES *.cc)
add_executable(unit_tests ${SOURCES})

target_link_libraries(unit_tests
  viya_db
  viya_input
  viya_query
  viya_codegen
  viya_util
  gtest
  glog
)

add_test(test unit_tests)
