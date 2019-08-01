set(CMAKE_CXX_FLAGS "-Wall -Wextra")

include_directories(
  ${Boost_INCLUDE_DIRS}
  ${CMAKE_CURRENT_BINARY_DIR})

# Code coverage configuration (only enabled in debug mode)
add_library(coverage_config INTERFACE)
option(CODE_COVERAGE "Enable coverage reporting" OFF)
if(CODE_COVERAGE AND CMAKE_BUILD_TYPE MATCHES DEBUG)
  target_compile_options(coverage_config INTERFACE --coverage)
  target_link_options(coverage_config INTERFACE --coverage)
endif(CODE_COVERAGE AND CMAKE_BUILD_TYPE MATCHES DEBUG)

add_subdirectory(util)
add_subdirectory(db)
add_subdirectory(input)
add_subdirectory(query)
add_subdirectory(codegen)
add_subdirectory(cluster)
add_subdirectory(server)
add_subdirectory(sql)

