set(CMAKE_CXX_FLAGS "-Wall -Wextra")

include_directories(
  ${Boost_INCLUDE_DIRS}
  ${CMAKE_CURRENT_BINARY_DIR})

add_subdirectory(util)
add_subdirectory(db)
add_subdirectory(input)
add_subdirectory(query)
add_subdirectory(codegen)
add_subdirectory(cluster)
add_subdirectory(server)

