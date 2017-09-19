set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

include(cityhash.cmake)

add_subdirectory(gflags)
add_subdirectory(glog)
add_subdirectory(fmt)

include_directories(
  ${Boost_INCLUDE_DIRS}
  ${CMAKE_CURRENT_SOURCE_DIR}/CRoaring/cpp
)

set(BUILD_STATIC ON CACHE BOOL "enable static CRoaring library" FORCE)
add_subdirectory(CRoaring)

install(FILES
  CRoaring/cpp/roaring.hh
  CRoaring/cpp/roaring64map.hh
  json/src/json.hpp
  DESTINATION include)

install(DIRECTORY fmt/fmt
  DESTINATION include
  FILES_MATCHING PATTERN "*.h" PATTERN "format.cc")

add_subdirectory(Simple-Web-Server EXCLUDE_FROM_ALL)
add_subdirectory(googletest EXCLUDE_FROM_ALL)

set(BUILD_CPR_TESTS OFF CACHE BOOL "disable cpr tests" FORCE)
set(INSECURE_CURL ON CACHE BOOL "use insecure curl" FORCE)
add_subdirectory(cpr)
