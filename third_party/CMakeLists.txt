set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
set(CMAKE_DEBUG_POSTFIX "" CACHE STRING "disable postfix in debug libraries")

include(cityhash.cmake)

add_subdirectory(gflags)
add_subdirectory(glog)

set(FMT_INSTALL ON CACHE BOOL "fmt: generate install target" FORCE)
add_subdirectory(fmt)

include_directories(
  ${Boost_INCLUDE_DIRS}
  ${CMAKE_CURRENT_SOURCE_DIR}/CRoaring/cpp
)

set(ROARING_BUILD_STATIC ON CACHE BOOL "croaring: build static library" FORCE)
add_subdirectory(CRoaring)

install(FILES
  CRoaring/cpp/roaring.hh
  CRoaring/cpp/roaring64map.hh
  DESTINATION include)

install(FILES
  json/include/nlohmann/json_fwd.hpp
  DESTINATION include/nlohmann)

add_subdirectory(Simple-Web-Server EXCLUDE_FROM_ALL)
add_subdirectory(googletest EXCLUDE_FROM_ALL)

set(BUILD_CPR_TESTS OFF CACHE BOOL "cpr: disable tests" FORCE)
set(BUILD_TESTING OFF CACHE BOOL "cpr: disable tests build" FORCE)
set(INSECURE_CURL ON CACHE BOOL "cpr: use insecure curl" FORCE)
add_subdirectory(cpr)

include(librdkafka.cmake)

set(CPPKAFKA_BUILD_SHARED OFF CACHE BOOL "cppkafka: build static library" FORCE)
add_subdirectory(cppkafka)

set(BUILD_TESTING OFF CACHE BOOL "libevent: disable tests build")
set(EVENT__DISABLE_BENCHMARK ON CACHE BOOL "libevent: disable benchmark")
set(EVENT__DISABLE_OPENSSL ON CACHE BOOL "libevent: disable openssl")
set(EVENT__DISABLE_REGRESS ON CACHE BOOL "libevent: disable regression tests")
set(EVENT__DISABLE_SAMPLES ON CACHE BOOL "libevent: disable samples")
set(EVENT__DISABLE_TESTS ON CACHE BOOL "libevent: disable tests")
set(EVENT__DISABLE_THREAD_SUPPORT ON CACHE BOOL "libevent: disable threads")
add_subdirectory(libevent)

install(FILES
  subprocess.hpp
  DESTINATION include)
