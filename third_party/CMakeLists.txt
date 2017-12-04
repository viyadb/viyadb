set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
set(CMAKE_DEBUG_POSTFIX "" CACHE STRING "Generate debug library name without a postfix.")

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
set(BUILD_TESTING OFF CACHE BOOL "disable url tests" FORCE)
set(INSECURE_CURL ON CACHE BOOL "use insecure curl" FORCE)
add_subdirectory(cpr)

#set(BUILD_ONLY "s3" CACHE STRING "AWS SDK enable modules: s3" FORCE)
#set(CPP_STANDARD "17" CACHE STRING "AWS SDK use C++ standard: 17" FORCE)
#set(ENABLE_TESTING OFF CACHE BOOL "AWS SDK disable testing" FORCE)
#set(AUTORUN_UNIT_TESTS OFF CACHE BOOL "AWS SDK disable autorun tests" FORCE)
#set(MINIMIZE_SIZE OFF CACHE BOOL "AWS SDK minimize size" FORCE)
#set(BUILD_SHARED_LIBS OFF CACHE BOOL "AWS SDK disable shared libraries build" FORCE)
#list(APPEND AWS_COMPILER_WARNINGS "-Wno-noexcept-type")
#add_subdirectory(aws-sdk-cpp)

include(librdkafka.cmake)

set(CPPKAFKA_BUILD_SHARED OFF CACHE BOOL "build static cppkafka" FORCE)
add_subdirectory(cppkafka)

