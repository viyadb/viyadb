project(viya)
cmake_minimum_required(VERSION 3.2)

file(STRINGS ${CMAKE_CURRENT_SOURCE_DIR}/VERSION VIYA_VERSION)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  set(CXX_COMPILER_IS_CLANG ON)
endif()

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CXX_FLAGS -m64)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG")

set(Boost_USE_STATIC_LIBS ON)
find_package(Boost 1.64.0 REQUIRED COMPONENTS system filesystem iostreams)

find_package(Git)
execute_process(COMMAND
  "${GIT_EXECUTABLE}" describe --match=NeVeRmAtCh --always --abbrev=40
  WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
  OUTPUT_VARIABLE GIT_SHA1
  ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE)

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/third_party/cmake)

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}/third_party
  ${CMAKE_CURRENT_SOURCE_DIR}/third_party/fmt
  ${CMAKE_CURRENT_SOURCE_DIR}/third_party/json/src
  ${CMAKE_CURRENT_SOURCE_DIR}/third_party/Simple-Web-Server
  ${CMAKE_CURRENT_SOURCE_DIR}/third_party/CRoaring/cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/third_party/CRoaring/include
  ${CMAKE_CURRENT_SOURCE_DIR}/third_party/cppkafka/include
  ${CMAKE_CURRENT_SOURCE_DIR}/src)

add_subdirectory(third_party)
add_subdirectory(src)
add_subdirectory(test)

