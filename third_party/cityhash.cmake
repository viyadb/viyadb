set(sources
  "cityhash/src/city.cc")

set(cityhash_gen "${CMAKE_BINARY_DIR}/cityhash")

configure_file("cityhash_config.h.in" "${cityhash_gen}/config.h" @ONLY)
configure_file("cityhash_byteswap.h.in" "${cityhash_gen}/byteswap.h" @ONLY)

add_library(cityhash STATIC ${sources})

target_include_directories(cityhash PUBLIC "${cityhash_gen}")
target_include_directories(cityhash PUBLIC "cityhash/src")
