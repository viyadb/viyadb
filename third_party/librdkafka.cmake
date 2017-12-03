set(RDKAFKA_ROOT_DIR ${CMAKE_CURRENT_BINARY_DIR}/librdkafka)

execute_process(COMMAND ./configure --prefix=${RDKAFKA_ROOT_DIR}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/librdkafka
)

execute_process(COMMAND make libs
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/librdkafka
)

execute_process(COMMAND make install
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/librdkafka
)

install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/librdkafka/lib/ DESTINATION lib FILES_MATCHING PATTERN "*.so*")
