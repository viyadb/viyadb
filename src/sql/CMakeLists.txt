find_package(BISON)
find_package(FLEX)

bison_target(Parser parser.y ${CMAKE_CURRENT_BINARY_DIR}/parser.cc)
flex_target(Scanner scanner.l ${CMAKE_CURRENT_BINARY_DIR}/scanner.cc)

add_flex_bison_dependency(Scanner Parser)

file(GLOB_RECURSE SOURCES *.cc)
add_library(viya_sql
  ${FLEX_Scanner_OUTPUTS}
  ${BISON_Parser_OUTPUTS}
  ${SOURCES})

