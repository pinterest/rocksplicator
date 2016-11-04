cmake_minimum_required(VERSION 3.1)

# Build thrift file
set(GEN_FILES
    "gen-cpp2/counter_types.tcc"
    "gen-cpp2/counter_types.cpp"
    "gen-cpp2/counter_types.h"
    "gen-cpp2/counter_constants.h"
    "gen-cpp2/counter_constants.cpp"
    "gen-cpp2/Counter.tcc"
    "gen-cpp2/Counter.cpp"
    "gen-cpp2/Counter.h"
    "gen-cpp2/Counter_client.cpp")

add_custom_command(
  OUTPUT ${GEN_FILES}
  COMMAND python -mthrift_compiler.main --gen cpp2 -I ${CMAKE_CURRENT_SOURCE_DIR}/../../.. -o ./ ${CMAKE_CURRENT_SOURCE_DIR}/counter.thrift
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/counter.thrift
  DEPENDS rocksdb_admin
)
INCLUDE_DIRECTORIES( ${CMAKE_BINARY_DIR}/rocksdb_admin/gen-cpp2 )
add_library(counter_thrift ${GEN_FILES})
