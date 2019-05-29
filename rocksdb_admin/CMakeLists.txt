cmake_minimum_required(VERSION 3.1)

set(GEN_FILES
    "gen-cpp2/rocksdb_admin_types.tcc"
    "gen-cpp2/rocksdb_admin_types.cpp"
    "gen-cpp2/rocksdb_admin_types.h"
    "gen-cpp2/rocksdb_admin_constants.h"
    "gen-cpp2/rocksdb_admin_constants.cpp"
    "gen-cpp2/Admin.tcc"
    "gen-cpp2/Admin.cpp"
    "gen-cpp2/Admin.h"
    "gen-cpp2/Admin_client.cpp")

add_custom_command(
  OUTPUT ${GEN_FILES}
  COMMAND python -mthrift_compiler.main --gen cpp2 -I. -o ./ ${CMAKE_CURRENT_SOURCE_DIR}/rocksdb_admin.thrift
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/rocksdb_admin.thrift
)

AUX_SOURCE_DIRECTORY(./ SRC_FILES)

add_library(rocksdb_admin ${GEN_FILES} ${SRC_FILES})
target_link_libraries(rocksdb_admin thrift thriftcpp2 rocksdb rocksdb_replicator folly glog pthread boost_filesystem rocksdb_glogger common jvm kafka_consumer rdkafka++)

add_subdirectory(tests)
