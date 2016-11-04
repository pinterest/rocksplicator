cmake_minimum_required(VERSION 3.1)

AUX_SOURCE_DIRECTORY(./ SRC_FILES)
add_library(rocksdb_glogger ${SRC_FILES})

target_link_libraries(rocksdb_glogger rocksdb)
