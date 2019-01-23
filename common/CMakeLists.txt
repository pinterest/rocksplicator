cmake_minimum_required(VERSION 3.1)

# Build common library
AUX_SOURCE_DIRECTORY(./ COMMON_FILES)

add_library(common ${COMMON_FILES})

target_link_libraries(common jemalloc glog gflags ssl wangle folly aws-cpp-sdk-core aws-cpp-sdk-s3 boost_system boost_filesystem thrift thriftcpp2 jsoncpp_lib_static zstd)

add_subdirectory(rocksdb_glogger)
add_subdirectory(stats)
add_subdirectory(tests)
add_subdirectory(jsoncpp)
