cmake_minimum_required(VERSION 3.1)


AUX_SOURCE_DIRECTORY(./ SRC_FILES)
list(REMOVE_ITEM SRC_FILES ${CMAKE_CURRENT_SOURCE_DIR}/performance.cpp)

add_library(rocksdb_replicator ${SRC_FILES})

target_link_libraries(rocksdb_replicator replicator_thrift jemalloc glog gflags folly thriftcpp2 thrift rocksdb pthread ssl stats boost_system common thriftprotocol)

# Build performance
add_executable(performance ./performance.cpp)

target_link_libraries(performance rocksdb_replicator)

add_subdirectory(thrift)
add_subdirectory(tests)
