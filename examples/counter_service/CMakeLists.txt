cmake_minimum_required(VERSION 3.1)

project(counter_service)

# Build counter_lib
AUX_SOURCE_DIRECTORY(./ SRC_FILES)
list(REMOVE_ITEM SRC_FILES "./counter.cpp")
list(REMOVE_ITEM SRC_FILES "./stress_test.cpp")
INCLUDE_DIRECTORIES( ${CMAKE_BINARY_DIR}/rocksdb_admin/gen-cpp2 )
add_library(counter_lib ${SRC_FILES})

target_link_libraries(counter_lib counter_thrift rocksdb_admin)


# Build counter service
add_executable(counter ./counter.cpp)
target_link_libraries(counter common counter_lib jemalloc kafka_consumer)

# Build stress test tool
add_executable(stress ./stress_test.cpp)
target_link_libraries(stress counter_lib jemalloc)

add_subdirectory(thrift)
