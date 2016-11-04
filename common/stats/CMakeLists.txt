cmake_minimum_required(VERSION 2.8)

AUX_SOURCE_DIRECTORY(./ SRC_FILES)
add_library(stats ${SRC_FILES})
target_link_libraries(stats glog microhttpd gflags folly)

add_subdirectory(tests)

