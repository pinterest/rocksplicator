cmake_minimum_required(VERSION 3.1)

project(tgrep)

AUX_SOURCE_DIRECTORY(./ SRC_FILES)

add_executable(tgrep ${SRC_FILES})

target_link_libraries(tgrep folly thrift thriftcpp2 jemalloc pthread gflags glog pcap zstd thriftprotocol)
