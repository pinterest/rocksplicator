cmake_minimum_required(VERSION 3.1)

project(rocksplicator)

set(CMAKE_CXX_FLAGS "-std=c++1y -gdwarf-2 -O3 -Wall -fPIC -fno-omit-frame-pointer")

include_directories(/usr/local/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/build")

link_directories("/usr/local/lib/linux/intel64/Release/")

enable_testing()

# submodules
add_subdirectory(googletest)

# Sub-projects
add_subdirectory(common)
add_subdirectory(examples)
add_subdirectory(rocksdb_admin)
add_subdirectory(rocksdb_replicator)
add_subdirectory(tgrep)
