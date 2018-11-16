cmake_minimum_required(VERSION 3.1)

project(rocksplicator)

set(CMAKE_CXX_FLAGS "-std=c++17 -gdwarf-2 -O3 -Wall -fPIC -fno-omit-frame-pointer")

include_directories(/usr/local/include)
include_directories(/usr/lib/jvm/java-8-openjdk-amd64/include)
include_directories(/usr/lib/jvm/java-8-openjdk-amd64/include/linux/)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/build")

link_directories("/usr/local/lib/linux/intel64/Release/")
link_directories("/usr/lib/jvm/java-8-openjdk-amd64/jre/lib/amd64/server/")

enable_testing()

# submodules
add_subdirectory(googletest)

# Sub-projects
add_subdirectory(common)
add_subdirectory(examples)
add_subdirectory(rocksdb_admin)
add_subdirectory(rocksdb_replicator)
add_subdirectory(tgrep)
