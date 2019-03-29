cmake_minimum_required(VERSION 3.1)

FILE(GLOB TEST_SOURCES *.cpp)
LIST(REMOVE_ITEM TEST_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/detector_benchmark.cpp)

foreach(testsourcefile ${TEST_SOURCES})
  get_filename_component(testname ${testsourcefile} NAME_WE)
  add_executable(${testname} ${testsourcefile})
  target_link_libraries(${testname} common dummy_service_thrift gtest ssl stats thriftprotocol)
  add_test(NAME ${testname} COMMAND ${testname})
endforeach(testsourcefile ${TEST_SOURCES})

add_subdirectory(thrift)
