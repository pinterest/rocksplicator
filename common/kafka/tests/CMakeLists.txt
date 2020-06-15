cmake_minimum_required(VERSION 3.1)

FILE(GLOB TEST_SOURCES *.cpp)

SET(kafka_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../)
SET(mock_kafka_SRCS
        ${CMAKE_CURRENT_SOURCE_DIR}/mock_kafka_cluster.h
        ${CMAKE_CURRENT_SOURCE_DIR}/mock_kafka_consumer.h
        ${CMAKE_CURRENT_SOURCE_DIR}/../../stats/stats.cpp)
ADD_LIBRARY(mock_kafka_consumer ${mock_kafka_SRCS})

foreach(testsourcefile ${TEST_SOURCES})
  get_filename_component(testname ${testsourcefile} NAME_WE)
  add_executable(${testname} ${testsourcefile})
  target_link_libraries(${testname} gtest kafka_consumer common mock_kafka_consumer rdkafka++)
  add_test(NAME ${testname} COMMAND ${testname})
  set_tests_properties(${testname} PROPERTIES WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
endforeach(testsourcefile ${TEST_SOURCES})

