cmake_minimum_required(VERSION 3.1)

add_executable(sst_binary sst_binary.cpp)
target_link_libraries(sst_binary rocksdb_admin)

file(GLOB TEST_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/*_test.cpp)
get_filename_component(EXCLUDE_FILES ${CMAKE_CURRENT_SOURCE_DIR}/hbase_backup_integration_test.cpp ABSOLUTE)
list(REMOVE_ITEM TEST_SOURCES "${EXCLUDE_FILES}")

foreach(testsourcefile ${TEST_SOURCES})
  get_filename_component(testname ${testsourcefile} NAME_WE)
  add_executable(${testname} ${testsourcefile})
  target_link_libraries(${testname} gtest rocksdb_admin boost_filesystem rocksdb_glogger kafka_consumer rdkafka++)
  add_test(NAME ${testname} COMMAND ${testname})
endforeach(testsourcefile ${TEST_SOURCES})

add_executable(hbase_backup_integration_test hbase_backup_integration_test.cpp)
target_link_libraries(hbase_backup_integration_test gtest rocksdb_admin boost_filesystem rocksdb_glogger kafka_consumer rdkafka++)
