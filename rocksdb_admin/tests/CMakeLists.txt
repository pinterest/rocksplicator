cmake_minimum_required(VERSION 3.1)

add_executable(sst_binary sst_binary.cpp)
target_link_libraries(sst_binary rocksdb_admin rocksdb_admin_detail)

file(GLOB TEST_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/*_test.cpp)
get_filename_component(EXCLUDE_HBASE_TEST_FILE ${CMAKE_CURRENT_SOURCE_DIR}/hbase_backup_integration_test.cpp ABSOLUTE)
get_filename_component(EXCLUDE_S3_TEST_FILE ${CMAKE_CURRENT_SOURCE_DIR}/s3_backup_integration_test.cpp ABSOLUTE)
list(REMOVE_ITEM TEST_SOURCES "${EXCLUDE_HBASE_TEST_FILE}" "${EXCLUDE_S3_TEST_FILE}")

foreach(testsourcefile ${TEST_SOURCES})
  get_filename_component(testname ${testsourcefile} NAME_WE)
  add_executable(${testname} ${testsourcefile})
  target_link_libraries(${testname} gtest rocksdb_admin rocksdb_admin_detail boost_filesystem rocksdb_glogger kafka_consumer rdkafka++)
  add_test(NAME ${testname} COMMAND ${testname})
endforeach(testsourcefile ${TEST_SOURCES})

add_executable(hbase_backup_integration_test hbase_backup_integration_test.cpp)
target_link_libraries(hbase_backup_integration_test gtest rocksdb_admin boost_filesystem rocksdb_glogger kafka_consumer rdkafka++)

add_executable(s3_backup_integration_test s3_backup_integration_test.cpp)
target_link_libraries(s3_backup_integration_test common gtest rocksdb_admin boost_filesystem rocksdb_glogger kafka_consumer rdkafka++)
#add_test(NAME s3_backup_integration_test COMMAND s3_backup_integration_test)