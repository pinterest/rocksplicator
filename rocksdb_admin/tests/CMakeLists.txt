cmake_minimum_required(VERSION 3.1)

file(GLOB TEST_SOURCES ./*.cpp)

foreach(testsourcefile ${TEST_SOURCES})
  get_filename_component(testname ${testsourcefile} NAME_WE)
  add_executable(${testname} ${testsourcefile})
  target_link_libraries(${testname} gtest rocksdb_admin boost_filesystem rocksdb_glogger)
  add_test(NAME ${testname} COMMAND ${testname})
endforeach(testsourcefile ${TEST_SOURCES})
