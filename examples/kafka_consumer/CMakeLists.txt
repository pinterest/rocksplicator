cmake_minimum_required(VERSION 3.1)

project(kafka_consumer_app)

AUX_SOURCE_DIRECTORY(./ SRC_FILES)

# Build counter service
add_executable(kafka_consumer_app ./kafka_consumer_app.cpp)
target_link_libraries(kafka_consumer_app common stats kafka_consumer rdkafka++ jemalloc pthread rocksdb_admin)
