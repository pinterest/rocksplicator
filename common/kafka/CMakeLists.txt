cmake_minimum_required(VERSION 3.1)

# Build common library
AUX_SOURCE_DIRECTORY(./ KAFKA_FILES)

add_library(kafka_consumer ${KAFKA_FILES})

add_subdirectory(tests)

