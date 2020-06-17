/// Copyright 2019 Pinterest Inc.
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
/// http://www.apache.org/licenses/LICENSE-2.0

/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.

#include "common/kafka/kafka_utils.h"

#include "gflags/gflags.h"
#include "glog/logging.h"

DEFINE_int32(kafka_consumer_timeout_ms, 1000,
"Kafka consumer timeout in ms");
DEFINE_int32(kafka_consumer_num_retries, 0,
"Number of retries to try the kafka consumer");
DEFINE_int32(kafka_consumer_time_between_retries_ms, 100,
"Time in ms to sleep between each retry");
DEFINE_string(kafka_consumer_reset_on_file_change, "",
"Comma separated files to watch and reset kafka consumer, when the file change is noticed");
DEFINE_string(enable_kafka_auto_offset_store, "false",
"Whether to automatically commit the kafka offsets");
DEFINE_string(kafka_client_global_config_file, "",
"Provide additional global parameters to kafka client library e.g.. ssl configuration");
