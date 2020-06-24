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

#include <thread>

#include "common/kafka/kafka_flags.h"
#include "librdkafka/rdkafkacpp.h"

namespace kafka {
RdKafka::ErrorCode ExecuteKafkaOperationWithRetry(
  std::function<RdKafka::ErrorCode()> func) {
  auto error_code = func();
  size_t num_retries = 0;
  while (num_retries < FLAGS_kafka_consumer_num_retries
         && error_code != RdKafka::ERR_NO_ERROR) {
    error_code = func();
    num_retries++;
    std::this_thread::sleep_for(
      std::chrono::milliseconds(
        FLAGS_kafka_consumer_time_between_retries_ms));
  }
  return error_code;
}

bool KafkaSeekWithRetry(RdKafka::KafkaConsumer *consumer,
                        const RdKafka::TopicPartition &topic_partition) {
  auto error_code = ExecuteKafkaOperationWithRetry([consumer,
                                                     &topic_partition]() {
    return consumer->seek(topic_partition, FLAGS_kafka_consumer_timeout_ms);
  });
  if (error_code != RdKafka::ERR_NO_ERROR) {
    LOG(ERROR) << "Failed to seek to offset for partition: " <<
               topic_partition.partition()
               << ", offset: " << topic_partition.offset()
               << ", error_code: " << RdKafka::err2str(error_code)
               << ", num retries: " << FLAGS_kafka_consumer_num_retries;
    return false;
  }
  return true;
}
} // namespace