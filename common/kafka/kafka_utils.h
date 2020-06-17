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

#pragma once

#include <unordered_set>
#include <unordered_map>
#include <functional>

#include "librdkafka/rdkafkacpp.h"
#include "boost/functional/hash.hpp"


namespace kafka {

typedef std::unordered_set<std::pair<std::string, int32_t>,
  boost::hash<std::pair<std::string, int32_t>>>
  TopicPartitionSet;

template <typename V>
using TopicPartitionToValueMap = std::
unordered_map<std::pair<std::string, int32_t>, V,
  boost::hash<std::pair<std::string, int32_t>>>;

inline std::string TopicPartitionSetToString(
  const TopicPartitionSet& topic_partition_set) {
  std::string s;
  for (const auto& pair : topic_partition_set) {
    s.append("(").append(pair.first).append(",").
      append(std::to_string(pair.second)).append(") ");
  }
  return s;
}

inline int64_t GetMessageTimestamp(const RdKafka::Message& message) {
  const auto ts = message.timestamp();
  if (ts.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME) {
    return ts.timestamp;
  }

  // We only expect the timestamp to be create time.
  return -1;
}

RdKafka::ErrorCode ExecuteKafkaOperationWithRetry(
  std::function<RdKafka::ErrorCode()> func);

bool KafkaSeekWithRetry(RdKafka::KafkaConsumer *consumer,
                        const RdKafka::TopicPartition &topic_partition);
} // namespace kafka