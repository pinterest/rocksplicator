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

#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <unordered_set>

#include "boost/functional/hash.hpp"
#include "common/MultiFilePoller.h"
#include "common/kafka/kafka_utils.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "librdkafka/rdkafkacpp.h"

namespace kafka {
typedef std::map<std::string, std::pair<std::string, bool>> KafkaConfigMap;

struct ConsumedOffset {
public:
  ConsumedOffset() : offset(-1), timestamp(-1) {}

  int64_t offset;
  int64_t timestamp;
};

class RdKafkaConsumerHolder {
public:
  RdKafkaConsumerHolder() {};

  // no copy nor move
  RdKafkaConsumerHolder(const RdKafkaConsumerHolder&) = delete;

  RdKafkaConsumerHolder(RdKafkaConsumerHolder&&) = delete;

  virtual ~RdKafkaConsumerHolder() {}

  virtual std::shared_ptr<RdKafka::KafkaConsumer> getInstance() = 0;

  virtual void resetInstance(const TopicPartitionToValueMap<ConsumedOffset>* consumedOffsets) = 0;

  virtual void close() = 0;
};

class RdKafkaConsumerHolderFactory {
public:
  static RdKafkaConsumerHolder* createInstance(const std::unordered_set<uint32_t>& partition_ids,
                                               const std::string& broker_list,
                                               const std::unordered_set<std::string>& topic_names,
                                               const std::string& group_id,
                                               const std::string& kafka_consumer_type);

  static RdKafkaConsumerHolder* createInstance(std::shared_ptr<RdKafka::KafkaConsumer> consumer);
};
} // namespace kafka