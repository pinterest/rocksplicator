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

#include <memory>
#include <string>
#include <unordered_set>

#include "folly/MPMCQueue.h"

namespace kafka {

class KafkaConsumer;

typedef folly::MPMCQueue<std::shared_ptr<KafkaConsumer>> KafkaConsumerQueue;

/**
 * A thread safe kafka consumer pool for one particular kafka partition. The
 * pool will be initialized with the fixed queue_size at the construction time.
 * Then caller can call Get() and Put() to use and return the consumer.
 * All Get() and Put() are blocking.
 */
class KafkaConsumerPool {
 public:
  explicit KafkaConsumerPool(
      uint32_t queue_size,
      const std::unordered_set<uint32_t>& partition_ids,
      const std::string& broker_list,
      const std::unordered_set<std::string>& topic_names,
      const std::string& group_id,
      const std::string& kafka_consumer_type);

  // Creates an empty consumer pool, caller needs to add the consumers in order
  // to use the pool. Note that the pool does not differentiate between
  // different consumers
  explicit KafkaConsumerPool(uint32_t queue_size)
      : queue_(std::make_unique<KafkaConsumerQueue>(queue_size)) {}


  KafkaConsumerPool(const KafkaConsumerPool&) = delete;

  KafkaConsumerPool(KafkaConsumerPool&&) = delete;

  ~KafkaConsumerPool() = default;

  virtual std::shared_ptr<KafkaConsumer> Get();

  virtual void Put(std::shared_ptr<KafkaConsumer> consumer);

 protected:
  // For tests only
  KafkaConsumerPool() {}

 private:
  std::unique_ptr<KafkaConsumerQueue> queue_;
  // Tag used when logging metrics, so we can differentiate between kafka
  // consumers for different use cases
  const std::string kafka_consumer_type_metric_tag_;

  // TODO: put those into a struct.
  const std::unordered_set<uint32_t> partition_ids_;
  const std::string broker_list_;
  const std::unordered_set<std::string> topic_names_;
  const std::string group_id_;
  const std::string kafka_consumer_type_;
};

}  // namespace kafka
