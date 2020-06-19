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
#include <string>
#include <unordered_set>

#include "common/MultiFilePoller.h"
#include "common/kafka/kafka_consumer_holder.h"


namespace kafka {

class KafkaConsumer {
public:
  KafkaConsumer(const std::unordered_set<uint32_t>& partition_ids,
                const std::string& broker_list,
                const std::unordered_set<std::string>& topic_names,
                const std::string& group_id,
                const std::string& kafka_consumer_type);

  KafkaConsumer(std::shared_ptr<RdKafka::KafkaConsumer> consumer,
                const std::unordered_set<uint32_t>& partition_ids,
                const std::unordered_set<std::string>& topic_names,
                const std::string& kafka_consumer_type);

  // no copy nor move
  KafkaConsumer(const KafkaConsumer&) = delete;

  KafkaConsumer(KafkaConsumer&&) = delete;

  // Returns false if this kafka consumer is not initialized correctly.
  // In this case, this Kafka consumer is not useable. Seek() always returns
  // false and Consume() always return nullptr.
  virtual bool IsHealthy() const;

  // Seek to the Kafka message whose offset is the earliest one greater than or
  // equal to the given timestamp. After calling this, messages returned by
  // Consume() will be starting from this new offset.
  bool Seek(const int64_t timestamp_ms);

  bool Seek(const std::string& topic_name, const int64_t timestamp_ms);

  // Seek to offsets specified in the KafkaTopicPartitionOffsets map.
  bool Seek(const std::map<std::string, std::map<int32_t,
    int64_t>>& last_offsets);

  // It returns nullptr if IsHealthy() returns false.
  virtual RdKafka::Message* Consume(int32_t timeout_ms);

  // Gets Kafka topic names this consumer is assigned to.
  const std::unordered_set<std::string>& GetTopicNames() const;

  // Get Kafka partition ids this consumer is assigned to.
  const std::unordered_set<uint32_t>& GetPartitionIds() const;

  // Return set of topics as a string for logging
  const std::string& GetTopicsString() const;

  virtual ~KafkaConsumer();

  const std::string partition_ids_str_;

private:
  KafkaConsumer(RdKafkaConsumerHolder* holder,
                const std::unordered_set<uint32_t>& partition_ids,
                const std::unordered_set<std::string>& topic_names,
                const std::string& kafka_consumer_type);

  virtual bool SeekInternal(const std::unordered_set<std::string>& topic_names,
                            const int64_t timestamp_ms);

  virtual bool SeekInternal(const std::map<std::string, std::map<int32_t,
    int64_t>>& last_offsets);

  inline bool isResettable() {
    return reset_callback_id_ptr_ != nullptr;
  }

  inline bool shouldResetAndExchange() {
    return isResettable() && std::atomic_exchange(&should_reset_rd_kafka_consumer_, false);
  }

  inline bool shouldResetLoad() {
    return isResettable() && std::atomic_load(&should_reset_rd_kafka_consumer_);
  }

  inline bool resetSeekedConsumer() {
    rd_kafka_consumer_provider_->resetInstance(&last_known_consumed_offsets_);
  }

  inline bool resetUnSeekedConsumer() {
    rd_kafka_consumer_provider_->resetInstance(nullptr);
  }

  inline bool isConsumerAvailable() {
    return !(rd_kafka_consumer_provider_ == nullptr
     || rd_kafka_consumer_provider_->getInstance() == nullptr);
  }

  const std::unordered_set<std::string> topic_names_;
  std::string topic_names_string_;
  const std::unordered_set<uint32_t> partition_ids_;
  const std::shared_ptr<RdKafkaConsumerHolder> rd_kafka_consumer_provider_;
  // Tag used when logging metrics, so we can differentiate between kafka
  // consumers for different use cases.
  const std::string kafka_consumer_type_metric_tag_;
  std::shared_ptr<common::MultiFilePoller::CallbackId> reset_callback_id_ptr_;
  std::atomic<bool> is_healthy_;
  std::atomic<bool> should_reset_rd_kafka_consumer_;
  TopicPartitionToValueMap<ConsumedOffset> last_known_consumed_offsets_;
};

}  // namespace kafka
