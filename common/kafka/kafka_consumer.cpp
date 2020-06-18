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
#include "common/kafka/kafka_consumer.h"

#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>
#include <iostream>

#include "common/kafka/kafka_consumer_holder.h"
#include "common/kafka/kafka_flags.h"
#include "common/kafka/kafka_utils.h"
#include "common/kafka/stats_enum.h"
#include "common/stats/stats.h"
#include "folly/String.h"
#include "librdkafka/rdkafka.h"
#include "librdkafka/rdkafkacpp.h"

namespace kafka {

class SSLFilePollerHolder {
public:
  static common::MultiFilePoller *getFilePollerInstance() {
    static common::MultiFilePoller multiFilePoller(std::chrono::seconds(5));
    return &multiFilePoller;
  }
};

KafkaConsumer::KafkaConsumer(const std::unordered_set<uint32_t> &partition_ids,
                             const std::string &broker_list,
                             const std::unordered_set<std::string> &topic_names,
                             const std::string &group_id,
                             const std::string &kafka_consumer_type)
  : KafkaConsumer(RdKafkaConsumerHolderFactory::createInstance(
  partition_ids,
  broker_list,
  topic_names,
  group_id,
  kafka_consumer_type),
                  partition_ids,
                  topic_names,
                  kafka_consumer_type) {}

KafkaConsumer::KafkaConsumer(std::shared_ptr<RdKafka::KafkaConsumer> consumer,
                             const std::unordered_set<uint32_t> &partition_ids,
                             const std::unordered_set<std::string> &topic_names,
                             const std::string &kafka_consumer_type)
  : KafkaConsumer(RdKafkaConsumerHolderFactory::createInstance(std::move(consumer)),
                  partition_ids,
                  topic_names,
                  kafka_consumer_type) {}

KafkaConsumer::KafkaConsumer(
  kafka::RdKafkaConsumerHolder *consumer,
  const std::unordered_set<uint32_t> &partition_ids,
  const std::unordered_set<std::string> &topic_names,
  const std::string &kafka_consumer_type) :
  consumer_(consumer),
  partition_ids_str_(folly::join(",", partition_ids)),
  topic_names_(topic_names),
  partition_ids_(partition_ids),
  kafka_consumer_type_metric_tag_("kafka_consumer_type=" + kafka_consumer_type),
  cbIdPtr_(nullptr),
  is_healthy_(false),
  reset_(false) {
  if (consumer_ == nullptr || consumer_->getInstance() == nullptr) {
    return;
  }

  /**
   * First set the initial timestamp to -1;
   * This will change once, Seek to given timestamp is called;
   */
  for (const auto &topic_name: topic_names_) {
    for (const auto partition_id : partition_ids_) {
      ConsumedOffset offset;
      offset.offset = -1;
      offset.timestamp = -1;
      consumedOffsets_.emplace(std::make_pair(topic_name, partition_id), offset);
    }
  }

  if (!FLAGS_kafka_consumer_reset_on_file_change.empty()) {
    std::vector<std::string> filePathsToWatch;
    folly::split(",", FLAGS_kafka_consumer_reset_on_file_change, filePathsToWatch);

    for (const auto &it : filePathsToWatch) {
      LOG(INFO) << "Will be watching path for change: " << it << std::endl << std::flush;
    }

    cbIdPtr_ = std::make_shared<common::MultiFilePoller::CallbackId>(
      SSLFilePollerHolder::getFilePollerInstance()->registerFiles(
        filePathsToWatch,
        [&](const common::MultiFilePoller::CallbackArg &newData) {
          std::atomic_exchange(&reset_, true);
        }));
  }

  do {
    while (std::atomic_exchange(&reset_, false)) {
      consumer_->resetInstance(nullptr);
    }

    // TODO: Change to unique_ptr
    std::vector<std::shared_ptr<RdKafka::TopicPartition>> topic_partitions;
    // A temporary RdKafka::TopicPartition* vector specifically used
    // for KafkaConsumer->assign(). The pointers inside this vector is
    // owned by `topic_partitions`.
    std::vector<RdKafka::TopicPartition *> tmp_topic_partitions;
    topic_partitions.reserve(partition_ids_.size() * topic_names_.size());
    tmp_topic_partitions.reserve(partition_ids_.size() * topic_names_.size());
    for (const auto partition_id : partition_ids_) {
      for (const auto &topic_name : topic_names_) {
        topic_partitions.push_back(
          std::shared_ptr<RdKafka::TopicPartition>(
            RdKafka::TopicPartition::create(
              topic_name,
              partition_id,
              // Use the end of the offset to init here, but it does not matter.
              // When kafkaconsumer is used, the caller is supposed to call
              // seek() to the expected offset before consuming messages.
              RdKafka::Topic::OFFSET_END)));
        tmp_topic_partitions.push_back(topic_partitions.back().get());
        topic_names_string_.append(topic_name);
        topic_names_string_.append(", ");
      }
    }
    const auto error_code = ExecuteKafkaOperationWithRetry(
      [this, &tmp_topic_partitions]() {
        return consumer_->getInstance()->assign(tmp_topic_partitions);
      });

    if (error_code != RdKafka::ERR_NO_ERROR) {
      LOG(ERROR) << "Failed to assign partitions: " << partition_ids_str_
                 << ", error_code: " << RdKafka::err2str(error_code)
                 << ", num retries: " << FLAGS_kafka_consumer_num_retries;
      common::Stats::get()->Incr(getFullStatsName(
        kKafkaConsumerErrorAssign,
        {kafka_consumer_type_metric_tag_,
         "error_code=" + std::to_string(error_code)}));

      if (std::atomic_load(&reset_)) {
        continue;
      }

      return;
    }
    LOG(INFO) << "Successfully created kafka consumer for partitions: "
              << partition_ids_str_ << ", topics: "
              << folly::join(",", topic_names_);
    is_healthy_.store(true);
  } while (std::atomic_load(&reset_));
}

KafkaConsumer::~KafkaConsumer() {
  if (!FLAGS_kafka_consumer_reset_on_file_change.empty()) {
    if (cbIdPtr_ != nullptr) {
      const auto cbId = *cbIdPtr_.get();
      SSLFilePollerHolder::getFilePollerInstance()->cancelCallback(cbId);
    }
  }

  if (consumer_) {
    consumer_->close();
  }
}

bool KafkaConsumer::IsHealthy() const { return is_healthy_.load(); }

bool KafkaConsumer::Seek(const int64_t timestamp_ms) {
  do {
    while (std::atomic_exchange(&reset_, false)) {
      consumer_->resetInstance(nullptr);
    }

    if (SeekInternal(topic_names_, timestamp_ms)) {
      return true;
    } else {
      common::Stats::get()->Incr(
        getFullStatsName(kKafkaConsumerErrorSeek,
                         {kafka_consumer_type_metric_tag_}));

      if (std::atomic_load(&reset_)) {
        continue;
      }
      return false;
    }
  } while (std::atomic_load(&reset_));
}

bool KafkaConsumer::Seek(const std::string &topic_name,
                         const int64_t timestamp_ms) {
  do {
    while (std::atomic_exchange(&reset_, false)) {
      consumer_->resetInstance(nullptr);
    }

    bool is_success = false;
    if (topic_names_.find(topic_name) != topic_names_.end()) {
      const std::unordered_set<std::string> tmp_topic_names{topic_name};
      is_success = SeekInternal(tmp_topic_names, timestamp_ms);
    } else {
      LOG(ERROR) << "Kakfa consumer is unaware of topic_name: " << topic_name;
      return false;
    }

    if (!is_success) {
      common::Stats::get()->Incr(getFullStatsName(
        kKafkaConsumerErrorSeek, {kafka_consumer_type_metric_tag_,
                                  "topic=" + topic_name}));
      if (std::atomic_load(&reset_)) {
        continue;
      }
    }
    return is_success;
  } while (std::atomic_load(&reset_));
}

bool KafkaConsumer::Seek(const std::map<std::string, std::map<int32_t,
  int64_t>> &last_offsets) {
  do {
    while (std::atomic_exchange(&reset_, false)) {
      consumer_->resetInstance(nullptr);
    }

    if (SeekInternal(last_offsets)) {
      return true;
    }

    common::Stats::get()->Incr(
      getFullStatsName(kKafkaConsumerErrorSeek,
                       {kafka_consumer_type_metric_tag_}));
    if (std::atomic_load(&reset_)) {
      continue;
    }
    return false;
  } while (std::atomic_load(&reset_));
}

bool KafkaConsumer::SeekInternal(
  const std::map<std::string, std::map<int32_t, int64_t>> &last_offsets) {
  if (consumer_ == nullptr || consumer_->getInstance() == nullptr) {
    return false;
  }
  for (const auto &topic_partitions_pair : last_offsets) {
    for (const auto &partition_offset_pair : topic_partitions_pair.second) {
      // Seek to offset + 1 for each topic and partition.
      std::unique_ptr<RdKafka::TopicPartition> topic_partition(
        RdKafka::TopicPartition::create(
          topic_partitions_pair.first,
          partition_offset_pair.first,
          // TODO: it is possible that offset+1 reach a invalid offset position
          partition_offset_pair.second + 1));

      LOG(INFO) << "Seeking topic: " << topic_partition->topic()
                << ", partition: " << topic_partition->partition()
                << ", offset: " << topic_partition->offset();

      if (!KafkaSeekWithRetry(consumer_->getInstance().get(), *topic_partition)) {
        return false;
      }

      if (partition_offset_pair.second != -1) {
        ConsumedOffset consumedOffset;
        consumedOffset.offset = partition_offset_pair.second;
        consumedOffset.timestamp = -1;
        std::pair<std::string, std::int32_t> topic_partition_pair
          = std::make_pair(topic_partitions_pair.first, partition_offset_pair.first);
        consumedOffsets_[topic_partition_pair] = consumedOffset;
      }
    }
  }
  return true;
}

bool KafkaConsumer::SeekInternal(
  const std::unordered_set<std::string> &topic_names,
  const int64_t timestamp_ms) {
  if (consumer_ == nullptr || consumer_->getInstance() == nullptr) {
    return false;
  }
  std::vector<std::shared_ptr<RdKafka::TopicPartition>> topic_partitions;
  // A temporary RdKafka::TopicPartition* vector specifically used for
  // KafkaConsumer->offsetsForTimes().
  // The pointers inside this vector is owned by `topic_partitions`.
  std::vector<RdKafka::TopicPartition *> tmp_topic_partitions;
  topic_partitions.reserve(topic_names.size());
  tmp_topic_partitions.reserve(topic_names.size());
  for (const auto partition_id : partition_ids_) {
    for (const auto &topic_name : topic_names) {
      topic_partitions.push_back(
        std::shared_ptr<RdKafka::TopicPartition>(
          RdKafka::TopicPartition::create(
            topic_name,
            partition_id,
            timestamp_ms)));
      tmp_topic_partitions.push_back(topic_partitions.back().get());
    }
  }
  // Find the offset starting from the given timestamp.

  auto error_code = ExecuteKafkaOperationWithRetry(
    [this, &tmp_topic_partitions]() {
      return consumer_->getInstance()->offsetsForTimes(tmp_topic_partitions,
                                                       FLAGS_kafka_consumer_timeout_ms);
    });
  if (error_code != RdKafka::ERR_NO_ERROR) {
    LOG(ERROR) << "Failed to get offset from partitions: " << partition_ids_str_
               << ", timestamp_ms: " << timestamp_ms
               << ", error_code: " << RdKafka::err2str(error_code)
               << ", num retries: " << FLAGS_kafka_consumer_num_retries;
    return false;
  }

  // For each topic, seek to the above offset.
  for (const auto *topic_partition : tmp_topic_partitions) {
    LOG(INFO) << "Seeking partition: " << topic_partition->partition()
              << ", offset: " << topic_partition->offset();

    std::pair<std::string, std::int32_t> topic_partition_pair =
      std::make_pair(topic_partition->topic(), topic_partition->partition());

    ConsumedOffset consumedOffset;
    consumedOffset.offset = topic_partition->offset();
    consumedOffset.timestamp = timestamp_ms;

    common::Stats::get()->Incr(
      getFullStatsName(kKafkaConsumerSeek,
                       {kafka_consumer_type_metric_tag_,
                        "topic=" + topic_partition->topic(),
                        "partition=" +
                        std::to_string(topic_partition->partition())}));
    if (!KafkaSeekWithRetry(consumer_->getInstance().get(), *topic_partition)) {
      return false;
    }
    consumedOffsets_[topic_partition_pair] = consumedOffset;
  }
  return true;
}

RdKafka::Message *KafkaConsumer::Consume(int32_t timeout_ms) {
  do {
    if (consumer_ == nullptr || consumer_->getInstance() == nullptr) {
      return nullptr;
    }

    while (std::atomic_exchange(&reset_, false)) {
      consumer_->resetInstance(&consumedOffsets_);
    }

    auto *message = consumer_->getInstance()->consume(timeout_ms);

    if (message == nullptr) {
      if (std::atomic_load(&reset_)) {
        consumer_->resetInstance(&consumedOffsets_);
        continue;
      }

      common::Stats::get()->Incr(
        getFullStatsName(kKafkaConsumerMessageNull,
                         {kafka_consumer_type_metric_tag_}));
      return nullptr;
    }

    const auto error_code = message->err();
    if (error_code != RdKafka::ERR_NO_ERROR
        && error_code != RdKafka::ERR__PARTITION_EOF
        && error_code != RdKafka::ERR__TIMED_OUT) {
      LOG(ERROR) << "Failed to consume from kafka, error_code: "
                 << RdKafka::err2str(error_code)
                 << ", partition ids: " << partition_ids_str_;
      common::Stats::get()->Incr(getFullStatsName(
        kKafkaConsumerErrorConsume,
        {kafka_consumer_type_metric_tag_,
         "error_code=" + std::to_string(error_code)}));

      if (std::atomic_load(&reset_)) {
        continue;
      }
    }

    /**
     * Keep track of offsets, timestamps of individual topic / partition pairs;
     */
    if (error_code == RdKafka::ERR_NO_ERROR) {
      const auto topic_partition_pair = std::make_pair(message->topic_name(),
                                                       message->partition());
      ConsumedOffset consumedOffset;
      consumedOffset.timestamp = message->timestamp().timestamp;
      consumedOffset.offset = message->offset();
      consumedOffsets_[topic_partition_pair] = consumedOffset;
    }

    return message;
  } while (std::atomic_load(&reset_));
}

const std::unordered_set<std::string> &KafkaConsumer::GetTopicNames() const {
  return topic_names_;
}

const std::unordered_set<uint32_t> &KafkaConsumer::GetPartitionIds() const {
  return partition_ids_;
}

const std::string &KafkaConsumer::GetTopicsString() const {
  return topic_names_string_;
}

}  // namespace kafka
