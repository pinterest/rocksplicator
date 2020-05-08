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

#include "common/stats/stats.h"
#include "folly/String.h"
#include "librdkafka/rdkafka.h"
#include "librdkafka/rdkafkacpp.h"
#include "common/kafka/stats_enum.h"
#include "common/kafka/kafka_config.h"

DEFINE_int32(kafka_consumer_timeout_ms, 1000,
    "Kafka consumer timeout in ms");
DEFINE_int32(kafka_consumer_num_retries, 0,
    "Number of retries to try the kafka consumer");
DEFINE_int32(kafka_consumer_time_between_retries_ms, 100,
    "Time in ms to sleep between each retry");
DEFINE_string(enable_kafka_auto_offset_store, "false",
    "Whether to automatically commit the kafka offsets");
DEFINE_string(kafka_client_global_config_file, "",
    "Provide additional global parameters to kafka client library e.g.. ssl configuration");

namespace {

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

bool KafkaSeekWithRetry(RdKafka::KafkaConsumer* consumer,
                        const RdKafka::TopicPartition& topic_partition) {
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

std::shared_ptr<RdKafka::KafkaConsumer> CreateRdKafkaConsumer(
  // https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
  const std::shared_ptr<kafka::ConfigMap>& config,
  const std::unordered_set<uint32_t>& partition_ids,
  const std::string kafka_consumer_type) {
  std::string err;
  auto conf = std::shared_ptr<RdKafka::Conf>(RdKafka::Conf::create(
    RdKafka::Conf::CONF_GLOBAL));

  // Next go through each of the GLOBAL config and set.
  for (kafka::ConfigMap::const_iterator it = config->begin(); it != config->end(); ++it) {
    const std::string& key = it->first;
    const std::pair<std::string, bool>& value = it->second;

    if (conf->set(key, value.first, err) != RdKafka::Conf::CONF_OK) {
      if (value.second) {
        LOG(ERROR) << "Failed to create kafka config for partitions: "
                   << folly::join(",", partition_ids)
                   << ", error: " << err
                   << "required conf couldn't be set "
                   << "key=" << key
                   << ", value=" << value.first << std::endl;
        common::Stats::get()->Incr(getFullStatsName(
          kKafkaConsumerErrorInit, {"kafka_consumer_type=" + kafka_consumer_type}));
        return nullptr;
      } else {
        LOG(ERROR) << "Couldn't set provided config key-value pair"
                   << "key=" << key
                   << ", value=" << value.first
                   << ", error:" << err << std::endl;
      }
    }
  }

  return std::shared_ptr<RdKafka::KafkaConsumer>(
    RdKafka::KafkaConsumer::create(conf.get(), err));
}

std::shared_ptr<RdKafka::KafkaConsumer> CreateRdKafkaConsumer(
    const std::unordered_set<uint32_t>& partition_ids,
    const std::string& broker_list,
    const std::string& group_id,
    const std::string kafka_consumer_type) {
  std::string err;
  auto conf = std::shared_ptr<RdKafka::Conf>(RdKafka::Conf::create(
    RdKafka::Conf::CONF_GLOBAL));

  std::shared_ptr<kafka::ConfigMap> configMap =
    std::shared_ptr<kafka::ConfigMap>(new kafka::ConfigMap);

  /**
   * Each of the config parameter provided must be valid and known
   * to librdkafka. This makes sure, we fail loud before setting any parameter
   * that doesn't belong in the kafka config w.r.t version of the librdkafka
   */
  if (!FLAGS_kafka_client_global_config_file.empty()) {
    if (!kafka::read_conf_file(FLAGS_kafka_client_global_config_file, configMap, true)) {
      LOG(ERROR) << "Can not read / parse config file: "
                 << FLAGS_kafka_client_global_config_file
                 << std::endl;
      return nullptr;
    }
  }
  // Following settings cannot be overwritten from config file...
  // https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
  (*configMap)["metadata.broker.list"] = std::make_pair(broker_list, true);
  (*configMap)["enable.partition.eof"] = std::make_pair("true", true);
  // set api.version.request to true so that Kafka timestamp can be used.
  (*configMap)["api.version.request"] = std::make_pair("true", true);
  (*configMap)["enable.auto.offset.store"] = std::make_pair(FLAGS_enable_kafka_auto_offset_store, true);
  (*configMap)["group.id"] = std::make_pair(group_id, true);

  return CreateRdKafkaConsumer(configMap, partition_ids, kafka_consumer_type);
}

}  // namespace

namespace kafka {

KafkaConsumer::KafkaConsumer(const std::unordered_set<uint32_t>& partition_ids,
                             const std::string& broker_list,
                             const std::unordered_set<std::string>& topic_names,
                             const std::string& group_id,
                             const std::string& kafka_consumer_type)
    : KafkaConsumer(
          CreateRdKafkaConsumer(partition_ids, broker_list,
              group_id, kafka_consumer_type),
          partition_ids,
          topic_names,
          kafka_consumer_type) {}

KafkaConsumer::KafkaConsumer(std::shared_ptr<RdKafka::KafkaConsumer> consumer,
                             const std::unordered_set<uint32_t>& partition_ids,
                             const std::unordered_set<std::string>& topic_names,
                             const std::string& kafka_consumer_type)
    : consumer_(std::move(consumer)),
      partition_ids_str_(folly::join(",", partition_ids)),
      topic_names_(topic_names),
      partition_ids_(partition_ids),
      kafka_consumer_type_metric_tag_("kafka_consumer_type=" +
      kafka_consumer_type),
      is_healthy_(false) {
  if (consumer_ == nullptr) {
    return;
  }

  // TODO: Change to unique_ptr
  std::vector<std::shared_ptr<RdKafka::TopicPartition>> topic_partitions;
  // A temporary RdKafka::TopicPartition* vector specifically used
  // for KafkaConsumer->assign(). The pointers inside this vector is
  // owned by `topic_partitions`.
  std::vector<RdKafka::TopicPartition*> tmp_topic_partitions;
  topic_partitions.reserve(partition_ids_.size() * topic_names_.size());
  tmp_topic_partitions.reserve(partition_ids_.size() * topic_names_.size());
  for (const auto partition_id : partition_ids_) {
    for (const auto& topic_name : topic_names_) {
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
        return consumer_->assign(tmp_topic_partitions); });
  if (error_code != RdKafka::ERR_NO_ERROR) {
    LOG(ERROR) << "Failed to assign partitions: " << partition_ids_str_
               << ", error_code: " << RdKafka::err2str(error_code)
               << ", num retries: " << FLAGS_kafka_consumer_num_retries;
    common::Stats::get()->Incr(getFullStatsName(
        kKafkaConsumerErrorAssign,
        {kafka_consumer_type_metric_tag_,
         "error_code=" + std::to_string(error_code)}));
    return;
  }
  LOG(INFO) << "Successfully created kafka consumer for partitions: "
            << partition_ids_str_ << ", topics: "
            << folly::join(",", topic_names_);
  is_healthy_.store(true);
}

bool KafkaConsumer::IsHealthy() const { return is_healthy_.load(); }

bool KafkaConsumer::Seek(const int64_t timestamp_ms) {
  if (SeekInternal(topic_names_, timestamp_ms)) {
    return true;
  } else {
    common::Stats::get()->Incr(
        getFullStatsName(kKafkaConsumerErrorSeek,
            {kafka_consumer_type_metric_tag_}));
    return false;
  }
}

bool KafkaConsumer::Seek(const std::string& topic_name,
    const int64_t timestamp_ms) {
  bool is_success = false;
  if (topic_names_.find(topic_name) != topic_names_.end()) {
    const std::unordered_set<std::string> tmp_topic_names{topic_name};
    is_success = SeekInternal(tmp_topic_names, timestamp_ms);
  } else {
    LOG(ERROR) << "Kakfa consumer is unaware of topic_name: " << topic_name;
  }

  if (!is_success) {
    common::Stats::get()->Incr(getFullStatsName(
        kKafkaConsumerErrorSeek, {kafka_consumer_type_metric_tag_,
                                  "topic=" + topic_name}));
  }
  return is_success;
}

bool KafkaConsumer::Seek(const std::map<std::string, std::map<int32_t,
    int64_t>>& last_offsets) {
  if (SeekInternal(last_offsets)) {
    return true;
  }

  common::Stats::get()->Incr(
      getFullStatsName(kKafkaConsumerErrorSeek,
          {kafka_consumer_type_metric_tag_}));
  return false;
}

bool KafkaConsumer::SeekInternal(
    const std::map<std::string, std::map<int32_t, int64_t>>& last_offsets) {
  if (consumer_ == nullptr) {
    return false;
  }
  for (const auto& topic_partitions_pair : last_offsets) {
    for (const auto& partition_offset_pair : topic_partitions_pair.second) {
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
      if (!KafkaSeekWithRetry(consumer_.get(), *topic_partition)) {
        return false;
      }
    }
  }
  return true;
}

bool KafkaConsumer::SeekInternal(
    const std::unordered_set<std::string>& topic_names,
    const int64_t timestamp_ms) {
  if (consumer_ == nullptr) {
    return false;
  }
  std::vector<std::shared_ptr<RdKafka::TopicPartition>> topic_partitions;
  // A temporary RdKafka::TopicPartition* vector specifically used for
  // KafkaConsumer->offsetsForTimes().
  // The pointers inside this vector is owned by `topic_partitions`.
  std::vector<RdKafka::TopicPartition*> tmp_topic_partitions;
  topic_partitions.reserve(topic_names.size());
  tmp_topic_partitions.reserve(topic_names.size());
  for (const auto partition_id : partition_ids_) {
    for (const auto& topic_name : topic_names) {
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
    return consumer_->offsetsForTimes(tmp_topic_partitions,
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
  for (const auto* topic_partition : tmp_topic_partitions) {
    LOG(INFO) << "Seeking partition: " << topic_partition->partition()
              << ", offset: " << topic_partition->offset();

    common::Stats::get()->Incr(
        getFullStatsName(kKafkaConsumerSeek,
                           {kafka_consumer_type_metric_tag_,
                            "topic=" + topic_partition->topic(),
                            "partition=" +
                            std::to_string(topic_partition->partition())}));
    if (!KafkaSeekWithRetry(consumer_.get(), *topic_partition)) {
      return false;
    }
  }
  return true;
}

RdKafka::Message* KafkaConsumer::Consume(int32_t timeout_ms) const {
  if (consumer_ == nullptr) {
    return nullptr;
  }

  auto* message = consumer_->consume(timeout_ms);

  if (message == nullptr) {
    common::Stats::get()->Incr(
        getFullStatsName(kKafkaConsumerMessageNull,
            {kafka_consumer_type_metric_tag_}));
    return nullptr;
  }

  const auto error_code = message->err();
  if (error_code != RdKafka::ERR_NO_ERROR && error_code !=
      RdKafka::ERR__PARTITION_EOF && error_code != RdKafka::ERR__TIMED_OUT) {
    LOG(ERROR) << "Failed to consume from kafka, error_code: "
               << RdKafka::err2str(error_code)
               << ", partition ids: " << partition_ids_str_;
    common::Stats::get()->Incr(getFullStatsName(
        kKafkaConsumerErrorConsume,
        {kafka_consumer_type_metric_tag_,
         "error_code=" + std::to_string(error_code)}));
  }

  return message;
}

const std::unordered_set<std::string>& KafkaConsumer::GetTopicNames() const {
  return topic_names_;
}

const std::unordered_set<uint32_t>& KafkaConsumer::GetPartitionIds() const {
  return partition_ids_;
}

const std::string& KafkaConsumer::GetTopicsString() const {
  return topic_names_string_;
}

}  // namespace kafka
