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
DEFINE_string(kafka_consumer_reset_on_file_change, "",
    "Comma separated files to watch and reset kafka consumer, when the file change is noticed");

namespace {

using namespace common;

class SSLFilePollerHolder {
public:
  static common::MultiFilePoller *getFilePollerInstance() {
    static common::MultiFilePoller multiFilePoller(std::chrono::seconds(5));
    return &multiFilePoller;
  }
};

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

typedef std::map<std::string, std::pair<std::string, bool>> KafkaConfigMap;
std::shared_ptr<RdKafka::KafkaConsumer> CreateRdKafkaConsumer(
  // https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
  const KafkaConfigMap& config,
  const std::unordered_set<uint32_t>& partition_ids,
  const std::string kafka_consumer_type) {
  std::string err;
  auto conf = std::shared_ptr<RdKafka::Conf>(RdKafka::Conf::create(
    RdKafka::Conf::CONF_GLOBAL));

  // Next go through each of the GLOBAL config and set.
  for (KafkaConfigMap::const_iterator it = config.begin(); it != config.end(); ++it) {
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

  KafkaConfigMap kafkaConfigMap;

  // Note: Following settings are overridden from configuration. We explicitly
  // set the enable.partition.eof property to true, so that control passes back
  // to client code one's partition eof is hit. This is important for rocksplicator's
  // BootstrapStateModel. Since this property default changed from true to false,
  // in librdkafka-1.0.0-RC4 (https://github.com/edenhill/librdkafka/issues/2020,
  // https://github.com/edenhill/librdkafka/commit/a869fa19359fc285b19496c4321abe199322a736)
  // upgrade to newer version of librdkafka will be impossible
  // without explicitly setting this parameter to true.
  //
  // However for compatibility with older versions of librdkafka that may not have
  // this flag in first place, we don't require this setting to be compatible.
  // We also allow this setting to be overriden from configuration file.
  kafkaConfigMap["enable.partition.eof"] = std::make_pair("true", false);

  // This preserves previous behavior. However this setting can be overwritten
  // from configuration file as well, in which case this need not be set from
  // flags.
  kafkaConfigMap["enable.auto.offset.store"] = std::make_pair(
    FLAGS_enable_kafka_auto_offset_store, false);

  kafka::ConfigMap configMap;
  if (!FLAGS_kafka_client_global_config_file.empty()) {
    if (!kafka::KafkaConfig::read_conf_file(
      FLAGS_kafka_client_global_config_file, &configMap)) {
      LOG(ERROR) << "Can not read / parse config file: "
                 << FLAGS_kafka_client_global_config_file
                 << std::endl;
      common::Stats::get()->Incr(getFullStatsName(
        kKafkaConsumerErrorInit, {"kafka_consumer_type=" + kafka_consumer_type}));
      return nullptr;
    }

    // Each of the config parameter provided must be valid and known to
    // librdkafka. This makes sure, we fail loud before setting any parameter
    // that doesn't belong in the kafka config w.r.t version of the librdkafka
    for (auto iter = configMap.begin(); iter != configMap.end(); ++iter) {
      kafkaConfigMap[iter->first] = std::make_pair(iter->second, true);
    }
  }

  // Following settings can't be set from configuration file... and are
  // overwritten, if provided in configuration file.

  // https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
  kafkaConfigMap["metadata.broker.list"] = std::make_pair(broker_list, true);

  // set api.version.request to true so that Kafka timestamp can be used.
  // This is necessary for seek to work based on timestamps, hence a required
  // setting and can't be overridden from configuration file.
  kafkaConfigMap["api.version.request"] = std::make_pair("true", true);

  // Group id is necessary and hence we fail if it can't be set.
  kafkaConfigMap["group.id"] = std::make_pair(group_id, true);

  return CreateRdKafkaConsumer(kafkaConfigMap, partition_ids, kafka_consumer_type);
}

}  // namespace

namespace kafka {

class RdKafkaConsumerHolderRenewable : virtual public RdKafkaConsumerHolder {
private:
  const std::unordered_set<uint32_t> partition_ids_;
  const std::string &broker_list_;
  const std::unordered_set<std::string> &topic_names_;
  const std::string &group_id_;
  const std::string &kafka_consumer_type_;

  std::shared_ptr<RdKafka::KafkaConsumer> kafkaConsumer_;

public:
  RdKafkaConsumerHolderRenewable(
    const std::unordered_set<uint32_t> &partition_ids,
    const std::string &broker_list,
    const std::unordered_set<std::string> &topic_names,
    const std::string &group_id,
    const std::string &kafka_consumer_type) :
    partition_ids_(partition_ids), broker_list_(broker_list),
    topic_names_(topic_names),
    group_id_(group_id),
    kafka_consumer_type_(kafka_consumer_type) {
    kafkaConsumer_ = CreateRdKafkaConsumer(
      partition_ids_,
      broker_list_,
      group_id_,
      kafka_consumer_type_);
  }

  virtual ~RdKafkaConsumerHolderRenewable() {
    if (kafkaConsumer_) {
      kafkaConsumer_->close();
    }
  }

  virtual std::shared_ptr<RdKafka::KafkaConsumer> getInstance() override {
    return kafkaConsumer_;
  }

  virtual void resetInstance() override {
    /*
     * Before creating a new KafkaConsumer instance, make sure old one is closed
     * and shutdown.
     */
    LOG(INFO) << "Closing kafka consumer for x" << std::endl;
    if (kafkaConsumer_) {
      kafkaConsumer_->close();
    }

    LOG(INFO) << "Recreating kafka consumer for topics "
    << folly::join(",", topic_names_)
    << "partitions"
    << folly::join(",", partition_ids_) << std::endl;
    kafkaConsumer_ = CreateRdKafkaConsumer(
      partition_ids_,
      broker_list_,
      group_id_,
      kafka_consumer_type_);
    LOG(INFO) << "Recreated kafka consumer for x" << std::endl;

  }

  virtual void close() override {
    if (kafkaConsumer_) {
      kafkaConsumer_->close();
    }
  }
};

class RdKafkaConsumerHolderCached : virtual public RdKafkaConsumerHolder {
private:
  const std::shared_ptr<RdKafka::KafkaConsumer> kafkaConsumer_;
public:
  RdKafkaConsumerHolderCached(std::shared_ptr<RdKafka::KafkaConsumer> kafkaConsumer)
  : kafkaConsumer_(std::move(kafkaConsumer)) {}

  virtual std::shared_ptr<RdKafka::KafkaConsumer> getInstance() override {
    return kafkaConsumer_;
  }

  virtual void resetInstance() override {
    // doNothing();
  }

  virtual void close() override {
    if (kafkaConsumer_) {
      kafkaConsumer_->close();
    }
  }
};

RdKafkaConsumerHolder* RdKafkaConsumerHolderFactory::createInstance(
  const std::unordered_set<uint32_t> &partition_ids,
  const std::string &broker_list,
  const std::unordered_set<std::string> &topic_names,
  const std::string &group_id,
  const std::string &kafka_consumer_type) {
  return new RdKafkaConsumerHolderRenewable(partition_ids, broker_list, topic_names, group_id, kafka_consumer_type);
}

RdKafkaConsumerHolder* RdKafkaConsumerHolderFactory::createInstance(
  std::shared_ptr<RdKafka::KafkaConsumer> consumer) {
  return new RdKafkaConsumerHolderCached(consumer);
}

KafkaConsumer::KafkaConsumer(const std::unordered_set<uint32_t>& partition_ids,
                             const std::string& broker_list,
                             const std::unordered_set<std::string>& topic_names,
                             const std::string& group_id,
                             const std::string& kafka_consumer_type)
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
                             const std::unordered_set<uint32_t>& partition_ids,
                             const std::unordered_set<std::string>& topic_names,
                             const std::string& kafka_consumer_type)
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
  if (consumer_ == nullptr) {
    return;
  }

  if (!FLAGS_kafka_consumer_reset_on_file_change.empty()) {
    cbIdPtr_ = std::make_shared<common::MultiFilePoller::CallbackId>(
      SSLFilePollerHolder::getFilePollerInstance()->registerFile(
        FLAGS_kafka_consumer_reset_on_file_change,
        [&](const common::MultiFilePoller::CallbackArg &newData) {
          reset_.store(true);
        }));
  }

  do {
    while (std::atomic_exchange(&reset_, false)) {
      consumer_->resetInstance();
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
      if (reset_) {
        continue;
      }

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
  } while (reset_);
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
      consumer_->resetInstance();
    }

    if (SeekInternal(topic_names_, timestamp_ms)) {
      return true;
    } else {
      common::Stats::get()->Incr(
        getFullStatsName(kKafkaConsumerErrorSeek,
                         {kafka_consumer_type_metric_tag_}));
      if (reset_) {
        continue;
      }
      return false;
    }
  } while (reset_);
}

bool KafkaConsumer::Seek(const std::string& topic_name,
    const int64_t timestamp_ms) {
  do {
    while (std::atomic_exchange(&reset_, false)) {
      consumer_->resetInstance();
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
      if (reset_) {
        continue;
      }
    }
    return is_success;
  }while (reset_);
}

bool KafkaConsumer::Seek(const std::map<std::string, std::map<int32_t,
    int64_t>>& last_offsets) {
  do {
    while (std::atomic_exchange(&reset_, false)) {
      consumer_->resetInstance();
    }

    if (SeekInternal(last_offsets)) {
      return true;
    }

    common::Stats::get()->Incr(
      getFullStatsName(kKafkaConsumerErrorSeek,
                       {kafka_consumer_type_metric_tag_}));
    if (reset_) {
      continue;
    }
    return false;
  } while (reset_);
}

bool KafkaConsumer::SeekInternal(
    const std::map<std::string, std::map<int32_t, int64_t>>& last_offsets) {
  if (consumer_ == nullptr || consumer_->getInstance() == nullptr) {
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

      if (!KafkaSeekWithRetry(consumer_->getInstance().get(), *topic_partition)) {
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
  for (const auto* topic_partition : tmp_topic_partitions) {
    LOG(INFO) << "Seeking partition: " << topic_partition->partition()
              << ", offset: " << topic_partition->offset();

    common::Stats::get()->Incr(
        getFullStatsName(kKafkaConsumerSeek,
                           {kafka_consumer_type_metric_tag_,
                            "topic=" + topic_partition->topic(),
                            "partition=" +
                            std::to_string(topic_partition->partition())}));
    if (!KafkaSeekWithRetry(consumer_->getInstance().get(), *topic_partition)) {
      return false;
    }
  }
  return true;
}

RdKafka::Message* KafkaConsumer::Consume(int32_t timeout_ms) {
  do {
    if (consumer_ == nullptr || consumer_->getInstance() == nullptr) {
      return nullptr;
    }

    if (std::atomic_exchange(&reset_, false)) {
      consumer_->resetInstance();
      // Seek to last known positive offsets.
      continue;
    }

    auto *message = consumer_->getInstance()->consume(timeout_ms);

    if (message == nullptr) {
      if (std::atomic_exchange(&reset_, false)) {
        consumer_->resetInstance();
        // Seek to last known positive offsets.
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

      if (std::atomic_exchange(&reset_, false)) {
        consumer_->resetInstance();
        // Seek to last known positive offsets.
        continue;
      }
    }
    return message;
  } while (reset_);
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
