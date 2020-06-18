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

#include "common/kafka/kafka_consumer_holder.h"
#include "common/kafka/kafka_config.h"
#include "common/kafka/kafka_flags.h"
#include "common/kafka/kafka_utils.h"

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

namespace kafka {

std::shared_ptr<RdKafka::KafkaConsumer> CreateRdKafkaConsumer(
  // https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
  const KafkaConfigMap &config,
  const std::unordered_set<uint32_t> &partition_ids,
  const std::string kafka_consumer_type) {
  std::string err;
  auto conf = std::shared_ptr<RdKafka::Conf>(RdKafka::Conf::create(
    RdKafka::Conf::CONF_GLOBAL));

  // Next go through each of the GLOBAL config and set.
  for (KafkaConfigMap::const_iterator it = config.begin(); it != config.end(); ++it) {
    const std::string &key = it->first;
    const std::pair<std::string, bool> &value = it->second;

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
  const std::unordered_set<uint32_t> &partition_ids,
  const std::string &broker_list,
  const std::string &group_id,
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

class RdKafkaConsumerHolderRenewable : virtual public RdKafkaConsumerHolder {
private:
  const std::unordered_set<uint32_t> partition_ids_;
  const std::string broker_list_;
  const std::unordered_set<std::string> topic_names_;
  const std::string group_id_;
  const std::string kafka_consumer_type_;

  std::shared_ptr<RdKafka::KafkaConsumer> kafkaConsumer_;

public:
  RdKafkaConsumerHolderRenewable(
    const std::unordered_set<uint32_t> &partition_ids,
    const std::string &broker_list,
    const std::unordered_set<std::string> &topic_names,
    const std::string &group_id,
    const std::string &kafka_consumer_type) :
    partition_ids_(partition_ids),
    broker_list_(broker_list),
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

  void syncPreviousInstance() {
    LOG(INFO) << "Commit syncing kafka consumer for topic: "
              << folly::join(",", topic_names_)
              << ", partitions: "
              << folly::join(",", partition_ids_)
              << std::endl << std::flush;

    if (kafkaConsumer_) {
      kafkaConsumer_->commitSync();
    }
  }

  void closePreviousInstance() {
    LOG(INFO) << "Closing kafka consumer for topic: "
              << folly::join(",", topic_names_)
              << ", partitions: "
              << folly::join(",", partition_ids_)
              << std::endl << std::flush;

    if (kafkaConsumer_) {
      kafkaConsumer_->close();
    }
  }

  void recreateInstance() {
    LOG(INFO) << "Recreating kafka consumer for topic: "
              << folly::join(",", topic_names_)
              << ", partitions: "
              << folly::join(",", partition_ids_)
              << std::endl << std::flush;

    kafkaConsumer_ = CreateRdKafkaConsumer(
      partition_ids_,
      broker_list_,
      group_id_,
      kafka_consumer_type_);

    LOG(INFO) << "Recreated kafka consumer for topic: "
              << folly::join(",", topic_names_)
              << ", partitions: "
              << folly::join(",", partition_ids_)
              << std::endl << std::flush;
  }

  bool assignPartitions() {
    /**
     * First assign the topic partitions to current kafka consumer.
     */
    // A temporary RdKafka::TopicPartition* vector specifically used
    // for KafkaConsumer->assign(). The pointers inside this vector is
    // owned by `topic_partitions`.

    LOG(INFO) << "Assigning kafka consumer for topic: "
              << folly::join(",", topic_names_)
              << ", partitions: "
              << folly::join(",", partition_ids_)
              << std::endl << std::flush;

    std::vector<std::shared_ptr<RdKafka::TopicPartition>> topic_partitions;
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
      }
    }
    const auto error_code = ExecuteKafkaOperationWithRetry(
      [this, &tmp_topic_partitions]() {
        return kafkaConsumer_->assign(tmp_topic_partitions);
      });

    if (error_code != RdKafka::ERR_NO_ERROR) {
      LOG(INFO) << "Failed to assign kafka consumer for topic: "
                << folly::join(",", topic_names_)
                << ", partitions: "
                << folly::join(",", partition_ids_)
                << std::endl << std::flush;

      return false;
    }

    LOG(INFO) << "Assigned kafka consumer for topic: "
              << folly::join(",", topic_names_)
              << ", partitions: "
              << folly::join(",", partition_ids_)
              << std::endl << std::flush;

    return true;
  }

  bool seekToRelevantOffsets(const TopicPartitionToValueMap<ConsumedOffset> *consumedOffsets) {
    LOG(INFO) << "Seeking kafka consumer to relevant Offsets for topic: "
              << folly::join(",", topic_names_)
              << ", partitions: "
              << folly::join(",", partition_ids_)
              << std::endl << std::flush;
    /**
     * Now seek individual partitions to offsets of last seen messages
     * For individual partitions, seek to timestamp, if offset not available.
     */
    for (const auto &consumedOffset : *consumedOffsets) {
      const std::string &topic_name = consumedOffset.first.first;
      int32_t partition = partition = consumedOffset.first.second;

      std::shared_ptr<RdKafka::TopicPartition> topic_partition;

      if (consumedOffset.second.offset != -1) {
        /**
         * Since committed offset is available, Seek to this offset directly.
         */
        LOG(INFO) << "Seeking kafka consumer based on offset for topic: "
                  << consumedOffset.first.first
                  << ", partition: " << consumedOffset.first.second
                  << ", offset: " << consumedOffset.second.offset
                  << std::endl << std::flush;

        topic_partition = std::shared_ptr<RdKafka::TopicPartition>(
          RdKafka::TopicPartition::create(
            topic_name,
            partition,
            consumedOffset.second.offset));

        if (!KafkaSeekWithRetry(kafkaConsumer_.get(), *topic_partition)) {
          return false;
        }
      } else if (consumedOffset.second.timestamp != -1) {
        // Since the consumed offset is not available (e.g. no message was yet retrieved)
        // In this case, try to find the first offset with timestamp greater than or equal
        // to given timestamp.
        LOG(INFO) << "Seeking based on timestamp for topic: " << consumedOffset.first.first
                  << ", partition: " << consumedOffset.first.second
                  << ", timestamp: " << consumedOffset.second.timestamp
                  << std::endl << std::flush;

        topic_partition = std::shared_ptr<RdKafka::TopicPartition>(
          RdKafka::TopicPartition::create(
            topic_name,
            partition,
            consumedOffset.second.timestamp));

        std::vector<RdKafka::TopicPartition *> tmp_topic_partitions;
        tmp_topic_partitions.push_back(topic_partition.get());

        auto error_code = ExecuteKafkaOperationWithRetry(
          [this, &tmp_topic_partitions]() {
            return kafkaConsumer_->offsetsForTimes(tmp_topic_partitions,
                                                   FLAGS_kafka_consumer_timeout_ms);
          });

        if (error_code != RdKafka::ERR_NO_ERROR) {
          return false;
        }

        LOG(INFO) << "Successfuly found offset for timestamp for topic: " << consumedOffset.first.first
                  << ", partition: " << consumedOffset.first.second
                  << ", timestamp: " << consumedOffset.second.timestamp
                  << ", offSet: " << topic_partition.get()->offset()
                  << std::endl << std::flush;


        if (!KafkaSeekWithRetry(kafkaConsumer_.get(), *topic_partition)) {
          return false;
        }
      } else {
        LOG(ERROR) << "Incorrect state for consumed offset for topic: " << consumedOffset.first.first
                   << ", partition: " << consumedOffset.first.second
                   << ", timestamp: " << consumedOffset.second.timestamp
                   << ", offSet: " << consumedOffset.second.offset
                   << std::endl << std::flush;
        return false;
      }

      LOG(INFO) << "Successfuly seeked to offset for topic: " << consumedOffset.first.first
                << ", partition: " << consumedOffset.first.second
                << ", timestamp: " << consumedOffset.second.timestamp
                << ", offSet: " << topic_partition.get()->offset()
                << std::endl << std::flush;
    }
    return true;
  }

  virtual void resetInstance(const TopicPartitionToValueMap<ConsumedOffset> *consumedOffsets) override {
    syncPreviousInstance();
    /*
     * Before creating a new KafkaConsumer instance, make sure old one is closed
     * and shutdown.
     */
    bool done = false;
    do {
      closePreviousInstance();

      recreateInstance();

      std::this_thread::sleep_for(
        std::chrono::milliseconds(1000));

      /**
       * If there are no consumedOffsets, return;
       */
      if (consumedOffsets == nullptr) {
        return;
      }

      if (!assignPartitions()) {
        continue;
      }

      if (!seekToRelevantOffsets(consumedOffsets)) {
        continue;
      }
      done = true;
    } while (!done);
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

  virtual void resetInstance(const TopicPartitionToValueMap<ConsumedOffset> *consumedOffsets) override {
    // doNothing();
  }

  virtual void close() override {
    if (kafkaConsumer_) {
      kafkaConsumer_->close();
    }
  }
};

RdKafkaConsumerHolder *RdKafkaConsumerHolderFactory::createInstance(
  const std::unordered_set<uint32_t> &partition_ids,
  const std::string &broker_list,
  const std::unordered_set<std::string> &topic_names,
  const std::string &group_id,
  const std::string &kafka_consumer_type) {
  return new RdKafkaConsumerHolderRenewable(partition_ids, broker_list, topic_names, group_id, kafka_consumer_type);
}

RdKafkaConsumerHolder *RdKafkaConsumerHolderFactory::createInstance(
  std::shared_ptr<RdKafka::KafkaConsumer> consumer) {
  return new RdKafkaConsumerHolderCached(consumer);
}
} // namespace kafka