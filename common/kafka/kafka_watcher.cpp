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

#include "common/kafka/kafka_watcher.h"

#include <algorithm>
#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "boost/functional/hash.hpp"
#include "common/stats/stats.h"
#include "common/timeutil.h"
#include "folly/String.h"
#include "glog/logging.h"
#include "librdkafka/rdkafkacpp.h"
#include "common/kafka/stats_enum.h"
#include "common/kafka/kafka_consumer.h"
#include "common/kafka/kafka_consumer_pool.h"

namespace {

typedef std::unordered_set<std::pair<std::string, int32_t>,
                           boost::hash<std::pair<std::string, int32_t>>>
    TopicPartitionSet;
template <typename V>
using TopicPartitionToValueMap = std::
    unordered_map<std::pair<std::string, int32_t>, V,
    boost::hash<std::pair<std::string, int32_t>>>;

std::string TopicPartitionSetToString(
    const TopicPartitionSet& topic_partition_set) {
  std::string s;
  for (const auto& pair : topic_partition_set) {
    s.append("(").append(pair.first).append(",").
      append(std::to_string(pair.second)).append(") ");
  }
  return s;
}

void VerifyAndUpdateTopicPartitionOffset(
    TopicPartitionToValueMap<int64_t>* topic_partition_to_prev_offset_map,
    const std::string& topic_name,
    const int32_t partition_id,
    const int64_t offset,
    const std::string& watcher_name) {
  CHECK_NOTNULL(topic_partition_to_prev_offset_map);

  const auto topic_partition_pair = std::make_pair(topic_name, partition_id);
  auto it = topic_partition_to_prev_offset_map->find(topic_partition_pair);
  if (it == topic_partition_to_prev_offset_map->end()) {
    CHECK(topic_partition_to_prev_offset_map->emplace(topic_partition_pair,
        offset).second);
  } else {
    const auto prev_offset = it->second;
    if (prev_offset + 1 != offset) {
      if (prev_offset + 1 < offset) {
        common::Stats::get()->Incr(getFullStatsName(
          kKafkaWatcherMessageMissing, {topic_name}));
      } else if (prev_offset +1 > offset) {
        common::Stats::get()->Incr(getFullStatsName(
          kKafkaWatcherMessageDuplicates, {topic_name}),
            (prev_offset - offset + 1));
      }
    }
    it->second = offset;
  }
}

inline int64_t GetMessageTimestamp(const RdKafka::Message& message) {
  const auto ts = message.timestamp();
  if (ts.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME) {
    return ts.timestamp;
  }

  // We only expect the timestamp to be create time.
  return -1;
}

}  // namespace

KafkaWatcher::KafkaWatcher(const std::string& name,
                           std::shared_ptr<kafka::KafkaConsumerPool>
                               kafka_consumer_pool,
                           int kafka_init_blocking_consume_timeout_ms,
                           int kafka_consumer_timeout_ms,
                           int loop_cycle_limit_ms)
    : KafkaWatcher(
          name,
          std::vector<std::shared_ptr<kafka::KafkaConsumerPool>>{
            std::move(kafka_consumer_pool)},
          kafka_init_blocking_consume_timeout_ms,
          kafka_consumer_timeout_ms,
          loop_cycle_limit_ms) {}

KafkaWatcher::KafkaWatcher(
    const std::string& name,
    const std::vector<std::shared_ptr<kafka::KafkaConsumerPool>>&
      kafka_consumer_pools,
    int kafka_init_blocking_consume_timeout_ms,
    int kafka_consumer_timeout_ms,
    int loop_cycle_limit_ms)
    : kafka_watcher_metric_tag_("kafka_watcher_name=" + name),
      name_(name),
      is_stopped_(false),
      kafka_consumer_pools_(kafka_consumer_pools),
      kafka_init_blocking_consume_timeout_ms_(
          kafka_init_blocking_consume_timeout_ms),
      kafka_consumer_timeout_ms_(kafka_consumer_timeout_ms),
      loop_cycle_limit_ms_(loop_cycle_limit_ms) {
  // Instantiate consumers from pools
  for (auto pool = kafka_consumer_pools_.begin(); pool !=
       kafka_consumer_pools_.end(); pool++) {
    CHECK(*pool != nullptr);
    const auto& kafka_consumer = (*pool)->Get();
    kafka_consumers_.push_back(kafka_consumer);
  }
}

KafkaWatcher::~KafkaWatcher() {
  for (int i = 0; i < kafka_consumer_pools_.size(); i++) {
    const auto& pool = kafka_consumer_pools_[i];
    const auto& consumer = kafka_consumers_[i];
    pool->Put(consumer);
  }
}

bool KafkaWatcher::InitKafkaConsumerSeek(
    kafka::KafkaConsumer* const kafka_consumer,
    int64_t initial_kafka_seek_timestamp_ms) {
  if (initial_kafka_seek_timestamp_ms == -1) {
    LOG(INFO) << name_ << ": Initial kafka seek timestamp ms not specified,"
                          " skipping initial seek";
    return true;
  }

  CHECK_NOTNULL(kafka_consumer);
  for (const auto& topic_name : kafka_consumer->GetTopicNames()) {
    LOG(INFO) << name_ << ": Seeking kafka consumer";
    if (!kafka_consumer->Seek(topic_name, initial_kafka_seek_timestamp_ms)) {
      LOG(ERROR) << name_ << ": Kafka seek failed; Will not watch and ingest"
                             " new documents";
      return false;
    }

    LOG(INFO) << name_ << ": Successfully seeked kafka topic_name: "
              << topic_name << " to timestamp_ms: "
              << initial_kafka_seek_timestamp_ms;
  }
  return true;
}

uint32_t KafkaWatcher::ConsumeUpToNow(kafka::KafkaConsumer& consumer) {
  // timestamps dealt with in this function are all ms.
  uint32_t num_msg_consumed = 0;
  const auto& topic_names = consumer.GetTopicNames();
  const auto& partition_ids = consumer.GetPartitionIds();
  // amt of topic names in which we have consumed "enough" messages.
  const auto num_topic_partitions = topic_names.size() * partition_ids.size();
  TopicPartitionSet finished_topic_partitions;
  finished_topic_partitions.reserve(num_topic_partitions);
  TopicPartitionToValueMap<int64_t> topic_partition_to_prev_offset;
  topic_partition_to_prev_offset.reserve(topic_names.size());
  TopicPartitionToValueMap<uint32_t> topic_partition_to_message_num;
  topic_partition_to_message_num.reserve(topic_names.size());
  auto timeout_timestamp_ms = common::timeutil::GetCurrentTimestamp(
      common::timeutil::TimeUnit::kMillisecond) +
      kafka_init_blocking_consume_timeout_ms_;

  // End the loop when finding fresher message or hitting partition end for
  // all the topics, or it takes longer than the allowed time limit.
  while (!is_stopped_.load() && finished_topic_partitions.size() !=
         num_topic_partitions) {
    const auto message =
        std::shared_ptr<RdKafka::Message>(
            consumer.Consume(kafka_consumer_timeout_ms_));
    if (message == nullptr) {
      // This should only happen if kafka consumer is unhealthy.
      break;
    }
    if (message->err() == RdKafka::ERR_NO_ERROR) {
      const auto msg_timestamp_ms = GetMessageTimestamp(*message);
      const auto time_now_ms = common::timeutil::GetCurrentTimestamp(
          common::timeutil::TimeUnit::kMillisecond);
      common::Stats::get()->AddMetric(
          getFullStatsName(kKafkaMsgTimeDiffFromCurrMs,
              {kafka_watcher_metric_tag_}),
          time_now_ms - msg_timestamp_ms);
      common::Stats::get()->AddMetric(
          getFullStatsName(kKafkaMsgNumBytes, {kafka_watcher_metric_tag_}),
          message->len());
      const auto topic_partition_pair = std::make_pair(message->topic_name(),
          message->partition());
      HandleKafkaNoErrorMessage(message, true /* replay */);
      auto it = topic_partition_to_message_num.find(topic_partition_pair);
      if (it == topic_partition_to_message_num.end()) {
        CHECK(topic_partition_to_message_num.emplace(topic_partition_pair,
            1).second);
      } else {
        it->second++;
      }

      // Check if messages are missing between current and previous offset
      VerifyAndUpdateTopicPartitionOffset(&topic_partition_to_prev_offset,
                                          message->topic_name(),
                                          message->partition(),
                                          message->offset(),
                                          name_);
    } else if (message->err() == RdKafka::ERR__PARTITION_EOF) {
      // Reached the end of the topic+partition queue on the broker.
      finished_topic_partitions.emplace(message->topic_name(),
          message->partition());
    } else if (message->err() == RdKafka::ERR__TIMED_OUT) {
      // This could happen even before getting ERR__PARTITION_EOF message. It
      // happens when consumer hasn't got any message from the broker within the
      // timeout. We should retry in this case.
    } else {
      // Abort the initialization when receiving an unexpected error
      // TODO: We probably need to re-establish Kafka connection here..
      break;
    }
    ++num_msg_consumed;
    if (num_msg_consumed % 100 == 0) {
      if (kafka_init_blocking_consume_timeout_ms_ != -1 &&
          common::timeutil::GetCurrentTimestamp(
              common::timeutil::TimeUnit::kMillisecond) >
          timeout_timestamp_ms) {
        common::Stats::get()->Incr(
            getFullStatsName(kKafkaWatcherBlockingConsumeTimeout,
                {kafka_watcher_metric_tag_}));
        LOG(ERROR) << name_
                   << ": Could not reach the end of partitions: "
                   << consumer.partition_ids_str_
                   << " for all topics: " << folly::join(", ", topic_names)
                   << ". Finished topics partitions: "
                   << TopicPartitionSetToString(finished_topic_partitions)
                   << ". Current timeout is: "
                   << kafka_init_blocking_consume_timeout_ms_ << " ms";
        break;
      }
    }
  }

  std::string s;
  for (const auto& pair : topic_partition_to_message_num) {
    const auto& topic_name = pair.first.first;
    const auto partition_id = pair.first.second;
    s += "(" + topic_name + "," + std::to_string(partition_id) +
         "):" + std::to_string(pair.second) + " ";
  }
  LOG(INFO) << name_ << ": Messages consumed per topic partition: " << s;
  return num_msg_consumed;
}

void KafkaWatcher::StartWith(int64_t initial_kafka_seek_timestamp_ms,
    KafkaMessageHandler handler) {
  CHECK(handler != nullptr);
  handler_ = handler;
  Start(initial_kafka_seek_timestamp_ms);
}

void KafkaWatcher::StartWith(const std::map<std::string, std::map<int32_t,
                             int64_t>>& last_offsets,
                             KafkaMessageHandler handler) {
  CHECK(handler != nullptr);
  handler_ = handler;
  Start(last_offsets);
}

// TODO: merge following two Start functions.
void KafkaWatcher::Start(const std::map<std::string, std::map<int32_t,
    int64_t>>& last_offsets) {
  const auto start_timestamp_ms = common::timeutil::GetCurrentTimestamp(
      common::timeutil::TimeUnit::kMillisecond);
  if (!Init()) {
    return;
  }
  for (const auto& kafka_consumer : kafka_consumers_) {
    const auto& topic_names = kafka_consumer->GetTopicsString();
    if (kafka_consumer != nullptr && kafka_consumer->IsHealthy()) {
      if (!kafka_consumer->Seek(last_offsets)) {
        LOG(ERROR) << name_ << ": Kafka seek failed with consumer for topics:"
                   << topic_names << "Will not watch and ingest new documents";
        return;
      }

      if (kafka_init_blocking_consume_timeout_ms_ != 0) {
        // Synchronously consume the messages up to the current
        // timestamp from Kafka.
        const auto num = ConsumeUpToNow(*kafka_consumer);
        LOG(INFO) << name_ << ": Finished consuming " << num
                  << " messages from Kafka consumer"
                  << "for topics:" << topic_names;
      }
    } else {
      LOG(INFO) << name_
                << ": Skipped consuming from kafka consumer for topics:"
                << topic_names << "because kafka consumer is not healthy";
    }
  }
  const auto duration_ms =
      common::timeutil::GetCurrentTimestamp(
          common::timeutil::TimeUnit::kMillisecond)
      - start_timestamp_ms;
  common::Stats::get()->AddMetric(
      getFullStatsName(kKafkaWatcherInitMs, {kafka_watcher_metric_tag_}),
      duration_ms);
  LOG(INFO) << name_ << ": Finished init watcher in: "
            << duration_ms << "ms. Starting watch loop";
  // We always spawn the watcher thread even though the kafka consumer is
  // unhealthy, because derived class could potentially watch data from
  // other sources, e.g. S3
  thread_ = std::thread(&KafkaWatcher::StartWatchLoop, this);
}

void KafkaWatcher::Start(int64_t initial_kafka_seek_timestamp_ms) {
  const auto start_timestamp_ms = common::timeutil::GetCurrentTimestamp(
      common::timeutil::TimeUnit::kMillisecond);
  if (!Init()) {
    return;
  }
  for (const auto& kafka_consumer : kafka_consumers_) {
    const auto& topic_names = kafka_consumer->GetTopicsString();
    if (kafka_consumer != nullptr && kafka_consumer->IsHealthy()) {
      if (!InitKafkaConsumerSeek(kafka_consumer.get(),
          initial_kafka_seek_timestamp_ms)) {
        return;
      }

      if (kafka_init_blocking_consume_timeout_ms_ != 0) {
        // Synchronously consume the messages up to the current timestamp
        // from Kafka.
        const auto num = ConsumeUpToNow(*kafka_consumer);
        LOG(INFO) << name_ << ": Finished consuming " << num
                  << " messages from Kafka consumer"
                  << "for topics:" << topic_names;
      }
    } else {
      LOG(INFO) << name_
                << ": Skipped consuming from kafka consumer for topics:"
                << topic_names
                << "because kafka consumer is not healthy";
    }
  }
  const auto duration_ms =
      common::timeutil::GetCurrentTimestamp(
          common::timeutil::TimeUnit::kMillisecond)
      - start_timestamp_ms;
  common::Stats::get()->AddMetric(
      getFullStatsName(kKafkaWatcherInitMs, {kafka_watcher_metric_tag_}),
      duration_ms);
  LOG(INFO) << name_ << ": Finished init watcher in: "
            << duration_ms << "ms. Starting watch loop";
  // We always spawn the watcher thread even though the kafka consumer is
  // unhealthy, because derived class could potentially watch data from other
  // sources, e.g. S3
  thread_ = std::thread(&KafkaWatcher::StartWatchLoop, this);
}

void KafkaWatcher::StartWatchLoop() {
  uint64_t cycle_end_timestamp_ms;

  // ensure all consunmers get
  while (!is_stopped_.load()) {
    cycle_end_timestamp_ms =
        common::timeutil::GetCurrentTimestamp(
            common::timeutil::TimeUnit::kMillisecond)
        + loop_cycle_limit_ms_;
    // 1) Pre-processing before kafka operations
    WatchLoopIterationStart();

    // 2) For each consumer, adjust seek if needed
    for (const auto& kafka_consumer : kafka_consumers_) {
      MaybeAdjustKafkaConsumerSeek(kafka_consumer.get());
    }
    while (!is_stopped_.load() && common::timeutil::GetCurrentTimestamp(
        common::timeutil::TimeUnit::kMillisecond) <=
        cycle_end_timestamp_ms) {
      // Round robin message consumption for each consumer
      for (const auto& kafka_consumer : kafka_consumers_) {
        // 3) Consume and apply the kafka updates
        // kafka consumption uses whatever time budget is left for this cycle.
        if (kafka_consumer != nullptr && kafka_consumer->IsHealthy()) {
          const auto& topic_names = kafka_consumer->GetTopicNames();
          TopicPartitionToValueMap<int64_t> topic_partition_to_prev_offset;
          topic_partition_to_prev_offset.reserve(topic_names.size());
          const auto message = std::shared_ptr<const RdKafka::Message>(
              kafka_consumer->Consume(kafka_consumer_timeout_ms_));
          if (message == nullptr) {
            continue;
          }
          if (message->err() == RdKafka::ERR_NO_ERROR) {
            const auto msg_timestamp_ms = GetMessageTimestamp(*message);
            const auto time_now_ms = common::timeutil::GetCurrentTimestamp(
                common::timeutil::TimeUnit::kMillisecond);
            common::Stats::get()->AddMetric(
                getFullStatsName(kKafkaMsgTimeDiffFromCurrMs,
                    {kafka_watcher_metric_tag_}),
                time_now_ms - msg_timestamp_ms);
            common::Stats::get()->AddMetric(
                getFullStatsName(kKafkaMsgNumBytes,
                    {kafka_watcher_metric_tag_}), message->len());
            HandleKafkaNoErrorMessage(message, false /* not replay */);
            // Check if messages are missing between current and previous offset
            VerifyAndUpdateTopicPartitionOffset(&topic_partition_to_prev_offset,
                                                message->topic_name(),
                                                message->partition(),
                                                message->offset(),
                                                name_);
          } else if (message->err() == RdKafka::ERR__TIMED_OUT ||
                     message->err() == RdKafka::ERR__PARTITION_EOF) {
            // ERR__PARTITION_EOF: Reached the end of the topic+partition queue
            // on the broker. Not really an error. ERR__TIMED_OUT: timeout due
            // to no message or event. This happens after
            // receiving ERR__PARTITION_EOF and there was still no messages to
            // be consumed after timeout.
          } else {
            // TODO: We probably need to re-establish Kafka connection here..
            // Sleep here to prevent potential busy loop which exhausts the CPU.
            if (!is_stopped_.load()) {
              std::this_thread::sleep_for(
                  std::chrono::milliseconds(kafka_consumer_timeout_ms_));
            }
          }
        }
      }
    }
    // 4) Post-processing after kafka operations
    WatchLoopIterationEnd();
    // Sleep here to prevent while(true) loop from exhausting CPU.
    auto now_ms = common::timeutil::GetCurrentTimestamp(
        common::timeutil::TimeUnit::kMillisecond);
    // TODO: Implement an interruptible sleep (e.g. using a condition variable)
    // so we can exit immediately once stop is called
    if (!is_stopped_.load() && now_ms < cycle_end_timestamp_ms) {
      std::this_thread::sleep_for(std::chrono::milliseconds(
          cycle_end_timestamp_ms - now_ms));
    }
  }
  LOG(INFO) << name_ << ": StartWatchLoop has ended";
}
