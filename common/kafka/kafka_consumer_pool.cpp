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

#include "common/kafka/kafka_consumer_pool.h"

#include <string>
#include <unordered_set>

#include "common/stats/stats.h"
#include "folly/MPMCQueue.h"
#include "librdkafka/rdkafkacpp.h"
#include "librdkafka/rdkafka.h"
#include "common/kafka/stats_enum.h"
#include "common/kafka/kafka_consumer.h"

namespace {

static const std::string kKafkaConsumerPoolGetQueueSize =
    "kafka_consumer_pool_get_queue_size";
static const std::string kKafkaConsumerPoolPutQueueSize =
    "kafka_consumer_pool_put_queue_size";

}  // namespace

namespace kafka {

KafkaConsumerPool::KafkaConsumerPool(
    uint32_t queue_size,
    const std::unordered_set<uint32_t>& partition_ids,
    const std::string& broker_list,
    const std::unordered_set<std::string>& topic_names,
    const std::string& group_id,
    const std::string& kafka_consumer_type)
    : queue_(std::make_unique<KafkaConsumerQueue>(queue_size)),
      kafka_consumer_type_metric_tag_("kafka_consumer_type="
        + kafka_consumer_type),
      partition_ids_(partition_ids),
      broker_list_(broker_list),
      topic_names_(topic_names),
      group_id_(group_id),
      kafka_consumer_type_(kafka_consumer_type) {
  for (uint32_t i = 0; i < queue_size; ++i) {
    queue_->write(std::make_shared<KafkaConsumer>(
        partition_ids,
        broker_list,
        topic_names,
        group_id,
        kafka_consumer_type));
  }
}

std::shared_ptr<KafkaConsumer> KafkaConsumerPool::Get() {
  common::Stats::get()->AddMetric(getFullMetricName(
      kKafkaConsumerPoolGetQueueSize, {kafka_consumer_type_metric_tag_}),
          queue_->size());

  // Try to get consumer from queue, otherwise return new one.
  std::shared_ptr<KafkaConsumer> consumer;
  if (queue_->read(consumer)) {
    return consumer;
  }
  return std::make_shared<KafkaConsumer>(
      partition_ids_,
      broker_list_,
      topic_names_,
      group_id_,
      kafka_consumer_type_);
}

void KafkaConsumerPool::Put(std::shared_ptr<KafkaConsumer> consumer) {
  common::Stats::get()->AddMetric(getFullMetricName(
      kKafkaConsumerPoolPutQueueSize, {kafka_consumer_type_metric_tag_}),
          queue_->size());

  queue_->write(std::move(consumer));
}

}  // namespace kafka
