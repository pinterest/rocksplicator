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

#include <algorithm>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "librdkafka/rdkafkacpp.h"
#include "common/kafka/tests/mock_kafka_cluster.h"
#include "common/kafka/tests/mock_kafka_consumer.h"

class MockKafkaConsumerTest : public ::testing::Test {
protected:
  void SetUp() override {
    mock_kafka_cluster_ = std::make_shared<::kafka::MockKafkaCluster>();

    topic_names_ = {"topic0", "topic1", "topic2", "topic3"};
    partition_ids_ = {1, 2, 3, 4, 5, 10};
    records_ = {{"a", 2}, {"b", 4}, {"c", 8}, {"d", 16}, {"e", 32}, {"f", 64}, {"g", 128}};

    for (auto& topic_name : topic_names_) {
      for (auto partition_id : partition_ids_) {
        for (auto& record : records_) {
          mock_kafka_cluster_->AddRecord(
              topic_name, partition_id, record.payload, record.timestamp_ms);
        }
      }
    }
  }

  std::shared_ptr<::kafka::MockKafkaCluster> mock_kafka_cluster_;
  std::vector<std::string> topic_names_;
  std::vector<int32_t> partition_ids_;
  std::vector<::kafka::MockKafkaCluster::Record> records_;
};

TEST_F(MockKafkaConsumerTest, TestSingleTopic) {
  RdKafka::MockKafkaConsumer consumer(mock_kafka_cluster_);

  // Set up TopicPartition
  const int32_t partition_id = 4;
  const std::string topic_name = "topic0";
  const auto topic_partition = std::unique_ptr<RdKafka::TopicPartition>(
      RdKafka::TopicPartition::create(topic_name, partition_id, RdKafka::Topic::OFFSET_END));

  consumer.assign({topic_partition.get()});

  // Seek to timestamp
  const auto seek_timestamp_ms = 16;
  topic_partition->set_offset(seek_timestamp_ms);
  std::vector<RdKafka::TopicPartition*> offsets{topic_partition.get()};
  consumer.offsetsForTimes(offsets, -1 /* timeout_ms */);
  ASSERT_EQ(3, topic_partition->offset());
  consumer.seek(*topic_partition, -1 /* timeout_ms */);

  int offset = 3;
  RdKafka::Message* message;
  for (size_t completed_topic_partitions = 0; completed_topic_partitions < 1;) {
    message = consumer.consume(-1 /* timeout_ms */);
    if (message->err() == RdKafka::ERR__PARTITION_EOF) {
      completed_topic_partitions++;
      continue;
    }

    // Since there's only one topic all consumed messages should be in the same order as the records
    EXPECT_EQ(RdKafka::ERR_NO_ERROR, message->err());
    EXPECT_EQ(records_[offset].payload, std::string(static_cast<char*>(message->payload())));
    EXPECT_EQ(RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME, message->timestamp().type);
    EXPECT_EQ(records_[offset].timestamp_ms, message->timestamp().timestamp);
    EXPECT_EQ(offset, message->offset());
    EXPECT_EQ(topic_name, message->topic_name());
    EXPECT_EQ(partition_id, message->partition());

    offset++;
  }

  EXPECT_EQ(records_.size(), offset);
}

TEST_F(MockKafkaConsumerTest, TestMultipleTopicPartitions) {
  const std::vector<int32_t> partition_ids{1, 2, 10};
  const std::unordered_set<std::string> topic_names({"topic0", "topic2"});
  RdKafka::MockKafkaConsumer consumer(mock_kafka_cluster_);

  // Set up TopicPartitions
  std::vector<std::unique_ptr<RdKafka::TopicPartition>> topic_partitions;
  std::vector<RdKafka::TopicPartition*> tmp_topic_partitions;
  for (const auto partition_id : partition_ids) {
    for (const auto& topic_name : topic_names) {
      auto topic_partition = std::unique_ptr<RdKafka::TopicPartition>(
          RdKafka::TopicPartition::create(topic_name, partition_id, RdKafka::Topic::OFFSET_END));
      tmp_topic_partitions.push_back(topic_partition.get());
      topic_partitions.push_back(std::move(topic_partition));
    }
  }

  consumer.assign(tmp_topic_partitions);

  // Seek to timestamp
  const auto seek_timestamp_ms = 63;
  for (auto* topic_partition : tmp_topic_partitions) {
    topic_partition->set_offset(seek_timestamp_ms);
  }
  consumer.offsetsForTimes(tmp_topic_partitions, -1 /* timeout_ms */);
  for (const auto* topic_partition : tmp_topic_partitions) {
    ASSERT_EQ(5, topic_partition->offset());
    consumer.seek(*topic_partition, -1 /* timeout_ms */);
  }

  std::vector<std::tuple<int32_t, std::string, std::string, int64_t>> expected_results{
      std::make_tuple(1, "topic0", "f", 64),
      std::make_tuple(1, "topic0", "g", 128),
      std::make_tuple(1, "topic2", "f", 64),
      std::make_tuple(1, "topic2", "g", 128),
      std::make_tuple(2, "topic0", "f", 64),
      std::make_tuple(2, "topic0", "g", 128),
      std::make_tuple(2, "topic2", "f", 64),
      std::make_tuple(2, "topic2", "g", 128),
      std::make_tuple(10, "topic0", "f", 64),
      std::make_tuple(10, "topic0", "g", 128),
      std::make_tuple(10, "topic2", "f", 64),
      std::make_tuple(10, "topic2", "g", 128)};

  size_t num_records = 0;
  RdKafka::Message* message;
  for (size_t completed_topic_partitions = 0; completed_topic_partitions < 6;) {
    message = consumer.consume(-1 /* timeout_ms */);
    if (message->err() == RdKafka::ERR__PARTITION_EOF) {
      completed_topic_partitions++;
      continue;
    }

    EXPECT_EQ(RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME, message->timestamp().type);
    std::string str_payload = std::string(static_cast<char*>(message->payload()));
    std::tuple<int32_t, std::string, std::string, int64_t> result = std::make_tuple(
        message->partition(), message->topic_name(), str_payload, message->timestamp().timestamp);
    EXPECT_TRUE(std::find(expected_results.begin(), expected_results.end(), result) !=
                expected_results.end());
    num_records++;
  }

  EXPECT_EQ(expected_results.size(), num_records);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
