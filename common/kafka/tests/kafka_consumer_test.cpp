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
#include <string>
#include <tuple>
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"
#include "librdkafka/rdkafkacpp.h"
#include "common/kafka/tests/mock_kafka_cluster.h"
#include "common/kafka/tests/mock_kafka_consumer.h"

namespace kafka {

class KafkaConsumerTest : public ::testing::Test {
protected:
  void SetUp() override {
    mock_kafka_cluster_ = std::make_shared<MockKafkaCluster>();

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

  std::shared_ptr<MockKafkaCluster> mock_kafka_cluster_;
  std::vector<std::string> topic_names_;
  std::vector<int32_t> partition_ids_;
  std::vector<MockKafkaCluster::Record> records_;
};

TEST_F(KafkaConsumerTest, TestSingleTopic) {
  const int32_t partition_id = 4;
  const std::string topic_name = "topic0";

  KafkaConsumer kafka_consumer(std::make_shared<RdKafka::MockKafkaConsumer>(mock_kafka_cluster_),
                               std::unordered_set<uint32_t>({partition_id}),
                               std::unordered_set<std::string>({topic_name}),
                               "UnitTestKafkaConsumer");

  ASSERT_TRUE(kafka_consumer.Seek(topic_name, 16 /* timestamp_ms */));

  int offset = 3;
  RdKafka::Message* message;
  for (size_t completed_topic_partitions = 0; completed_topic_partitions < 1;) {
    message = kafka_consumer.Consume(-1 /* timeout_ms */);
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

TEST_F(KafkaConsumerTest, TestMultipleTopicPartitions) {
  const std::unordered_set<uint32_t> partition_ids{1, 2, 10};
  const std::unordered_set<std::string> topic_names({"topic0", "topic2"});

  KafkaConsumer kafka_consumer(std::make_shared<RdKafka::MockKafkaConsumer>(mock_kafka_cluster_),
                               partition_ids,
                               topic_names,
                               "UnitTestKafkaConsumer");

  // Seek to timestamp
  ASSERT_TRUE(kafka_consumer.Seek(63 /* timestamp_ms */));

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
    message = kafka_consumer.Consume(-1 /* timeout_ms */);
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

TEST_F(KafkaConsumerTest, TestMultipleTopicPartitionsDiffSeekTimes) {
  const std::unordered_set<uint32_t> partition_ids{1, 2, 10};
  const std::unordered_set<std::string> topic_names({"topic0", "topic2"});

  KafkaConsumer kafka_consumer(std::make_shared<RdKafka::MockKafkaConsumer>(mock_kafka_cluster_),
                               partition_ids,
                               topic_names,
                               "UnitTestKafkaConsumer");

  // Seek to timestamp
  ASSERT_TRUE(kafka_consumer.Seek("topic0", 32 /* timestamp_ms */));
  ASSERT_TRUE(kafka_consumer.Seek("topic2", 64 /* timestamp_ms */));

  {
    std::vector<std::tuple<int32_t, std::string, std::string, int64_t>> expected_results{
        std::make_tuple(1, "topic0", "e", 32),
        std::make_tuple(1, "topic0", "f", 64),
        std::make_tuple(1, "topic0", "g", 128),
        std::make_tuple(1, "topic2", "f", 64),
        std::make_tuple(1, "topic2", "g", 128),
        std::make_tuple(2, "topic0", "e", 32),
        std::make_tuple(2, "topic0", "f", 64),
        std::make_tuple(2, "topic0", "g", 128),
        std::make_tuple(2, "topic2", "f", 64),
        std::make_tuple(2, "topic2", "g", 128),
        std::make_tuple(10, "topic0", "e", 32),
        std::make_tuple(10, "topic0", "f", 64),
        std::make_tuple(10, "topic0", "g", 128),
        std::make_tuple(10, "topic2", "f", 64),
        std::make_tuple(10, "topic2", "g", 128)};

    size_t num_records = 0;
    RdKafka::Message* message;
    for (size_t completed_topic_partitions = 0; completed_topic_partitions < 6;) {
      message = kafka_consumer.Consume(-1 /* timeout_ms */);
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

  // Seek again after consume to check that reseeking to an earlier position works
  ASSERT_TRUE(kafka_consumer.Seek("topic0", 16 /* timestamp_ms */));
  {
    std::vector<std::tuple<int32_t, std::string, std::string, int64_t>> expected_results{
        std::make_tuple(1, "topic0", "d", 16),
        std::make_tuple(1, "topic0", "e", 32),
        std::make_tuple(1, "topic0", "f", 64),
        std::make_tuple(1, "topic0", "g", 128),
        std::make_tuple(2, "topic0", "d", 16),
        std::make_tuple(2, "topic0", "e", 32),
        std::make_tuple(2, "topic0", "f", 64),
        std::make_tuple(2, "topic0", "g", 128),
        std::make_tuple(10, "topic0", "d", 16),
        std::make_tuple(10, "topic0", "e", 32),
        std::make_tuple(10, "topic0", "f", 64),
        std::make_tuple(10, "topic0", "g", 128)};

    size_t num_records = 0;
    RdKafka::Message* message;
    for (size_t completed_topic_partitions = 0; completed_topic_partitions < 3;) {
      message = kafka_consumer.Consume(-1 /* timeout_ms */);
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
}

TEST_F(KafkaConsumerTest, TestMultipleTopicPartitionsSeekOffset) {
  const std::unordered_set<uint32_t> partition_ids{1, 2, 10};
  const std::unordered_set<std::string> topic_names({"topic0", "topic2"});

  KafkaConsumer kafka_consumer(std::make_shared<RdKafka::MockKafkaConsumer>(mock_kafka_cluster_),
                               partition_ids,
                               topic_names,
                               "UnitTestKafkaConsumer");

  // Seek to offset
  std::map<std::string, std::map<int32_t, int64_t>> last_offsets{
      {"topic0", {{1, 3}, {2, 4}, {10, 5}}},
      {"topic2", {{1, 4}, {2, 4}, {10, 4}}},
  };
  kafka_consumer.Seek(last_offsets);

  std::vector<std::tuple<int32_t, std::string, std::string, int64_t>> expected_results{
      std::make_tuple(1, "topic0", "e", 32),
      std::make_tuple(1, "topic0", "f", 64),
      std::make_tuple(1, "topic0", "g", 128),
      std::make_tuple(2, "topic0", "f", 64),
      std::make_tuple(2, "topic0", "g", 128),
      std::make_tuple(10, "topic0", "g", 128),
      std::make_tuple(1, "topic2", "f", 64),
      std::make_tuple(1, "topic2", "g", 128),
      std::make_tuple(2, "topic2", "f", 64),
      std::make_tuple(2, "topic2", "g", 128),
      std::make_tuple(10, "topic2", "f", 64),
      std::make_tuple(10, "topic2", "g", 128)};

  size_t num_records = 0;
  RdKafka::Message* message;
  for (size_t completed_topic_partitions = 0; completed_topic_partitions < 6;) {
    message = kafka_consumer.Consume(-1 /* timeout_ms */);
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

TEST_F(KafkaConsumerTest, TestSeekInvalidTopic) {
  const std::unordered_set<uint32_t> partition_ids{1, 2, 10};
  const std::unordered_set<std::string> topic_names({"topic0", "topic2"});

  KafkaConsumer kafka_consumer(std::make_shared<RdKafka::MockKafkaConsumer>(mock_kafka_cluster_),
                               partition_ids,
                               topic_names,
                               "UnitTestKafkaConsumer");

  EXPECT_FALSE(kafka_consumer.Seek("topic1", 2));
}

}  // namespace kafka

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
// Created by Timothy Koh on 4/15/19.
//
