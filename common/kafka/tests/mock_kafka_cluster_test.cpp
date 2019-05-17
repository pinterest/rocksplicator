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

#include <vector>
#include <string>
#include <cstdint>
#include <unordered_map>

#include "gtest/gtest.h"
#include "common/kafka/tests/mock_kafka_cluster.h"

namespace kafka {

namespace {

std::string GenerateRecordPayload(const std::string& topic_name,
                                  int32_t partition_id,
                                  int64_t timestamp_ms) {
  return topic_name + ":" + std::to_string(partition_id) + ":" + std::to_string(timestamp_ms);
}

void MaybeGenerateAndAddRecords(
    MockKafkaCluster* mock_kafka_cluster,
    std::unordered_map<std::string, std::unordered_map<int32_t, size_t>>* num_records_tracker,
    const std::vector<std::string>& topic_names,
    int32_t partition_id,
    int64_t mock_timestamp) {
  for (size_t i = 0; i < topic_names.size(); i++) {
    if (mock_timestamp % (i + 1) == 0) {
      // Arbitrary decision to add if mock_timestamp is divisible by the index of the topic + 1
      auto& topic_name = topic_names[i];

      std::string payload = GenerateRecordPayload(topic_names[i], partition_id, mock_timestamp);
      mock_kafka_cluster->AddRecord(topic_names[i], partition_id, payload, mock_timestamp);

      (*num_records_tracker)[topic_name][partition_id]++;
    }
  }
}

void VerifyIter(const std::vector<MockKafkaCluster::Record>& reference_vec,
                size_t start_idx,
                MockKafkaCluster::Iterator* iter) {
  for (size_t i = start_idx; i < reference_vec.size(); i++) {
    EXPECT_TRUE(iter->Next());
    EXPECT_EQ(reference_vec[i].payload, iter->GetRecord().payload);
    EXPECT_EQ(reference_vec[i].timestamp_ms, iter->GetRecord().timestamp_ms);
  }
}

}  // namespace


class MockKafkaClusterTest : public ::testing::Test {};

TEST_F(MockKafkaClusterTest, TestAdd) {
  MockKafkaCluster mock_kafka_cluster;

  // map of topic_name -> partition_id -> num_records
  std::unordered_map<std::string, std::unordered_map<int32_t, size_t>> num_records_tracker;

  const std::vector<std::string> topic_names {"topic0", "topic1", "topic2", "topic3", "topic4",
                                              "topic5"};
  const std::vector<int32_t> partition_ids {1, 2, 3, 4, 5, 10};

  // Add records
  for (int64_t mock_timestamp = 0; mock_timestamp < 1000; mock_timestamp++) {
    for (auto partition_id : partition_ids) {
      if (mock_timestamp % partition_id == 0) {
        // Arbitrary decision to add if mock_timestamp is divisible by partition_id
        MaybeGenerateAndAddRecords(&mock_kafka_cluster, &num_records_tracker, topic_names,
                                   partition_id, mock_timestamp);
      }
    }
  }

  // Verify
  for (const auto& name_topic_pair : mock_kafka_cluster.topics_) {
    const auto& topic_name = name_topic_pair.first;
    const MockKafkaCluster::Topic& topic = name_topic_pair.second;

    for (const auto& id_partition_pair : topic) {
      const auto& partition_id = id_partition_pair.first;
      const MockKafkaCluster::Partition& partition = id_partition_pair.second;
      EXPECT_EQ(num_records_tracker[topic_name][partition_id], partition.size());

      for (const MockKafkaCluster::Record& record : partition) {
        EXPECT_EQ(GenerateRecordPayload(topic_name, partition_id, record.timestamp_ms),
                  record.payload);
      }
    }
  }
}

TEST_F(MockKafkaClusterTest, TestSeek) {
  MockKafkaCluster mock_kafka_cluster;

  const std::string topic_name = "topic_name";
  const int32_t partition_id = 42;

  const std::vector<MockKafkaCluster::Record> records
      {{"a", 2}, {"b", 4}, {"c", 8}, {"d", 16}, {"e", 32}, {"f", 64}, {"g", 128}, {"h", 256}};

  // Add records
  for (const auto& record : records) {
    mock_kafka_cluster.AddRecord(topic_name, partition_id, record.payload, record.timestamp_ms);
  }

  // Test seek
  for (size_t i = 0; i < records.size(); i++) {
    const int64_t before_timestamp = records[i].timestamp_ms - 1;
    const int64_t at_timestamp = records[i].timestamp_ms;
    const int64_t after_timestamp = records[i].timestamp_ms + 1;

    auto before_iter = mock_kafka_cluster.Seek(topic_name, partition_id, before_timestamp);
    auto at_iter = mock_kafka_cluster.Seek(topic_name, partition_id, at_timestamp);
    auto after_iter = mock_kafka_cluster.Seek(topic_name, partition_id, after_timestamp);

    // Seek should get an iter pointing to a record with timestamp >= seek_timestamp, so seeking
    // to a seek_timestamp <= timestamp of the current index should return an iter pointing
    // to the record of the current index
    VerifyIter(records, i, before_iter.get());
    VerifyIter(records, i, at_iter.get());

    // Seek to timestamp after should return iter pointing to the record at the next index
    VerifyIter(records, i + 1, after_iter.get());
  }
}

}  // namespace kafka


int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
