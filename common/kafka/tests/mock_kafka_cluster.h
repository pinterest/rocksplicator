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

#include <string>
#include <unordered_map>
#include <vector>

#include "glog/logging.h"
#include "gtest/gtest.h"

namespace kafka {

// This is intended to be used for sequential tests where we initialize the data once and use it for
// read only after
// TODO: Implement thread safe writes after initialization if needed
class MockKafkaCluster {
  FRIEND_TEST(MockKafkaClusterTest, TestAdd);

public:
  struct Record {
    std::string payload;
    int64_t timestamp_ms;
    int64_t offset;
  };
  using Partition = std::vector<Record>;
  using Topic = std::unordered_map<int32_t, Partition>;  // partition_id -> partition

  class Iterator {
  public:
    Iterator(std::string topic_name, int32_t partition_id, const std::vector<Record>* vec, int idx)
        : topic_name_(std::move(topic_name)), partition_id_(partition_id), vec_(vec), idx_(idx) {}

    bool HasNext() {
      if (vec_ == nullptr) {
        return false;
      }

      return idx_ + 1 < vec_->size();
    }

    bool Next() {
      if (vec_ == nullptr) {
        return false;
      }

      idx_++;
      return idx_ < vec_->size();
    }

    // Only call if Next() returns true, if not, behavior is not defined
    Record GetRecord() {
      CHECK(vec_ != nullptr);
      CHECK(idx_ < vec_->size());
      return (*vec_)[idx_];
    }

    const std::string topic_name_;
    const int32_t partition_id_;
    const std::vector<Record>* vec_;
    int idx_;
  };

  // Find offset of the first record in the partition with timestamp >= timestamp_ms
  // Returns -1 if the partition does not exist
  int64_t offsetForTime(const std::string& topic_name,
                        const int32_t partition_id,
                        const int64_t timestamp_ms) {
    const auto name_topic_pair = topics_.find(topic_name);
    if (name_topic_pair == topics_.end()) {
      return -1;
    }

    const Topic& topic = name_topic_pair->second;
    auto id_partition_pair = topic.find(partition_id);
    if (id_partition_pair == topic.end()) {
      return -1;
    }

    return offsetForTime(id_partition_pair->second, timestamp_ms);
  }

  // Find offset of the first record in the partition with timestamp >= timestamp_ms
  int64_t offsetForTime(const Partition& partition, int64_t timestamp_ms) {
    for (const auto& record : partition) {
      if (record.timestamp_ms >= timestamp_ms) {
        return record.offset;
      }
    }

    return partition.size();
  }

  // Finds the relevant Partition and returns an iterator to that partition which can retrieve
  // Records >= seek_timestamp_ms. Returns an empty iterator if topic_name or partition_id is not
  // found
  std::unique_ptr<Iterator> Seek(const std::string& topic_name,
                                 int32_t partition_id,
                                 int64_t seek_timestamp_ms) {
    // Find topic
    auto name_topic_pair = topics_.find(topic_name);
    if (name_topic_pair == topics_.end()) {
      return std::make_unique<Iterator>(topic_name, partition_id, nullptr, -1);
    }
    Topic& topic = name_topic_pair->second;

    // Find partition
    auto id_partition_pair = topic.find(partition_id);
    if (id_partition_pair == topic.end()) {
      return std::make_unique<Iterator>(topic_name, partition_id, nullptr, -1);
    }
    Partition& partition = id_partition_pair->second;

    // -1 to offset since Next() will be called first which will do offset++
    const auto offset = offsetForTime(partition, seek_timestamp_ms) - 1;
    return std::make_unique<Iterator>(topic_name, partition_id, &partition, offset);
  }

  std::unique_ptr<Iterator> SeekOffset(const std::string& topic_name,
                                       int32_t partition_id,
                                       int64_t last_offset) {
    // Find topic
    auto name_topic_pair = topics_.find(topic_name);
    if (name_topic_pair == topics_.end()) {
      return std::make_unique<Iterator>(topic_name, partition_id, nullptr, -1);
    }
    Topic& topic = name_topic_pair->second;

    // Find partition
    auto id_partition_pair = topic.find(partition_id);
    if (id_partition_pair == topic.end()) {
      return std::make_unique<Iterator>(topic_name, partition_id, nullptr, -1);
    }
    Partition& partition = id_partition_pair->second;
    return std::make_unique<Iterator>(topic_name, partition_id, &partition, last_offset);
  }

  void AddRecord(const std::string& topic_name,
                 int32_t partition_id,
                 std::string payload,
                 int64_t timestamp_ms) {
    Partition& partition = topics_[topic_name][partition_id];
    if (!partition.empty()) {
      auto last_timestamp_ms = partition.back().timestamp_ms;
      CHECK(timestamp_ms >= last_timestamp_ms)
          << "Error adding record, timestamp is not in increasing order. Last timestamp: "
          << last_timestamp_ms << ", new record timestamp: " << timestamp_ms;
    }

    partition.push_back(Record{std::move(payload), timestamp_ms, partition.size()});
  }

private:
  std::unordered_map<std::string, Topic> topics_;
};

}  // namespace kafka
