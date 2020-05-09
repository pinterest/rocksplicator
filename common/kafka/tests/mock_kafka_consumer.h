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

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "gtest/gtest.h"
#include "librdkafka/rdkafkacpp.h"
#include "common/kafka/kafka_consumer.h"
#include "common/kafka/tests/mock_kafka_cluster.h"


namespace RdKafka {

class MockMessage : public Message {
public:
  MockMessage(std::string topic_name,
              int32_t partition_id,
              std::string payload_str,
              int64_t timestamp_ms,
              int64_t offset)
      : topic_name_(std::move(topic_name)),
        partition_id_(partition_id),
        payload_str_(std::move(payload_str)),
        timestamp_ms_(timestamp_ms),
        offset_(offset) {}
  ~MockMessage() {}

  // Mock methods
  std::string errstr() const override {
    return "MockMessage error returned from errstr(). code: " + std::to_string(err());
  }
  ErrorCode err() const override { return ErrorCode::ERR_NO_ERROR; }
  std::string topic_name() const override { return topic_name_; }
  int32_t partition() const override { return partition_id_; }
  void* payload() const override {
    const char* data = payload_str_.data();
    return reinterpret_cast<void*>(const_cast<char*>(data));
  }
  size_t len() const override { return payload_str_.size(); }
  MessageTimestamp timestamp() const override {
    MessageTimestamp message_timestamp;

    // We only use create time in mock right now
    message_timestamp.type = RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME;
    message_timestamp.timestamp = timestamp_ms_;
    return message_timestamp;
  }

  // Not implemented methods, implement if you need to use it in a test
  Topic* topic() const override { return nullptr; }
  const std::string* key() const override {
    CHECK(false) << "Not implemented";
    return nullptr;
  }
  const void* key_pointer() const override {
    CHECK(false) << "Not implemented";
    return nullptr;
  }
  size_t key_len() const override {
    CHECK(false) << "Not implemented";
    return 0;
  }
  int64_t offset() const override { return offset_; }
  void* msg_opaque() const override {
    CHECK(false) << "Not implemented";
    return nullptr;
  }
  int64_t latency() const override {
    CHECK(false) << "Not implemented";
    return -1;
  }
  struct rd_kafka_message_s* c_ptr() override {
    CHECK(false) << "Not implemented";
    return nullptr;
  }

  Status status() const override {
    return MSG_STATUS_PERSISTED;
  }

  Headers *headers() override {
    return nullptr;
  }

  Headers *headers(RdKafka::ErrorCode *err) override {
    return nullptr;
  }

private:
  const std::string topic_name_;
  const int32_t partition_id_;
  const std::string payload_str_;
  const int64_t timestamp_ms_;
  const int64_t offset_;
};

class MockErrorMessage : public MockMessage {
public:
  explicit MockErrorMessage(ErrorCode error_code) : MockErrorMessage(error_code, "", -1) {}

  MockErrorMessage(ErrorCode error_code, const std::string& topic_name, const int32_t partition_id)
      : MockMessage(topic_name, partition_id, "", -1, -1), error_code_(error_code) {}

  // Mock methods
  ErrorCode err() const override { return error_code_; }

private:
  const ErrorCode error_code_;
};

class MockKafkaConsumer : public KafkaConsumer {
public:
  explicit MockKafkaConsumer(std::shared_ptr<::kafka::MockKafkaCluster> mock_kafka_cluster)
      : mock_kafka_cluster_(std::move(mock_kafka_cluster)) {}

  ////////////////////////////////////////////////////////////////
  // RdKafka::KafkaConsumer Implemented Methods
  ErrorCode assign(const std::vector<TopicPartition*>& partitions) override {
    assigned_topic_partitions_.clear();
    topic_partition_to_iter_map_.clear();

    for (const auto* input_topic_partition : partitions) {
      const auto& topic_name = input_topic_partition->topic();
      const auto partition = input_topic_partition->partition();

      CHECK(assigned_topic_partitions_.emplace(GetTopicPartitionKey(topic_name, partition)).second);
    }

    return ERR_NO_ERROR;
  }

  Message* consume(int timeout_ms) override {
    for (auto it = topic_partition_to_iter_map_.begin();
         it != topic_partition_to_iter_map_.end();) {
      const auto& kafka_iter = it->second;
      const auto& topic_name = kafka_iter->topic_name_;
      const auto partition_id = kafka_iter->partition_id_;

      // This will fail if the topic partition is assigned but not seeked before consume is called.
      // This is a stricter check than RdKafka does. Instead of relying on the initial offset, we
      // want to enforce that a seek MUST happen before consume is called
      CHECK(kafka_iter != nullptr)
          << "Kafka iter is nullptr for topic partition: " << it->first
          << ", make sure you seek all assigned topic partitions before consuming";

      if (kafka_iter->Next()) {
        ::kafka::MockKafkaCluster::Record record = kafka_iter->GetRecord();
        return new MockMessage(
            topic_name, partition_id, record.payload, record.timestamp_ms, record.offset);
      } else {
        // Create copy before we delete
        const auto topic = topic_name;

        // Since we won't write anymore after the data is initialized, it is safe to remove
        // the kafka_iter if there are no more records in it
        it = topic_partition_to_iter_map_.erase(it);
        return new RdKafka::MockErrorMessage(ERR__PARTITION_EOF, topic, partition_id);
      }
    }

    // No topic partition to consume from. Instead of waiting unitl a timeout for messages to come
    // in, we can timeout immediately since we know no more messages will come in
    return new RdKafka::MockErrorMessage(ERR__TIMED_OUT);
  }

  ErrorCode seek(const TopicPartition& topic_partition, int timeout_ms) override {
    const auto& topic_name = topic_partition.topic();
    const auto partition_id = topic_partition.partition();
    const auto key = GetTopicPartitionKey(topic_name, partition_id);

    // Do a fatal check if we seek a TopicPartition that is not assigned, this is stricter than
    // RdKafka which would return an error code instead
    CHECK(assigned_topic_partitions_.find(key) != assigned_topic_partitions_.end())
        << "Error. topic : " << topic_name << ", partition: " << partition_id
        << " is not assigned. Must assign before seek";

    // -1 to offset since Next() will be called first which will do offset++
    topic_partition_to_iter_map_[key] =
        mock_kafka_cluster_->SeekOffset(topic_name, partition_id, topic_partition.offset() - 1);
    return ERR_NO_ERROR;
  }

  ErrorCode close() override { return ERR_NO_ERROR; }

  ////////////////////////////////////////////////////////////////
  // RdKafka::Handle Implemented Methods
  ErrorCode offsetsForTimes(std::vector<TopicPartition*>& offsets, int timeout_ms) override {
    for (auto* topic_partition : offsets) {
      const auto& topic_name = topic_partition->topic();
      const auto partition_id = topic_partition->partition();

      // Timestamp is specified through this function call using offset
      const auto timestamp_ms = topic_partition->offset();
      const auto offset =
          mock_kafka_cluster_->offsetForTime(topic_name, partition_id, timestamp_ms);
      if (offset != -1) {
        topic_partition->set_offset(offset);
      }
    }

    return ERR_NO_ERROR;
  }

private:
  ////////////////////////////////////////////////////////////////
  // NotImpl error methods
  ErrorCode NotImplErrorCode() const {
    LOG(FATAL) << "Mock function not implemented, should not be called";
    return ERR_UNKNOWN;
  }

  std::string NotImplStr() const {
    LOG(FATAL) << "Mock function not implemented, should not be called";
    return std::string();
  }

  int NotImplInt() const {
    LOG(FATAL) << "Mock function not implemented, should not be called";
    return -1;
  }

  template <typename T>
  T* NotImplNullptr() const {
    LOG(FATAL) << "Mock function not implemented, should not be called";
    return nullptr;
  }

  void NotImplVoid() const { LOG(FATAL) << "Mock function not implemented, should not be called"; }

  ////////////////////////////////////////////////////////////////
  // RdKafka::KafkaConsumer Not Implemented Methods
  ErrorCode subscription(std::vector<std::string>& topics) override { return NotImplErrorCode(); }

  ErrorCode subscribe(const std::vector<std::string>& topics) override {
    return NotImplErrorCode();
  }

  ErrorCode unsubscribe() override { return NotImplErrorCode(); }

  ErrorCode commitSync() override { return NotImplErrorCode(); }

  ErrorCode commitAsync() override { return NotImplErrorCode(); }

  ErrorCode commitSync(Message* message) override { return NotImplErrorCode(); }

  ErrorCode commitAsync(Message* message) override { return NotImplErrorCode(); }

  ErrorCode commitSync(std::vector<TopicPartition*>& offsets) override {
    return NotImplErrorCode();
  }

  ErrorCode commitAsync(const std::vector<TopicPartition*>& offsets) override {
    return NotImplErrorCode();
  }

  ErrorCode commitSync(OffsetCommitCb* offset_commit_cb) override { return NotImplErrorCode(); }

  ErrorCode commitSync(std::vector<TopicPartition*>& offsets,
                       OffsetCommitCb* offset_commit_cb) override {
    return NotImplErrorCode();
  }

  ErrorCode committed(std::vector<TopicPartition*>& partitions, int timeout_ms) override {
    return NotImplErrorCode();
  }

  ErrorCode position(std::vector<TopicPartition*>& partitions) override {
    return NotImplErrorCode();
  }

  ErrorCode offsets_store(std::vector<TopicPartition*>& offsets) override {
    return NotImplErrorCode();
  }

  ErrorCode assignment(std::vector<RdKafka::TopicPartition*>& partitions) override {
    return NotImplErrorCode();
  }

  ErrorCode unassign() override { return NotImplErrorCode(); }

  ////////////////////////////////////////////////////////////////
  // RdKafka::Handle Not Implemented Methods
  const std::string name() const override { return NotImplStr(); }

  const std::string memberid() const override { return NotImplStr(); }

  int poll(int timeout_ms) override { return NotImplInt(); }

  int outq_len() override { return NotImplInt(); }

  ErrorCode metadata(bool all_topics,
                     const Topic* only_rkt,
                     Metadata** metadatap,
                     int timeout_ms) override {
    return NotImplErrorCode();
  }

  ErrorCode pause(std::vector<TopicPartition*>& partitions) override { return NotImplErrorCode(); }

  ErrorCode resume(std::vector<TopicPartition*>& partitions) override { return NotImplErrorCode(); }

  ErrorCode query_watermark_offsets(const std::string& topic,
                                    int32_t partition,
                                    int64_t* low,
                                    int64_t* high,
                                    int timeout_ms) override {
    return NotImplErrorCode();
  }

  ErrorCode get_watermark_offsets(const std::string& topic,
                                  int32_t partition,
                                  int64_t* low,
                                  int64_t* high) override {
    return NotImplErrorCode();
  }

  Queue* get_partition_queue(const TopicPartition* partition) override {
    return NotImplNullptr<Queue>();
  }

  ErrorCode set_log_queue(Queue* queue) override { return NotImplErrorCode(); }

  void yield() override { NotImplVoid(); }

  const std::string clusterid(int timeout_ms) override { return NotImplStr(); }

  struct rd_kafka_s* c_ptr() override {
    return NotImplNullptr<struct rd_kafka_s>();
  }

  ////////////////////////////////////////////////////////////////
  // Other private methods
  std::string GetTopicPartitionKey(const std::string& topic, const int partition) {
    return topic + ":" + std::to_string(partition);
  }

  std::shared_ptr<::kafka::MockKafkaCluster> mock_kafka_cluster_;

  std::unordered_set<std::string> assigned_topic_partitions_;
  std::unordered_map<std::string, std::unique_ptr<::kafka::MockKafkaCluster::Iterator>>
      topic_partition_to_iter_map_;
};

}  // namespace RdKafka
