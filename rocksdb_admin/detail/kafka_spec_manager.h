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

//
// @author rajathprasad (rajathprasad@pinterest.com)
//

#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

class KafkaWatcher;

namespace admin {
namespace detail {

// Class which contains the kafka variables needed per db.
class KafkaSpec {
public:
  KafkaSpec(std::shared_ptr<KafkaWatcher> kafka_watcher,
      const int64_t timestamp_ms)
    : kafka_watcher_(kafka_watcher),
    last_kafka_message_timestamp_ms_(timestamp_ms) {}

  int64_t getTimestamp() {
    return last_kafka_message_timestamp_ms_.load();
  }

  void storeTimestamp(const int64_t timestamp) {
    last_kafka_message_timestamp_ms_.store(timestamp);
  }

  std::shared_ptr<KafkaWatcher> getKafkaWatcher() {
    return kafka_watcher_;
  }

private:
  std::shared_ptr<KafkaWatcher> kafka_watcher_;
  std::atomic<int64_t> last_kafka_message_timestamp_ms_;
};

// Manages the KafkaSpecs for all db's
class KafkaSpecManager {
public:
  KafkaSpecManager() {};

  std::shared_ptr<KafkaSpec> addSpec(const std::string& db_name,
      std::shared_ptr<KafkaWatcher> kafka_watcher, const int64_t timestamp_ms) {
    std::lock_guard<std::mutex> lock(lock_);
    auto kafka_spec = std::make_shared<KafkaSpec>(kafka_watcher, timestamp_ms);
    kafka_spec_map_[db_name] = kafka_spec;
    return kafka_spec;
  }

  void removeSpec(const std::string& db_name) {
    std::lock_guard<std::mutex> lock(lock_);
    kafka_spec_map_.erase(db_name);
  }

  std::shared_ptr<KafkaSpec> getSpec(const std::string& db_name) {
    std::lock_guard<std::mutex> lock(lock_);
    return kafka_spec_map_[db_name];
  }

  bool isSpecPresent(const std::string& db_name) {
    std::lock_guard<std::mutex> lock(lock_);
    return kafka_spec_map_.find(db_name) != kafka_spec_map_.end();
  }

private:
  std::unordered_map<std::string, std::shared_ptr<KafkaSpec>> kafka_spec_map_;
  // Lock for synchronizing access to kafka_watcher_map_
  std::mutex lock_;
};

} //namespace detail
} // namespace admin
