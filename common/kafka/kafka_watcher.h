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

#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "boost/functional/hash.hpp"

namespace RdKafka {
class Message;
}

namespace kafka {
class KafkaConsumer;

class KafkaConsumerPool;
}  // namespace kafka

typedef std::function<void(std::shared_ptr<const RdKafka::Message> message,
    const bool is_replay)> KafkaMessageHandler;

/**
 * Base class for watchers that want to consume from kafka. Manages the kafka
 * consumer and the consuming thread. Derived class just has to implement the
 * methods to initialize state, handle the messages consumed, and optionally
 * adjust the seek at each iteration of the loop.
 *
 * IMPORTANT: Derived class MUST CALL StopAndWait() IN ITS DESTRUCTOR to stop
 * the watcher thread before any derived class member variables or methods of
 * the derived class are destructed.
 */
class KafkaWatcher {
public:
  KafkaWatcher(const std::string& name,
               std::shared_ptr<kafka::KafkaConsumerPool> kafka_consumer_pool,
               int kafka_init_blocking_consume_timeout_ms = 5000,
               int kafka_consumer_timeout_ms = 0,
               int loop_cycle_limit_ms = 10000);

  KafkaWatcher(const std::string& name,
               const std::vector<std::shared_ptr<kafka::KafkaConsumerPool>>&
                kafka_consumer_pools,
               int kafka_init_blocking_consume_timeout_ms = 5000,
               int kafka_consumer_timeout_ms = 0,
               int loop_cycle_limit_ms = 10000);

  virtual ~KafkaWatcher();

  KafkaWatcher(const KafkaWatcher&) = delete;

  KafkaWatcher& operator=(const KafkaWatcher&) = delete;

  std::string GetTopicString(const kafka::KafkaConsumer& consumer);

  // If set to a value other than -1, will seek kafka consumer to this
  // timestamp ms during initialization.
  // return false if init or seek fails internally rather than fail silently
  bool Start(int64_t initial_kafka_seek_timestamp_ms = -1);

  // If last_offsets is not empty, will seek kafka consumer to the
  // last_offsets + 1 during initialization.
  // return false if init or seek fails internally rather than fail silently
  bool Start(const std::map<std::string, std::map<int32_t,
      int64_t>>& last_offsets);

  bool StartWith(int64_t initial_kafka_seek_timestamp_ms,
      KafkaMessageHandler handler);

  bool StartWith(const std::map<std::string, std::map<int32_t,
      int64_t>>& last_offsets,
      KafkaMessageHandler handler);

  // Non blocking call to signal the watch loop to terminate at the
  // next iteration. Can be called to signal multiple KafkaWatchers to
  // stop in parallel
  void Stop() { is_stopped_.store(true); }

  uint64_t ErrorCount() { return err_count_.load(); }

  // Blocking call which signals the watch loop to terminate at the next
  // iteration and blocks and waits for the watch thread to end.
  void StopAndWait() {
    is_stopped_.store(true);
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  // Shutdown the watcher.
  // If graceful is true, block and wait for the watch thread to end.
  // Otherwise detach the watcher thread.
  void Shutdown(bool graceful = true) {
    is_stopped_.store(true);
    if (thread_.joinable()) {
      if (graceful) {
        thread_.join();
      } else {
        thread_.detach();
      }
    }
  }

protected:
  // Tag used when logging metrics, so we can differentiate between
  // kafka watchers for different use cases
  const std::string kafka_watcher_metric_tag_;
  // The name of the kafka watcher
  const std::string name_;

private:
  virtual void HandleKafkaNoErrorMessage(
      std::shared_ptr<const RdKafka::Message> message,
      const bool is_replay) {
    if (handler_) {
      handler_(message, is_replay);
    }
  }

  // Derived class should implement if there is code to be run before getting
  // the kafka consumer from the pool. Return false to abort starting the
  // watcher
  virtual bool Init() { return true; }

  // Derived class should implement if there is code that needs to be run at
  // the start of the watch loop (before messages are consumed)
  virtual void WatchLoopIterationStart() {}

  // Derived class should implement if there is a need to adjust
  // the seek of the kafka consumer before messages are consumed
  virtual void MaybeAdjustKafkaConsumerSeek(
      kafka::KafkaConsumer* const kafka_consumer) {}

  // Derived class should implement if there is code that needs to be run at
  // the end of the watch loop (after messages are consumed)
  virtual void WatchLoopIterationEnd() {}

  // Performs the initial seek of the kafka consumer. Default behavior is to
  // seek all topics to the initial timestamp ms (if specified). Derived class
  // can override to implement custom seek operations. Return false to abort
  // starting the watcher
  virtual bool InitKafkaConsumerSeek(kafka::KafkaConsumer* const kafka_consumer,
                                     int64_t initial_kafka_seek_timestamp_ms);

  // Consumes messages up to the current timestamp for a given consumer.
  // Returns how many messages are consumed.
  uint32_t ConsumeUpToNow(kafka::KafkaConsumer& consumer);

  void StartWatchLoop();

  std::thread thread_;
  std::atomic<bool> is_stopped_;
  std::atomic<uint64_t> err_count_{0};

  const std::vector<std::shared_ptr<kafka::KafkaConsumerPool>>
    kafka_consumer_pools_;
  std::vector<std::shared_ptr<kafka::KafkaConsumer>> kafka_consumers_;

  // The time limit used to blocking consume kafka messages until the current
  // timestamp when initializing index segment manager, -1 means wait for
  // consuming up to now.
  const int kafka_init_blocking_consume_timeout_ms_;
  // The time spent waiting on a consume if data is not available
  const int kafka_consumer_timeout_ms_;
  // The time limit for payload update in each loop cycle
  const int loop_cycle_limit_ms_;
  // Kafka message handler provided by caller
  KafkaMessageHandler handler_;
};
