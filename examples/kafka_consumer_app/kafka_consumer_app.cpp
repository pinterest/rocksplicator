#include <chrono>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "common/kafka/kafka_broker_file_watcher.h"
#include "common/kafka/kafka_consumer_pool.h"
#include "common/kafka/kafka_watcher.h"
#include "folly/String.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "librdkafka/rdkafkacpp.h"
#include "rocksdb_admin/detail/kafka_broker_file_watcher_manager.h"

const uint32_t kKafkaConsumerPoolSize = 1;
const char kKafkaConsumerType[] = "rocksplicator_consumer";
const char kKafkaWatcherName[] = "rocksplicator_watcher";

DEFINE_int32(num_consumers, 1, "Number of kafka consumers to create");
DEFINE_int32(num_partitions_per_consumer, 1, "Number of partitions per kafka consumer");
DEFINE_int64(replay_from_minutes_ago, 1, "Start consuming messages from these many minutes ago");
DEFINE_int64(wait_for_seconds_after_catchup, 1, "Wait these many seconds after catching up");
DEFINE_int64(wait_for_seconds_before_start,
             1,
             "Wait these many seconds before starting consumption");
DEFINE_int32(sleep_between_message_msec, 1,
             "Sleeps these many seconds between individual messages consumption");
DEFINE_string(topic_name,
              "",
              "topic to consume data from");
DEFINE_string(kafka_broker_serverset_path,
              "",
              "kafka broken serverset path");
DEFINE_string(kafka_consumer_group, "test_consumer_group_id", "Kafka Consumer group name");

using std::cout;
using std::endl;
using std::string;
using std::unordered_set;

void run_partition_set(const unordered_set<uint32_t>& partition_ids_set) {
  const string db_name = "test_db";
  const string segment = "test_segment";
  const string topic_name = FLAGS_topic_name;
  const string kafka_broker_serverset_path = FLAGS_kafka_broker_serverset_path;
  const string kafka_consumer_group = FLAGS_kafka_consumer_group;
  const uint64_t replay_timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                           std::chrono::system_clock::now().time_since_epoch())
                                           .count() -
                                       (FLAGS_replay_from_minutes_ago * 60 * 1000);

  cout << "Sleeping for " << FLAGS_wait_for_seconds_before_start << " seconds before consuming"
       << endl;
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_wait_for_seconds_before_start));
  cout << "Waking up after sleep before starting consumers" << endl;

  auto kafka_broker_file_watcher =
      admin::detail::KafkaBrokerFileWatcherManager ::getInstance().getFileWatcher(
          kafka_broker_serverset_path);

  const auto kafka_consumer_pool = std::make_shared<::kafka::KafkaConsumerPool>(
      kKafkaConsumerPoolSize,
      partition_ids_set,
      kafka_broker_file_watcher->GetKafkaBrokerList(),
      std::unordered_set<std::string>({topic_name}),
      kafka_consumer_group,
      folly::stringPrintf("%s_%s", kKafkaConsumerType, segment.c_str()));

  auto kafka_watcher = std::make_shared<KafkaWatcher>(
      folly::stringPrintf("%s_%s", kKafkaWatcherName, segment.c_str()),
      kafka_consumer_pool,
      -1,   // kafka_init_blocking_consume_timeout_ms
      1000  // kafka consumer timeout ms
  );

  int64_t message_count = 0;
  kafka_watcher->StartWith(
      replay_timestamp_ms,
      [message_count, db_name, segment = std::move(segment)](
          std::shared_ptr<const RdKafka::Message> message, const bool is_replay) mutable {
        if (message == nullptr) {
          LOG(ERROR) << "Message nullptr";
          return;
        }
        if (FLAGS_sleep_between_message_msec != 0) {
          std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_sleep_between_message_msec));
        }
        message_count++;
        cout << "DB name: " << db_name << ", Key " << *message->key() << ", "
             << "partition: " << message->partition() << ", "
             << "offset: " << message->offset() << ", "
             << "payload len: " << message->len()
             << ", "
             // << "msg_timestamp: " << message->timestamp()
             << endl;
      });
  cout << "Caught up" << endl;

  // Now wait for some time before leaving this thread.
  cout << "Sleeping for " << FLAGS_wait_for_seconds_after_catchup << " seconds after catchup"
       << endl;
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_wait_for_seconds_after_catchup));
  cout << "Waking up after sleep after catching up" << endl;

  cout << "Stopping and Waiting kafka consumer " << endl;
  kafka_watcher->StopAndWait();
  cout << "Stopped kafka consumer " << endl;

  cout << "Shutting down kafka consumer" << endl;
  kafka_watcher->Shutdown(true);
  cout << "Shutdown kafka consumer" << endl;
}

void run_partition(const uint32_t partition_id) {
  const unordered_set<uint32_t> partition_ids_set({partition_id});
  run_partition_set(partition_ids_set);
  cout << "Returning from kafka watcher for partition_id" << partition_id << endl;
}

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_topic_name.empty()) {
    std::cout << "Must provide topic name" << std::endl;
    exit(-1);
  }
  if (FLAGS_kafka_broker_serverset_path.empty()) {
    std::cout << "Must provide broker serverset path" << std::endl;
    exit(-1);
  }
  if (FLAGS_kafka_consumer_group.empty()) {
    std::cout << "Must provide consumer group to use. Don't use production consumer group" << std::endl;
    exit(-1);
  }

  uint32_t batchSize = FLAGS_num_partitions_per_consumer;
  uint32_t numBatches = FLAGS_num_consumers;
  uint32_t numPartitions = batchSize * numBatches;

  std::vector<std::thread> threads(numPartitions);

  for (uint32_t batchId = 0; batchId < numBatches; ++batchId) {
    unordered_set<uint32_t> partitions = unordered_set<uint32_t>();
    for (uint32_t itemId = 0; itemId < batchSize; ++itemId) {
      uint32_t partition_id = batchId * batchSize + itemId;
      partitions.insert(partition_id);
    }

    std::thread thread =
        std::thread([partitions = std::move(partitions)] { run_partition_set(partitions); });
    threads.push_back(std::move(thread));
    cout << "Returning from kafka watcher for batchId" << batchId << endl;
  }

  for (uint32_t i = 1; i <= numBatches; ++i) {
    while (!threads.at(i - 1).joinable()) {
      cout << "Waiting for consumer thread to be joinable" << endl;
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }

  for (uint32_t i = 1; i <= numBatches; ++i) {
    threads.at(i - 1).join();
  }

  return 0;
}
