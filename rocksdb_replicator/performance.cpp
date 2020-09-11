/// Copyright 2016 Pinterest Inc.
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
// @author bol (bol@pinterest.com)
//

#include <folly/ScopeGuard.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common/stats/stats.h"
#define private public
#include "rocksdb_replicator/rocksdb_replicator.h"
#include "rocksdb/cache.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/table.h"


using folly::SocketAddress;
using common::Stats;
using replicator::DBRole;
using replicator::ReturnCode;
using replicator::RocksDBReplicator;
using rocksdb::DB;
using rocksdb::Env;
using rocksdb::Options;
using rocksdb::WriteBatch;
using rocksdb::WriteOptions;

using std::chrono::seconds;
using std::shared_ptr;
using std::string;
using std::this_thread::sleep_for;
using std::thread;
using std::to_string;
using std::vector;

DEFINE_bool(is_leader, false, "Leader or Follower?");
DEFINE_bool(verify_data, false, "Verify data on followers(s)?");
DEFINE_int32(num_shards, 200, "Number of shards");
DEFINE_int32(num_write_threads, 2, "Number of threads issuing writes");
DEFINE_int32(num_keys_per_shard_thread, 10 * 1024,
             "Number of keys written for per shard per thread");
DEFINE_string(db_path, "/tmp/", "The path to dbs");
DEFINE_string(upstream_ip, "127.0.0.1", "upstream ip address");
DEFINE_int32(value_size, 1024, "value size");

DECLARE_int32(rocksdb_replicator_port);

bool notFinished(const vector<RocksDBReplicator::ReplicatedDB*>& dbs) {
  for (auto& db : dbs) {
    if (static_cast<int64_t>(db->db_->GetLatestSequenceNumber()) <
        FLAGS_num_write_threads * FLAGS_num_keys_per_shard_thread) {
      return true;
    }
  }

  return false;
}

shared_ptr<DB> cleanAndOpenDB(const string& path, Options& options) {
  CHECK_EQ(system(("rm -rf " + path).c_str()), 0);

  DB* db;
  CHECK(DB::Open(options, path, &db).ok());
  return shared_ptr<DB>(db);
}

uint64_t GetCurrentTime() {
  return std::chrono::duration_cast<seconds>(
    std::chrono::system_clock::now().time_since_epoch()).count();
}

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  LOG(INFO) << common::Stats::get()->DumpStatsAsText();

  Options options;
  options.env = Env::Default();
  options.WAL_ttl_seconds = 3600;  // keep WAL for one hour
  options.rate_limiter.reset(rocksdb::NewGenericRateLimiter(50 * 1024 * 1024));
  options.create_if_missing = true;
  options.IncreaseParallelism(8);
  options.target_file_size_base = 128 * 1024 * 1024;
  options.max_bytes_for_level_base = 16 * 1024 * 1024;
  rocksdb::BlockBasedTableOptions table_options;
  table_options.block_cache = rocksdb::NewLRUCache(
    static_cast<uint64_t>(10) * 1024 * 1024 * 1024, 10);
  table_options.cache_index_and_filter_blocks = true;
  table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));
  options.table_factory.reset(
    rocksdb::NewBlockBasedTableFactory(table_options));
  options.create_if_missing = true;

  vector<RocksDBReplicator::ReplicatedDB*> dbs;
  RocksDBReplicator::ReplicatedDB* db;
  auto replicator = RocksDBReplicator::instance();
  // setup db replications
  for (int i = 0; i < FLAGS_num_shards; ++i) {
    auto db_name = "shard" + to_string(i);
    SocketAddress addr(FLAGS_upstream_ip, FLAGS_rocksdb_replicator_port);
    replicator->addDB(db_name, cleanAndOpenDB(FLAGS_db_path + db_name, options),
                      FLAGS_is_leader ? DBRole::MASTER : DBRole::SLAVE, addr,
                      &db);
    dbs.push_back(db);
  }

  if (FLAGS_is_leader) {
    sleep_for(seconds(3));
    LOG(INFO) << "Starting write threads...";
    const auto start = GetCurrentTime();
    vector<thread> threads(FLAGS_num_write_threads);
    for (int i = 0; i < FLAGS_num_write_threads; ++i) {
      threads[i] = thread([&dbs, thread_id = i] {
          string dummy_data;
          dummy_data.resize(FLAGS_value_size);
          const rocksdb::WriteOptions options;
          for (int n = 0; n < FLAGS_num_keys_per_shard_thread; ++n) {
            for (auto db : dbs) {
              WriteBatch update;
              update.Put("thread_" + to_string(thread_id) + "_key_" +
                         to_string(n), dummy_data);
              CHECK(db->Write(options, &update).ok());
            }
          }
        });
    }

    for (auto& thread : threads) {
      thread.join();
    }
    const auto end = GetCurrentTime();
    LOG(INFO) << "Total time : " << end - start << " speed " <<
      (static_cast<double>(FLAGS_num_write_threads) *
       FLAGS_num_keys_per_shard_thread *
       FLAGS_num_shards * FLAGS_value_size) / (end - start);
    while (true) {
      sleep_for(seconds(10));
      LOG(INFO) << common::Stats::get()->DumpStatsAsText();
    }
  } else {
    while (notFinished(dbs)) {
      sleep_for(seconds(1));
      LOG(INFO) << common::Stats::get()->DumpStatsAsText();
    }

    LOG(INFO) << "Done";
    LOG(INFO) << common::Stats::get()->DumpStatsAsText();

    if (FLAGS_verify_data) {
      LOG(INFO) << "Starting verifying threads...";
      vector<thread> threads(FLAGS_num_write_threads);
      for (int i = 0; i < FLAGS_num_write_threads; ++i) {
        threads[i] = thread([&dbs, thread_id = i] {
            string dummy_data;
            dummy_data.resize(FLAGS_value_size);
            const rocksdb::ReadOptions options;
            for (int n = 0; n < FLAGS_num_keys_per_shard_thread; ++n) {
              string key = "thread_" + to_string(thread_id) + "_key_" +
                to_string(n);
              for (auto db : dbs) {
                string value;
                auto status = db->db_->Get(options, key, &value);
                CHECK(status.ok()) << key << " " << status.ToString();
                CHECK(value == dummy_data) << key << " " << value.size() << " "
                                           << dummy_data.size();
              }
            }
          });
      }

      for (auto& thread : threads) {
        thread.join();
      }

      for (auto db : dbs) {
        CHECK(static_cast<int64_t>(db->db_->GetLatestSequenceNumber()) ==
              FLAGS_num_write_threads * FLAGS_num_keys_per_shard_thread);
      }
    }
  }

  LOG(INFO) << common::Stats::get()->DumpStatsAsText();
}
