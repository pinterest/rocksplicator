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

#include "examples/counter_service/rocksdb_options.h"

#include <string>
#include <unistd.h>


#include "examples/counter_service/merge_operator.h"
#include "gflags/gflags.h"
#include "rocksdb/cache.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/table.h"

DEFINE_int32(max_compaction_threads,
             std::min(16L, sysconf(_SC_NPROCESSORS_ONLN)),
             "The max number of threads for rocksdb compaction");
DEFINE_int32(max_flush_threads, 4,
             "The max number of threads for rocksdb flush");
DEFINE_int32(max_write_buffer_number, 4,
             "The max number of write buffers per db");
DEFINE_uint64(rocksdb_block_cache_size_gb, 20,
              "The size of RocksDB LRU block cache in GB");
DEFINE_uint64(rocksdb_block_cache_shard_bits, 8,
              "The number of shard bits for RocksDB block cache");
DEFINE_uint64(rocksdb_wal_ttl_seconds, 3600, "The WAL ttl for RocksDB");
DEFINE_int32(rocksdb_block_size_kb, 4,
             "The rocksdb block size in KB.");
DEFINE_int32(rocksdb_write_buffer_size_mb, 64,
             "The rocksdb write buffer size in MB.");
DEFINE_int32(rocksdb_sst_file_size_mb, 64,
             "The rocksdb SST file size in MB.");
DEFINE_int32(rocksdb_bytes_per_sync_mb, 1,
             "The amount of data per sync.");
DEFINE_int32(rocksdb_wal_bytes_per_sync_mb, 1,
             "The amount of wal per sync.");

namespace {

const uint64_t KB = 1024;
const uint64_t MB = 1024 * KB;
const uint64_t GB = 1024 * MB;

rocksdb::Options __GetRocksdbOptions() {
  rocksdb::Options options;
  options.env = rocksdb::Env::Default();
  options.create_if_missing = true;
  options.max_background_compactions = FLAGS_max_compaction_threads;
  options.max_background_flushes = FLAGS_max_flush_threads;
  options.env->SetBackgroundThreads(
    FLAGS_max_compaction_threads + FLAGS_max_flush_threads,
    rocksdb::Env::LOW);
  options.env->SetBackgroundThreads(FLAGS_max_flush_threads,
                                    rocksdb::Env::HIGH);

  rocksdb::BlockBasedTableOptions table_options;
  table_options.block_size = FLAGS_rocksdb_block_size_kb * KB;
  table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));
  table_options.block_cache = rocksdb::NewLRUCache(
    FLAGS_rocksdb_block_cache_size_gb * GB,
    FLAGS_rocksdb_block_cache_shard_bits);
  options.table_factory.reset(
    rocksdb::NewBlockBasedTableFactory(table_options));

  options.write_buffer_size = FLAGS_rocksdb_write_buffer_size_mb * MB;
  options.max_write_buffer_number = FLAGS_max_write_buffer_number;
  options.min_write_buffer_number_to_merge = 1;

  options.level0_file_num_compaction_trigger = 4;

  options.target_file_size_base = FLAGS_rocksdb_sst_file_size_mb * MB;

  // ensure L0 size == L1 size
  options.max_bytes_for_level_base = options.write_buffer_size *
    options.min_write_buffer_number_to_merge *
    options.level0_file_num_compaction_trigger;

  options.compression = rocksdb::kNoCompression;

  options.WAL_ttl_seconds = FLAGS_rocksdb_wal_ttl_seconds;
  options.bytes_per_sync = FLAGS_rocksdb_bytes_per_sync_mb * MB;
  options.wal_bytes_per_sync = FLAGS_rocksdb_wal_bytes_per_sync_mb * MB;
  options.max_open_files = -1;
  options.merge_operator = std::make_shared<counter::CounterMergeOperator>();
  return options;
}

}  // anonymous namespace

namespace counter {

rocksdb::Options GetRocksdbOptions(const std::string& segment, const std::string& db) {
  static const rocksdb::Options options = __GetRocksdbOptions();
  return options;
}

}  // namespace counter
