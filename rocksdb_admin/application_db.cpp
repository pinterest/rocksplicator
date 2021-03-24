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

#include "rocksdb_admin/application_db.h"

#include <string>

#include "folly/Conv.h"
#include "rocksdb/convenience.h"
#include "common/stats/stats.h"
#include "common/timer.h"

DEFINE_bool(disable_rocksplicator_db_stats, false,
            "Disable the stats for rocksplicator db");

namespace {

const std::string kRocksdbNewIterator = "rocksdb_new_iterator";
const std::string kRocksdbNewIteratorMs = "rocksdb_new_iterator_ms";
const std::string kRocksdbGet = "rocksdb_get";
const std::string kRocksdbGetMs = "rocksdb_get_ms";
const std::string kRocksdbMultiGet = "rocksdb_multi_get";
const std::string kRocksdbMultiGetMs = "rocksdb_multi_get_ms";
const std::string kRocksdbWrite = "rocksdb_write";
const std::string kRocksdbWriteBytes = "rocksdb_write_bytes";
const std::string kRocksdbWriteMs = "rocksdb_write_ms";
const std::string kRocksdbCompaction = "rocksdb_compact_range";
const std::string kRocksdbCompactionMs = "rocksdb_compact_range_ms";

}  // anonymous namespace

namespace admin {

static const std::string rocksdb_prefix = "rocksdb.";
static const std::string applicationdb_prefix = "applicationdb.";
const std::string ApplicationDB::Properties::kNumLevels =
    applicationdb_prefix + "num-levels";
const std::string ApplicationDB::Properties::kHighestEmptyLevel =
    applicationdb_prefix + "highest-empty-level";

ApplicationDB::ApplicationDB(
    const std::string& db_name,
    std::shared_ptr<rocksdb::DB> db,
    replicator::DBRole role,
    std::unique_ptr<folly::SocketAddress> upstream_addr)
    : db_name_(db_name)
    , db_(std::move(db))
    , role_(role)
    , upstream_addr_(std::move(upstream_addr))
    , replicated_db_(nullptr) {
  if (!IsSlave() || upstream_addr_) {
    auto ret = replicator::RocksDBReplicator::instance()->addDB(db_name_,
      db_, role_, upstream_addr_ ? *upstream_addr_ : folly::SocketAddress(),
      &replicated_db_);
    if (ret != replicator::ReturnCode::OK) {
      throw ret;
    }
  }
}

ApplicationDB::~ApplicationDB() {
  if (replicated_db_) {
    replicator::RocksDBReplicator::instance()->removeDB(db_name_);
  }
}

rocksdb::Iterator* ApplicationDB::NewIterator(
    const rocksdb::ReadOptions& options) {
  common::Stats::get()->Incr(kRocksdbNewIterator);
  common::Timer timer(kRocksdbNewIteratorMs);
  return db_->NewIterator(options);
}

rocksdb::Status ApplicationDB::Get(
    const rocksdb::ReadOptions& options,
    const rocksdb::Slice& slice,
    std::string* value) {
  // TODO(bol) apply it to all other stats or sample the stats.
  // We need to call Get() nearly 10M times per second, which makes it too
  // expensive to tract stats for every call.
  if (FLAGS_disable_rocksplicator_db_stats) {
    return db_->Get(options, slice, value);
  } else {
    common::Stats::get()->Incr(kRocksdbGet);
    common::Timer timer(kRocksdbGetMs);
    return db_->Get(options, slice, value);
  }
}

rocksdb::Status ApplicationDB::Get(const rocksdb::ReadOptions& options,
                                   const rocksdb::Slice& key,
                                   rocksdb::PinnableSlice* value) {
  if (FLAGS_disable_rocksplicator_db_stats) {
    return db_->Get(options, db_->DefaultColumnFamily(), key, value);
  } else {
    common::Stats::get()->Incr(kRocksdbGet);
    common::Timer timer(kRocksdbGetMs);
    return db_->Get(options, db_->DefaultColumnFamily(), key, value);
  }
}

std::vector<rocksdb::Status> ApplicationDB::MultiGet(
    const rocksdb::ReadOptions& options,
    const std::vector<rocksdb::Slice>& slice,
    std::vector<std::string>* value) {
  common::Stats::get()->Incr(kRocksdbMultiGet);
  common::Timer timer(kRocksdbMultiGetMs);
  return db_->MultiGet(options, slice, value);
}

rocksdb::Status ApplicationDB::Write(const rocksdb::WriteOptions& options,
    rocksdb::WriteBatch* write_batch) {
  common::Stats::get()->Incr(kRocksdbWrite);
  common::Stats::get()->Incr(kRocksdbWriteBytes, write_batch->GetDataSize());
  common::Timer timer(kRocksdbWriteMs);
  if (replicated_db_) {
    return replicated_db_->Write(options, write_batch);
  } else {
    // ApplicationDBManager can be use to manage rocksdb instance lifecycle
    // without replication. In this case, ApplicationDB has the replicator::DBRole
    // as SLAVE and no upstream_addr. Thus the replicated_db_ is nullptr, and
    // we'll write to the local db_
    return db_->Write(options, write_batch);
  }
}

rocksdb::Status ApplicationDB::CompactRange(
        const rocksdb::CompactRangeOptions& options,
        const rocksdb::Slice* begin, const rocksdb::Slice* end) {
  common::Stats::get()->Incr(kRocksdbCompaction);
  common::Timer timer(kRocksdbCompactionMs);
  return db_->CompactRange(options, begin, end);
}

rocksdb::Status ApplicationDB::GetOptions(
    std::vector<std::string>& option_names,
    std::map<std::string, std::string>* options_map) {
  rocksdb::Options options = db_->GetOptions();
  std::string opts_str;
  std::unordered_map<std::string, std::string> opts_map;
  auto get_s = GetStringFromOptions(&opts_str, options);
  auto to_map_s = rocksdb::StringToMap(opts_str, &opts_map);
  if (!get_s.ok() || !to_map_s.ok()) {
    return !get_s.ok() ? get_s : to_map_s;
  }
  for (const auto& option_name : option_names) {
    if (opts_map.find(option_name) != opts_map.end()) {
      (*options_map)[option_name] = opts_map[option_name];
    } else {
      LOG(ERROR) << "Option name not found, " << option_name;
    }
  }
  return rocksdb::Status();
}

rocksdb::Status ApplicationDB::GetStringFromOptions(
    std::string* options_str, const rocksdb::Options& options,
    const std::string& delimiter) {
  std::string dboptions_str;
  std::string cfoptions_str;
  auto db_s =
      rocksdb::GetStringFromDBOptions(&dboptions_str, options, delimiter);
  auto cf_s = rocksdb::GetStringFromColumnFamilyOptions(&cfoptions_str, options,
                                                        delimiter);
  if (!db_s.ok() || !cf_s.ok()) {
    return !db_s.ok() ? db_s : cf_s;
  }
  *options_str = dboptions_str + delimiter + cfoptions_str;
  return rocksdb::Status();
}

bool ApplicationDB::GetProperty(const rocksdb::Slice& property,
                                std::string* value) {
  if (property.starts_with(applicationdb_prefix)) {
    if (property == Properties::kHighestEmptyLevel) {
      *value = std::to_string(getHighestEmptyLevel());
      return true;
    } else if (property == Properties::kNumLevels) {
      *value = std::to_string(db_->NumberLevels());
      return true;
    }
  } else if (property.starts_with(rocksdb_prefix)) {
    return db_->GetProperty(property, value);
  }
  LOG(ERROR) << "Property not defined, " << property.ToString();
  return false;
}

bool ApplicationDB::DBLmaxEmpty() {
  std::string num_levels;
  std::string highest_empty_level;
  if (GetProperty(Properties::kNumLevels, &num_levels) &&
      GetProperty(Properties::kHighestEmptyLevel, &highest_empty_level) &&
      folly::to<int32_t>(num_levels) - 1 ==
          folly::to<int32_t>(highest_empty_level)) {
    return true;
  }
  LOG(INFO) << "DBLmax not empty, num_levels: " << num_levels
            << ", highest_empty_level: " << highest_empty_level;
  return false;
}

uint32_t ApplicationDB::getHighestEmptyLevel() {
  rocksdb::ColumnFamilyMetaData cf_metadata;
  db_->GetColumnFamilyMetaData(&cf_metadata);
  std::vector<rocksdb::LevelMetaData>& level_metas = cf_metadata.levels;
  std::set<uint32_t> empty_levels;
  for (const auto& level_meta : level_metas) {
    if (level_meta.size == 0) {
      empty_levels.insert(level_meta.level);
    }
  }
  return *empty_levels.rbegin();
}

}  // namespace admin
