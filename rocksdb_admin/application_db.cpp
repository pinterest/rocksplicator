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

#include "common/stats/stats.h"
#include "common/timer.h"

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

}  // anonymous namespace

namespace admin {

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
  common::Stats::get()->Incr(kRocksdbGet);
  common::Timer timer(kRocksdbGetMs);
  return db_->Get(options, slice, value);
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
  return replicated_db_->Write(options, write_batch);
}

}  // namespace admin
