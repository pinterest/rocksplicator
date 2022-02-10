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

#pragma once

#include <string>

#include "folly/SocketAddress.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/write_batch.h"
#include "rocksdb_replicator/rocksdb_replicator.h"

namespace admin {

// This class is the wrapper of Rocksdb::DB, it adds the replication logic
// along with basic data operations(read/write)
class ApplicationDB {
 public:

  struct Properties {
    // "applicationdb.num-levels" - return string of the configured level of DB
    static const std::string kNumLevels;
    // "applicationdb.highest-empty-level" - return string of the highest empty
    // level number
    static const std::string kHighestEmptyLevel;
  };

  // Create a ApplicationDB instance
  // db_name:       (IN) name of this db instance
  // db:            (IN) shared pointer of rocksdb instance
  // role:          (IN) replication role of this db
  // upstream_addr: (IN) upstream address if applicable
  ApplicationDB(const std::string& db_name,
                std::shared_ptr<rocksdb::DB> db,
                replicator::DBRole role,
                std::unique_ptr<folly::SocketAddress> upstream_addr);

  // Create a rocksdb iterator based on the give options.
  // options: (IN) Read options
  //
  // Return non-null pointer on success
  rocksdb::Iterator* NewIterator(const rocksdb::ReadOptions& options);

  // Get rocksdb value for a given key
  // options: (IN) Read options
  // key: (IN) rocksdb key
  // value: (OUT) the value of the key
  //
  // Return OK status if the key exist, otherwise the status would be NotFound
  rocksdb::Status Get(const rocksdb::ReadOptions& options,
                      const rocksdb::Slice& key,
                      std::string* value);

  // Similar to the above Get(). Output a PinnableSlice instead of a string
  rocksdb::Status Get(const rocksdb::ReadOptions& options,
                      const rocksdb::Slice& key,
                      rocksdb::PinnableSlice* value);


  // MultiGet  rocksdb value for multiple keys
  // options: (IN) Read options
  // key: (IN) rocksdb keys
  // value: (OUT) the value of the keys
  //
  // Return a list of status for each key

  std::vector<rocksdb::Status> MultiGet(const rocksdb::ReadOptions& options,
                                        const std::vector<rocksdb::Slice>& keys,
                                        std::vector<std::string>* values);

  // Batch write with the given options and data.
  // options:     (IN) Write options
  // write_batch: (IN) Batch operations
  //
  // Return rocksdb::Status::ok on success
  rocksdb::Status Write(const rocksdb::WriteOptions& options,
                        rocksdb::WriteBatch* write_batch);

  // Compact the db.
  // options:     (IN) CompactRange options
  // begin:       (IN) Start key of the compaction.
  //                   If nullptr start from the very beginning.
  // end:         (IN) End key of the compaction.
  //                   If nullptr it will compact to the very last key.
  rocksdb::Status CompactRange(const rocksdb::CompactRangeOptions& options,
                               const rocksdb::Slice* begin,
                               const rocksdb::Slice* end);

  rocksdb::Status GetOptions(std::vector<std::string>& option_names,
                             std::map<std::string, std::string>* options_map);

  rocksdb::Status GetStringFromOptions(std::string* options_str,
                                       const rocksdb::Options& options,
                                       const std::string& delimiter = ";  ");

  bool GetProperty(const rocksdb::Slice& property, std::string* value);

  bool DBLmaxEmpty();

  // Whether this db instance is slave
  bool IsSlave() const { return role_ == replicator::DBRole::SLAVE; }

  // Name of this db
  const std::string& db_name() const { return db_name_; }

  // Return a raw pointer to the underlying rocksdb object. We don't return a
  // shared_ptr here to indicate that we must hold the outer object while using
  // the returned pointer
  rocksdb::DB* rocksdb() const {
    return db_.get();
  }

  folly::SocketAddress* upstream_addr() const {
    return upstream_addr_.get();
  }

  std::string Introspect();

  ~ApplicationDB();

 private:
  // get the highest empty level of default column family
  uint32_t getHighestEmptyLevel();

  const std::string db_name_;
  std::shared_ptr<rocksdb::DB> db_;

  const replicator::DBRole role_;
  std::unique_ptr<folly::SocketAddress> upstream_addr_;
  replicator::RocksDBReplicator::ReplicatedDB* replicated_db_;

  friend class ApplicationDBManager;
};

}  // namespace admin
