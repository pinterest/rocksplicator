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

#pragma once

#include <folly/io/async/EventBase.h>

#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

#include "common/thrift_client_pool.h"
#include "rocksdb_replicator/fast_read_map.h"
#include "rocksdb_replicator/max_number_box.h"
#include "rocksdb_replicator/non_blocking_condition_variable.h"
#include "rocksdb_replicator/thrift/gen-cpp2/Replicator.h"
#include "folly/SocketAddress.h"
#include "rocksdb/db.h"
#include "thrift/lib/cpp2/server/ThriftServer.h"

namespace folly {
  class Executor;
}

namespace wangle {
  class CPUThreadPoolExecutor;
}

namespace replicator {

/*
 * An extractor to extract update time from an update
 */
struct LogExtractor : public rocksdb::WriteBatch::Handler {
 public:
  void LogData(const rocksdb::Slice& blob) override {
    CHECK(blob.size() == sizeof(ms));
    memcpy(&ms, blob.data(), sizeof(ms));
  }

  uint64_t ms;
};

enum class DBRole {
  MASTER,
  SLAVE,
  NOOP, // Don't perform any replication with this DB
};

enum class ReturnCode {
  OK = 0,
  DB_NOT_FOUND = 1,
  DB_PRE_EXIST = 2,
  WRITE_TO_SLAVE = 3,
  WRITE_ERROR = 4,
  WAIT_SLAVE_TIMEOUT = 5,
};

/*
 * All public interfaces of RocksDBReplicator are thread safe.
 */
class RocksDBReplicator {
 public:
  class ReplicatedDB : public std::enable_shared_from_this<ReplicatedDB> {
   public:
    // Similar to rocksdb::DB::Write(). Only two differences:
    // 1) seq_no, it will be filled with a sequence # after applying the
    // updates. This is useful to implement read-after-write consistency at
    // higher level.
    // 2) WRITE_TO_SLAVE will be thrown if this is a SLAVE db.
    // 3) WAIT_SLAVE_TIMEOUT will be thrown if replication mode 1 and 2 is
    // enabled, and no slave gets back to us in time. In this case, the update
    // is guaranteed to be committed to Master. Slaves may or may not have got
    // the update.
    rocksdb::Status Write(const rocksdb::WriteOptions& options,
                          rocksdb::WriteBatch* updates,
                          rocksdb::SequenceNumber* seq_no = nullptr);

    // read APIs may be added later on demand. They can be simply implmented by
    // delegating to the internal rocksdb::DB object.

   private:
    ReplicatedDB(const std::string& db_name,
                 std::shared_ptr<rocksdb::DB> db,
                 folly::Executor* executor,
                 const DBRole role,
                 const folly::SocketAddress& upstream_addr
                 = folly::SocketAddress(),
                 common::ThriftClientPool<ReplicatorAsyncClient>* client_pool
                 = nullptr);

    void pullFromUpstream();
    using CallbackType =
      apache::thrift::HandlerCallback<std::unique_ptr<ReplicateResponse>>;
    void handleReplicateRequest(std::unique_ptr<CallbackType> callback,
                                std::unique_ptr<ReplicateRequest> request);
    std::unique_ptr<rocksdb::TransactionLogIterator> getCachedIter(
        rocksdb::SequenceNumber seq_no);
    void putCachedIter(rocksdb::SequenceNumber seq_no,
                       std::unique_ptr<rocksdb::TransactionLogIterator>);
    void cleanIdleCachedIters();

    const std::string db_name_;
    std::shared_ptr<rocksdb::DB> db_;
    folly::Executor* const executor_;
    const DBRole role_;
    const folly::SocketAddress upstream_addr_;
    common::ThriftClientPool<ReplicatorAsyncClient>* const client_pool_;
    std::shared_ptr<ReplicatorAsyncClient> client_;
    detail::NonBlockingConditionVariable cond_var_;
    apache::thrift::RpcOptions rpc_options_;
    rocksdb::WriteOptions write_options_;
    std::unordered_multimap<rocksdb::SequenceNumber,
      std::pair<std::unique_ptr<rocksdb::TransactionLogIterator>,
                uint64_t>> cached_iters_;
    std::mutex cached_iters_mutex_;
    detail::MaxNumberBox max_seq_no_acked_;

    friend class ReplicatorHandler;
    friend class RocksDBReplicator;
    friend class CachedIterCleaner;
  };

  static RocksDBReplicator* instance() {
    static RocksDBReplicator instance;
    return &instance;
  }

  /*
   * Add a db to be replicated.
   * If the db is already managed by the library, DB_PRE_EXIST will be returned.
   * Otherwise, OK is returned.
   * replicated_db is an out parameter. Client may use it to read/write the
   * replicated db directly instead of using the write() function below. It is
   * valid until the subsequent call of removeDB with db_name.
   * If role is SLAVE, upstream_addr is where the library should pull updates
   * from for this db.
   */
  ReturnCode addDB(const std::string& db_name,
                   std::shared_ptr<rocksdb::DB> db,
                   const DBRole role,
                   const folly::SocketAddress& upstream_addr
                   = folly::SocketAddress(),
                   ReplicatedDB** replicated_db = nullptr);

  /*
   * Remove a db from the library.
   * Return DB_NOT_FOUND if the library is not managing this db.
   * Otherwise, OK is returned.
   */
  ReturnCode removeDB(const std::string& db_name);

  /*
   * Similar to the rocksdb::DB::Write() interface.
   * Write updates to the specified db.
   * If seq_no is not nullptr, it is filled with a sequence # after applying the
   * updates. This is useful to implement read-after-write consistency at higher
   * level.
   * If the db is SLAVE, WRITE_TO_SLAVE is returned, and the updates won't be
   * applied.
   * If rocksdb::DB::Write() fails, WRITE_ERROR is returned.
   * Otherwise, OK is returned.
   */
  ReturnCode write(const std::string& db_name,
                   const rocksdb::WriteOptions& options,
                   rocksdb::WriteBatch* updates,
                   rocksdb::SequenceNumber* seq_no = nullptr);

  /*
   * Get stats of the library in the same text format as the java ostrich
   * library.
   */
  std::string getTextStats();

  // no copy or move
  RocksDBReplicator(const RocksDBReplicator&) = delete;
  RocksDBReplicator& operator=(const RocksDBReplicator&) = delete;

 private:
  class CachedIterCleaner {
   public:
    CachedIterCleaner();
    void addDB(std::weak_ptr<ReplicatedDB> db);
    void stopAndWait();

   private:
    void scheduleCleanup();
    std::list<std::weak_ptr<ReplicatedDB>> dbs_;
    std::mutex dbs_mutex_;
    std::thread thread_;
    folly::EventBase evb_;
  };

  RocksDBReplicator();
  ~RocksDBReplicator();

  std::unique_ptr<wangle::CPUThreadPoolExecutor> executor_;

  common::ThriftClientPool<ReplicatorAsyncClient> client_pool_;

  detail::FastReadMap<std::string,
    std::shared_ptr<RocksDBReplicator::ReplicatedDB>> db_map_;

  apache::thrift::ThriftServer server_;

  std::thread thread_;

  CachedIterCleaner cleaner_;
};

}  // namespace replicator
