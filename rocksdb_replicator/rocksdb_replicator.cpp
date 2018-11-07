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

#include "rocksdb_replicator/rocksdb_replicator.h"

#include <gflags/gflags.h>

#include <string>

#include "rocksdb_replicator/replicator_handler.h"
#include "wangle/concurrent/CPUThreadPoolExecutor.h"

DEFINE_int32(rocksdb_replicator_port, 9091,
             "The port # for the internal thrift server.");

DEFINE_int32(num_replicator_io_threads, 8,
             "The number of io threads.");

DEFINE_int32(rocksdb_replicator_executor_threads, 32,
             "The number of rocksplicator executor threads.");

namespace replicator {

RocksDBReplicator::RocksDBReplicator()
    : executor_()
    , client_pool_(FLAGS_num_replicator_io_threads)
    , db_map_()
    , server_("disabled", false)
    , thread_()
    , cleaner_() {
  executor_ = std::make_unique<wangle::CPUThreadPoolExecutor>(
    std::max(FLAGS_rocksdb_replicator_executor_threads, 16),
    std::make_shared<wangle::NamedThreadFactory>("rptor-worker-"));

  server_.setInterface(std::make_unique<ReplicatorHandler>(&db_map_));
  server_.setPort(FLAGS_rocksdb_replicator_port);
  auto io_thread_pool = std::make_shared<wangle::IOThreadPoolExecutor>(
    0, std::make_shared<wangle::NamedThreadFactory>("rptor-svr-io-"));
  server_.setIOThreadPool(std::move(io_thread_pool));
  // TODO(bol) share io threads between server_ and client_pool_
  server_.setNWorkerThreads(FLAGS_num_replicator_io_threads);

  thread_ = std::thread([this] {
      LOG(INFO) << "Starting replicator server ...";
      this->server_.serve();
      LOG(INFO) << "Stoping replicator server ...";
    });
}

RocksDBReplicator::~RocksDBReplicator() {
  db_map_.clear();
  cleaner_.stopAndWait();
  server_.stop();
  thread_.join();
}

ReturnCode RocksDBReplicator::addDB(const std::string& db_name,
                                    std::shared_ptr<rocksdb::DB> db,
                                    const DBRole role,
                                    const folly::SocketAddress& upstream_addr,
                                    ReplicatedDB** replicated_db) {
  std::shared_ptr<ReplicatedDB> new_db(
    new ReplicatedDB(db_name, std::move(db), executor_.get(),
                     role, upstream_addr, &client_pool_));

  if (!db_map_.add(db_name, new_db)) {
    return ReturnCode::DB_PRE_EXIST;
  }

  if (replicated_db) {
    *replicated_db = new_db.get();
  }

  if (role == DBRole::SLAVE) {
    new_db->pullFromUpstream();
  }

  cleaner_.addDB(new_db);

  return ReturnCode::OK;
}

ReturnCode RocksDBReplicator::removeDB(const std::string& db_name) {
  std::shared_ptr<RocksDBReplicator::ReplicatedDB> db;
  auto exist = db_map_.remove(db_name, &db);
  if (!exist) {
    return ReturnCode::DB_NOT_FOUND;
  }

  std::weak_ptr<RocksDBReplicator::ReplicatedDB> weak_db(db);
  db.reset();

  while (!weak_db.expired()) {
    static const int kRemoveDBRefWaitMilliSec = 200;
    LOG(INFO) << db_name << " is still holding by others, wait "
      << kRemoveDBRefWaitMilliSec << " milliseconds";
    std::this_thread::sleep_for(
      std::chrono::milliseconds(kRemoveDBRefWaitMilliSec));
  }

  return ReturnCode::OK;
}

ReturnCode RocksDBReplicator::write(const std::string& db_name,
                                    const rocksdb::WriteOptions& options,
                                    rocksdb::WriteBatch* updates,
                                    rocksdb::SequenceNumber* seq_no) {
  std::shared_ptr<ReplicatedDB> db;
  if (!db_map_.get(db_name, &db)) {
    return ReturnCode::DB_NOT_FOUND;
  }

  try {
    auto status = db->Write(options, updates, seq_no);
    return status.ok() ? ReturnCode::OK : ReturnCode::WRITE_ERROR;
  } catch (const ReturnCode code) {
    return code;
  }
}

std::string RocksDBReplicator::getTextStats() {
  // TODO(bol) add stats
  return "TBD";
}

}  // namespace replicator
