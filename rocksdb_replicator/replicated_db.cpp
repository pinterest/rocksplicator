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

#include <gflags/gflags.h>

#include <chrono>
#include <string>
#include <vector>

#include "rocksdb_replicator/replicator_stats.h"
#include "rocksdb_replicator/rocksdb_replicator.h"

DEFINE_int32(replicator_max_server_wait_time_ms, 10 * 1000,
             "Max wait time before an empty response is returned");

DEFINE_int32(replicator_client_server_timeout_difference_ms, 10 * 1000,
             "The difference between server and client side timeouts");

DEFINE_int32(replicator_max_updates_per_response, 50,
             "Max number of RocksDB updates a response can contain");

DEFINE_int32(replicator_pull_delay_on_error_ms, 5 * 1000,
             "How long to wait before sending the next pull request on error");

DECLARE_int32(replicator_idle_iter_timeout_ms);

namespace {

struct LogExtractor : public rocksdb::WriteBatch::Handler {
 public:
  void LogData(const rocksdb::Slice& blob) override {
    CHECK(blob.size() == sizeof(ms_));
    memcpy(&ms_, blob.data(), sizeof(ms_));
  }

  uint64_t ms_;
};

uint64_t GetCurrentTimeMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::system_clock::now().time_since_epoch()).count();
}

}  // namespace

namespace replicator {
rocksdb::Status RocksDBReplicator::ReplicatedDB::Write(
    const rocksdb::WriteOptions& options,
    rocksdb::WriteBatch* updates,
    rocksdb::SequenceNumber* seq_no) {
  if (role_ == DBRole::SLAVE) {
    throw ReturnCode::WRITE_TO_SLAVE;
  }

  incCounter(kReplicatorWriteBytes, updates->GetDataSize(), db_name_);

  auto ms = GetCurrentTimeMs();
  updates->PutLogData(rocksdb::Slice(reinterpret_cast<const char*>(&ms),
                                     sizeof(ms)));
  auto start = GetCurrentTimeMs();
  auto status = db_->Write(options, updates);
  auto end = GetCurrentTimeMs();
  logMetric(kReplicatorWriteMs, start < end ? end - start : 0, db_name_);
  if (status.ok()) {
    if (seq_no) {
      *seq_no = db_->GetLatestSequenceNumber();
    }

    cond_var_.notifyAll();
  }

  return status;
}


RocksDBReplicator::ReplicatedDB::ReplicatedDB(
    const std::string& db_name,
    std::shared_ptr<rocksdb::DB> db,
    folly::Executor* executor,
    const DBRole role,
    const folly::SocketAddress& upstream_addr,
    common::ThriftClientPool<ReplicatorAsyncClient>* client_pool)
    : db_name_(db_name)
    , db_(std::move(db))
    , executor_(executor)
    , role_(role)
    , upstream_addr_(upstream_addr)
    , client_pool_(client_pool)
    , client_()
    , cond_var_(executor)
    , rpc_options_()
    , write_options_()
    , cached_iters_()
    , cached_iters_mutex_() {
  if (role == DBRole::SLAVE) {
    client_ = client_pool_->getClient(upstream_addr);
  }

  rpc_options_.setTimeout(
      std::chrono::milliseconds(
          FLAGS_replicator_max_server_wait_time_ms
          + FLAGS_replicator_client_server_timeout_difference_ms));
}

void RocksDBReplicator::ReplicatedDB::pullFromUpstream() {
  CHECK(role_ == DBRole::SLAVE);
  ReplicateRequest req;
  req.seq_no = db_->GetLatestSequenceNumber();
  req.db_name = db_name_;
  req.max_wait_ms = FLAGS_replicator_max_server_wait_time_ms;
  req.max_updates = FLAGS_replicator_max_updates_per_response;

  std::weak_ptr<ReplicatedDB> weak_db = shared_from_this();
  auto options = rpc_options_;
  client_->future_replicate(options, req).via(executor_)
    .then([weak_db = std::move(weak_db)] (folly::Try<ReplicateResponse>&& t) {
        auto db = weak_db.lock();
        if (db == nullptr) {
          return;
        }

        bool delay_next_pull = false;
        if (t.hasException()) {
          delay_next_pull = true;
          try {
            t.exception().throwException();
          } catch (const ReplicateException& ex) {
            LOG(ERROR) << "ReplicateException: " << static_cast<int>(ex.code)
                       << " " << ex.msg;
            incCounter(kReplicatorRemoteApplicationExceptions, 1, db->db_name_);
          } catch (const std::exception& ex) {
            LOG(ERROR) << "std::exception: " << ex.what();
            incCounter(kReplicatorConnectionErrors, 1, db->db_name_);
            db->client_ = db->client_pool_->getClient(db->upstream_addr_);
          }
        } else {
          auto& response = t.value();
          uint64_t write_bytes = 0;
          const auto now = GetCurrentTimeMs();
          for (auto& update : response.updates) {
            if (update.timestamp != 0) {
              uint64_t then = update.timestamp;
              logMetric(kReplicatorLatency, then < now ? now - then : 0,
                        db->db_name_);
            }

            auto byteRange = update.raw_data.coalesce();
            write_bytes += byteRange.size();
            rocksdb::WriteBatch write_batch(
                std::string(reinterpret_cast<const char*>(byteRange.data()),
                            byteRange.size()));
            write_batch.PutLogData(
              rocksdb::Slice(reinterpret_cast<const char*>(&update.timestamp),
                             sizeof(update.timestamp)));

            auto status = db->db_->Write(db->write_options_, &write_batch);
            if (!status.ok()) {
              LOG(ERROR) << "Failed to apply updates to SLAVE " << db->db_name_
                         << " " << status.ToString();
              delay_next_pull = true;
              break;
            }
          }

          if (!response.updates.empty()) {
            db->cond_var_.notifyAll();
          }
          incCounter(kReplicatorInBytes, write_bytes, db->db_name_);
        }

        if (delay_next_pull) {
          auto eb = db->client_->getChannel()->getEventBase();
          // It is very bad if we fail to rescheudle a pull request, we'd prefer
          // crashing.
          eb->runInEventBaseThread([eb, weak_db = std::move(weak_db)] {
              eb->runAfterDelay([weak_db = std::move(weak_db)] {
                  auto db = weak_db.lock();
                  if (db == nullptr) {
                    return;
                  }
                  db->pullFromUpstream();
                },
                FLAGS_replicator_pull_delay_on_error_ms);
            });
        } else {
          db->pullFromUpstream();
        }
      });
}

void RocksDBReplicator::ReplicatedDB::handleReplicateRequest(
    std::unique_ptr<CallbackType> callback,
    std::unique_ptr<ReplicateRequest> request) {
  CHECK(request->db_name == db_name_);

  auto db = shared_from_this();
  std::weak_ptr<ReplicatedDB> weak_db = db;
  auto seq_no = static_cast<rocksdb::SequenceNumber>(request->seq_no);
  auto timeout = request->max_wait_ms;

  cond_var_.runIfConditionOrWaitForNotify(
      // Operation
      [weak_db = std::move(weak_db),
       // TODO(bol) remove folly::makeMoveWrapper() when move to gcc 5.1
       request = folly::makeMoveWrapper(std::move(request)),
       callback = folly::makeMoveWrapper(std::move(callback))] () mutable {
        auto db = weak_db.lock();
        if (db == nullptr) {
          ReplicateException e;
          e.msg = (*request)->db_name + " has been removed";
          e.code = ErrorCode::SOURCE_NOT_FOUND;
          (*callback).release()->exceptionInThread(std::move(e));
          return;
        }

        const auto expected_seq_no = (*request)->seq_no + 1;
        rocksdb::SequenceNumber next_seq_no = expected_seq_no;
        auto iter = db->getCachedIter(expected_seq_no);
        if (iter && !iter->Valid()) {
          iter->Next();
          if (!iter->Valid()) {
            // this can only happen when cond_var_ timeout, or a new log file
            // got created. Either way, it is ok (required) to create a new
            // iterator.
            iter.reset(nullptr);
          }
        }

        rocksdb::Status status;
        bool use_cached_iter = (iter != nullptr);
        if (!use_cached_iter) {
          auto start = GetCurrentTimeMs();
          status = db->db_->GetUpdatesSince(expected_seq_no, &iter);
          auto end = GetCurrentTimeMs();
          logMetric(kReplicatorGetUpdatesSinceMs, start < end ? end - start : 0,
                    db->db_name_);
        }

        if (use_cached_iter || status.ok() || status.IsNotFound()) {
          ReplicateResponse response;
          uint64_t read_bytes = 0;
          for (int32_t i = 0;
               i < (*request)->max_updates && iter && iter->Valid();
               ++i, iter->Next()) {
            auto result = iter->GetBatch();
            Update update;
            next_seq_no += result.writeBatchPtr->Count();
            const auto& str = result.writeBatchPtr->Data();
            read_bytes += str.size();
            update.raw_data = std::move(*folly::IOBuf::copyBuffer(str.data(),
                                                                  str.size()));
            LogExtractor extractor;
            auto ret = result.writeBatchPtr->Iterate(&extractor);
            if (ret.ok()) {
              update.timestamp = extractor.ms_;
            } else {
              update.timestamp = 0;
              LOG(ERROR) << "Failed to extract timestamp for " << db->db_name_;
            }
            response.updates.emplace_back(std::move(update));
          }

          (*callback).release()->resultInThread(std::move(response));
          incCounter(kReplicatorOutBytes, read_bytes, db->db_name_);
        } else {
          LOG(ERROR) << "Failed to pull updates from " << db->db_name_
                     << " with error: " << status.ToString();
          incCounter(kReplicatorGetUpdatesSinceErrors, 1, db->db_name_);
          ReplicateException e;
          e.msg = status.ToString();
          e.code = ErrorCode::SOURCE_READ_ERROR;
          (*callback).release()->exceptionInThread(std::move(e));
        }

        if (iter) {
          db->putCachedIter(next_seq_no, std::move(iter));
        }
      },
      // Predicate
      [db = std::move(db), seq_no] {
        return db->db_->GetLatestSequenceNumber() > seq_no;
      },
      // timeout
      timeout);
}

std::unique_ptr<rocksdb::TransactionLogIterator>
RocksDBReplicator::ReplicatedDB::getCachedIter(
    rocksdb::SequenceNumber seq_no) {
  std::lock_guard<std::mutex> g(cached_iters_mutex_);
  auto iter = cached_iters_.find(seq_no);
  if (iter == cached_iters_.end()) {
    return nullptr;
  }

  auto ret = std::move(iter->second.first);
  cached_iters_.erase(iter);
  return ret;
}

void RocksDBReplicator::ReplicatedDB::putCachedIter(
    rocksdb::SequenceNumber seq_no,
    std::unique_ptr<rocksdb::TransactionLogIterator> iter) {
  std::lock_guard<std::mutex> g(cached_iters_mutex_);
  cached_iters_.emplace(seq_no,
                        std::make_pair(std::move(iter), GetCurrentTimeMs()));
}

void RocksDBReplicator::ReplicatedDB::cleanIdleCachedIters() {
  auto now = GetCurrentTimeMs();
  std::lock_guard<std::mutex> g(cached_iters_mutex_);
  auto itor = cached_iters_.begin();
  while (itor != cached_iters_.end()) {
    if (itor->second.second + FLAGS_replicator_idle_iter_timeout_ms < now) {
      itor = cached_iters_.erase(itor);
      continue;
    }

    ++itor;
  }
}

}  // namespace replicator
