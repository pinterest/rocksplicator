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

#include "common/dbconfig.h"
#include "common/helix_client.h"
#include "common/network_util.h"
#include "common/segment_utils.h"
#include "common/timer.h"
#include "folly/MoveWrapper.h"
#include "folly/Random.h"
#include "rocksdb_replicator/replicator_stats.h"
#include "rocksdb_replicator/rocksdb_replicator.h"
#include "rocksdb_replicator/utils.h"

DEFINE_int32(replicator_max_server_wait_time_ms, 10 * 1000,
             "Max wait time before an empty response is returned");

DEFINE_int32(replicator_client_server_timeout_difference_ms, 10 * 1000,
             "The difference between server and client side timeouts");

DEFINE_int32(replicator_max_updates_per_response, 50,
             "Max number of RocksDB updates a response can contain");

DEFINE_int32(replicator_pull_delay_on_error_ms, 5 * 1000,
             "How long to wait before sending the next pull request on error");

// When there are no updates, it roughly takes
// replicator_pull_delay_on_error_ms * replicator_max_consecutive_no_updates_before_upstream_reset
// to trigger a upstream reset. (e.g. with 5s pull delay and 5 consecutive pulls, that's 25s wait before an upstreamReset)
// Additionally resetUpstream may be skipped as controlled by replication_error_reset_upstream_percentage (default to 10%),
// So it may take up to 250s to reset the upstream.
// This is mainly a protection against checking the upstream too often (currently adds load to ZK).
// Once we move to checking upstream based on a local shardmap file,
// we can check more aggressively.
DEFINE_int32(replicator_max_consecutive_no_updates_before_upstream_reset, 5,
             "The max number of consecutive replication responses with no updates, before a upstream reset is triggered");

DEFINE_int32(replicator_replication_mode, 0,
             "Replication mode. "
             "0: ack client once committed to Master; "
             "1: ack client once committed to Master and written to the TCP "
             "stack for the connection to one of the Slave; "
             "2: ack client once committed to Master and one of the Slaves.");

DEFINE_uint64(replicator_timeout_ms, 5 * 1000,
              "How long to wait for Slave before timeout a client write, 0 means"
              " waiting forever");

DECLARE_int32(replicator_idle_iter_timeout_ms);
DEFINE_bool(emit_stat_for_leader_behind,
            false,
            "Flag to control whether to emit a stat when the leader is behind the follower during a sync request.");
DEFINE_string(replicator_zk_cluster, "", "Zookeeper cluster");
DEFINE_string(replicator_helix_cluster, "", "Helix cluster");
DEFINE_int32(replication_error_reset_upstream_percentage, 10,
             "what percentage of replication errors should query helix for the latest leader");
DEFINE_bool(reset_upstream_on_std_exception, false, 
            "Flag to control whether to reset the upstream address for a generic exception in replication");
DEFINE_bool(reset_upstream_on_empty_updates_from_non_leader, false,
            "Flag to control whether to reset the upstream address when empty updates are provided from a non-leader upstream");
DECLARE_int32(rocksdb_replicator_port);


namespace {

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
  if (role_ == ReplicaRole::FOLLOWER) {
    throw ReturnCode::WRITE_TO_SLAVE;
  }

  auto write_begin = GetCurrentTimeMs();

  incCounter(kReplicatorWriteBytes, updates->GetDataSize(), db_name_);

  auto ms = GetCurrentTimeMs();
  updates->PutLogData(rocksdb::Slice(reinterpret_cast<const char*>(&ms),
                                     sizeof(ms)));
  auto write_leader_begin = GetCurrentTimeMs();
  auto status = db_wrapper_->WriteToLeader(options, updates);
  auto write_leader_end = GetCurrentTimeMs();
  auto write_leader_time = write_leader_begin < write_leader_end ? write_leader_end - write_leader_begin : 0;
  logMetric(kReplicatorWriteToLeaderMs, write_leader_time, db_name_);

  if (!status.ok()) {
    incCounter(kReplicatorWriteLeaderFailure, 1, db_name_);
    auto write_failure_end = GetCurrentTimeMs();
    logMetric(kReplicatorWriteFailureResponseTime, write_begin < write_failure_end? write_failure_end - write_begin : 0, db_name_);
    return status;
  }

  auto replication_mode =  common::DBConfigManager::get()->getReplicationMode(db_name_);
  // TODO(prem) : remove support for gflags soon
  // for now we have to support both till all clusters are migrated
  if (FLAGS_replicator_replication_mode > replication_mode) {
    replication_mode = FLAGS_replicator_replication_mode;
  }

  cond_var_.notifyAll();

  // TODO(bol): change it once RocksDB guarantees the sequence number is in
  // the write batch.
  auto cur_seq_no = db_wrapper_->LatestSequenceNumber();
  if (seq_no) {
    *seq_no = cur_seq_no;
  }

  switch (replication_mode) {
  case 1:
  case 2:
    // TODO(bol): This potentially could block all worker threads. We may
    // consider having a dedicated set of worker threads for admin requests,
    // and/or provide async write API when this turns out to be a problem.
    if (!max_seq_no_acked_.wait(cur_seq_no, FLAGS_replicator_timeout_ms)) {
      incCounter(kReplicatorWriteWaitTimedOut, 1, db_name_);
      LOG(ERROR) << "Failed to receive ack from follower, timing out for " << db_name_;
      return rocksdb::Status::TimedOut("Failed to receive ack from follower");
    }
    break;
  default:
    CHECK(replication_mode == 0)
      << "Invalid replicaton mode " << replication_mode;
  }
  auto write_success_end = GetCurrentTimeMs();
  logMetric(kReplicatorWriteSuccessResponseTime, write_begin < write_success_end? write_success_end - write_begin : 0, db_name_);
  incCounter(kReplicatorWriteSuccess, 1, db_name_);

  return status;
}

std::string RocksDBReplicator::ReplicatedDB::Introspect() {
  auto upstream_addr_str = common::getNetworkAddressStr(upstream_addr_);
  auto cur_seq_no = db_wrapper_->LatestSequenceNumber();

  // TODO(jz): consider json output
  std::stringstream ss;
  ss << "ReplicatedDB:" << std::endl;
  ss << "  name: " << db_name_ << std::endl;
  ss << "  ReplicaRole: " << role_str_ << std::endl;
  ss << "  upstream_addr: " << upstream_addr_str << std::endl;
  ss << "  cur_seq_no: " << cur_seq_no << std::endl;
  // TODO(jz): add max_seq_no_acked_
  return ss.str();
}


RocksDBReplicator::ReplicatedDB::ReplicatedDB(
    const std::string& db_name,
    std::shared_ptr<DbWrapper> db_wrapper,
    folly::Executor* executor,
    const ReplicaRole role,
    const folly::SocketAddress& upstream_addr,
    common::ThriftClientPool<ReplicatorAsyncClient>* client_pool,
    const std::string& replicator_zk_cluster,
    const std::string& replicator_helix_cluster)
    : db_name_(db_name)
    , db_wrapper_(std::move(db_wrapper))
    , executor_(executor)
    , role_(role)
    , role_str_(ReplicaRoleString(role))
    , upstream_addr_(upstream_addr)
    , client_pool_(client_pool)
    , replicator_zk_cluster_(replicator_zk_cluster)
    , replicator_helix_cluster_(replicator_helix_cluster)
    , client_()
    , cond_var_(executor)
    , rpc_options_()
    , write_options_()
    , cached_iters_()
    , cached_iters_mutex_() {
  if (role == ReplicaRole::FOLLOWER) {
    client_ = client_pool_->getClient(upstream_addr);
  }

  if (replicator_zk_cluster_.empty()) {
    replicator_zk_cluster_ = FLAGS_replicator_zk_cluster;
  }

  if (replicator_helix_cluster_.empty()) {
    replicator_helix_cluster_ = FLAGS_replicator_helix_cluster;
  }

  rpc_options_.setTimeout(
      std::chrono::milliseconds(
          FLAGS_replicator_max_server_wait_time_ms
          + FLAGS_replicator_client_server_timeout_difference_ms));
}

/*
 * Helper function to reset the upstream IP after querying for latest leader from helix.
 */
void RocksDBReplicator::ReplicatedDB::resetUpstream() {
  resetUpstreamAttempts_++;
  if (replicator_zk_cluster_.empty() || replicator_helix_cluster_.empty()) {
    LOG(ERROR) << "[resetUpstream] ZK cluster or helix cluster name not provided.";
    return;
  }

  // Query helix only FLAGS_replication_error_reset_upstream_percentage %ge of the requests.
  // This is to avoid excessive load to Helix (zk)
  if (folly::Random::rand32(0, 99) >= FLAGS_replication_error_reset_upstream_percentage) {
    LOG(ERROR) << "[resetUpstream] Skip checking latest upstream for " << db_name_;
    return;
  }

  auto segment_name = common::DbNameToSegment(db_name_);
  auto helix_partition_name = common::DbNameToHelixPartitionName(db_name_);
  LOG(ERROR) << "[resetUpstream] Zookeeper: " << replicator_zk_cluster_ << " cluster: " <<
    replicator_helix_cluster_ << " segment: " << segment_name
     << " helix_partition_name: " << helix_partition_name;
  auto leader_id = common::GetLeaderInstanceId(
    replicator_zk_cluster_, replicator_helix_cluster_, segment_name, helix_partition_name);
  LOG(ERROR) << "[resetUpstream] Leader for " << helix_partition_name << " is " << leader_id;

  if (!leader_id.empty()) {
    std::vector<std::string> tokens;
    folly::split("_", leader_id, tokens);
    if (tokens.size() == 2 && tokens[0] != upstream_addr_.getAddressStr()) {
      incCounter(kReplicatorLeaderReset, 1, db_name_);
      LOG(ERROR) << "[resetUpstream] Resetting upstream for " << db_name_ << " to " << tokens[0];
      upstream_addr_.setFromIpPort(tokens[0], FLAGS_rocksdb_replicator_port);
      // Update client with new upstream address.
      client_ = client_pool_->getClient(upstream_addr_);
    }
  }
}

void RocksDBReplicator::ReplicatedDB::pullFromUpstream() {
  CHECK(role_ == ReplicaRole::FOLLOWER);
  ReplicateRequest req;
  req.seq_no = db_wrapper_->LatestSequenceNumber();
  req.db_name = db_name_;
  req.max_wait_ms = FLAGS_replicator_max_server_wait_time_ms;
  req.max_updates = FLAGS_replicator_max_updates_per_response;

  incCounter(kReplicatorPullRequests, 1, db_name_);

  std::weak_ptr<ReplicatedDB> weak_db = shared_from_this();
  auto options = rpc_options_;
  common::Timer timer(kReplicatorPullLatency);
  client_->future_replicate(options, req).via(executor_)
    .then([weak_db = std::move(weak_db), timer = std::move(timer)] (folly::Try<ReplicateResponse>&& t) {
        auto db = weak_db.lock();
        if (db == nullptr) {
          return;
        }
        bool delay_next_pull = false;
        if (t.hasException()) {
          incCounter(kReplicatorPullRequestsFailure, 1, db->db_name_);
          delay_next_pull = true;
          try {
#if __GNUC__ >= 8
            t.exception().throw_exception();
#else
            t.exception().throwException();
#endif
          } catch (const ReplicateException& ex) {
            LOG(ERROR) << "ReplicateException: upstream = " << db->upstream_addr_.getAddressStr() << ", code = " << static_cast<int>(ex.code)
                       << ", message = " << ex.msg;
            incCounter(kReplicatorRemoteApplicationExceptions, 1, db->db_name_);
            
            if (ex.code == ErrorCode::SOURCE_NOT_FOUND) {
              // This could happen if this db missed a request about the latest upstream.
              // So try to reset it.
              incCounter(kReplicatorRemoteApplicationExceptionsNotFound, 1, db->db_name_);
              db->resetUpstream();
            }
          } catch (const std::exception& ex) {
            LOG(ERROR) << "std::exception: " << ex.what();
            incCounter(kReplicatorConnectionErrors, 1, db->db_name_);
            if (FLAGS_reset_upstream_on_std_exception) {
              db->resetUpstream();
            }
            db->client_ = db->client_pool_->getClient(db->upstream_addr_);
          }
        } else {
          incCounter(kReplicatorPullRequestsSuccess, 1, db->db_name_);
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
            if (!db->db_wrapper_->HandleReplicateResponse(&update)) {
              incCounter(kReplicatorHandleResponseFailure, 1, db->db_name_);
              delay_next_pull = true;
              break;
            }
          }

          if (response.__isset.role && response.role != ReplicaRole::LEADER) {
            incCounter(kReplicatorPullFromNonLeader, 1, db->db_name_);
          }

          if (!response.updates.empty()) {
            db->pullFromUpstreamNoUpdates_ = 0;
            db->cond_var_.notifyAll();
          } else {
            incCounter(kReplicatorPullRequestsNoUpdates, 1, db->db_name_);
            // no updates consecutively, and the upstream says it's NOT a leader.
            // Therefore we reset upstream.
            db->pullFromUpstreamNoUpdates_++;
            if (response.__isset.role && response.role != ReplicaRole::LEADER
                && FLAGS_reset_upstream_on_empty_updates_from_non_leader
                && db->pullFromUpstreamNoUpdates_ >= FLAGS_replicator_max_consecutive_no_updates_before_upstream_reset) {
              LOG(ERROR) << "No updates when fetching from a non-leader (" + std::string(ReplicaRoleString(response.role))
                           + ") upstream " + common::getNetworkAddressStr(db->upstream_addr_)
                         << " for " << FLAGS_replicator_max_consecutive_no_updates_before_upstream_reset
                         << " consecutive times, resetting upstream for " + db->db_name_;
              incCounter(kReplicatorResetUpstreamOnNoUpdates, 1, db->db_name_);
              db->resetUpstream();
              db->pullFromUpstreamNoUpdates_ = 0;
            }
          }
          incCounter(kReplicatorInBytes, write_bytes, db->db_name_);
        }

        if (delay_next_pull) {
          auto eb = db->client_->getChannel()->getEventBase();
          // It is very bad if we fail to rescheudle a pull request, we'd prefer
          // crashing.
          eb->runInEventBaseThread([eb, weak_db = std::move(weak_db)] {
              auto delay = FLAGS_replicator_pull_delay_on_error_ms;
              // Randomize the delay so that helix (zk) is not overloaded from ext view requests.
              auto randomized_delay = folly::Random::rand32(delay, delay * 2);
              eb->runAfterDelay([weak_db = std::move(weak_db)] {
                  auto db = weak_db.lock();
                  if (db == nullptr) {
                    return;
                  }
                  db->pullFromUpstream();
                },
                randomized_delay);
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

  // Inverse of predicate below: if requested sequence number is HIGHER than latest sequence number on leader, emit a stat)
  auto leaderSeqNum = db->db_wrapper_->LatestSequenceNumber();
  if (FLAGS_emit_stat_for_leader_behind && leaderSeqNum < seq_no) {
    logMetric(kReplicatorLeaderSequenceNumbersBehind, seq_no - leaderSeqNum, db ->db_name_);
  }

  // post the largest sequence number the Slave has committed
  max_seq_no_acked_.post(seq_no);

  auto replication_mode =  common::DBConfigManager::get()->getReplicationMode(db_name_);
  // for now we have to support both till all clusters are migrated
  if (FLAGS_replicator_replication_mode > replication_mode) {
    replication_mode = FLAGS_replicator_replication_mode;
  }

  auto timeout = request->max_wait_ms;

  cond_var_.runIfConditionOrWaitForNotify(
      // Operation
      [weak_db = std::move(weak_db),
      replication_mode,
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

        auto start_ts = GetCurrentTimeMs();

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
          status = db->db_wrapper_->GetUpdatesFromLeader(expected_seq_no, &iter);
          auto end = GetCurrentTimeMs();
          logMetric(kReplicatorGetUpdatesSinceMs, start < end ? end - start : 0,
                    db->db_name_);
        }
        if (use_cached_iter || status.ok() || status.IsNotFound()) {
          ReplicateResponse response;
          response.set_role(db->role_);
          uint64_t read_bytes = 0;
          for (int32_t i = 0;
               i < (*request)->max_updates && iter && iter->Valid();
               ++i, iter->Next()) {
            auto result = iter->GetBatch();

            // emit a metrics on missing sequence number, possibly due to WAL deletion after TTL expires.
            // ref: https://github.com/facebook/rocksdb/blob/7ae4da924ad4df9ffc04ba4b3577d1aa7025f4aa/include/rocksdb/db.h#L1417
            // "If the sequence number is non existent, it returns an iterator at the first available seq_no after the requested seq_no"
            if (i == 0 && result.sequence != expected_seq_no) {
              LOG(ERROR) << "Missing updates for " << db->db_name_ << ", expected sequence number: "
                         << expected_seq_no << ", got: " << response.updates[0].get_seq_no();
              incCounter(kReplicatorGetUpdatesMissingSequence, 1, db->db_name_);
            }

            Update update;
            update.set_seq_no(result.sequence);
            next_seq_no += result.writeBatchPtr->Count();
            const auto& str = result.writeBatchPtr->Data();
            read_bytes += str.size();
            update.raw_data = std::move(*folly::IOBuf::copyBuffer(str.data(),
                                                                  str.size()));
            LogExtractor extractor;
            auto ret = result.writeBatchPtr->Iterate(&extractor);
            if (ret.ok()) {
              update.timestamp = extractor.ms;
            } else {
              update.timestamp = 0;
              LOG(WARNING) << "Failed to extract timestamp for " << db->db_name_;
            }
            response.updates.emplace_back(std::move(update));
          }

          (*callback).release()->resultInThread(std::move(response));
          if (replication_mode == 1) {
            // post the largest sequence number we have written to the Slave.
            db->max_seq_no_acked_.post(next_seq_no - 1);
          }
          logMetric(kReplicatorOutNumUpdates, response.updates.size(), db->db_name_);
          incCounter(kReplicatorOutBytes, read_bytes, db->db_name_);

          auto end_success_ts = GetCurrentTimeMs();
          logMetric(kReplicatorReplyUpdatesSuccessLatency, start_ts < end_success_ts ? end_success_ts - start_ts : 0, db->db_name_);
        } else {
          LOG(ERROR) << "Failed to pull updates from " << db->db_name_
                     << " with error: " << status.ToString();
          incCounter(kReplicatorGetUpdatesSinceErrors, 1, db->db_name_);
          ReplicateException e;
          e.msg = status.ToString();
          e.code = ErrorCode::SOURCE_READ_ERROR;
          (*callback).release()->exceptionInThread(std::move(e));

          auto end_failure_ts = GetCurrentTimeMs();
          logMetric(kReplicatorReplyUpdatesFailureLatency, start_ts < end_failure_ts ? end_failure_ts - start_ts : 0, db->db_name_);
        }

        if (iter) {
          db->putCachedIter(next_seq_no, std::move(iter));
        }
      },
      // Predicate
      [db = std::move(db), seq_no] {
        return db->db_wrapper_->LatestSequenceNumber() > seq_no;
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
