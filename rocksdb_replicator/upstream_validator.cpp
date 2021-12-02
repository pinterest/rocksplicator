/// Copyright 2021 Pinterest Inc.
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
// @author prem (prem@pinterest.com)
//

#include <folly/io/async/EventBase.h>
#if __GNUC__ >= 8
#include "folly/system/ThreadName.h"
#else
#include <folly/ThreadName.h>
#endif
#include <gflags/gflags.h>
#include <time.h>

#include "common/segment_utils.h"
#include "common/stats/stats.h"
#include "common/thrift_router.h"
#include "rocksdb_replicator/rocksdb_replicator.h"

// 600 seconds / 10m
#define INITIAL_DELAY 10
// TODO(prem) change to DBConfig soon
DEFINE_int32(replicator_upstreamvalidator_interval_ms, 60 * 1000, "Interval at which Upstreams are checked");

namespace replicator {


void UpstreamValidator::State::setSequenceNo(uint64_t seq_no, uint64_t lastmod) {
  this->seq_no = seq_no;
  this->lastmod = lastmod;
}

UpstreamValidator::UpstreamValidator()
    : thread_(), evb_() {
  runCount = 0;
  scheduleValidation();
  thread_ = std::thread([this] {
      if (!folly::setThreadName("UpstreamValidator")) {
        LOG(ERROR) << "failed to setThreadName() for UpstreamValidator thread";
      }

      LOG(INFO) << "starting upstream validator thread";
      this->evb_.loopForever();
      LOG(INFO) << "stopping upstream validator thread";
    });
}

void UpstreamValidator::scheduleValidation() {
  evb_.runAfterDelay([this] {
    validate();
    this->scheduleValidation();
  },
  FLAGS_replicator_upstreamvalidator_interval_ms);
}

/**
 * @brief 
 * dormant_followers = list of whose slaves whose sequence number has not progressed
 *  if dormant_followers.size() > 0:
 *    get_shardmap()
 *    for each dormant_follower:
 *      check if upstream is set correctly from shardmap
 *      if not:
 *         reset to the correct upstream
 */
void UpstreamValidator::validate() {
  static std::string localhostip = "127.0.0.1";
  static std::string localhost = "localhost";
  
  /**
   * check if the feature is disabled
   */ 

  // TODO: feature check

  auto strShardMapPath = RocksDBReplicator::instance()->strShardMapPath;

  if (strShardMapPath.empty()) {
    LOG(ERROR) << "ShardMap path not set, unable to validate...";
    return;
  }

  // load the cluster info
  std::string content;
  CHECK(folly::readFile(strShardMapPath.c_str(), content));

  auto cluster = common::parseConfig(std::move(content), "");
  CHECK(cluster);
  
  LOG(INFO) << "shardmap location: " << strShardMapPath;
  std::lock_guard<std::mutex> g(mutex_);
  runCount++;
  for (auto item : followers) {
    const auto& name = item.first;
    const auto& state = item.second;

    std::shared_ptr<RocksDBReplicator::ReplicatedDB> db;
    auto& db_map_ = RocksDBReplicator::instance()->db_map_;
    if (!db_map_.get(name, &db)) {
      // db missing.. something wrong
      LOG(ERROR) << "db missing in map : " << name;
      continue;
    }

    // get the latest seq number
    const auto seq_no = db->db_wrapper_->LatestSequenceNumber();
    if (state.seq_no != seq_no) {
      followers[name].setSequenceNo(seq_no, runCount);
    } else {
      // there has been no progress

      LOG(INFO) << "seqno has not changed for db=" << name << " seqno=" << seq_no << " after run=" << runCount;

      /**
       * check if the service has just started
       * Effectively start checking after some time
       */ 
      if (runCount < INITIAL_DELAY) {
        continue;
      }

      /**
       * Wait for atleast few runs of mistmatch to see if the system recovers
       * by itself (3 runs)
       */
      if (runCount - followers[name].lastmod < 3) {
        continue;
      }

      /**
       * Check if the upstream was reset recently 
       */

      if (runCount - followers[name].reset < 2) {
        // skip if needed
      }

      const auto upstream = db->upstream_addr_.getAddressStr();

      if (upstream == localhost || upstream == localhostip) {
         LOG(ERROR) << "localhost set as upstream for db=" << name << " up:" << upstream;
      }

      // get the segment and shardid from dbname
      auto segment = common::DbNameToSegment(name);
      int  shardid = common::ExtractShardId(name);
      if (shardid < 0) {
        LOG(ERROR) << "unable to extract shardid from " << name;
        continue;
      }

      // check if the upstream has been set correctly.
      auto leader = cluster->getLeader(segment, shardid);
      if (leader.empty()) {
        LOG(ERROR) << "No Leader set for segment=" << segment << " shard=" << shardid;
        continue;
      }

      if (upstream != leader.getAddressStr()) {
        // current upstream does NOT match the leader set in the shardmap
        LOG(INFO) << "resetting upstream ... current:" << upstream << " expected:" << leader.getAddressStr();
        
        followers[name].reset = runCount;
        // TODO() : add metric
        // set it to proper upstream !!
        db->resetUpstream(leader);
      }
    }
  }
}

bool UpstreamValidator::add(const std::string& name) {
  std::lock_guard<std::mutex> g(mutex_);
  const auto& iter = followers.find(name);
  if (iter != followers.end()) {
    // already exists
    return false;
  }
  followers[name].setSequenceNo(0, runCount);
  return true;
}

bool UpstreamValidator::remove(const std::string& name) {
  std::lock_guard<std::mutex> g(mutex_);
  const auto& iter = followers.find(name);
  if (iter == followers.end()) {
    // does not exist
    return false;
  }
  followers.erase(iter);
  return true;
}

void UpstreamValidator::stopAndWait() {
  evb_.terminateLoopSoon();
  thread_.join();
}

}  // namespace replicator
