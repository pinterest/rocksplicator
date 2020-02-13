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

#include <folly/io/async/EventBase.h>
#if __GNUC__ >= 8
#include "folly/system/ThreadName.h"
#else
#include <folly/ThreadName.h>
#endif
#include <gflags/gflags.h>

#include "rocksdb_replicator/rocksdb_replicator.h"

DEFINE_int32(replicator_idle_iter_timeout_ms, 60 * 1000,
             "Timeout value after which idle cached iters are removed");

namespace replicator {

RocksDBReplicator::CachedIterCleaner::CachedIterCleaner()
    : dbs_(), dbs_mutex_(), thread_(), evb_() {
  scheduleCleanup();
  thread_ = std::thread([this] {
      if (!folly::setThreadName("IterCleaner")) {
        LOG(ERROR) << "Failed to setThreadName() for CachedIterCleaner thread";
      }

      LOG(INFO) << "Starting idle iter cleanup thread ...";
      this->evb_.loopForever();
      LOG(INFO) << "Stopping idle iter cleanup thread ...";
    });
}

void RocksDBReplicator::CachedIterCleaner::scheduleCleanup() {
  evb_.runAfterDelay([this] {
        {
          std::lock_guard<std::mutex> g(dbs_mutex_);
          auto itor = dbs_.begin();
          while (itor != dbs_.end()) {
            auto db = itor->lock();
            if (db == nullptr) {
              itor = dbs_.erase(itor);
              continue;
            }

            db->cleanIdleCachedIters();
            ++itor;
          }
        }
        this->scheduleCleanup();
  },
  FLAGS_replicator_idle_iter_timeout_ms);
}

void RocksDBReplicator::CachedIterCleaner::addDB(
    std::weak_ptr<ReplicatedDB> db) {
  std::lock_guard<std::mutex> g(dbs_mutex_);
  dbs_.emplace_back(std::move(db));
}

void RocksDBReplicator::CachedIterCleaner::stopAndWait() {
  evb_.terminateLoopSoon();
  thread_.join();
}

}  // namespace replicator
