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

#include "cdc_admin/cdc_application_db.h"

#include <string>

#include "common/stats/stats.h"
#include "common/timer.h"
#include "folly/Conv.h"
#include "rocksdb/convenience.h"

namespace cdc_admin {

CDCApplicationDB::CDCApplicationDB(const std::string& db_name,
                                   std::shared_ptr<replicator::DbWrapper> db_wrapper,
                                   replicator::DBRole role,
                                   std::unique_ptr<folly::SocketAddress> upstream_addr)
    : db_name_(db_name),
      db_(std::move(db_wrapper)),
      role_(role),
      upstream_addr_(std::move(upstream_addr)),
      replicated_db_(nullptr) {
  if (role_ == replicator::DBRole::SLAVE) {
    CHECK(upstream_addr_);
    auto ret = replicator::RocksDBReplicator::instance()->addDB(
        db_name_,
        db_,
        role_,
        upstream_addr_ ? *upstream_addr_ : folly::SocketAddress(),
        &replicated_db_);
    if (ret != replicator::ReturnCode::OK) {
      throw ret;
    }
  } else {
    // For NOOP, do not need to initialize the replicator, just standby
  }
}

CDCApplicationDB::~CDCApplicationDB() {
  if (replicated_db_) {
    replicator::RocksDBReplicator::instance()->removeDB(db_name_);
  }
}

}  // namespace cdc_admin
