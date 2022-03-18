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

namespace cdc_admin {

CDCApplicationDB::CDCApplicationDB(const std::string& db_name,
                                   std::shared_ptr<replicator::DbWrapper> db_wrapper,
                                   replicator::ReplicaRole role,
                                   std::unique_ptr<folly::SocketAddress> upstream_addr,
                                   const std::string& replicator_zk_cluster,
                                   const std::string& replicator_helix_cluster)
    : db_name_(db_name),
      db_(std::move(db_wrapper)),
      upstream_addr_(std::move(upstream_addr)),
      replicated_db_(nullptr) {
  if (role == replicator::ReplicaRole::OBSERVER) {
    CHECK(upstream_addr_);
    auto ret = replicator::RocksDBReplicator::instance()->addDB(
        db_name_,
        db_,
        role,
        upstream_addr_ ? *upstream_addr_ : folly::SocketAddress(),
        &replicated_db_,
        replicator_zk_cluster,
        replicator_helix_cluster);
    if (ret != replicator::ReturnCode::OK) {
      throw ret;
    }
  }
  // OBSERVER should be the only role we use for CDC
}

CDCApplicationDB::~CDCApplicationDB() {
  if (replicated_db_) {
    replicator::RocksDBReplicator::instance()->removeDB(db_name_);
  }
}

}  // namespace cdc_admin
