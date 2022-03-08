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

#include "cdc_admin/cdc_application_db_manager.h"
#include "rocksdb_replicator/thrift/gen-cpp2/Replicator.h"

namespace cdc_admin {

// This class is the wrapper of the replicator
class CDCApplicationDB {
public:
  // Create a CDCApplicationDB instance
  // db_name:       (IN) name of this db instance
  // db_wrapper:    (IN) shared pointer of db wrapper instance
  // role:          (IN) replication role of this db
  // NOTE: This should always be the follower role, but it is taken in here to maintain consistency
  // with the API upstream_addr: (IN) upstream address if applicable
  CDCApplicationDB(const std::string& db_name,
                   std::shared_ptr<replicator::DbWrapper> db_wrapper,
                   replicator::ReplicaRole role,
                   std::unique_ptr<folly::SocketAddress> upstream_addr,
                   const std::string& replicator_zk_cluster,
                   const std::string& replicator_helix_cluster);

  // Name of this db
  const std::string& db_name() const { return db_name_; }

  // Return a raw pointer to the underlying db wrapper object. We don't return a
  // shared_ptr here to indicate that we must hold the outer object while using
  // the returned pointer
  replicator::DbWrapper* dbWrapper() const { return db_.get(); }

  folly::SocketAddress* upstream_addr() const { return upstream_addr_.get(); }

  ~CDCApplicationDB();

private:
  const std::string db_name_;
  std::shared_ptr<replicator::DbWrapper> db_;
  std::unique_ptr<folly::SocketAddress> upstream_addr_;
  replicator::RocksDBReplicator::ReplicatedDB* replicated_db_;

  friend class CDCApplicationDBManager<CDCApplicationDB, replicator::DbWrapper>;
};

}  // namespace cdc_admin
