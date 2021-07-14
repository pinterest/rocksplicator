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

#include <map>
#include <shared_mutex>
#include <string>

#include "common/segment_utils.h"
#include "rocksdb/db.h"
#include "rocksdb_admin/application_db.h"

namespace cdc_admin {

template <typename ApplicationDBType, typename UnderlyingDbType> class CDCApplicationDBManager;

const int kRemoveDBRefWaitMilliSec = 200;
// This class manages application rocksdb instances, it offers functionality to
// add/remove/get application rocksdb instance.
// Note: this class is thread-safe.
template <typename ApplicationDBType, typename UnderlyingDbType> class CDCApplicationDBManager{
 public:
  CDCApplicationDBManager(): dbs_()
    , dbs_lock_() {}

  // Add a db instance into db manager.
  // db_name:        (IN) Name of the associated underlying db instance
  // db:             (IN) The unique pointer of the associated db instance
  // role:           (IN) Replicating role of the associated db instance
  // upstream_addr   (IN) Address of upstream rocksdb instance to replicate from
  // error_message: (OUT) This field will be set if something goes wrong
  //
  // Return true on success
  bool addDB(const std::string& db_name,
             std::unique_ptr<UnderlyingDbType> db,
             replicator::DBRole role,
             std::unique_ptr<folly::SocketAddress> upstream_addr,
             std::string* error_message) {
  std::unique_lock<std::shared_mutex> lock(dbs_lock_);
  if (dbs_.find(db_name) != dbs_.end()) {
    if (error_message) {
      *error_message = db_name + " has already been added";
    }
    return false;
  }
  auto underlying_db_ptr = std::shared_ptr<UnderlyingDbType>(db.release(),
    [](UnderlyingDbType* db){});
  auto application_db_ptr = std::make_shared<ApplicationDBType>(db_name,
    std::move(underlying_db_ptr), role, std::move(upstream_addr));

  dbs_.emplace(db_name, std::move(application_db_ptr));
  return true;
}

  // Get ApplicationDB instance of the given name
  // Returned db is not supposed to be long held by client
  // db_name:        (IN) Name of the ApplicationDB instance to be returned
  // error_message: (OUT) This field will be set if something goes wrong
  //
  // Return non-null pointer on success
  const std::shared_ptr<ApplicationDBType> getDB(const std::string& db_name,
                                             std::string* error_message) {

  std::shared_lock<std::shared_mutex> lock(dbs_lock_);
  auto itor = dbs_.find(db_name);
  if (itor == dbs_.end()) {
    if (error_message) {
      *error_message = db_name + " does not exist";
    }
    return nullptr;
  }
  return itor -> second;
}

  // Remove ApplicationDB instance of the given name
  // db_name:        (IN) Name of the ApplicationDB instance to be removed
  // error_message: (OUT) This field will be set if something goes wrong
  //
  // Return non-null pointer on success
  std::unique_ptr<UnderlyingDbType> removeDB(const std::string& db_name,
                                        std::string* error_message) {

  std::shared_ptr<ApplicationDBType> ret;

  {
    std::unique_lock<std::shared_mutex> lock(dbs_lock_);
    auto itor = dbs_.find(db_name);
    if (itor == dbs_.end()) {
      if (error_message) {
        *error_message = db_name + " does not exist";
      }
      return nullptr;
    }
  
    ret = std::move(itor->second);
    dbs_.erase(itor);
  }

  waitOnApplicationDBRef(ret);
  return std::unique_ptr<UnderlyingDbType>(ret->db_.get());
                                        }

  // Dump stats for all DBs as a text string
  std::string DumpDBStatsAsText() {
    // TODO(indy): Add more stats here if needed e.g. latest sequence number
    // For now just return the DB names
    const auto db_names = getAllDBNames();

  std::string stats;
  for (const auto& db_name : db_names) {
    stats += folly::stringPrintf(
        "  segment=%s db=%s\n",
        common::DbNameToSegment(db_name).c_str(), db_name.c_str());
  }

  return stats;
  }

  // Get the names of all DBs currently held by the CDCApplicationDBManager
  // This can be used if some service wants to perform some action such
  // as compaction across all dbs currently maintained.
  std::vector<std::string> getAllDBNames() {
    std::vector<std::string> db_names;
    std::shared_lock<std::shared_mutex> lock(dbs_lock_);
    db_names.reserve(dbs_.size());
    for (const auto& db : dbs_) {
        db_names.push_back(db.first);
    }
    return db_names;
  }

  ~CDCApplicationDBManager() {
  auto itor = dbs_.begin();
  while (itor != dbs_.end()) {
    waitOnApplicationDBRef(itor->second);
    // we want to first remove the ApplicationDB and then release the RocksDB
    // it contains.
    auto tmp = std::unique_ptr<UnderlyingDbType>(itor->second->db_.get());
    itor = dbs_.erase(itor);
  }
  }

 private:
  std::unordered_map<std::string, std::shared_ptr<ApplicationDBType>> dbs_;
  mutable std::shared_mutex dbs_lock_;

  void waitOnApplicationDBRef(const std::shared_ptr<ApplicationDBType>& db) {
  while (db.use_count() > 1) {
    LOG(INFO) << db->db_name() << " is still holding by others, wait "
      << kRemoveDBRefWaitMilliSec << " milliseconds";
    std::this_thread::sleep_for(
      std::chrono::milliseconds(kRemoveDBRefWaitMilliSec));
  }

};
};

}  // namespace cdc_admin
