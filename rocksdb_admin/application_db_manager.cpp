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


#include "rocksdb_admin/application_db_manager.h"

#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "glog/logging.h"

namespace admin {

const int kRemoveDBRefWaitMilliSec = 200;

ApplicationDBManager::ApplicationDBManager()
    : dbs_()
    , dbs_lock_() {}

bool ApplicationDBManager::addDB(const std::string& db_name,
                                 std::unique_ptr<rocksdb::DB> db,
                                 replicator::DBRole role,
                                 std::string* error_message) {
  return addDB(db_name, std::move(db), role, nullptr, error_message);
}

bool ApplicationDBManager::addDB(const std::string& db_name,
                                 std::unique_ptr<rocksdb::DB> db,
                                 replicator::DBRole role,
                                 std::unique_ptr<folly::SocketAddress> up_addr,
                                 std::string* error_message) {
  std::unique_lock<std::shared_mutex> lock(dbs_lock_);
  if (dbs_.find(db_name) != dbs_.end()) {
    if (error_message) {
      *error_message = db_name + " has already been added";
    }
    return false;
  }
  auto rocksdb_ptr = std::shared_ptr<rocksdb::DB>(db.release(),
    [](rocksdb::DB* db){});
  auto application_db_ptr = std::make_shared<ApplicationDB>(db_name,
    std::move(rocksdb_ptr), role, std::move(up_addr));

  dbs_.emplace(db_name, std::move(application_db_ptr));
  return true;
}

const std::shared_ptr<ApplicationDB> ApplicationDBManager::getDB(
    const std::string& db_name,
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

std::unique_ptr<rocksdb::DB> ApplicationDBManager::removeDB(
    const std::string& db_name,
    std::string* error_message) {
  std::shared_ptr<ApplicationDB> ret;

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
  return std::unique_ptr<rocksdb::DB>(ret->db_.get());
}

std::string ApplicationDBManager::DumpDBStatsAsText() const {
  std::vector<std::shared_ptr<ApplicationDB>> dbs;
  {
    std::shared_lock<std::shared_mutex> lock(dbs_lock_);
    dbs.reserve(dbs_.size());
    for (const auto& db : dbs_) {
      dbs.push_back(db.second);
    }
  }

  std::string stats;
  // Add stats for DB size
  // total_sst_file_size db=abc00001: 12345
  // total_sst_file_size db=abc00002: 54321
  uint64_t sz;
  for (const auto& db : dbs) {
    if (!db->rocksdb()->GetIntProperty(
          rocksdb::DB::Properties::kTotalSstFilesSize, &sz)) {
      LOG(ERROR) << "Failed to get kTotalSstFilesSize for " << db->db_name();
      sz = 0;
    }

    stats += folly::stringPrintf("  total_sst_file_size db=%s: %" PRIu64 "\n",
                                 db->db_name().c_str(), sz);
  }

  return stats;
}

ApplicationDBManager::~ApplicationDBManager() {
  auto itor = dbs_.begin();
  while (itor != dbs_.end()) {
    waitOnApplicationDBRef(itor->second);
    // we want to first remove the ApplicationDB and then release the RocksDB
    // it contains.
    auto tmp = std::unique_ptr<rocksdb::DB>(itor->second->db_.get());
    itor = dbs_.erase(itor);
  }
}

void ApplicationDBManager::waitOnApplicationDBRef(
    const std::shared_ptr<ApplicationDB>& db) {
  while (db.use_count() > 1) {
    LOG(INFO) << db->db_name() << " is still holding by others, wait "
      << kRemoveDBRefWaitMilliSec << " milliseconds";
    std::this_thread::sleep_for(
      std::chrono::milliseconds(kRemoveDBRefWaitMilliSec));
  }
}

}  // namespace admin
