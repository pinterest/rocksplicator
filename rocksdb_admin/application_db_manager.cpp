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
  folly::RWSpinLock::UpgradedHolder upgraded_guard(dbs_lock_);
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

  folly::RWSpinLock::WriteHolder write_guard(std::move(upgraded_guard));
  dbs_.emplace(db_name, std::move(application_db_ptr));
  return true;
}

const std::shared_ptr<ApplicationDB> ApplicationDBManager::getDB(
    const std::string& db_name,
    std::string* error_message) {
  folly::RWSpinLock::ReadHolder read_guard(dbs_lock_);
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
  folly::RWSpinLock::UpgradedHolder upgraded_guard(dbs_lock_);
  auto itor = dbs_.find(db_name);
  if (itor == dbs_.end()) {
    if (error_message) {
      *error_message = db_name + " does not exist";
    }
    return nullptr;
  }
  {
    folly::RWSpinLock::WriteHolder write_guard(std::move(upgraded_guard));
    ret = std::move(itor->second);
    dbs_.erase(itor);
  }
  waitOnApplicationDBRef(ret);
  return std::unique_ptr<rocksdb::DB>(ret->db_.get());
}

const std::unordered_map<std::string, std::shared_ptr<ApplicationDB>>
ApplicationDBManager::getDBs() {
  // return copy as a snapshot
  folly::RWSpinLock::ReadHolder read_guard(dbs_lock_);
  return dbs_;
};

ApplicationDBManager::~ApplicationDBManager() {
  auto itor = dbs_.begin();
  while (itor != dbs_.end()) {
    waitOnApplicationDBRef(itor->second);
    std::unique_ptr<rocksdb::DB>(itor->second->db_.get());
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
