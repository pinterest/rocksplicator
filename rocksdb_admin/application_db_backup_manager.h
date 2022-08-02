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

#include "common/object_lock.h"
#include "common/s3util.h"
#include "rocksdb/db.h"
#include "rocksdb_admin/application_db_manager.h"

#if __GNUC__ >= 8
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "folly/system/ThreadName.h"
#else
#include "wangle/concurrent/CPUThreadPoolExecutor.h"
#endif

#if __GNUC__ >= 8
using folly::CPUThreadPoolExecutor;
#else
using wangle::CPUThreadPoolExecutor;
#endif

DECLARE_bool(enable_async_incremental_backup_dbs);

DECLARE_int32(async_incremental_backup_dbs_frequency_sec);

DECLARE_int32(async_incremental_backup_dbs_wait_sec);

DECLARE_string(s3_incre_backup_bucket);

DECLARE_string(s3_incre_backup_prefix);

DECLARE_int32(incre_backup_limit_mbs);

DECLARE_bool(incre_backup_include_meta);

namespace admin {

class ApplicationDBBackupManager {
 public:
  
  ApplicationDBBackupManager(
    ApplicationDBManager* db_manager,
    CPUThreadPoolExecutor* executor,
    rocksdb::DB* meta_db,
    common::ObjectLock<std::string>* db_admin_lock,
    const std::string& rocksdb_dir,
    const int32_t checkpoint_backup_batch_num_upload);
  
  ~ApplicationDBBackupManager();

  void stopBackup();

  bool backupAllDBsToS3();

  bool backupDBToS3(const std::shared_ptr<ApplicationDB>& db);

 private:
    // copy from admin_hamdler..
  std::shared_ptr<common::S3Util> createLocalS3Util(const uint32_t limit_mbs,
                                                    const std::string& s3_bucket);

  ApplicationDBManager* db_manager_;
  CPUThreadPoolExecutor* executor_;
  rocksdb::DB* meta_db_;
  common::ObjectLock<std::string>* db_admin_lock_; 
  std::string rocksdb_dir_;
  int32_t checkpoint_backup_batch_num_upload_;

  // used to store all backups for each db. 
  // I only store the timestamps since I use that to name different backups.
  std::unordered_map<std::string, std::vector<int64_t>> db_backups_;

  std::shared_ptr<common::S3Util> s3_util_;
  mutable std::mutex s3_util_lock_;

  std::unique_ptr<std::thread> db_incremental_backup_thread_;
  std::atomic<bool> stop_db_incremental_backup_thread_;
};

}