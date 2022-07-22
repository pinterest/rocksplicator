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
#include "rocksdb_replicator/thrift/gen-cpp2/Replicator.h"

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

namespace admin {

class ApplicationDBBackupManager {
 public:
  ApplicationDBBackupManager(std::shared_ptr<ApplicationDBManager> db_manager,
                                                       const std::string& rocksdb_dir,
                                                       uint32_t checkpoint_backup_batch_num_upload,
                                                       const std::string& s3_bucket,
                                                       const std::string& s3_backup_dir_prefix,
                                                       const std::string& snapshot_host_port,
                                                       uint32_t limit_mbs,
                                                       bool include_meta);
  
  ApplicationDBBackupManager(std::shared_ptr<ApplicationDBManager> db_manager,
                             const std::string& rocksdb_dir,
                             uint32_t checkpoint_backup_batch_num_upload);
  
  void setS3Config(const std::string& s3_bucket,
                   const std::string& s3_backup_dir_prefix,
                   const std::string& snapshot_host_port,
                   const uint32_t limit_mbs,
                   const bool include_meta);

  bool backupAllDBsToS3(CPUThreadPoolExecutor* executor,
                        std::unique_ptr<rocksdb::DB>& meta_db,
                        common::ObjectLock<std::string>& db_admin_lock);

  bool backupDBToS3(const std::shared_ptr<ApplicationDB>& db,
                    CPUThreadPoolExecutor* executor,
                    std::unique_ptr<rocksdb::DB>& meta_db,
                    common::ObjectLock<std::string>& db_admin_lock);

 private:
    // copy from admin_hamdler..
  std::shared_ptr<common::S3Util> createLocalS3Util(const uint32_t limit_mbs,
                                                    const std::string& s3_bucket);

  std::shared_ptr<ApplicationDBManager> db_manager_;
  std::string rocksdb_dir_;
  uint32_t checkpoint_backup_batch_num_upload_;
  std::string s3_bucket_;
  std::string s3_backup_dir_prefix_;
  std::string snapshot_host_port_;
  uint32_t limit_mbs_;
  bool include_meta_;

  std::shared_ptr<common::S3Util> s3_util_;
  mutable std::mutex s3_util_lock_;
};

}