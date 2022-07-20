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

#include "rocksdb_admin/application_db_backup_manager.h"

#include <thread>
#include <vector>

#include "boost/filesystem.hpp"
#include "common/file_util.h"
#include "common/stats/stats.h"
#include "common/timer.h"
#include "common/timeutil.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb_admin/gen-cpp2/rocksdb_admin_types.h"
#include "rocksdb_admin/utils.h"
#include "thrift/lib/cpp2/protocol/Serializer.h"

using admin::DBMetaData;

namespace admin {

const std::string kMetaFilename = "dbmeta";
const std::string kS3BackupMs = "s3_backup_ms";
const std::string kS3BackupFailure = "s3_backup_failure";
const int kS3UtilRecheckSec = 5;

ApplicationDBBackupManager::ApplicationDBBackupManager(
    std::shared_ptr<ApplicationDBManager> db_manager,
    const std::string& rocksdb_dir,
    uint32_t checkpoint_backup_batch_num_upload,
    const std::string& s3_bucket,
    const std::string& s3_backup_dir,
    uint32_t limit_mbs,
    bool include_meta) 
    : db_manager_(std::move(db_manager))
    , rocksdb_dir_(rocksdb_dir)
    , checkpoint_backup_batch_num_upload_(checkpoint_backup_batch_num_upload)
    , s3_bucket_(s3_bucket)
    , s3_backup_dir_(s3_backup_dir)
    , limit_mbs_(limit_mbs)
    , include_meta_(include_meta)
    , s3_util_()
    , s3_util_lock_() {}

ApplicationDBBackupManager::ApplicationDBBackupManager(
    std::shared_ptr<ApplicationDBManager> db_manager,
    const std::string& rocksdb_dir,
    uint32_t checkpoint_backup_batch_num_upload)
    : db_manager_(std::move(db_manager))
    , rocksdb_dir_(rocksdb_dir)
    , checkpoint_backup_batch_num_upload_(checkpoint_backup_batch_num_upload)
    , s3_util_()
    , s3_util_lock_() {}

void ApplicationDBBackupManager::setS3Config(
    const std::string& s3_bucket,
    const std::string& s3_backup_dir,
    uint32_t limit_mbs,
    bool include_meta) {
  s3_bucket_ = std::move(s3_bucket);
  s3_backup_dir_ = std::move(s3_backup_dir);
  limit_mbs_ = limit_mbs;
  include_meta_ = include_meta;
}

inline std::string ensure_ends_with_pathsep(const std::string& s) {
  if (!s.empty() && s.back() != '/') {
    return s + "/";
  }
  return s;
}

// copy from admin_hamdler.cpp
inline bool should_new_s3_client(
    const common::S3Util& s3_util, const uint32_t limit_mbs, const std::string& s3_bucket) {
  return s3_util.getBucket() != s3_bucket ||
         s3_util.getRateLimit() != limit_mbs;
}

// copy from admin_hamdler.cpp
std::shared_ptr<common::S3Util> ApplicationDBBackupManager::createLocalS3Util(
    const uint32_t limit_mbs,
    const std::string& s3_bucket) {
  // Though it is claimed that AWS s3 sdk is a light weight library. However,
  // we couldn't afford to create a new client for every SST file downloading
  // request, which is not even on any critical code path. Otherwise, we will
  // see latency spike when uploading data to production clusters.
  std::shared_ptr<common::S3Util> local_s3_util;

  {
    std::lock_guard<std::mutex> guard(s3_util_lock_);
    if (s3_util_ == nullptr || should_new_s3_client(*s3_util_, limit_mbs, s3_bucket)) {
      // Request with different ratelimit or bucket has to wait for old
      // requests to drain.
      while (s3_util_ != nullptr && s3_util_.use_count() > 1) {
        LOG(INFO) << "There are other downloads happening, wait "
                  << kS3UtilRecheckSec << " seconds";
        std::this_thread::sleep_for(std::chrono::seconds(kS3UtilRecheckSec));
      }
      // Invoke destructor explicitly to make sure Aws::InitAPI()
      // and Aps::ShutdownApi() appear in pairs.
      s3_util_ = nullptr;
      s3_util_ = common::S3Util::BuildS3Util(limit_mbs, s3_bucket);
    }
    local_s3_util = s3_util_;
  }

  return local_s3_util;
}

bool ApplicationDBBackupManager::backupDBToS3(
    const std::shared_ptr<ApplicationDB>& db,
    CPUThreadPoolExecutor* executor,
    std::unique_ptr<rocksdb::DB>& meta_db,
    common::ObjectLock<std::string>& db_admin_lock) {
  // ::admin::AdminException e;

  common::Timer timer(kS3BackupMs);
  LOG(INFO) << "S3 Backup " << db->db_name() << " to " << s3_backup_dir_;
  auto ts = common::timeutil::GetCurrentTimestamp();
  auto local_path = folly::stringPrintf("%ss3_tmp/%s%d/", rocksdb_dir_.c_str(), db->db_name().c_str(), ts);
  boost::system::error_code remove_err;
  boost::system::error_code create_err;
  boost::filesystem::remove_all(local_path, remove_err);
  boost::filesystem::create_directories(local_path, create_err);
  SCOPE_EXIT { boost::filesystem::remove_all(local_path, remove_err); };
  if (remove_err || create_err) {
    // SetException("Cannot remove/create dir for backup: " + local_path, AdminErrorCode::DB_ADMIN_ERROR, &callback);
    common::Stats::get()->Incr(kS3BackupFailure);
    return false;
  }

  db_admin_lock.Lock(db->db_name());
  SCOPE_EXIT { db_admin_lock.Unlock(db->db_name()); };

  rocksdb::Checkpoint* checkpoint;
  auto status = rocksdb::Checkpoint::Create(db->rocksdb(), &checkpoint);
  if (!status.ok()) {
    // OKOrSetException(status, AdminErrorCode::DB_ADMIN_ERROR, &callback);
    LOG(ERROR) << "Error happened when trying to initialize checkpoint: " << status.ToString();
    common::Stats::get()->Incr(kS3BackupFailure);
    return false;
  }

  auto checkpoint_local_path = local_path + "checkpoint";
  status = checkpoint->CreateCheckpoint(checkpoint_local_path);
  if (!status.ok()) {
    // OKOrSetException(status, AdminErrorCode::DB_ADMIN_ERROR, &callback);
    LOG(ERROR) << "Error happened when trying to create checkpoint: " << status.ToString();
    common::Stats::get()->Incr(kS3BackupFailure);
    return false;
  }
  std::unique_ptr<rocksdb::Checkpoint> checkpoint_holder(checkpoint);

  std::vector<std::string> checkpoint_files;
  status = rocksdb::Env::Default()->GetChildren(checkpoint_local_path, &checkpoint_files);
  if (!status.ok()) {
    // OKOrSetException(status, AdminErrorCode::DB_ADMIN_ERROR, &callback);
    LOG(ERROR) << "Error happened when trying to list files in the checkpoint: " << status.ToString();
    common::Stats::get()->Incr(kS3BackupFailure);
    return false;
  }

  // Upload checkpoint to s3
  auto local_s3_util = createLocalS3Util(limit_mbs_, s3_bucket_);
  std::string formatted_s3_dir_path = folly::stringPrintf("%s/%s/%d", ensure_ends_with_pathsep(checkpoint_local_path).c_str(), db->db_name().c_str(), ts);
  std::string formatted_checkpoint_local_path = ensure_ends_with_pathsep(checkpoint_local_path);
  auto upload_func = [&](const std::string& dest, const std::string& source) {
    LOG(INFO) << "Copying " << source << " to " << dest;
    auto copy_resp = local_s3_util->putObject(dest, source);
    if (!copy_resp.Error().empty()) {
      LOG(ERROR)
          << "Error happened when uploading files from checkpoint to S3: "
          << copy_resp.Error();
      return false;
    }
    return true;
  };

  if (checkpoint_backup_batch_num_upload_ > 1) {
    // Upload checkpoint files to s3 in parallel
    std::vector<std::vector<std::string>> file_batches(checkpoint_backup_batch_num_upload_);
    for (size_t i = 0; i < checkpoint_files.size(); ++i) {
      auto& file = checkpoint_files[i];
      if (file == "." || file == "..") {
        continue;
      }
      file_batches[i%checkpoint_backup_batch_num_upload_].push_back(file);
    }

    std::vector<folly::Future<bool>> futures;
    for (auto& files : file_batches) {
      auto p = folly::Promise<bool>();
      futures.push_back(p.getFuture());

      executor->add(
          [&, files = std::move(files), p = std::move(p)]() mutable {
            for (const auto& file : files) {
              if (!upload_func(formatted_s3_dir_path + file, formatted_checkpoint_local_path + file)) {
                p.setValue(false);
                return;
              }
            }
            p.setValue(true);
          });
    }

    for (auto& f : futures) {
      auto res = std::move(f).get();
      if (!res) {
        // SetException("Error happened when uploading files from checkpoint to S3",
        //               AdminErrorCode::DB_ADMIN_ERROR,
        //               &callback);
        common::Stats::get()->Incr(kS3BackupFailure);
        return false;
      }
    }
  } else {
    for (const auto& file : checkpoint_files) {
      if (file == "." || file == "..") {
        continue;
      }
      if (!upload_func(formatted_s3_dir_path + file, formatted_checkpoint_local_path + file)) {
        // If there is error in one file uploading, then we fail the whole backup process
        // SetException("Error happened when uploading files from checkpoint to S3",
        //               AdminErrorCode::DB_ADMIN_ERROR,
        //               &callback);
        common::Stats::get()->Incr(kS3BackupFailure);
        return false;
      }
    }
  }

  if (include_meta_) {
    DBMetaData meta;
    meta.db_name = db->db_name();

    std::string buffer;
    rocksdb::ReadOptions options;
    auto s = meta_db->Get(options, db->db_name(), &buffer);
    if (s.ok()) {
      apache::thrift::CompactSerializer::deserialize(buffer, meta);
    }
    std::string dbmeta_path;
    try {
      std::string encoded_meta;
      EncodeThriftStruct(meta, &encoded_meta);
      dbmeta_path = common::FileUtil::createFileWithContent(
          formatted_checkpoint_local_path, kMetaFilename, encoded_meta);
    } catch (std::exception& e) {
      // SetException("Failed to create meta file, " + std::string(e.what()),
      //               AdminErrorCode::DB_ADMIN_ERROR, &callback);
      common::Stats::get()->Incr(kS3BackupFailure);
      return false;
    }
    if (!upload_func(formatted_s3_dir_path + kMetaFilename, dbmeta_path)) {
      // SetException("Error happened when upload meta from checkpoint to S3",
      //               AdminErrorCode::DB_ADMIN_ERROR, &callback);
      common::Stats::get()->Incr(kS3BackupFailure);
      return false;
    }
  }

  // Delete the directory to remove the snapshot.
  boost::filesystem::remove_all(local_path);
}

bool ApplicationDBBackupManager::backupAllDBsToS3(
    CPUThreadPoolExecutor* executor,
    std::unique_ptr<rocksdb::DB>& meta_db,
    common::ObjectLock<std::string>& db_admin_lock) {
  if (s3_bucket_.empty() || s3_backup_dir_.empty() || !limit_mbs_) {
    LOG(INFO) << "S3 config has not been set, so incremental backup cannot be triggered";
    return false;
  }
  for (const auto& db : db_manager_->getAllDBs()) {
    LOG(INFO) << "Incremental backup for " << db.first;
    if(backupDBToS3(db.second, executor, meta_db, db_admin_lock)) {
      LOG(INFO) << "Backup for " << db.first << " succeeds";
    } else {
      LOG(INFO) << "Backup for " << db.first << " fails";
    }
  }
}

}