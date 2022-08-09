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
#ifdef PINTEREST_INTERNAL
// NEVER SET THIS UNLESS PINTEREST INTERNAL USAGE.
#include "schemas/gen-cpp2/rocksdb_admin_types.h"
#else
#include "rocksdb_admin/gen-cpp2/rocksdb_admin_types.h"
#endif
#include "rocksdb_admin/utils.h"
#include "thrift/lib/cpp2/protocol/Serializer.h"

using admin::DBMetaData;

DEFINE_bool(enable_async_incremental_backup_dbs, false, "Enable incremental backup for db files");

DEFINE_int32(async_incremental_backup_dbs_frequency_sec, 5,
             "How frequently in sec to check the dbs need deleting in async way");

DEFINE_int32(async_incremental_backup_dbs_wait_sec, 60,
             "How long in sec to wait between the dbs deletion");

DEFINE_string(s3_incre_backup_bucket, "pinterest-jackson", "The s3 bucket");

DEFINE_string(s3_incre_backup_prefix, "tmp/backup_test/incremental_backup_test/",
              "The s3 key prefix for backup");

DEFINE_int32(incre_backup_limit_mbs, 100, "the rate limit for s3 client");

DEFINE_bool(incre_backup_include_meta, false, "whether to backup meta data on s3");

DECLARE_bool(s3_direct_io);

namespace {

const std::string kMetaFilename = "dbmeta";
const std::string kS3BackupMs = "s3_backup_ms";
const std::string kS3BackupFailure = "s3_backup_failure";
const int32_t kS3UtilRecheckSec = 5;
const std::string backupDesc = "backup_descriptor";
const std::string lastBackupTs = "last_backup_ts";
const std::string fileToTs = "file_to_ts";

}

namespace admin {

ApplicationDBBackupManager::ApplicationDBBackupManager(
    ApplicationDBManager* db_manager,
    CPUThreadPoolExecutor* executor,
    rocksdb::DB* meta_db,
    common::ObjectLock<std::string>* db_admin_lock,
    const std::string& rocksdb_dir,
    const int32_t checkpoint_backup_batch_num_upload)
  : db_manager_(db_manager)
  , executor_(executor)
  , meta_db_(meta_db)
  , db_admin_lock_(db_admin_lock)
  , rocksdb_dir_(rocksdb_dir)
  , checkpoint_backup_batch_num_upload_(checkpoint_backup_batch_num_upload)
  , s3_util_()
  , s3_util_lock_() 
  , stop_db_incremental_backup_thread_(false) {
  db_incremental_backup_thread_ = std::make_unique<std::thread>([this] {
    if (!folly::setThreadName("DBBackuper")) {
      LOG(ERROR) << "Failed to set thread name for DB backup thread";
    }

    LOG(INFO) << "Starting DB backup thread ...";
    while (!stop_db_incremental_backup_thread_.load()) {
      std::this_thread::sleep_for(std::chrono::seconds(FLAGS_async_incremental_backup_dbs_frequency_sec));
      backupAllDBsToS3();
    }
    LOG(INFO) << "Stopping DB backup thread ...";
  });    
}

ApplicationDBBackupManager::~ApplicationDBBackupManager() {
  stop_db_incremental_backup_thread_ = true;
  db_incremental_backup_thread_->join();
}

void ApplicationDBBackupManager::stopBackup() {
  stop_db_incremental_backup_thread_ = true;
}

inline std::string ensure_ends_with_pathsep(const std::string& s) {
  if (!s.empty() && s.back() != '/') {
    return s + "/";
  }
  return s;
}

inline bool ends_with(const std::string& str, const std::string& suffix) {
  return str.size() >= suffix.size() && 
    0 == str.compare(str.size()-suffix.size(), suffix.size(), suffix);
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

// convert file_to_ts map, last_ts to a json value.
void ApplicationDBBackupManager::makeUpBackupDescJson(const std::unordered_map<std::string, int64_t>& 
    file_to_ts, int64_t last_ts, Json::Value& contents) {
  contents.clear();
  Json::Value temp_map(Json::objectValue);
  for (auto& item : file_to_ts) {
    temp_map[item.first] = item.second;
  }
  contents[fileToTs] = temp_map;
  contents[lastBackupTs] = last_ts;
}

bool ApplicationDBBackupManager::backupDBToS3(const std::shared_ptr<ApplicationDB>& db) {
  // ::admin::AdminException e;

  common::Timer timer(kS3BackupMs);
  LOG(INFO) << "S3 Backup " << db->db_name() << " to " << ensure_ends_with_pathsep(FLAGS_s3_incre_backup_prefix) << db->db_name();
  auto ts = common::timeutil::GetCurrentTimestamp();
  std::string s3_incre_path = rocksdb_dir_ + "s3_incre/"; 
  auto local_path = folly::stringPrintf("%s/%s%ld/", s3_incre_path.c_str(), db->db_name().c_str(), ts);
  boost::system::error_code remove_err;
  boost::system::error_code create_err;
  boost::filesystem::remove_all(local_path, remove_err);
  boost::filesystem::create_directories(local_path, create_err);
  SCOPE_EXIT { boost::filesystem::remove_all(local_path, remove_err); };
  if (remove_err || create_err) {
    LOG(ERROR) << "Cannot remove/create dir for backup: " << local_path;
    common::Stats::get()->Incr(kS3BackupFailure);
    return false;
  }

  db_admin_lock_->Lock(db->db_name());
  SCOPE_EXIT { db_admin_lock_->Unlock(db->db_name()); };

  rocksdb::Checkpoint* checkpoint;
  auto status = rocksdb::Checkpoint::Create(db->rocksdb(), &checkpoint);
  if (!status.ok()) {
    LOG(ERROR) << "Error happened when trying to initialize checkpoint: " << status.ToString();
    common::Stats::get()->Incr(kS3BackupFailure);
    return false;
  }

  auto checkpoint_local_path = local_path + "checkpoint";
  status = checkpoint->CreateCheckpoint(checkpoint_local_path);
  if (!status.ok()) {
    LOG(ERROR) << "Error happened when trying to create checkpoint: " << status.ToString();
    common::Stats::get()->Incr(kS3BackupFailure);
    return false;
  }
  std::unique_ptr<rocksdb::Checkpoint> checkpoint_holder(checkpoint);

  std::vector<std::string> checkpoint_files;
  status = rocksdb::Env::Default()->GetChildren(checkpoint_local_path, &checkpoint_files);
  if (!status.ok()) {
    LOG(ERROR) << "Error happened when trying to list files in the checkpoint: " << status.ToString();
    common::Stats::get()->Incr(kS3BackupFailure);
    return false;
  }

  // find the timestamp of last backup
  int64_t last_ts = -1;
  auto itor = db_backups_.find(db->db_name());
  if (itor != db_backups_.end()) {
    last_ts = itor->second[(int)itor->second.size()-1];
  }

  // if there exists last backup, we need to get the backup_descriptor
  auto local_s3_util = createLocalS3Util(FLAGS_incre_backup_limit_mbs, FLAGS_s3_incre_backup_bucket);
  std::string formatted_s3_dir_path_upload = folly::stringPrintf("%s%s/%ld/", 
    ensure_ends_with_pathsep(FLAGS_s3_incre_backup_prefix).c_str(), db->db_name().c_str(), ts);
  std::string formatted_checkpoint_local_path = ensure_ends_with_pathsep(checkpoint_local_path);

  std::unordered_map<std::string, int64_t> file_to_ts;
  const string sst_suffix = ".sst";

  if (last_ts >= 0) {
    LOG(INFO) << "find the last backup on " << last_ts;
    std::string local_path_last_backup = folly::stringPrintf("%s/%s%ld/", s3_incre_path.c_str(), db->db_name().c_str(), last_ts) + "checkpoint/";
    std::string formatted_s3_last_backup_dir_path = folly::stringPrintf("%s%s/%ld/", 
      ensure_ends_with_pathsep(FLAGS_s3_incre_backup_prefix).c_str(), db->db_name().c_str(), last_ts);

    boost::system::error_code remove_last_backup_err;
    boost::system::error_code create_last_backup_err;
    // if we have not removed the last backup, we do not need to download it
    boost::filesystem::create_directories(local_path_last_backup, create_last_backup_err);
    SCOPE_EXIT { boost::filesystem::remove_all(local_path_last_backup, remove_last_backup_err); };
    if (create_err) {
      // SetException("Cannot remove/create dir for backup: " + local_path, AdminErrorCode::DB_ADMIN_ERROR, &callback);
      common::Stats::get()->Incr(kS3BackupFailure);
      return false;
    }

    auto resp = local_s3_util->getObject(formatted_s3_last_backup_dir_path + backupDesc, 
      local_path_last_backup + backupDesc, FLAGS_s3_direct_io);
    if (!resp.Error().empty()) {
      LOG(ERROR) << "Error happened when downloading the backup descriptor from S3 to local: "
                 << resp.Error();
      return false;
    }

    Json::Reader reader;
    Json::Value last_backup_desc;
    std::string last_backup_desc_contents;
    common::FileUtil::readFileToString(local_path_last_backup + backupDesc, &last_backup_desc_contents);
    if (!reader.parse(last_backup_desc_contents, last_backup_desc) || !last_backup_desc.isObject()) {
      LOG(ERROR) << "Error happened when parsing the backup descriptor: "
                 << resp.Error();
      return false;
    }

    const auto& file_map = last_backup_desc[fileToTs];
    for (Json::Value::const_iterator it = file_map.begin(); it != file_map.end(); ++it) {
      file_to_ts.emplace(it.key().asString(), it->asInt64());
    }

    if (file_to_ts.empty()) {
      LOG(INFO) << "The last backup of " << db->db_name() << " on timestamp " << last_ts << " is broken"; 
    } 

    // Remove all the duplicate files
    checkpoint_files.erase(std::remove_if(checkpoint_files.begin(), checkpoint_files.end(), [&](string file){
      return file_to_ts.find(file) != file_to_ts.end() && ends_with(file, sst_suffix);}), checkpoint_files.end());

    LOG(INFO) << "finish parsing last backup";
  } 

  // Put new files into the map
  for (auto& file : checkpoint_files) {
    if (ends_with(file, sst_suffix)) {
      file_to_ts.emplace(file, ts);
    }
  }

  // Create the new backup descriptor file
  Json::Value backup_desc_contents(Json::objectValue);
  makeUpBackupDescJson(file_to_ts, last_ts, backup_desc_contents);
  common::FileUtil::createFileWithContent(formatted_checkpoint_local_path, backupDesc, backup_desc_contents.toStyledString());

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

      executor_->add(
          [&, files = std::move(files), p = std::move(p)]() mutable {
            for (const auto& file : files) {
              if (!upload_func(formatted_s3_dir_path_upload + file, formatted_checkpoint_local_path + file)) {
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
        LOG(ERROR) << "Error happened when uploading files from checkpoint to S3";
        common::Stats::get()->Incr(kS3BackupFailure);
        return false;
      }
    }
  } else {
    for (const auto& file : checkpoint_files) {
      if (file == "." || file == "..") {
        continue;
      }
      if (!upload_func(formatted_s3_dir_path_upload + file, formatted_checkpoint_local_path + file)) {
        // If there is error in one file uploading, then we fail the whole backup process
        LOG(ERROR) << "Error happened when uploading files from checkpoint to S3";
        common::Stats::get()->Incr(kS3BackupFailure);
        return false;
      }
    }
  }

  if (FLAGS_incre_backup_include_meta) {
    DBMetaData meta;
    meta.db_name = db->db_name();

    std::string buffer;
    rocksdb::ReadOptions options;
    auto s = meta_db_->Get(options, db->db_name(), &buffer);
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
      LOG(ERROR) << "Failed to create meta file, " << std::string(e.what());
      common::Stats::get()->Incr(kS3BackupFailure);
      return false;
    }
    if (!upload_func(formatted_s3_dir_path_upload + kMetaFilename, dbmeta_path)) {
      LOG(ERROR) << "Error happened when upload meta from checkpoint to S3";
      common::Stats::get()->Incr(kS3BackupFailure);
      return false;
    }
  }

  // make sure the all other files has been uploaded successfully before uploading the backup descriptor
  // TODO: add another json to represent the timestamp for the latest backup, we can name it as the "backup starter"
  if (!upload_func(formatted_s3_dir_path_upload + backupDesc, 
    formatted_checkpoint_local_path + backupDesc)) {
    LOG(ERROR) << "Error happened when upload backup descriptor from checkpoint to S3";
    common::Stats::get()->Incr(kS3BackupFailure);
    return false;
  }

  auto it = db_backups_.find(db->db_name());
  if (it != db_backups_.end()) {
    it->second.emplace_back(ts);
  } else {
    db_backups_.emplace(db->db_name(), std::vector<int64_t>(1, ts));
  }

  return true;
}

bool ApplicationDBBackupManager::backupAllDBsToS3() {
  if (FLAGS_s3_incre_backup_bucket.empty() || FLAGS_s3_incre_backup_prefix.empty() || !FLAGS_incre_backup_limit_mbs) {
    LOG(INFO) << "S3 config has not been set, so incremental backup cannot be triggered";
    return false;
  }

  bool ret = true;
  for (const auto& db : db_manager_->getAllDBs()) {
    LOG(INFO) << "Incremental backup for " << db.first;
    if(backupDBToS3(db.second)) {
      LOG(INFO) << "Backup for " << db.first << " succeeds";
    } else {
      LOG(INFO) << "Backup for " << db.first << " fails";
      ret = false;
    }
  }

  return ret;
}

std::vector<int64_t> ApplicationDBBackupManager::getTimeStamp(const std::string& db) {
  if (db_backups_.find(db) != db_backups_.end()) return db_backups_[db];
  else return std::vector<int64_t>();
}

}