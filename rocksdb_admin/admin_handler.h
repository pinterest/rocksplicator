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

//
// @author bol (bol@pinterest.com)
//

#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>

#include "common/object_lock.h"
#include "common/s3util.h"
#include "rocksdb_admin/application_db_manager.h"
#ifdef PINTEREST_INTERNAL
// NEVER SET THIS UNLESS PINTEREST INTERNAL USAGE.
#include "schemas/gen-cpp2/Admin.h"
#else
#include "rocksdb_admin/gen-cpp2/Admin.h"
#endif

class KafkaWatcher;

namespace admin {

using RocksDBOptionsGeneratorType =
  std::function<rocksdb::Options(const std::string&)>;

class AdminHandler : virtual public AdminSvIf {
 public:
  AdminHandler(
    std::unique_ptr<ApplicationDBManager> db_manager,
    RocksDBOptionsGeneratorType rocksdb_options);

  virtual ~AdminHandler() {}

  void async_tm_ping(
      std::unique_ptr<apache::thrift::HandlerCallback<void>> callback) override;

  void async_tm_addDB(
      std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
          AddDBResponse>>> callback,
      std::unique_ptr<AddDBRequest> request) override;


  void async_tm_backupDB(
      std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
        BackupDBResponse>>> callback,
      std::unique_ptr<BackupDBRequest> request) override;

  void async_tm_restoreDB(
      std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
        RestoreDBResponse>>> callback,
      std::unique_ptr<RestoreDBRequest> request) override;

  void async_tm_checkDB(
      std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
          CheckDBResponse>>> callback,
      std::unique_ptr<CheckDBRequest> request) override;

  void async_tm_closeDB(
      std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
        CloseDBResponse>>> callback,
      std::unique_ptr<CloseDBRequest> request) override;

  void async_tm_changeDBRoleAndUpStream(
      std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
        ChangeDBRoleAndUpstreamResponse>>> callback,
      std::unique_ptr<
        ChangeDBRoleAndUpstreamRequest> request) override;

  void async_tm_getSequenceNumber(
      std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
        GetSequenceNumberResponse>>> callback,
      std::unique_ptr<GetSequenceNumberRequest> request) override;

  void async_tm_clearDB(
      std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
        ClearDBResponse>>> callback,
      std::unique_ptr<ClearDBRequest> request) override;

  void async_tm_addS3SstFilesToDB(
      std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
        AddS3SstFilesToDBResponse>>> callback,
      std::unique_ptr<AddS3SstFilesToDBRequest> request) override;

  void async_tm_startMessageIngestion(
      std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
          StartMessageIngestionResponse>>> callback,
      std::unique_ptr<StartMessageIngestionRequest> request) override;

  void async_tm_stopMessageIngestion(
      std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
          StopMessageIngestionResponse>>> callback,
      std::unique_ptr<StopMessageIngestionRequest> request) override;

  void async_tm_setDBOptions(
      std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
        SetDBOptionsResponse>>> callback,
      std::unique_ptr<SetDBOptionsRequest> request) override;

  void async_tm_compactDB(
      std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
        CompactDBResponse>>> callback,
      std::unique_ptr<CompactDBRequest> request) override;

  std::shared_ptr<ApplicationDB> getDB(const std::string& db_name,
                                       AdminException* ex);

  // Dump stats for all DBs as a text string
  std::string DumpDBStatsAsText() const;

 private:
  std::unique_ptr<rocksdb::DB> removeDB(const std::string& db_name,
                                        AdminException* ex);

  DBMetaData getMetaData(const std::string& db_name);
  bool clearMetaData(const std::string& db_name);
  bool writeMetaData(const std::string& db_name,
                     const std::string& s3_bucket,
                     const std::string& s3_path);

  std::unique_ptr<ApplicationDBManager> db_manager_;
  RocksDBOptionsGeneratorType rocksdb_options_;
  // Lock to synchronize DB admin operations at per DB granularity
  common::ObjectLock<std::string> db_admin_lock_;
  // S3 util used for download
  std::shared_ptr<common::S3Util> s3_util_;
  // Lock for protecting the s3 util
  mutable std::mutex s3_util_lock_;
  // db that contains meta data for all local rocksdb instances
  std::unique_ptr<rocksdb::DB> meta_db_;
  // segments which allow for overlapping keys when adding SST files
  std::unordered_set<std::string> allow_overlapping_keys_segments_;
  // number of the current concurrenty s3 downloadings
  std::atomic<int> num_current_s3_sst_downloadings_;
  // Map of db_name to kafka watcher
  std::unordered_map<std::string, std::shared_ptr<KafkaWatcher>>
    kafka_watcher_map_;
  // Lock for synchronizing access to kafka_watcher_map_
  std::mutex kafka_watcher_lock_;
};

}  // namespace admin
