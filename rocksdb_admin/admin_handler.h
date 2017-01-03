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

#include <functional>
#include <memory>
#include <string>

#include "common/object_lock.h"
#include "rocksdb_admin/application_db_manager.h"
#ifdef PINTEREST_INTERNAL
#include "schemas/gen-cpp2/Admin.h"
#else
#include "rocksdb_admin/gen-cpp2/Admin.h"
#endif

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

  void async_tm_backupDB(
      std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
        BackupDBResponse>>> callback,
      std::unique_ptr<BackupDBRequest> request) override;

  void async_tm_restoreDB(
      std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
        RestoreDBResponse>>> callback,
      std::unique_ptr<RestoreDBRequest> request) override;

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

  void async_tm_setDBOptions(
      std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
        SetDBOptionsResponse>>> callback,
      std::unique_ptr<SetDBOptionsRequest> request) override;


  std::shared_ptr<ApplicationDB> getDB(const std::string& db_name,
                                       AdminException* ex);

 private:
  std::unique_ptr<rocksdb::DB> removeDB(const std::string& db_name,
                                        AdminException* ex);

  std::unique_ptr<ApplicationDBManager> db_manager_;
  RocksDBOptionsGeneratorType rocksdb_options_;
  // Lock to synchronize DB admin operations at per DB granularity
  common::ObjectLock<std::string> db_admin_lock_;
};

}  // namespace admin
