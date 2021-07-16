
/// Copyright 2021 Pinterest Inc.
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
// @author indy (indy@pinterest.com)
//

#pragma once

#include "cdc_admin/cdc_application_db.h"
#include "common/object_lock.h"
#ifdef PINTEREST_INTERNAL
// NEVER SET THIS UNLESS PINTEREST INTERNAL USAGE.
#include "schemas/gen-cpp2/Admin.h"
#else
#include "cdc_admin/gen-cpp2/CdcAdmin.h"
#endif

namespace cdc_admin {

class CdcAdminHandler : virtual public CdcAdminSvIf {
public:
  CdcAdminHandler(
      std::unique_ptr<CDCApplicationDBManager<CDCApplicationDB, replicator::DbWrapper>> db_manager);

  virtual ~CdcAdminHandler();

  void async_tm_ping(std::unique_ptr<apache::thrift::HandlerCallback<void>> callback) override;

  void async_tm_addObserver(
      std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<AddObserverResponse>>>
          callback,
      std::unique_ptr<AddObserverRequest> request) override;

  void async_tm_checkObserver(
      std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<CheckObserverResponse>>>
          callback,
      std::unique_ptr<CheckObserverRequest> request) override;

  void async_tm_removeObserver(
      std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<RemoveObserverResponse>>>
          callback,
      std::unique_ptr<RemoveObserverRequest> request) override;

  void async_tm_getSequenceNumber(
      std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<GetSequenceNumberResponse>>>
          callback,
      std::unique_ptr<GetSequenceNumberRequest> request) override;

  std::shared_ptr<CDCApplicationDB> getDB(const std::string& db_name, CDCAdminException* ex);

  // Dump stats for all DBs as a text string
  std::string DumpDBStatsAsText() const;

  // Get all the db names held by the AdminHandler
  std::vector<std::string> getAllDBNames();

protected:
  // Lock to synchronize DB admin operations at per DB granularity.
  // Put db_admin_lock in protected to provide flexibility
  // of overriding some admin functions
  common::ObjectLock<std::string> db_admin_lock_;

private:
  std::unique_ptr<replicator::DbWrapper> removeDB(const std::string& db_name,
                                                  CDCAdminException* ex);

  std::unique_ptr<CDCApplicationDBManager<CDCApplicationDB, replicator::DbWrapper>> db_manager_;
};

}  // namespace cdc_admin
