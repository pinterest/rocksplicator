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

#include "cdc_admin/cdc_admin_handler.h"

#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "boost/filesystem.hpp"
#include "common/file_util.h"
#include "common/network_util.h"
#include "common/rocksdb_glogger/rocksdb_glogger.h"
#include "common/segment_utils.h"
#include "common/stats/stats.h"
#include "common/thrift_router.h"
#include "common/timer.h"
#include "common/timeutil.h"
#include "folly/FileUtil.h"
#include "folly/MoveWrapper.h"
#include "folly/ScopeGuard.h"
#include "folly/String.h"
#include "folly/futures/Future.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/backupable_db.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb_admin/utils.h"
#include "rocksdb_replicator/rocksdb_replicator.h"
#include "rocksdb_replicator/test_db_proxy.h"
#include "thrift/lib/cpp2/protocol/Serializer.h"
#if __GNUC__ >= 8
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "folly/system/ThreadName.h"
#else
#include "wangle/concurrent/CPUThreadPoolExecutor.h"
#endif

DEFINE_string(rocksdb_dir, "/tmp/", "The dir for local rocksdb instances");

DECLARE_int32(port);

DEFINE_string(shard_config_path, "", "Local path of file storing shard mapping for Aperture");

DECLARE_int32(rocksdb_replicator_port);

#if __GNUC__ >= 8
using folly::CPUThreadPoolExecutor;
using folly::LifoSemMPMCQueue;
using folly::QueueBehaviorIfFull;
#else
using wangle::CPUThreadPoolExecutor;
using wangle::LifoSemMPMCQueue;
using wangle::QueueBehaviorIfFull;
#endif

namespace {

const int64_t kMillisPerSec = 1000;
const std::string kDeleteDBFailure = "delete_db_failure";

rocksdb::DB* OpenMetaDB() {
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::DB* db;
  auto s = rocksdb::DB::Open(options, FLAGS_rocksdb_dir + "meta_db", &db);
  CHECK(s.ok()) << "Failed to open meta DB"
                << " with error " << s.ToString();

  return db;
}

template <typename T>
void SetException(const std::string& message,
                  const ::cdc_admin::AdminErrorCode code,
                  std::unique_ptr<T>* callback) {
  ::cdc_admin::CDCAdminException e;
  e.errorCode = code;
  e.message = message;
  callback->release()->exceptionInThread(std::move(e));
}

template <typename T>
bool SetAddressOrException(const std::string& ip,
                           const uint16_t port,
                           folly::SocketAddress* addr,
                           std::unique_ptr<T>* callback) {
  try {
    addr->setFromIpPort(ip, port);
    return true;
  } catch (...) {
    ::cdc_admin::CDCAdminException e;
    e.errorCode = ::cdc_admin::AdminErrorCode::INVALID_UPSTREAM;
    e.message = folly::stringPrintf("Invalid ip:port %s:%d", ip.c_str(), port);
    callback->release()->exceptionInThread(std::move(e));
    return false;
  }
}

}  // anonymous namespace

namespace cdc_admin {

CdcAdminHandler::CdcAdminHandler(
    std::unique_ptr<CDCApplicationDBManager<CDCApplicationDB, replicator::DbWrapper>> db_manager)
    : db_admin_lock_(), db_manager_(std::move(db_manager)), meta_db_(OpenMetaDB()) {}

CdcAdminHandler::~CdcAdminHandler() {}

std::shared_ptr<CDCApplicationDB> CdcAdminHandler::getDB(const std::string& db_name,
                                                         CDCAdminException* ex) {
  std::string err_msg;
  auto db = db_manager_->getDB(db_name, &err_msg);
  if (db == nullptr && ex) {
    ex->errorCode = AdminErrorCode::DB_NOT_FOUND;
    ex->message = std::move(err_msg);
  }

  return db;
}

std::unique_ptr<replicator::DbWrapper> CdcAdminHandler::removeDB(const std::string& db_name,
                                                                 CDCAdminException* ex) {
  std::string err_msg;
  auto db = db_manager_->removeDB(db_name, &err_msg);
  if (db == nullptr && ex) {
    ex->errorCode = AdminErrorCode::DB_NOT_FOUND;
    ex->message = std::move(err_msg);
  }

  return db;
}

DBMetaData CdcAdminHandler::getMetaData(const std::string& db_name) {
  DBMetaData meta;
  meta.db_name = db_name;

  std::string buffer;
  rocksdb::ReadOptions options;
  auto s = meta_db_->Get(options, db_name, &buffer);
  if (s.ok()) {
    apache::thrift::CompactSerializer::deserialize(buffer, meta);
  }

  return meta;
}

bool CdcAdminHandler::clearMetaData(const std::string& db_name) {
  rocksdb::WriteOptions options;
  options.sync = true;
  auto s = meta_db_->Delete(options, db_name);
  return s.ok();
}

bool CdcAdminHandler::writeMetaData(const std::string& db_name,
                                    const std::string& s3_bucket,
                                    const std::string& s3_path,
                                    const int64_t last_kafka_msg_timestamp_ms) {
  DBMetaData meta;
  meta.db_name = db_name;
  meta.set_s3_bucket(s3_bucket);
  meta.set_s3_path(s3_path);
  meta.set_last_kafka_msg_timestamp_ms(last_kafka_msg_timestamp_ms);

  std::string buffer;
  apache::thrift::CompactSerializer::serialize(meta, &buffer);

  rocksdb::WriteOptions options;
  options.sync = true;
  auto s = meta_db_->Put(options, db_name, buffer);
  return s.ok();
}

void CdcAdminHandler::async_tm_addDB(
    std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<AddDBResponse>>> callback,
    std::unique_ptr<AddDBRequest> request) {
  db_admin_lock_.Lock(request->db_name);
  SCOPE_EXIT { db_admin_lock_.Unlock(request->db_name); };

  CDCAdminException e;
  auto db = getDB(request->db_name, &e);
  if (db) {
    e.errorCode = AdminErrorCode::DB_EXIST;
    e.message = "Db already exists";
    callback.release()->exceptionInThread(std::move(e));
    return;
  }

  // Get the upstream for the db to be added
  auto upstream_addr = std::make_unique<folly::SocketAddress>();
  if (!SetAddressOrException(
          request->upstream_ip, FLAGS_rocksdb_replicator_port, upstream_addr.get(), &callback)) {
    return;
  }

  auto segment = common::DbNameToSegment(request->db_name);
  auto db_path = FLAGS_rocksdb_dir + request->db_name;

  // add the db to db_manager
  std::string err_msg;
  replicator::DBRole role = replicator::DBRole::SLAVE;
  if (request->__isset.db_role) {
    if (request->db_role == "FOLLOWER") {
      role = replicator::DBRole::SLAVE;
    } else if (request->db_role == "NOOP") {
      role = replicator::DBRole::NOOP;
    } else {
      e.errorCode = AdminErrorCode::INVALID_DB_ROLE;
      e.message = std::move(request->db_role);
      callback.release()->exceptionInThread(std::move(e));
      return;
    }
  }

  // TODO(indy): Redo meta DB based on fields actually needed
  auto meta = getMetaData(request->db_name);
  if (!meta.__isset.s3_bucket && !meta.__isset.s3_path &&
      !meta.__isset.last_kafka_msg_timestamp_ms) {
    LOG(INFO) << "No previous meta exist, write a fresh meta to metadb for db: "
              << request->db_name;
    if (!writeMetaData(request->db_name, "", "")) {
      std::string errMsg = "AddDB failed to write initial DBMetaData for " + request->db_name;
      SetException(errMsg, cdc_admin::AdminErrorCode::DB_ADMIN_ERROR, &callback);
      LOG(ERROR) << errMsg;
      return;
    }
  }

  // TODO(indy): Read sequence # from kafka
  std::unique_ptr<replicator::TestDBProxy> db_wrapper =
      std::make_unique<replicator::TestDBProxy>(request->db_name, 0);
  if (!db_manager_->addDB(
          request->db_name, std::move(db_wrapper), role, std::move(upstream_addr), &err_msg)) {
    e.errorCode = AdminErrorCode::DB_ADMIN_ERROR;
    e.message = std::move(err_msg);
    callback.release()->exceptionInThread(std::move(e));
    return;
  }
  // :+1:
  callback->result(AddDBResponse());
}

void CdcAdminHandler::async_tm_ping(
    std::unique_ptr<apache::thrift::HandlerCallback<void>> callback) {
  callback->done();
}

void CdcAdminHandler::async_tm_checkDB(
    std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<CheckDBResponse>>> callback,
    std::unique_ptr<CheckDBRequest> request) {
  CDCAdminException e;
  auto db = getDB(request->db_name, &e);
  if (db == nullptr) {
    callback.release()->exceptionInThread(std::move(e));
    return;
  }

  CheckDBResponse response;
  response.set_seq_num(db->dbWrapper()->LatestSequenceNumber());
  response.set_is_leader(db->IsFollower());

  callback->result(response);
}

void CdcAdminHandler::async_tm_closeDB(
    std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<CloseDBResponse>>> callback,
    std::unique_ptr<CloseDBRequest> request) {
  db_admin_lock_.Lock(request->db_name);
  SCOPE_EXIT { db_admin_lock_.Unlock(request->db_name); };

  CDCAdminException e;
  if (removeDB(request->db_name, &e) == nullptr) {
    callback.release()->exceptionInThread(std::move(e));
    return;
  }

  callback->result(CloseDBResponse());
}

void CdcAdminHandler::async_tm_changeDBRoleAndUpStream(
    std::unique_ptr<
        apache::thrift::HandlerCallback<std::unique_ptr<ChangeDBRoleAndUpstreamResponse>>> callback,
    std::unique_ptr<ChangeDBRoleAndUpstreamRequest> request) {
  db_admin_lock_.Lock(request->db_name);
  SCOPE_EXIT { db_admin_lock_.Unlock(request->db_name); };

  CDCAdminException e;
  replicator::DBRole new_role;
  if (request->new_role == "STANDBY") {
    new_role = replicator::DBRole::NOOP;
  } else if (request->new_role == "FOLLOWER") {
    new_role = replicator::DBRole::SLAVE;
  } else {
    e.errorCode = AdminErrorCode::INVALID_DB_ROLE;
    e.message = std::move(request->new_role);
    callback.release()->exceptionInThread(std::move(e));
    return;
  }

  std::unique_ptr<folly::SocketAddress> upstream_addr(nullptr);
  if (new_role == replicator::DBRole::SLAVE && request->__isset.upstream_ip &&
      request->__isset.upstream_port) {
    upstream_addr = std::make_unique<folly::SocketAddress>();
    if (!SetAddressOrException(
            request->upstream_ip, FLAGS_rocksdb_replicator_port, upstream_addr.get(), &callback)) {
      return;
    }
  }

  auto db = removeDB(request->db_name, &e);
  if (db == nullptr) {
    callback.release()->exceptionInThread(std::move(e));
    return;
  }

  std::string err_msg;
  if (!db_manager_->addDB(
          request->db_name, std::move(db), new_role, std::move(upstream_addr), &err_msg)) {
    e.errorCode = AdminErrorCode::DB_ADMIN_ERROR;
    e.message = std::move(err_msg);
    callback.release()->exceptionInThread(std::move(e));
    return;
  }

  callback->result(ChangeDBRoleAndUpstreamResponse());
}

void CdcAdminHandler::async_tm_getSequenceNumber(
    std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<GetSequenceNumberResponse>>>
        callback,
    std::unique_ptr<GetSequenceNumberRequest> request) {
  CDCAdminException e;
  std::shared_ptr<CDCApplicationDB> db = getDB(request->db_name, &e);
  if (db == nullptr) {
    callback.release()->exceptionInThread(std::move(e));
    return;
  }

  GetSequenceNumberResponse response;
  callback->result(response);
}

std::string CdcAdminHandler::DumpDBStatsAsText() const { return db_manager_->DumpDBStatsAsText(); }

std::vector<std::string> CdcAdminHandler::getAllDBNames() { return db_manager_->getAllDBNames(); }

}  // namespace cdc_admin
