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
// @author indy (indy@pinterest.com)
//

#include "cdc_admin/cdc_admin_handler.h"

#include "rocksdb_replicator/test_db_proxy.h"
#include "thrift/lib/cpp2/protocol/Serializer.h"
#include "rocksdb_replicator/thrift/gen-cpp2/Replicator.h"

DEFINE_string(rocksdb_dir, "/tmp/", "The dir for local rocksdb instances");

DECLARE_int32(rocksdb_replicator_port);

namespace {

const int64_t kMillisPerSec = 1000;
const std::string kDeleteDBFailure = "delete_db_failure";

template <typename T>
void SetException(const std::string& message,
                  const ::cdc_admin::CDCAdminErrorCode code,
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
    e.errorCode = ::cdc_admin::CDCAdminErrorCode::INVALID_UPSTREAM;
    e.message = folly::stringPrintf("Invalid ip:port %s:%d", ip.c_str(), port);
    callback->release()->exceptionInThread(std::move(e));
    return false;
  }
}

}  // anonymous namespace

namespace cdc_admin {

CDCAdminHandler::CDCAdminHandler(
    std::unique_ptr<CDCApplicationDBManager<CDCApplicationDB, replicator::DbWrapper>> db_manager)
    : db_admin_lock_(), db_manager_(std::move(db_manager)) {}

CDCAdminHandler::~CDCAdminHandler() {}

std::shared_ptr<CDCApplicationDB> CDCAdminHandler::getDB(const std::string& db_name,
                                                         CDCAdminException* ex) {
  std::string err_msg;
  auto db = db_manager_->getDB(db_name, &err_msg);
  if (db == nullptr && ex) {
    ex->errorCode = CDCAdminErrorCode::NOT_FOUND;
    ex->message = std::move(err_msg);
  }

  return db;
}

std::unique_ptr<replicator::DbWrapper> CDCAdminHandler::removeDB(const std::string& db_name,
                                                                 CDCAdminException* ex) {
  std::string err_msg;
  auto db = db_manager_->removeDB(db_name, &err_msg);
  if (db == nullptr && ex) {
    ex->errorCode = CDCAdminErrorCode::NOT_FOUND;
    ex->message = std::move(err_msg);
  }

  return db;
}

void CDCAdminHandler::async_tm_addObserver(
    std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<AddObserverResponse>>> callback,
    std::unique_ptr<AddObserverRequest> request) {
  db_admin_lock_.Lock(request->db_name);
  SCOPE_EXIT { db_admin_lock_.Unlock(request->db_name); };

  CDCAdminException e;
  auto db = getDB(request->db_name, &e);
  if (db) {
    e.errorCode = CDCAdminErrorCode::ALREADY_EXIST;
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

  // add the db to db_manager
  std::string err_msg;
  replicator::ReplicaRole role = replicator::ReplicaRole::OBSERVER;
  const std::string replicator_zk_cluster =
      request->__isset.replicator_zk_cluster ? request->replicator_zk_cluster : std::string("");
  const std::string replicator_helix_cluster = request->__isset.replicator_helix_cluster
                                                   ? request->replicator_helix_cluster
                                                   : std::string("");

  try {
    std::unique_ptr<replicator::DbWrapper> db_wrapper = getDbWrapper(request->db_name);
    if (!db_manager_->addDB(request->db_name,
                            std::move(db_wrapper),
                            role,
                            std::move(upstream_addr),
                            replicator_zk_cluster,
                            replicator_helix_cluster,
                            &err_msg)) {
      e.errorCode = CDCAdminErrorCode::ADMIN_ERROR;
      e.message = std::move(err_msg);
      callback.release()->exceptionInThread(std::move(e));
    }
  } catch (CDCAdminException& ex) {
    callback.release()->exceptionInThread(std::move(ex));
  }
  callback->result(AddObserverResponse());
}

void CDCAdminHandler::async_tm_ping(
    std::unique_ptr<apache::thrift::HandlerCallback<void>> callback) {
  callback->done();
}

void CDCAdminHandler::async_tm_checkObserver(
    std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<CheckObserverResponse>>>
        callback,
    std::unique_ptr<CheckObserverRequest> request) {
  CDCAdminException e;
  auto db = getDB(request->db_name, &e);
  if (db == nullptr) {
    callback.release()->exceptionInThread(std::move(e));
    return;
  }

  CheckObserverResponse response;
  response.set_seq_num(db->dbWrapper()->LatestSequenceNumber());

  callback->result(response);
}

void CDCAdminHandler::async_tm_removeObserver(
    std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<RemoveObserverResponse>>>
        callback,
    std::unique_ptr<RemoveObserverRequest> request) {
  db_admin_lock_.Lock(request->db_name);
  SCOPE_EXIT { db_admin_lock_.Unlock(request->db_name); };

  CDCAdminException e;
  if (removeDB(request->db_name, &e) == nullptr) {
    callback.release()->exceptionInThread(std::move(e));
    return;
  }

  callback->result(RemoveObserverResponse());
}

void CDCAdminHandler::async_tm_getSequenceNumber(
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
  response.seq_num = db->dbWrapper()->LatestSequenceNumber();
  callback->result(response);
}

std::string CDCAdminHandler::DumpDBStatsAsText() const { return db_manager_->DumpDBStatsAsText(); }

std::vector<std::string> CDCAdminHandler::getAllDBNames() { return db_manager_->getAllDBNames(); }

}  // namespace cdc_admin
