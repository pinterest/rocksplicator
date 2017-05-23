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


#include "rocksdb_admin/admin_handler.h"

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "boost/filesystem.hpp"
#include "common/network_util.h"
#include "common/rocksdb_glogger/rocksdb_glogger.h"
#include "common/thrift_router.h"
#include "folly/FileUtil.h"
#include "folly/ScopeGuard.h"
#include "folly/String.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/backupable_db.h"
#include "rocksdb_admin/utils.h"


DEFINE_string(hdfs_name_node, "hdfs://hdfsbackup-a02-namenode-001:8020",
              "The hdfs name node used for backup");

DEFINE_string(rocksdb_dir, "/data/xvdb/aperture/",
              "The dir for local rocksdb instances");

DEFINE_int32(num_hdfs_access_threads, 8,
             "The number of threads for backup or restore to/from HDFS");

DEFINE_int32(port, 9090, "Port of the server");

DEFINE_string(shard_config_path, "",
             "Local path of file storing shard mapping for Aperture");

DEFINE_bool(compact_db_after_load_sst, false,
            "Compact DB after loading SST files");

DECLARE_int32(rocksdb_replicator_port);

DEFINE_bool(s3_direct_io, false, "Whether to enable direct I/O for s3 client");

namespace {

const int kMB = 1024 * 1024;

std::unique_ptr<rocksdb::DB> GetRocksdb(const std::string& dir,
                                        const rocksdb::Options& options) {
  rocksdb::DB* db;
  auto s = rocksdb::DB::Open(options, dir, &db);
  if (!s.ok()) {
    LOG(ERROR) << "Failed to create db at " << dir << " with error "
               << s.ToString();
    return nullptr;
  }

  return std::unique_ptr<rocksdb::DB>(db);
}

folly::Future<std::unique_ptr<rocksdb::DB>> GetRocksdbFuture(
    const std::string& dir,
    const rocksdb::Options&options) {
  folly::Promise<std::unique_ptr<rocksdb::DB>> promise;
  auto future = promise.getFuture();

  std::thread opener([dir, options,
                      promise = std::move(promise)] () mutable {
      LOG(INFO) << "Start opening " << dir;
      promise.setValue(GetRocksdb(dir, options));
      LOG(INFO) << "Finished opening " << dir;
    });

  opener.detach();

  return future;
}


std::unique_ptr<::admin::ApplicationDBManager> CreateDBBasedOnConfig(
    const admin::RocksDBOptionsGeneratorType& rocksdb_options) {
  auto db_manager = std::make_unique<::admin::ApplicationDBManager>();
  std::string content;
  CHECK(folly::readFile(FLAGS_shard_config_path.c_str(), content));

  auto cluster_layout = common::parseConfig(std::move(content));
  CHECK(cluster_layout);

  folly::SocketAddress local_addr(common::getLocalIPAddress(), FLAGS_port);

  std::vector<std::function<void(void)>> ops;
  for (const auto& segment : cluster_layout->segments) {
    int shard_id = -1;
    for (const auto& shard : segment.second.shard_to_hosts) {
      ++shard_id;
      bool do_i_own_it = false;
      common::detail::Role my_role;

      for (const auto& host : shard) {
        if (host.first->addr == local_addr) {
          do_i_own_it = true;
          my_role = host.second;
          break;
        }
      }

      if (!do_i_own_it) {
        continue;
      }

      auto db_name = admin::SegmentToDbName(segment.first.c_str(), shard_id);
      auto options = rocksdb_options(segment.first);
      auto db_future = GetRocksdbFuture(FLAGS_rocksdb_dir + db_name, options);
      std::unique_ptr<folly::SocketAddress> upstream_addr(nullptr);
      if (my_role == common::detail::Role::SLAVE) {
        for (const auto& host : shard) {
          if (host.second == common::detail::Role::MASTER) {
            upstream_addr =
              std::make_unique<folly::SocketAddress>(host.first->addr);
            upstream_addr->setPort(FLAGS_rocksdb_replicator_port);
            break;
          }
        }
      }

      ops.push_back(
        [db_name = std::move(db_name),
         db_future = folly::makeMoveWrapper(std::move(db_future)),
         upstream_addr = folly::makeMoveWrapper(std::move(upstream_addr)),
         my_role, &db_manager] () mutable {
          std::string err_msg;
          auto db = (*db_future).get();
          CHECK(db);
          if (my_role == common::detail::Role::MASTER) {
            LOG(ERROR) << "Hosting master " << db_name;
            CHECK(db_manager->addDB(db_name, std::move(db),
                                    replicator::DBRole::MASTER,
                                    &err_msg)) << err_msg;
            return;
          }

          CHECK(my_role == common::detail::Role::SLAVE);
          LOG(ERROR) << "Hosting slave " << db_name;
          CHECK(db_manager->addDB(db_name, std::move(db),
                                  replicator::DBRole::SLAVE,
                                  std::move(*upstream_addr),
                                  &err_msg)) << err_msg;
        });
    }
  }

  for (auto& op : ops) {
    op();
  }

  return db_manager;
}

template<typename T>
bool OKOrSetException(const rocksdb::Status& status,
                      const ::admin::AdminErrorCode code,
                      std::unique_ptr<T>* callback) {
  if (status.ok()) {
    return true;
  }

  ::admin::AdminException e;
  e.errorCode = code;
  e.message = status.ToString();
  callback->release()->exceptionInThread(std::move(e));
  return false;
}

template<typename T>
bool SetAddressOrException(const std::string& ip,
                           const uint16_t port,
                           folly::SocketAddress* addr,
                           std::unique_ptr<T>* callback) {
  try {
    addr->setFromIpPort(ip, port);
    return true;
  } catch (...) {
    ::admin::AdminException e;
    e.errorCode = ::admin::AdminErrorCode::INVALID_UPSTREAM;
    e.message = folly::stringPrintf("Invalid ip:port %s:%d", ip.c_str(), port);
    callback->release()->exceptionInThread(std::move(e));
    return false;
  }
}

}  // anonymous namespace

namespace admin {

AdminHandler::AdminHandler(
    std::unique_ptr<ApplicationDBManager> db_manager,
    RocksDBOptionsGeneratorType rocksdb_options)
  : db_manager_(std::move(db_manager))
  , rocksdb_options_(std::move(rocksdb_options))
  , db_admin_lock_() {
  if (db_manager_ == nullptr) {
    db_manager_ = CreateDBBasedOnConfig(rocksdb_options_);
  }
}


std::shared_ptr<ApplicationDB> AdminHandler::getDB(
    const std::string& db_name,
    AdminException* ex) {
  std::string err_msg;
  auto db = db_manager_->getDB(db_name, &err_msg);
  if (db == nullptr && ex) {
    ex->errorCode = AdminErrorCode::DB_NOT_FOUND;
    ex->message = std::move(err_msg);
  }

  return db;
}

std::unique_ptr<rocksdb::DB> AdminHandler::removeDB(
    const std::string& db_name,
    AdminException* ex) {
  std::string err_msg;
  auto db = db_manager_->removeDB(db_name, &err_msg);
  if (db == nullptr && ex) {
    ex->errorCode = AdminErrorCode::DB_NOT_FOUND;
    ex->message = std::move(err_msg);
  }

  return db;
}

void AdminHandler::async_tm_ping(
    std::unique_ptr<apache::thrift::HandlerCallback<void>> callback) {
    callback->done();
}

void AdminHandler::async_tm_backupDB(
    std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
      BackupDBResponse>>> callback,
    std::unique_ptr<BackupDBRequest> request) {
  db_admin_lock_.Lock(request->db_name);
  SCOPE_EXIT { db_admin_lock_.Unlock(request->db_name); };

  AdminException e;
  auto db = getDB(request->db_name, &e);
  if (db == nullptr) {
    callback.release()->exceptionInThread(std::move(e));
    return;
  }

  auto full_path = FLAGS_hdfs_name_node + request->hdfs_backup_dir;
  LOG(INFO) << "Backup " << request->db_name << " to " << full_path;

  rocksdb::Env* hdfs_env;
  auto status = rocksdb::NewHdfsEnv(&hdfs_env, full_path);
  if (!OKOrSetException(status,
                        AdminErrorCode::DB_ADMIN_ERROR,
                        &callback)) {
    return;
  }
  std::unique_ptr<rocksdb::Env> hdfs_env_holder(hdfs_env);

  rocksdb::BackupableDBOptions options(full_path);
  common::RocksdbGLogger logger;
  options.info_log = &logger;
  options.max_background_operations = FLAGS_num_hdfs_access_threads;
  if (request->__isset.limit_mbs && request->limit_mbs > 0) {
    options.backup_rate_limit = request->limit_mbs * kMB;
  }
  options.backup_env = hdfs_env;

  rocksdb::BackupEngine* backup_engine;
  status = rocksdb::BackupEngine::Open(
    rocksdb::Env::Default(), options, &backup_engine);
  if (!OKOrSetException(status,
                        AdminErrorCode::DB_ADMIN_ERROR,
                        &callback)) {
    return;
  }
  std::unique_ptr<rocksdb::BackupEngine> backup_engine_holder(backup_engine);

  status = backup_engine->CreateNewBackup(db->rocksdb());
  if (!OKOrSetException(status,
                        AdminErrorCode::DB_ADMIN_ERROR,
                        &callback)) {
    return;
  }

  LOG(INFO) << "Backup is done.";
  callback->result(BackupDBResponse());
}

void AdminHandler::async_tm_restoreDB(
    std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
      RestoreDBResponse>>> callback,
    std::unique_ptr<RestoreDBRequest> request) {
  db_admin_lock_.Lock(request->db_name);
  SCOPE_EXIT { db_admin_lock_.Unlock(request->db_name); };

  AdminException e;
  auto upstream_addr = std::make_unique<folly::SocketAddress>();
  if (!SetAddressOrException(request->upstream_ip,
                             FLAGS_rocksdb_replicator_port,
                             upstream_addr.get(),
                             &callback)) {
    return;
  }

  auto db = db_manager_->getDB(request->db_name, nullptr);
  if (db) {
    e.errorCode = AdminErrorCode::DB_EXIST;
    e.message = "Could not restore an opened DB, close it first";
    callback.release()->exceptionInThread(std::move(e));
    return;
  }

  auto full_path = FLAGS_hdfs_name_node + request->hdfs_backup_dir;
  LOG(INFO) << "Restore " << request->db_name << " from " << full_path;

  rocksdb::Env* hdfs_env;
  auto status = rocksdb::NewHdfsEnv(&hdfs_env, full_path);
  if (!OKOrSetException(status,
                        AdminErrorCode::DB_ADMIN_ERROR,
                        &callback)) {
    return;
  }
  std::unique_ptr<rocksdb::Env> hdfs_env_holder(hdfs_env);

  rocksdb::BackupableDBOptions options(full_path);
  common::RocksdbGLogger logger;
  options.info_log = &logger;
  options.max_background_operations = FLAGS_num_hdfs_access_threads;
  if (request->__isset.limit_mbs && request->limit_mbs > 0) {
    options.restore_rate_limit = request->limit_mbs * kMB;
  }
  options.backup_env = hdfs_env;

  rocksdb::BackupEngine* backup_engine;
  status = rocksdb::BackupEngine::Open(
    rocksdb::Env::Default(), options, &backup_engine);
  if (!OKOrSetException(status,
                        AdminErrorCode::DB_ADMIN_ERROR,
                        &callback)) {
    return;
  }
  std::unique_ptr<rocksdb::BackupEngine> backup_engine_holder(backup_engine);

  auto db_path = FLAGS_rocksdb_dir + request->db_name;
  status = backup_engine->RestoreDBFromLatestBackup(db_path, db_path);
  if (!OKOrSetException(status,
                        AdminErrorCode::DB_ADMIN_ERROR,
                        &callback)) {
    return;
  }

  rocksdb::DB* rocksdb_db;
  auto segment = admin::DbNameToSegment(request->db_name);
  status = rocksdb::DB::Open(rocksdb_options_(segment), db_path, &rocksdb_db);
  if (!OKOrSetException(status,
                        AdminErrorCode::DB_ERROR,
                        &callback)) {
    return;
  }

  std::string err_msg;
  if (!db_manager_->addDB(request->db_name,
                          std::unique_ptr<rocksdb::DB>(rocksdb_db),
                          replicator::DBRole::SLAVE,
                          std::move(upstream_addr), &err_msg)) {
    e.errorCode = AdminErrorCode::DB_ADMIN_ERROR;
    e.message = std::move(err_msg);
    callback.release()->exceptionInThread(std::move(e));
    return;
  }

  LOG(INFO) << "Restore is done.";
  callback->result(RestoreDBResponse());
}

void AdminHandler::async_tm_checkDB(
    std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
      CheckDBResponse>>> callback,
    std::unique_ptr<CheckDBRequest> request) {
  AdminException e;
  auto db = getDB(request->db_name, &e);
  if (db == nullptr) {
    callback.release()->exceptionInThread(std::move(e));
    return;
  }
  callback->result(CheckDBResponse());
}

void AdminHandler::async_tm_closeDB(
    std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
      CloseDBResponse>>> callback,
    std::unique_ptr<CloseDBRequest> request) {
  db_admin_lock_.Lock(request->db_name);
  SCOPE_EXIT { db_admin_lock_.Unlock(request->db_name); };

  AdminException e;
  if (removeDB(request->db_name, &e) == nullptr) {
    callback.release()->exceptionInThread(std::move(e));
    return;
  }

  callback->result(CloseDBResponse());
}

void AdminHandler::async_tm_changeDBRoleAndUpStream(
    std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
      ChangeDBRoleAndUpstreamResponse>>> callback,
    std::unique_ptr<ChangeDBRoleAndUpstreamRequest> request) {
  db_admin_lock_.Lock(request->db_name);
  SCOPE_EXIT { db_admin_lock_.Unlock(request->db_name); };

  AdminException e;
  replicator::DBRole new_role;
  if (request->new_role == "MASTER") {
    new_role = replicator::DBRole::MASTER;
  } else if (request->new_role == "SLAVE") {
    new_role = replicator::DBRole::SLAVE;
  } else {
    e.errorCode = AdminErrorCode::INVALID_DB_ROLE;
    e.message = std::move(request->new_role);
    callback.release()->exceptionInThread(std::move(e));
    return;
  }

  std::unique_ptr<folly::SocketAddress> upstream_addr(nullptr);
  if (new_role == replicator::DBRole::SLAVE &&
      request->__isset.upstream_ip &&
      request->__isset.upstream_port) {
    upstream_addr = std::make_unique<folly::SocketAddress>();
    if (!SetAddressOrException(request->upstream_ip,
                               FLAGS_rocksdb_replicator_port,
                               upstream_addr.get(),
                               &callback)) {
      return;
    }
  }

  auto db = removeDB(request->db_name, &e);
  if (db == nullptr) {
    callback.release()->exceptionInThread(std::move(e));
    return;
  }

  std::string err_msg;
  if (!db_manager_->addDB(request->db_name, std::move(db), new_role,
                          std::move(upstream_addr), &err_msg)) {
    e.errorCode = AdminErrorCode::DB_ADMIN_ERROR;
    e.message = std::move(err_msg);
    callback.release()->exceptionInThread(std::move(e));
    return;
  }

  callback->result(ChangeDBRoleAndUpstreamResponse());
}

void AdminHandler::async_tm_getSequenceNumber(
    std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
      GetSequenceNumberResponse>>> callback,
    std::unique_ptr<GetSequenceNumberRequest> request) {
  AdminException e;
  auto db = getDB(request->db_name, &e);
  if (db == nullptr) {
    callback.release()->exceptionInThread(std::move(e));
    return;
  }

  GetSequenceNumberResponse response;
  response.seq_num = db->rocksdb()->GetLatestSequenceNumber();
  callback->result(response);
}

void AdminHandler::async_tm_clearDB(
    std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
      ClearDBResponse>>> callback,
    std::unique_ptr<ClearDBRequest> request) {
  db_admin_lock_.Lock(request->db_name);
  SCOPE_EXIT { db_admin_lock_.Unlock(request->db_name); };

  bool need_to_reopen = false;
  replicator::DBRole db_role;
  std::unique_ptr<folly::SocketAddress> upstream_addr;
  {
    auto db = getDB(request->db_name, nullptr);
    if (db) {
      need_to_reopen = true;
      db_role = db->IsSlave() ? replicator::DBRole::SLAVE :
        replicator::DBRole::MASTER;
      if (db->upstream_addr()) {
        upstream_addr =
          std::make_unique<folly::SocketAddress>(*(db->upstream_addr()));
      }
    }
  }

  removeDB(request->db_name, nullptr);

  auto options = rocksdb_options_(admin::DbNameToSegment(request->db_name));
  auto db_path = FLAGS_rocksdb_dir + request->db_name;
  LOG(INFO) << "Clearing DB: " << request->db_name;
  auto status = rocksdb::DestroyDB(db_path, options);
  if (!OKOrSetException(status,
                        AdminErrorCode::DB_ADMIN_ERROR,
                        &callback)) {
    LOG(ERROR) << "Failed to clear DB " << request->db_name << " "
               << status.ToString();
    return;
  }
  LOG(INFO) << "Done clearing DB: " << request->db_name;

  if (request->reopen_db && need_to_reopen) {
    LOG(INFO) << "Open DB: " << request->db_name;
    admin::AdminException e;
    e.errorCode = AdminErrorCode::DB_ADMIN_ERROR;
    auto db = GetRocksdb(db_path, options);
    if (db == nullptr) {
      e.message = "Failed to open DB: " + request->db_name;
      callback.release()->exceptionInThread(std::move(e));
      return;
    }

    std::string err_msg;
    if (!db_manager_->addDB(request->db_name, std::move(db), db_role,
                            std::move(upstream_addr), &err_msg)) {
      e.message = std::move(err_msg);
      callback.release()->exceptionInThread(std::move(e));
      return;
    }
    LOG(INFO) << "Done open DB: " << request->db_name;
  }

  callback->result(ClearDBResponse());
}

inline bool should_new_s3_client(
  shared_ptr<common::S3Util> s3_util, AddS3SstFilesToDBRequest* request) {
  return s3_util == nullptr || s3_util->getBucket() != request->s3_bucket ||
      s3_util->getRateLimit() != request->s3_download_limit_mb;
}

void AdminHandler::async_tm_addS3SstFilesToDB(
    std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
      AddS3SstFilesToDBResponse>>> callback,
    std::unique_ptr<AddS3SstFilesToDBRequest> request) {
  // Though it is claimed that AWS s3 sdk is a light weight library. However,
  // we couldn't afford to create a new client for every SST file downloading
  // request, which is not even on any critical code path. Otherwise, we will
  // see latency spike when uploading data to production clusters.
  folly::RWSpinLock::UpgradedHolder upgraded_guard(s3_util_lock_);
  if (should_new_s3_client(s3_util_, request.get())) {
    // Request with different ratelimit or bucket has to wait for old
    // requests to drain.
    folly::RWSpinLock::WriteHolder write_guard(s3_util_lock_);
    // Double check to achieve compare-exchange-like operation.
    // The reason not using atomic_compare_exchange_* is to save
    // expensive S3Util creation if multiple requests arrive.
    if (should_new_s3_client(s3_util_, request.get())) {
      LOG(INFO) << "Request with different bucket "
          << request->s3_bucket << ". Or different rate limit: "
          << request->s3_download_limit_mb;
      s3_util_ = common::S3Util::BuildS3Util(request->s3_download_limit_mb,
                                             request->s3_bucket);
    }
  }

  admin::AdminException e;
  e.errorCode = AdminErrorCode::DB_ADMIN_ERROR;

  db_admin_lock_.Lock(request->db_name);
  SCOPE_EXIT { db_admin_lock_.Unlock(request->db_name); };

  replicator::DBRole db_role;
  std::unique_ptr<folly::SocketAddress> upstream_addr;
  {
    auto db = getDB(request->db_name, nullptr);
    if (db == nullptr) {
      e.message = request->db_name + " not exists.";
      callback.release()->exceptionInThread(std::move(e));
      LOG(ERROR) << "Could not add SST files to a non existing DB "
                 << request->db_name;
      return;
    }

    db_role = db->IsSlave() ? replicator::DBRole::SLAVE :
      replicator::DBRole::MASTER;
    if (db->upstream_addr()) {
      upstream_addr =
        std::make_unique<folly::SocketAddress>(*(db->upstream_addr()));
    }
  }

  auto local_path = FLAGS_rocksdb_dir + "s3_tmp/" + request->db_name + "/";
  boost::system::error_code remove_err;
  boost::system::error_code create_err;
  boost::filesystem::remove_all(local_path, remove_err);
  boost::filesystem::create_directories(local_path, create_err);
  SCOPE_EXIT { boost::filesystem::remove_all(local_path, remove_err); };
  if (remove_err || create_err) {
    e.message = "Cannot remove/create dir: " + local_path;
    callback.release()->exceptionInThread(std::move(e));
    return;
  }

  auto responses = s3Util->getObjects(request->s3_path,
                                      local_path, "/", FLAGS_s3_direct_io);
  if (!responses.Error().empty() || responses.Body().size() == 0) {
    e.message = "Failed to list any object from " + request->s3_path;

    if (!responses.Error().empty()) {
      e.message += " AWS Error: " + responses.Error();
    }

    LOG(ERROR) << e.message;
    callback.release()->exceptionInThread(std::move(e));
    return;
  }

  for (auto& response : responses.Body()) {
    if (!response.Body()) {
      e.message = response.Error();
      callback.release()->exceptionInThread(std::move(e));
      return;
    }
  }

  auto db = removeDB(request->db_name, nullptr);
  const boost::filesystem::directory_iterator end_itor;
  boost::filesystem::directory_iterator itor(local_path);
  static const std::string suffix = ".sst";
  for (; itor != end_itor; ++itor) {
    auto file_name = itor->path().filename().string();
    if (file_name.size() < suffix.size() + 1 ||
        file_name.compare(file_name.size() - suffix.size(), suffix.size(),
                          suffix) != 0) {
      // skip non "*.sst" files
      continue;
    }

    auto status = db->AddFile(local_path + file_name, true /* move_file */);
    if (!OKOrSetException(status,
                          AdminErrorCode::DB_ADMIN_ERROR,
                          &callback)) {
      LOG(ERROR) << "Failed to add file " << local_path + file_name << " "
                 << status.ToString();
      return;
    }
  }

  if (FLAGS_compact_db_after_load_sst) {
    auto status = db->CompactRange(nullptr, nullptr);
    if (!status.ok()) {
      LOG(ERROR) << "Failed to compact DB: " << status.ToString();
    }
  }

  std::string err_msg;
  if (!db_manager_->addDB(request->db_name, std::move(db), db_role,
                          std::move(upstream_addr), &err_msg)) {
    e.message = std::move(err_msg);
    callback.release()->exceptionInThread(std::move(e));
    return;
  }

  callback->result(AddS3SstFilesToDBResponse());
}

void AdminHandler::async_tm_setDBOptions(
    std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
      SetDBOptionsResponse>>> callback,
    std::unique_ptr<SetDBOptionsRequest> request) {
  std::unordered_map<string, string> options;
  for (auto& option_pair : request->options) {
    options.emplace(std::move(option_pair));
  }
  db_admin_lock_.Lock(request->db_name);
  SCOPE_EXIT { db_admin_lock_.Unlock(request->db_name); };
  ::admin::AdminException e;
  auto db = getDB(request->db_name, &e);
  if (db == nullptr) {
    callback.release()->exceptionInThread(std::move(e));
    return;
  }
  // Assume we always use default column family
  auto status = db->rocksdb()->SetOptions(options);
  if (!OKOrSetException(status,
                        AdminErrorCode::DB_ADMIN_ERROR,
                        &callback)) {
    return;
  }
  callback->result(SetDBOptionsResponse());
}

void AdminHandler::async_tm_compactDB(
    std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
      CompactDBResponse>>> callback,
    std::unique_ptr<CompactDBRequest> request) {
  ::admin::AdminException e;
  auto db = getDB(request->db_name, &e);
  if (db == nullptr) {
    callback.release()->exceptionInThread(std::move(e));
    return;
  }

  auto status = db->CompactRange(
      rocksdb::CompactRangeOptions(), nullptr, nullptr);
  if (!status.ok()) {
    e.message = status.ToString();
    e.errorCode = AdminErrorCode::DB_ERROR;
    callback.release()->exceptionInThread(std::move(e));
    return;
  }
  callback.release()->result(CompactDBResponse());
}

}  // namespace admin
