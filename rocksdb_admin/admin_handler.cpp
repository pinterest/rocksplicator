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

#include <chrono>
#include <memory>
#include <string>
#include <thread>
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
#include "rocksdb/options.h"
#include "rocksdb/utilities/backupable_db.h"
#include "rocksdb_admin/utils.h"
#include "rocksdb_replicator/rocksdb_replicator.h"
#include "thrift/lib/cpp2/protocol/Serializer.h"

DEFINE_string(hdfs_name_node, "hdfs://hbasebak-infra-namenode-prod1c01-001:8020",
              "The hdfs name node used for backup");

DEFINE_string(rocksdb_dir, "/tmp/",
              "The dir for local rocksdb instances");

DEFINE_int32(num_hdfs_access_threads, 8,
             "The number of threads for backup or restore to/from HDFS");

DEFINE_int32(port, 9090, "Port of the server");

DEFINE_string(shard_config_path, "",
             "Local path of file storing shard mapping for Aperture");

// For rocksdb_allow_overlapping_keys and allow_overlapping_keys_segments,
// we take the logical OR of the bool and if the set contains the segment
// to determine whether or not to allow overlapping keys on ingesting sst files

DEFINE_bool(rocksdb_allow_overlapping_keys, false,
            "Allow overlapping keys in sst bulk load");

DEFINE_string(allow_overlapping_keys_segments, "",
              "comma separated list of segments supporting overlapping keys");

DEFINE_bool(compact_db_after_load_sst, false,
            "Compact DB after loading SST files");

DECLARE_int32(rocksdb_replicator_port);

DEFINE_bool(s3_direct_io, false, "Whether to enable direct I/O for s3 client");

DEFINE_int32(max_s3_sst_loading_concurrency, 999,
             "Max S3 SST loading concurrency");

DEFINE_int32(s3_download_limit_mb, 0, "S3 download sst bandwidth");

namespace {

const int kMB = 1024 * 1024;
const int kS3UtilRecheckSec = 5;

rocksdb::DB* OpenMetaDB() {
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::DB* db;
  auto s = rocksdb::DB::Open(options, FLAGS_rocksdb_dir + "meta_db", &db);
  CHECK(s.ok()) << "Failed to open meta DB" << " with error " << s.ToString();

  return db;
}

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

  auto cluster_layout = common::parseConfig(std::move(content), "");
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
  , db_admin_lock_()
  , s3_util_()
  , s3_util_lock_()
  , meta_db_(OpenMetaDB())
  , allow_overlapping_keys_segments_()
  , num_current_s3_sst_downloadings_(0) {
  if (db_manager_ == nullptr) {
    db_manager_ = CreateDBBasedOnConfig(rocksdb_options_);
  }
  folly::splitTo<std::string>(
      ",", FLAGS_allow_overlapping_keys_segments,
      std::inserter(allow_overlapping_keys_segments_,
                    allow_overlapping_keys_segments_.begin()));

  CHECK(FLAGS_max_s3_sst_loading_concurrency > 0)
    << "Invalid FLAGS_max_s3_sst_loading_concurrency: "
    << FLAGS_max_s3_sst_loading_concurrency;
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

DBMetaData AdminHandler::getMetaData(const std::string& db_name) {
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

bool AdminHandler::clearMetaData(const std::string& db_name) {
  rocksdb::WriteOptions options;
  options.sync = true;
  auto s = meta_db_->Delete(options, db_name);
  return s.ok();
}

bool AdminHandler::writeMetaData(const std::string& db_name,
                                 const std::string& s3_bucket,
                                 const std::string& s3_path) {
  DBMetaData meta;
  meta.db_name = db_name;
  meta.set_s3_bucket(s3_bucket);
  meta.set_s3_path(s3_path);

  std::string buffer;
  apache::thrift::CompactSerializer::serialize(meta, &buffer);

  rocksdb::WriteOptions options;
  options.sync = true;
  auto s = meta_db_->Put(options, db_name, buffer);
  return s.ok();
}

void AdminHandler::async_tm_addDB(
      std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
          AddDBResponse>>> callback,
      std::unique_ptr<AddDBRequest> request) {
  db_admin_lock_.Lock(request->db_name);
  SCOPE_EXIT { db_admin_lock_.Unlock(request->db_name); };

  AdminException e;
  auto db = getDB(request->db_name, &e);
  if (db) {
    e.errorCode = AdminErrorCode::DB_EXIST;
    e.message = "Db already exists";
    callback.release()->exceptionInThread(std::move(e));
    return;
  }

  // Get the upstream for the db to be added
  auto upstream_addr = std::make_unique<folly::SocketAddress>();
  if (!SetAddressOrException(request->upstream_ip,
                             FLAGS_rocksdb_replicator_port,
                             upstream_addr.get(),
                             &callback)) {
    return;
  }

  auto segment = admin::DbNameToSegment(request->db_name);
  auto db_path = FLAGS_rocksdb_dir + request->db_name;
  rocksdb::Status status;
  if (request->overwrite) {
    LOG(INFO) << "Clearing DB: " << request->db_name;
    clearMetaData(request->db_name);
    status = rocksdb::DestroyDB(db_path, rocksdb_options_(segment));
    if (!OKOrSetException(status,
                          AdminErrorCode::DB_ADMIN_ERROR,
                          &callback)) {
      LOG(ERROR) << "Failed to clear DB " << request->db_name << " "
                 << status.ToString();
      return;
    }
  }

  // Open the actual rocksdb instance
  rocksdb::DB* rocksdb_db;
  status = rocksdb::DB::Open(rocksdb_options_(segment), db_path, &rocksdb_db);
  if (!OKOrSetException(status,
                        AdminErrorCode::DB_ERROR,
                        &callback)) {
    return;
  }


  // add the db to db_manager
  std::string err_msg;
  if (!db_manager_->addDB(request->db_name,
                          std::unique_ptr<rocksdb::DB>(rocksdb_db),
                          replicator::DBRole::SLAVE, std::move(upstream_addr),
                          &err_msg)) {
    e.errorCode = AdminErrorCode::DB_ADMIN_ERROR;
    e.message = std::move(err_msg);
    callback.release()->exceptionInThread(std::move(e));
    return;
  }
  callback->result(AddDBResponse());
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

  CheckDBResponse response;
  response.set_seq_num(db->rocksdb()->GetLatestSequenceNumber());
  response.set_wal_ttl_seconds(db->rocksdb()->GetOptions().WAL_ttl_seconds);
  response.set_is_master(!db->IsSlave());

  // If there is at least one update
  if (response.seq_num != 0) {
    std::unique_ptr<rocksdb::TransactionLogIterator> iter;
    auto status = db->rocksdb()->GetUpdatesSince(response.seq_num, &iter);

    if (status.ok() && iter && iter->Valid()) {
      auto batch = iter->GetBatch();
      replicator::LogExtractor extractor;
      status = batch.writeBatchPtr->Iterate(&extractor);
      if (status.ok()) {
        response.set_last_update_timestamp_ms(extractor.ms);
      }
    }
  }

  callback->result(response);
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
  clearMetaData(request->db_name);
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
    const common::S3Util& s3_util, AddS3SstFilesToDBRequest* request) {
  return s3_util.getBucket() != request->s3_bucket ||
         s3_util.getRateLimit() != request->s3_download_limit_mb;
}

void AdminHandler::async_tm_addS3SstFilesToDB(
    std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
      AddS3SstFilesToDBResponse>>> callback,
    std::unique_ptr<AddS3SstFilesToDBRequest> request) {
  admin::AdminException e;
  e.errorCode = AdminErrorCode::DB_ADMIN_ERROR;

  db_admin_lock_.Lock(request->db_name);
  SCOPE_EXIT { db_admin_lock_.Unlock(request->db_name); };

  auto db = getDB(request->db_name, nullptr);
  if (db == nullptr) {
    e.message = request->db_name + " doesnt exist.";
    callback.release()->exceptionInThread(std::move(e));
    LOG(ERROR) << "Could not add SST files to a non existing DB "
               << request->db_name;
    return;
  }

  auto meta = getMetaData(request->db_name);
  if (meta.__isset.s3_bucket && meta.s3_bucket == request->s3_bucket &&
      meta.__isset.s3_path && meta.s3_path == request->s3_path) {
    LOG(INFO) << "Already hosting " << meta.s3_bucket << "/" << meta.s3_path;
    callback->result(AddS3SstFilesToDBResponse());
    return;
  }

  // The local data is not the latest, so we need to download the latest data
  // from S3 and load it into the DB. This is to limit the allowed concurrent
  // loadings.
  auto n = num_current_s3_sst_downloadings_.fetch_add(1);
  SCOPE_EXIT {
    num_current_s3_sst_downloadings_.fetch_sub(1);
  };

  if (n >= FLAGS_max_s3_sst_loading_concurrency) {
    auto err_str =
      folly::stringPrintf("Concurrent downloading limit hits %d by %s",
                          n, request->db_name.c_str());

    e.message = err_str;
    callback.release()->exceptionInThread(std::move(e));
    LOG(ERROR) << err_str;
    return;
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

  // Though it is claimed that AWS s3 sdk is a light weight library. However,
  // we couldn't afford to create a new client for every SST file downloading
  // request, which is not even on any critical code path. Otherwise, we will
  // see latency spike when uploading data to production clusters.
  std::shared_ptr<common::S3Util> local_s3_util;
  if (FLAGS_s3_download_limit_mb > 0) {
    request->s3_download_limit_mb = FLAGS_s3_download_limit_mb;
  }
  {
    std::lock_guard<std::mutex> guard(s3_util_lock_);
    if (s3_util_ == nullptr || should_new_s3_client(*s3_util_, request.get())) {
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
      s3_util_ = common::S3Util::BuildS3Util(request->s3_download_limit_mb,
                                             request->s3_bucket);
    }
    local_s3_util = s3_util_;
  }

  auto responses = local_s3_util->getObjects(request->s3_path,
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

  const boost::filesystem::directory_iterator end_itor;
  boost::filesystem::directory_iterator itor(local_path);
  static const std::string suffix = ".sst";
  std::vector<std::string> sst_file_paths;
  for (; itor != end_itor; ++itor) {
    auto file_name = itor->path().filename().string();
    if (file_name.size() < suffix.size() + 1 ||
        file_name.compare(file_name.size() - suffix.size(), suffix.size(),
                          suffix) != 0) {
      // skip non "*.sst" files
      continue;
    }

    sst_file_paths.push_back(local_path + file_name);
  }

  clearMetaData(request->db_name);

  auto segment = admin::DbNameToSegment(request->db_name);
  bool allow_overlapping_keys =
      allow_overlapping_keys_segments_.find(segment) !=
      allow_overlapping_keys_segments_.end();
  // OR with the flag to make backwards compatibility
  allow_overlapping_keys =
      allow_overlapping_keys || FLAGS_rocksdb_allow_overlapping_keys;
  if (!allow_overlapping_keys) {
    // clear DB if overlapping keys are not allowed
    auto db_role = db->IsSlave() ?
      replicator::DBRole::SLAVE : replicator::DBRole::MASTER;
    std::unique_ptr<folly::SocketAddress> upstream_addr;
    if (db_role == replicator::DBRole::SLAVE &&
        db->upstream_addr() != nullptr) {
      upstream_addr.reset(new folly::SocketAddress(*db->upstream_addr()));
    }
    db.reset();
    removeDB(request->db_name, nullptr);
    auto options = rocksdb_options_(segment);
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

    // reopen it
    LOG(INFO) << "Open DB: " << request->db_name;
    admin::AdminException e;
    e.errorCode = AdminErrorCode::DB_ADMIN_ERROR;
    auto rocksdb_db = GetRocksdb(db_path, options);
    if (rocksdb_db == nullptr) {
      e.message = "Failed to open DB: " + request->db_name;
      callback.release()->exceptionInThread(std::move(e));
      return;
    }

    std::string err_msg;
    if (!db_manager_->addDB(request->db_name, std::move(rocksdb_db),
                            db_role, std::move(upstream_addr), &err_msg)) {
      e.message = std::move(err_msg);
      callback.release()->exceptionInThread(std::move(e));
      return;
    }
    LOG(INFO) << "Done open DB: " << request->db_name;
    db = getDB(request->db_name, nullptr);
  }

  rocksdb::IngestExternalFileOptions ifo;
  ifo.move_files = true;
  /* if true, rocksdb will allow for overlapping keys */
  ifo.allow_global_seqno = allow_overlapping_keys;
  ifo.allow_blocking_flush = allow_overlapping_keys;
  auto status = db->rocksdb()->IngestExternalFile(sst_file_paths, ifo);
  if (!OKOrSetException(status,
                        AdminErrorCode::DB_ADMIN_ERROR,
                        &callback)) {
    LOG(ERROR) << "Failed to add files to DB " << request->db_name
               << status.ToString();
    return;
  }

  writeMetaData(request->db_name, request->s3_bucket, request->s3_path);

  if (FLAGS_compact_db_after_load_sst) {
    auto status = db->rocksdb()->CompactRange(nullptr, nullptr);
    if (!status.ok()) {
      LOG(ERROR) << "Failed to compact DB: " << status.ToString();
    }
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

void AdminHandler::async_tm_startMessageIngestion(
    std::unique_ptr<apache::thrift::HandlerCallback<
        std::unique_ptr<StartMessageIngestionResponse>>>
        callback,
    std::unique_ptr<StartMessageIngestionRequest> request) {
    LOG(INFO) << "Start message ingestion on db : " << request->db_name
              << " from topic " << request->topic_name
              << " on kafka broker serverset " << request->kafka_broker_serverset_path
              << " from timestamp " << request->replay_timestamp_ms;
    callback.release()->result(StartMessageIngestionResponse());
}

void AdminHandler::async_tm_stopMessageIngestion(
    std::unique_ptr<apache::thrift::HandlerCallback<
        std::unique_ptr<StopMessageIngestionResponse>>>
        callback,
    std::unique_ptr<StopMessageIngestionRequest> request) {
    LOG(INFO) << "Stop message ingestion on db " << request->db_name;
    callback.release()->result(StopMessageIngestionResponse());
}

std::string AdminHandler::DumpDBStatsAsText() const {
  return db_manager_->DumpDBStatsAsText();
}

}  // namespace admin
