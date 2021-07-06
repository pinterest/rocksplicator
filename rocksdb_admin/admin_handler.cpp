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
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>
#include <map>

#include "boost/filesystem.hpp"
#include "common/identical_name_thread_factory.h"
#include "common/kafka/kafka_broker_file_watcher.h"
#include "common/kafka/kafka_consumer_pool.h"
#include "common/kafka/kafka_watcher.h"
#include "common/network_util.h"
#include "common/rocksdb_env_s3.h"
#include "common/rocksdb_glogger/rocksdb_glogger.h"
#include "common/segment_utils.h"
#include "common/stats/stats.h"
#include "common/thrift_router.h"
#include "common/timer.h"
#include "common/timeutil.h"
#include "common/file_util.h"
#include "folly/futures/Future.h"
#include "folly/FileUtil.h"
#include "folly/MoveWrapper.h"
#include "folly/ScopeGuard.h"
#include "folly/String.h"
#include "librdkafka/rdkafkacpp.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/backupable_db.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb_admin/detail/kafka_broker_file_watcher_manager.h"
#include "rocksdb_admin/utils.h"
#include "rocksdb_replicator/rocksdb_replicator.h"
#include "thrift/lib/cpp2/protocol/Serializer.h"
#if __GNUC__ >= 8
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "folly/system/ThreadName.h"
#else
#include "wangle/concurrent/CPUThreadPoolExecutor.h"
#endif

DEFINE_string(hdfs_name_node, "hdfs://hbasebak-infra-namenode-prod1c01-001:8020",
              "The hdfs name node used for backup");

DEFINE_string(rocksdb_dir, "/tmp/",
              "The dir for local rocksdb instances");

DEFINE_int32(num_hdfs_access_threads, 8,
             "The number of threads for backup or restore to/from HDFS");

DECLARE_int32(port);

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

DEFINE_int32(kafka_ts_update_interval, 1000, "Number of kafka messages consumed"
                                             " before updating meta_db");

DEFINE_bool(enable_logging_consumer_log, false,
            "Enable logging consumer messages meta data at given log frequency");

DEFINE_bool(enable_logging_consumer_log_with_payload, false,
            "Enable logging consumer messages at given log frequency");

DEFINE_int32(consumer_log_frequency, 100,
  "only output one log in every log_frequency of logs");

DECLARE_int32(kafka_consumer_timeout_ms);

DEFINE_bool(enable_checkpoint_backup, false, "Enable backup via creating checkpoints");

DEFINE_int32(checkpoint_backup_batch_num_upload, 1, "how many batches could be uploaded in paralell");

DEFINE_int32(checkpoint_backup_batch_num_download, 1, "how many batches could be downloaded in paralell");

DEFINE_int32(num_s3_upload_download_threads, 8,
             "The number of threads for upload to/download from s3");

DEFINE_int64(max_s3_upload_download_task_queue_size, 1000, "The queue size in the executor");

DEFINE_bool(enable_async_delete_dbs, false, "Enable delete db files in async way");

DEFINE_int32(async_delete_dbs_frequency_sec,
             20,
             "How frequently in sec to check the dbs need deleting in async way");
DEFINE_int32(async_delete_dbs_wait_sec,
             60,
             "How long in sec to wait between the dbs deletion");

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

const std::string kMetaFilename = "dbmeta";

const int kMB = 1024 * 1024;
const int kS3UtilRecheckSec = 5;

const int64_t kMillisPerSec = 1000;
const char kKafkaConsumerType[] = "rocksplicator_consumer";
const char kKafkaWatcherName[] = "rocksplicator_watcher";
const uint32_t kKafkaConsumerPoolSize = 1;
const std::string kKafkaConsumerLatency = "kafka_consumer_latency";
const std::string kKafkaDbPutMessage = "kafka_put_msg_consumed";
const std::string kKafkaDbDelMessage = "kafka_del_msg_consumed";
const std::string kKafkaDbMergeMessage = "kafka_merge_msg_consumed";
const std::string kKafkaDbPutErrors = "kafka_db_put_errors";
const std::string kKafkaDbDeleteErrors = "kafka_db_delete_errors";
const std::string kKafkaDbMergeErrors = "kafka_db_merge_errors";
const std::string kKafkaDeserFailure = "kafka_deser_failure";
const std::string kKafkaInvalidOpcode = "kafka_invalid_opcode";
const std::string kHDFSBackupSuccess = "hdfs_backup_success";
const std::string kS3BackupSuccess = "s3_backup_success";
const std::string kHDFSBackupFailure = "hdfs_backup_failure";
const std::string kS3BackupFailure = "s3_backup_failure";
const std::string kHDFSRestoreSuccess = "hdfs_restore_success";
const std::string kS3RestoreSuccess = "s3_restore_success";
const std::string kHDFSRestoreFailure = "hdfs_restore_failure";
const std::string kS3RestoreFailure = "s3_restore_failure";
const std::string kHDFSBackupMs = "hdfs_backup_ms";
const std::string kHDFSRestoreMs = "hdfs_restore_ms";
const std::string kS3BackupMs = "s3_backup_ms";
const std::string kS3RestoreMs = "s3_restore_ms";
const std::string kDeleteDBFailure = "delete_db_failure";

int64_t GetMessageTimestampSecs(const RdKafka::Message& message) {
  const auto ts = message.timestamp();
  if (ts.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME) {
    return ts.timestamp / kMillisPerSec;
  }

  // We only expect the timestamp to be create time.
  return -1;
}

std::string ToUTC(const int64_t time_secs) {
  time_t raw_time(time_secs);
  char buf[48];
  static const std::string time_format = "%Y-%d-%m %H:%M:%S UTC";
  std::strftime(buf, sizeof(buf), time_format.c_str(), std::gmtime(&raw_time));
  return std::string(buf);
}

std::string getConsumerGroupId(
    const std::string& db_name) {
  return common::getLocalIPAddress() + '_' + db_name;
}

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

      auto db_name = common::SegmentToDbName(segment.first.c_str(), shard_id);
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
          auto db = std::move(*db_future).get();
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
void SetException(const std::string& message,
                  const ::admin::AdminErrorCode code,
                  std::unique_ptr<T>* callback) {
  ::admin::AdminException e;
  e.errorCode = code;
  e.message = message;
  callback->release()->exceptionInThread(std::move(e));
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

template <typename T>
bool DecodeThriftStruct(const void* data, const size_t size, T* obj) {
  try {
    apache::thrift::BinarySerializer::deserialize(
        folly::StringPiece(static_cast<const char *>(data), size), *obj);
  } catch (const std::exception& ex) {
    LOG(ERROR) << "Error when decoding message : " << ex.what();
    return false;
  }
  return true;
}

bool DeserializeKafkaPayload(
    const void* kafka_payload,
    const size_t payload_len,
    admin::KafkaOperationCode* op_code,
    std::string* value) {
  admin::KafkaMessagePayload msg_payload;

  if (DecodeThriftStruct<admin::KafkaMessagePayload>(kafka_payload, payload_len,
      &msg_payload)) {
    *op_code = msg_payload.op_code;
    if (msg_payload.__isset.value) {
      *value = std::move(msg_payload.value);
    }
    return true;
  } else {
    common::Stats::get()->Incr(kKafkaDeserFailure);
    return false;
  }
}

CPUThreadPoolExecutor* S3UploadAndDownloadExecutor() {
  static CPUThreadPoolExecutor executor(
      FLAGS_num_s3_upload_download_threads,
      std::make_unique<LifoSemMPMCQueue<CPUThreadPoolExecutor::CPUTask>>(
      FLAGS_max_s3_upload_download_task_queue_size),
      std::make_shared<common::IdenticalNameThreadFactory>("s3-upload-download"));

  return &executor;
}

// The dbs moved to db_tmp/ shouldnt be re-used or re-opened, so we can
// delete them via boost filesystem operations rather than rocksdb::DestroyDB()
void deleteTmpDBs() {
  static const std::string db_tmp_path = FLAGS_rocksdb_dir + "db_tmp/";
  boost::system::error_code dir_itr_err;
  boost::filesystem::directory_iterator itr(db_tmp_path, dir_itr_err);
  if (dir_itr_err) {
    LOG(ERROR) << "Can't list files in db_tmp/:" << dir_itr_err.message();
    return;
  }
  for (; itr != boost::filesystem::directory_iterator(); ++itr) {
    if (itr->path().filename() == "." || itr->path().filename() == "..") {
      continue;
    }
    boost::system::error_code remove_err;
    boost::filesystem::remove_all(itr->path(), remove_err);
    if (remove_err) {
      common::Stats::get()->Incr(kDeleteDBFailure);
      LOG(ERROR) << "Cant delete db: " << itr->path() << remove_err.message();
    } else {
      LOG(INFO) << "Done deleting db: " << itr->path();
    }
    std::this_thread::sleep_for(std::chrono::seconds(FLAGS_async_delete_dbs_wait_sec));
  }
}

}  // anonymous namespace

namespace admin {

AdminHandler::AdminHandler(
    std::unique_ptr<ApplicationDBManager> db_manager,
    RocksDBOptionsGeneratorType rocksdb_options)
  : db_admin_lock_()
  , db_manager_(std::move(db_manager))
  , rocksdb_options_(std::move(rocksdb_options))
  , s3_util_()
  , s3_util_lock_()
  , meta_db_(OpenMetaDB())
  , allow_overlapping_keys_segments_()
  , num_current_s3_sst_downloadings_(0)
  , stop_db_deletion_thread_(false) {
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

  if (FLAGS_enable_async_delete_dbs) {
    static const std::string db_tmp_path = FLAGS_rocksdb_dir + "db_tmp/";
    if (!boost::filesystem::exists(db_tmp_path)) {
      boost::system::error_code create_err;
      boost::filesystem::create_directories(db_tmp_path, create_err);
      if (create_err) {
        LOG(ERROR) << "Failed to create dir: " << db_tmp_path << create_err.message();
        return;
      }
    }

    db_deletion_thread_ = std::make_unique<std::thread>([this] {
      if (!folly::setThreadName("DBDeleter")) {
        LOG(ERROR) << "Failed to set thread name for DB deletion thread";
      }

      LOG(INFO) << "Starting DB deletion thread ...";
      while (!stop_db_deletion_thread_.load()) {
        deleteTmpDBs();
        std::this_thread::sleep_for(std::chrono::seconds(FLAGS_async_delete_dbs_frequency_sec));
      }
      LOG(INFO) << "Stopping DB deletioin thread ...";
    });
  }

  int upload_MBps = 600; // set as fixed for now.
  s3_conc_ = std::make_shared<common::S3Concurrent>("pinterest-jackson", upload_MBps); 
}

AdminHandler::~AdminHandler() {
  if (FLAGS_enable_async_delete_dbs) {
    stop_db_deletion_thread_ = true;
    db_deletion_thread_->join();
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

bool AdminHandler::writeMetaData(
    const std::string& db_name,
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

  auto segment = common::DbNameToSegment(request->db_name);
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
  replicator::DBRole role = replicator::DBRole::SLAVE;
  if (request->__isset.db_role) {
    if (request->db_role == "SLAVE" || request->db_role == "FOLLOWER") {
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

  // update meta if not exist
  auto meta = getMetaData(request->db_name);
  if (!meta.__isset.s3_bucket && !meta.__isset.s3_path &&
      !meta.__isset.last_kafka_msg_timestamp_ms) {
    LOG(INFO) << "No preivous meta exist, write a fresh meta to metadb for db: "
              << request->db_name;
    if (!writeMetaData(request->db_name, "", "")) {
      std::string errMsg =
          "AddDB failed to write initial DBMetaData for " + request->db_name;
      SetException(errMsg, admin::AdminErrorCode::DB_ADMIN_ERROR, &callback);
      LOG(ERROR) << errMsg;
      return;
    }
  }

  if (!db_manager_->addDB(request->db_name,
                          std::unique_ptr<rocksdb::DB>(rocksdb_db),
                          role, std::move(upstream_addr),
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

bool AdminHandler::backupDBHelper(const std::string& db_name,
                                  const std::string& backup_dir,
                                  std::unique_ptr<rocksdb::Env> env_holder,
                                  const bool enable_backup_rate_limit,
                                  const uint32_t backup_rate_limit,
                                  const bool share_files_with_checksum,
                                  bool include_meta,
                                  AdminException* e) {
  CHECK(env_holder != nullptr);
  db_admin_lock_.Lock(db_name);
  SCOPE_EXIT { db_admin_lock_.Unlock(db_name); };

  auto db = getDB(db_name, e);
  if (db == nullptr) {
    LOG(ERROR) << "Error happened when getting db for backup: " << e->message;
    return false;
  }

  rocksdb::BackupableDBOptions options(backup_dir);
  if (share_files_with_checksum) {
    options.share_files_with_checksum = true;
  }
  common::RocksdbGLogger logger;
  options.info_log = &logger;
  options.max_background_operations = FLAGS_num_hdfs_access_threads;
  if (enable_backup_rate_limit && backup_rate_limit > 0) {
    options.backup_rate_limit = backup_rate_limit * kMB;
  }
  options.backup_env = env_holder.get();

  rocksdb::BackupEngine* backup_engine;
  auto status = rocksdb::BackupEngine::Open(
      rocksdb::Env::Default(), options, &backup_engine);
  if (!status.ok()) {
    e->errorCode = AdminErrorCode::DB_ADMIN_ERROR;
    e->message = status.ToString();
    LOG(ERROR) << "Error happened when opending db for backup: " << e->message;
    return false;
  }
  std::unique_ptr<rocksdb::BackupEngine> backup_engine_holder(backup_engine);

  if (include_meta) {
    std::string db_meta;
    const auto meta = getMetaData(db_name);
    if (!EncodeThriftStruct(meta, &db_meta)) {
      e->errorCode = AdminErrorCode::DB_ADMIN_ERROR;
      e->message = "Failed to encode DBMetaData";
      return false;
    } else {
      LOG(INFO) << "Create new backup with encoded meta: " << db_meta;
      return backup_engine->CreateNewBackupWithMetadata(db->rocksdb(), db_meta).ok();
    }
  } else {
    return backup_engine->CreateNewBackup(db->rocksdb()).ok();
  }  
}

bool AdminHandler::restoreDBHelper(const std::string& db_name,
                                   const std::string& backup_dir,
                                   std::unique_ptr<rocksdb::Env> env_holder,
                                   std::unique_ptr<folly::SocketAddress> upstream_addr,
                                   const bool enable_restore_rate_limit,
                                   const uint32_t restore_rate_limit,
                                   AdminException* e) {
  assert(env_holder != nullptr);
  db_admin_lock_.Lock(db_name);
  SCOPE_EXIT { db_admin_lock_.Unlock(db_name); };
  
  auto db = db_manager_->getDB(db_name, nullptr);
  if (db) {
    e->errorCode = AdminErrorCode::DB_EXIST;
    e->message = "Could not restore an opened DB, close it first";
    return false;
  }

  rocksdb::BackupableDBOptions options(backup_dir);
  common::RocksdbGLogger logger;
  options.info_log = &logger;
  options.max_background_operations = FLAGS_num_hdfs_access_threads;
  if (enable_restore_rate_limit && restore_rate_limit > 0) {
    options.restore_rate_limit = restore_rate_limit * kMB;
  }
  options.backup_env = env_holder.get();

  rocksdb::BackupEngine* backup_engine;
  auto status = rocksdb::BackupEngine::Open(
      rocksdb::Env::Default(), options, &backup_engine);
  if (!status.ok()) {
    e->errorCode = AdminErrorCode::DB_ADMIN_ERROR;
    e->message = status.ToString();
    LOG(ERROR) << "Error happened when opending db for restore: " << e->message;
    return false;
  }
  std::unique_ptr<rocksdb::BackupEngine> backup_engine_holder(backup_engine);

  std::vector<rocksdb::BackupInfo> backup_infos;
  backup_engine->GetBackupInfo(&backup_infos);
  if (backup_infos.size() < 1) {
    e->errorCode = AdminErrorCode::DB_NOT_FOUND;
    e->message = "Failed to getBackupInfo with backupEngine";
    return false;
  }
  std::sort(backup_infos.begin(), backup_infos.end(), [](rocksdb::BackupInfo& a, rocksdb::BackupInfo& b) {
    return a.backup_id < b.backup_id;
  });
  uint32_t latest_backup_id = backup_infos.back().backup_id;

  auto db_path = FLAGS_rocksdb_dir + db_name;
  status = backup_engine->RestoreDBFromBackup(latest_backup_id, db_path, db_path);
  if (!status.ok()) {
    e->errorCode = AdminErrorCode::DB_ADMIN_ERROR;
    e->message = status.ToString();
    return false;
  }

  rocksdb::DB* rocksdb_db;
  auto segment = common::DbNameToSegment(db_name);
  status = rocksdb::DB::Open(rocksdb_options_(segment), db_path, &rocksdb_db);
  if (!status.ok()) {
    e->errorCode = AdminErrorCode::DB_ERROR;
    e->message = status.ToString();
    return false;
  }

  DBMetaData meta;
  const std::string& meta_from_backup = backup_infos.back().app_metadata;
  if (!meta_from_backup.empty() &&
      !DecodeThriftStruct(meta_from_backup, &meta)) {
    e->errorCode = AdminErrorCode::DB_ERROR;
    e->message = "Failed to decode DBMetaData";
    return false;
  }
  meta.set_db_name(db_name);
  LOG(INFO) << "Write DBMetaData db_name: " << meta.db_name
            << ", s3_bucket: " << meta.s3_bucket << ", s3_path" << meta.s3_path
            << " from backupId: " << std::to_string(latest_backup_id);
  if (!writeMetaData(meta.db_name, meta.s3_bucket, meta.s3_path)) {
    e->errorCode = AdminErrorCode::DB_ADMIN_ERROR;
    e->message = "RestoreDBHelper failed to write DBMetaData from restore's app_metadata for " + meta.db_name;
    return false;
  }

  std::string err_msg;
  if (!db_manager_->addDB(db_name,
                          std::unique_ptr<rocksdb::DB>(rocksdb_db),
                          replicator::DBRole::SLAVE,
                          std::move(upstream_addr), &err_msg)) {
    e->errorCode = AdminErrorCode::DB_ADMIN_ERROR;
    e->message = std::move(err_msg);
    return false;
  }
  return true;
}

void AdminHandler::async_tm_backupDB(
    std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
      BackupDBResponse>>> callback,
    std::unique_ptr<BackupDBRequest> request) {
  auto full_path = FLAGS_hdfs_name_node + request->hdfs_backup_dir;
  rocksdb::Env* hdfs_env;
  auto status = rocksdb::NewHdfsEnv(&hdfs_env, full_path);
  if (!OKOrSetException(status,
                        AdminErrorCode::DB_ADMIN_ERROR,
                        &callback)) {
    common::Stats::get()->Incr(kHDFSBackupFailure);
    return;
  }

  common::Timer timer(kHDFSBackupMs);
  LOG(INFO) << "HDFS Backup " << request->db_name << " to " << full_path;
  AdminException e;
  const bool share_files_with_checksum = request->__isset.share_files_with_checksum && request->share_files_with_checksum;
  const bool include_meta = request->__isset.include_meta && request->include_meta;
  if (!backupDBHelper(request->db_name,
                      full_path,
                      std::unique_ptr<rocksdb::Env>(hdfs_env),
                      request->__isset.limit_mbs,
                      request->limit_mbs,
                      share_files_with_checksum,
                      include_meta,
                      &e)) {
    callback.release()->exceptionInThread(std::move(e));
    common::Stats::get()->Incr(kHDFSBackupFailure);
    return;
  }

  LOG(INFO) << "HDFS Backup is done.";
  common::Stats::get()->Incr(kHDFSBackupSuccess);
  callback->result(BackupDBResponse());
}

void AdminHandler::async_tm_restoreDB(
    std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
      RestoreDBResponse>>> callback,
    std::unique_ptr<RestoreDBRequest> request) {
  auto upstream_addr = std::make_unique<folly::SocketAddress>();
  if (!SetAddressOrException(request->upstream_ip,
                             FLAGS_rocksdb_replicator_port,
                             upstream_addr.get(),
                             &callback)) {
    common::Stats::get()->Incr(kHDFSRestoreFailure);
    return;
  }

  auto full_path = FLAGS_hdfs_name_node + request->hdfs_backup_dir;
  rocksdb::Env* hdfs_env;
  auto status = rocksdb::NewHdfsEnv(&hdfs_env, full_path);
  if (!OKOrSetException(status,
                        AdminErrorCode::DB_ADMIN_ERROR,
                        &callback)) {
    common::Stats::get()->Incr(kHDFSRestoreFailure);
    return;
  }

  common::Timer timer(kHDFSRestoreMs);
  LOG(INFO) << "HDFS Restore " << request->db_name << " from " << full_path;
  AdminException e;
  if (!restoreDBHelper(request->db_name,
                       full_path,
                       std::unique_ptr<rocksdb::Env>(hdfs_env),
                       std::move(upstream_addr),
                       request->__isset.limit_mbs,
                       request->limit_mbs,
                       &e)) {
    callback.release()->exceptionInThread(std::move(e));
    common::Stats::get()->Incr(kHDFSRestoreFailure);
    return;
  }

  LOG(INFO) << "HDFS Restore is done.";
  common::Stats::get()->Incr(kHDFSRestoreSuccess);
  callback->result(RestoreDBResponse());
}

inline std::string rtrim(std::string str, char c) {
  if (str.length() > 0 && str.back() == c) {
    str.pop_back();
  }

  return str;
}

inline std::string ensure_ends_with_pathsep(const std::string& s) {
  if (!s.empty() && s.back() != '/') {
    return s + "/";
  }
  return s;
}

void AdminHandler::async_tm_backupDBToS3(
    std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
      BackupDBToS3Response>>> callback,
    std::unique_ptr<BackupDBToS3Request> request) {
  LOG(INFO) << "S3 Backup " << request->db_name << " to " << request->s3_backup_dir;

  AdminException e;
  const auto n = num_current_s3_sst_uploadings_.fetch_add(1);
  SCOPE_EXIT {
    num_current_s3_sst_uploadings_.fetch_sub(1);
  };
  common::Timer timer(kS3BackupMs);

  auto ts = common::timeutil::GetCurrentTimestamp();
  auto local_path = folly::stringPrintf("%ss3_tmp/%s%d/", FLAGS_rocksdb_dir.c_str(), request->db_name.c_str(), ts);
  const std::string sync_group_id = request->s3_backup_dir;
  
  boost::system::error_code remove_err;
  boost::system::error_code create_err;
  boost::filesystem::remove_all(local_path, remove_err);
  boost::filesystem::create_directories(local_path, create_err);

  SCOPE_EXIT { boost::filesystem::remove_all(local_path, remove_err); };
  if (remove_err || create_err) {
    SetException("Cannot remove/create dir for backup: " + local_path, AdminErrorCode::DB_ADMIN_ERROR, &callback);
    common::Stats::get()->Incr(kS3BackupFailure);
    return;
  }
  
if (FLAGS_enable_checkpoint_backup) {
    db_admin_lock_.Lock(request->db_name);
    SCOPE_EXIT { db_admin_lock_.Unlock(request->db_name); };

    auto db = getDB(request->db_name, &e);
    if (db == nullptr) {
      LOG(ERROR) << "Error happened when getting db for creating checkpoint: " << e.message;
      callback.release()->exceptionInThread(std::move(e));
      common::Stats::get()->Incr(kS3BackupFailure);
      return;
    }

    rocksdb::Checkpoint* checkpoint;
    auto status = rocksdb::Checkpoint::Create(db->rocksdb(), &checkpoint);
    if (!status.ok()) {
      OKOrSetException(status, AdminErrorCode::DB_ADMIN_ERROR, &callback);
      LOG(ERROR) << "Error happened when trying to initialize checkpoint: " << status.ToString();
      common::Stats::get()->Incr(kS3BackupFailure);
      return;
    }

    auto checkpoint_local_path = local_path + "checkpoint";
    status = checkpoint->CreateCheckpoint(checkpoint_local_path);
    if (!status.ok()) {
      OKOrSetException(status, AdminErrorCode::DB_ADMIN_ERROR, &callback);
      LOG(ERROR) << "Error happened when trying to create checkpoint: " << status.ToString();
      common::Stats::get()->Incr(kS3BackupFailure);
      return;
    }
    std::unique_ptr<rocksdb::Checkpoint> checkpoint_holder(checkpoint);

    std::vector<std::string> checkpoint_files;
    status = rocksdb::Env::Default()->GetChildren(checkpoint_local_path, &checkpoint_files);
    if (!status.ok()) {
      OKOrSetException(status, AdminErrorCode::DB_ADMIN_ERROR, &callback);
      LOG(ERROR) << "Error happened when trying to list files in the checkpoint: " << status.ToString();
      common::Stats::get()->Incr(kS3BackupFailure);
      return;
    }

    // Upload checkpoint to s3
    auto local_s3_util = createLocalS3Util(request->limit_mbs, request->s3_bucket);
    std::string formatted_s3_dir_path = ensure_ends_with_pathsep(request->s3_backup_dir);
    std::string formatted_checkpoint_local_path = ensure_ends_with_pathsep(checkpoint_local_path);

    for (const auto& file : checkpoint_files) {
      if (file == "." || file == "..") {
        continue;
      }
      s3_conc_->enqueuePutObject(formatted_s3_dir_path + file,
                                 formatted_checkpoint_local_path + file,
                                 //request->s3_bucket,
                                 sync_group_id);
    }

    if (request->__isset.include_meta && request->include_meta) {
      auto meta = getMetaData(request->db_name);
      std::string dbmeta_path;
      try {
        std::string encoded_meta;
        EncodeThriftStruct(meta, &encoded_meta);
        dbmeta_path = common::FileUtil::createFileWithContent(
            formatted_checkpoint_local_path, kMetaFilename, encoded_meta);
      } catch (std::exception& e) {
        SetException("Failed to create meta file, " + std::string(e.what()),
                     AdminErrorCode::DB_ADMIN_ERROR, &callback);
        common::Stats::get()->Incr(kS3BackupFailure);
        return;
      }
      s3_conc_->enqueuePutObject(formatted_s3_dir_path + kMetaFilename, dbmeta_path,
                                 //request->s3_bucket,
                                 sync_group_id);
    }
    if (!s3_conc_->Sync(formatted_s3_dir_path)) {
      SetException("Failed to sync all files to s3", AdminErrorCode::DB_ADMIN_ERROR, &callback);
      common::Stats::get()->Incr(kS3BackupFailure);
    }
    // Delete the directory to remove the snapshot.
    boost::filesystem::remove_all(local_path);

  } else {
    auto local_s3_util = createLocalS3Util(request->limit_mbs, request->s3_bucket);
    std::string formatted_s3_dir_path = rtrim(request->s3_backup_dir, '/');
    rocksdb::Env* s3_env = new rocksdb::S3Env(request->s3_bucket, formatted_s3_dir_path, local_path, std::move(local_s3_util), s3_conc_.get(), sync_group_id);
    const bool share_files_with_checksum = request->__isset.share_files_with_checksum && request->share_files_with_checksum;
    const bool include_meta = request->__isset.include_meta && request->include_meta;
    if (!backupDBHelper(request->db_name,
                        formatted_s3_dir_path,
                        std::unique_ptr<rocksdb::Env>(s3_env),
                        request->__isset.limit_mbs,
                        request->limit_mbs,
                        share_files_with_checksum,
                        include_meta,
                        &e)) {
      callback.release()->exceptionInThread(std::move(e));
      common::Stats::get()->Incr(kS3BackupFailure);
      return;
    }
  }
  if (!s3_conc_->Sync(sync_group_id)) {
    callback.release()->exceptionInThread(std::move(e));
    common::Stats::get()->Incr(kS3BackupFailure);
    return;
  }
  LOG(INFO) << "S3 Backup is done for " << request->db_name
            << " with latency(ms)" << timer.getElapsedTimeMs();
  common::Stats::get()->Incr(kS3BackupSuccess);
  callback->result(BackupDBToS3Response());
}

void AdminHandler::async_tm_restoreDBFromS3(
    std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
      RestoreDBFromS3Response>>> callback,
    std::unique_ptr<RestoreDBFromS3Request> request) {
  AdminException e;
  auto n = num_current_s3_sst_downloadings_.fetch_add(1);
  SCOPE_EXIT {
    num_current_s3_sst_downloadings_.fetch_sub(1);
  };

  if (n >= FLAGS_max_s3_sst_loading_concurrency) {
    auto err_str =
        folly::stringPrintf("Concurrent uploading/downloading limit hits %d by %s",
                            n, request->db_name.c_str());
    SetException(err_str, AdminErrorCode::DB_ADMIN_ERROR, &callback);
    LOG(ERROR) << err_str;
    common::Stats::get()->Incr(kS3RestoreFailure);
    return;
  }

  auto ts = common::timeutil::GetCurrentTimestamp();
  auto local_path = FLAGS_enable_checkpoint_backup ? FLAGS_rocksdb_dir + request->db_name :
                    folly::stringPrintf("%ss3_tmp/%s%d/", FLAGS_rocksdb_dir.c_str(), request->db_name.c_str(), ts);
  const std::string sync_group_id = request->s3_backup_dir;
  boost::system::error_code remove_err;
  boost::system::error_code create_err;
  boost::filesystem::remove_all(local_path, remove_err);
  boost::filesystem::create_directories(local_path, create_err);
  SCOPE_EXIT {
    if (!FLAGS_enable_checkpoint_backup) {
      boost::filesystem::remove_all(local_path, remove_err);
    }
  };
  if (remove_err || create_err) {
    SetException("Cannot remove/create dir for restore: " + local_path, AdminErrorCode::DB_ADMIN_ERROR, &callback);
    common::Stats::get()->Incr(kS3RestoreFailure);
    return;
  }

  auto upstream_addr = std::make_unique<folly::SocketAddress>();
  if (!SetAddressOrException(request->upstream_ip,
                             FLAGS_rocksdb_replicator_port,
                             upstream_addr.get(),
                             &callback)) {
    common::Stats::get()->Incr(kS3RestoreFailure);
    return;
  }

  common::Timer timer(kS3RestoreMs);
  LOG(INFO) << "S3 Restore " << request->db_name << " from " << request->s3_backup_dir;
  auto local_s3_util = createLocalS3Util(request->limit_mbs, request->s3_bucket);

  if (FLAGS_enable_checkpoint_backup) {
    std::string formatted_s3_dir_path = ensure_ends_with_pathsep(request->s3_backup_dir);
    std::string formatted_local_path = ensure_ends_with_pathsep(local_path);
    // fetch all files using the given path as the key prefix in S3
    auto resp = local_s3_util->listAllObjects(formatted_s3_dir_path);
    if (!resp.Error().empty()) {
      auto err_msg = folly::stringPrintf(
          "Error happened when fetching files in checkpoint from S3: %s under path: %s",
          resp.Error().c_str(), formatted_s3_dir_path.c_str());
      LOG(ERROR) << err_msg;
      SetException(err_msg, AdminErrorCode::DB_ADMIN_ERROR, &callback);
      common::Stats::get()->Incr(kS3RestoreFailure);
      return;
    }

    auto download_func = [&](const std::string& s3_path) {
      const string dest =
          formatted_local_path + s3_path.substr(formatted_s3_dir_path.size());
      LOG(INFO) << "Copying " << s3_path << " to " << dest;
      auto get_resp =
          local_s3_util->getObject(s3_path, dest, FLAGS_s3_direct_io);
      if (!get_resp.Error().empty()) {
        LOG(ERROR) << "Error happened when downloading the file in checkpoint "
                      "from S3 to local: "
                   << get_resp.Error();
        return false;
      }
      return true;
    };

    if (FLAGS_checkpoint_backup_batch_num_download> 1) {
      // Download checkpoint files to s3 in parallel
      std::vector<std::vector<std::string>> file_batches(FLAGS_checkpoint_backup_batch_num_download);
      for (size_t i = 0; i < resp.Body().objects.size(); ++i) {
        file_batches[i%FLAGS_checkpoint_backup_batch_num_download].push_back(resp.Body().objects[i]);
      }

      std::vector<folly::Future<bool>> futures;
      for (auto& files : file_batches) {
        auto p = folly::Promise<bool>();
        futures.push_back(p.getFuture());

        S3UploadAndDownloadExecutor()->add(
            [&, files = std::move(files), p = std::move(p)]() mutable {
              for (const auto& file : files) {
                if (!download_func(file)) {
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
          SetException("Error happened when downloading the file in checkpoint from S3 to local",
                       AdminErrorCode::DB_ADMIN_ERROR,
                       &callback);
          common::Stats::get()->Incr(kS3RestoreFailure);
          return;
        }
      }
    } else {
      for (auto& v : resp.Body().objects) {
        if (!download_func(v)) {
          // If there is error in one file uploading, then we fail the whole backup process
          SetException("Error happened when downloading the file in checkpoint from S3 to local",
                       AdminErrorCode::DB_ADMIN_ERROR,
                       &callback);
          common::Stats::get()->Incr(kS3RestoreFailure);
          return;
        }
      }
    }


    rocksdb::DB* restore_db;
    auto segment = common::DbNameToSegment(request->db_name);
    auto status = rocksdb::DB::Open(rocksdb_options_(segment), formatted_local_path, &restore_db);
    if (!status.ok()) {
      OKOrSetException(status, AdminErrorCode::DB_ERROR, &callback);
      LOG(ERROR) << "Error happened when opening db via checkpoint: " << status.ToString();
      common::Stats::get()->Incr(kS3RestoreFailure);
      return;
    }

    DBMetaData meta;
    std::string meta_content;
    std::string restored_dbmeta_file = formatted_local_path + kMetaFilename;
    if (boost::filesystem::exists(restored_dbmeta_file)) {
      common::FileUtil::readFileToString(restored_dbmeta_file, &meta_content);
      boost::filesystem::remove_all(restored_dbmeta_file);
      if (!DecodeThriftStruct(meta_content, &meta)) {
        std::string errMsg = "Failed to decode DBMetaData from " +
                             meta_content + " for " + request->db_name;
        SetException(errMsg, AdminErrorCode::DB_ADMIN_ERROR, &callback);
        common::Stats::get()->Incr(kS3RestoreFailure);
        return;
      }
    }
    if (!writeMetaData(request->db_name, meta.s3_bucket, meta.s3_path)) {
      std::string errMsg =
          "RestoreDBFromS3 failed to write DBMetaData for " + request->db_name;
      SetException(errMsg, AdminErrorCode::DB_ADMIN_ERROR, &callback);
      common::Stats::get()->Incr(kS3RestoreFailure);
      return;
    }

    std::string err_msg;
    if (!db_manager_->addDB(request->db_name,
                            std::unique_ptr<rocksdb::DB>(restore_db),
                            replicator::DBRole::SLAVE,
                            std::move(upstream_addr), &err_msg)) {
      LOG(ERROR) << "Error happened when adding db after restore by checkpoint: " << err_msg;
      SetException(err_msg, AdminErrorCode::DB_ADMIN_ERROR, &callback);
      common::Stats::get()->Incr(kS3RestoreFailure);
      return;
    }
  } else {
    std::string formatted_s3_dir_path = rtrim(request->s3_backup_dir, '/');
    rocksdb::Env* s3_env = new rocksdb::S3Env(request->s3_bucket, formatted_s3_dir_path, std::move(local_path),
                                              std::move(local_s3_util), s3_conc_.get(), sync_group_id);

    if (!restoreDBHelper(request->db_name,
                         formatted_s3_dir_path,
                         std::unique_ptr<rocksdb::Env>(s3_env),
                         std::move(upstream_addr),
                         request->__isset.limit_mbs,
                         request->limit_mbs,
                         &e)) {
      callback.release()->exceptionInThread(std::move(e));
      common::Stats::get()->Incr(kS3RestoreFailure);
      return;
    }
  }

  LOG(INFO) << "S3 Restore is done for " << request->db_name
            << " with latency(ms) " << timer.getElapsedTimeMs();
  callback->result(RestoreDBFromS3Response());
  common::Stats::get()->Incr(kS3RestoreSuccess);
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

  if (request->__isset.option_names && !request->option_names.empty()) {
    std::map<std::string, std::string> options_map;
    auto s = db->GetOptions(request->option_names, &options_map);
    if (s.ok()) {
      response.options = options_map;
      response.__isset.options = true;
    } else {
      LOG(ERROR) << "Failed to GetOptions from db, " << s.ToString();
    }
  }

  if (request->__isset.include_meta && request->include_meta) {
    auto meta = getMetaData(request->db_name);
    std::map<std::string, std::string> metas;
    metas["s3_bucket"] = meta.s3_bucket;
    metas["s3_path"] = meta.s3_path;
    metas["last_kafka_msg_timestamp_ms"] =
        std::to_string(meta.last_kafka_msg_timestamp_ms);
    response.db_metas = metas;
    response.__isset.db_metas = true;
  }

  if (request->__isset.property_names && !request->property_names.empty()) {
    std::map<std::string, std::string> properties;
    for (const auto& p : request->property_names) {
      std::string p_val;
      if (db->GetProperty(p, &p_val)) {
        properties[p] = p_val;
      } else {
        LOG(ERROR) << "Failed to getProperty for " << p;
      }
    }
    response.properties = properties;
    response.__isset.properties = true;
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
  if (request->new_role == "MASTER" || request->new_role == "LEADER") {
    new_role = replicator::DBRole::MASTER;
  } else if (request->new_role == "SLAVE" || request->new_role == "FOLLOWER") {
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

  auto options = rocksdb_options_(common::DbNameToSegment(request->db_name));
  auto db_path = FLAGS_rocksdb_dir + request->db_name;
  LOG(INFO) << "Clearing DB: " << request->db_name;
  clearMetaData(request->db_name);

  if (FLAGS_enable_async_delete_dbs) {
    // Move db to db_tmp/ to be cleared in the async way
    auto db_tmp_path = FLAGS_rocksdb_dir + "db_tmp/" + request->db_name + "/";
    boost::system::error_code rename_err;
    boost::filesystem::rename(db_path, db_tmp_path, rename_err);
    if (rename_err) {
      admin::AdminException e;
      e.errorCode = AdminErrorCode::DB_ADMIN_ERROR;
      e.message = folly::stringPrintf("Cannot mv db: %s to %s: %s",
                                      db_path.c_str(),
                                      db_tmp_path.c_str(),
                                      rename_err.message().c_str());
      LOG(ERROR) << e.message ;
      callback.release()->exceptionInThread(std::move(e));
      return;
    }

    LOG(INFO) << "Delay clearing DB: " << request->db_name;
  } else {
    auto status = rocksdb::DestroyDB(db_path, options);
    if (!OKOrSetException(status,
                          AdminErrorCode::DB_ADMIN_ERROR,
                          &callback)) {
      LOG(ERROR) << "Failed to clear DB " << request->db_name << " "
                 << status.ToString();
      return;
    }

    LOG(INFO) << "Done clearing DB: " << request->db_name;
  }

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

    if (!writeMetaData(request->db_name, "", "")) {
      e.message = "ClearDB with reopen failed to write initial DBMetaData for " + request->db_name;
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
    const common::S3Util& s3_util, const uint32_t s3_download_limit_mb, const std::string& s3_bucket) {
  return s3_util.getBucket() != s3_bucket ||
         s3_util.getRateLimit() != s3_download_limit_mb;
}

std::shared_ptr<common::S3Util> AdminHandler::createLocalS3Util(
    const uint32_t read_ratelimit_mb,
    const std::string& bucket) {
  // Though it is claimed that AWS s3 sdk is a light weight library. However,
  // we couldn't afford to create a new client for every SST file downloading
  // request, which is not even on any critical code path. Otherwise, we will
  // see latency spike when uploading data to production clusters.
  std::shared_ptr<common::S3Util> local_s3_util;

  {
    std::lock_guard<std::mutex> guard(s3_util_lock_);
    if (s3_util_ == nullptr || should_new_s3_client(*s3_util_, read_ratelimit_mb, bucket)) {
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
      s3_util_ = common::S3Util::BuildS3Util(read_ratelimit_mb, bucket);
    }
    local_s3_util = s3_util_;
  }

  return local_s3_util;
}

// This API is used to ingest sst files to DB with two models: ingest ahead or ingest behind.
// It will check local metaData to avoid duplicate ingestion. If ingest_behind, DB's Lmax
// must also be emtpy and DB must created with allow_ingest_behind. 
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

  // it is important to check meta first to avoid double ingestion
  auto meta = getMetaData(request->db_name);
  if (meta.__isset.s3_bucket && meta.s3_bucket == request->s3_bucket &&
      meta.__isset.s3_path && meta.s3_path == request->s3_path) {
    LOG(INFO) << "Already hosting " << meta.s3_bucket << "/" << meta.s3_path;
    callback->result(AddS3SstFilesToDBResponse());
    return;
  } else {
    LOG(INFO) << folly::stringPrintf(
        "Current meta, s3_bucket: %s, s3_path: %s. Update with, s3_bucket: %s, "
        "s3_path: %s",
        meta.s3_bucket.c_str(), meta.s3_path.c_str(),
        request->s3_bucket.c_str(), request->s3_path.c_str());
  }

  bool ingest_behind = request->__isset.ingest_behind && request->ingest_behind;
  if (ingest_behind) {
    if (!db->rocksdb()->GetDBOptions().allow_ingest_behind) {
      e.message = request->db_name + " DBOptions.allow_ingest_behind false";
      LOG(ERROR) << "DBOptions.allow_ingest_behind false, can't ingest behind "
                 << request->db_name;
      callback.release()->exceptionInThread(std::move(e));
      return;
    }
    if (!db->DBLmaxEmpty()) {
      // note: default num levels for DB is 7 (0, 1, ..., 6)
      std::string errMsg =
          "The Lmax of DB is not empty, skip ingestion to " + request->db_name;
      e.message = errMsg;
      callback.release()->exceptionInThread(std::move(e));
      LOG(ERROR) << errMsg;
      return;
    }
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


  if (FLAGS_s3_download_limit_mb > 0) {
    request->s3_download_limit_mb = FLAGS_s3_download_limit_mb;
  }
  auto local_s3_util = createLocalS3Util(request->s3_download_limit_mb, request->s3_bucket);
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

  auto segment = common::DbNameToSegment(request->db_name);
  bool allow_overlapping_keys =
      allow_overlapping_keys_segments_.find(segment) !=
      allow_overlapping_keys_segments_.end();
  // OR with the flag to make backwards compatibility
  // It is very important to allow overlapping keys if ingest to an existing DB,
  // and do not intend to clear the existing data
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
  if (ingest_behind) {
    ifo.ingest_behind = true;
  }
  auto status = db->rocksdb()->IngestExternalFile(sst_file_paths, ifo);
  if (!OKOrSetException(status,
                        AdminErrorCode::DB_ADMIN_ERROR,
                        &callback)) {
    LOG(ERROR) << "Failed to add files to DB " << request->db_name
               << status.ToString();
    return;
  }

  if (!writeMetaData(request->db_name, request->s3_bucket, request->s3_path)) {
    std::string errMsg = "AddS3SstFilesToDB failed to write DBMetaData from request for " + request->db_name;
    SetException(errMsg, AdminErrorCode::DB_ADMIN_ERROR, &callback);
    LOG(ERROR) << errMsg;
    return;
  }
  

  // attention: no compaction during backfill to prevent removing deletion tombstone
  if (FLAGS_compact_db_after_load_sst) {
    auto status = db->rocksdb()->CompactRange(nullptr, nullptr);
    if (!status.ok()) {
      LOG(ERROR) << "Failed to compact DB: " << status.ToString();
    }
  }

  callback->result(AddS3SstFilesToDBResponse());
}

void AdminHandler::async_tm_startMessageIngestion(
    std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
      StartMessageIngestionResponse>>> callback,
    std::unique_ptr<StartMessageIngestionRequest> request) {

  admin::AdminException e;
  e.errorCode = AdminErrorCode::DB_ADMIN_ERROR;

  const auto db_name = request->db_name;
  const auto topic_name = request->topic_name;
  const auto kafka_broker_serverset_path = request->kafka_broker_serverset_path;
  auto replay_timestamp_ms = request->replay_timestamp_ms;

  LOG(INFO) << "Called startMessageIngestion for db: " << db_name << ", "
            << "topic_name: " << topic_name << ", "
            << "serverset path: " << kafka_broker_serverset_path << ", "
            << "replay_timestamp_ms: " << replay_timestamp_ms;

  db_admin_lock_.Lock(db_name);
  SCOPE_EXIT { db_admin_lock_.Unlock(db_name); };
  auto db = getDB(db_name, &e);
  if (db == nullptr) {
    e.message = db_name + " doesn't exist.";
    callback.release()->exceptionInThread(std::move(e));
    LOG(ERROR) << "Database doesn't exist: " << db_name;
    return;
  }

  // Compare the value in local_meta_db with replay_timestamp_ms and choose
  // the latest.
  const auto meta = getMetaData(db_name);
  replay_timestamp_ms = std::max(meta.last_kafka_msg_timestamp_ms,
                                 replay_timestamp_ms);
  LOG(ERROR) << "Using " << replay_timestamp_ms << " as the replay timestamp "
             << "for " << db_name;

  {
    std::lock_guard<std::mutex> lock(kafka_watcher_lock_);
    // Check if there's already a thread consuming the same partition.
    if (kafka_watcher_map_.find(db_name) != kafka_watcher_map_.end()) {
      // This can happen if there are duplicate state transition messages from helix.
      // Since there is aleady a thread consuming kafka messages for this db,
      // just log and return.
      LOG(ERROR) << "Already consuming messages to " << db_name <<
                 "in another thread";
      callback.release()->result(StartMessageIngestionResponse());
      return;
    }
  }

  // Kafka partition to consume is the shard id in rocksdb.
  const auto segment = common::DbNameToSegment(db_name);
  const auto partition_id = common::ExtractShardId(db_name);

  if (partition_id == -1) {
    e.message = "Invalid db_name: " + db_name;
    callback.release()->exceptionInThread(std::move(e));
    LOG(ERROR) << "Could not find partition in db_name" << db_name;
    return;
  }

  const std::unordered_set<uint32_t> partition_ids_set({partition_id});

  auto kafka_broker_file_watcher = detail::KafkaBrokerFileWatcherManager
      ::getInstance().getFileWatcher(kafka_broker_serverset_path);

  const auto kafka_consumer_pool = std::make_shared<::kafka::KafkaConsumerPool>(
      kKafkaConsumerPoolSize,
      partition_ids_set,
      // TODO: fix this to return a string object rather than a reference
      kafka_broker_file_watcher->GetKafkaBrokerList(),
      std::unordered_set<std::string>({topic_name}),
      getConsumerGroupId(db_name),
      folly::stringPrintf("%s_%s", kKafkaConsumerType, segment.c_str()));

  auto kafka_watcher = std::make_shared<KafkaWatcher>(
      folly::stringPrintf("%s_%s", kKafkaWatcherName, segment.c_str()),
      kafka_consumer_pool,
      -1, // kafka_init_blocking_consume_timeout_ms
      FLAGS_kafka_consumer_timeout_ms);

  {
    std::lock_guard<std::mutex> lock(kafka_watcher_lock_);
    kafka_watcher_map_[db_name] = kafka_watcher;
  }

  int64_t message_count = 0;
  const auto should_deserialize = request->is_kafka_payload_serialized;

  // With kafka_init_blocking_consume_timeout_ms set to -1, messages from
  // replay_timestamp_ms to the current are synchronously consumed. The
  // calling thread then returns after spawning a new thread to consume
  // live messages.
  kafka_watcher->StartWith(
      replay_timestamp_ms,
      [message_count, db_name, db, should_deserialize,
       segment = std::move(segment), this](
          std::shared_ptr<const RdKafka::Message> message,
          const bool is_replay) mutable {
    if (message == nullptr) {
      LOG(ERROR) << "Message nullptr";
      return;
    }
    const int64_t msg_timestamp_secs = GetMessageTimestampSecs(*message);
    message_count++;

    // Logs for debugging, only enabled if flag is specified.
    // In case of sensitive data... we shouldn't be logging message, unless
    // explicitly configured to do so (may be in order to debug)
    if (FLAGS_enable_logging_consumer_log) {
      LOG_EVERY_N(INFO, FLAGS_consumer_log_frequency)
        << "DB name: " << db_name << ", Key " << folly::hexlify(*message->key())
        << ", "
        << "value "
        << ((FLAGS_enable_logging_consumer_log_with_payload)
          ? (folly::hexlify(folly::StringPiece(
            static_cast<const char *>(message->payload()), message->len())))
          : ("***REDACTED***"))
        << ", "
        << "partition: " << message->partition() << ", "
        << "offset: " << message->offset() << ", "
        << "payload len: " << message->len() << ", "
        << "msg_timestamp: " << ToUTC(msg_timestamp_secs) << " or "
        << std::to_string(msg_timestamp_secs) << " secs";
    }

    if (!is_replay) {
      auto latency_ms = common::timeutil::GetCurrentTimestamp(
          common::timeutil::TimeUnit::kMillisecond)
                        - message->timestamp().timestamp;
      common::Stats::get()->AddMetric(folly::stringPrintf("%s segment=%s",
          kKafkaConsumerLatency.c_str(), segment.c_str()), latency_ms);
    }

    auto key = rocksdb::Slice(static_cast<const char *>(message->key_pointer()),
                              message->key_len());

    // Deserialize the kafka payload
    KafkaOperationCode op_code;
    std::string deser_val;
    rocksdb::Slice value;
    if (should_deserialize) {
      if (DeserializeKafkaPayload(message->payload(),
          message->len(), &op_code, &deser_val)) {
        value = rocksdb::Slice(deser_val);
      } else {
        LOG(ERROR) << "Failed to deserialize. Ignoring kafka message";
        return;
      }
    } else {
      // If serialization is not required, just put the value to rocksdb
      op_code = KafkaOperationCode::PUT;
      value = rocksdb::Slice(static_cast<const char *>(message->payload()),
          message->len());
    }

    // Write the message to rocksdb
    rocksdb::Status status;
    static const rocksdb::WriteOptions write_options;

    switch (op_code) {
      case KafkaOperationCode::PUT:
        common::Stats::get()->Incr(folly::stringPrintf("%s segment=%s",
            kKafkaDbPutMessage.c_str(), segment.c_str()));

        status = db->rocksdb()->Put(write_options, key, value);
        if (!status.ok()) {
          LOG(ERROR) << "Failure while writing to " << db_name << ": "
                     << status.ToString();
          common::Stats::get()->Incr(folly::stringPrintf("%s segment=%s",
              kKafkaDbPutErrors.c_str(), segment.c_str()));
        }

        break;
      case KafkaOperationCode::DELETE:
        common::Stats::get()->Incr(folly::stringPrintf("%s segment=%s",
            kKafkaDbDelMessage.c_str(), segment.c_str()));
        status = db->rocksdb()->Delete(write_options, key);
        if (!status.ok()) {
          LOG(ERROR) << "Failure while deleting from " << db_name << ": "
                     << status.ToString();
          common::Stats::get()->Incr(folly::stringPrintf("%s segment=%s",
              kKafkaDbDeleteErrors.c_str(), segment.c_str()));
        }
        break;
      case KafkaOperationCode::MERGE:
        common::Stats::get()->Incr(folly::stringPrintf("%s segment=%s",
            kKafkaDbMergeMessage.c_str(), segment.c_str()));
        status = db->rocksdb()->Merge(write_options, key, value);
        if (!status.ok()) {
          LOG(ERROR) << "Failure while merging to " << db_name << ": "
                     << status.ToString();
          common::Stats::get()->Incr(folly::stringPrintf("%s segment=%s",
              kKafkaDbMergeErrors.c_str(), segment.c_str()));
        }
        break;
      default:
        common::Stats::get()->Incr(folly::stringPrintf("%s segment=%s",
            kKafkaInvalidOpcode.c_str(), segment.c_str()));
        LOG(ERROR) << "Invalid op_code in kafka payload";
    }

    // Update meta_db with kafka message timestamp periodically.
    if (message_count % FLAGS_kafka_ts_update_interval == 0) {
      const auto timestamp_ms = message->timestamp().timestamp;
      const auto meta = getMetaData(db_name);
      if (!writeMetaData(db_name, meta.s3_bucket, meta.s3_path, timestamp_ms)) {
        LOG(ERROR) << "StartMessageIngestion failed to write DBMetaData for " << db_name;
        return;
      } 
      LOG(INFO) << "[meta_db] Writing timestamp " << timestamp_ms
                << " for db: " << db_name;
    }
  });

  LOG(INFO) << "Now consuming live messages for " << db_name;

  // Live messages continue to consumed in a separate thread, but release
  // callback and return so that helix can transition this partition to
  // bootstrap stage and the admin thread can be freed.
  callback.release()->result(StartMessageIngestionResponse());
}

void AdminHandler::async_tm_stopMessageIngestion(
    std::unique_ptr<apache::thrift::HandlerCallback<std::unique_ptr<
      StopMessageIngestionResponse>>> callback,
    std::unique_ptr<StopMessageIngestionRequest> request) {

  admin::AdminException e;
  e.errorCode = AdminErrorCode::DB_ADMIN_ERROR;

  const auto db_name = request->db_name;
  LOG(ERROR) << "Called stopMessageIngestion for " << db_name;
  db_admin_lock_.Lock(db_name);
  SCOPE_EXIT { db_admin_lock_.Unlock(db_name); };
  auto db = getDB(db_name, &e);
  if (db == nullptr) {
    e.message = db_name + " doesn't exist.";
    callback.release()->exceptionInThread(std::move(e));
    LOG(ERROR) << "Database doesn't exist: " << db_name;
    return;
  }

  std::shared_ptr<KafkaWatcher> kafka_watcher;
  {
    std::lock_guard<std::mutex> lock(kafka_watcher_lock_);
    auto iter = kafka_watcher_map_.find(db_name);
    // Verify that there is thread consuming messages for this db.
    if (iter == kafka_watcher_map_.end()) {
      // Log and return if there is no thread consuming kafka messages.
      callback.release()->result(StopMessageIngestionResponse());
      LOG(ERROR) << db_name << " is not being currently consumed";
      return;
    }
    kafka_watcher = iter->second;
  }

  // Stop the watcher.
  LOG(ERROR) << "Stopping kafka watcher";
  kafka_watcher->StopAndWait();
  LOG(ERROR) << "Kafka watcher stopped";

  {
    std::lock_guard<std::mutex> lock(kafka_watcher_lock_);
    kafka_watcher_map_.erase(db_name);
  }

  callback.release()->result(StopMessageIngestionResponse());
  return;
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

std::string AdminHandler::DumpDBStatsAsText() const {
  return db_manager_->DumpDBStatsAsText();
}

std::vector<std::string> AdminHandler::getAllDBNames() {
    return db_manager_->getAllDBNames();
}

}  // namespace admin
