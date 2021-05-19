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

#include <algorithm>
#include <ctime>
#include <map>
#include <thread>

#include "boost/filesystem.hpp"
#include "folly/SocketAddress.h"
#include "gtest/gtest.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb_admin/application_db.h"
#define private public
#include "rocksdb_admin/admin_handler.h"
#undef private
#include "rocksdb_admin/gen-cpp2/Admin.h"
#include "rocksdb_admin/tests/test_util.h"
#include "rocksdb_admin/utils.h"

using admin::AddDBRequest;
using admin::AddDBResponse;
using admin::AddS3SstFilesToDBRequest;
using admin::AddS3SstFilesToDBResponse;
using admin::AdminAsyncClient;
using admin::AdminException;
using admin::AdminHandler;
using admin::ApplicationDBManager;
using admin::BackupDBToS3Request;
using admin::BackupDBToS3Response;
using admin::ChangeDBRoleAndUpstreamRequest;
using admin::ChangeDBRoleAndUpstreamResponse;
using admin::CheckDBRequest;
using admin::CheckDBResponse;
using admin::ClearDBRequest;
using admin::ClearDBResponse;
using admin::CloseDBRequest;
using admin::DBMetaData;
using admin::RestoreDBFromS3Request;
using admin::RestoreDBFromS3Response;
using admin::SetDBOptionsRequest;
using admin::SetDBOptionsResponse;
using apache::thrift::HeaderClientChannel;
using apache::thrift::RpcOptions;
using apache::thrift::ThriftServer;
using apache::thrift::async::TAsyncSocket;
using common::ThriftClientPool;
using rocksdb::FlushOptions;
using rocksdb::Options;
using rocksdb::ReadOptions;
using rocksdb::Status;
using std::make_shared;
using std::make_tuple;
using std::make_unique;
using std::map;
using std::string;
using std::thread;
using std::to_string;
using std::tuple;
using std::chrono::seconds;
using std::this_thread::sleep_for;

namespace fs = boost::filesystem;

// FLAGS: enable_integration_test, s3_bucket, s3_backup_prefix must be assigned
// if intend to test the admin APIs and enable data upload/download from cloud
DEFINE_bool(enable_integration_test, false,
            "Enable actual upload/download data from cloud");
DEFINE_string(s3_bucket, "pinterest-jackson", "The s3 bucket");
DEFINE_string(s3_backup_prefix, "tmp/backup_test/admin_handler_test/",
              "The s3 key prefix for backup");
DEFINE_bool(
    test_db_create_with_allow_ingest_behind, false,
    "If true, db is created with allow_ingest_behind; by default, it is false");
DECLARE_string(rocksdb_dir);
DECLARE_bool(enable_checkpoint_backup);
DECLARE_bool(rocksdb_allow_overlapping_keys);

void clearAndCreateDir(string dir_path) {
  boost::system::error_code create_err;
  boost::system::error_code remove_err;
  fs::remove_all(dir_path, remove_err);
  fs::create_directories(dir_path, create_err);
  EXPECT_FALSE(remove_err || create_err);
}

string generateRandIntAsStr() {
  static thread_local unsigned int seed = time(nullptr);
  return to_string(rand_r(&seed));
}

const string generateDBName() { return "test_db_" + generateRandIntAsStr(); }

Options getOptions(const string& segment) {
  Options options;
  options.create_if_missing = true;
  options.WAL_ttl_seconds = 123;
  // in production, this will be cluster level once-and-set flag
  if (FLAGS_test_db_create_with_allow_ingest_behind) {
    options.allow_ingest_behind = true;
  }
  return options;
}

tuple<shared_ptr<AdminHandler>, shared_ptr<ThriftServer>, shared_ptr<thread>,
      ApplicationDBManager*>
makeServer(uint16_t port) {
  auto db_manager = std::make_unique<ApplicationDBManager>();
  ApplicationDBManager* manager = db_manager.get();

  auto handler = make_shared<AdminHandler>(std::move(db_manager), getOptions);

  auto server = make_shared<ThriftServer>();
  server->setPort(port);
  server->setInterface(handler);
  auto t = make_shared<thread>([server, port] {
    LOG(INFO) << "Start routing server on port " << port;
    server->serve();
    LOG(INFO) << "Exit routing server on port " << port;
  });
  return make_tuple(handler, server, move(t), manager);
}

namespace admin {

class AdminHandlerTestBase : public testing::Test {
 public:
  AdminHandlerTestBase() {
    // setup local , s3 paths for test
    FLAGS_rocksdb_dir = testDir();
    FLAGS_s3_backup_prefix =
        FLAGS_s3_backup_prefix + testSessionIdAsStr() + "/";

    LOG(INFO) << "Test start, clearAndCreateDir " << testDir();
    clearAndCreateDir(testDir());

    tie(handler_, server_, thread_, db_manager_) = makeServer(8090);
    sleep_for(seconds(1));

    pool_ = make_shared<ThriftClientPool<AdminAsyncClient>>(1);
    client_ = pool_->getClient("127.0.0.1", 8090);
  }

  ~AdminHandlerTestBase() {
    LOG(INFO)
        << "Test done, now stop serer and join server thread and cleanup: "
        << testDir();

    server_->stop();
    thread_->join();

    boost::system::error_code remove_err;
    fs::remove_all(testDir(), remove_err);
    EXPECT_FALSE(remove_err);
    // TODO: should remove test files from S3 as well
  }

  void addDBWithRole(const string db_name, const string db_role) {
    AddDBRequest req;
    req.db_name = db_name;
    req.upstream_ip = localIP();
    if (db_role == "SLAVE" || db_role == "FOLLOWER" || db_role == "NOOP") {
      req.set_db_role(db_role);
    }
    AddDBResponse resp;
    EXPECT_NO_THROW(resp = client_->future_addDB(req).get());
    if (db_role == "MASTER" || db_role == "LEADER") {
      changeDBRoleAndUpstream(db_name, db_role, localIP());
    }
    LOG(INFO) << "Successfully added db: " << db_name
              << " with role: " << db_role;
  }

  void changeDBRoleAndUpstream(const string db_name, const string new_role,
                               const string upstream_ip) {
    if (new_role != "MASTER" && new_role != "LEADER" && new_role != "SLAVE" &&
        new_role != "FOLLOWER") {
      ASSERT_FALSE(true) << "Unkownn new_role: " + new_role;
    }
    ChangeDBRoleAndUpstreamRequest req;
    req.db_name = db_name;
    req.new_role = new_role;
    req.set_upstream_ip(upstream_ip);
    ChangeDBRoleAndUpstreamResponse resp;
    EXPECT_NO_THROW(resp = client_->future_changeDBRoleAndUpStream(req).get());
    LOG(INFO) << "Successfully change db role to " << new_role
              << ", upstreamIP " << upstream_ip;
  }

  void deleteKeyFromDB(const string db_name, const string key) {
    rocksdb::WriteBatch batch;
    EXPECT_TRUE(batch.Delete(key).ok());
    auto app_db = db_manager_->getDB(db_name, nullptr);
    EXPECT_TRUE(app_db->Write(rocksdb::WriteOptions(), &batch).ok());
  }

  void writeToDB(const string db_name, const string key, const string val) {
    rocksdb::WriteBatch batch;
    batch.Put(key, val);
    auto app_db = db_manager_->getDB(db_name, nullptr);
    EXPECT_TRUE(app_db->Write(rocksdb::WriteOptions(), &batch).ok());
  }

  void flushDB(const string db_name) {
    auto app_db = db_manager_->getDB(db_name, nullptr);
    auto s = app_db->rocksdb()->Flush(FlushOptions());
    EXPECT_TRUE(s.ok());
  }

  void clearDB(const string db_name) {
    ClearDBResponse clear_resp;
    ClearDBRequest clear_req;
    clear_req.db_name = db_name;
    // clear_req.set_reopen_db(true); // by default, alwasys reopen
    EXPECT_NO_THROW(clear_resp = client_->future_clearDB(clear_req).get());
  }

  void EXPECT_dbValForKey(const string db_name, const string key,
                          const string expected_val) {
    auto testdb_appdb = db_manager_->getDB(db_name, nullptr);
    EXPECT_TRUE(testdb_appdb != nullptr);
    string val;
    auto s = testdb_appdb->Get(ReadOptions(), key, &val);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(val, expected_val);
  }

  void EXPECT_noValForKey(const string db_name, const string key) {
    auto testdb_appdb = db_manager_->getDB(db_name, nullptr);
    EXPECT_TRUE(testdb_appdb != nullptr);
    string val;
    auto s = testdb_appdb->Get(ReadOptions(), key, &val);
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), Status::Code::kNotFound);
  }

  const string& testDir() {
    static const string testDir =
        "/tmp/admin_handler_test/" + testSessionIdAsStr() + "/";
    return testDir;
  }

  // a random int (str) append to path to avoid collision among multiple runs of
  // the same test
  const string& testSessionIdAsStr() {
    static const string testSessionIdAsStr = generateRandIntAsStr();
    return testSessionIdAsStr;
  }

  const string& localIP() {
    static const string localIP = "127.0.0.1";
    return localIP;
  }

  void verifyMeta(DBMetaData meta, const string& db_name, bool option_set,
                  const string s3_bucket, const string s3_path) {
    EXPECT_EQ(meta.db_name, db_name);
    if (option_set) {
      EXPECT_TRUE(meta.__isset.s3_bucket);
      EXPECT_EQ(meta.s3_bucket, s3_bucket);
      EXPECT_TRUE(meta.__isset.s3_path);
      EXPECT_EQ(meta.s3_path, s3_path);
    } else {
      EXPECT_FALSE(meta.__isset.s3_bucket);
      EXPECT_FALSE(meta.__isset.s3_path);
    }
  }

 public:
  shared_ptr<ThriftClientPool<AdminAsyncClient>> pool_;
  shared_ptr<AdminAsyncClient> client_;
  shared_ptr<AdminHandler> handler_;
  shared_ptr<ThriftServer> server_;
  shared_ptr<thread> thread_;
  // this is very hacky, since db_manager is a unique_ptr inside AdminHandler
  ApplicationDBManager* db_manager_;
};

TEST_F(AdminHandlerTestBase, SetRocksDBOptions) {
  const string testdb = generateDBName();
  addDBWithRole(testdb, "MASTER");
  auto app_db = db_manager_->getDB(testdb, nullptr);

  SetDBOptionsResponse resp;
  SetDBOptionsRequest req;
  req.set_db_name(testdb);

  // Verify: set mutable db options -> ok
  EXPECT_FALSE(app_db->rocksdb()->GetOptions().disable_auto_compactions);
  req.options = {{"disable_auto_compactions", "true"}};
  EXPECT_NO_THROW(resp = client_->future_setDBOptions(req).get());
  EXPECT_TRUE(app_db->rocksdb()->GetOptions().disable_auto_compactions);

  CheckDBRequest check_req;
  CheckDBResponse check_resp;
  check_req.db_name = testdb;
  check_req.__isset.option_names = true;
  check_req.option_names = {"disable_auto_compactions"};
  EXPECT_NO_THROW(check_resp = client_->future_checkDB(check_req).get());
  EXPECT_EQ(check_resp.options["disable_auto_compactions"], "true");

  // Verify: set immutable db options -> !ok
  EXPECT_FALSE(app_db->rocksdb()->GetOptions().allow_ingest_behind);
  req.options = {{"allow_ingest_behind", "true"}};
  EXPECT_THROW(resp = client_->future_setDBOptions(req).get(), AdminException);
}

TEST_F(AdminHandlerTestBase, AddS3SstFilesToDBTest) {
  const string testdb = generateDBName();
  addDBWithRole(testdb, "MASTER");
  auto meta = handler_->getMetaData(testdb);
  verifyMeta(meta, testdb, true, "", "");

  string sstDir1 = FLAGS_s3_backup_prefix + "tempsst/";
  string sstDir2 = FLAGS_s3_backup_prefix + "tempsst2/";

  if (FLAGS_enable_integration_test) {
    // not use checkpoint for db upload/download
    {
      EXPECT_FALSE(FLAGS_enable_checkpoint_backup);

      AddS3SstFilesToDBResponse adds3_resp;

      // 0st ingest from an empty s3_path -> exception
      {
        AddS3SstFilesToDBRequest adds3_req_emptys3path;
        adds3_req_emptys3path.db_name = testdb;
        adds3_req_emptys3path.s3_bucket = FLAGS_s3_bucket;
        adds3_req_emptys3path.s3_path = sstDir1;
        EXPECT_THROW(
            adds3_resp =
                client_->future_addS3SstFilesToDB(adds3_req_emptys3path).get(),
            AdminException);
      }

      // 1st addS3 from sstDir1 -> ok
      string sst_file1 = "/tmp/file1.sst";
      list<pair<string, string>> sst1_content = {{"1", "1"}};
      createSstWithContent(sst_file1, sst1_content);
      string s3_fullpath_sst1 = sstDir1 + "/file1.sst";
      putObjectFromLocalToS3(sst_file1, FLAGS_s3_bucket, s3_fullpath_sst1);

      AddS3SstFilesToDBRequest adds3_req_sstdir1;
      adds3_req_sstdir1.db_name = testdb;
      adds3_req_sstdir1.s3_bucket = FLAGS_s3_bucket;
      adds3_req_sstdir1.s3_path = sstDir1;

      // 1s ingest to a new DB from a new s3_path -> ok
      {
        adds3_resp = client_->future_addS3SstFilesToDB(adds3_req_sstdir1).get();
        auto meta_after_adds3 = handler_->getMetaData(testdb);
        verifyMeta(meta_after_adds3, testdb, true, adds3_req_sstdir1.s3_bucket,
                   adds3_req_sstdir1.s3_path);
        EXPECT_dbValForKey(testdb, "1", "1");
      }

      // update the data at the same s3_path
      string sst_file2 = "/tmp/file2.sst";
      list<pair<string, string>> sst2_content = {{"2", "2"}};
      createSstWithContent(sst_file2, sst2_content);
      string s3_fullpath_sst2 = sstDir1 + "/file2.sst";
      putObjectFromLocalToS3(sst_file2, FLAGS_s3_bucket, s3_fullpath_sst2);

      // 2nd ingestion from ssDir1 again -> skipped
      // Verify: addS3SstFilesToDB with same s3_path will be skipped even the
      // data at the s3_path might have updated since last ingestion
      {
        EXPECT_NO_THROW(
            adds3_resp =
                client_->future_addS3SstFilesToDB(adds3_req_sstdir1).get());
        auto meta_after_adds3_sames3path = handler_->getMetaData(testdb);
        verifyMeta(meta_after_adds3_sames3path, testdb, true,
                   adds3_req_sstdir1.s3_bucket, adds3_req_sstdir1.s3_path);
        EXPECT_noValForKey(testdb, "2");
      }

      // 3rd ingest, ingest behind after clearDB and reopen a default DB (ie
      // allow_ingest_behind=false) -> exception
      {
        adds3_req_sstdir1.set_ingest_behind(true);
        SCOPE_EXIT { adds3_req_sstdir1.set_ingest_behind(false); };

        clearDB(testdb);
        EXPECT_THROW(
            adds3_resp =
                client_->future_addS3SstFilesToDB(adds3_req_sstdir1).get(),
            AdminException);
      }

      // create a new sst a new path
      string sst_file3 = "/tmp/file2.sst";
      list<pair<string, string>> sst3_content = {{"3", "3"}};
      createSstWithContent(sst_file3, sst3_content);
      string s3_fullpath_sst3 = sstDir2 + "/file3.sst";
      putObjectFromLocalToS3(sst_file3, FLAGS_s3_bucket, s3_fullpath_sst3);

      // 4th ingest, ingest behind from ssDir1 after clearDB with reopen with
      // allow_ingest_behind -> ok
      // Then, ingest behind from sstDir2 will violate Lmax.empty()->exception
      {
        FLAGS_test_db_create_with_allow_ingest_behind = true;
        // by default, not allow overlap, so always will recreate DB
        EXPECT_FALSE(FLAGS_rocksdb_allow_overlapping_keys);

        adds3_req_sstdir1.set_ingest_behind(true);
        SCOPE_EXIT {
          FLAGS_test_db_create_with_allow_ingest_behind = false;
          adds3_req_sstdir1.set_ingest_behind(false);
        };

        clearDB(testdb);
        EXPECT_NO_THROW(
            adds3_resp =
                client_->future_addS3SstFilesToDB(adds3_req_sstdir1).get());

        // violate Lmax.empty()
        AddS3SstFilesToDBRequest adds3_req_sstdir2;
        adds3_req_sstdir2.db_name = testdb;
        adds3_req_sstdir2.s3_bucket = FLAGS_s3_bucket;
        // a new s3_path is used
        adds3_req_sstdir2.s3_path = sstDir2;
        adds3_req_sstdir2.set_ingest_behind(true);
        EXPECT_THROW(
            adds3_resp =
                client_->future_addS3SstFilesToDB(adds3_req_sstdir2).get(),
            AdminException);
      }

      // 5th ingest ahead to existing DB from sstDir1 -> ok
      // and, data is indeed ingested ahead
      {
        FLAGS_rocksdb_allow_overlapping_keys = true;
        SCOPE_EXIT { FLAGS_rocksdb_allow_overlapping_keys = false; };

        clearDB(testdb);
        writeToDB(testdb, "1", "1_1");
        EXPECT_NO_THROW(
            adds3_resp =
                client_->future_addS3SstFilesToDB(adds3_req_sstdir1).get());
        EXPECT_dbValForKey(testdb, "1", "1");
      }

      // 6th, ingest behind to existing DB from sstDir1 -> ok
      // and, data is indeed ingested behind
      {
        FLAGS_test_db_create_with_allow_ingest_behind = true;
        FLAGS_rocksdb_allow_overlapping_keys = true;
        adds3_req_sstdir1.set_ingest_behind(true);
        SCOPE_EXIT {
          FLAGS_test_db_create_with_allow_ingest_behind = false;
          FLAGS_rocksdb_allow_overlapping_keys = false;
          adds3_req_sstdir1.set_ingest_behind(false);
        };

        clearDB(testdb);
        writeToDB(testdb, "1", "1_1");
        EXPECT_NO_THROW(
            adds3_resp =
                client_->future_addS3SstFilesToDB(adds3_req_sstdir1).get());
        EXPECT_dbValForKey(testdb, "1", "1_1");
      }
    }
  }
}

TEST_F(AdminHandlerTestBase, AdminAPIsWithWriteMeta) {
  // verify: meta is written with init val (db_name, empty s3_path/bucket) at
  // addDB
  const string testdb = generateDBName();
  addDBWithRole(testdb, "MASTER");
  auto meta = handler_->getMetaData(testdb);
  verifyMeta(meta, testdb, true, "", "");

  if (FLAGS_enable_integration_test) {
    // use checkpoint for db upload/download
    {
      FLAGS_enable_checkpoint_backup = true;
      SCOPE_EXIT { FLAGS_enable_checkpoint_backup = false; };

      handler_->writeMetaData(testdb, "fakes3bucket", "fakes3path");

      BackupDBToS3Request backup_req;
      backup_req.db_name = testdb;
      backup_req.s3_bucket = FLAGS_s3_bucket;
      backup_req.s3_backup_dir =
          FLAGS_s3_backup_prefix + "checkpoint/" + testdb;
      BackupDBToS3Response backup_resp;
      LOG(INFO) << "Backup db: " << testdb << " to "
                << backup_req.s3_backup_dir;
      EXPECT_NO_THROW(backup_resp =
                          client_->future_backupDBToS3(backup_req).get());

      CloseDBRequest close_req;
      close_req.db_name = testdb;
      auto close_resp = client_->future_closeDB(close_req).get();

      LOG(INFO) << "Restore db: " << testdb;
      RestoreDBFromS3Request restore_req;
      restore_req.db_name = testdb;
      restore_req.s3_bucket = FLAGS_s3_bucket;
      restore_req.s3_backup_dir =
          FLAGS_s3_backup_prefix + "checkpoint/" + testdb;
      restore_req.upstream_ip = localIP();
      restore_req.upstream_port = 8090;
      RestoreDBFromS3Response restore_resp;
      EXPECT_NO_THROW(restore_resp =
                          client_->future_restoreDBFromS3(restore_req).get());
      // the restoreDBHelper will overwrite existing meta with an empty_meta
      // since no dbmeta file is uploaded from Backup path
      auto meta_after_restore = handler_->getMetaData(testdb);
      verifyMeta(meta_after_restore, testdb, true, "", "");

      handler_->clearMetaData(testdb);

      // verify backup & restore with meta for checkpoint
      handler_->writeMetaData(testdb, "fakes3bucket", "fakes3path");
      backup_req.set_include_meta(true);
      EXPECT_NO_THROW(backup_resp =
                          client_->future_backupDBToS3(backup_req).get());
      close_resp = client_->future_closeDB(close_req).get();
      EXPECT_NO_THROW(restore_resp =
                          client_->future_restoreDBFromS3(restore_req).get());
      meta_after_restore = handler_->getMetaData(testdb);
      verifyMeta(meta_after_restore, testdb, true, "fakes3bucket",
                 "fakes3path");
    }

    // not use checkpoint for db upload/download
    {
      EXPECT_FALSE(FLAGS_enable_checkpoint_backup);

      // verify: restoreDBHelper write meta
      BackupDBToS3Request backup_req;
      backup_req.db_name = testdb;
      backup_req.s3_bucket = FLAGS_s3_bucket;
      backup_req.s3_backup_dir = FLAGS_s3_backup_prefix + testdb;
      BackupDBToS3Response backup_resp;
      LOG(INFO) << "Backup db: " << testdb << " to "
                << backup_req.s3_backup_dir;
      EXPECT_NO_THROW(backup_resp =
                          client_->future_backupDBToS3(backup_req).get());

      CloseDBRequest close_req;
      close_req.db_name = testdb;
      auto close_resp = client_->future_closeDB(close_req).get();

      EXPECT_TRUE(handler_->clearMetaData(testdb));
      auto empty_meta = handler_->getMetaData(testdb);
      verifyMeta(empty_meta, testdb, false, "", "");

      LOG(INFO) << "Restore db: " << testdb;
      RestoreDBFromS3Request restore_req;
      restore_req.db_name = testdb;
      restore_req.s3_bucket = FLAGS_s3_bucket;
      restore_req.s3_backup_dir = FLAGS_s3_backup_prefix + testdb;
      restore_req.upstream_ip = localIP();
      restore_req.upstream_port = 8090;
      RestoreDBFromS3Response restore_resp;
      EXPECT_NO_THROW(restore_resp =
                          client_->future_restoreDBFromS3(restore_req).get());
      // the restoreDBHelper will write from an empty_meta
      auto meta_after_restore = handler_->getMetaData(testdb);
      verifyMeta(meta_after_restore, testdb, true, "", "");

      // Verify: clearDB with reopen will have a DBMetaData with init val
      EXPECT_TRUE(handler_->clearMetaData(testdb));
      clearDB(testdb);
      auto meta_after_clearreopen = handler_->getMetaData(testdb);
      verifyMeta(meta_after_clearreopen, testdb, true, "", "");

      // Verify: addS3SstFilesToDB with updated meta
      string sst_file1 = "/tmp/file1.sst";
      list<pair<string, string>> sst1_content = {{"1", "1"}, {"2", "2"}};
      createSstWithContent(sst_file1, sst1_content);
      string s3_fullpath_sst1 = FLAGS_s3_backup_prefix + "tempsst/file1.sst";
      putObjectFromLocalToS3(sst_file1, FLAGS_s3_bucket, s3_fullpath_sst1);

      AddS3SstFilesToDBRequest adds3_req;
      adds3_req.db_name = testdb;
      adds3_req.s3_bucket = FLAGS_s3_bucket;
      adds3_req.s3_path = FLAGS_s3_backup_prefix + "tempsst/";
      auto adds3_resp = client_->future_addS3SstFilesToDB(adds3_req).get();
      auto meta_after_adds3 = handler_->getMetaData(testdb);
      verifyMeta(meta_after_adds3, testdb, true, adds3_req.s3_bucket,
                 adds3_req.s3_path);
    }

    // Skip Verify: startMessageIngestion with updated meta
  }
}

TEST_F(AdminHandlerTestBase, CheckDB) {
  const string testdb1 = generateDBName();
  addDBWithRole(testdb1, "MASTER");
  auto dbs = db_manager_->getAllDBNames();
  EXPECT_EQ(dbs.size(), (unsigned)1);
  EXPECT_NE(std::find(dbs.begin(), dbs.end(), testdb1), dbs.end());

  CheckDBRequest req;
  CheckDBResponse res;

  {
    SCOPE_EXIT {
      req.__clear();
      res.__clear();
    };

    req.db_name = "unknown_db";
    EXPECT_THROW(res = client_->future_checkDB(req).get(), AdminException);
  }

  // Verify checkDB of newly DB with default vals
  {
    SCOPE_EXIT {
      req.__clear();
      res.__clear();
    };

    req.db_name = testdb1;

    EXPECT_NO_THROW(res = client_->future_checkDB(req).get());
    EXPECT_EQ(res.seq_num, 0);
    EXPECT_EQ(res.wal_ttl_seconds, 123);
    EXPECT_EQ(res.last_update_timestamp_ms, 0);
    EXPECT_FALSE(res.__isset.db_metas);
    EXPECT_FALSE(res.__isset.properties);
  }

  // Verify checkDB from existed DB
  {
    SCOPE_EXIT {
      req.__clear();
      res.__clear();
    };

    const string testdb2 = generateDBName();
    addDBWithRole(testdb2, "MASTER");
    dbs = db_manager_->getAllDBNames();
    EXPECT_NE(std::find(dbs.begin(), dbs.end(), testdb2), dbs.end());
    deleteKeyFromDB(testdb2, "a");

    req.db_name = testdb2;

    EXPECT_NO_THROW(res = client_->future_checkDB(req).get());
    EXPECT_EQ(res.seq_num, 1);
    EXPECT_EQ(res.wal_ttl_seconds, 123);
    // 1521000000 is 03/14/2018 @ 4:00am (UTC)
    EXPECT_GT(res.last_update_timestamp_ms, 1521000000);
  }

  // verify: checkDB get metadata
  {
    SCOPE_EXIT {
      req.__clear();
      res.__clear();
    };

    const string testdb3 = generateDBName();
    addDBWithRole(testdb3, "MASTER");
    auto meta = handler_->getMetaData(testdb3);
    verifyMeta(meta, testdb3, true, "", "");

    // write fake DBMetadata for <testdb3>
    handler_->writeMetaData(testdb3, "fakes3bucket", "fakes3path");
    meta = handler_->getMetaData(testdb3);
    verifyMeta(meta, testdb3, true, "fakes3bucket", "fakes3path");

    req.db_name = testdb3;
    req.set_include_meta(true);

    EXPECT_NO_THROW(res = client_->future_checkDB(req).get());
    EXPECT_TRUE(res.__isset.db_metas);
    EXPECT_EQ(res.db_metas["s3_bucket"], "fakes3bucket");
    EXPECT_EQ(res.db_metas["s3_path"], "fakes3path");
  }

  // verify CheckDB get options
  {
    SCOPE_EXIT {
      req.__clear();
      res.__clear();
    };

    map<string, string> option_with_expvals{
        {"WAL_ttl_seconds", "123"},
        {"allow_ingest_behind", "false"},
        {"disable_auto_compactions", "false"}};
    vector<string> option_names{"unknown_option_name"};
    for (const auto& map_entry : option_with_expvals) {
      option_names.push_back(map_entry.first);
    }

    req.db_name = testdb1;
    req.option_names = option_names;
    req.__isset.option_names = true;

    EXPECT_NO_THROW(res = client_->future_checkDB(req).get());
    for (const auto& option_name : option_names) {
      if (option_name == "unknown_option_name") {
        EXPECT_EQ(res.options.find(option_name), res.options.end());
      } else {
        EXPECT_EQ(res.options[option_name], option_with_expvals[option_name]);
      }
    }

    // get DB options when created with allow_ingest_behind
    {
      FLAGS_test_db_create_with_allow_ingest_behind = true;
      SCOPE_EXIT { FLAGS_test_db_create_with_allow_ingest_behind = false; };
      const string testdbOptions2 = generateDBName();
      addDBWithRole(testdbOptions2, "MASTER");

      req.db_name = testdbOptions2;

      EXPECT_NO_THROW(res = client_->future_checkDB(req).get());
      EXPECT_EQ(res.options["allow_ingest_behind"], "true");
    }
  }

  const string app_p_not_define = "applicationdb.not-defined-property";
  const string rocksdb_p_not_define = "rocksdb.not-defined-property";
  const string p_not_define = "not-defined-property";
  const string num_files_at_l0 =
      rocksdb::DB::Properties::kNumFilesAtLevelPrefix + "0";
  vector<string> property_names{{ApplicationDB::Properties::kNumLevels,
                                 ApplicationDB::Properties::kHighestEmptyLevel,
                                 app_p_not_define, num_files_at_l0,
                                 rocksdb_p_not_define, p_not_define}};
  // Verify: checkDB get properties
  {
    SCOPE_EXIT {
      req.__clear();
      res.__clear();
    };

    const string testdbP = generateDBName();
    addDBWithRole(testdbP, "MASTER");

    req.db_name = testdbP;
    req.property_names = property_names;
    req.__isset.property_names = true;

    EXPECT_NO_THROW(res = client_->future_checkDB(req).get());
    EXPECT_TRUE(res.__isset.properties);
    EXPECT_EQ(res.properties[ApplicationDB::Properties::kNumLevels], "7");
    EXPECT_EQ(res.properties[ApplicationDB::Properties::kHighestEmptyLevel],
              "6");
    EXPECT_EQ(res.properties.find(app_p_not_define), res.properties.end());
    EXPECT_EQ(res.properties[num_files_at_l0], "0");
    EXPECT_EQ(res.properties.find(rocksdb_p_not_define), res.properties.end());
    EXPECT_EQ(res.properties.find(p_not_define), res.properties.end());

    writeToDB(testdbP, "1", "1");
    flushDB(testdbP);
    EXPECT_NO_THROW(res = client_->future_checkDB(req).get());
    EXPECT_EQ(res.seq_num, 1);
    EXPECT_EQ(res.properties[num_files_at_l0], "1");
  }
}

TEST(AdminHandlerTest, MetaData) {
  FLAGS_rocksdb_dir = "/tmp/admin_handler_test/";
  clearAndCreateDir(FLAGS_rocksdb_dir);

  auto db_manager = std::make_unique<admin::ApplicationDBManager>();
  admin::AdminHandler handler(std::move(db_manager),
                              admin::RocksDBOptionsGeneratorType());

  const std::string db_name = "test_db";
  const std::string s3_bucket = "test_bucket";
  const std::string s3_path = "test_path";
  const int64_t timestamp_ms = 1560211388899;
  auto meta = handler.getMetaData(db_name);
  EXPECT_EQ(meta.db_name, db_name);
  EXPECT_FALSE(meta.__isset.s3_bucket);
  EXPECT_FALSE(meta.__isset.s3_path);
  EXPECT_FALSE(meta.__isset.last_kafka_msg_timestamp_ms);

  EXPECT_TRUE(handler.writeMetaData(db_name, s3_bucket, s3_path, timestamp_ms));

  meta = handler.getMetaData(db_name);
  EXPECT_EQ(meta.db_name, db_name);
  EXPECT_TRUE(meta.__isset.s3_bucket);
  EXPECT_EQ(meta.s3_bucket, s3_bucket);
  EXPECT_TRUE(meta.__isset.s3_path);
  EXPECT_EQ(meta.s3_path, s3_path);
  EXPECT_TRUE(meta.__isset.last_kafka_msg_timestamp_ms);
  EXPECT_EQ(meta.last_kafka_msg_timestamp_ms, timestamp_ms);

  EXPECT_TRUE(handler.clearMetaData(db_name));

  // Test last_kafka_msg_timestamp_ms's default value
  EXPECT_TRUE(handler.writeMetaData(db_name, s3_bucket, s3_path));
  meta = handler.getMetaData(db_name);
  EXPECT_EQ(meta.db_name, db_name);
  EXPECT_TRUE(meta.__isset.s3_bucket);
  EXPECT_EQ(meta.s3_bucket, s3_bucket);
  EXPECT_TRUE(meta.__isset.s3_path);
  EXPECT_EQ(meta.s3_path, s3_path);
  EXPECT_TRUE(meta.__isset.last_kafka_msg_timestamp_ms);
  EXPECT_EQ(meta.last_kafka_msg_timestamp_ms, -1);

  EXPECT_TRUE(handler.clearMetaData(db_name));

  meta = handler.getMetaData(db_name);
  EXPECT_EQ(meta.db_name, db_name);
  EXPECT_FALSE(meta.__isset.s3_bucket);
  EXPECT_FALSE(meta.__isset.s3_path);
  EXPECT_FALSE(meta.__isset.last_kafka_msg_timestamp_ms);

  // Test DBMetaData inheritance from Leader to Follower
  // Verify write empty meta is ok && get db_name("") is also ok
  DBMetaData db_meta;
  meta = handler.getMetaData(db_meta.db_name);
  EXPECT_TRUE(meta.db_name.empty());
  EXPECT_FALSE(meta.__isset.s3_bucket);
  EXPECT_FALSE(meta.__isset.s3_path);

  // verify meta is written, meta from get always has s3_path/s3_bucket isset
  EXPECT_TRUE(handler.writeMetaData(db_meta.db_name, db_meta.s3_bucket,
                                    db_meta.s3_path));
  meta = handler.getMetaData(db_meta.db_name);
  EXPECT_TRUE(meta.db_name.empty());
  EXPECT_TRUE(meta.__isset.s3_bucket);
  EXPECT_TRUE(meta.s3_bucket.empty());
  EXPECT_TRUE(meta.__isset.s3_path);
  EXPECT_TRUE(meta.s3_path.empty());

  EXPECT_TRUE(handler.clearMetaData(db_meta.db_name));

  std::string meta_from_backup = "";
  EXPECT_FALSE(DecodeThriftStruct(meta_from_backup, &meta));
  EXPECT_TRUE(meta.db_name.empty());

  // failed decode did not impact meta's old values
  meta.set_db_name(db_name);
  EXPECT_FALSE(DecodeThriftStruct(meta_from_backup, &meta));
  EXPECT_EQ(meta.db_name, db_name);
}

}  // namespace admin

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
