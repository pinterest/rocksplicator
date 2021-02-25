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
#include <thread>

#include "boost/filesystem.hpp"
#include "common/s3util.h"
#include "folly/SocketAddress.h"
#include "gtest/gtest.h"
#define private public
#include "rocksdb_admin/admin_handler.h"
#undef private
#include "rocksdb_admin/gen-cpp2/Admin.h"
#include "rocksdb_admin/tests/test_util.h"
#include "rocksdb_admin/utils.h"

using admin::AddDBRequest;
using admin::AddDBResponse;
using admin::AddS3SstFilesToDBRequest;
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
using admin::CloseDBRequest;
using admin::DBMetaData;
using admin::RestoreDBFromS3Request;
using admin::RestoreDBFromS3Response;
using apache::thrift::HeaderClientChannel;
using apache::thrift::RpcOptions;
using apache::thrift::ThriftServer;
using apache::thrift::async::TAsyncSocket;
using common::ThriftClientPool;
using rocksdb::Options;
using std::make_shared;
using std::make_tuple;
using std::make_unique;
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
DECLARE_string(rocksdb_dir);

void clearAndCreateDir(string dir_path) {
  boost::system::error_code create_err;
  boost::system::error_code remove_err;
  fs::remove_all(dir_path, remove_err);
  fs::create_directories(dir_path, create_err);
  EXPECT_FALSE(remove_err || create_err);
}

const string generateDBName() {
  static thread_local unsigned int seed = time(nullptr);
  return "test_db_" + to_string(rand_r(&seed));
}

Options getOptions(const string& segment) {
  Options options;
  options.create_if_missing = true;
  options.WAL_ttl_seconds = 123;
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
    FLAGS_rocksdb_dir = testDir();

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

  bool deleteKeyFromDB(const string db_name, const string key) {
    rocksdb::WriteBatch batch;
    EXPECT_TRUE(batch.Delete(key).ok());
    auto app_db = db_manager_->getDB(db_name, nullptr);
    EXPECT_TRUE(app_db->Write(rocksdb::WriteOptions(), &batch).ok());
  }

  const string& testDir() {
    static const string testDir = "/tmp/admin_handler_test/";
    return testDir;
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

TEST_F(AdminHandlerTestBase, AdminAPIsWithWriteMeta) {
  // verify: meta is written with init val (db_name, empty s3_path/bucket) at
  // addDB
  const string testdb_1 = generateDBName();
  addDBWithRole(testdb_1, "MASTER");
  auto meta = handler_->getMetaData(testdb_1);
  verifyMeta(meta, testdb_1, true, "", "");

  if (FLAGS_enable_integration_test) {
    // verify: restoreDBHelper write meta
    BackupDBToS3Request backup_req;
    backup_req.db_name = testdb_1;
    backup_req.s3_bucket = FLAGS_s3_bucket;
    backup_req.s3_backup_dir = FLAGS_s3_backup_prefix + testdb_1;
    BackupDBToS3Response backup_resp;
    LOG(INFO) << "Backup db: " << testdb_1;
    EXPECT_NO_THROW(backup_resp =
                        client_->future_backupDBToS3(backup_req).get());

    CloseDBRequest close_req;
    close_req.db_name = testdb_1;
    auto close_resp = client_->future_closeDB(close_req).get();

    EXPECT_TRUE(handler_->clearMetaData(testdb_1));
    auto empty_meta = handler_->getMetaData(testdb_1);
    verifyMeta(empty_meta, testdb_1, false, "", "");

    LOG(INFO) << "Restore db: " << testdb_1;
    RestoreDBFromS3Request restore_req;
    restore_req.db_name = testdb_1;
    restore_req.s3_bucket = FLAGS_s3_bucket;
    restore_req.s3_backup_dir = FLAGS_s3_backup_prefix + testdb_1;
    restore_req.upstream_ip = localIP();
    restore_req.upstream_port = 8090;
    RestoreDBFromS3Response restore_resp;
    EXPECT_NO_THROW(restore_resp =
                        client_->future_restoreDBFromS3(restore_req).get());
    // the restoreDBHelper will write from an empty_meta
    auto meta_after_restore = handler_->getMetaData(testdb_1);
    verifyMeta(meta_after_restore, testdb_1, true, "", "");

    // Verify: clearDB with reopen will have a DBMetaData with init val
    EXPECT_TRUE(handler_->clearMetaData(testdb_1));
    ClearDBRequest clear_req;
    clear_req.db_name = testdb_1;
    auto clear_resp = client_->future_clearDB(clear_req).get();
    auto meta_after_clearreopen = handler_->getMetaData(testdb_1);
    verifyMeta(meta_after_clearreopen, testdb_1, true, "", "");

    // Verify: addS3SstFilesToDB with updated meta
    string sst_file1 = "/tmp/file1.sst";
    list<pair<string, string>> sst1_content = {{"1", "1"}, {"2", "2"}};
    createSstWithContent(sst_file1, sst1_content);

    std::shared_ptr<common::S3Util> s3_util =
        common::S3Util::BuildS3Util(50, FLAGS_s3_bucket);
    auto copy_resp = s3_util->putObject(
        FLAGS_s3_backup_prefix + "tempsst/file1.sst", sst_file1);
    ASSERT_TRUE(copy_resp.Error().empty())
        << "Error happened when uploading files to S3: " + copy_resp.Error();

    AddS3SstFilesToDBRequest adds3_req;
    adds3_req.db_name = testdb_1;
    adds3_req.s3_bucket = FLAGS_s3_bucket;
    adds3_req.s3_path = FLAGS_s3_backup_prefix + "tempsst/";
    auto adds3_resp = client_->future_addS3SstFilesToDB(adds3_req).get();
    auto meta_after_adds3 = handler_->getMetaData(testdb_1);
    verifyMeta(meta_after_adds3, testdb_1, true, adds3_req.s3_bucket,
               adds3_req.s3_path);

    // Skip Verify: startMessageIngestion with updated meta
  }
}

TEST_F(AdminHandlerTestBase, CheckDB) {
  addDBWithRole("imp00001", "MASTER");
  auto dbs = db_manager_->getAllDBNames();
  EXPECT_EQ(dbs.size(), 1);
  EXPECT_NE(std::find(dbs.begin(), dbs.end(), "imp00001"), dbs.end());

  CheckDBRequest req;
  CheckDBResponse res;
  req.db_name = "unknown_db";

  EXPECT_THROW(res = client_->future_checkDB(req).get(), AdminException);

  req.db_name = "imp00001";
  EXPECT_NO_THROW(res = client_->future_checkDB(req).get());
  EXPECT_EQ(res.seq_num, 0);
  EXPECT_EQ(res.wal_ttl_seconds, 123);
  EXPECT_EQ(res.last_update_timestamp_ms, 0);

  addDBWithRole("imp00002", "MASTER");
  dbs = db_manager_->getAllDBNames();
  EXPECT_NE(std::find(dbs.begin(), dbs.end(), "imp00002"), dbs.end());

  deleteKeyFromDB("imp00002", "a");
  req.db_name = "imp00002";
  EXPECT_NO_THROW(res = client_->future_checkDB(req).get());
  EXPECT_EQ(res.seq_num, 1);
  EXPECT_EQ(res.wal_ttl_seconds, 123);
  // 1521000000 is 03/14/2018 @ 4:00am (UTC)
  EXPECT_GT(res.last_update_timestamp_ms, 1521000000);
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
