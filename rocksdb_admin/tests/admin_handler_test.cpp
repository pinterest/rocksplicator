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

#include "boost/filesystem.hpp"
#include "folly/SocketAddress.h"
#include "gtest/gtest.h"
#define private public
#include "rocksdb_admin/admin_handler.h"
#undef private
#include "rocksdb_admin/gen-cpp2/Admin.h"
#include "rocksdb_admin/utils.h"

using admin::AddDBRequest;
using admin::AddDBResponse;
using admin::AdminAsyncClient;
using admin::AdminException;
using admin::AdminHandler;
using admin::ApplicationDBManager;
using admin::ChangeDBRoleAndUpstreamRequest;
using admin::ChangeDBRoleAndUpstreamResponse;
using admin::CheckDBRequest;
using admin::CheckDBResponse;
using admin::DBMetaData;
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
using std::tuple;
using std::chrono::seconds;
using std::this_thread::sleep_for;

namespace fs = boost::filesystem;

DECLARE_string(rocksdb_dir);

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

class AdminHandlerTestBase : public testing::Test {
 public:
  AdminHandlerTestBase() {
    FLAGS_rocksdb_dir = testDir();

    LOG(INFO) << "Test start, new remove /tmp/meta_db and " << testDir();
    EXPECT_EQ(std::system("rm -rf /tmp/meta_db"), 0);
    boost::system::error_code create_err;
    boost::system::error_code remove_err;
    fs::remove_all(testDir(), remove_err);
    fs::create_directories(testDir(), create_err);
    EXPECT_FALSE(remove_err || create_err);

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

 public:
  shared_ptr<ThriftClientPool<AdminAsyncClient>> pool_;
  shared_ptr<AdminAsyncClient> client_;
  shared_ptr<AdminHandler> handler_;
  shared_ptr<ThriftServer> server_;
  shared_ptr<thread> thread_;
  // this is very hacky, since db_manager is a unique_ptr inside AdminHandler
  ApplicationDBManager* db_manager_;
};

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
  EXPECT_EQ(std::system("rm -rf /tmp/meta_db"), 0);

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

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
