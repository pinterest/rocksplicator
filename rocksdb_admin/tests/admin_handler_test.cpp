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

#include "gtest/gtest.h"
#define private public
#include "rocksdb_admin/admin_handler.h"
#undef private
#include "rocksdb_admin/gen-cpp2/Admin.h"

using admin::AdminAsyncClient;
using admin::AdminException;
using admin::AdminHandler;
using admin::ApplicationDBManager;
using admin::CheckDBRequest;
using admin::CheckDBResponse;
using apache::thrift::async::TAsyncSocket;
using apache::thrift::HeaderClientChannel;
using apache::thrift::ThriftServer;
using common::ThriftClientPool;
using std::chrono::seconds;
using std::make_shared;
using std::make_tuple;
using std::make_unique;
using std::string;
using std::this_thread::sleep_for;
using std::thread;
using std::tuple;

DECLARE_string(rocksdb_dir);

std::unique_ptr<rocksdb::DB> GetTestDB(const std::string& dir) {
  EXPECT_EQ(system(("rm -rf " + dir).c_str()), 0);

  rocksdb::DB* db;
  rocksdb::Options options;
  options.create_if_missing = true;
  options.WAL_ttl_seconds = 123;
  EXPECT_TRUE(rocksdb::DB::Open(options, dir, &db).ok());

  return std::unique_ptr<rocksdb::DB>(db);
}

tuple<shared_ptr<AdminHandler>,
      shared_ptr<ThriftServer>,
      shared_ptr<thread>>
makeServer(uint16_t port) {
  auto db_manager = std::make_unique<ApplicationDBManager>();
  auto db = GetTestDB("/tmp/handler_test");
  EXPECT_TRUE(db_manager->addDB("imp00001", std::move(db),
                                replicator::DBRole::MASTER, nullptr));

  db = GetTestDB("/tmp/handler_test_with_data");
  EXPECT_TRUE(db_manager->addDB("imp00002", std::move(db),
                                replicator::DBRole::MASTER, nullptr));

  rocksdb::WriteBatch batch;
  EXPECT_TRUE(batch.Delete("a").ok());
  auto app_db = db_manager->getDB("imp00002", nullptr);
  EXPECT_TRUE(app_db->Write(rocksdb::WriteOptions(), &batch).ok());

  auto handler = make_shared<AdminHandler>(
    std::move(db_manager), admin::RocksDBOptionsGeneratorType());

  auto server = make_shared<ThriftServer>();
  server->setPort(port);
  server->setInterface(handler);
  auto t = make_shared<thread>([server, port] {
      LOG(INFO) << "Start routing server on port " << port;
      server->serve();
      LOG(INFO) << "Exit routing server on port " << port;
    });
  return make_tuple(handler, server, move(t));
}


TEST(AdminHandlerTest, MetaData) {
  EXPECT_EQ(std::system("rm -rf /tmp/meta_db"), 0);

  auto db_manager = std::make_unique<admin::ApplicationDBManager>();
  admin::AdminHandler handler(std::move(db_manager),
                              admin::RocksDBOptionsGeneratorType());

  const std::string db_name = "test_db";
  const std::string s3_bucket = "test_bucket";
  const std::string s3_path = "test_path";
  auto meta = handler.getMetaData(db_name);
  EXPECT_EQ(meta.db_name, db_name);
  EXPECT_FALSE(meta.__isset.s3_bucket);
  EXPECT_FALSE(meta.__isset.s3_path);

  EXPECT_TRUE(handler.writeMetaData(db_name, s3_bucket, s3_path));

  meta = handler.getMetaData(db_name);
  EXPECT_EQ(meta.db_name, db_name);
  EXPECT_TRUE(meta.__isset.s3_bucket);
  EXPECT_EQ(meta.s3_bucket, s3_bucket);
  EXPECT_TRUE(meta.__isset.s3_path);
  EXPECT_EQ(meta.s3_path, s3_path);

  EXPECT_TRUE(handler.clearMetaData(db_name));

  meta = handler.getMetaData(db_name);
  EXPECT_EQ(meta.db_name, db_name);
  EXPECT_FALSE(meta.__isset.s3_bucket);
  EXPECT_FALSE(meta.__isset.s3_path);
}

TEST(AdminHandlerTest, CheckDB) {
  EXPECT_EQ(std::system("rm -rf /tmp/meta_db"), 0);

  shared_ptr<AdminHandler> handler;
  shared_ptr<ThriftServer> server;
  shared_ptr<thread> thread;
  tie(handler, server, thread) = makeServer(8090);
  sleep_for(seconds(1));


  ThriftClientPool<AdminAsyncClient> pool(1);
  auto client = pool.getClient("127.0.0.1", 8090);

  CheckDBRequest req;
  CheckDBResponse res;
  req.db_name = "unknown_db";

  EXPECT_THROW(res = client->future_checkDB(req).get(), AdminException);

  req.db_name = "imp00001";
  EXPECT_NO_THROW(res = client->future_checkDB(req).get());
  EXPECT_EQ(res.seq_num, 0);
  EXPECT_EQ(res.wal_ttl_seconds, 123);
  EXPECT_EQ(res.last_update_timestamp_ms, 0);

  req.db_name = "imp00002";
  EXPECT_NO_THROW(res = client->future_checkDB(req).get());
  EXPECT_EQ(res.seq_num, 1);
  EXPECT_EQ(res.wal_ttl_seconds, 123);
  // 1521000000 is 03/14/2018 @ 4:00am (UTC)
  EXPECT_GT(res.last_update_timestamp_ms, 1521000000);

  server->stop();
  thread->join();
}

int main(int argc, char** argv) {
  FLAGS_rocksdb_dir = "/tmp/";
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

