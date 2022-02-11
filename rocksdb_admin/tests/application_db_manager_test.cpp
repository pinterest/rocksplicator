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


#include <string>

#include "rocksdb_admin/application_db.h"
#include "rocksdb_admin/application_db_manager.h"
#include "rocksdb_replicator/rocksdb_replicator.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "rocksdb/db.h"

std::unique_ptr<rocksdb::DB> GetTestDB(const std::string& dir) {
  EXPECT_EQ(std::system(("rm -rf " + dir).c_str()), 0);
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::DB* db;
  auto s = rocksdb::DB::Open(options, dir, &db);
  if (!s.ok()) {
    LOG(ERROR) << "Failed to create db at " << dir << " with error "
               << s.ToString();
    return nullptr;
  }
  return std::unique_ptr<rocksdb::DB>(db);
}

TEST(ApplicationDBManagerTest, Basics) {
  auto test_db = GetTestDB("/tmp/application_db_manager_test_db");
  admin::ApplicationDBManager db_manager;
  std::string error_message;
  auto ret = db_manager.addDB("test_db", std::move(test_db),
    replicator::DBRole::MASTER, &error_message);
  ASSERT_TRUE(ret);

  ret = db_manager.addDB("test_db", std::move(test_db),
    replicator::DBRole::MASTER, &error_message);
  ASSERT_FALSE(ret);

  {
    auto ret_db = db_manager.getDB("test_db", &error_message);
    EXPECT_NE(ret_db, nullptr);
  }

  const char* test_db_state = 
"ApplicationDBManager:\n\
test_db:\n\
 ReplicatedDB:\n\
  name: test_db\n\
  DBRole: LEADER\n\
  upstream_addr: unknown_addr\n\
  cur_seq_no: 0\n\n";
  EXPECT_EQ(db_manager.Introspect(), std::string(test_db_state));

  auto ret_rocksdb = db_manager.removeDB("test_db", &error_message);
  EXPECT_NE(ret_rocksdb, nullptr);

  ret_rocksdb = db_manager.removeDB("test_db", &error_message);
  EXPECT_EQ(ret_rocksdb, nullptr);

  auto test_db1 = GetTestDB("/tmp/application_db_manager_test_db1");
  ret = db_manager.addDB("test_db1", std::move(test_db1),
    replicator::DBRole::SLAVE, &error_message);
  ASSERT_TRUE(ret);

  const char* test_db1_state = 
"ApplicationDBManager:\n\
test_db1:\n\
 __no_replicated_db__\n";
  EXPECT_EQ(db_manager.Introspect(), std::string(test_db1_state));
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
