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
// @author evening (evening@pinterest.com)
//

#include "rocksdb_admin/admin_handler.h"

#include "boost/filesystem.hpp"
#include "gtest/gtest.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/utilities/backupable_db.h"

using boost::filesystem::remove_all;
using rocksdb::BackupableDBOptions;
using rocksdb::BackupEngine;
using rocksdb::DB;
using rocksdb::DestroyDB;
using rocksdb::Env;
using rocksdb::NewHdfsEnv;
using rocksdb::Options;
using rocksdb::ReadOptions;
using rocksdb::SequenceNumber;
using std::string;
using std::to_string;
using std::unique_ptr;

DEFINE_string(hdfs_name_node, "hdfs://hbasebak-infra-namenode-prod1c01-001:8020",
              "The hdfs name node used for backup");

DEFINE_string(backup_dir, "/migration_test", "The dir for backeduped rocksdb instances");

DEFINE_string(original_db_path, "/tmp/test_hdfs_backup_migration_original",
              "The dir for the original rocksdb instance");

DEFINE_string(restored_db_path, "/tmp/test_hdfs_backup_migration_copy",
              "The dir for the restored racksdb instance");

// helper function to open a rocksdb instance
unique_ptr<DB> OpenDB(const string& path, bool error_if_exists = false) {
  Options options;
  options.create_if_missing = true;
  options.error_if_exists = error_if_exists;
  options.WAL_size_limit_MB = 100;
  DB* db;
  auto status = DB::Open(options, path, &db);
  EXPECT_TRUE(status.ok());
  return unique_ptr<DB>(db);
}

unique_ptr<DB> CleanAndOpenDB(const string& path) {
  //EXPECT_NO_THROW(remove_all(path));
  Options options;
  auto status = DestroyDB(path, options);
  EXPECT_TRUE(status.ok());
  return OpenDB(path, true);
}

TEST(HDFSBackupMigrationTest, Basics) {

  // backup can be found in the hdfs_name_node by using the following command
  // hdfs dfs -ls /migration_test/private
 
  string full_path = FLAGS_hdfs_name_node + FLAGS_backup_dir;
  int expected_seq_no = 10;

  // create a new db
  auto original_db = CleanAndOpenDB(FLAGS_original_db_path);
  for (int i = 0; i < expected_seq_no; ++i) {
    EXPECT_EQ(original_db->GetLatestSequenceNumber(), i);
    auto status = original_db->Put(rocksdb::WriteOptions(),
                    "key" + to_string(i), "value" + to_string(i));
    EXPECT_TRUE(status.ok());
  }

  // back up db
  Env* hdfs_env;
  auto status = NewHdfsEnv(&hdfs_env, full_path);
  EXPECT_TRUE(status.ok());
  unique_ptr<Env> hdfs_env_holder(hdfs_env);

  BackupableDBOptions options(full_path);
  options.backup_env = hdfs_env;

  BackupEngine* backup_engine;
  status = BackupEngine::Open(Env::Default(), options, &backup_engine);
  EXPECT_TRUE(status.ok());
  unique_ptr<BackupEngine> engine(backup_engine);

  status = engine->CreateNewBackup(original_db.get());
  EXPECT_TRUE(status.ok());

  // restore db
  status = engine->RestoreDBFromLatestBackup(FLAGS_restored_db_path, FLAGS_restored_db_path);
  EXPECT_TRUE(status.ok());

  // compare the original db with the restored db
  auto restored_db = OpenDB(FLAGS_restored_db_path);
  EXPECT_EQ(restored_db->GetLatestSequenceNumber(), expected_seq_no);
  for (int i = 0; i < expected_seq_no; ++i) {
    string value;
    status = restored_db->Get(ReadOptions(), "key" + to_string(i), &value);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(value, "value" + to_string(i));  
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

