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
// @author ghan (ghan@pinterest.com)
//

#include "rocksdb_admin/admin_handler.h"

#include "common/rocksdb_env_s3.h"
#include "common/rocksdb_glogger/rocksdb_glogger.h"
#include "common/s3util.h"
#include "common/timeutil.h"
#include "boost/filesystem.hpp"
#include "gtest/gtest.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/backupable_db.h"
#include "rocksdb/utilities/checkpoint.h"

using rocksdb::BackupableDBOptions;
using rocksdb::BackupEngine;
using rocksdb::DB;
using rocksdb::DestroyDB;
using rocksdb::Env;
using rocksdb::S3Env;
using rocksdb::Options;
using rocksdb::ReadOptions;
using rocksdb::SequenceNumber;
using std::string;
using std::to_string;
using std::unique_ptr;


DEFINE_string(s3_backup_dir, "tmp/backup_test", "The s3 key prefix for backup");

DEFINE_string(s3_bucket, "pinterest-realpin", "The s3 bucket");

DEFINE_string(local_backup_dir, "/tmp/backup_test/backup/", "");

DEFINE_string(local_restore_dir, "/tmp/backup_test/restore/", "");

DEFINE_string(original_db_path, "/tmp/test_s3_backup_original",
              "The dir for the original rocksdb instance");

DEFINE_string(restored_db_path, "/tmp/test_s3_backup_copy",
              "The dir for the restored racksdb instance");
DEFINE_string(s3_backup_dir_checkpoint, "tmp/backup_test/backup/checkpoint", "The s3 key prefix for backup checkpoint");

DEFINE_string(local_backup_dir_checkpoint, "/tmp/backup_test/backup/checkpoint", "");

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
  Options options;
  auto status = DestroyDB(path, options);
  EXPECT_TRUE(status.ok());
  return OpenDB(path, true);
}

TEST(S3BackupRestoreTest, Basics) {
  boost::system::error_code remove_err;
  boost::system::error_code create_err;
  boost::filesystem::remove_all(FLAGS_local_backup_dir, remove_err);
  boost::filesystem::create_directories(FLAGS_local_backup_dir, create_err);
  boost::filesystem::remove_all(FLAGS_local_restore_dir, remove_err);
  boost::filesystem::create_directories(FLAGS_local_restore_dir, create_err);
  EXPECT_FALSE(remove_err || create_err);
  SCOPE_EXIT {
      boost::filesystem::remove_all(FLAGS_local_backup_dir, remove_err);
      boost::filesystem::remove_all(FLAGS_local_restore_dir, remove_err);
  };
  EXPECT_FALSE(remove_err || create_err);

  int expected_seq_no = 10;

  // create a new db
  auto original_db = CleanAndOpenDB(FLAGS_original_db_path);
  for (int i = 0; i < expected_seq_no; ++i) {
    EXPECT_EQ(original_db->GetLatestSequenceNumber(), i);
    auto status = original_db->Put(rocksdb::WriteOptions(),
                    "key" + to_string(i), "value" + to_string(i));
    EXPECT_TRUE(status.ok());
  }

  // back up db. backup can be found in the s3 by using the following command
  // aws s3 ls s3://FLAGS_s3_bucket/FLAGS_s3_backup_dir
  auto local_s3_util = common::S3Util::BuildS3Util(50, FLAGS_s3_bucket);
  Env* backup_s3_env = new rocksdb::S3Env(FLAGS_s3_backup_dir, FLAGS_local_backup_dir, local_s3_util);
  unique_ptr<Env> backup_s3_env_holder(backup_s3_env);

  BackupableDBOptions backup_options(FLAGS_s3_backup_dir);
  backup_options.backup_env = backup_s3_env;
  common::RocksdbGLogger logger;
  backup_options.info_log = &logger;
  backup_options.max_background_operations = 1;

  BackupEngine* backup_engine;
  auto status = BackupEngine::Open(Env::Default(), backup_options, &backup_engine);
  EXPECT_TRUE(status.ok());
  unique_ptr<BackupEngine> backup_engine_holder(backup_engine);

  status = backup_engine->CreateNewBackup(original_db.get());
  EXPECT_TRUE(status.ok());

  // restore db
  Env* restore_s3_env = new rocksdb::S3Env(FLAGS_s3_backup_dir, FLAGS_local_restore_dir, local_s3_util);
  unique_ptr<Env> restore_s3_env_holder(restore_s3_env);
  backup_options.backup_env = restore_s3_env;

  BackupEngine* restore_engine;
  status = BackupEngine::Open(Env::Default(), backup_options, &restore_engine);
  EXPECT_TRUE(status.ok());
  unique_ptr<BackupEngine> restore_engine_holder(restore_engine);
  status = restore_engine->RestoreDBFromLatestBackup(FLAGS_restored_db_path, FLAGS_restored_db_path);
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

TEST(S3BackupRestoreCheckpointTest, Basics) {
  boost::system::error_code remove_err;
  boost::system::error_code create_err;
  boost::filesystem::remove_all(FLAGS_local_backup_dir_checkpoint, remove_err);
  boost::filesystem::create_directories(FLAGS_local_backup_dir_checkpoint, create_err);
  boost::filesystem::remove_all(FLAGS_restored_db_path, remove_err);
  boost::filesystem::create_directories(FLAGS_restored_db_path, create_err);
  EXPECT_FALSE(remove_err || create_err);
  SCOPE_EXIT {
      boost::filesystem::remove_all(FLAGS_local_backup_dir_checkpoint, remove_err);
      boost::filesystem::remove_all(FLAGS_restored_db_path, remove_err);
  };
  EXPECT_FALSE(remove_err || create_err);

  int expected_seq_no = 10;

  // create a new db
  auto original_db = CleanAndOpenDB(FLAGS_original_db_path);
  for (int i = 0; i < expected_seq_no; ++i) {
    EXPECT_EQ(original_db->GetLatestSequenceNumber(), i);
    auto status = original_db->Put(rocksdb::WriteOptions(),
                                   "key" + to_string(i), "value" + to_string(i));
    EXPECT_TRUE(status.ok());
  }

  // back up db. backup can be found in the s3 by using the following command
  // aws s3 ls s3://FLAGS_s3_bucket/FLAGS_s3_backup_dir_checkpoint
  auto local_s3_util = common::S3Util::BuildS3Util(50, FLAGS_s3_bucket);
  rocksdb::Checkpoint* checkpoint;
  auto status = rocksdb::Checkpoint::Create(original_db.get(), &checkpoint);
  EXPECT_TRUE(status.ok());

  auto ts = common::timeutil::GetCurrentTimestamp();
  std::string formatted_local_dir_path = folly::stringPrintf("%s/test%d", FLAGS_local_backup_dir_checkpoint.c_str(), ts);
  std::string formatted_s3_dir_path = folly::stringPrintf("%s/test%d/", FLAGS_s3_backup_dir_checkpoint.c_str(), ts);
  status = checkpoint->CreateCheckpoint(formatted_local_dir_path);
  EXPECT_TRUE(status.ok());
  std::unique_ptr<rocksdb::Checkpoint> checkpoint_holder(checkpoint);

  std::vector<std::string> checkpoint_files;
  status = rocksdb::Env::Default()->GetChildren(formatted_local_dir_path, &checkpoint_files);
  EXPECT_TRUE(status.ok());

  // Upload checkpoint to s3
  for (const auto& file : checkpoint_files) {
    if (file == "." || file == "..") {
      continue;
    }
    auto copy_resp = local_s3_util->putObject(formatted_s3_dir_path + file, formatted_local_dir_path + "/" + file);
    EXPECT_TRUE(copy_resp.Error().empty());
  }

  // restore db
  auto resp = local_s3_util->listAllObjects(formatted_s3_dir_path);
  EXPECT_TRUE(resp.Error().empty());
  std::string formatted_local_path = FLAGS_restored_db_path + "/";

  for (auto& v : resp.Body().objects) {
    // If there is error in one file downloading, then we fail the whole restore process
    auto get_resp = local_s3_util->getObject(v, formatted_local_path + v.substr(formatted_s3_dir_path.size()), false);
    EXPECT_TRUE(get_resp.Error().empty());
  }

  rocksdb::DB* restored_db;
  status = rocksdb::DB::Open(rocksdb::Options(), formatted_local_path, &restored_db);
  EXPECT_TRUE(status.ok());

  // compare the original db with the restored db
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

