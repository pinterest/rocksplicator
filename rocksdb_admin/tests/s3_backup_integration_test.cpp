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

#include <cstdlib>
#include <vector>

#include "boost/filesystem.hpp"
#include "common/rocksdb_env_s3.h"
#include "common/rocksdb_glogger/rocksdb_glogger.h"
#include "common/s3util.h"
#include "common/timeutil.h"
#include "gtest/gtest.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/backupable_db.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb_admin/gen-cpp2/rocksdb_admin_types.h"
#include "rocksdb_admin/utils.h"

using admin::DBMetaData;
using rocksdb::BackupableDBOptions;
using rocksdb::BackupEngine;
using rocksdb::BackupInfo;
using rocksdb::DB;
using rocksdb::DestroyDB;
using rocksdb::Env;
using rocksdb::Options;
using rocksdb::ReadOptions;
using rocksdb::S3Env;
using rocksdb::SequenceNumber;
using std::string;
using std::to_string;
using std::unique_ptr;

DEFINE_string(s3_backup_prefix, "tmp/backup_test", "The s3 key prefix for backup");
DEFINE_string(s3_bucket, "pinterest-jackson", "The s3 bucket");
DEFINE_string(local_backup_dir, "/tmp/backup_test/backup/", "");
DEFINE_string(local_restore_dir, "/tmp/backup_test/restore/", "");

DEFINE_string(s3_backup_prefix_checkpoint,
              "tmp/backup_test/checkpoint",
              "The s3 key prefix for backup checkpoint");
DEFINE_string(local_backup_dir_checkpoint, "/tmp/backup_test/backup/checkpoint/", "");
DEFINE_string(local_restore_dir_checkpoint, "/tmp/backup_test/restore/checkpoint/", "");

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

std::string getDBName() {
  static thread_local unsigned int seed = time(nullptr);
  return "test_db_" + to_string(rand_r(&seed));
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

  std::string test_db_name = getDBName();
  LOG(INFO) << "Local test db name: " << test_db_name;

  // create a new db
  std::string local_db_path = FLAGS_local_backup_dir + test_db_name;
  std::string s3_db_path = FLAGS_s3_backup_prefix + "/" + test_db_name;

  auto original_db = CleanAndOpenDB(local_db_path);
  for (int i = 0; i < expected_seq_no; ++i) {
    EXPECT_EQ(original_db->GetLatestSequenceNumber(), i);
    auto status =
        original_db->Put(rocksdb::WriteOptions(), "key" + to_string(i), "value" + to_string(i));
    EXPECT_TRUE(status.ok());
  }

  vector<bool> backup_with_metas = {true, false};
  vector<bool> restore_by_ids = {true, false};
  for (const auto& backup_with_meta : backup_with_metas) {
    for (const auto& restore_by_id : restore_by_ids) {
      LOG(INFO) << "Backup/Restore Test with backup_with_meta=" + to_string(backup_with_meta) +
                       " restore_by_id=" + to_string(restore_by_id);

      // back up db. backup can be found in the s3 by using the following command
      // aws s3 ls s3://FLAGS_s3_bucket/FLAGS_s3_backup_prefix/test_db_name
      auto local_s3_util = common::S3Util::BuildS3Util(50, FLAGS_s3_bucket);
      Env* backup_s3_env = new rocksdb::S3Env(s3_db_path, local_db_path, local_s3_util);
      unique_ptr<Env> backup_s3_env_holder(backup_s3_env);

      BackupableDBOptions backup_options(s3_db_path);
      backup_options.backup_env = backup_s3_env;
      common::RocksdbGLogger logger;
      backup_options.info_log = &logger;
      backup_options.max_background_operations = 1;

      BackupEngine* backup_engine;
      auto status = BackupEngine::Open(Env::Default(), backup_options, &backup_engine);
      EXPECT_TRUE(status.ok());
      unique_ptr<BackupEngine> backup_engine_holder(backup_engine);

      // call rocksdb API based on backup_with_meta or not
      if (backup_with_meta) {
        std::string db_meta;
        DBMetaData meta;
        meta.db_name = test_db_name;
        if (!EncodeThriftStruct(meta, &db_meta)) {
          LOG(ERROR) << "Failed to encode DBMetadata from meta: " << db_meta;
          EXPECT_TRUE(false);
        } else {
          LOG(INFO) << "Succesfully encode DBMetadata to: " << db_meta;
        }
        status = backup_engine->CreateNewBackupWithMetadata(original_db.get(), db_meta);
      } else {
        status = backup_engine->CreateNewBackup(original_db.get());
      }
      EXPECT_TRUE(status.ok());

      // restore db
      std::string local_restored_db_path = FLAGS_local_restore_dir + test_db_name;
      Env* restore_s3_env = new rocksdb::S3Env(s3_db_path, local_restored_db_path, local_s3_util);
      unique_ptr<Env> restore_s3_env_holder(restore_s3_env);
      backup_options.backup_env = restore_s3_env;

      BackupEngine* restore_engine;
      status = BackupEngine::Open(Env::Default(), backup_options, &restore_engine);
      EXPECT_TRUE(status.ok());
      unique_ptr<BackupEngine> restore_engine_holder(restore_engine);

      // call rocksdb API based on restore by backup_id or not
      if (restore_by_id) {
        std::vector<BackupInfo> backup_infos;
        backup_engine->GetBackupInfo(&backup_infos);
        if (backup_infos.size() < 1) {
          LOG(ERROR) << "Failed to getBackupInfo with backupEngine";
        }
        std::sort(backup_infos.begin(), backup_infos.end(), [](BackupInfo& a, BackupInfo& b) {
          return a.backup_id < b.backup_id;
        });
        std::string db_meta = backup_infos.back().app_metadata;
        DBMetaData meta;

        // DBMetaData has required field db_name. Thus, it can't decode from empty meta data.
        bool decode_meta_status = DecodeThriftStruct(db_meta, &meta);

        // verify metadata retrieval match backup's metadata
        if (backup_with_meta) {
          LOG(INFO) << "Successfully decode DBMetaData from: " << db_meta;
          EXPECT_TRUE(decode_meta_status);
          EXPECT_EQ(meta.db_name, test_db_name);
        } else {
          LOG(ERROR) << "Failed to decode DBMetaData from backup_info.app_metadata: " << db_meta;
          EXPECT_FALSE(decode_meta_status);
          EXPECT_EQ(meta.db_name, "");
        }
        EXPECT_FALSE(meta.__isset.s3_bucket);
        EXPECT_FALSE(meta.__isset.s3_path);

        uint32_t latest_backup_id = backup_infos.back().backup_id;

        LOG(INFO) << "Ready to restore from backupId " << to_string(latest_backup_id);
        status = backup_engine->RestoreDBFromBackup(
            latest_backup_id, local_restored_db_path, local_restored_db_path);
      } else {
        status = restore_engine->RestoreDBFromLatestBackup(local_restored_db_path,
                                                           local_restored_db_path);
      }
      EXPECT_TRUE(status.ok());

      {
        // compare the original db with the restored db
        auto restored_db = OpenDB(local_restored_db_path);
        EXPECT_EQ(restored_db->GetLatestSequenceNumber(), expected_seq_no);
        for (int i = 0; i < expected_seq_no; ++i) {
          string value;
          status = restored_db->Get(ReadOptions(), "key" + to_string(i), &value);
          EXPECT_TRUE(status.ok());
          EXPECT_EQ(value, "value" + to_string(i));
        }
      }

      status = DestroyDB(local_restored_db_path, Options());
      EXPECT_TRUE(status.ok());
    }
  }
}

// TEST(S3BackupRestoreCheckpointTest, Basics) {
//   boost::system::error_code remove_err;
//   boost::system::error_code create_err;
//   boost::filesystem::remove_all(FLAGS_local_backup_dir, remove_err);
//   boost::filesystem::create_directories(FLAGS_local_backup_dir, create_err);
//   boost::filesystem::remove_all(FLAGS_local_backup_dir_checkpoint, remove_err);
//   boost::filesystem::create_directories(FLAGS_local_backup_dir_checkpoint, create_err);
//   boost::filesystem::remove_all(FLAGS_local_restore_dir_checkpoint, remove_err);
//   boost::filesystem::create_directories(FLAGS_local_restore_dir_checkpoint, create_err);
//   EXPECT_FALSE(remove_err || create_err);
//   SCOPE_EXIT {
//     boost::filesystem::remove_all(FLAGS_local_backup_dir, remove_err);
//     boost::filesystem::remove_all(FLAGS_local_backup_dir_checkpoint, remove_err);
//     boost::filesystem::remove_all(FLAGS_local_restore_dir_checkpoint, remove_err);
//   };
//   EXPECT_FALSE(remove_err || create_err);

//   int expected_seq_no = 10;

//   std::string test_db_name = getDBName();

//   // create a new db
//   std::string local_db_path = FLAGS_local_backup_dir + test_db_name;
//   std::string s3_db_path = FLAGS_s3_backup_prefix_checkpoint + "/" + test_db_name;

//   // create a new db
//   auto original_db = CleanAndOpenDB(local_db_path);
//   for (int i = 0; i < expected_seq_no; ++i) {
//     EXPECT_EQ(original_db->GetLatestSequenceNumber(), i);
//     auto status =
//         original_db->Put(rocksdb::WriteOptions(), "key" + to_string(i), "value" + to_string(i));
//     EXPECT_TRUE(status.ok());
//   }

//   // back up db. backup can be found in the s3 by using the following command
//   // aws s3 ls s3://FLAGS_s3_bucket/FLAGS_s3_backup_dir_checkpoint/test_db_name
//   auto local_s3_util = common::S3Util::BuildS3Util(50, FLAGS_s3_bucket);
//   rocksdb::Checkpoint* checkpoint;
//   auto status = rocksdb::Checkpoint::Create(original_db.get(), &checkpoint);
//   EXPECT_TRUE(status.ok());

//   auto ts = common::timeutil::GetCurrentTimestamp();
//   std::string local_db_checkpoint_path =
//       folly::stringPrintf("%s%s/%d", FLAGS_local_backup_dir.c_str(), test_db_name.c_str(), ts);
//   std::string formatted_s3_dir_path = folly::stringPrintf(
//       "%s/%s/%d/", FLAGS_s3_backup_prefix_checkpoint.c_str(), test_db_name.c_str(), ts);

//   LOG(INFO) << "Create checkpoint for localDB: " << local_db_path
//             << " to path: " << local_db_checkpoint_path;
//   status = checkpoint->CreateCheckpoint(local_db_checkpoint_path);
//   EXPECT_TRUE(status.ok());
//   std::unique_ptr<rocksdb::Checkpoint> checkpoint_holder(checkpoint);

//   std::vector<std::string> checkpoint_files;
//   status = rocksdb::Env::Default()->GetChildren(local_db_checkpoint_path, &checkpoint_files);
//   EXPECT_TRUE(status.ok());

//   // Upload checkpoint to s3
//   for (const auto& file : checkpoint_files) {
//     if (file == "." || file == "..") {
//       continue;
//     }
//     std::string local_file_path = local_db_checkpoint_path + "/" + file;
//     std::string s3_file_path = formatted_s3_dir_path + file;
//     LOG(INFO) << "PutObject from localPath: " << local_file_path << " to path: " << s3_file_path;
//     auto copy_resp = local_s3_util->putObject(s3_file_path, local_file_path);
//     EXPECT_TRUE(copy_resp.Error().empty());
//   }

//   // restore db
//   auto resp = local_s3_util->listAllObjects(formatted_s3_dir_path);
//   EXPECT_TRUE(resp.Error().empty());
//   std::string formatted_local_path = local_db_path + "/";

//   for (auto& v : resp.Body().objects) {
//     // If there is error in one file downloading, then we fail the whole restore process
//     auto get_resp = local_s3_util->getObject(
//         v, formatted_local_path + v.substr(formatted_s3_dir_path.size()), false);
//     EXPECT_TRUE(get_resp.Error().empty());
//   }

//   rocksdb::DB* restored_db;
//   status = rocksdb::DB::Open(rocksdb::Options(), formatted_local_path, &restored_db);
//   EXPECT_TRUE(status.ok());

//   // compare the original db with the restored db
//   EXPECT_EQ(restored_db->GetLatestSequenceNumber(), expected_seq_no);
//   for (int i = 0; i < expected_seq_no; ++i) {
//     string value;
//     status = restored_db->Get(ReadOptions(), "key" + to_string(i), &value);
//     EXPECT_TRUE(status.ok());
//     EXPECT_EQ(value, "value" + to_string(i));
//   }
// }

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
