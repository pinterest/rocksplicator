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
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "boost/filesystem.hpp"
#include "gtest/gtest.h"
#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/utilities/backupable_db.h"

using boost::filesystem::remove_all;
using rocksdb::AssociativeMergeOperator;
using rocksdb::BackupableDBOptions;
using rocksdb::BackupEngine;
using rocksdb::BatchResult;
using rocksdb::DB;
using rocksdb::DestroyDB;
using rocksdb::Env;
using rocksdb::EnvOptions;
using rocksdb::Logger;
using rocksdb::Options;
using rocksdb::ReadOptions;
using rocksdb::SequenceNumber;
using rocksdb::Slice;
using rocksdb::SstFileWriter;
using rocksdb::TransactionLogIterator;
using rocksdb::WriteBatch;
using std::function;
using std::random_shuffle;
using std::string;
using std::thread;
using std::to_string;
using std::unique_ptr;
using std::vector;

class SimpleMergeOperator : public AssociativeMergeOperator {
 public:
  bool Merge(const Slice& key,
             const Slice* existing_value,
             const Slice& value,
             std::string* new_value,
             Logger* logger) const override {
    if (existing_value) {
      *new_value = existing_value->ToString();
    }

    *new_value += value.ToString();

    return true;
  }

  const char* Name() const override {
    return "SimpleMergeOperator";
  }
};

unique_ptr<DB> OpenDB(const string& path, bool error_if_exists = false) {
  Options options;
  options.create_if_missing = true;
  options.error_if_exists = error_if_exists;
  options.WAL_size_limit_MB = 100;
  options.merge_operator = std::make_shared<SimpleMergeOperator>();
  DB* db;
  auto status = DB::Open(options, path, &db);
  EXPECT_TRUE(status.ok());

  return unique_ptr<DB>(db);
}

unique_ptr<DB> CleanAndOpenDB(const string& path) {
  EXPECT_NO_THROW(remove_all(path));
  return OpenDB(path, true);
}

TEST(RocksDBAssumptionTest, SequenceNumberAfterRestore) {
  string backup_dir = "/tmp/rocksdb_backup";
  int expected_seq_no = 999;

  // backup a db
  {
    auto db = CleanAndOpenDB("/tmp/test_sequence_after_restore_db");
    for (int i = 0; i < expected_seq_no; ++i) {
      EXPECT_EQ(db->GetLatestSequenceNumber(), i);
      auto s = db->Put(rocksdb::WriteOptions(),
                       "key" + to_string(i), "value" + to_string(i));
      EXPECT_TRUE(s.ok());
    }

    BackupEngine* backup_engine;
    auto s = BackupEngine::Open(Env::Default(), BackupableDBOptions(backup_dir),
                                &backup_engine);
    EXPECT_TRUE(s.ok());
    unique_ptr<BackupEngine> engine(backup_engine);
    s = engine->CreateNewBackup(db.get());
    EXPECT_TRUE(s.ok());
  }

  // restore the db
  {
    string db_dir = "/tmp/test_sequence_after_restore_new_db";
    BackupEngine* backup_engine;
    auto s = BackupEngine::Open(Env::Default(), BackupableDBOptions(backup_dir),
                                &backup_engine);
    EXPECT_TRUE(s.ok());
    unique_ptr<BackupEngine> engine(backup_engine);
    s = engine->RestoreDBFromLatestBackup(db_dir, db_dir);
    EXPECT_TRUE(s.ok());

    auto db = OpenDB(db_dir);
    EXPECT_EQ(db->GetLatestSequenceNumber(), expected_seq_no);
  }
}

TEST(RocksDBAssumptionTest, SequenceNumber) {
  const string db_path = "/tmp/test_sequence_number_db";
  auto db = CleanAndOpenDB(db_path);
  EXPECT_TRUE(db != nullptr);

  // Sequence # always starts with 0
  EXPECT_EQ(db->GetLatestSequenceNumber(), 0);

  const string key1 = "key1";
  const string key2 = "key2";
  const string value1 = "value1";
  const string value2 = "value2";
  string value;

  // Put(), Delete(), Merge() will consume 1 sequence #
  // And Get() won't consume any sequence #
  auto s = db->Put(rocksdb::WriteOptions(), key1, value1);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(db->GetLatestSequenceNumber(), 1);

  s = db->Get(rocksdb::ReadOptions(), key1, &value);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(value, value1);
  EXPECT_EQ(db->GetLatestSequenceNumber(), 1);

  s = db->Delete(rocksdb::WriteOptions(), key1);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(db->GetLatestSequenceNumber(), 2);

  s = db->Get(rocksdb::ReadOptions(), key1, &value);
  EXPECT_TRUE(s.IsNotFound());
  EXPECT_EQ(db->GetLatestSequenceNumber(), 2);

  s = db->Merge(rocksdb::WriteOptions(), key1, value1);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(db->GetLatestSequenceNumber(), 3);

  s = db->Get(rocksdb::ReadOptions(), key1, &value);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(db->GetLatestSequenceNumber(), 3);
  EXPECT_EQ(value, value1);


  // Write() will consume n sequence #, where n is the # operations in the batch
  WriteBatch batch;
  batch.Delete(key1);
  batch.Put(key2, value2);
  batch.Put(key2, value2);
  batch.Merge(key1, value1);
  s = db->Write(rocksdb::WriteOptions(), &batch);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(db->GetLatestSequenceNumber(), 7);


  // close, destroy and reopen DB, sequence # should start from zero again
  db.reset(nullptr);
  Options options;
  s = DestroyDB(db_path, options);
  EXPECT_TRUE(s.ok());
  db = OpenDB(db_path);
  EXPECT_TRUE(db != nullptr);
  EXPECT_EQ(db->GetLatestSequenceNumber(), 0);


  // manually write two SST files
  SstFileWriter sst_file_writer1(EnvOptions(), options, options.comparator);
  SstFileWriter sst_file_writer2(EnvOptions(), options, options.comparator);
  string sst_file1 = "/tmp/file1.sst";
  string sst_file2 = "/tmp/file2.sst";
  s = sst_file_writer1.Open(sst_file1);
  EXPECT_TRUE(s.ok());
  s = sst_file_writer2.Open(sst_file2);
  EXPECT_TRUE(s.ok());

  const int nKeys = 5;
  for (int i = 0; i < nKeys; ++i) {
    s = sst_file_writer1.Add(to_string(i), to_string(i));
    EXPECT_TRUE(s.ok());
    s = sst_file_writer2.Add(to_string(i + nKeys), to_string(i + nKeys));
    EXPECT_TRUE(s.ok());
  }

  s = sst_file_writer1.Finish();
  EXPECT_TRUE(s.ok());
  s = sst_file_writer2.Finish();
  EXPECT_TRUE(s.ok());


  // add two SST files to the empty DB, the sequence # should still be zero
  rocksdb::IngestExternalFileOptions ifo;
  ifo.move_files = true;
  /* allow for overlapping keys */
  ifo.allow_global_seqno = true;
  EXPECT_EQ(db->GetLatestSequenceNumber(), 0);
  s = db->IngestExternalFile({sst_file1}, ifo);
  EXPECT_TRUE(s.ok());
  ifo.move_files = false;
  EXPECT_EQ(db->GetLatestSequenceNumber(), 0);
  s = db->IngestExternalFile({sst_file2}, ifo);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(db->GetLatestSequenceNumber(), 0);


  // verify the data is correct
  for (int i = 0; i < 2 * nKeys; ++i) {
    string value;
    ReadOptions read_options;
    s = db->Get(read_options, to_string(i), &value);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(value, to_string(i));
  }
  EXPECT_EQ(db->GetLatestSequenceNumber(), 0);


  // Online writes after adding files
  s = db->Put(rocksdb::WriteOptions(), to_string(2 * nKeys),
              to_string(2 * nKeys));
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(db->GetLatestSequenceNumber(), 1);

  s = db->Get(rocksdb::ReadOptions(), to_string(2 * nKeys), &value);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(value, to_string(2 * nKeys));
}

void ReplicateDB(const unique_ptr<DB>& master, unique_ptr<DB>* slave,
                 int nOps, bool reopen_db, string slave_path) {
  SequenceNumber seq_no = 0;

  for (int i = 0; i < nOps; ++i) {
    if (reopen_db && i % 100 == 0) {
      slave->reset(nullptr);
      *slave = OpenDB(slave_path);
      EXPECT_TRUE(*slave != nullptr);
    }

    EXPECT_EQ((*slave)->GetLatestSequenceNumber(), seq_no);

    unique_ptr<rocksdb::TransactionLogIterator> iter;
    auto s = master->GetUpdatesSince(seq_no + 1, &iter);
    // polling here. We shouldn't do this in production
    while (!s.ok()) {
      s = master->GetUpdatesSince(seq_no + 1, &iter);
    }

    auto batch_result = iter->GetBatch();
    EXPECT_EQ(seq_no + 1, batch_result.sequence);

    s = (*slave)->Write(rocksdb::WriteOptions(),
                        batch_result.writeBatchPtr.get());
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(batch_result.writeBatchPtr->Count(), 4);
    seq_no += 4;
    EXPECT_EQ((*slave)->GetLatestSequenceNumber(), seq_no);
  }
}

TEST(RocksDBAssumptionTest, GetUpdatesSince) {
  const string master_path = "/tmp/test_get_updates_since_master_db";
  const string slave1_path = "/tmp/test_get_updates_since_slave_db1";
  const string slave2_path = "/tmp/test_get_updates_since_slave_db2";
  auto db_master = CleanAndOpenDB(master_path);
  EXPECT_TRUE(db_master != nullptr);

  vector<string> keys;
  vector<string> values;
  int nKeys = 100;
  for (int i = 0; i < nKeys; ++i) {
    keys.emplace_back(to_string(i));
    values.emplace_back("value" + to_string(i));
  }

  vector<function<void()>> ops;
  int nOps = 5000;
  for (int i = 0; i < nOps; ++i) {
    ops.emplace_back([&db_master, &keys, &values, i, nKeys] () {
        WriteBatch batch;
        batch.Put(keys[i % nKeys], values[i % nKeys]);
        batch.Merge(keys[i % nKeys], values[i % nKeys]);
        batch.Merge(keys[(i + 1) % nKeys], values[(i + 1) % nKeys]);
        batch.Delete(keys[(i + 2) % nKeys]);
        auto s = db_master->Write(rocksdb::WriteOptions(), &batch);
        EXPECT_TRUE(s.ok());
      });
  }

  random_shuffle(ops.begin(), ops.end());

  auto db_slave1 = CleanAndOpenDB(slave1_path);
  auto db_slave2 = CleanAndOpenDB(slave2_path);
  thread t1([&db_master, &db_slave1, &slave1_path, nOps] () {
      ReplicateDB(db_master, &db_slave1, nOps, true, slave1_path);
    });
  thread t2([&db_master, &db_slave2, &slave2_path, nOps] () {
      ReplicateDB(db_master, &db_slave2, nOps, false, slave2_path);
    });

  for (const auto& op : ops) {
    op();
  }

  t1.join();
  t2.join();

  EXPECT_EQ(db_master->GetLatestSequenceNumber(),
            db_slave1->GetLatestSequenceNumber());
  EXPECT_EQ(db_master->GetLatestSequenceNumber(),
            db_slave2->GetLatestSequenceNumber());

  string value_master;
  string value_slave1;
  string value_slave2;
  for (int i = 0; i < nKeys; ++i) {
    auto sm = db_master->Get(rocksdb::ReadOptions(), keys[i], &value_master);
    auto ss1 = db_master->Get(rocksdb::ReadOptions(), keys[i], &value_slave1);
    auto ss2 = db_master->Get(rocksdb::ReadOptions(), keys[i], &value_slave2);

    if (sm.ok()) {
      EXPECT_TRUE(ss1.ok());
      EXPECT_TRUE(ss2.ok());
      EXPECT_EQ(value_master, value_slave1);
      EXPECT_EQ(value_master, value_slave2);
    } else {
      EXPECT_TRUE(sm.IsNotFound());
      EXPECT_TRUE(ss1.IsNotFound());
      EXPECT_TRUE(ss2.IsNotFound());
    }
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

