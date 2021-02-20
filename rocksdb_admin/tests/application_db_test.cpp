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
// @kangnan li (kangnanli@pinterest.com)
//

#include <stdlib.h>
#include <ctime>
#include <iostream>
#include <thread>
#include <vector>

#include "boost/filesystem.hpp"
#include "gtest/gtest.h"
#include "rocksdb/db.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb_admin/application_db.h"
#include "rocksdb_replicator/rocksdb_replicator.h"

namespace admin {

using boost::filesystem::remove_all;
using rocksdb::DB;
using rocksdb::DestroyDB;
using rocksdb::EnvOptions;
using rocksdb::Logger;
using rocksdb::Options;
using rocksdb::Slice;
using rocksdb::SstFileWriter;
using std::cout;
using std::endl;
using std::list;
using std::make_unique;
using std::pair;
using std::shared_ptr;
using std::string;
using std::to_string;
using std::unique_ptr;
using std::vector;

class ApplicationDBTestBase : public testing::Test {
public:
  ApplicationDBTestBase() {
    static thread_local unsigned int seed = time(nullptr);
    db_name_ = "test_db_" + to_string(rand_r(&seed));
    db_path_ = testDir() + "/" + db_name_;
    // default ApplicationDB with DB.allow_ingest_behind=false
    last_options_ = getDefaultOptions();
    Reopen(last_options_);
  }

  ~ApplicationDBTestBase(){};

  void createSstWithContent(const string& sst_filename, list<pair<string, string>>& key_vals) {
    EXPECT_NO_THROW(remove_all(sst_filename));

    Options options;
    SstFileWriter sst_file_writer(EnvOptions(), options, options.comparator);
    auto s = sst_file_writer.Open(sst_filename);
    EXPECT_TRUE(s.ok());

    list<pair<string, string>>::iterator it;
    for (it = key_vals.begin(); it != key_vals.end(); ++it) {
      s = sst_file_writer.Put(it->first, it->second);
      EXPECT_TRUE(s.ok());
    }

    s = sst_file_writer.Finish();
    EXPECT_TRUE(s.ok());
  }

  void Reopen(const Options& options) {
    close();
    last_options_ = options;
    DB* db;
    auto status = DB::Open(options, db_path_, &db);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(db != nullptr);
    db_ = make_unique<ApplicationDB>(
        db_name_, shared_ptr<DB>(db), replicator::DBRole::SLAVE, nullptr);
  }

  void DestroyAndReopen(const Options& options) {
    Destroy(last_options_);
    Reopen(options);
  }

  void Destroy(const Options& options) {
    close();
    ASSERT_TRUE(DestroyDB(db_path_, options).ok());
  }

  void close() {
    if (db_ != nullptr) {
      db_.reset(nullptr);
    }
  }

  const string testDir() { return "/tmp"; }

  Options getDefaultOptions() {
    Options options;
    options.create_if_missing = true;
    options.error_if_exists = true;
    options.WAL_size_limit_MB = 100;
    return options;
  }

public:
  unique_ptr<ApplicationDB> db_;
  string db_name_;
  string db_path_;
  Options last_options_;
};

TEST_F(ApplicationDBTestBase, GetLSMLevelInfo) {
  // Verify: DB level=7 at new create
  EXPECT_EQ(db_->rocksdb()->NumberLevels(), 7);
  // level num: 0, 1, ..., 6
  EXPECT_EQ(db_->getHighestEmptyLevel(), 6);

  string sst_file1 = "/tmp/file1.sst";
  list<pair<string, string>> sst1_content = {{"1", "1"}, {"2", "2"}};
  createSstWithContent(sst_file1, sst1_content);

  // ingest sst files, and try to read the current occupied LSM level agin
  rocksdb::IngestExternalFileOptions ifo;
  ifo.move_files = true;
  ifo.allow_global_seqno = true;
  ifo.ingest_behind = true;
  EXPECT_FALSE(db_->rocksdb()->GetOptions().allow_ingest_behind);
  auto s = db_->rocksdb()->IngestExternalFile({sst_file1}, ifo);
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(db_->getHighestEmptyLevel(), 6);

  auto options = getDefaultOptions();
  options.allow_ingest_behind = true;
  DestroyAndReopen(options);

  ifo.ingest_behind = true;
  EXPECT_TRUE(db_->rocksdb()->GetOptions().allow_ingest_behind);
  s = db_->rocksdb()->IngestExternalFile({sst_file1}, ifo);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(db_->getHighestEmptyLevel(), 5);  // level6 is occupied by ingested data

  // compact DB
  rocksdb::CompactRangeOptions compact_options;
  compact_options.change_level = false;
  // if change_level is false (default), compacted data will move to bottommost
  db_->rocksdb()->CompactRange(compact_options, nullptr, nullptr);
  EXPECT_EQ(db_->getHighestEmptyLevel(), 5);

  compact_options.change_level = true;
  // if change_level is false (default), compacted data will move to bottommost
  db_->rocksdb()->CompactRange(compact_options, nullptr, nullptr);
  EXPECT_EQ(db_->getHighestEmptyLevel(), 6);
}

}  // namespace admin

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}