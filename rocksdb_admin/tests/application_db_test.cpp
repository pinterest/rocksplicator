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
using std::list;
using std::make_unique;
using std::pair;
using std::string;
using std::to_string;
using std::unique_ptr;
using std::vector;

class ApplicationDBTestBase : public testing::Test {
public:
  ApplicationDBTestBase() {
    static thread_local unsigned int seed = time(nullptr);
    static string db_name = "test_db_" + to_string(rand_r(&seed));
    // default ApplicationDB with DB.allow_ingest_behind=false
    db_ = CleanAndOpenApplicationDB(testDir() + "/" + db_name, db_name, false);
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

  void recreateDBWithAllowIngestBehind() {
    string name = db_name();
    string path = db_path();
    db_.reset(nullptr);

    Options options = buildDBOptions(false);
    auto status = DestroyDB(path, options);
    EXPECT_TRUE(status.ok());

    db_ = CleanAndOpenApplicationDB(path, name, true);
    EXPECT_NE(db_, nullptr);
  }

  const string testDir() { return "/tmp"; }
  const string db_name() { return db_->db_name(); }
  const string db_path() { return testDir() + "/" + db_->db_name(); }

private:
  Options buildDBOptions(bool allow_ingest_behind) {
    static Options options;
    options.create_if_missing = true;
    options.error_if_exists = true;
    options.WAL_size_limit_MB = 100;
    options.allow_ingest_behind = allow_ingest_behind;
    return options;
  }

  unique_ptr<DB> OpenDB(const string& path, bool allow_ingest_behind) {
    Options options = buildDBOptions(allow_ingest_behind);
    DB* db;
    auto status = DB::Open(options, path, &db);
    EXPECT_TRUE(status.ok());
    return unique_ptr<DB>(db);
  }

  unique_ptr<ApplicationDB> CleanAndOpenApplicationDB(const string& path,
                                                      const string& db_name,
                                                      bool allow_ingest_behind) {
    EXPECT_NO_THROW(remove_all(path));
    auto db = OpenDB(path, allow_ingest_behind);
    EXPECT_TRUE(db != nullptr);
    return make_unique<ApplicationDB>(db_name, std::move(db), replicator::DBRole::SLAVE, nullptr);
  }

public:
  unique_ptr<ApplicationDB> db_;
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
  // EXPECT_EQ(db_->rocksdb()->GetLatestSequenceNumber(), (uint64_t)0);
  EXPECT_FALSE(db_->rocksdb()->GetOptions().allow_ingest_behind);
  auto s = db_->rocksdb()->IngestExternalFile({sst_file1}, ifo);
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(db_->getHighestEmptyLevel(), 6);

  recreateDBWithAllowIngestBehind();

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