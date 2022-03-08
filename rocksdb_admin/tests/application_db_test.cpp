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
#include <map>
#include <thread>
#include <unordered_map>
#include <vector>

#include "boost/filesystem.hpp"
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#define private public
#include "rocksdb_admin/application_db.h"
#undef private
#include "rocksdb_admin/tests/test_util.h"
#include "rocksdb_replicator/rocksdb_replicator.h"
#include "rocksdb_replicator/thrift/gen-cpp2/Replicator.h"

DEFINE_bool(log_to_stdout, false,
            "Enable output some assisting logs to stdout");

namespace admin {

using boost::filesystem::remove_all;
using rocksdb::DB;
using rocksdb::DestroyDB;
using rocksdb::FlushOptions;
using rocksdb::Options;
using rocksdb::Slice;
using rocksdb::WriteOptions;
using std::cout;
using std::endl;
using std::list;
using std::make_unique;
using std::pair;
using std::shared_ptr;
using std::string;
using std::to_string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using std::chrono::seconds;
using std::this_thread::sleep_for;

class ApplicationDBTestBase : public testing::Test {
 public:
  ApplicationDBTestBase() {
    static thread_local unsigned int seed = time(nullptr);
    db_name_ = "test_db_" + to_string(rand_r(&seed));
    db_path_ = testDir() + "/" + db_name_;
    remove_all(db_path_);
    // default ApplicationDB with DB.allow_ingest_behind=false
    last_options_ = getDefaultOptions();
    Reopen(last_options_);
  }

  ~ApplicationDBTestBase(){};

  int numTableFilesAtLevel(int level) {
    std::string p;
    db_->rocksdb()->GetProperty(
        DB::Properties::kNumFilesAtLevelPrefix + std::to_string(level), &p);
    return atoi(p.c_str());
  }

  int numCompactPending() {
    uint64_t p;
    db_->rocksdb()->GetIntProperty(DB::Properties::kCompactionPending, &p);
    return static_cast<int>(p);
  }

  string levelStats() {
    std::string p;
    db_->rocksdb()->GetProperty(DB::Properties::kLevelStats, &p);
    return p;
  }

  void Reopen(const Options& options) {
    close();
    last_options_ = options;
    DB* db;
    if (FLAGS_log_to_stdout) {
      cout << "Open DB with db_path: " << db_path_ << endl;
    }
    auto status = DB::Open(options, db_path_, &db);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_TRUE(db != nullptr);
    db_ = make_unique<ApplicationDB>(db_name_, shared_ptr<DB>(db),
                                     replicator::ReplicaRole::FOLLOWER, nullptr);
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

TEST_F(ApplicationDBTestBase, GetOptionsAsStrMap) {
  unordered_map<string, string> option_with_expvals{
      {"create_if_missing", "true"},
      {"error_if_exists", "true"},
      {"WAL_size_limit_MB", "100"},
      {"allow_ingest_behind", "false"},
      {"bottommost_compression", "kDisableCompressionOption"},
      {"compaction_style", "kCompactionStyleLevel"},
      {"disable_auto_compactions", "false"}};

  vector<string> option_names{"unknown_option_name"};
  for (const auto& map_entry : option_with_expvals) {
    option_names.push_back(map_entry.first);
  }

  map<string, string> resp_options;
  EXPECT_TRUE(db_->GetOptions(option_names, &resp_options).ok());

  for (const auto& option_name : option_names) {
    if (option_name == "unknown_option_name") {
      EXPECT_EQ(resp_options.find(option_name), resp_options.end());
    } else {
      EXPECT_EQ(resp_options[option_name], option_with_expvals[option_name]);
    }
  }
}

TEST_F(ApplicationDBTestBase, SetDBOptions) {
  // set immutable DBOptions failed
  EXPECT_FALSE(last_options_.allow_ingest_behind);
  // allow_ingest_behind is immutable DBoption, Option not changeable
  auto s = db_->rocksdb()->SetDBOptions({{"allow_ingest_behind", "true"}});
  EXPECT_FALSE(s.ok());
  EXPECT_FALSE(last_options_.allow_ingest_behind);

  s = db_->rocksdb()->SetDBOptions({{"stats_dump_period_sec", "700"}});
  EXPECT_TRUE(s.ok());
}

TEST_F(ApplicationDBTestBase, SetCFImmutableOptionsOrDBOptionsFail) {
  // set immutable CF options -> !ok
  EXPECT_EQ(last_options_.num_levels, 7);
  auto s = db_->rocksdb()->SetOptions({{"num_levels", "5"}});
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(last_options_.num_levels, 7);

  // SetOptions only set Column family options, set dboptions -> !ok
  s = db_->rocksdb()->SetOptions({{"stats_dump_period_sec", "700"}});
  EXPECT_FALSE(s.ok());
}

TEST_F(ApplicationDBTestBase, SetOptionsAndTakeEffect) {
  // Control: default has auto compaction on
  EXPECT_FALSE(last_options_.disable_auto_compactions);

  // Verify: lastest Options returned after SetOptions
  db_->rocksdb()->SetOptions({{"disable_auto_compactions", "true"}});
  // setOptions won't update cached last_options_ at Test
  EXPECT_FALSE(last_options_.disable_auto_compactions);
  EXPECT_TRUE(db_->rocksdb()->GetOptions().disable_auto_compactions);

  // Verify: db's options reload when reopen
  DestroyAndReopen(getDefaultOptions());
  Options options = last_options_;
  options.error_if_exists = false;  // It is set true by default at Test
  options.disable_auto_compactions = true;
  Reopen(options);
  EXPECT_TRUE(last_options_.disable_auto_compactions);
  EXPECT_TRUE(db_->rocksdb()->GetOptions().disable_auto_compactions);
}

TEST_F(ApplicationDBTestBase, SetOptionsDisableEnableAutoCompaction) {
  // Control: verify auto compaction execution when level0_file_num reach config
  Options options = getDefaultOptions();
  options.error_if_exists = false;
  options.level0_file_num_compaction_trigger = 1;
  Reopen(options);
  EXPECT_EQ(numTableFilesAtLevel(1), 0);
  for (int i = 0; i < 5; ++i) {
    db_->rocksdb()->Put(WriteOptions(), to_string(i), to_string(i));
  }
  db_->rocksdb()->Flush(FlushOptions());
  if (FLAGS_log_to_stdout) {
    cout << "Level Stats Right After Flush 1st sst: \n" << levelStats() << endl;
  }
  // After flush 1st sst into L0, an auto compaction will be triggered. Ideally,
  // we should wait the for compaction complete. But, the API is not avail.
  // Thus, here we wait for 1s for compaction finish
  sleep_for(seconds(1));
  if (FLAGS_log_to_stdout) {
    cout << "Level Stats After Wait(1s) for Compaction: \n"
         << levelStats() << endl;
  }
  EXPECT_EQ(numTableFilesAtLevel(0), 0);
  EXPECT_EQ(numTableFilesAtLevel(1), 1);

  // Verify auto compaction paused after setOptions
  db_->rocksdb()->SetOptions({{"disable_auto_compactions", "true"}});
  for (int i = 5; i < 10; ++i) {
    db_->rocksdb()->Put(WriteOptions(), to_string(i), to_string(i));
  }
  db_->rocksdb()->Flush(FlushOptions());
  sleep_for(seconds(1));  // wait for 1s for compaction finish if exist
  if (FLAGS_log_to_stdout) {
    cout << "Level Stats after flush a 2nd sst, with auto compaction disabled: "
            "\n"
         << levelStats() << endl;
  }
  EXPECT_EQ(numTableFilesAtLevel(0), 1);
  EXPECT_EQ(numTableFilesAtLevel(1), 1);
  for (int i = 10; i < 15; ++i) {
    db_->rocksdb()->Put(WriteOptions(), to_string(i), to_string(i));
  }
  db_->rocksdb()->Flush(FlushOptions());
  sleep_for(seconds(1));  // wait for 1s for compaction finish if exist
  if (FLAGS_log_to_stdout) {
    cout << "Level Stats after flush a 3rd sst, with auto compaction disabled: "
            "\n"
         << levelStats() << endl;
  }
  EXPECT_EQ(numTableFilesAtLevel(0), 2);
  EXPECT_EQ(numTableFilesAtLevel(1), 1);
  // with auto compact disabled, numCompactPending is still calculated
  EXPECT_EQ(numCompactPending(), 1);

  // Verify the compaction continue after enable auto compaction again.
  // in db.h, it claims that EnableAutoCompaction will first set options and
  // then schedule a flush/compaction; but, the impl only has the 1st step of
  // setting options.
  // (https://github.com/facebook/rocksdb/blob/cf160b98e1a9bd7b45f115337a923e6b6da7d9c2/db/db_impl/db_impl_compaction_flush.cc#L2142).
  // db_->rocksdb()->EnableAutoCompaction({db_->rocksdb()->DefaultColumnFamily()});
  // from our test, SetOptions deliver expected result.
  db_->rocksdb()->SetOptions({{"disable_auto_compactions", "false"}});
  sleep_for(seconds(1));  // wait for 1s for compaction finish if exist
  if (FLAGS_log_to_stdout) {
    cout << "Level Stats after enable auto compaction again \n"
         << levelStats() << endl;
  }
  EXPECT_EQ(numTableFilesAtLevel(0), 0);
  // the two sst at L0 will merge into 1 at L1
  EXPECT_EQ(numTableFilesAtLevel(1), 2);
  EXPECT_EQ(numCompactPending(), 0);
}

TEST_F(ApplicationDBTestBase, GetProperty) {
  string p_val;
  EXPECT_TRUE(db_->GetProperty(ApplicationDB::Properties::kNumLevels, &p_val));
  EXPECT_EQ(atoi(p_val.c_str()), 7);
  EXPECT_TRUE(
      db_->GetProperty(ApplicationDB::Properties::kHighestEmptyLevel, &p_val));
  EXPECT_EQ(atoi(p_val.c_str()), 6);
  EXPECT_TRUE(db_->DBLmaxEmpty());
}

TEST_F(ApplicationDBTestBase, GetLSMLevelInfo) {
  // Verify: DB level=7 at new create
  EXPECT_EQ(db_->rocksdb()->NumberLevels(), 7);
  // level num: 0, 1, ..., 6
  EXPECT_EQ(db_->getHighestEmptyLevel(), (unsigned)6);

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
  EXPECT_EQ(db_->getHighestEmptyLevel(), (unsigned)6);

  auto options = getDefaultOptions();
  options.allow_ingest_behind = true;
  DestroyAndReopen(options);

  ifo.ingest_behind = true;
  EXPECT_TRUE(db_->rocksdb()->GetOptions().allow_ingest_behind);
  s = db_->rocksdb()->IngestExternalFile({sst_file1}, ifo);
  EXPECT_TRUE(s.ok());
  // level6 is occupied by ingested data
  EXPECT_EQ(db_->getHighestEmptyLevel(), (unsigned)5);
  EXPECT_FALSE(db_->DBLmaxEmpty());

  // compact DB
  rocksdb::CompactRangeOptions compact_options;
  compact_options.change_level = false;
  // if change_level is false (default), compacted data will move to bottommost
  db_->rocksdb()->CompactRange(compact_options, nullptr, nullptr);
  EXPECT_EQ(db_->getHighestEmptyLevel(), (unsigned)5);

  compact_options.change_level = true;
  // if change_level is false (default), compacted data will move to bottommost
  db_->rocksdb()->CompactRange(compact_options, nullptr, nullptr);
  EXPECT_EQ(db_->getHighestEmptyLevel(), (unsigned)6);
  EXPECT_TRUE(db_->DBLmaxEmpty());
}

}  // namespace admin

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}