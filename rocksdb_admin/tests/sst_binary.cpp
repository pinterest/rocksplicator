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

#include <memory>
#include <string>

#include "boost/filesystem.hpp"
#include "glog/logging.h"
#include "rocksdb/db.h"

static const std::string kDBPath = "/tmp/sst_load_compatibility_test_db";
static const int kNumKeys = 10;

std::unique_ptr<rocksdb::DB> OpenDB(const std::string& path) {
  boost::filesystem::remove_all(path);

  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::DB* db;
  auto status = rocksdb::DB::Open(options, path, &db);
  CHECK(status.ok());

  return std::unique_ptr<rocksdb::DB>(db);
}

int main(int argc, char** argv) {
  if (argc != 2) {
    LOG(ERROR) << "Invalid cmd parameters: " << argc;
    return -1;
  }

  auto db = OpenDB(kDBPath);
  auto status = db->AddFile(argv[1]);
  CHECK(status.ok()) << "AddFile() failed: " << status.ToString();

  std::unique_ptr<rocksdb::Iterator> itor(
    db->NewIterator(rocksdb::ReadOptions()));
  CHECK(itor) << "null iterator returned";

  itor->SeekToFirst();
  for (int i = 0; i < kNumKeys; ++i) {
    CHECK(itor->Valid()) << "Invalid iterator: " << i;
    CHECK(itor->key() == "key" + std::to_string(i)) << itor->key().ToString();
    CHECK(itor->value() == "value" + std::to_string(i)) <<
      itor->value().ToString();
    itor->Next();
  }
}
