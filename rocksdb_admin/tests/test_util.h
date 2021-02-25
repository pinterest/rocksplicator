

#include <stdlib.h>
#include <list>
#include <string>

#include "boost/filesystem.hpp"
#include "gtest/gtest.h"
#include "rocksdb/db.h"
#include "rocksdb/sst_file_writer.h"

using boost::filesystem::remove_all;
using rocksdb::EnvOptions;
using rocksdb::Logger;
using rocksdb::Options;
using rocksdb::SstFileWriter;
using std::list;
using std::pair;
using std::string;

void createSstWithContent(const string& sst_filename,
                          list<pair<string, string>>& key_vals) {
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