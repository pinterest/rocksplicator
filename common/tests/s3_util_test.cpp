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
// @author shu (shu@pinterest.com)
//

#include <string>
#include <tuple>

#include "boost/filesystem.hpp"
#include "boost/filesystem/fstream.hpp"
#include "boost/iostreams/stream.hpp"
#include "common/s3util.h"
#include "gtest/gtest.h"

namespace fs = boost::filesystem;

using std::string;

TEST(S3UtilTest, ParseS3StringTest) {
  string test_path = "invalid/string";
  tuple<string, string> result = common::S3Util::parseFullS3Path(test_path);
  EXPECT_TRUE(std::get<0>(result).empty());
  EXPECT_TRUE(std::get<1>(result).empty());

  test_path = "s3://bucket/key";
  result = common::S3Util::parseFullS3Path(test_path);
  EXPECT_TRUE(std::get<0>(result) == "bucket");
  EXPECT_TRUE(std::get<1>(result) == "key");

  test_path = "s3n://bucket/key";
  result = common::S3Util::parseFullS3Path(test_path);
  EXPECT_TRUE(std::get<0>(result) == "bucket");
  EXPECT_TRUE(std::get<1>(result) == "key");

  test_path = "s3n://bucket/";
  result = common::S3Util::parseFullS3Path(test_path);
  EXPECT_TRUE(std::get<0>(result) == "bucket");
  EXPECT_TRUE(std::get<1>(result) == "");

  test_path = "s3://";
  result = common::S3Util::parseFullS3Path(test_path);
  EXPECT_TRUE(std::get<0>(result) == "");
  EXPECT_TRUE(std::get<1>(result) == "");
}

TEST(S3UtilTest, DirectIOTest) {
  const string file_path = "/tmp/s3DirectIO";

  {
    // write less than page size
    {
      boost::iostreams::stream<common::DirectIOFileSink> os(file_path);
      os << "hello ";
      os.write("world!", 6);
    }

    fs::ifstream f_in;
    f_in.open(file_path, std::ios::in);
    std::stringstream ss;
    ss << f_in.rdbuf();
    EXPECT_EQ("hello world!", ss.str());
  }

  {
    // write equal to page size
    char buf[4096];
    std::memset(buf, 'f', 4096);
    {
      common::DirectIOWritableFile file(file_path);
      file.write(buf, 4096);
    }
    fs::ifstream f_in;
    f_in.open(file_path, std::ios::in);
    std::stringstream ss;
    ss << f_in.rdbuf();
    EXPECT_EQ(string(buf, 4096), ss.str());
  }

  {
    // write larger than page size
    char buf[4097];
    std::memset(buf, 'g', 4096);
    buf[4096] = 'h';
    {
      common::DirectIOWritableFile file(file_path);
      file.write(buf, 4097);
    }
    fs::ifstream f_in;
    f_in.open(file_path, std::ios::in);
    std::stringstream ss;
    ss << f_in.rdbuf();
    EXPECT_EQ(string(buf, 4097), ss.str());
  }

  {
    // multi page size buffer, write equal to buffer size
    FLAGS_direct_io_buffer_n_pages = 2;
    char buf[4096 * 2];
    std::memset(buf, 'f', 4096 * 2);
    {
      common::DirectIOWritableFile file(file_path);
      file.write(buf, 4096 * 2);
    }
    fs::ifstream f_in;
    f_in.open(file_path, std::ios::in);
    std::stringstream ss;
    ss << f_in.rdbuf();
    EXPECT_EQ(string(buf, 4096 * 2), ss.str());
  }

  {
    // multi page size buffer, write larger than buffer size
    FLAGS_direct_io_buffer_n_pages = 2;
    char buf[4096 * 2 + 1];
    std::memset(buf, 'f', 4096 * 2);
    buf[4096 * 2] = 'k';
    {
      common::DirectIOWritableFile file(file_path);
      file.write(buf, 4096 * 2 + 1);
    }
    fs::ifstream f_in;
    f_in.open(file_path, std::ios::in);
    std::stringstream ss;
    ss << f_in.rdbuf();
    EXPECT_EQ(string(buf, 4096 * 2 + 1), ss.str());
  }
  fs::remove(file_path);
}

TEST(S3UtilTest, CallDestructorOnlyOnce) {
  SDKOptions options;
  Aws::InitAPI(options);

  EXPECT_EQ(0, common::S3Util::getInstanceCounter());
  auto instance1 = common::S3Util::BuildS3Util(0, "", 0, 0);
  EXPECT_EQ(1, common::S3Util::getInstanceCounter());
  auto instance2 = common::S3Util::BuildS3Util(0, "", 0, 0);
  EXPECT_EQ(2, common::S3Util::getInstanceCounter());
  auto instance3 = common::S3Util::BuildS3Util(0, "", 0, 0);
  EXPECT_EQ(3, common::S3Util::getInstanceCounter());
  instance1 = nullptr;
  EXPECT_EQ(2, common::S3Util::getInstanceCounter());
  instance2 = nullptr;
  EXPECT_EQ(1, common::S3Util::getInstanceCounter());
  instance3 = nullptr;
  EXPECT_EQ(0, common::S3Util::getInstanceCounter());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}



