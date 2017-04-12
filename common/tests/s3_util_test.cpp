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
#include "common/s3util.h"
#include "gtest/gtest.h"

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

TEST(S3UtilTest, GetObjectS3Test) {
  static shared_ptr<common::S3Util> myUtil =
      common::S3Util::BuildS3Util(50, "media.pinterest.com", 60000, 60000);

  std::string local_path1 = "/tmp/png1";
  auto resp = myUtil->getObject("appimg-ffd41c0b40b526209375026b74666266.png",
                                local_path1);
  EXPECT_EQ(resp.Error(), "");

  myUtil.reset();
  myUtil = common::S3Util::BuildS3Util(50, "media.pinterest.com", 60000, 60000);
  std::string local_path2 = "/tmp/png2";
  resp = myUtil->getObject("appimg-ffd41c0b40b526209375026b74666266.png",
                           local_path2);
  EXPECT_EQ(resp.Error(), "");

  boost::filesystem::remove_all(local_path1);
  boost::filesystem::remove_all(local_path2);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
