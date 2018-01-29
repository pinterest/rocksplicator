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

#include "gtest/gtest.h"
#define private public
#include "rocksdb_admin/admin_handler.h"
#undef private

DECLARE_string(rocksdb_dir);

TEST(AdminHandlerTest, MetaData) {
  EXPECT_EQ(std::system("rm -rf /tmp/meta_db"), 0);

  auto db_manager = std::make_unique<admin::ApplicationDBManager>();
  admin::AdminHandler handler(std::move(db_manager),
                              admin::RocksDBOptionsGeneratorType());

  const std::string db_name = "test_db";
  const std::string s3_bucket = "test_bucket";
  const std::string s3_path = "test_path";
  auto meta = handler.getMetaData(db_name);
  EXPECT_EQ(meta.db_name, db_name);
  EXPECT_FALSE(meta.__isset.s3_bucket);
  EXPECT_FALSE(meta.__isset.s3_path);

  EXPECT_TRUE(handler.writeMetaData(db_name, s3_bucket, s3_path));

  meta = handler.getMetaData(db_name);
  EXPECT_EQ(meta.db_name, db_name);
  EXPECT_TRUE(meta.__isset.s3_bucket);
  EXPECT_EQ(meta.s3_bucket, s3_bucket);
  EXPECT_TRUE(meta.__isset.s3_path);
  EXPECT_EQ(meta.s3_path, s3_path);

  EXPECT_TRUE(handler.clearMetaData(db_name));

  meta = handler.getMetaData(db_name);
  EXPECT_EQ(meta.db_name, db_name);
  EXPECT_FALSE(meta.__isset.s3_bucket);
  EXPECT_FALSE(meta.__isset.s3_path);
}

int main(int argc, char** argv) {
  FLAGS_rocksdb_dir = "/tmp/";
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

