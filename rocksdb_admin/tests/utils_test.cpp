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


#include "rocksdb_admin/utils.h"

#include "gtest/gtest.h"

TEST(SegmentToDbNameTest, Basics) {
  EXPECT_EQ(admin::SegmentToDbName("seg", 1), "seg00001");
  EXPECT_EQ(admin::SegmentToDbName("seg", 12345), "seg12345");
}

TEST(DbNameToSegmentTest, Basics) {
  EXPECT_EQ(admin::DbNameToSegment("seg00001"), "seg");
  EXPECT_EQ(admin::DbNameToSegment("seg12345"), "seg");
  EXPECT_EQ(admin::DbNameToSegment("seg12"), "seg12");
}

TEST(ExtractShardIDTest, Basics) {
  std::string db_name;

  db_name = "test_db00000";
  EXPECT_EQ(admin::ExtractShardId(db_name), 0);

  db_name = "test_db00030";
  EXPECT_EQ(admin::ExtractShardId(db_name), 30);

  db_name = "test_db";
  EXPECT_EQ(admin::ExtractShardId(db_name), -1);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
