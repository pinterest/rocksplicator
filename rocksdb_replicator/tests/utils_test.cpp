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

#include "gtest/gtest.h"
#include "rocksdb_replicator/utils.h"

TEST(ReplicatorUtilsTest, ReplicaRoleString) {
  std::unordered_map<replicator::ReplicaRole, std::string> testscases = {
      {replicator::ReplicaRole::LEADER, "LEADER"},
      {replicator::ReplicaRole::FOLLOWER, "FOLLOWER"},
      {replicator::ReplicaRole::OBSERVER, "OBSERVER"},
      {replicator::ReplicaRole::NOOP, "NOOP"},
  };
  for (auto p : testscases) {
      EXPECT_EQ(p.second, replicator::ReplicaRoleString(p.first));
  }
}

TEST(ReplicatorUtilsTest, ReturnCodeString) {
  std::unordered_map<replicator::ReturnCode, std::string> testscases = {
      {replicator::ReturnCode::OK, "OK"},
      {replicator::ReturnCode::DB_NOT_FOUND, "DB_NOT_FOUND"},
      {replicator::ReturnCode::DB_PRE_EXIST, "DB_PRE_EXIST"},
      {replicator::ReturnCode::WRITE_TO_SLAVE, "WRITE_TO_SLAVE"},
      {replicator::ReturnCode::WRITE_ERROR, "WRITE_ERROR"},
      {replicator::ReturnCode::WAIT_SLAVE_TIMEOUT, "WAIT_SLAVE_TIMEOUT"},
  };
  for (auto p : testscases) {
      EXPECT_EQ(p.second, replicator::ReturnCodeString(p.first));
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
