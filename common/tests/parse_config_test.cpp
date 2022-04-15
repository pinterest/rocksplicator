#include <atomic>
#include <fstream>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#include "common/thrift_router.h"

static const char* g_config_v1 =
  "{"
  "  \"a\": {"
  "  \"num_leaf_segments\": 1,"
  "  \"127.0.0.1:8090\": [\"00000\"]"
  "   },"
  "  \"b\": {"
  "  \"num_leaf_segments\": 1,"
  "  \"127.0.0.1:8090\": [\"00000\"]"
  "   }"
  "}";


// This test requires ASAN enabled to function correctly
TEST(ParseConfigTest, TestUseAfterFree) {
    auto result = common::parseConfig(g_config_v1, "");
    for(auto& segment: result->segments) {
      for(auto& shard_hosts: segment.second.shard_to_hosts) {
        for(auto& host: shard_hosts) {
          auto* hostPtr = host.first;
          EXPECT_EQ(hostPtr->addr.describe(), "127.0.0.1:8090");
          EXPECT_EQ(hostPtr->groups_prefix_lengths.size(), 2);
        }
      }
    }
}

int main(int argc, char** argv) {
  FLAGS_always_prefer_local_host = false;
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_channel_cleanup_min_interval_seconds = -1;
  return RUN_ALL_TESTS();
}
