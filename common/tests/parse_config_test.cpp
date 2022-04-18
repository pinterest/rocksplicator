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


TEST(ParseConfigTest, TestUseAfterFree) {
    /*
    This test was originally introduced to test for use after free in common::parseConfig
    Since use after free produces undefined behavior, sometimes it may work sometimes it
    doesn't for example: 
    - In rocksplicator repo using cmake this test will succeed because newly allocated data will use same memory as freed data for set datastructure.
    - In cosmos this test will fail

    If you enable asan in cosmos this test will fail and show the use after free.
    */
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
