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

#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "rocksdb_replicator/fast_read_map.h"

using replicator::detail::FastReadMap;
using std::string;
using std::thread;
using std::to_string;
using std::vector;

void stress(int n_write_threads, int n_read_threads, int n_keys_per_thread) {
  FastReadMap<string, int> map;

  vector<thread> threads(n_write_threads + n_read_threads);

  for (int i = 0; i < n_write_threads; ++i) {
    threads[i] = thread([&map, i, n_keys_per_thread] {
        int start_key = i * n_keys_per_thread;

        // write my key range
        for (int key = start_key; key < start_key + n_keys_per_thread; ++key) {
          EXPECT_TRUE(map.add(to_string(key), key));
        }

        // remove 1/2 of my key range
        for (int key = start_key;
             key < start_key + n_keys_per_thread;
             key += 2) {
          EXPECT_TRUE(map.remove(to_string(key)));
        }
      });
  }

  for (int i = 0; i < n_read_threads; ++i) {
    threads[i + n_write_threads] = thread(
      [&map, total_keys = n_write_threads * n_keys_per_thread,
       i, n_read_threads] {
        int start_key = i * (total_keys / n_read_threads);
        for (int key = start_key;
             key < start_key + total_keys / n_read_threads;
             ++key) {
          int value;
          if (map.get(to_string(key), &value)) {
            EXPECT_EQ(value, key);
          }
        }
      });
  }

  for (auto& t : threads) {
    t.join();
  }
}

TEST(FastReadMapTest, Stress) {
  stress(1, 100, 10000);
  stress(10, 10, 1000);
}

TEST(FastReadMapTest, Basics) {
  FastReadMap<string, int> map;

  EXPECT_TRUE(map.add("1", 1));
  EXPECT_TRUE(map.add("2", 2));

  int value;
  EXPECT_TRUE(map.get("1", &value));
  EXPECT_EQ(value, 1);
  EXPECT_FALSE(map.get("3", &value));
  EXPECT_TRUE(map.get("2", &value));
  EXPECT_EQ(value, 2);
  EXPECT_TRUE(map.remove("1"));
  EXPECT_TRUE(map.remove("2"));
  EXPECT_FALSE(map.remove("2"));
  EXPECT_FALSE(map.remove("3"));
  EXPECT_FALSE(map.get("1", &value));
  EXPECT_FALSE(map.get("3", &value));
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
