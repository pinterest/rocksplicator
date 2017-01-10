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

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "rocksdb_replicator/max_number_box.h"

using replicator::detail::MaxNumberBox;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::system_clock;

TEST(MaxNumberBoxTest, Basics) {
  MaxNumberBox box;
  std::atomic<uint64_t> num_to_post(0);
  std::atomic<bool> stop(false);
  std::thread poster([&num_to_post, &stop, &box] () {
      while (!stop.load()) {
        std::this_thread::sleep_for(milliseconds(1));
        box.post(num_to_post.load());
      }
    });

  EXPECT_TRUE(box.wait(0, 10));
  EXPECT_FALSE(box.wait(1, 10));
  box.post(5);
  EXPECT_TRUE(box.wait(0, 10));
  EXPECT_TRUE(box.wait(5, 10));
  EXPECT_FALSE(box.wait(6, 10));

  num_to_post.store(6);
  EXPECT_TRUE(box.wait(6, 10));
  num_to_post.store(11);
  EXPECT_TRUE(box.wait(10, 10));
  EXPECT_FALSE(box.wait(20, 10));

  num_to_post.store(1);
  EXPECT_TRUE(box.wait(0, 10));
  EXPECT_TRUE(box.wait(1, 10));
  EXPECT_TRUE(box.wait(5, 10));
  EXPECT_TRUE(box.wait(11, 10));
  EXPECT_FALSE(box.wait(12, 10));

  stop.store(true);
  poster.join();
}

TEST(MaxNumberBoxTest, Stress) {
  // reduce the number of threads to make travis happy.
  // we may need to restore the numbers if we need to stress test it.
  const int n_waiters = 8; // 128;
  const int n_posters = 4; // 16;
  const int n_boxes = 2;
  MaxNumberBox boxes[n_boxes];
  std::atomic<uint64_t> num_to_post(0);
  std::atomic<uint64_t> num_to_wait(0);
  std::atomic<bool> stop_waiter(false);
  std::atomic<bool> stop_poster(false);
  std::atomic<uint64_t> wait_ms(1);
  std::vector<std::thread> waiters(n_waiters);
  std::vector<std::thread> posters(n_posters);

  for (int i = 0; i < n_posters; ++i) {
    posters[i] = std::thread([&num_to_post, &stop_poster,
                              boxes = &boxes[0], n_boxes] () {
        int i = 0;
        while (!stop_poster.load()) {
          ++i;
          std::this_thread::sleep_for(milliseconds(1));
          boxes[i % n_boxes].post(num_to_post.load());
        }
      });
  }

  for (int i = 0; i < n_waiters; ++i) {
    waiters[i] = std::thread([&num_to_wait, &stop_waiter,
                              boxes = &boxes[0], n_boxes, &wait_ms] () {
        int i = 0;
        while (!stop_waiter.load()) {
          ++i;
          std::this_thread::sleep_for(milliseconds(1));
          auto wait_ms_local = wait_ms.load();
          auto then_ms = duration_cast<milliseconds>(
            system_clock::now().time_since_epoch());
          auto got = boxes[i % n_boxes].wait(num_to_wait.load(), wait_ms_local);
          auto now_ms = duration_cast<milliseconds>(
            system_clock::now().time_since_epoch());
          EXPECT_TRUE(got || now_ms - then_ms >= milliseconds(wait_ms_local));
        }
      });
  }

  const uint64_t ms_for_each_stage = 1000;
  // bump num_to_post first, then num_to_wait
  auto start_ms =
    duration_cast<milliseconds>(system_clock::now().time_since_epoch());
  while (duration_cast<milliseconds>(system_clock::now().time_since_epoch()) -
         start_ms <= milliseconds(ms_for_each_stage)) {
    num_to_post.fetch_add(1);
    std::this_thread::sleep_for(milliseconds(3));
    num_to_wait.fetch_add(1);
  }

  // bump num_to_wait first, then num_to_post
  start_ms =
    duration_cast<milliseconds>(system_clock::now().time_since_epoch());
  auto m = std::max(num_to_post.load(), num_to_wait.load());
  num_to_post.store(m);
  num_to_wait.store(m);
  while (duration_cast<milliseconds>(system_clock::now().time_since_epoch()) -
         start_ms <= milliseconds(ms_for_each_stage)) {
    num_to_wait.fetch_add(1);
    std::this_thread::sleep_for(milliseconds(3));
    num_to_post.fetch_add(1);
  }

  // bump num_to_post and num_to_wait at the same time
  start_ms =
    duration_cast<milliseconds>(system_clock::now().time_since_epoch());
  m = std::max(num_to_post.load(), num_to_wait.load());
  num_to_post.store(m);
  num_to_wait.store(m);
  while (duration_cast<milliseconds>(system_clock::now().time_since_epoch()) -
         start_ms <= milliseconds(ms_for_each_stage)) {
    num_to_wait.fetch_add(1);
    num_to_post.fetch_add(1);
    std::this_thread::sleep_for(milliseconds(1));
  }

  stop_waiter.store(true);
  for (auto& t : waiters) {
    t.join();
  }

  stop_poster.store(true);
  for (auto& t : posters) {
    t.join();
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
