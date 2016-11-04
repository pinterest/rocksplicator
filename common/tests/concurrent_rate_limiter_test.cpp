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
#include <thread>
#include <vector>

#include "common/concurrent_rate_limiter.h"
#include "gtest/gtest.h"

using common::ConcurrentRateLimiter;

struct TestClock {
  static uint32_t GetCurrentTimeSeconds() {
    return current_time_.load();
  }

  static void SetCurrentTime(const uint32_t t) {
    current_time_.store(t);
  }

  static std::atomic<uint32_t> current_time_;
};

std::atomic<uint32_t> TestClock::current_time_;

TEST(ConcurrentRateLimiterTest, Basics) {
  TestClock::SetCurrentTime(0);
  double rate = 99;
  double n_seconds = 3;
  ConcurrentRateLimiter rl(rate, n_seconds, TestClock::GetCurrentTimeSeconds);

  for (int i = 0; i < rate; ++i) {
    EXPECT_TRUE(rl.GetTokens());
  }
  EXPECT_FALSE(rl.GetTokens());
  EXPECT_FALSE(rl.GetTokens());

  TestClock::SetCurrentTime(1);
  for (int i = 0; i < rate / n_seconds; ++i) {
    EXPECT_TRUE(rl.GetTokens());
  }
  EXPECT_FALSE(rl.GetTokens());
  EXPECT_FALSE(rl.GetTokens());

  TestClock::SetCurrentTime(3);
  for (int i = 0; i < 2 * rate / n_seconds; ++i) {
    EXPECT_TRUE(rl.GetTokens());
  }
  EXPECT_FALSE(rl.GetTokens());
  EXPECT_FALSE(rl.GetTokens());

  TestClock::SetCurrentTime(100);
  for (int i = 0; i < rate; ++i) {
    EXPECT_TRUE(rl.GetTokens());
  }
  EXPECT_FALSE(rl.GetTokens());
  EXPECT_FALSE(rl.GetTokens());

  TestClock::SetCurrentTime(100);
  EXPECT_FALSE(rl.GetTokens());
  EXPECT_FALSE(rl.GetTokens());

  TestClock::SetCurrentTime(99);
  EXPECT_FALSE(rl.GetTokens());
  EXPECT_FALSE(rl.GetTokens());
}


TEST(ConcurrentRateLimiterTest, MultiThreads) {
  TestClock::SetCurrentTime(0);
  double rate = 9999;
  double n_seconds = 3;
  ConcurrentRateLimiter rl(rate, n_seconds, TestClock::GetCurrentTimeSeconds);
  const int kThreadNum = 8;
  std::vector<std::thread> threads;
  std::atomic<int> tokens(0);
  std::atomic<bool> stop(false);
  for (int i = 0; i < kThreadNum; ++i) {
    threads.push_back(std::thread([&tokens, &stop, &rl] () {
      while (!stop.load()) {
        if (rl.GetTokens()) {
          tokens.fetch_add(1);
        }
      }
    }));
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(100));;
  EXPECT_EQ(tokens.load(), rate);

  TestClock::SetCurrentTime(1);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));;
  EXPECT_EQ(tokens.load(), rate + rate / n_seconds);

  TestClock::SetCurrentTime(3);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));;
  EXPECT_EQ(tokens.load(), rate + rate / n_seconds + 2 * rate / n_seconds);

  TestClock::SetCurrentTime(100);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));;
  EXPECT_EQ(tokens.load(),
            rate + rate / n_seconds + 2 * rate / n_seconds + rate);

  TestClock::SetCurrentTime(100);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));;
  EXPECT_EQ(tokens.load(),
            rate + rate / n_seconds + 2 * rate / n_seconds + rate);

  stop.store(true);

  for (auto& thread : threads) {
    thread.join();
  }
}


int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
