/// Copyright 2018 Pinterest Inc.
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

#include "common/aws_s3_rate_limiter.h"
#include "gtest/gtest.h"

using common::AwsS3RateLimiter;

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

TEST(AwsS3RateLimiterTest, Basics) {
  TestClock::SetCurrentTime(0);
  float rate = 10;
  AwsS3RateLimiter rl(rate, TestClock::GetCurrentTimeSeconds);

  auto delay = rl.ApplyCost(0);
  EXPECT_EQ(delay.count(), 0);

  delay = rl.ApplyCost(1);
  EXPECT_EQ(delay.count(), 0);

  delay = rl.ApplyCost(0);
  EXPECT_EQ(delay.count(), 0);

  // budget left = 9
  delay = rl.ApplyCost(8);
  EXPECT_EQ(delay.count(), 0);

  // budget left = 1
  delay = rl.ApplyCost(2);
  EXPECT_EQ(delay.count(), 100);

  delay = rl.ApplyCost(2);
  EXPECT_EQ(delay.count(), 300);

  delay = rl.ApplyCost(10);
  EXPECT_EQ(delay.count(), 1300);

  delay = rl.ApplyCost(0);
  EXPECT_EQ(delay.count(), 1300);

  // refill budget
  TestClock::SetCurrentTime(3);

  delay = rl.ApplyCost(10000);
  EXPECT_EQ(delay.count(), (10000 - 10) * 1000 / 10);
}


int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
