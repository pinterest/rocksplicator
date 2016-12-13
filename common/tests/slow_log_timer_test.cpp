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

#include <chrono>
#include <thread>

#include "common/slow_log_timer.h"
#include "gtest/gtest.h"

class TestSlowLogTimer : public common::SlowLogTimer {
 public:
  explicit TestSlowLogTimer(const std::string& metric,
                            std::string log_message,
                            uint64_t log_latency_threshold_ms=-1,
                            double log_sample_rate=0)
    : SlowLogTimer(metric, log_message,
                   log_latency_threshold_ms, log_sample_rate) {}
  static int logged_count;

  ~TestSlowLogTimer() {
    if (shouldLog()) {
      logged_count += 1;
    }
  }
};
int TestSlowLogTimer::logged_count = 0;

TEST(SlowLogTest, DoLog) {
  TestSlowLogTimer::logged_count = 0;
  for (int i = 0; i < 200; i ++) {
    TestSlowLogTimer timer("metric", "dolog", 1, 0.01);
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  // We expect to reach 399 and logged_count is actually be bumped 4 times
  EXPECT_EQ(TestSlowLogTimer::logged_count, 4);
}

TEST(SlowLogTest, DoNotLogForShortLifeCycle) {
  TestSlowLogTimer::logged_count = 0;
  {
    TestSlowLogTimer timer("metric", "donotlog", 100, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  EXPECT_EQ(TestSlowLogTimer::logged_count, 0);
}

TEST(SlowLogTest, DoNotLogForLowSampleRate) {
  TestSlowLogTimer::logged_count = 0;
  {
    TestSlowLogTimer timer("metric", "donotlog", 100, 0.0);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
  EXPECT_EQ(TestSlowLogTimer::logged_count, 0);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
