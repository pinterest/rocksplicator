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


/**
 * Unit tests for stats.h
 */

#include "common/stats/stats.h"

#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

using common::Stats;
using std::chrono::milliseconds;
using std::chrono::seconds;
using std::this_thread::sleep_for;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;

namespace {

// The interval over which we collect stats. To prevent long tests,
// we condense a minute to be equal to 5 seconds.
static const int kIntervalSeconds = 5;

enum CounterEnum {
  kCounter1 = 0,
};

const std::string kCounter2 = "counter2";
const std::string kCounter3 = "counter3";

std::vector<std::string> counter_names = { "counter1", };

enum MetricEnum {
  kMetric1 = 0,
};

const std::string kMetric2 = "metric2";
const std::string kMetric3 = "metric3";

std::vector<std::string> metric_names = { "metric1", };

class StatsTest : public ::testing::Test {
 public:
  static void SetUpTestCase() {
    Stats::SetSecondsPerMin(kIntervalSeconds);
    Stats::init(&counter_names, &metric_names);
  }
};

// Function to increment counters every n milliseconds.
template <typename T>
void IncrCounter(const vector<T>& counters, milliseconds interval,
                 int num_incr) {
  for (int i = 0; i < num_incr; ++i) {
    for (auto& counter : counters) {
      Stats::get()->Incr(counter);
    }
    sleep_for(interval);
  }
}

// Function to record metrics every n milliseconds.
template <typename T>
void AddMetric(const vector<T>& metrics, milliseconds interval, int64_t min_val,
               int64_t max_val, int num_val) {
  for (int i = 0; i < num_val; ++i) {
    for (auto& metric : metrics) {
      static thread_local unsigned int seed = time(nullptr);
      Stats::get()->AddMetric(metric,
                              min_val + rand_r(&seed) % (max_val - min_val));
    }
    sleep_for(interval);
  }
}

template <typename T>
thread* LaunchCounterThread(const vector<T>& counters, milliseconds interval,
                            int num_incr) {
  return new thread(IncrCounter<T>, counters, interval, num_incr);
}

template <typename T>
thread* LaunchMetricThread(const vector<T>& metrics, milliseconds interval,
                           int64_t min_val, int64_t max_val, int num_val) {
  return new thread(AddMetric<T>, metrics, interval, min_val, max_val, num_val);
}

// Runs 2 threads 1 for 15 seconds updating counter1 and an overlapping 3 second
// long thread updating counter1, counter2. The counts are approximate as they
// can be off by a fudge factor roughly equal to the count in bucket (since
// one bucket may not be fully flushed).
TEST_F(StatsTest, StatsCounterTest) {
  unique_ptr<thread> thread1(
      LaunchCounterThread<uint32_t>({
    kCounter1
  },
                                    milliseconds(10), 1500));
  sleep_for(seconds(3));

  unique_ptr<Stats::Counter> counter1 = Stats::get()->GetCounter(kCounter1);
  EXPECT_NEAR(300, counter1->GetLastMinute(), 110);
  EXPECT_NEAR(300, counter1->GetTotal(), 110);

  unique_ptr<thread> thread2(
      LaunchCounterThread<uint32_t>({
    kCounter1
  },
                                    milliseconds(5), 600));
  unique_ptr<thread> thread3(LaunchCounterThread<string>({
    kCounter2
  },
                                                         milliseconds(5), 600));
  thread2->join();
  thread3->join();

  unique_ptr<Stats::Counter> counter2 = Stats::get()->GetCounter(kCounter2);
  EXPECT_NEAR(1100, counter1->GetLastMinute(), 160);
  EXPECT_NEAR(1200, counter1->GetTotal(), 160);
  EXPECT_NEAR(600, counter2->GetLastMinute(), 110);
  EXPECT_NEAR(600, counter2->GetTotal(), 110);

  sleep_for(seconds(3));

  EXPECT_NEAR(300, counter2->GetLastMinute(), 110);
  EXPECT_NEAR(600, counter2->GetTotal(), 110);
  thread1->join();

  EXPECT_NEAR(500, counter1->GetLastMinute(), 110);
  EXPECT_NEAR(2100, counter1->GetTotal(), 160);
  EXPECT_EQ(0, counter2->GetLastMinute());
  EXPECT_NEAR(600, counter2->GetTotal(), 110);

  EXPECT_EQ(nullptr, Stats::get()->GetCounter(kCounter3));
}

// Runs one thread updating metric1 for 15 seconds in the range (0, 100)
// uniformly distributed. Runs a second thread overlapping with above
// thread for 3 seconds updating metric2 for 3 seconds in the range
// (100, 200).
TEST_F(StatsTest, StatsHistogramTest) {
  unique_ptr<thread> thread1(
      LaunchMetricThread<uint32_t>({
    kMetric1
  },
                                   milliseconds(2), 0, 100, 7500));
  // Wait for 3 seconds.
  sleep_for(seconds(3));

  unique_ptr<Stats::Metric> metric1 = Stats::get()->GetMetric(kMetric1);
  EXPECT_NEAR(90, metric1->GetPercentileTotal(90), 5);
  EXPECT_NEAR(99, metric1->GetPercentileTotal(99), 5);
  EXPECT_NEAR(90, metric1->GetPercentileLastMinute(90), 5);
  EXPECT_NEAR(99, metric1->GetPercentileLastMinute(99), 5);

  unique_ptr<thread> thread2(
      LaunchMetricThread<uint32_t>({
    kMetric1
  },
                                   milliseconds(2), 100, 200, 1500));
  unique_ptr<thread> thread3(
      LaunchMetricThread<string>({
    kMetric2
  },
                                 milliseconds(2), 100, 200, 1500));
  thread2->join();
  thread3->join();

  EXPECT_NEAR(170, metric1->GetPercentileTotal(90), 5);
  EXPECT_NEAR(197, metric1->GetPercentileTotal(99), 5);
  EXPECT_NEAR(173, metric1->GetPercentileLastMinute(90), 5);
  EXPECT_NEAR(197, metric1->GetPercentileLastMinute(99), 5);
  unique_ptr<Stats::Metric> metric2 = Stats::get()->GetMetric(kMetric2);
  EXPECT_NEAR(190, metric2->GetPercentileTotal(90), 5);
  EXPECT_NEAR(199, metric2->GetPercentileTotal(99), 5);
  EXPECT_NEAR(190, metric2->GetPercentileLastMinute(90), 5);
  EXPECT_NEAR(199, metric2->GetPercentileLastMinute(99), 5);

  thread1->join();
  EXPECT_NEAR(90, metric1->GetPercentileLastMinute(90), 5);
  EXPECT_NEAR(99, metric1->GetPercentileLastMinute(99), 5);
  EXPECT_NEAR(140, metric1->GetPercentileTotal(90), 5);
  EXPECT_NEAR(194, metric1->GetPercentileTotal(99), 5);
  EXPECT_EQ(0, metric2->GetPercentileLastMinute(90));
  EXPECT_EQ(0, metric2->GetPercentileLastMinute(99));
  EXPECT_NEAR(190, metric2->GetPercentileTotal(90), 5);
  EXPECT_NEAR(199, metric2->GetPercentileTotal(99), 5);
}
}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
