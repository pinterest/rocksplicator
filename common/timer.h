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


#pragma once

#include <chrono>
#include <string>

#include "common/stats/stats.h"

namespace common {

class Timer {
 public:
  // start the clock
  explicit Timer(const uint32_t metric)
    : metric_int_(metric)
    , metric_str_("")
    , start_(std::chrono::steady_clock::now()) {}

  explicit Timer(const std::string& metric)
    : metric_int_(0)
    , metric_str_(metric)
    , start_(std::chrono::steady_clock::now()) {}

  int64_t getElapsedTimeMs() {
    auto end = std::chrono::steady_clock::now();
    auto diff = end - start_;
    return std::chrono::duration_cast<std::chrono::milliseconds>(diff).count();
  }

  // stop the clock and report the delta through metric_[str|int]_
  virtual ~Timer() {
    auto elapsed_time = getElapsedTimeMs();

    if (metric_str_.empty()) {
      Stats::get()->AddMetric(metric_int_, elapsed_time);
    } else {
      Stats::get()->AddMetric(metric_str_, elapsed_time);
    }
  }

 protected:
  // fetch the current time in MS
  static uint64_t now() {
    struct timeval now;
    gettimeofday(&now, nullptr);
    return now.tv_sec * 1000 + now.tv_usec / 1000;
  }

 private:
  const uint32_t metric_int_;
  const std::string metric_str_;
  const std::chrono::time_point<std::chrono::steady_clock> start_;
};

}  // namespace common
