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

#pragma once

#include <glog/logging.h>
#include <sys/time.h>
#include <stdlib.h>
#include <string>

#include "common/stats/stats.h"
#include "common/timer.h"

namespace common {

// A timer which uses GLOG to log slow requests (long Timer object life cycle)
// and logs the content with a life cycle time threshold and a sample rate.
class SlowLogTimer : public common::Timer {
 public:
  explicit SlowLogTimer(const uint32_t metric,
                        const std::string& log_message,
                        uint64_t log_latency_threshold=-1,
                        double log_sample_rate=0)
    : Timer(metric), log_message_(log_message),
      log_latency_threshold_(log_latency_threshold),
      log_sample_rate_(log_sample_rate) {}

  explicit SlowLogTimer(const std::string& metric,
                        const std::string& log_message,
                        uint64_t log_latency_threshold=-1,
                        double log_sample_rate=0)
    : Timer(metric), log_message_(log_message),
      log_latency_threshold_(log_latency_threshold),
      log_sample_rate_(log_sample_rate) {}

  // stop the clock and report the delta through metric_[str|int]_
  // also log the string content
  ~SlowLogTimer() {
    if (shouldLog()) {
      LOG(WARNING) << "Slow request: " << log_message_;
    }
  }

 protected:
  double generatereRandomDouble0To1() {
    return rand() / (RAND_MAX + 1.);
  }

  bool shouldLog() {
    auto elapsed_time = getElapsedTimeMs();
    return generatereRandomDouble0To1() <= log_sample_rate_ &&
            elapsed_time > log_latency_threshold_;
  }

  const std::string log_message_;
  const uint64_t log_latency_threshold_;
  const double log_sample_rate_;
};

}  // namespace common
