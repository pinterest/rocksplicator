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

#include <cstdint>
#include <string>

#include "common/stats/stats.h"
#include "common/timer.h"

namespace common {

// A timer which uses GLOG to log slow requests (long Timer object life cycle)
// and logs the content with a life cycle time threshold and a sample rate.
class SlowLogTimer : public common::Timer {
 public:
  explicit SlowLogTimer(const uint32_t metric,
                        std::string log_message,
                        uint64_t log_latency_threshold_ms = 0,
                        uint64_t log_one_for_every_n_slow_requests = 0)
    : Timer(metric), log_message_(std::move(log_message)),
      log_latency_threshold_ms_(log_latency_threshold_ms),
      log_one_for_every_n_slow_requests_(log_one_for_every_n_slow_requests) {}

  explicit SlowLogTimer(const std::string& metric,
                        std::string log_message,
                        uint64_t log_latency_threshold_ms = -1,
                        uint64_t log_one_for_every_n_slow_requests = 0)
    : Timer(metric), log_message_(std::move(log_message)),
      log_latency_threshold_ms_(log_latency_threshold_ms),
      log_one_for_every_n_slow_requests_(log_one_for_every_n_slow_requests) {}

  // stop the clock and report the delta through metric_[str|int]_
  // also log the string content
  virtual ~SlowLogTimer() {
    if (shouldLog()) {
      LOG(WARNING) << "Slow request: " << log_message_;
    }
  }

 protected:
  bool shouldLog() {
    if (log_one_for_every_n_slow_requests_ == 0) {
      // Never log
      return false;
    }
    thread_local uint64_t total_slow_request_count = 1;
    auto elapsed_time = getElapsedTimeMs();
    if (elapsed_time > log_latency_threshold_ms_) {
      total_slow_request_count ++;
      return total_slow_request_count % log_one_for_every_n_slow_requests_ == 0;
    }
    return false;
  }

 private:
  const std::string log_message_;
  const uint64_t log_latency_threshold_ms_;
  const uint64_t log_one_for_every_n_slow_requests_;
};

}  // namespace common
