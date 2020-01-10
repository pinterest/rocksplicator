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

#pragma once

#include "aws/core/utils/ratelimiter/RateLimiterInterface.h"

#if __GNUC__ >= 8
#include "folly/synchronization/AtomicStruct.h"
#else
#include "folly/AtomicStruct.h"
#endif

#include "glog/logging.h"

namespace common {

/**
 * AwsS3RateLimiter is an implementation of AWS RateLimiterInterface.
 */
class AwsS3RateLimiter : public Aws::Utils::RateLimits::RateLimiterInterface {
  struct State {
    uint32_t last_update_seconds;
    float budget;
  };

 public:
  // Allow at most "rate" tokens per "n_seconds".
  explicit AwsS3RateLimiter(
      const float rate,
      std::function<uint32_t(void)> clock = GetCurrentTimeSeconds)
      : rate_(rate)
      , clock_(std::move(clock))
      , atomic_state_() {
    CHECK_GT(rate_.load(), 0);
    State s;
    s.last_update_seconds = clock_();
    s.budget = rate_.load();
    atomic_state_.store(s);
  }

  /**
   * Calculates time in milliseconds that should be delayed before letting anymore data through.
   */
  DelayType ApplyCost(int64_t cost) override;

  /**
   * Same as ApplyCost() but then goes ahead and sleeps the current thread.
   */
  void ApplyAndPayForCost(int64_t cost) override;

  /**
   * Update the bandwidth rate to allow.
   */
  void SetRate(int64_t rate, bool resetAccumulator = false) override;

 private:
  static uint32_t GetCurrentTimeSeconds();

  std::atomic<float> rate_;
  const std::function<uint32_t(void)> clock_;
  folly::AtomicStruct<State> atomic_state_;
};

}  // namespace common
