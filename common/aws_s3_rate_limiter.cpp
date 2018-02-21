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

#include "common/aws_s3_rate_limiter.h"

#include <thread>

using Aws::Utils::RateLimits::RateLimiterInterface;


namespace common {

RateLimiterInterface::DelayType AwsS3RateLimiter::ApplyCost(int64_t cost) {
  auto pre_state = atomic_state_.load();
  while (true) {
    auto current_seconds = clock_();
    State new_state(pre_state);
    if (current_seconds > pre_state.last_update_seconds) {
      auto new_budget =
        (current_seconds - pre_state.last_update_seconds) * rate_.load();

      new_state.budget = std::min(new_state.budget + new_budget, rate_.load());
      new_state.last_update_seconds = current_seconds;
    }

    new_state.budget -= cost;

    RateLimiterInterface::DelayType res(0);

    if (new_state.budget < 0) {
      res = RateLimiterInterface::DelayType(
        static_cast<int>((-new_state.budget) * 1000 / rate_.load()));
    }

    if (atomic_state_.compare_exchange_weak(pre_state, new_state)) {
      return res;
    }
  }
}

void AwsS3RateLimiter::ApplyAndPayForCost(int64_t cost) {
  auto time_to_sleep = ApplyCost(cost);
  if (time_to_sleep.count() > 0) {
    std::this_thread::sleep_for(time_to_sleep);
  }
}

void AwsS3RateLimiter::SetRate(int64_t rate, bool) {
  rate_.store(rate);
}

uint32_t AwsS3RateLimiter::GetCurrentTimeSeconds() {
  return static_cast<uint32_t>(time(nullptr));
}

}  // namespace common
