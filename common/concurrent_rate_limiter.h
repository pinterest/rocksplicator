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

#pragma once

#include "folly/synchronization/AtomicStruct.h"
#include "glog/logging.h"

namespace common {

/*
 * ConcurrentRateLimiter is a thread safe data structure for rate limiting.
 */
class ConcurrentRateLimiter {
  struct State {
    uint32_t last_update_seconds_;
    float n_tokens_;
  };

 public:
  // Allow at most "rate" tokens per "n_seconds".
  explicit ConcurrentRateLimiter(
      const double rate, const double n_seconds = 1,
      std::function<uint32_t(void)> clock = GetCurrentTimeSeconds)
      : rate_(rate)
      , n_seconds_(n_seconds)
      , clock_(std::move(clock))
      , atomic_state_() {
    CHECK_GT(rate_, 0);
    CHECK_GT(n_seconds_, 0);
    State s;
    s.last_update_seconds_ = clock_();
    s.n_tokens_ = rate_;
    atomic_state_.store(s);
  }

  // Try to get some tokens.
  // @return true if it is allowed.
  bool GetTokens(const uint32_t tokens = 1);

 private:
  static uint32_t GetCurrentTimeSeconds();

  const double rate_;
  const double n_seconds_;
  const std::function<uint32_t(void)> clock_;
  folly::AtomicStruct<State> atomic_state_;
};

}  // namespace common
