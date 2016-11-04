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

#include "common/concurrent_rate_limiter.h"

#include <algorithm>

#include "folly/Likely.h"

namespace common {

bool ConcurrentRateLimiter::GetTokens(const uint32_t tokens) {
  if (UNLIKELY(tokens <= 0)) {
    return true;
  }

  auto pre_state = atomic_state_.load();
  while (true) {
    auto current_seconds = clock_();
    State new_state(pre_state);
    if (current_seconds > pre_state.last_update_seconds_) {
      auto new_tokens =
        ((current_seconds - pre_state.last_update_seconds_) * rate_)
        / n_seconds_;

      new_state.n_tokens_ = std::min(new_state.n_tokens_ + new_tokens, rate_);
      new_state.last_update_seconds_ = current_seconds;
    }

    if (new_state.n_tokens_ < tokens) {
      return false;
    }

    new_state.n_tokens_ -= tokens;

    if (atomic_state_.compare_exchange_weak(pre_state, new_state)) {
      return true;
    }
  }
}

uint32_t ConcurrentRateLimiter::GetCurrentTimeSeconds() {
  return static_cast<uint32_t>(time(nullptr));
}

}  // namespace common
