/// Copyright 2019 Pinterest Inc.
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

#include <glog/logging.h>
#include <ctime>
#include <ostream>
#include <ratio>
#include <type_traits>

#include "common/timeutil.h"

using std::chrono::duration;

namespace common {

int64_t GetCurrentTimestamp(const TimeUnit type) {
  time_point<hclock> t0 = hclock::now();
  switch (type) {
    case kHour:
      return duration_cast<hours>(t0.time_since_epoch()).count();
    case kMinute:
      return duration_cast<minutes>(t0.time_since_epoch()).count();
    case kSecond:
      return duration_cast<seconds>(t0.time_since_epoch()).count();
    case kMillisecond:
      return duration_cast<milliseconds>(t0.time_since_epoch()).count();
    case kMicrosecond:
      return duration_cast<microseconds>(t0.time_since_epoch()).count();
    case kNanosecond:
      return duration_cast<nanoseconds>(t0.time_since_epoch()).count();
    default:
      // By default return millisecond results
      return duration_cast<milliseconds>(t0.time_since_epoch()).count();
  }
}

}  // namespace timeutil
