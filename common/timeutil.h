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
//
// Utility declares and functions for time.

#pragma once

#include <stdint.h>
#include <chrono>
#include <ctime>
#include <string>
#include <boost/chrono.hpp>

using std::string;
using std::chrono::time_point;
typedef std::chrono::high_resolution_clock hclock;
typedef std::chrono::steady_clock sclock;
using std::chrono::duration_cast;
typedef std::chrono::hours hours;
typedef std::chrono::minutes minutes;
typedef std::chrono::seconds seconds;
typedef std::chrono::milliseconds milliseconds;
typedef std::chrono::microseconds microseconds;
typedef std::chrono::nanoseconds nanoseconds;

namespace common {

enum TimeUnit { kHour, kMinute, kSecond, kMillisecond,
    kMicrosecond, kNanosecond };

// Timestamp values serialized with the timestamp type are encoded as 64-bit
// unsigned integers representing a number of seconds/milliseconds/microseconds
// since the standard base time known as the epoch: January 1 1970 at
//  00:00:00 GMT.
int64_t GetCurrentTimestamp(const TimeUnit type = kMillisecond);

}  // namespace common
