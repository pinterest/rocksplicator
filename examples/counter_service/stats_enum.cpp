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


#include "examples/counter_service/stats_enum.h"

#include <string>
#include <vector>

namespace counter {

const std::vector<std::string>* getCounterNames() {
  static const std::vector<std::string> counter_names = {
#define NEW_COUNTER_STAT(a, b) b,
#define NEW_METRIC_STAT(a, b)
#include "examples/counter_service/stats_def.h" // NOLINT
#undef NEW_METRIC_STAT
#undef NEW_COUNTER_STAT
  };

  return &counter_names;
}

const std::vector<std::string>* getMetricNames() {
  static const std::vector<std::string> metric_names = {
#define NEW_METRIC_STAT(a, b) b,
#define NEW_COUNTER_STAT(a, b)
#include "examples/counter_service/stats_def.h" // NOLINT
#undef NEW_COUNTER_STAT
#undef NEW_METRIC_STAT
  };

  return &metric_names;
}

}  // namespace counter
