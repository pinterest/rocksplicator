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

#include "common/kafka/stats_enum.h"

#include <string>
#include <utility>
#include <vector>

#include "gflags/gflags.h"

DEFINE_string(kafka_stats_prefix, "", "Prefix for the kafka stats");
DEFINE_string(kafka_stats_suffix, "", "Suffix for the kafka stats");

std::string getFullStatsName(const std::string& metric_name,
                             const std::initializer_list<std::string>& tags) {
  std::string full_metric_name;
  full_metric_name.append(FLAGS_kafka_stats_prefix)
      .append(metric_name)
      .append(FLAGS_kafka_stats_suffix);
  for (const auto& tag : tags) {
    full_metric_name.append(" ").append(tag);
  }
  return full_metric_name;
}
