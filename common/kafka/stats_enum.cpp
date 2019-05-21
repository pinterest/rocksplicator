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

DEFINE_string(corpus_type, "", "The corpus type");
DEFINE_string(index_type, "", "The index type");
DEFINE_string(normalized_cluster_name, "", "The normalized cluster name");
DEFINE_string(stats_prefix, "", "Prefix for the stats");

std::string getFullMetricName(const std::string& metric_name,
                              const std::initializer_list<std::string>& tags) {
  std::string full_metric_name;
  full_metric_name.append(FLAGS_stats_prefix)
      .append(metric_name)
      .append("_")
      .append(FLAGS_index_type)
      .append("_")
      .append(FLAGS_corpus_type)
      .append(" cluster=")
      .append(FLAGS_normalized_cluster_name);
  for (const auto& tag : tags) {
    full_metric_name.append(" ").append(tag);
  }
  return full_metric_name;
}
