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

#include "rocksdb_replicator/replicator_stats.h"

#include <string>

#include "common/stats/stats.h"

DEFINE_bool(replicator_enable_per_db_stats, true,
            "Enable per db stats");

namespace replicator {

const std::string kReplicatorLatency = "replicator_latency_ms";
const std::string kReplicatorOutBytes = "replicator_out_bytes";
const std::string kReplicatorInBytes = "replicator_in_bytes";
const std::string kReplicatorWriteBytes = "replicator_write_bytes";
const std::string kReplicatorConnectionErrors = "replicator_connection_errors";
const std::string kReplicatorRemoteApplicationExceptions =
  "replicator_remote_app_exceptions";
const std::string kReplicatorGetUpdatesSinceErrors =
  "replicator_get_update_since_errors";
const std::string kReplicatorGetUpdatesSinceMs =
  "replicator_get_update_since_ms";
const std::string kReplicatorWriteMs = "replicator_write_ms";
const std::string kReplicatorLeaderSequenceNumbersBehind = "replicator_leader_sequence_numbers_behind";


void logMetric(const std::string& metric_name, int64_t value,
               const std::string& db_name) {
  common::Stats::get()->AddMetric(metric_name, value);
  if (FLAGS_replicator_enable_per_db_stats && !db_name.empty()) {
    common::Stats::get()->AddMetric(metric_name + " db=" + db_name, value);
  }
}

void incCounter(const std::string& counter_name, uint64_t value,
                const std::string& db_name) {
  common::Stats::get()->Incr(counter_name, value);
  if (FLAGS_replicator_enable_per_db_stats && !db_name.empty()) {
    common::Stats::get()->Incr(counter_name + " db=" + db_name, value);
  }
}

}  // namespace replicator
